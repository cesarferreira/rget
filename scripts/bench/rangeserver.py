#!/usr/bin/env python3
"""Threaded HTTP/1.1 range server for benchmarking download clients.

Serves one file. Advertises `Accept-Ranges: bytes`, supplies a strong ETag,
honours single byte-range requests, and never closes a connection early. This is
deliberately a *well-behaved* server -- the opposite of tests/harness/mod.rs,
which sets `Connection: close` on everything and exists to misbehave.

Three knobs let it model the network conditions that matter for a download
client. They are independent and can be combined:

  --cap-mibs N        throttle EACH response independently to N MiB/s. Models an
                      origin/CDN that limits a single response. This is the case
                      parallel ranged downloads are designed to win.

  --cap-mibs-total N  throttle ALL responses to N MiB/s in aggregate, via one
                      shared token bucket. Models a bottleneck link, where extra
                      connections cannot buy bandwidth. This is the common case
                      on the public internet, and the one where naive
                      parallelism only adds overhead.

  --ttfb-ms N         wait N ms before the first byte of each response. Models
                      per-request latency: RTT, TLS handshake, CDN edge lookup.
                      Cost scales with the NUMBER of requests a client makes, so
                      it prices a client's request overhead (range probes,
                      re-requests, chunk count).

  GET /stats          JSON request counters, so a benchmark can report how many
                      requests each client actually made.

What it does NOT model: TCP slow start, congestion-driven loss, or per-flow
fairness on a congested path. For those you need `tc netem` / `tc tbf` on a real
interface. Treat --cap-mibs-total as a *friendly* bottleneck: it shares
perfectly and never drops a packet, so it understates the cost of parallelism on
a genuinely congested link.
"""
import argparse
import hashlib
import json
import os
import re
import socket
import socketserver
import threading
import time

RANGE_RE = re.compile(r"^bytes=(\d*)-(\d*)$")
SEND_BLOCK = 1 << 20  # 1 MiB per send-loop iteration


class TokenBucket:
    """Shared aggregate rate limiter. Hands out send permits at `rate` bytes/s."""

    def __init__(self, rate):
        self.rate = rate
        self.lock = threading.Lock()
        self.allowance = float(rate)
        self.last = time.monotonic()

    def take(self, want):
        """Block until `want` bytes may be sent. Returns bytes granted."""
        while True:
            with self.lock:
                now = time.monotonic()
                self.allowance = min(self.rate,
                                     self.allowance + (now - self.last) * self.rate)
                self.last = now
                if self.allowance >= 1:
                    grant = int(min(want, self.allowance))
                    self.allowance -= grant
                    return grant
                deficit = (1 - self.allowance) / self.rate
            time.sleep(max(deficit, 0.0005))


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        sock = self.request
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        buf = b""
        while True:
            while b"\r\n\r\n" not in buf:
                try:
                    more = sock.recv(65536)
                except OSError:
                    return
                if not more:
                    return
                buf += more
            head, buf = buf.split(b"\r\n\r\n", 1)
            lines = head.decode("latin-1").split("\r\n")
            parts = lines[0].split()
            if len(parts) < 2:
                return
            method, path = parts[0].upper(), parts[1]
            headers = {}
            for line in lines[1:]:
                if ":" in line:
                    k, v = line.split(":", 1)
                    headers[k.strip().lower()] = v.strip()

            if path == "/stats":
                self._serve_stats(sock)
                continue
            if not self._serve_file(sock, method, headers):
                return

    def _serve_stats(self, sock):
        srv = self.server
        with srv.counter_lock:
            body = json.dumps({
                "requests": srv.request_count,
                "range_requests": srv.range_count,
                "head_requests": srv.head_count,
                "bytes_sent": srv.bytes_sent,
                "connections": srv.conn_count,
            }).encode()
        hdrs = [
            "HTTP/1.1 200 OK",
            "Content-Type: application/json",
            f"Content-Length: {len(body)}",
            "Connection: keep-alive",
        ]
        try:
            sock.sendall(("\r\n".join(hdrs) + "\r\n\r\n").encode("latin-1") + body)
        except OSError:
            pass

    def _serve_file(self, sock, method, headers):
        """Returns False if the connection should be dropped."""
        cfg = self.server.cfg
        total, etag = cfg["size"], cfg["etag"]

        start, end, partial = 0, total - 1, False
        rng = headers.get("range")
        if rng:
            m = RANGE_RE.match(rng.strip())
            if not m:
                self._send_error(sock, 416, {"Content-Range": f"bytes */{total}"})
                return True
            lo, hi = m.group(1), m.group(2)
            if lo == "" and hi == "":
                self._send_error(sock, 416, {"Content-Range": f"bytes */{total}"})
                return True
            if lo == "":
                length = min(int(hi), total)
                start, end = total - length, total - 1
            else:
                start = int(lo)
                end = int(hi) if hi != "" else total - 1
            if start >= total or start > end:
                self._send_error(sock, 416, {"Content-Range": f"bytes */{total}"})
                return True
            end = min(end, total - 1)
            partial = True

        length = end - start + 1
        srv = self.server
        with srv.counter_lock:
            srv.request_count += 1
            if partial:
                srv.range_count += 1
            if method == "HEAD":
                srv.head_count += 1

        hdrs = {
            "Content-Type": "application/octet-stream",
            "Content-Length": str(length),
            "Accept-Ranges": "bytes",
            "ETag": etag,
            "Last-Modified": cfg["last_modified"],
            "Connection": "keep-alive",
        }
        if partial:
            hdrs["Content-Range"] = f"bytes {start}-{end}/{total}"

        # Per-request latency, paid before the first byte. A client that makes
        # more requests pays this more times.
        if cfg["ttfb"]:
            time.sleep(cfg["ttfb"])

        status = "206 Partial Content" if partial else "200 OK"
        resp = [f"HTTP/1.1 {status}"] + [f"{k}: {v}" for k, v in hdrs.items()]
        try:
            sock.sendall(("\r\n".join(resp) + "\r\n\r\n").encode("latin-1"))
        except OSError:
            return False
        if method == "HEAD":
            return True

        per_cap = cfg["cap_bytes"]
        bucket = self.server.bucket
        try:
            with open(cfg["path"], "rb") as fh:
                fh.seek(start)
                remaining = length
                t0 = time.monotonic()
                sent = 0
                while remaining > 0:
                    want = min(SEND_BLOCK, remaining)
                    if bucket is not None:
                        want = bucket.take(want)
                    data = fh.read(want)
                    if not data:
                        return False
                    sock.sendall(data)
                    remaining -= len(data)
                    sent += len(data)
                    with srv.counter_lock:
                        srv.bytes_sent += len(data)
                    if per_cap:
                        slack = (t0 + sent / per_cap) - time.monotonic()
                        if slack > 0:
                            time.sleep(slack)
        except OSError:
            return False
        return True

    def _send_error(self, sock, code, extra):
        hdrs = {"Content-Length": "0", "Connection": "keep-alive", **extra}
        resp = [f"HTTP/1.1 {code} Error"] + [f"{k}: {v}" for k, v in hdrs.items()]
        try:
            sock.sendall(("\r\n".join(resp) + "\r\n\r\n").encode("latin-1"))
        except OSError:
            pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128

    def process_request(self, request, client_address):
        with self.counter_lock:
            self.conn_count += 1
        super().process_request(request, client_address)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("path")
    ap.add_argument("--port", type=int, default=8099)
    ap.add_argument("--cap-mibs", type=float, default=0.0,
                    help="throttle EACH response to this many MiB/s (0 = off)")
    ap.add_argument("--cap-mibs-total", type=float, default=0.0,
                    help="throttle ALL responses to this many MiB/s combined (0 = off)")
    ap.add_argument("--ttfb-ms", type=float, default=0.0,
                    help="delay before the first byte of every response")
    args = ap.parse_args()

    size = os.path.getsize(args.path)
    h = hashlib.md5()
    with open(args.path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)

    srv = Server(("127.0.0.1", args.port), Handler)
    srv.cfg = {
        "path": args.path,
        "size": size,
        "etag": '"%s"' % h.hexdigest(),
        "cap_bytes": int(args.cap_mibs * 1024 * 1024),
        "ttfb": args.ttfb_ms / 1000.0,
        "last_modified": time.strftime("%a, %d %b %Y %H:%M:%S GMT", time.gmtime()),
    }
    srv.bucket = (TokenBucket(args.cap_mibs_total * 1024 * 1024)
                  if args.cap_mibs_total else None)
    srv.counter_lock = threading.Lock()
    srv.request_count = srv.range_count = srv.head_count = 0
    srv.bytes_sent = srv.conn_count = 0
    print(f"serving {args.path} ({size} bytes)\n"
          f"  per-response cap : {args.cap_mibs or 'none'} MiB/s\n"
          f"  aggregate cap    : {args.cap_mibs_total or 'none'} MiB/s\n"
          f"  ttfb             : {args.ttfb_ms or 0} ms\n"
          f"  url              : http://127.0.0.1:{args.port}/file\n"
          f"  stats            : http://127.0.0.1:{args.port}/stats", flush=True)
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
