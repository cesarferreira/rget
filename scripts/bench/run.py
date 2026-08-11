#!/usr/bin/env python3
"""Paired A/B benchmark driver for rget against wget.

Runs each config N times with the order rotated every replicate, gives every
invocation a fresh output directory and a fresh state directory, verifies the
output SHA-256 against the source, and reports medians.

Clients:
  wget            one default request. Note: leaves its output dirty in page
                  cache, so this measures transfer, NOT durability.
  wget+fsync      same, plus an explicit fsync of the output. This is the
                  like-for-like comparison against rget, which fsyncs itself.
  rget            takes `conns`, and optionally `commit_ms` (needs the
                  RGET_BENCH_COMMIT_INTERVAL_MS override in src/engine.rs) and
                  `state_dir` (to place the SQLite store on another filesystem).

Read scripts/bench/README.md before interpreting any number this prints.

Example — concurrency ladder:

  python3 scripts/bench/run.py --url http://127.0.0.1:8099/file \\
      --source /tmp/big.bin --workdir /tmp/bench --reps 5 \\
      --configs '[{"label":"wget","client":"wget"},
                  {"label":"wget+fsync","client":"wget+fsync"},
                  {"label":"rget -c1","client":"rget","conns":1},
                  {"label":"rget -c8","client":"rget","conns":8}]'
"""
import argparse
import hashlib
import json
import os
import shutil
import statistics
import subprocess
import sys
import time

MIB = 1024 * 1024
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DEFAULT_RGET = os.environ.get("RGET_BIN", os.path.join(REPO_ROOT, "target", "release", "rget"))


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 22), b""):
            h.update(block)
    return h.hexdigest()


def fsync_file(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def run_once(cfg, args, expect_sha, size):
    """One measured invocation. Returns (seconds, ok). Leaves nothing behind."""
    out = os.path.join(args.workdir, "out")
    state = cfg.get("state_dir") or os.path.join(args.workdir, "state")
    for d in (out, state):
        shutil.rmtree(d, ignore_errors=True)
        os.makedirs(d)

    env = dict(os.environ)
    env["XDG_DATA_HOME"] = state
    env["XDG_CONFIG_HOME"] = os.path.join(state, "cfg")

    dst = os.path.join(out, "s.bin")
    client = cfg["client"]
    if client in ("wget", "wget+fsync"):
        cmd = ["wget", "-q", f"--timeout={args.timeout}", "-O", dst, args.url]
    elif client == "rget":
        cmd = [args.rget, "--quiet", "--dir", out, "-o", "s.bin",
               "--timeout", f"{args.timeout}s", "-c", str(cfg["conns"]), args.url]
        if cfg.get("commit_ms") is not None:
            env["RGET_BENCH_COMMIT_INTERVAL_MS"] = str(cfg["commit_ms"])
    else:
        sys.exit(f"unknown client {client!r}")

    start = time.monotonic()
    proc = subprocess.run(cmd, env=env, capture_output=True)
    if client == "wget+fsync":
        fsync_file(dst)
    elapsed = time.monotonic() - start

    ok = proc.returncode == 0 and os.path.exists(dst) and os.path.getsize(dst) == size
    if ok and sha256(dst) != expect_sha:
        ok = False
        sys.stderr.write(f"  !! {cfg['label']}: SHA-256 MISMATCH\n")
    if proc.returncode != 0:
        sys.stderr.write(f"  !! {cfg['label']}: rc={proc.returncode} "
                         f"{proc.stderr[-300:]!r}\n")

    shutil.rmtree(out, ignore_errors=True)
    shutil.rmtree(state, ignore_errors=True)
    return elapsed, ok


PRESETS = {
    # The default comparison: transfer speed and time-to-durable side by side.
    "default": [
        {"label": "wget", "client": "wget"},
        {"label": "wget+fsync", "client": "wget+fsync"},
        {"label": "rget -c1", "client": "rget", "conns": 1},
        {"label": "rget -c8", "client": "rget", "conns": 8},
    ],
    # Does connection count buy anything under these conditions?
    "ladder": [
        {"label": "wget", "client": "wget"},
        {"label": "rget -c1", "client": "rget", "conns": 1},
        {"label": "rget -c2", "client": "rget", "conns": 2},
        {"label": "rget -c4", "client": "rget", "conns": 4},
        {"label": "rget -c8", "client": "rget", "conns": 8},
    ],
    # Price the durability barrier. Needs RGET_BENCH_COMMIT_INTERVAL_MS support.
    "cadence": [
        {"label": "rget -c8 commit=500ms", "client": "rget", "conns": 8, "commit_ms": 500},
        {"label": "rget -c8 commit=5s", "client": "rget", "conns": 8, "commit_ms": 5000},
        {"label": "rget -c8 commit=never", "client": "rget", "conns": 8, "commit_ms": 3600000},
    ],
}


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", required=True)
    ap.add_argument("--source", required=True,
                    help="the file the server is serving, for SHA-256 verification")
    ap.add_argument("--workdir", required=True)
    ap.add_argument("--configs", help="JSON list of config objects")
    ap.add_argument("--preset", choices=sorted(PRESETS), default=None,
                    help="use a built-in config list instead of --configs")
    ap.add_argument("--reps", type=int, default=5)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--rget", default=DEFAULT_RGET)
    ap.add_argument("--json-out", default=None)
    args = ap.parse_args()

    if not os.path.exists(args.rget):
        sys.exit(f"no rget binary at {args.rget} (build it, or pass --rget/RGET_BIN)")

    if not args.configs and not args.preset:
        sys.exit("pass --preset or --configs")

    size = os.path.getsize(args.source)
    expect = sha256(args.source)
    configs = json.loads(args.configs) if args.configs else PRESETS[args.preset]
    os.makedirs(args.workdir, exist_ok=True)
    print(f"source {size} bytes ({size/MIB:.1f} MiB) sha256={expect[:16]}...  "
          f"reps={args.reps}", flush=True)

    results = {c["label"]: [] for c in configs}
    for rep in range(args.reps):
        # Rotate the order so no client is always first (cache/thermal drift).
        shift = rep % len(configs)
        for cfg in configs[shift:] + configs[:shift]:
            elapsed, ok = run_once(cfg, args, expect, size)
            if ok:
                results[cfg["label"]].append(elapsed)
            print(f"  rep{rep+1} {cfg['label']:<30} {elapsed:7.3f} s "
                  f"{size/MIB/elapsed:8.2f} MiB/s {'ok' if ok else 'FAIL'}", flush=True)

    print("\n=== medians ===", flush=True)
    summary = []
    for cfg in configs:
        times = results[cfg["label"]]
        if not times:
            print(f"{cfg['label']:<30} NO SUCCESSFUL RUNS", flush=True)
            continue
        med = statistics.median(times)
        summary.append({"label": cfg["label"], "runs": len(times), "median_s": med,
                        "median_mibs": size / MIB / med, "min_s": min(times),
                        "max_s": max(times), "all_s": times})
        print(f"{cfg['label']:<30} median {med:7.3f} s  {size/MIB/med:8.2f} MiB/s  "
              f"min {min(times):.3f} max {max(times):.3f}  n={len(times)}", flush=True)

    if args.json_out:
        with open(args.json_out, "w") as fh:
            json.dump({"size": size, "expect_sha256": expect, "summary": summary},
                      fh, indent=2)


if __name__ == "__main__":
    main()
