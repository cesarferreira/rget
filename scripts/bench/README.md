# Benchmark harness

Tools for comparing rget against wget honestly. Read this before quoting any
number these scripts print.

## The three traps

**1. wget does not fsync. rget does.** wget writes into page cache and exits;
its bytes may not be on disk for another 30 seconds. rget makes progress durable
before it exits. A naive wall-clock comparison is therefore "write to disk"
versus "write to RAM", and rget loses by ~12× on a slow disk while doing strictly
more work. Use the `wget+fsync` client for a like-for-like number.

**2. A loopback server is not a network.** No RTT, no TLS, no congestion, no
packet loss. It measures the *client*, which is useful and specific — it is not a
download-speed claim. Use `--ttfb-ms` and `--cap-mibs-total` to model the parts
that matter, and `netem.sh` when you need the real thing.

**3. Small files measure process startup.** At loopback speeds a 64 MiB payload
is dominated by exec, DNS, and the SQLite open. Use ≥512 MiB for uncapped work.

**4. Never measure wasted bytes without a bandwidth cap.** An uncapped loopback
server races arbitrarily far ahead of a client that is about to stop reading, so
any abandoned response body looks enormous. The same build measured +4.12%
over-fetch on loopback and **+0.003%** over a 50 MiB/s link. Always use
`--cap-mibs-total` before quoting a wasted-bytes figure.

## Layout

| File | Purpose |
|---|---|
| `rangeserver.py` | Well-behaved HTTP/1.1 range server with latency/bandwidth knobs |
| `run.py` | Paired A/B driver: rotated order, fresh state, SHA-256 verified, medians |
| `writepat.py` | Isolates write-pattern cost with no HTTP and no rget involved |
| `netem.sh` | Opt-in `tc` RTT/loss on loopback. Needs root. Reverts cleanly. |

`rangeserver.py` is deliberately *well-behaved*. It is the opposite of
`tests/harness/mod.rs`, which sets `Connection: close` on every response and
exists to misbehave for correctness tests — that makes it useless for throughput.

## Quickstart

```bash
cargo build --release
head -c 536870912 /dev/urandom > /tmp/big.bin

python3 scripts/bench/rangeserver.py /tmp/big.bin &

python3 scripts/bench/run.py \
  --url http://127.0.0.1:8099/file --source /tmp/big.bin \
  --workdir /tmp/bench --reps 5 \
  --configs '[{"label":"wget","client":"wget"},
              {"label":"wget+fsync","client":"wget+fsync"},
              {"label":"rget -c1","client":"rget","conns":1},
              {"label":"rget -c8","client":"rget","conns":8}]'
```

`make bench` runs this as a preset.

## Server knobs, and what each one models

| Flag | Models | Who should win |
|---|---|---|
| *(none)* | An unlimited pipe | Tests the client's own byte path |
| `--cap-mibs N` | Origin/CDN throttling **each response** | rget, by roughly the connection count |
| `--cap-mibs-total N` | A **bottleneck link** shared by all flows | Nobody — extra connections buy nothing |
| `--ttfb-ms N` | RTT, TLS handshake, edge lookup per request | Whoever makes **fewer requests** |

`GET /stats` returns request counters, so you can see how many requests each
client actually made. Request count is a first-class cost once `--ttfb-ms` is on.

`--cap-mibs-total` is a *friendly* bottleneck: it shares perfectly and never
drops a packet. Real congested links do neither, so it **understates** the cost
of parallelism. That is exactly why it could not reproduce the live-CDN
regression in issue #1 — see `docs/PERFORMANCE.md`.

## Isolating a suspected bottleneck

Put the destination and the SQLite state store on different filesystems to find
out which one you are actually measuring:

```bash
python3 scripts/bench/run.py --url ... --source ... --workdir /tmp/bench --reps 3 \
  --configs '[{"label":"out=disk state=tmpfs","client":"rget","conns":8,"state_dir":"/dev/shm/st"},
              {"label":"out=disk state=disk","client":"rget","conns":8}]'
```

Set `"commit_ms": 5000` on a config to change the durability barrier cadence
(requires the `RGET_BENCH_COMMIT_INTERVAL_MS` override in `src/engine.rs`). Use
this to price checkpointing rather than guessing at it.

Always sanity-check the harness ceiling before trusting a fast number:

```bash
wget -q -O /dev/null http://127.0.0.1:8099/file   # server ceiling
dd if=/dev/zero of=/tmp/t bs=285K count=1838 conv=fsync   # durable disk ceiling
dd if=/dev/zero of=/tmp/t bs=285K count=1838              # page-cache ceiling
```

If rget's number equals the `conv=fsync` number, rget is at the hardware limit
and there is nothing left to optimise. That is what happened in issue #1.
