# Performance: where rget stands, and the plan to beat wget everywhere

Status: 2026-08-11. Baseline commit `064fd25`.

This document exists because issue #1 produced a live-CDN matrix in which rget
was 12–24% **slower** than wget on three public hosts, and a local matrix in
which it was faster. Both were real. This is the reconciliation and the plan.

## 1. What is actually true today

Measured on x86_64/8-core, ext4 root (98% full), GNU Wget 1.21.4, rget 0.1.0
release, loopback range server. Full method in `scripts/bench/README.md`.

| Condition | wget | rget | Winner |
|---|---:|---:|:--|
| Destination on tmpfs, unlimited pipe, 512 MiB | 1254 MiB/s | **2151 MiB/s** (`-c1`) | rget, 1.7× |
| Destination on ext4, naive wall clock | 1239 MiB/s | 105 MiB/s | wget, 12× |
| Destination on ext4, **time to durable** | 84 MiB/s (`+fsync`) | **109 MiB/s** (`-c8`) | rget, 1.30× |
| Per-response cap 4 MiB/s, 64 MiB | 4.06 MiB/s | **32.72 MiB/s** | rget, 8.05× |
| Shared bottleneck link 8 MiB/s + 30 ms TTFB | 7.931 s | 7.933 s (`-c1`), 7.934 s (`-c8`) | tie |
| **Live public CDNs (issue #1, aarch64 host)** | 7.2–9.2 MiB/s | 6.3–7.0 MiB/s | **wget, 12–24%** |

### The 12× "loss" is not a loss

rget's 105 MiB/s on ext4 is exactly `dd conv=fsync`'s 105 MiB/s on the same
disk. rget saturates the hardware. wget's 1239 MiB/s is page cache — 512 MiB of
dirty pages that were never on the platter when the process exited. Compared
like with like (`wget` + explicit fsync), rget wins by 1.30×, because it
interleaves writeback with the transfer instead of serialising one flush at the
end.

**Conclusion: on every axis reproducible locally, rget already equals or beats
wget.** There is no local throughput bug.

### Hypotheses tested and killed

Recording these so nobody re-tests them:

| Hypothesis | Verdict | Evidence |
|---|---|---|
| The 500 ms checkpoint barrier is expensive | **Dead** | 500 ms vs 5 s vs never: no difference. Only **2** `fdatasync` calls per 512 MiB download. |
| Sparse `set_len` preallocation is allocator-hostile | **Dead** | `ftruncate` + sequential `pwrite` = 3414 MiB/s, same as plain append (3348). |
| 8 scattered `pwrite` streams fragment the file | **Dead** | 8 interleaved streams = 3334 MiB/s. No measurable cost. |
| The SQLite state store is in the hot path | **Dead** | Moving it to tmpfs changes nothing; only the destination's filesystem matters. |
| rget has a CPU/lock ceiling around 105 MiB/s | **Dead** | On tmpfs it does 2151 MiB/s. The ceiling was the disk. |
| More connections cost throughput on a shared link | **Not reproducible** | Under a fair token-bucket bottleneck, `-c1` and `-c8` finish within 3 ms of each other. |

## 2. The one real gap

The live regression is **not explained**, and nothing in the local harness
reproduces it. What differs between the harness and the live runs:

1. **TLS.** 8 connections means 8 handshakes, each a full RTT or two, versus
   wget's one.
2. **Real RTT.** Loopback is ~0 ms. Request setup cost is invisible locally.
3. **TCP slow start.** Eight flows each ramp their own congestion window from
   scratch. One flow ramps once and stays at full rate. On a high-BDP path this
   alone can explain a 20% deficit.
4. **Congestion response and loss.** Eight flows sharing a bottleneck queue
   overflow it more readily and each back off. The token bucket in
   `rangeserver.py` shares perfectly and never drops a packet, so it structurally
   cannot show this.
5. **Redirects.** The LM Studio URL redirects; that cost is paid per connection
   if connections do not share the resolved URL.

Items 3 and 4 are the leading candidates, and both point the same direction: **on
a path where one connection already saturates the bottleneck, opening eight is
pure cost.** rget currently always fans out to `--connections` regardless of
whether fanning out helps.

## 3. Plan

Ordered by expected value. Each workstream names its hypothesis, its acceptance
test, and the branch carrying it.

### W1 — Reproduce the live gap deterministically `bench/harness`

Nothing else can be validated until the regression is reproducible without a CDN.

- Harness knobs for per-response cap, **aggregate** cap, and per-request TTFB.
  Done; see `scripts/bench/`.
- `netem.sh` for real RTT and loss on loopback, so slow start and congestion
  response come into play. Done, opt-in, needs root.
- **Acceptance:** a local configuration in which `rget -c8` is ≥10% slower than
  `wget`, reproducible across 5 reps. Until this exists, every fix below is
  reasoning rather than measurement.
- Next: sweep `netem` RTT ∈ {20, 100} ms × loss ∈ {0, 0.1}% × aggregate cap
  ∈ {8, 50} MiB/s, comparing `-c1` / `-c2` / `-c8` / wget.

### W2 — Never pay for parallelism that does not help `perf/adaptive-connections`

**Hypothesis:** the live regression is the cost of 8 flows on a path where 1
saturates. If concurrency is earned rather than assumed, the regression cannot
occur by construction.

- `--connections auto` (new default): start with one connection and the
  open-ended lease that already exists in the scheduler. Sample throughput; add
  a connection only while aggregate throughput improves by a meaningful margin;
  stop and remember the plateau. Back off if throughput degrades.
- Skip fan-out entirely below a size threshold, where per-request setup cannot
  be amortised.
- **Acceptance:** on the W1 configuration, `auto` is within 2% of `wget`. On a
  per-response-capped server, `auto` still reaches the current 8× win. Neither
  regresses.
- **Risk:** a probing ramp can settle on a plateau caused by transient
  congestion. Mitigate with a floor (never below 1), a re-probe interval, and a
  hard override when `--connections N` is given explicitly.

### W3 — One round trip less, always `perf/fuse-probe`

**Hypothesis:** rget's first request is pure overhead relative to wget's.

`http::probe` issues `GET Range: bytes=0-0`, reads one byte, throws it away, and
only then starts real work. That is one extra RTT plus one extra request on every
download, and it is the reason rget can never match wget on a small file.

Fuse it: probe with `Range: bytes=0-`, and hand the still-open response body to
the first worker as an open-ended lease. The scheduler already supports
open-ended leases whose end is reassigned mid-flight (`set_end`, `OPEN_END`), so
this fits the existing design.

- 206 + parseable `Content-Range` → ranges work, size known, **and** the first
  chunk is already streaming.
- 200 → sequential mode, and the body is the whole file. This is byte-for-byte
  what wget does, so the sequential path becomes exactly as cheap as wget's.
- **Acceptance:** request count for a fresh download drops by exactly 1
  (verify against `GET /stats`); with `--ttfb-ms 200`, measured wall clock drops
  by ~200 ms; all existing tests pass.
- **Scope guard:** fresh downloads only. On resume, byte 0 may already be
  complete, so the fused probe would need the first *missing* offset. Keep the
  cheap one-byte probe for the resume path until W3 is proven.

### W4 — Fewer requests per download `perf/chunk-plan`

**Hypothesis:** `chunk_size = total / (connections × 4)` is tuned for load
balance and ignores request cost. With TTFB, every extra chunk is an extra RTT.

Now that work stealing exists, the 4× oversubscription is partly redundant: one
chunk per connection plus stealing for the tail should balance as well with a
quarter of the requests. Needs measurement, not assertion — stealing quality is
the whole question.

- **Acceptance:** request count drops ~4× with no regression in tail latency
  (p95 wall clock) on a heterogeneous-latency server.

### W5 — Lock the wins in `perf/regression-guard`

A perf claim that is not in CI decays. Add a `make bench` preset with fixed
payload and thresholds, and fail on regressions beyond noise. Keep it off the
default `make check` path, since it needs a couple of minutes and a quiet host.

### Explicitly not doing

**A `--no-fsync` fast mode.** It would "win" the naive wall-clock comparison by
giving up the property that distinguishes rget. rget already reaches durability
faster than wget can. The right response to the 12× naive gap is a benchmark
that measures durability, not a flag that abandons it.

## 4. Positioning

The defensible claims, all measured:

- Faster to **durability** than wget (1.30× here), because writeback overlaps the
  transfer.
- Dramatically faster when a server throttles **individual responses** (8.05×).
- Faster on the client's own byte path when storage is not the limit (1.7×).
- Resumable, with byte-exact integrity across kills.

The claim to avoid is a blanket "faster than wget". On a bottleneck link both
tools are limited by the link, and saying otherwise invites exactly the matrix
in issue #1.
