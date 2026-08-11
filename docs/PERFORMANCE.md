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
| **Request setup cost makes fan-out lose** | **Confirmed** | With 200 ms per-request latency, `-c8` took 0.634 s against wget's 0.254 s — 2.5× slower, on bandwidth that was never the limit. |

### The reproduction (W1 acceptance met)

Latency, not bandwidth, is where rget was losing. Round trips before the last
byte can move scale as `ceil(ranges / connections) × RTT`, and the plan created
four ranges per connection, so **every** download paid 4 × RTT of setup. A
loopback server with `--ttfb-ms 200` reproduces the live regression deterministically:

| 64 MiB, 200 ms TTFB | Baseline `064fd25` | After W3+W4 |
|---|---:|---:|
| wget | 0.249 s | 0.249 s |
| rget `-c1` | 0.438 s | **0.242 s** |
| rget `-c2` | 1.027 s | 0.436 s |
| rget `-c4` | 1.033 s | 0.437 s |
| rget `-c8` | 0.636 s | 0.440 s |

`-c1` now beats wget. Parallel modes improved by 1.4–2.4× but still sit at
**2 × RTT**, and the reason is structural — see W2.

## 2. The live regression is still not explained

**W3 and W4 do not account for the live numbers, and it would be wrong to read
them as a fix for issue #1.** Do the arithmetic: on the Rust static host, rget
lost by 5.8 s over 167.8 MiB. With 8 connections that plan was 32 ranges, so 4
setup waves; at ~20 ms RTT plus a TLS handshake per connection, the setup cost
removed by W3+W4 is on the order of **240 ms**, not 5.8 s. The mechanism is real
and worth removing — it just is not the one that produced the live matrix.

The 200 ms TTFB used to reproduce it locally is 10× a realistic CDN RTT. It
exposes the mechanism cleanly, which is what a diagnostic is for; it does not
prove the mechanism dominates in the field.

What still differs between the harness and the live runs:

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

Items 3 and 4 are the leading candidates — they are the only ones with the right
order of magnitude — and both point the same direction: **on a path where one
connection already saturates the bottleneck, opening eight is pure cost.** rget
still fans out to `--connections` regardless of whether fanning out helps, which
is W2.

Confirming this needs `netem` (slow start and loss cannot be modelled by a token
bucket) or instrumentation on a real CDN: per-range TTFB, retransmit counts, and
per-connection throughput over time. Until one of those runs, the cause is a
well-motivated hypothesis, not a finding.

## 3. Plan

Ordered by expected value. Each workstream names its hypothesis, its acceptance
test, and the branch carrying it.

### W1 — Reproduce the live gap deterministically `bench/harness` — **done**

Nothing else can be validated until the regression is reproducible without a CDN.

- Harness knobs for per-response cap, **aggregate** cap, and per-request TTFB.
  Done; see `scripts/bench/`.
- `netem.sh` for real RTT and loss on loopback, so slow start and congestion
  response come into play. Done, opt-in, needs root.
- **Acceptance met:** `--ttfb-ms 200` produces a configuration where `-c8` is
  2.5× slower than wget, reproducible to within 3 ms across reps. The cause was
  request-setup waves, not bandwidth.
- Still worth doing: sweep `netem` RTT ∈ {20, 100} ms × loss ∈ {0, 0.1}% ×
  aggregate cap ∈ {8, 50} MiB/s. The TTFB model prices request *count* but not
  TCP slow start or congestion response, so it cannot confirm whether those
  contribute to the live numbers on top of what W3/W4 already fixed.

### W2 — Earn the second round trip `perf/adaptive-connections` — **next up**

**The remaining gap, precisely.** A parallel download cannot issue its ranged
requests until it knows the file's size, and it only learns that from the probe.
So the floor for any parallel transfer is **2 × RTT**: one to learn the size, one
to fan out. wget's floor is 1 × RTT. After W3+W4 that is the entire difference
— 0.440 s versus 0.249 s in the table above is 0.191 s, one round trip.

That round trip is worth paying on a download lasting minutes and absurd on one
lasting 250 ms. Today rget pays it unconditionally.

**Design.** `--connections auto` (new default):

1. The priming probe (W3) already has byte 0 streaming on one connection at
   1 × RTT. Nothing else has happened yet, so there is nothing to lose.
2. Observe that stream briefly — a few hundred ms, or a couple of MiB.
3. Estimate remaining time as `remaining_bytes / observed_rate`. Fan out only
   when it exceeds the measured setup cost by a healthy multiple.
4. When fanning out, let the existing stealing machinery do it: workers that find
   no pending range split the running one. No re-planning required.

A short download therefore makes exactly one request and matches wget. A long one
pays a single extra round trip amortised over minutes, and gets whatever
parallelism actually helps.

- **Acceptance:** on the W1 configuration `auto` is within 2% of wget at every
  size; on a per-response-capped server it still reaches the ~9× win; on the
  aggregate-bottleneck configuration it stays within noise of `-c1`.
- **Risk:** a throughput estimate taken during slow start reads low and suppresses
  fan-out on a genuinely fast path. Mitigate by re-evaluating periodically rather
  than deciding once, and by keeping `--connections N` an absolute override.
- **Prerequisite met:** W4 already made splitting the primary rebalancing
  mechanism, so growing concurrency mid-transfer needs no new machinery.

### W3 — One round trip less, always `perf/fuse-probe` — **done**

**Result:** `-c1` went from 0.438 s to 0.242 s under 200 ms TTFB, overtaking
wget's 0.249 s. Request count drops by exactly one on every download, verified
against `GET /stats`. A download of a server with no range support now costs
exactly **one** request — byte-for-byte what wget does — and that is pinned by
`the_probe_costs_no_extra_request` and by the rewritten
`falls_back_to_sequential_without_range_support`.

<details>
<summary>Original design notes</summary>


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

</details>

**As shipped**, the scope guard turned out to be unnecessary: the primed body is
tagged with its source URL, claimed at most once, and simply dropped unless byte 0
is actually outstanding. A resume past byte 0 costs one aborted response and is no
worse than before, so `probe_priming` is used unconditionally for the primary URL.
`probe` remains for mirrors, where we want metadata and emphatically not a body.

### W4 — Fewer requests per download `perf/chunk-plan` — **done**

**Hypothesis:** `chunk_size = total / (connections × 4)` was tuned for load
balance and ignored request cost. Confirmed, and it was the dominant cost under
latency: 4 ranges per connection means 4 × RTT of setup on every download.

**Result:** now one range per connection, with `Scheduler::acquire`'s splitting as
the rebalancing mechanism instead of up-front oversubscription. `-c4` went 1.033 s
→ 0.437 s and `-c8` 0.636 s → 0.440 s under 200 ms TTFB. Verified no regression
where parallelism actually pays: against a 4 MiB/s per-response cap, `-c8` finishes
in 2.010 s against wget's 15.754 s, still ~7.8×.

The claim that stealing balances as well as oversubscription is the part that
deserves review scrutiny. `MAX_CHUNK` still bounds range size, so crash-recovery
granularity is unchanged for large files; `MIN_SPLIT_TAIL` and `SPLIT_MARGIN`
govern whether a straggler can be split at all, and those thresholds have not been
re-tuned for the new plan shape. A heterogeneous-latency server test (one range
served far slower than the rest) is the missing coverage.

### W4b — Abandoned in-flight bytes, and a harness trap worth knowing

Both changes above abandon response bodies: the fused probe stops at the first
chunk boundary, and every steal lowers a victim's ceiling while its response is
still streaming. Measured on loopback, that looked alarming — up to **4.12%** of a
512 MiB transfer, 22 MiB of paid-for bytes thrown away.

Two findings came out of chasing it, and the second is the more useful one.

**1. The fused probe should ask for a bounded range.** `bytes=0-` makes the server
send the whole file down a connection whose worker wants only the first chunk. It
now asks for `bytes=0-<MIN_CHUNK-1>` when fanning out, and `plan_primed` pins the
plan's first range to exactly the bytes the server served. Requests stay at one
per connection, so the round-trip win is untouched, and nothing is asked for that
will not be used. `-c1` still probes open-ended, because there the primed body is
the whole transfer.

**2. An uncapped loopback server wildly exaggerates abandoned-tail waste.** With
no bandwidth limit the server races arbitrarily far ahead of a client that is
about to stop reading. Give it a realistic link and the effect nearly vanishes:

| 512 MiB, `-c8` | Over-fetch |
|---|---:|
| Loopback, no bandwidth limit | +4.12% (22.1 MiB) |
| Same build over a 50 MiB/s link | **+0.003%** (15.8 KiB) |

On any real network, in-flight bytes are bounded by the receive window and the
bandwidth-delay product, not by how fast the server can spin. Baseline `064fd25`
measures ~0% on loopback only because its 4× oversubscription means idle workers
find pending ranges instead of stealing — it has fewer splits, not cheaper ones.

**Rule for this harness:** never quote a wasted-bytes number measured without
`--cap-mibs-total`. It is measuring the harness, in the same way the very first
uncapped throughput numbers in issue #1 were measuring the disk.

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
