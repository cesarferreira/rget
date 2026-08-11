#!/usr/bin/env python3
"""Isolate the write-pattern cost, with no HTTP and no rget involved.

Three patterns, same 512 MiB payload, same 285 KiB block size:
  append     - plain sequential append to a growing file   (what wget does)
  prealloc   - ftruncate(size) then sequential pwrite      (what rget -c1 does)
  prealloc8  - ftruncate(size) then 8 interleaved pwrite streams (rget -c8)
"""
import os
import statistics
import sys
import time

SIZE = 512 * 1024 * 1024
BLOCK = 285 * 1024
REPS = 3
blk = b"\xa5" * BLOCK


def timed(fn, path):
    if os.path.exists(path):
        os.unlink(path)
    t0 = time.monotonic()
    fn(path)
    dt = time.monotonic() - t0
    os.unlink(path)
    return dt


def append(path):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
    written = 0
    while written < SIZE:
        n = min(BLOCK, SIZE - written)
        os.write(fd, blk[:n])
        written += n
    os.close(fd)


def prealloc(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    os.ftruncate(fd, SIZE)
    off = 0
    while off < SIZE:
        n = min(BLOCK, SIZE - off)
        os.pwrite(fd, blk[:n], off)
        off += n
    os.close(fd)


def prealloc8(path):
    fd = os.open(path, os.O_RDWR | os.O_CREAT)
    os.ftruncate(fd, SIZE)
    span = SIZE // 8
    cursors = [i * span for i in range(8)]
    ends = [(i + 1) * span for i in range(8)]
    ends[-1] = SIZE
    live = True
    while live:
        live = False
        for i in range(8):  # round-robin: interleaved offsets, like 8 workers
            if cursors[i] < ends[i]:
                n = min(BLOCK, ends[i] - cursors[i])
                os.pwrite(fd, blk[:n], cursors[i])
                cursors[i] += n
                live = True
    os.close(fd)


def main():
    target_dir = sys.argv[1]
    print(f"--- {target_dir} ---", flush=True)
    for name, fn in (("append", append), ("prealloc", prealloc), ("prealloc8", prealloc8)):
        ts = [timed(fn, os.path.join(target_dir, f"wp_{name}.bin")) for _ in range(REPS)]
        med = statistics.median(ts)
        print(f"  {name:<10} median {med:7.3f} s  {SIZE/1024/1024/med:8.2f} MiB/s  "
              f"runs={[round(t,3) for t in ts]}", flush=True)


if __name__ == "__main__":
    main()
