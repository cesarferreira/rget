# SQLite ↔ filesystem crash-consistency protocol

This is the load-bearing design decision of the project. Everything else
(scheduler, workers, resume) is written to obey it.

## The problem

A naive downloader does:

```
write(fd, buf, offset)        // returns Ok
UPDATE ranges SET state='complete'
```

`write()` returning `Ok` means "the kernel accepted these bytes into the page
cache". It does **not** mean the bytes survive power loss. SQLite, meanwhile,
*is* durable on commit (with the right pragmas). So the naive ordering produces
the one failure mode we must never have:

> metadata claims a range is complete, the file data for it was never written
> to stable storage, and on resume we skip re-downloading it — producing a file
> with a hole full of zeros that passes every size check.

Checksums would catch it at the very end of a 500 GB download. That is not
good enough.

## The rule

**A durability barrier separates writing bytes from claiming bytes.**

```
                 t0                    t1                       t2
    worker writes bytes ──▶  fdatasync(dest) ──▶  SQLite COMMIT (durable)
    [claim NOTHING]          [bytes now stable]   [claim only bytes < t0]
```

Formally, the committer task runs this loop:

1. **Snapshot.** Read each active worker's `written_upto` atomic. This is the
   file offset up to which that worker has called `write_all_at` and had it
   return `Ok`. Call the snapshot `S`.
2. **Barrier.** `fdatasync(dest_fd)` (on a blocking thread). When this returns,
   every byte written before step 1 is on stable storage.
3. **Claim.** In one SQLite transaction, persist `S`: set `bytes_written` for
   partial ranges and `state='complete'` for ranges fully covered by `S`.
   `synchronous=FULL` makes the commit itself durable before we return.

Because step 2 is strictly between the writes and the claim, the persisted
state is always a **subset** of what is durable on disk. The converse — disk
ahead of metadata — is safe and expected: it just means we re-download a
bounded amount (at most one commit interval, default 500 ms of throughput)
after a crash.

`bytes_written` therefore means *durable prefix*, never *in-flight prefix*.
Workers never write it themselves; only the committer does, and only after a
barrier.

## Why not fsync per chunk

Because that caps throughput at the device's sync rate and destroys the
multi-gigabit target. Instead the barrier is **amortised**: one `fdatasync`
covers all workers' writes since the last barrier, at a fixed ~2 Hz. The cost
of a crash is bounded re-download, not corruption — which is exactly the
tradeoff §12 of the PRD asks for ("prefer downloading a small amount of data
again over trusting potentially corrupt data").

`fdatasync` rather than `fsync`: the file's size and block map are what we need
durable, not its mtime. Because the file is preallocated (or extended) before
any range that touches new blocks is claimed, `fdatasync` is sufficient.

## SQLite settings

| Pragma | Value | Why |
|---|---|---|
| `journal_mode` | `WAL` | A torn commit rolls back on next open, so the DB is always readable after a crash (PRD Invariant 7). |
| `synchronous` | `FULL` | WAL commits are fsynced. `NORMAL` would let a commit be lost *and* — worse — reordered relative to the data fsync. |
| `foreign_keys` | `ON` | `forget` must not leave orphan ranges. |
| `busy_timeout` | 5000 ms | Two `rget` processes may touch the store concurrently. |

Every state transition is a single transaction. There is no multi-statement
"partially updated" state to observe.

## Trusting the file, not just the database

Persisted state is necessary but not sufficient: the file could have been
deleted, truncated, moved, or replaced by a different file at the same path
between runs. Before trusting any range, resume checks the destination's
**identity**:

- `st_dev` / `st_ino` recorded at creation must still match, and
- a random 128-bit `file_cookie` is recorded in the DB at creation time, and
- `len(file) >= max(end of every range claimed complete)`.

If identity does not match, the on-disk file is a stranger: refuse to resume
into it (offer `--restart` / `--overwrite`). If the file is merely shorter than
claimed — a truncating filesystem recovery — every range beyond the real length
is demoted to `pending` rather than trusted. This is the "do not trust
persisted state more than the filesystem justifies" principle from §40.

Ranges found in `downloading` state at startup were owned by a process that
died. Their bytes past the durable watermark are unknown, so they resume from
`start + bytes_written`, never from a guess.

## What this does *not* protect against

- **A filesystem that lies about `fdatasync`** (some consumer SSDs with write
  caching, `nobarrier` mounts, some network filesystems). No userspace program
  can fix that. The end-to-end checksum is the backstop, which is why
  `--sha256` is strongly recommended for very large transfers.
- **Concurrent external writers** to the destination file. Out of scope.
- **Silent bit rot** after a successful verify. Out of scope.

## Invariant mapping

| PRD invariant | Enforced by |
|---|---|
| 1 — never complete before bytes reach the writer | committer's barrier; `bytes_written` is durable-prefix only |
| 2 — no overlapping ownership | scheduler splits by raising the new owner's `start` above the victim's atomic `end`; a worker writes only below its own `end` |
| 3 — completed ranges have no gaps | plan is a partition of `[0, size)`; ranges only shrink at a split, and the split's remainder becomes a new range in the same transaction |
| 4 — no mixing resource versions | validators checked on every resume and on every mirror; `If-Range` on every ranged request |
| 5 — checksum failure is never success | verification result gates the terminal state; mismatch sets `status='failed'` and exits non-zero |
| 6 — one worker's failure preserves others | range-level retry; a failed range is demoted, never the download |
| 7 — state readable after a mid-update kill | WAL + single-transaction updates |
