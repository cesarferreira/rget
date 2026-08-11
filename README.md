<div align="center">
  <h1>rget</h1>

  <p><strong>High-performance resumable download manager</strong></p>

  <p>
    <img alt="License" src="https://img.shields.io/badge/license-MIT-green">
    <img alt="Rust" src="https://img.shields.io/badge/rust-1.85%2B-orange">
    <img alt="Edition" src="https://img.shields.io/badge/edition-2024-blue">
  </p>

  <p>
    <a href="#install">Install</a>
    &nbsp;·&nbsp;
    <a href="#quickstart">Quickstart</a>
    &nbsp;·&nbsp;
    <a href="#development">Development</a>
  </p>
</div>

---

## Install

Requires [Rust](https://rustup.rs) **1.85+** and `~/.cargo/bin` on your `PATH`.

```bash
cargo install rget-cli
```

> Published as **`rget-cli`**, installs a command called **`rget`**. The bare
> `rget` name on crates.io belongs to an unrelated project last published in
> 2017. Before the first release, install from git instead:
> `cargo install --git https://github.com/cesarferreira/rget`

Verify:

```bash
rget --help
```

<details>
<summary><strong>Build from source</strong> — for development or unreleased changes</summary>

```bash
git clone https://github.com/cesarferreira/rget.git
cd rget
cargo install --path . --locked
# or
make install-release
```

Debug install (faster compile, larger binary):

```bash
make install
```

Run without installing:

```bash
make build-release
./target/release/rget
```

</details>

<a id="quickstart"></a>
## Quickstart

Give it a URL. It works out the filename, uses as many connections as the server
supports, and writes straight into the destination file.

```bash
rget https://cdn.kernel.org/pub/linux/kernel/v6.x/linux-6.7.tar.xz
```

```
linux-6.7.tar.xz
  74.2 MiB / 135 MiB   55.0%
  91.5 MiB/s           ETA 1s
  8 connections        18/33 chunks
███████████████████████░░░░░░░░░░░░░░░░░ 55%
```

The first time you run it, `rget` asks where downloads should go, prefilled with
your system's Downloads folder:

```
Where should rget save downloads?
  Folder [~/Downloads]:
```

Press Enter to accept, or type somewhere else. It only asks once. `--dir`
overrides it for a single download, and `rget config --dir <path>` changes it
for good. If stdin is not a terminal — a script, a pipeline, `--json`, `--quiet`
— it never asks and quietly uses the system Downloads folder, so automation
behaves identically to a terminal.

**If anything interrupts it, run the same command again.** No `--resume` flag,
no `.part` files to clean up:

```
Resuming linux-6.7.tar.xz
36.70 MiB / 135 MiB already downloaded
✓ Checksum verified
✓ linux-6.7.tar.xz
  135 MiB downloaded in 5s
  Average speed: 16.4 MiB/s
```

That survives Ctrl+C, `SIGKILL`, a dropped network, a closed terminal and a
reboot. Progress is kept in a central SQLite database, so it survives across
runs and across machines-worth-of-downloads.

### Common uses

```bash
# Name it yourself, or pick a directory
rget URL -o ubuntu.iso
rget URL --dir ~/Downloads

# Verify what you got
rget URL --sha256 da1ed7d47c97ed72c9354091628740aa3c40a3c9cd7382871f3cedbd60588234

# Tune the transfer
rget URL --connections 16 --limit 20MiB/s --timeout 30s

# Mirrors of the same file: failed ranges retry against whichever is healthy
rget https://mirror1/f.iso https://mirror2/f.iso --sha256 <digest>

# Authentication and custom headers
rget URL --user alice:secret
rget URL --header 'Authorization: Bearer …' --header 'X-Trace: 1'
```

### Managing downloads

```bash
rget config           # where downloads go, and where state lives
rget config --dir ~/ISOs
rget config --reset   # be asked again next time

rget list             # everything this machine knows about
rget info a82fd1      # validators, ranges, progress, last error
rget resume a82fd1    # continue one, with no other flags needed
rget resume --all     # continue everything that was interrupted
rget forget a82fd1    # drop the metadata; never touches the file
```

### Scripting

`--json` writes one structured event per line to **stdout**; human progress
always goes to stderr, and turns itself off when stderr is not a terminal.

```bash
rget URL --json | jq -c 'select(.event == "download_completed")'
```

```json
{"event":"download_started","filename":"linux-6.7.tar.xz","total_size":141975800,"connections":8,"parallel":true}
{"event":"download_completed","downloaded":141975800,"elapsed_ms":5012,"average_bps":17196544}
```

`RUST_LOG=debug rget URL` explains its decisions: range assignment, retries,
resume reasoning and validator mismatches. Credentials are never logged.

## How it works

| Concern | Approach |
|---|---|
| **Parallelism** | The file is split into byte ranges; workers `pwrite` straight into the destination at the right offset. No temp files, no merge pass, so a 500 GB download needs 500 GB of disk and finishing costs nothing. |
| **Memory** | One body chunk per connection, streamed to disk. Peak memory is independent of file size. |
| **Resume** | Progress lives in SQLite in your platform's data dir (`~/.local/share/rget/downloads.db`, `~/Library/Application Support/rget/downloads.db`). Settings live in the same database, so there is one file to move or delete. |
| **Where files land** | The system Downloads folder by default — on Linux that is `XDG_DOWNLOAD_DIR`, which is localised, so it is read rather than guessed. |
| **Safety** | Before reusing a byte, `rget` re-validates the remote with `ETag`/`Last-Modified`/size and checks the local file is still the same file. If the remote changed, it refuses rather than splicing two versions together. |
| **Slow servers** | Ranges are subdivided on the fly, so one slow connection does not hold up the tail of a download. |
| **Failure** | Per-range retries with exponential backoff and jitter, honouring `Retry-After`. One range failing never discards another's progress. |

### Durability

The interesting part. `write()` returning `Ok` does **not** mean the bytes
survive power loss, but a SQLite commit does — so recording progress before
flushing data would let the database claim a range is complete when the file has
a hole in it. `rget` therefore puts a durability barrier between the two:

```
worker writes ──▶ fdatasync(dest) ──▶ SQLite COMMIT
[claim nothing]   [bytes durable]     [claim only pre-barrier bytes]
```

Persisted state is always a *subset* of what is on disk. The worst a crash can
cost is one commit interval of re-downloaded bytes — never a corrupt file.

The barrier runs at ~2 Hz rather than per chunk, so it does not cap throughput.
Full rationale, including what this does *not* protect against, is in
[`docs/CRASH_CONSISTENCY.md`](docs/CRASH_CONSISTENCY.md).

## Testing

`rget` is tested against a purpose-built hostile HTTP server rather than the
public internet. It can ignore `Range`, lie about `Content-Length`, send
malformed `Content-Range`, hang up mid-body, dribble bytes, change its `ETag`
mid-download, and answer with 429/500/502/503 plus `Retry-After`.

```bash
cargo test                        # unit + integration + property tests
cargo test -- --ignored           # the soak test: repeated random kills
```

The resume tests `SIGKILL` the real binary mid-transfer, restart it, and require
the finished file to match the source cryptographically. Range management is
property-tested over randomised complete/fail/split/restart sequences, asserting
that ranges never overlap, never gap, and never extend past the content length.

<a id="development"></a>
## Development

Module layout mirrors the concerns above — `engine` orchestrates, `scheduler`
plans ranges, `worker` moves bytes, `storage` persists, `resume` reconciles,
`ui` renders, and nothing else knows what a terminal is.

Common tasks via the `Makefile`:

```bash
make              # check + build + test
make build        # debug build
make build-release
make install      # install debug binary
make install-release
make run ARGS="--help"
make check        # cargo check + clippy
make fmt          # format
make lint         # fmt check + clippy
make test
make clean
make demo         # install + show --help
```

Releasing (requires [cargo-release](https://github.com/crate-ci/cargo-release) and [git-cliff](https://github.com/orhun/git-cliff)):

```bash
make release                  # default minor bump
make release LEVEL=patch      # patch bump
make release LEVEL=major      # major bump
```

The pre-release hook regenerates `CHANGELOG.md` with `git-cliff` from your conventional-commit history (grouped into Features, Bug Fixes, etc. per `cliff.toml`) and commits it alongside the version bump. Pushing the resulting `v*` tag triggers the release workflow, which builds the multi-platform binaries and publishes a GitHub Release whose notes are generated by `git-cliff` from the same config.

## License

MIT
