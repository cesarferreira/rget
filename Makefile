.PHONY: all build build-release install install-release clean test check fmt lint run demo release bench

LEVEL ?= minor

# Benchmark payload size and location (see scripts/bench/README.md)
BENCH_SIZE ?= 536870912
BENCH_DIR ?= /tmp/rget-bench
BENCH_REPS ?= 5
BENCH_PRESET ?= default

# Default target
all: check build test

# Build debug version
build:
	cargo build

# Build release version
build-release:
	cargo build --release

# Install debug binary to ~/.cargo/bin
install:
	CARGO_INCREMENTAL=0 cargo install --path . --locked --bins --debug --force

# Install release binary to ~/.cargo/bin
install-release:
	CARGO_INCREMENTAL=0 cargo install --path . --locked --bins --force

# Clean build artifacts
clean:
	cargo clean

# Run tests
test:
	cargo test

# Run clippy and check
check:
	cargo check
	cargo clippy -- -D warnings

# Format code
fmt:
	cargo fmt

# Lint (check formatting)
lint:
	cargo fmt -- --check
	cargo clippy -- -D warnings

# Run with arguments (usage: make run ARGS="--hello")
run:
	cargo run -- $(ARGS)

# Quick demo
demo: install
	@echo "=== rget demo ==="
	rget --help

# Bump version, regenerate CHANGELOG.md, tag, publish, and push (requires cargo-release + git-cliff)
release:
	cargo release $(LEVEL) --execute --no-confirm

# Compare against wget on a controlled range server. Needs a quiet host; read
# scripts/bench/README.md before quoting any number this prints.
bench: build-release
	@mkdir -p $(BENCH_DIR)
	@test -f $(BENCH_DIR)/payload.bin || \
		(echo "generating $(BENCH_SIZE) byte payload..." && \
		 head -c $(BENCH_SIZE) /dev/urandom > $(BENCH_DIR)/payload.bin)
	@python3 scripts/bench/rangeserver.py $(BENCH_DIR)/payload.bin --port 8099 & \
	 SRV=$$!; sleep 2; \
	 python3 scripts/bench/run.py \
	   --url http://127.0.0.1:8099/file \
	   --source $(BENCH_DIR)/payload.bin \
	   --workdir $(BENCH_DIR)/wd --reps $(BENCH_REPS) --preset $(BENCH_PRESET); \
	 RC=$$?; kill $$SRV 2>/dev/null; exit $$RC
