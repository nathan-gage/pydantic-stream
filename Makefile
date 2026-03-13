PYTHON_SOURCES := src/ tests/ demo/ benchmarks/

.DEFAULT_GOAL := help

.PHONY: .uv help dev build build-release test test-rust test-py bench bench-large bench-memory bench-memory-large lint fmt check

.uv:
	@uv -V >/dev/null 2>&1 || (printf "Please install uv: https://docs.astral.sh/uv/getting-started/installation/\n" >&2; exit 2)

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev            Sync deps and build the editable extension." \
	"  make build          Build the editable extension." \
	"  make build-release  Build the editable extension with --release." \
	"  make test           Run Rust + Python tests." \
	"  make test-rust      Run cargo tests only." \
	"  make test-py        Run Python tests only." \
	"  make bench          Run the standard timing benchmark suites." \
	"  make bench-large    Run the large-payload timing benchmarks." \
	"  make bench-memory   Run the standard memory benchmark suite." \
	"  make bench-memory-large  Run the large-payload memory benchmarks." \
	"  make lint           Run clippy, rustfmt --check, and ruff." \
	"  make fmt            Format Rust and Python code." \
	"  make check          Run lint + test." \
	"" \
	"For custom pytest filters or benchmark save/compare runs, use pytest directly:" \
	"  uv run pytest tests/ -k chunked -q" \
	"  uv run pytest benchmarks/ --help" \
	"  uv run pytest benchmarks/test_memory_benchmarks.py benchmarks/test_slice_benchmarks.py -m 'not large_payload' -k wide --benchmark-enable" \
	"  uv run pytest benchmarks/test_memory_profiles.py -m 'not large_payload' --benchmark-save baseline --benchmark-histogram" \
	"" \
	"See also: benchmarks/README.md"

dev: .uv
	uv sync
	uv run maturin develop

build: .uv
	uv run maturin develop

build-release: .uv
	uv run maturin develop --release

test: test-rust test-py

test-rust: .uv
	cargo test --workspace

test-py: .uv
	uv run pytest tests/

bench: .uv
	uv run pytest benchmarks/test_memory_benchmarks.py benchmarks/test_slice_benchmarks.py -m "not large_payload" --benchmark-enable

bench-large: .uv
	uv run pytest benchmarks/test_memory_benchmarks.py -m large_payload --benchmark-enable

bench-memory: .uv
	uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload"

bench-memory-large: .uv
	uv run pytest benchmarks/test_memory_profiles.py -m large_payload

lint: .uv
	cargo clippy --workspace -- -D warnings
	cargo fmt --check
	uv run ruff check $(PYTHON_SOURCES)

fmt: .uv
	cargo fmt
	uv run ruff check --fix $(PYTHON_SOURCES)
	uv run ruff format $(PYTHON_SOURCES)

check: lint test
