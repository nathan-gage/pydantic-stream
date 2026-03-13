# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

Rust-accelerated JSON projection for pydantic. A Rust projector (using jiter for zero-copy scanning) strips unknown fields from raw JSON bytes, then pydantic-core validates the compact result. This avoids allocating Python objects for fields the model doesn't declare.

## Build & Development

Requires: `uv`, Rust stable toolchain (pinned via `rust-toolchain.toml`).

```bash
make dev            # uv sync + maturin develop (debug build)
make build-release  # maturin develop --release (needed before benchmarks)
```

After any Rust change you must rebuild before running Python tests.

## Commands

```bash
make test           # cargo test + pytest tests/
make test-rust      # Rust only
make test-py        # Python only
make lint           # clippy (deny warnings) + rustfmt --check + ruff check
make fmt            # cargo fmt + ruff fix + ruff format
make check          # lint + test

uv run pytest tests/ -k "test_name" -q   # single test

make bench              # standard timing
make bench-large        # large timing (~100 MB)
make bench-memory       # standard memory (memray)
make bench-memory-large # large memory
```

Ruff checks `src/ tests/ demo/ benchmarks/` locally; CI only checks `src/`.

## Benchmark Save/Compare

```bash
# Timing
uv run pytest benchmarks/test_memory_benchmarks.py benchmarks/test_slice_benchmarks.py \
  -m "not large_payload" --benchmark-enable --benchmark-save before
uv run pytest ... --benchmark-enable --benchmark-compare

# Memory (artifacts under .benchmarks/memory/)
uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload" --benchmark-save before
uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload" --benchmark-compare

# Filtering: -m for payload size, -k for shape/model variant
# -k wide / -k deep / -k stream_basemodel / -k pydantic_dataclass_slots
```

See `benchmarks/README.md` for histograms and advanced options.

## Architecture

```
crates/pydantic-stream-core/     Pure Rust core — projection and streaming logic
crates/pydantic-stream-python/   PyO3 bindings (cdylib → _native.so)
src/pydantic_stream/             Python package — mixins, schema compilation, StreamArray
```

**Data flow:** Raw JSON bytes → Rust projector (skips unknown fields, memcpys known fields) → compact JSON → `TypeAdapter.validate_json` → Python model.

**Key files:**
- `_schema.py` — compiles pydantic core_schema → ObjectSpec. Handles aliases, AliasChoices, validate_by_name, nested models. Rejects nested alias paths (AliasPath with len > 1).
- `base_model.py` / `dataclass.py` — mixins that cache ObjectSpec + TypeAdapter as ClassVars.
- `stream_array.py` — `StreamArray[T]`: lazy random-access container that projects + validates on demand.

## Conventions

- Rust: `unsafe_code = "forbid"`, clippy pedantic+nursery as warnings, `unwrap_used`/`expect_used` warned (tests `#[allow]`), MSRV 1.75, line width 100
- Python: >=3.11, ruff (E/F/I/UP/B), line length 100 (relaxed in tests/benchmarks/demo), `filterwarnings = ["error"]` in pytest
