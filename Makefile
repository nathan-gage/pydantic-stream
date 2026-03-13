.PHONY: dev test bench bench-large lint fmt check clean

dev:
	uv sync
	maturin develop

test: dev
	cargo test
	uv run pytest tests/

bench:
	uv sync
	maturin develop --release
	uv run pytest benchmarks/ --benchmark-enable

bench-large:
	uv sync
	maturin develop --release
	uv run pytest benchmarks/ --benchmark-enable --large-payload

lint:
	cargo clippy --workspace -- -D warnings
	cargo fmt --check
	uv run ruff check src/

fmt:
	cargo fmt
	uv run ruff check --fix src/
	uv run ruff format src/

check: lint test

clean:
	cargo clean
	rm -rf dist/ *.egg-info/ .pytest_cache/ .ruff_cache/ .mypy_cache/
