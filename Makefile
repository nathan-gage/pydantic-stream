PYTHON_SOURCES := src/ tests/ demo/ benchmarks/

.PHONY: dev test bench bench-large lint fmt check

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
	uv run ruff check $(PYTHON_SOURCES)

fmt:
	cargo fmt
	uv run ruff check --fix $(PYTHON_SOURCES)
	uv run ruff format $(PYTHON_SOURCES)

check: lint test
