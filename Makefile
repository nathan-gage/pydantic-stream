PYTHON_SOURCES := src/ tests/ demo/ benchmarks/ tools/
VENV_PYTHON := .venv/bin/python

.DEFAULT_GOAL := help

.PHONY: .uv help venv dev test test-rust test-py test-help bench bench-large bench-help bench-save bench-compare lint fmt check

.uv:
	@uv -V >/dev/null 2>&1 || (printf "Please install uv: https://docs.astral.sh/uv/getting-started/installation/\n" >&2; exit 2)

help:
	@printf "%s\n" \
	"Targets:" \
	"  make dev                 Sync deps and build the editable extension." \
	"  make test                Run Rust + Python tests." \
	"  make test-rust           Run cargo tests only." \
	"  make test-py             Run Python tests only." \
	"  make test-help           Show test CLI help." \
	"  make bench               Run standard benchmarks." \
	"  make bench-large         Run large-payload benchmarks." \
	"  make bench-save          Run benchmarks and save a JSON artifact (path=...)." \
	"  make bench-compare       Run benchmarks and compare against a prior artifact (path=... compare=...)." \
	"  make bench-help          Show benchmark CLI help." \
	"  make lint                Run clippy, rustfmt --check, and ruff." \
	"  make fmt                 Format Rust and Python code." \
	"  make check               Run lint + test." \
	"" \
	"Common usage:" \
	"  uv run python tools/dev.py --help" \
	"  uv run python tools/dev.py build --help" \
	"  uv run python tools/dev.py test --help" \
	"  uv run python tools/dev.py bench --help" \
	"" \
	"Examples:" \
	"  make dev" \
	"  make test-py args='--target tests/ -k chunked -q'" \
	"  make bench args='--shape default -k test_stream_basemodel -q'" \
	"  make bench-save path=benchmarks/results/before.json args='--shape default'" \
	"  make bench-compare path=benchmarks/results/after.json compare=benchmarks/results/before.json args='--shape default'" \
	"" \
	"See also: benchmarks/README.md"

dev: .uv
	uv sync
	$(VENV_PYTHON) tools/dev.py build

venv:
	@test -x "$(VENV_PYTHON)" || (printf "missing $(VENV_PYTHON); run 'make dev' or 'uv sync' first\n" >&2; exit 2)

test: venv
	$(VENV_PYTHON) tools/dev.py test $(args)

test-rust: venv
	$(VENV_PYTHON) tools/dev.py test --rust-only $(args)

test-py: venv
	$(VENV_PYTHON) tools/dev.py test --python-only $(args)

test-help:
	python tools/dev.py test --help

bench: venv
	$(VENV_PYTHON) tools/dev.py bench $(args)

bench-large: venv
	$(VENV_PYTHON) tools/dev.py bench --large $(args)

bench-save: venv
	@test -n "$(path)" || (printf "usage: make bench-save path=benchmarks/results/run.json [args='...']\n" >&2; exit 2)
	$(VENV_PYTHON) tools/dev.py bench --json $(path) $(args)

bench-compare: venv
	@test -n "$(path)" || (printf "usage: make bench-compare path=benchmarks/results/after.json compare=benchmarks/results/before.json [args='...']\n" >&2; exit 2)
	@test -n "$(compare)" || (printf "usage: make bench-compare path=benchmarks/results/after.json compare=benchmarks/results/before.json [args='...']\n" >&2; exit 2)
	$(VENV_PYTHON) tools/dev.py bench --json $(path) --compare-json $(compare) $(args)

bench-help:
	python tools/dev.py bench --help

lint: .uv
	cargo clippy --workspace -- -D warnings
	cargo fmt --check
	uv run ruff check $(PYTHON_SOURCES)

fmt: .uv
	cargo fmt
	uv run ruff check --fix $(PYTHON_SOURCES)
	uv run ruff format $(PYTHON_SOURCES)

check: lint
	$(MAKE) test
