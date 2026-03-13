# Benchmarks

The recommended entry point is the developer CLI:

```bash
uv sync
uv run python tools/dev.py build --release
uv run python tools/dev.py bench --help
make help
make bench-help
```

The raw pytest benchmark flags are still available if you want them directly:

```bash
uv run pytest benchmarks/ --help
```

## Save and Compare with the Developer CLI

Use `pytest-benchmark`'s JSON output as the saved artifact format:

```bash
uv run python tools/dev.py build --release
uv run python tools/dev.py bench --json benchmarks/results/before.json
uv run python tools/dev.py bench --json benchmarks/results/after.json --compare-json benchmarks/results/before.json
```

Useful CLI flags:

- `--target benchmarks/test_memory_benchmarks.py`
- `--shape default --shape wide`
- `--large`
- `--no-memory`
- `--json benchmarks/results/run.json`
- `--compare-json benchmarks/results/base.json`
- `-k test_stream_basemodel`
- `-q`

## Save and Compare with Raw `pytest`

Use `pytest-benchmark`'s JSON output as the saved artifact format:

```bash
uv run pytest benchmarks/ --benchmark-json benchmarks/results/before.json
```

Then compare a later run against that saved artifact:

```bash
uv run pytest benchmarks/ \
  --benchmark-json benchmarks/results/after.json \
  --benchmark-compare-json benchmarks/results/before.json
```

Notes:

- `--benchmark-json` saves the standard `pytest-benchmark` timing data.
- `pydantic-stream` also injects its memray results into the same JSON artifact when memory profiling is enabled.
- `--benchmark-compare-json` compares the current run against any prior `--benchmark-json` artifact, including artifacts produced before this feature existed. Older artifacts will compare timing only.
- The compare path must be different from the current `--benchmark-json` output path, or pytest will refuse to run.

## Save and Compare with `make`

The `Makefile` now wraps the developer CLI instead of exposing a large matrix
of variables. Run `make dev` once first so the project `.venv` and extension are ready:

```bash
make dev
make bench
make bench-save path=benchmarks/results/before.json
make bench-compare path=benchmarks/results/after.json compare=benchmarks/results/before.json
```

Use `args='...'` when you want to pass through CLI flags:

```bash
make bench args='--shape default -k test_stream_basemodel -q'
make bench-save path=benchmarks/results/default-before.json args='--shape default'
make bench-compare path=benchmarks/results/default-after.json compare=benchmarks/results/default-before.json args='--shape default'
make bench-large args='--shape wide --no-memory'
```

Common variants:

```bash
uv run python tools/dev.py bench --shape default --json benchmarks/results/default-before.json
uv run python tools/dev.py bench --shape default --json benchmarks/results/default-after.json --compare-json benchmarks/results/default-before.json
uv run python tools/dev.py bench --no-memory --json benchmarks/results/timing-only.json
uv run python tools/dev.py bench --large --shape wide --json benchmarks/results/wide-large.json

uv run pytest benchmarks/ --payload-shape default --benchmark-json benchmarks/results/default-before.json
uv run pytest benchmarks/ --payload-shape default --benchmark-json benchmarks/results/default-after.json --benchmark-compare-json benchmarks/results/default-before.json
uv run pytest benchmarks/ --no-memory --benchmark-json benchmarks/results/timing-only.json
uv run pytest benchmarks/ --large-payload --payload-shape wide --benchmark-json benchmarks/results/wide-large.json

make bench-save path=benchmarks/results/default-before.json args='--shape default'
make bench-compare path=benchmarks/results/default-after.json compare=benchmarks/results/default-before.json args='--shape default'
make bench-save path=benchmarks/results/timing-only.json args='--no-memory'
make bench-large args='--shape wide --json benchmarks/results/wide-large.json'
```
