# Benchmarks

Build the extension once before running benchmarks:

```bash
uv sync
uv run maturin develop --release
```

There are now two separate benchmark suites:

- Timing: [benchmarks/test_memory_benchmarks.py](/Users/ngage/repos/pydantic-stream/benchmarks/test_memory_benchmarks.py) and [benchmarks/test_slice_benchmarks.py](/Users/ngage/repos/pydantic-stream/benchmarks/test_slice_benchmarks.py)
- Memory: [benchmarks/test_memory_profiles.py](/Users/ngage/repos/pydantic-stream/benchmarks/test_memory_profiles.py)

Selection is pytest-native:

- Standard-size runs: `-m "not large_payload"`
- Large-payload runs: `-m large_payload`
- Shape filtering: `-k wide`, `-k deep`
- Case filtering: `-k stream_basemodel`, `-k pydantic_dataclass_slots`

## Timing Benchmarks

Run the standard timing suites:

```bash
uv run pytest \
  benchmarks/test_memory_benchmarks.py \
  benchmarks/test_slice_benchmarks.py \
  -m "not large_payload" \
  --benchmark-enable
```

Save and compare timing runs with stock `pytest-benchmark`:

```bash
uv run pytest \
  benchmarks/test_memory_benchmarks.py \
  benchmarks/test_slice_benchmarks.py \
  -m "not large_payload" \
  --benchmark-enable \
  --benchmark-save before

uv run pytest \
  benchmarks/test_memory_benchmarks.py \
  benchmarks/test_slice_benchmarks.py \
  -m "not large_payload" \
  --benchmark-enable \
  --benchmark-compare
```

Common variants:

```bash
uv run pytest benchmarks/test_memory_benchmarks.py -m large_payload --benchmark-enable
uv run pytest benchmarks/test_memory_benchmarks.py -m "not large_payload" -k wide --benchmark-enable
uv run pytest benchmarks/test_memory_benchmarks.py -m "not large_payload" -k "deep and stream_basemodel" --benchmark-enable
```

## Memory Benchmarks

Run the standard memory suite:

```bash
uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload"
```

The memory suite reuses `pytest-benchmark`'s save/compare/storage/histogram
flags, but stores its artifacts under `<benchmark-storage>/memory` so they do
not collide with timing artifacts:

```bash
uv run pytest benchmarks/test_memory_profiles.py \
  -m "not large_payload" \
  --benchmark-save before

uv run pytest benchmarks/test_memory_profiles.py \
  -m "not large_payload" \
  --benchmark-compare

uv run pytest benchmarks/test_memory_profiles.py \
  -m "not large_payload" \
  --benchmark-histogram
```

Notes:

- `--benchmark-save` and `--benchmark-autosave` save standalone memory artifacts under `.benchmarks/memory` by default.
- `--benchmark-compare` with no value compares against the latest saved run. The memory suite also accepts a saved-name match such as `--benchmark-compare smoke`.
- `--benchmark-histogram` writes per-case SVG plots for the current memory run. When memray exposes enough snapshots, the x-axis is time since start and the y-axis is RSS. Fast cases fall back to peak-memory-by-round bars.
- `--benchmark-save-data` includes the raw per-round memory timelines in the saved artifact.

Common variants:

```bash
uv run pytest benchmarks/test_memory_profiles.py -m large_payload
uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload" -k wide --benchmark-save wide-baseline
uv run pytest benchmarks/test_memory_profiles.py -m "not large_payload" -k "deep and stream_dataclass"
```

## Make Targets

The fixed targets cover the common entrypoints:

```bash
make bench
make bench-large
make bench-memory
make bench-memory-large
```

Use direct `pytest` commands whenever you need custom `-k`, `-m`, save/compare,
or histogram combinations.
