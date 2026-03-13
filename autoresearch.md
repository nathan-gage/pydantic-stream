# Autoresearch: Minimize projection-parsing latency

## Objective
Minimize `total_ms`: sum of mean latencies for `stream_basemodel` and `stream_dataclass_slots`
across all 5 payload shapes (default, wide, deep, string-heavy, many-small). 500 JSON records per
run. Primary focus: Rust projection code that strips unknown fields before pydantic-core validation.

## Metrics
- **Primary**: `total_ms` (ms, lower is better) — sum of all 10 benchmark means
- **Secondary**: per-shape timings to catch regressions

## How to Run
```bash
./autoresearch.sh
```
Outputs `METRIC name=number` lines. Builds release extension first (~10s first time, incremental thereafter).

## Files in Scope
```
crates/pydantic-stream-core/src/projection.rs   # Core hot path: project_object_inner, copy_raw_value
crates/pydantic-stream-core/src/spec.rs         # FieldSpec / ObjectSpec data structures
crates/pydantic-stream-core/src/streaming.rs    # extract_array_items, navigate_to_prefix
crates/pydantic-stream-core/Cargo.toml          # Can add deps (ahash, smallvec already in lockfile)
crates/pydantic-stream-python/src/lib.rs        # PyO3 bindings (rarely needs changing)
src/pydantic_stream/stream_array.py             # Python StreamArray
src/pydantic_stream/base_model.py               # Python mixin (StreamingBaseModelMixin)
src/pydantic_stream/_schema.py                  # Spec compilation from pydantic schema
```

## Off Limits
- `benchmarks/` and `tests/` must not be modified
- Public Python API must not break

## Constraints
- `unsafe_code = "forbid"` in workspace — no unsafe blocks anywhere
- Tests must continue to pass (run `make test` to verify before final commit)
- clippy pedantic+nursery as warnings — don't add `#[allow]` without good reason
- New Rust crates OK if they're already in the lockfile (ahash 0.8.12, smallvec 1.15.1)
- MSRV 1.75, max line width 100

## Benchmark Shapes (500 records each)
- `many-small` (~0.9ms): No unknown fields, ~200B/record — baseline for pydantic-core overhead
- `deep`       (~1.4ms): 8-level nested unknown fields, ~27KB/record
- `default`    (~4.0ms): Mixed unknown blobs with nesting, ~27KB/record  
- `wide`       (~4.7ms): 200 flat unknown keys, ~27KB/record — stresses key-skipping
- `string-heavy` (~6.0ms): 3 huge string values (270KB/record) — stresses large-value skipping

## Architecture Notes
**Hot path for each record**: `project_object_inner` loops over JSON keys:
1. `jiter.known_object()` → returns `&str` key (borrows from jiter/tape)
2. `key.to_string()` → heap allocates a String (NECESSARY due to borrow checker)
3. `spec.fields.get(&key_owned)` → HashMap lookup
4. If unknown: `jiter.next_skip()` (skip value)
5. If known: `copy_raw_value` → get byte range, emit `"key":value` bytes
6. Loop via `jiter.next_key()` + `key.to_string()` for remaining keys

The `to_string()` call on step 2 is forced by jiter's API: `known_object()` returns `&str`
with lifetime tied to `&mut jiter`, so we can't call any jiter methods while holding it.
This causes O(keys) heap allocations per object.

**Key insight**: `ahash` and `smallvec` are already in the lockfile (transitive deps of jiter).
We can add them directly to pydantic-stream-core's Cargo.toml without adding new crates.

## What's Been Tried

### Baseline (commit: initial)
- total_ms: ~33.3ms
- stream_basemodel: wide=4.67, default=3.99, string-heavy=5.97, deep=1.44, many-small=0.88
- stream_dataclass_slots: wide=4.65, default=3.74, string-heavy=5.79, deep=1.37, many-small=0.76

### Ideas Queue
1. **AHashMap for spec.fields** — replace `HashMap` with `AHashMap` (ahash already in lockfile)
   - Expected: 10-20% speedup for wide/default (hash-heavy)
2. **Pre-encoded output keys** — store `"key":` as `Box<[u8]>` in FieldSpec, avoid per-call `write_json_string`
   - Expected: 5-10% speedup on all shapes
3. **Stack-allocated key buffer** — avoid heap alloc for `key.to_string()` using inline storage
   - Expected: significant speedup for wide (100K allocs/run → 0)
4. **Reuse projection output buffer** — pass a `&mut Vec<u8>` down from project_array* to avoid re-alloc
5. **project_array capacity tuning** — better initial capacity for output Vec
6. **SIMD/lookup-table for is_ascii_whitespace** in skip_ws
7. **Compute jiter index before/after to avoid double-parsing** for key extraction
