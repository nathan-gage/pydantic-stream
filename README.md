# pydantic_stream

## The Problem It Solves

When you call `Model.model_validate_json(huge_payload)`, pydantic deserializes **every** field in the JSON into Python objects — even fields your model doesn't declare. If the JSON has 200 extra keys with large nested blobs, embeddings, or audit trails, pydantic's Rust core (`pydantic-core`) still has to parse them, build Python objects for them, then immediately discard them. That's wasted CPU and, critically, wasted **memory** (every intermediate Python object goes on the heap and needs GC attention).

## The Two-Pass Architecture

`pydantic_stream` inserts a **Rust projection pass** before pydantic ever sees the data:

```
Raw JSON bytes
    │
    ▼
┌──────────────────────────────┐
│  Pass 1: Rust Projector      │  ← jiter (zero-copy JSON scanner)
│  Strips unknown fields,      │
│  copies known fields raw     │
│  Output: minimal JSON bytes  │
└──────────────────────────────┘
    │
    ▼
┌──────────────────────────────┐
│  Pass 2: pydantic-core       │  ← TypeAdapter.validate_json()
│  Validates + deserializes    │
│  the now-tiny JSON           │
└──────────────────────────────┘
    │
    ▼
  Model instance
```

## What Makes the Projector Fast and Memory-Efficient

### 1. `jiter` — zero-copy JSON scanning (no deserialization)

The Rust projector uses the [`jiter`](https://github.com/pydantic/jiter) crate (same one pydantic-core uses internally). When it hits an unknown field, it calls `jiter.next_skip()` which **advances the cursor past the value without allocating anything** — no strings, no numbers, no Python objects. It doesn't even parse the value; it just counts brackets/braces and skips.

### 2. Raw byte copying for known fields

For fields that *are* in the spec, the projector doesn't deserialize either. It records `start = jiter.current_index()`, skips the value, records `end = jiter.current_index()`, then does a `memcpy` of `input[start..end]` into the output buffer. The value's bytes are transferred verbatim — no parsing, no allocation, no Python object creation.

### 3. The hot path is entirely Rust → Rust

The projected bytes never become Python objects between passes. They go from the Rust projector's output buffer directly into `TypeAdapter.validate_json()`, which is also implemented in Rust (via pydantic-core). Python is only involved at the very end, when the final model instance is constructed.

### 4. Nested recursion with the same zero-copy approach

When a field's type is itself a model/dataclass, the `ObjectSpec` has a nested spec. The projector recurses into the nested object and strips unknown fields there too — at every depth level. Leaf fields (scalars, lists, unions) are copied as opaque raw bytes.

## What Parts of Pydantic's Flow It Replaces

It doesn't replace pydantic's validation — it **fronts** it with a filter:

| Step | Standard Pydantic | With pydantic_stream |
|------|------------------|---------------------|
| JSON parsing | pydantic-core parses **all** fields | Rust projector **skips** unknown fields via jiter |
| Python object creation | Creates Python objects for every value | Only creates objects for declared fields |
| Validation | Validates declared fields | Same — pydantic validates the projected JSON |
| Extra field handling | `extra="ignore"` still parses then drops | Never parsed in the first place |

The key insight: pydantic's `extra="ignore"` means "parse it, build a Python object, then throw it away." The projector means "never parse it at all."

## The Spec Compilation Layer

The `_schema.py` module bridges pydantic's schema system to the Rust projector:

1. It walks pydantic's **core schema** (the internal representation, not the JSON Schema)
2. It unwraps wrapper nodes (defaults, nullable, function validators, etc.)
3. It extracts all accepted input keys per field (handling `alias`, `AliasChoices`, `validate_by_name`)
4. For nested model/dataclass fields, it recursively compiles a nested `ObjectSpec`
5. The compiled `ObjectSpec` is cached as a class variable — compiled once, used forever

## Concrete Memory Savings

Consider a 13 MB JSON array where each record has ~27 KB but your model only needs ~200 bytes of fields. Standard pydantic allocates Python objects for all 27 KB per record. The projector:

- Scans the 13 MB in a single forward pass through the byte buffer
- Produces maybe ~100 KB of projected JSON (just the fields you need)
- Pydantic then only allocates objects for that ~100 KB

The benchmarks in the repo test this across 5 payload shapes (wide, deep, string-heavy, many-small, default) and 7 model variants, measuring both wall time (pytest-benchmark) and peak RSS (memray).

## API Surface

```python
# BaseModel usage
class MyModel(StreamingBaseModelMixin, BaseModel):
    id: int
    name: str

obj = MyModel.stream_model_validate_json(huge_bytes)           # single object
items = MyModel.stream_model_validate_json_array(huge_bytes)    # array → list
for item in MyModel.stream_model_validate_json_array_iter(b):   # array → lazy iter
    ...
for item in MyModel.stream_model_validate_jsonl_iter(b):        # JSONL → lazy iter
    ...

# Dataclass usage
@pydantic.dataclasses.dataclass
class MyDC(StreamingDataclassMixin):
    id: int
    name: str

obj = MyDC.stream_validate_json(huge_bytes)
```

The mixin is a drop-in addition — your existing pydantic models/dataclasses keep working normally, and you call `stream_*` methods when you want the projection optimization.

## Development

Common fixed workflows live in the `Makefile`:

```bash
uv sync
make dev
make test
make bench
make bench-memory
```

The Makefile is intentionally small. Use direct `pytest` commands whenever you
need filters, benchmark save/compare output, or one-off options:

```bash
uv run pytest tests/ -k chunked -q
uv run pytest benchmarks/ --help
uv run pytest benchmarks/test_memory_benchmarks.py benchmarks/test_slice_benchmarks.py \
  -m "not large_payload" -k wide --benchmark-enable --benchmark-save before
uv run pytest benchmarks/test_memory_profiles.py \
  -m "not large_payload" -k wide --benchmark-save before --benchmark-histogram
```

Useful fixed targets:

```bash
make dev
make help
make build-release
make test-rust
make test-py
make bench-large
make bench-memory-large
```

For raw pytest usage and the benchmark artifact save/compare flow, see
`benchmarks/README.md`.
