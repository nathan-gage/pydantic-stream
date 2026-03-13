# pydantic_stream

Rust-accelerated JSON projection for pydantic. Strips unknown fields from raw JSON bytes *before* pydantic validates, so Python objects are never allocated for fields your model doesn't declare.

## How It Works

```
Raw JSON bytes
    │
    ▼
Pass 1 — Rust projector (jiter, zero-copy)
  Scans forward through JSON bytes.
  Known fields: memcpy raw bytes into output buffer.
  Unknown fields: jiter.next_skip() — advances cursor, zero allocation.
    │
    ▼
Pass 2 — pydantic-core (TypeAdapter.validate_json)
  Validates the now-compact JSON normally.
    │
    ▼
Model instance
```

Pydantic's `extra="ignore"` parses unknown fields into Python objects, then discards them. The projector never parses them at all.

## API

```python
from pydantic import BaseModel
from pydantic_stream import StreamingBaseModelMixin

class MyModel(StreamingBaseModelMixin, BaseModel):
    id: int
    name: str

obj = MyModel.stream_model_validate_json(huge_bytes)           # single object
arr = MyModel.stream_model_validate_json_array(huge_bytes)      # array → StreamArray
for item in MyModel.stream_model_validate_json_array_iter(b):   # array → lazy iter
    ...
for item in MyModel.stream_model_validate_jsonl_iter(b):        # JSONL → lazy iter
    ...
```

`StreamingDataclassMixin` provides the same API for `@pydantic.dataclasses.dataclass` (methods are `stream_validate_*` instead of `stream_model_validate_*`).

## Development

```bash
uv sync
make dev            # build debug extension
make build-release  # build release extension (needed for benchmarks)
make test           # Rust + Python tests
make lint           # clippy + rustfmt + ruff
make fmt            # auto-format everything
make bench          # timing benchmarks
make bench-memory   # memory benchmarks (memray)
```

Run `make help` for all targets. See `benchmarks/README.md` for save/compare workflows and filtering options.
