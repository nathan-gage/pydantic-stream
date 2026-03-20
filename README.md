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
for item in MyModel.stream_model_validate_json_array_iter(b):   # sync array stream → iter
    ...
async for item in MyModel.stream_model_validate_json_array_aiter(resp.content):
    ...
for item in MyModel.stream_model_validate_jsonl_iter(b):        # JSONL → lazy iter
    ...
```

`StreamingDataclassMixin` provides the same API for `@pydantic.dataclasses.dataclass` (methods are `stream_validate_*` instead of `stream_model_validate_*`).

For projection-free use cases, the top-level helpers are available too:

```python
from pydantic import TypeAdapter
from pydantic_stream import stream_json_array, stream_json_array_async

adapter = TypeAdapter(MyModel)
items = list(stream_json_array(open("data.json", "rb"), adapter))
items_async = [
    item
    async for item in stream_json_array_async(response.content.iter_chunked(65536), adapter)
]
```

## Development

```bash
uv sync
make dev            # build debug extension
make build-release  # build release extension (needed for benchmarks)
make test           # Rust tests + fresh editable extension + Python tests
make lint           # clippy + rustfmt + ruff
make fmt            # auto-format everything
make bench          # release build + timing benchmarks
make bench-memory   # release build + memory benchmarks (memray)
```

Python tests now fail fast if the native extension is missing or older than the Rust sources, so stale editable builds cannot hide regressions.

Run `make help` for all targets. See `benchmarks/README.md` for save/compare workflows and filtering options.
