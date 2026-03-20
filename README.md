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

Keep the public story mixin-first:

```python
from pydantic import BaseModel
from pydantic_stream import StreamingBaseModelMixin

class Page(StreamingBaseModelMixin, BaseModel):
    number: int
    width: float

page = Page.stream_model_validate_json(page_bytes)

for page in Page.stream_model_validate_json_array_iter(array_body):
    ...

async for page in Page.stream_model_validate_json_array_aiter(async_array_body):
    ...

for page in Page.stream_model_validate_jsonl_iter(jsonl_body):
    ...

async for page in Page.stream_model_validate_jsonl_aiter(async_jsonl_body):
    ...
```

`StreamingDataclassMixin` provides the same API for `@pydantic.dataclasses.dataclass` (methods are `stream_validate_*` instead of `stream_model_validate_*`).

### Top-level array vs nested array

Top-level array input works directly:

```json
[ ... ]
```

```python
for page in Page.stream_model_validate_json_array_iter(body):
    ...
```

Document-envelope input uses `root_prefix` to target the nested array:

```json
{ "pages": [ ... ] }
```

```python
async for page in Page.stream_model_validate_json_array_aiter(
    body,
    root_prefix="pages",
):
    ...
```

This is the common “large document object with one dominant nested array” case.
`root_prefix` also supports dotted paths such as `"data.results"`.

When streamed validation fails, the raised `ValidationError` includes the streamed
array item index in its location, and prefixed iterators also include the
`root_prefix` context.

### Advanced: projected-item escape hatch

If you want the projection machinery without model instances, there is also a
narrow public escape hatch:

```python
import json

from pydantic_stream import stream_projected_json_array_iter

for item in stream_projected_json_array_iter(
    body,
    Page._streaming_spec(),
    json.loads,
    root_prefix="pages",
):
    ...
```

This is useful for lightweight dict/TypedDict-style consumers, but the primary
public API remains the mixin methods above.

### Projection-free helpers

For projection-free use cases, the top-level helpers are available too:

```python
from pydantic import TypeAdapter
from pydantic_stream import stream_json_array, stream_json_array_async

adapter = TypeAdapter(Page)
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
