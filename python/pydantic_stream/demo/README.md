# pydantic_stream demos

`pydantic_stream` uses Rust-accelerated JSON projection to validate only the fields you care about — stripping unknown fields before they ever reach Python.

## Demos

| # | Script | What it demonstrates |
|---|--------|---------------------|
| 01 | `01_basics.py` | Single object projection — define 5 fields, feed 30+, get back only yours |
| 02 | `02_stream_array.py` | `StreamArray` lazy access — indexing, slicing, iteration without full parse |
| 03 | `03_s3_streaming.py` | Chunked streaming — process a large JSON array in bounded memory (headline feature) |
| 04 | `04_nested_projection.py` | Nested model stripping + `root_prefix` navigation into wrapped payloads |
| 05 | `05_jsonl_streaming.py` | JSONL line-by-line streaming vs eager list path |
| 06 | `06_dataclass_api.py` | Dataclass mixin parity — same Rust projection, choose your preferred type |

## Running

All demos run from the `pydantic_stream_python/` directory:

```bash
cd sweetspot/pydantic_stream

uv run python pydantic_stream_python/demo/01_basics.py
uv run python pydantic_stream_python/demo/02_stream_array.py
uv run python pydantic_stream_python/demo/03_s3_streaming.py
uv run python pydantic_stream_python/demo/04_nested_projection.py
uv run python pydantic_stream_python/demo/05_jsonl_streaming.py
uv run python pydantic_stream_python/demo/06_dataclass_api.py
```

Or run all at once:

```bash
for f in pydantic_stream_python/demo/0*.py; do echo "---"; uv run python "$f"; echo; done
```
