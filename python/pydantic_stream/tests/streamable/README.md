# Streamable Test Suite

Tests for `pydantic_stream` streaming projection and validation mixins.

```bash
just pytest sweetspot/pydantic_stream/pydantic_stream_python tests/streamable/       # run the full suite
just pytest sweetspot/pydantic_stream/pydantic_stream_python tests/streamable/ -k foo # run tests matching "foo"
```

## Test files

| File | What it covers |
|------|---------------|
| `test_error_paths.py` | Error paths, edge cases, Rust projection, caching, source types, Unicode, number handling, model/field validators, extra fields config |
| `test_streamable_basemodel.py` | Integration tests for `StreamingBaseModelMixin`: rich field types, frozen models, validators, discriminated unions, deep nesting, empty inputs |
| `test_streamable_dataclass.py` | Integration tests for `StreamingDataclassMixin`: mirrors the BaseModel tests for pydantic dataclasses |
| `test_streamable_properties.py` | Hypothesis property-based tests: streaming-vs-direct parity, CRLF invariance, source format invariance, large integers, Unicode keys |
| `test_harness_smoke.py` | Smoke tests exercising the shared harness fixtures across all model families |

## Shared test infrastructure

| File | Purpose |
|------|---------|
| `conftest.py` | Parameterized fixtures that run assertions against both dataclass and BaseModel variants. Hypothesis profile config. |
| `cases.py` | Canonical streamable model families plus `StreamableCase`, which wraps the streaming and direct-validation entrypoints. |
| `helpers.py` | Source-shape builders (`SourceFactory`). |
| `assertions.py` | Parity helpers for single-object, JSON array, and JSONL flows. |
| `strategies.py` | Hypothesis strategies for valid payloads and noisy JSON branches. |
