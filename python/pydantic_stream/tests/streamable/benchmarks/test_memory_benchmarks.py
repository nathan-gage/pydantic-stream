"""Pytest-benchmark timing + memray peak memory for seven parsing approaches.

Timing: pytest-benchmark (statistical, multi-round).
Memory: memray (all allocations including C/Rust), printed as a comparison table.

Large-payload tests (``--large-payload``)
-----------------------------------------
Run with ``--large-payload`` to enable ~100 MB benchmarks across multiple
payload shapes.  Use ``--payload-shape`` to restrict to specific shapes::

    pytest --large-payload                            # all shapes
    pytest --large-payload --payload-shape wide       # just "wide"
    pytest --large-payload --payload-shape deep --payload-shape string-heavy
"""

import io
import json
import random

import pytest

from ._data_gen import (
    LARGE_RECORD_COUNTS,
    SHAPES,
    PayloadShape,
    make_benchmark_payload_bytes,
    make_shaped_payload_bytes,
)
from ._models import (
    BenchAddress_StdlibDC,
    BenchUser_StdlibDC,
    BenchUser_StreamDC,
    BenchUser_StreamDCSlots,
    BenchUser_StreamModel,
    pydantic_dc_list_adapter,
    pydantic_dc_slots_list_adapter,
    pydantic_model_list_adapter,
)
from .conftest import measure_memory, metadata

BENCH_N = 500

_skip_memory: bool | None = None


def _memory_disabled(request: pytest.FixtureRequest) -> bool:
    global _skip_memory
    if _skip_memory is None:
        _skip_memory = request.config.getoption("--no-memory", default=False)
    return _skip_memory


# ---------------------------------------------------------------------------
# Standard benchmarks (~13 MB, always run)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def source_bytes() -> bytes:
    random.seed(42)
    data = make_benchmark_payload_bytes(BENCH_N)
    metadata["payload_size"] = len(data)
    return data


@pytest.mark.benchmark(group="parse")
def test_stream_basemodel(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return list(BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(source_bytes)))

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("stream-basemodel", fn)


@pytest.mark.benchmark(group="parse")
def test_stream_dataclass(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return list(BenchUser_StreamDC.stream_validate_json_array(io.BytesIO(source_bytes)))

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("stream-dataclass", fn)


@pytest.mark.benchmark(group="parse")
def test_stream_dataclass_slots(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return list(BenchUser_StreamDCSlots.stream_validate_json_array(io.BytesIO(source_bytes)))

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("stream-dataclass-slots", fn)


@pytest.mark.benchmark(group="parse")
def test_pydantic_basemodel(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return pydantic_model_list_adapter.validate_json(source_bytes)

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("pydantic-basemodel", fn)


@pytest.mark.benchmark(group="parse")
def test_pydantic_dataclass(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return pydantic_dc_list_adapter.validate_json(source_bytes)

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("pydantic-dataclass", fn)


@pytest.mark.benchmark(group="parse")
def test_pydantic_dataclass_slots(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def fn() -> list:
        return pydantic_dc_slots_list_adapter.validate_json(source_bytes)

    result = benchmark(fn)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("pydantic-dataclass-slots", fn)


@pytest.mark.benchmark(group="parse")
def test_stdlib_slots(benchmark, source_bytes: bytes, request: pytest.FixtureRequest) -> None:
    def parse() -> list[BenchUser_StdlibDC]:
        raw = json.loads(source_bytes)
        return [
            BenchUser_StdlibDC(
                id=r["id"],
                name=r["name"],
                score=r["score"],
                active=r["active"],
                address=BenchAddress_StdlibDC(
                    city=r["address"]["city"],
                    country=r["address"]["country"],
                ),
                tags=r["tags"],
            )
            for r in raw
        ]

    result = benchmark(parse)
    assert len(result) == BENCH_N
    if not _memory_disabled(request):
        measure_memory("stdlib-slots", parse)


# ---------------------------------------------------------------------------
# Large-payload benchmarks (~100 MB, gated by --large-payload)
# ---------------------------------------------------------------------------


# Cache generated payloads across tests within the same process.
_large_payload_cache: dict[PayloadShape, bytes] = {}


def _get_large_payload(shape: PayloadShape) -> bytes:
    if shape not in _large_payload_cache:
        random.seed(42)
        _large_payload_cache[shape] = make_shaped_payload_bytes(shape)
    return _large_payload_cache[shape]


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_basemodel(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return list(BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(d)))

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    metadata[f"payload_size_{shape}"] = len(data)
    if not _memory_disabled(request):
        measure_memory(f"large-stream-basemodel-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_dataclass(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return list(BenchUser_StreamDC.stream_validate_json_array(io.BytesIO(d)))

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-stream-dataclass-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_basemodel(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return pydantic_model_list_adapter.validate_json(d)

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-pydantic-basemodel-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_dataclass_slots(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return list(BenchUser_StreamDCSlots.stream_validate_json_array(io.BytesIO(d)))

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-stream-dataclass-slots-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_dataclass(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return pydantic_dc_list_adapter.validate_json(d)

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-pydantic-dataclass-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_dataclass_slots(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def fn(d: bytes = data) -> list:
        return pydantic_dc_slots_list_adapter.validate_json(d)

    result = benchmark.pedantic(fn, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-pydantic-dataclass-slots-{shape}", fn, rounds=1, group=f"large-parse-{shape}")


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stdlib_slots(benchmark, shape: PayloadShape, request: pytest.FixtureRequest) -> None:
    benchmark.group = f"large-parse-{shape}"
    data = _get_large_payload(shape)
    expected = LARGE_RECORD_COUNTS[shape]

    def parse(d: bytes = data) -> list[BenchUser_StdlibDC]:
        raw = json.loads(d)
        return [
            BenchUser_StdlibDC(
                id=r["id"],
                name=r["name"],
                score=r["score"],
                active=r["active"],
                address=BenchAddress_StdlibDC(
                    city=r["address"]["city"],
                    country=r["address"]["country"],
                ),
                tags=r["tags"],
            )
            for r in raw
        ]

    result = benchmark.pedantic(parse, rounds=3, warmup_rounds=0)
    assert len(result) == expected
    if not _memory_disabled(request):
        measure_memory(f"large-stdlib-slots-{shape}", parse, rounds=1, group=f"large-parse-{shape}")
