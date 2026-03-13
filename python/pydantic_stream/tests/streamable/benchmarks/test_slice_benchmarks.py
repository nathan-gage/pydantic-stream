"""Benchmarks for array slice operations: StreamArray slicing vs pydantic full-parse-then-slice.

Compares the cost of extracting a small slice (10 items) from a 500-item JSON array
using StreamArray's skip-based indexing vs parsing the entire array with pydantic
and slicing the resulting list.
"""

import io
import random

import pytest

from ._data_gen import make_benchmark_payload_bytes
from ._models import (
    BenchUser_StreamDCSlots,
    BenchUser_StreamModel,
    pydantic_dc_slots_list_adapter,
    pydantic_model_list_adapter,
)

BENCH_N = 500
SLICE_START = 245
SLICE_STOP = 255


@pytest.fixture(scope="module")
def slice_source_bytes() -> bytes:
    random.seed(42)
    return make_benchmark_payload_bytes(BENCH_N)


# ---------------------------------------------------------------------------
# StreamArray slice (Rust skip-based — only projects matched items)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="slice")
def test_slice_stream_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(slice_source_bytes))
        return sa[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


@pytest.mark.benchmark(group="slice")
def test_slice_stream_dataclass_slots(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        sa = BenchUser_StreamDCSlots.stream_validate_json_array(io.BytesIO(slice_source_bytes))
        return sa[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


# ---------------------------------------------------------------------------
# Pydantic full-parse then list slice (baseline)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="slice")
def test_slice_pydantic_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        all_items = pydantic_model_list_adapter.validate_json(slice_source_bytes)
        return all_items[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


@pytest.mark.benchmark(group="slice")
def test_slice_pydantic_dataclass_slots(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        all_items = pydantic_dc_slots_list_adapter.validate_json(slice_source_bytes)
        return all_items[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


# ---------------------------------------------------------------------------
# StreamArray single-item indexing vs pydantic full-parse
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="index")
def test_index_stream_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> object:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(slice_source_bytes))
        return sa[SLICE_START]

    result = benchmark(fn)
    assert result is not None


@pytest.mark.benchmark(group="index")
def test_index_pydantic_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> object:
        all_items = pydantic_model_list_adapter.validate_json(slice_source_bytes)
        return all_items[SLICE_START]

    result = benchmark(fn)
    assert result is not None


# ---------------------------------------------------------------------------
# StreamArray to_list() vs pydantic full-parse (both parse everything)
# ---------------------------------------------------------------------------


@pytest.mark.benchmark(group="to_list")
def test_to_list_stream_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(slice_source_bytes))
        return sa.to_list()

    result = benchmark(fn)
    assert len(result) == BENCH_N


@pytest.mark.benchmark(group="to_list")
def test_to_list_pydantic_basemodel(benchmark, slice_source_bytes: bytes) -> None:
    def fn() -> list:
        return pydantic_model_list_adapter.validate_json(slice_source_bytes)

    result = benchmark(fn)
    assert len(result) == BENCH_N
