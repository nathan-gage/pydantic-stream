"""Benchmarks for array slice operations: StreamArray slicing vs pydantic full-parse-then-slice.

Compares the cost of extracting a small slice (10 items) from a 500-item JSON array
using StreamArray's skip-based indexing vs parsing the entire array with pydantic
and slicing the resulting list.  All payload shapes are tested.
"""

import io
import random

import pytest

from ._data_gen import SHAPES, PayloadShape, make_shaped_payload_bytes
from ._models import (
    BenchUser_StreamDCSlots,
    BenchUser_StreamModel,
    pydantic_dc_slots_list_adapter,
    pydantic_model_list_adapter,
)

BENCH_N = 500
SLICE_START = 245
SLICE_STOP = 255

# Cache generated payloads across tests within the same process.
_payload_cache: dict[PayloadShape, bytes] = {}


def _get_payload(shape: PayloadShape) -> bytes:
    if shape not in _payload_cache:
        random.seed(42)
        _payload_cache[shape] = make_shaped_payload_bytes(shape, n=BENCH_N)
    return _payload_cache[shape]


# ---------------------------------------------------------------------------
# StreamArray slice (Rust skip-based — only projects matched items)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="slice")
def test_slice_stream_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"slice-{shape}"

    def fn() -> list:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(data))
        return sa[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="slice")
def test_slice_stream_dataclass_slots(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"slice-{shape}"

    def fn() -> list:
        sa = BenchUser_StreamDCSlots.stream_validate_json_array(io.BytesIO(data))
        return sa[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


# ---------------------------------------------------------------------------
# Pydantic full-parse then list slice (baseline)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="slice")
def test_slice_pydantic_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"slice-{shape}"

    def fn() -> list:
        all_items = pydantic_model_list_adapter.validate_json(data)
        return all_items[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="slice")
def test_slice_pydantic_dataclass_slots(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"slice-{shape}"

    def fn() -> list:
        all_items = pydantic_dc_slots_list_adapter.validate_json(data)
        return all_items[SLICE_START:SLICE_STOP]

    result = benchmark(fn)
    assert len(result) == SLICE_STOP - SLICE_START


# ---------------------------------------------------------------------------
# StreamArray single-item indexing vs pydantic full-parse
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="index")
def test_index_stream_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"index-{shape}"

    def fn() -> object:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(data))
        return sa[SLICE_START]

    result = benchmark(fn)
    assert result is not None


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="index")
def test_index_pydantic_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"index-{shape}"

    def fn() -> object:
        all_items = pydantic_model_list_adapter.validate_json(data)
        return all_items[SLICE_START]

    result = benchmark(fn)
    assert result is not None


# ---------------------------------------------------------------------------
# StreamArray to_list() vs pydantic full-parse (both parse everything)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="to_list")
def test_to_list_stream_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"to_list-{shape}"

    def fn() -> list:
        sa = BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(data))
        return sa.to_list()

    result = benchmark(fn)
    assert len(result) == BENCH_N


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="to_list")
def test_to_list_pydantic_basemodel(benchmark, shape: PayloadShape) -> None:
    data = _get_payload(shape)
    benchmark.group = f"to_list-{shape}"

    def fn() -> list:
        return pydantic_model_list_adapter.validate_json(data)

    result = benchmark(fn)
    assert len(result) == BENCH_N
