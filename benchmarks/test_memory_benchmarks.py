"""Pytest-benchmark timing benchmarks for the full-array parsing cases.

Run the default suite with:

    pytest benchmarks/test_memory_benchmarks.py -m "not large_payload" --benchmark-enable

Use native pytest selection for heavier cases and shapes:

    pytest benchmarks/test_memory_benchmarks.py -m large_payload --benchmark-enable
    pytest benchmarks/test_memory_benchmarks.py -k wide --benchmark-enable
    pytest benchmarks/test_memory_benchmarks.py -k "deep and stream_basemodel" --benchmark-enable
"""

from __future__ import annotations

import pytest

from ._parse_cases import (
    LARGE_BENCHMARK_ROUNDS,
    SHAPES,
    expected_count,
    get_large_payload,
    get_payload,
    parse_pydantic_basemodel,
    parse_pydantic_dataclass,
    parse_pydantic_dataclass_slots,
    parse_stdlib_slots,
    parse_stream_basemodel,
    parse_stream_dataclass,
    parse_stream_dataclass_slots,
)
from .conftest import metadata


def _record_payload_size(shape: str, data: bytes) -> None:
    metadata.setdefault(f"payload_size_{shape}", len(data))


def _run_standard_benchmark(benchmark, shape: str, parse_fn) -> None:
    data = get_payload(shape)
    _record_payload_size(shape, data)
    benchmark.group = f"parse-{shape}"
    result = benchmark(lambda d=data: parse_fn(d))
    assert len(result) == expected_count(shape, large=False)


def _run_large_benchmark(benchmark, shape: str, parse_fn) -> None:
    data = get_large_payload(shape)
    _record_payload_size(shape, data)
    benchmark.group = f"large-parse-{shape}"
    result = benchmark.pedantic(
        lambda d=data: parse_fn(d), rounds=LARGE_BENCHMARK_ROUNDS, warmup_rounds=0
    )
    assert len(result) == expected_count(shape, large=True)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_stream_basemodel(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_stream_basemodel)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_stream_dataclass(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_stream_dataclass)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_stream_dataclass_slots(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_stream_dataclass_slots)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_pydantic_basemodel(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_pydantic_basemodel)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_pydantic_dataclass(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_pydantic_dataclass)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_pydantic_dataclass_slots(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_pydantic_dataclass_slots)


@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
@pytest.mark.benchmark(group="parse")
def test_stdlib_slots(benchmark, shape: str) -> None:
    _run_standard_benchmark(benchmark, shape, parse_stdlib_slots)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_basemodel(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_stream_basemodel)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_dataclass(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_stream_dataclass)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_basemodel(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_pydantic_basemodel)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stream_dataclass_slots(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_stream_dataclass_slots)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_dataclass(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_pydantic_dataclass)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_pydantic_dataclass_slots(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_pydantic_dataclass_slots)


@pytest.mark.large_payload
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_stdlib_slots(benchmark, shape: str) -> None:
    _run_large_benchmark(benchmark, shape, parse_stdlib_slots)
