"""Memray-backed memory benchmarks for the full-array parsing cases.

This suite reuses pytest-benchmark's save/compare/storage/histogram options:

    pytest benchmarks/test_memory_profiles.py -m "not large_payload" --benchmark-save baseline
    pytest benchmarks/test_memory_profiles.py -m "not large_payload" --benchmark-compare
    pytest benchmarks/test_memory_profiles.py -m "not large_payload" --benchmark-histogram

The saved memory artifacts live under ``<benchmark-storage>/memory`` so they
do not collide with pytest-benchmark's timing artifacts.
"""

from __future__ import annotations

import pytest

from ._parse_cases import (
    MEMORY_PARSE_CASES,
    SHAPES,
    expected_count,
    get_large_payload,
    get_payload,
)
from .conftest import MEMORY_ROUNDS, measure_memory, metadata


def _record_payload_size(shape: str, data: bytes) -> None:
    metadata.setdefault(f"payload_size_{shape}", len(data))


def _run_memory_case(
    *,
    case_name: str,
    shape: str,
    data: bytes,
    parse_fn,
    rounds: int,
    large: bool,
) -> None:
    _record_payload_size(shape, data)
    result = parse_fn(data)
    assert len(result) == expected_count(shape, large=large)
    measure_memory(
        f"{case_name}-{shape}",
        lambda d=data: parse_fn(d),
        rounds=rounds,
        group=("large-parse-" if large else "parse-") + shape,
    )


@pytest.mark.parametrize("case", MEMORY_PARSE_CASES, ids=lambda case: case.id.replace("-", "_"))
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_parse_memory(case, shape: str) -> None:
    data = get_payload(shape)
    _run_memory_case(
        case_name=case.id,
        shape=shape,
        data=data,
        parse_fn=case.parse,
        rounds=MEMORY_ROUNDS,
        large=False,
    )


@pytest.mark.large_payload
@pytest.mark.parametrize("case", MEMORY_PARSE_CASES, ids=lambda case: case.id.replace("-", "_"))
@pytest.mark.parametrize("shape", SHAPES, ids=SHAPES)
def test_large_parse_memory(case, shape: str) -> None:
    data = get_large_payload(shape)
    _run_memory_case(
        case_name=case.id,
        shape=shape,
        data=data,
        parse_fn=case.parse,
        rounds=1,
        large=True,
    )
