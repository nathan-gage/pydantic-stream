from __future__ import annotations

import io
import json
import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from ._data_gen import LARGE_RECORD_COUNTS, SHAPES, PayloadShape, make_shaped_payload_bytes
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

BENCH_N = 500
LARGE_BENCHMARK_ROUNDS = 3


@dataclass(frozen=True)
class ParseCase:
    id: str
    parse: Callable[[bytes], list[Any]]


_payload_cache: dict[PayloadShape, bytes] = {}
_large_payload_cache: dict[PayloadShape, bytes] = {}


def get_payload(shape: PayloadShape) -> bytes:
    if shape not in _payload_cache:
        random.seed(42)
        _payload_cache[shape] = make_shaped_payload_bytes(shape, n=BENCH_N)
    return _payload_cache[shape]


def get_large_payload(shape: PayloadShape) -> bytes:
    if shape not in _large_payload_cache:
        random.seed(42)
        _large_payload_cache[shape] = make_shaped_payload_bytes(shape)
    return _large_payload_cache[shape]


def expected_count(shape: PayloadShape, *, large: bool) -> int:
    return LARGE_RECORD_COUNTS[shape] if large else BENCH_N


def parse_stream_basemodel(data: bytes) -> list[Any]:
    return list(BenchUser_StreamModel.stream_model_validate_json_array(io.BytesIO(data)))


def parse_stream_dataclass(data: bytes) -> list[Any]:
    return list(BenchUser_StreamDC.stream_validate_json_array(io.BytesIO(data)))


def parse_stream_dataclass_slots(data: bytes) -> list[Any]:
    return list(BenchUser_StreamDCSlots.stream_validate_json_array(io.BytesIO(data)))


def parse_pydantic_basemodel(data: bytes) -> list[Any]:
    return pydantic_model_list_adapter.validate_json(data)


def parse_pydantic_dataclass(data: bytes) -> list[Any]:
    return pydantic_dc_list_adapter.validate_json(data)


def parse_pydantic_dataclass_slots(data: bytes) -> list[Any]:
    return pydantic_dc_slots_list_adapter.validate_json(data)


def parse_stdlib_slots(data: bytes) -> list[BenchUser_StdlibDC]:
    raw = json.loads(data)
    return [
        BenchUser_StdlibDC(
            id=row["id"],
            name=row["name"],
            score=row["score"],
            active=row["active"],
            address=BenchAddress_StdlibDC(
                city=row["address"]["city"],
                country=row["address"]["country"],
            ),
            tags=row["tags"],
        )
        for row in raw
    ]


MEMORY_PARSE_CASES: tuple[ParseCase, ...] = (
    ParseCase("stream-basemodel", parse_stream_basemodel),
    ParseCase("stream-dataclass", parse_stream_dataclass),
    ParseCase("stream-dataclass-slots", parse_stream_dataclass_slots),
    ParseCase("pydantic-basemodel", parse_pydantic_basemodel),
    ParseCase("pydantic-dataclass", parse_pydantic_dataclass),
    ParseCase("pydantic-dataclass-slots", parse_pydantic_dataclass_slots),
    ParseCase("stdlib-slots", parse_stdlib_slots),
)


__all__ = [
    "BENCH_N",
    "LARGE_BENCHMARK_ROUNDS",
    "MEMORY_PARSE_CASES",
    "ParseCase",
    "SHAPES",
    "expected_count",
    "get_large_payload",
    "get_payload",
    "parse_pydantic_basemodel",
    "parse_pydantic_dataclass",
    "parse_pydantic_dataclass_slots",
    "parse_stdlib_slots",
    "parse_stream_basemodel",
    "parse_stream_dataclass",
    "parse_stream_dataclass_slots",
]
