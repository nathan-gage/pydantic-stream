"""Tests for prefixed sync/async streaming iterators and projected-array APIs."""

from __future__ import annotations

import asyncio
import io
import json
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest
from pydantic_core import ValidationError

from pydantic_stream import (
    StreamingProjectionError,
    stream_projected_json_array_aiter,
    stream_projected_json_array_iter,
)

from .cases import StreamableCase, json_bytes


def _chunk(data: bytes, size: int) -> Iterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


async def _achunk(data: bytes, size: int) -> AsyncIterator[bytes]:
    for i in range(0, len(data), size):
        await asyncio.sleep(0)
        yield data[i : i + size]


async def _achunks(chunks: list[bytes]) -> AsyncIterator[bytes]:
    for chunk in chunks:
        await asyncio.sleep(0)
        yield chunk


class AsyncReader:
    def __init__(self, data: bytes) -> None:
        self._stream = io.BytesIO(data)

    async def read(self, amt: int | None = None) -> bytes:
        await asyncio.sleep(0)
        return self._stream.read(amt)


def _wrapped_payload(records: list[dict[str, Any]]) -> bytes:
    payload = {
        "meta": {"source": "test"},
        "items": records,
        "tail": {"count": len(records)},
    }
    return json_bytes(payload)


def _stream_array_aiter(case: StreamableCase, source: Any, **kwargs: Any) -> AsyncIterator[Any]:
    model_type = case.model_type
    if hasattr(model_type, "stream_model_validate_json_array_aiter"):
        return model_type.stream_model_validate_json_array_aiter(source, **kwargs)
    return model_type.stream_validate_json_array_aiter(source, **kwargs)


async def _collect_async(source: AsyncIterator[Any]) -> list[Any]:
    return [item async for item in source]


class TestPrefixedSyncIterators:
    def test_root_prefix_file_like_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(5)]
        results = list(
            user_case.stream_validate_json_array_iter(
                io.BytesIO(_wrapped_payload(records)),
                root_prefix="items",
                chunk_size=17,
            )
        )
        assert [item.id for item in results] == [record["id"] for record in records]
        assert [item.name for item in results] == [record["name"] for record in records]

    def test_root_prefix_iterable_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(4)]
        results = list(
            user_case.stream_validate_json_array_iter(
                _chunk(_wrapped_payload(records), 11),
                root_prefix="items",
                chunk_size=11,
            )
        )
        assert [item.id for item in results] == [record["id"] for record in records]

    def test_top_level_bytes_source(self, user_case: StreamableCase) -> None:
        records = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        results = list(user_case.stream_validate_json_array_iter(json_bytes(records)))
        assert [item.id for item in results] == [1, 2]

    def test_root_prefix_truncated_array_raises(self, user_case: StreamableCase) -> None:
        data = _wrapped_payload([{"id": 1, "name": "Ada"}]).replace(b'],"tail"', b',"tail"', 1)
        with pytest.raises(StreamingProjectionError):
            list(
                user_case.stream_validate_json_array_iter(
                    io.BytesIO(data),
                    root_prefix="items",
                    chunk_size=9,
                )
            )

    def test_root_prefix_validation_error_includes_item_index_and_prefix(
        self, user_case: StreamableCase
    ) -> None:
        data = _wrapped_payload([{"id": 1, "name": "Ada"}, {"id": "bad", "name": "Grace"}])

        with pytest.raises(ValidationError) as exc_info:
            list(
                user_case.stream_validate_json_array_iter(
                    io.BytesIO(data),
                    root_prefix="items",
                    chunk_size=9,
                )
            )

        error = exc_info.value.errors(include_url=False)[0]
        assert error["loc"][:2] == ("items", 1)
        assert "array item 1 under root_prefix 'items'" in str(exc_info.value)


class TestProjectedArrayIterators:
    def test_sync_root_prefix_projection(self, user_case: StreamableCase) -> None:
        records = [{"id": 1, "name": "Ada", "noise": "drop"}]
        projected = list(
            stream_projected_json_array_iter(
                _chunk(_wrapped_payload(records), 7),
                user_case.compile_spec(),
                json.loads,
                root_prefix="items",
                chunk_size=7,
            )
        )
        assert projected == [{"id": 1, "name": "Ada"}]

    def test_sync_top_level_projection(self, user_case: StreamableCase) -> None:
        records = [{"id": 1, "name": "Ada", "noise": "drop"}]
        projected = list(
            stream_projected_json_array_iter(
                json_bytes(records),
                user_case.compile_spec(),
                json.loads,
            )
        )
        assert projected == [{"id": 1, "name": "Ada"}]

    def test_async_root_prefix_projection(self, user_case: StreamableCase) -> None:
        async def collect() -> list[dict[str, Any]]:
            return [
                item
                async for item in stream_projected_json_array_aiter(
                    _achunk(_wrapped_payload([{"id": 1, "name": "Ada", "noise": "drop"}]), 5),
                    user_case.compile_spec(),
                    json.loads,
                    root_prefix="items",
                    chunk_size=5,
                )
            ]

        assert asyncio.run(collect()) == [{"id": 1, "name": "Ada"}]


class TestPrefixedAsyncIterators:
    def test_root_prefix_async_iterable_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(3)]
        results = asyncio.run(
            _collect_async(
                _stream_array_aiter(
                    user_case,
                    _achunk(_wrapped_payload(records), 13),
                    root_prefix="items",
                    chunk_size=13,
                )
            )
        )
        assert [item.id for item in results] == [record["id"] for record in records]

    def test_root_prefix_async_reader_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(3)]
        results = asyncio.run(
            _collect_async(
                _stream_array_aiter(
                    user_case,
                    AsyncReader(_wrapped_payload(records)),
                    root_prefix="items",
                    chunk_size=10,
                )
            )
        )
        assert [item.name for item in results] == [record["name"] for record in records]

    def test_root_prefix_async_handles_weird_chunk_boundaries(
        self, user_case: StreamableCase
    ) -> None:
        chunks = [
            b'{"it',
            b'ems"',
            b":",
            b"[",
            b'{"id":1,"na',
            b'me":"Ada"},',
            b'{"id":2,"name":"Grace"}',
            b"]",
            b',"tail":{"count":2}}',
        ]

        async def consume() -> list[Any]:
            return [
                item
                async for item in _stream_array_aiter(
                    user_case,
                    _achunks(chunks),
                    root_prefix="items",
                    chunk_size=3,
                )
            ]

        results = asyncio.run(consume())
        assert [item.id for item in results] == [1, 2]

    def test_root_prefix_async_truncated_array_raises(self, user_case: StreamableCase) -> None:
        data = _wrapped_payload([{"id": 1, "name": "Ada"}]).replace(b'],"tail"', b',"tail"', 1)

        async def consume() -> list[Any]:
            return [
                item
                async for item in _stream_array_aiter(
                    user_case,
                    AsyncReader(data),
                    root_prefix="items",
                    chunk_size=8,
                )
            ]

        with pytest.raises(StreamingProjectionError):
            asyncio.run(consume())

    def test_root_prefix_async_validation_error_includes_item_index_and_prefix(
        self, user_case: StreamableCase
    ) -> None:
        data = _wrapped_payload([{"id": 1, "name": "Ada"}, {"id": "bad", "name": "Grace"}])

        async def consume() -> list[Any]:
            return [
                item
                async for item in _stream_array_aiter(
                    user_case,
                    AsyncReader(data),
                    root_prefix="items",
                    chunk_size=8,
                )
            ]

        with pytest.raises(ValidationError) as exc_info:
            asyncio.run(consume())

        error = exc_info.value.errors(include_url=False)[0]
        assert error["loc"][:2] == ("items", 1)
        assert "array item 1 under root_prefix 'items'" in str(exc_info.value)
