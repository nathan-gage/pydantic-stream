"""Streaming JSON array helpers."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterator
from typing import Any, TypeVar

from pydantic import TypeAdapter

from ._native import StreamingProjectionError, extract_array_items

T = TypeVar("T")


def _coerce_chunk_bytes(chunk: Any) -> bytes:
    if isinstance(chunk, bytes):
        return chunk
    if isinstance(chunk, str):
        return chunk.encode("utf-8")
    if isinstance(chunk, (bytearray, memoryview)):
        return bytes(chunk)
    raise StreamingProjectionError(f"Unsupported chunk type: {type(chunk).__name__!r}")


def _source_to_chunks(source: Any, chunk_size: int) -> Iterator[bytes]:
    """Normalize a synchronous source into an iterator of byte chunks."""
    if isinstance(source, (bytes, str, bytearray, memoryview)):
        return iter((_coerce_chunk_bytes(source),))
    if hasattr(source, "read"):
        return iter(lambda: _coerce_chunk_bytes(source.read(chunk_size)), b"")
    return (_coerce_chunk_bytes(chunk) for chunk in source)


async def _async_source_to_chunks(source: Any, chunk_size: int) -> AsyncIterator[bytes]:
    """Normalize async or sync sources into an async iterator of byte chunks."""
    if isinstance(source, (bytes, str, bytearray, memoryview)):
        yield _coerce_chunk_bytes(source)
        return

    read = getattr(source, "read", None)
    if callable(read):
        while True:
            chunk = read(chunk_size)
            if inspect.isawaitable(chunk):
                chunk = await chunk
            if not chunk:
                return
            yield _coerce_chunk_bytes(chunk)

    elif hasattr(source, "__aiter__"):
        async for chunk in source:
            if chunk:
                yield _coerce_chunk_bytes(chunk)

    else:
        for chunk in source:
            if chunk:
                yield _coerce_chunk_bytes(chunk)


def _has_trailing_array_content(remainder: bytes, chunks: Iterator[bytes]) -> bool:
    if remainder.strip():
        return True
    for chunk in chunks:
        if chunk and chunk.strip():
            return True
    return False


async def _has_trailing_array_content_async(remainder: bytes, chunks: AsyncIterator[bytes]) -> bool:
    if remainder.strip():
        return True
    async for chunk in chunks:
        if chunk and chunk.strip():
            return True
    return False


def _normalize_jsonl_line(line: bytes) -> bytes:
    return line[:-1] if line.endswith(b"\r") else line


def _iter_jsonl_lines_from_chunks(chunks: Iterator[bytes]) -> Iterator[tuple[int, bytes]]:
    """Split a synchronous byte-chunk stream into numbered JSONL lines."""
    buffer = bytearray()
    line_number = 0

    for chunk in chunks:
        if not chunk:
            continue
        buffer.extend(chunk)
        while True:
            try:
                newline_index = buffer.index(b"\n")
            except ValueError:
                break
            line = bytes(buffer[:newline_index])
            del buffer[: newline_index + 1]
            line_number += 1
            yield line_number, _normalize_jsonl_line(line)

    if buffer:
        line_number += 1
        yield line_number, _normalize_jsonl_line(bytes(buffer))


async def _aiter_jsonl_lines_from_chunks(
    chunks: AsyncIterator[bytes],
) -> AsyncIterator[tuple[int, bytes]]:
    """Split an async byte-chunk stream into numbered JSONL lines."""
    buffer = bytearray()
    line_number = 0

    async for chunk in chunks:
        if not chunk:
            continue
        buffer.extend(chunk)
        while True:
            try:
                newline_index = buffer.index(b"\n")
            except ValueError:
                break
            line = bytes(buffer[:newline_index])
            del buffer[: newline_index + 1]
            line_number += 1
            yield line_number, _normalize_jsonl_line(line)

    if buffer:
        line_number += 1
        yield line_number, _normalize_jsonl_line(bytes(buffer))


def stream_json_array(
    source: Any,
    adapter: TypeAdapter[T],
    *,
    chunk_size: int = 1_048_576,
) -> Iterator[T]:
    """Iterate over validated items from a JSON array, reading incrementally.

    Args:
        source: Bytes, str, file-like with ``.read()``, or an iterable of chunks.
        adapter: ``TypeAdapter`` for the item type.
        chunk_size: Bytes per read from file-like sources.
    """
    chunks = _source_to_chunks(source, chunk_size)

    buffer = bytearray()
    is_start = True
    saw_input = False

    for chunk in chunks:
        if not chunk:
            continue
        saw_input = True
        buffer.extend(chunk)
        items, consumed, finished = extract_array_items(bytes(buffer), is_start)
        for item_bytes in items:
            yield adapter.validate_json(item_bytes)
        del buffer[:consumed]
        is_start = False
        if finished:
            if _has_trailing_array_content(bytes(buffer), chunks):
                raise StreamingProjectionError("Trailing content after JSON array")
            return

    if saw_input and not buffer:
        raise StreamingProjectionError("Unexpected end of JSON array")

    if buffer:
        items, consumed, finished = extract_array_items(bytes(buffer), is_start)
        for item_bytes in items:
            yield adapter.validate_json(item_bytes)
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


async def stream_json_array_async(
    source: Any,
    adapter: TypeAdapter[T],
    *,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[T]:
    """Like :func:`stream_json_array` but accepts async sources."""
    chunks = _async_source_to_chunks(source, chunk_size)

    buffer = bytearray()
    is_start = True
    saw_input = False

    async for chunk in chunks:
        if not chunk:
            continue
        saw_input = True
        buffer.extend(chunk)
        items, consumed, finished = extract_array_items(bytes(buffer), is_start)
        for item_bytes in items:
            yield adapter.validate_json(item_bytes)
        del buffer[:consumed]
        is_start = False
        if finished:
            if await _has_trailing_array_content_async(bytes(buffer), chunks):
                raise StreamingProjectionError("Trailing content after JSON array")
            return

    if saw_input and not buffer:
        raise StreamingProjectionError("Unexpected end of JSON array")

    if buffer:
        items, consumed, finished = extract_array_items(bytes(buffer), is_start)
        for item_bytes in items:
            yield adapter.validate_json(item_bytes)
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


__all__ = ["stream_json_array", "stream_json_array_async"]
