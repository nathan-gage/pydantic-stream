"""Streaming JSON array parsing and projection helpers."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, TypeVar

from pydantic import TypeAdapter

from ._native import (
    ObjectSpec,
    StreamingProjectionError,
    extract_array_items,
    locate_array_start,
    project_array_items_partial,
)

T = TypeVar("T")


def _coerce_bytes(value: Any, *, description: str) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bytearray):
        return bytes(value)
    if isinstance(value, memoryview):
        return value.tobytes()
    raise StreamingProjectionError(f"Unsupported {description} type: {type(value).__name__!r}")


def _source_to_chunks(source: Any, chunk_size: int) -> Iterator[bytes]:
    """Normalize any sync source into an iterator of byte chunks."""
    if isinstance(source, (bytes, str, bytearray, memoryview)):
        return iter((_coerce_bytes(source, description="source"),))

    if callable(source):
        return _source_to_chunks(source(), chunk_size)

    if hasattr(source, "read"):

        def _reader() -> Iterator[bytes]:
            while True:
                chunk = source.read(chunk_size)
                if not chunk:
                    return
                yield _coerce_bytes(chunk, description="chunk")

        return _reader()

    try:
        iterator = iter(source)
    except TypeError as exc:
        raise StreamingProjectionError(
            f"Unsupported source type: {type(source).__name__!r}"
        ) from exc

    def _iterable() -> Iterator[bytes]:
        for chunk in iterator:
            if not chunk:
                continue
            yield _coerce_bytes(chunk, description="chunk")

    return _iterable()


async def _source_to_achunks(source: Any, chunk_size: int) -> AsyncIterator[bytes]:
    """Normalize any sync/async source into an async iterator of byte chunks."""
    if isinstance(source, (bytes, str, bytearray, memoryview)):
        yield _coerce_bytes(source, description="source")
        return

    if callable(source):
        resolved = source()
        if inspect.isawaitable(resolved):
            resolved = await resolved
        async for chunk in _source_to_achunks(resolved, chunk_size):
            yield chunk
        return

    if hasattr(source, "__aiter__"):
        async for chunk in source:
            if not chunk:
                continue
            yield _coerce_bytes(chunk, description="chunk")
        return

    if hasattr(source, "read"):
        while True:
            chunk = source.read(chunk_size)
            if inspect.isawaitable(chunk):
                chunk = await chunk
            if not chunk:
                return
            yield _coerce_bytes(chunk, description="chunk")

    else:
        for chunk in _source_to_chunks(source, chunk_size):
            yield chunk


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


def _root_prefix_eof_error(root_prefix: str) -> StreamingProjectionError:
    return StreamingProjectionError(
        f"Unexpected end before JSON array at root_prefix {root_prefix!r}"
    )


def _stream_projected_json_array_item_bytes_iter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[bytes]:
    """Yield projected item bytes from a top-level or prefixed JSON array."""
    chunks = _source_to_chunks(source, chunk_size)
    buffer = bytearray()
    is_start = True
    saw_input = False
    array_started = root_prefix is None

    for chunk in chunks:
        if not chunk:
            continue
        saw_input = True
        buffer.extend(chunk)

        if not array_started:
            array_start = locate_array_start(bytes(buffer), root_prefix)
            if array_start is None:
                continue
            del buffer[:array_start]
            array_started = True
            is_start = True

        items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
        for item_bytes in items:
            yield item_bytes
        del buffer[:consumed]
        is_start = False

        if finished:
            if root_prefix is not None:
                return
            if _has_trailing_array_content(bytes(buffer), chunks):
                raise StreamingProjectionError("Trailing content after JSON array")
            return

    if not saw_input and not buffer:
        return

    if not array_started:
        if buffer and bytes(buffer).strip():
            raise _root_prefix_eof_error(root_prefix or "")
        return

    if saw_input and not buffer:
        raise StreamingProjectionError("Unexpected end of JSON array")

    if buffer:
        items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
        for item_bytes in items:
            yield item_bytes
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if root_prefix is None and bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


async def _stream_projected_json_array_item_bytes_aiter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[bytes]:
    """Async variant of _stream_projected_json_array_item_bytes_iter."""
    chunks = _source_to_achunks(source, chunk_size)
    buffer = bytearray()
    is_start = True
    saw_input = False
    array_started = root_prefix is None

    async for chunk in chunks:
        if not chunk:
            continue
        saw_input = True
        buffer.extend(chunk)

        if not array_started:
            array_start = locate_array_start(bytes(buffer), root_prefix)
            if array_start is None:
                continue
            del buffer[:array_start]
            array_started = True
            is_start = True

        items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
        for item_bytes in items:
            yield item_bytes
        del buffer[:consumed]
        is_start = False

        if finished:
            if root_prefix is not None:
                return
            if await _has_trailing_array_content_async(bytes(buffer), chunks):
                raise StreamingProjectionError("Trailing content after JSON array")
            return

    if not saw_input and not buffer:
        return

    if not array_started:
        if buffer and bytes(buffer).strip():
            raise _root_prefix_eof_error(root_prefix or "")
        return

    if saw_input and not buffer:
        raise StreamingProjectionError("Unexpected end of JSON array")

    if buffer:
        items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
        for item_bytes in items:
            yield item_bytes
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if root_prefix is None and bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


def stream_projected_json_array_iter(
    source: Any,
    spec: ObjectSpec,
    validator: Callable[[bytes], T],
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[T]:
    """Stream projected JSON array items through a caller-supplied validator."""
    for item_bytes in _stream_projected_json_array_item_bytes_iter(
        source,
        spec,
        root_prefix=root_prefix,
        chunk_size=chunk_size,
    ):
        yield validator(item_bytes)


async def stream_projected_json_array_aiter(
    source: Any,
    spec: ObjectSpec,
    validator: Callable[[bytes], T],
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[T]:
    """Async stream projected JSON array items through a validator."""
    async for item_bytes in _stream_projected_json_array_item_bytes_aiter(
        source,
        spec,
        root_prefix=root_prefix,
        chunk_size=chunk_size,
    ):
        yield validator(item_bytes)


def stream_json_array(
    source: Any,
    adapter: TypeAdapter[T],
    *,
    chunk_size: int = 1_048_576,
) -> Iterator[T]:
    """Stream-validate a JSON array from a byte source in bounded memory.

    Reads *source* in chunks, finds complete JSON items via Rust,
    and yields validated instances. Peak memory is bounded by
    *chunk_size* (plus one incomplete item), not by total input size.

    This function does NOT perform projection — items are validated as-is.
    For projection + streaming, use
    ``stream_projected_json_array_iter(...)`` or the streamable mixin helpers.

    Args:
        source: A file-like object with .read(), bytes/str, or an iterable of chunks.
        adapter: A pydantic TypeAdapter for the item type.
        chunk_size: Bytes to read per chunk (default 1MB).

    Yields:
        Validated instances of the adapter's type.
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


__all__ = [
    "stream_json_array",
    "stream_projected_json_array_iter",
    "stream_projected_json_array_aiter",
]
