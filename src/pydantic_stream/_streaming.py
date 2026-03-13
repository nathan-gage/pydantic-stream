"""Streaming JSON array parsing — projection-free buffer management.

This module contains the core streaming logic that could become
TypeAdapter.validate_json_stream() in pydantic. It handles:
- Chunked reading from any byte source
- Partial JSON array parsing via Rust
- Yielding complete item bytes for validation

No ObjectSpec or projection is involved — items are yielded as raw bytes.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, TypeVar

from pydantic import TypeAdapter

from ._native import StreamingProjectionError, extract_array_items

T = TypeVar("T")


def _source_to_chunks(source: Any, chunk_size: int) -> Iterator[bytes]:
    """Normalize any source into an iterator of byte chunks."""
    if hasattr(source, "read"):
        return iter(lambda: source.read(chunk_size), b"")
    return iter(source)


def _has_trailing_array_content(remainder: bytes, chunks: Iterator[bytes]) -> bool:
    if remainder.strip():
        return True
    for chunk in chunks:
        if chunk and bytes(chunk).strip():
            return True
    return False


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
    For projection + streaming, use StreamingBaseModelMixin.stream_model_validate_json_array_iter.

    Args:
        source: A file-like object with .read(), or an iterable of bytes chunks.
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

    # Drain remaining buffer
    if buffer:
        items, consumed, finished = extract_array_items(bytes(buffer), is_start)
        for item_bytes in items:
            yield adapter.validate_json(item_bytes)
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")
