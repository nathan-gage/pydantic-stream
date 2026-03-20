"""Public streaming helpers used by the mixins and advanced callers."""

from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any, TypeVar

from pydantic import TypeAdapter
from pydantic_core import ValidationError

from ._native import (
    ObjectSpec,
    ProjectedArrayBlobStreamer,
    StreamingProjectionError,
    extract_array_items,
    locate_array_start,
    project_array_items_partial,
)

T = TypeVar("T")


def _chunk_bytes(data: bytes, chunk_size: int) -> Iterator[bytes]:
    if len(data) <= chunk_size:
        yield data
        return

    for start in range(0, len(data), chunk_size):
        yield data[start : start + chunk_size]


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
        return _chunk_bytes(_coerce_bytes(source, description="source"), chunk_size)

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
        for chunk in _chunk_bytes(_coerce_bytes(source, description="source"), chunk_size):
            yield chunk
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


def _recontext_stream_validation_error(
    exc: ValidationError,
    *,
    item_index: int,
    root_prefix: str | None,
    approx_byte_offset: int | None = None,
) -> ValidationError:
    loc_prefix = ((*root_prefix.split("."), item_index) if root_prefix else (item_index,))

    if root_prefix is None:
        context = f"array item {item_index}"
    else:
        context = f"array item {item_index} under root_prefix {root_prefix!r}"
    if approx_byte_offset is not None:
        context += f" near byte offset {approx_byte_offset}"

    errors = []
    for error in exc.errors(include_url=False):
        error_data = dict(error)
        loc = error_data.get("loc", ())
        if isinstance(loc, tuple):
            loc_tuple = loc
        elif isinstance(loc, list):
            loc_tuple = tuple(loc)
        else:
            loc_tuple = (loc,)
        error_data["loc"] = loc_prefix + loc_tuple
        errors.append(error_data)

    return ValidationError.from_exception_data(
        f"{exc.title} ({context})",
        errors,
        input_type="json",
    )


def _stream_projected_json_array_item_batches_iter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[list[bytes]]:
    """Yield batches of projected item bytes from a top-level or prefixed JSON array."""
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
        if items:
            yield items
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
        if items:
            yield items
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if root_prefix is None and bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


async def _stream_projected_json_array_item_batches_aiter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[list[bytes]]:
    """Async variant of _stream_projected_json_array_item_batches_iter."""
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
        if items:
            yield items
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
        if items:
            yield items
        del buffer[:consumed]
        if not finished:
            raise StreamingProjectionError("Unexpected end of JSON array")
        if root_prefix is None and bytes(buffer).strip():
            raise StreamingProjectionError("Trailing content after JSON array")


def _stream_projected_json_array_blob_batches_iter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[bytes]:
    """Yield projected JSON array blobs for each chunk of complete items."""
    chunks = _source_to_chunks(source, chunk_size)
    streamer = ProjectedArrayBlobStreamer(spec, root_prefix)

    for chunk in chunks:
        blob = streamer.push(chunk)
        if blob is not None:
            yield blob

    blob = streamer.finish()
    if blob is not None:
        yield blob


async def _stream_projected_json_array_blob_batches_aiter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[bytes]:
    """Async variant of _stream_projected_json_array_blob_batches_iter."""
    chunks = _source_to_achunks(source, chunk_size)
    streamer = ProjectedArrayBlobStreamer(spec, root_prefix)

    async for chunk in chunks:
        blob = streamer.push(chunk)
        if blob is not None:
            yield blob

    blob = streamer.finish()
    if blob is not None:
        yield blob


def _stream_projected_json_array_item_bytes_iter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[bytes]:
    """Yield projected item bytes from a top-level or prefixed JSON array."""
    for items in _stream_projected_json_array_item_batches_iter(
        source,
        spec,
        root_prefix=root_prefix,
        chunk_size=chunk_size,
    ):
        yield from items


async def _stream_projected_json_array_item_bytes_aiter(
    source: Any,
    spec: ObjectSpec,
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> AsyncIterator[bytes]:
    """Async variant of _stream_projected_json_array_item_bytes_iter."""
    async for items in _stream_projected_json_array_item_batches_aiter(
        source,
        spec,
        root_prefix=root_prefix,
        chunk_size=chunk_size,
    ):
        for item_bytes in items:
            yield item_bytes


def _validate_json_blob_batches_iter(
    batches: Iterator[bytes],
    adapter: TypeAdapter[T],
    list_adapter: TypeAdapter[list[T]],
    *,
    root_prefix: str | None = None,
) -> Iterator[T]:
    validate = adapter.validate_json
    validate_list = list_adapter.validate_json
    item_index = 0

    for blob in batches:
        try:
            validated = validate_list(blob)
        except ValidationError:
            items, _, _ = extract_array_items(blob, is_start=True)
            for batch_index, item_bytes in enumerate(items):
                try:
                    yield validate(item_bytes)
                except ValidationError as exc:
                    raise _recontext_stream_validation_error(
                        exc,
                        item_index=item_index + batch_index,
                        root_prefix=root_prefix,
                    ) from exc
            item_index += len(items)
        else:
            item_index += len(validated)
            yield from validated


async def _validate_json_blob_batches_aiter(
    batches: AsyncIterator[bytes],
    adapter: TypeAdapter[T],
    list_adapter: TypeAdapter[list[T]],
    *,
    root_prefix: str | None = None,
) -> AsyncIterator[T]:
    validate = adapter.validate_json
    validate_list = list_adapter.validate_json
    item_index = 0

    async for blob in batches:
        try:
            validated = validate_list(blob)
        except ValidationError:
            items, _, _ = extract_array_items(blob, is_start=True)
            for batch_index, item_bytes in enumerate(items):
                try:
                    yield validate(item_bytes)
                except ValidationError as exc:
                    raise _recontext_stream_validation_error(
                        exc,
                        item_index=item_index + batch_index,
                        root_prefix=root_prefix,
                    ) from exc
            item_index += len(items)
        else:
            item_index += len(validated)
            for item in validated:
                yield item


def _validate_json_item_batches_iter(
    batches: Iterator[list[bytes]],
    adapter: TypeAdapter[T],
    list_adapter: TypeAdapter[list[T]],
) -> Iterator[T]:
    validate = adapter.validate_json
    validate_list = list_adapter.validate_json

    for items in batches:
        if len(items) == 1:
            yield validate(items[0])
            continue

        blob = b"[" + b",".join(items) + b"]"
        try:
            yield from validate_list(blob)
        except ValidationError:
            for item_bytes in items:
                yield validate(item_bytes)


async def _validate_json_item_batches_aiter(
    batches: AsyncIterator[list[bytes]],
    adapter: TypeAdapter[T],
    list_adapter: TypeAdapter[list[T]],
) -> AsyncIterator[T]:
    validate = adapter.validate_json
    validate_list = list_adapter.validate_json

    async for items in batches:
        if len(items) == 1:
            yield validate(items[0])
            continue

        blob = b"[" + b",".join(items) + b"]"
        try:
            for item in validate_list(blob):
                yield item
        except ValidationError:
            for item_bytes in items:
                yield validate(item_bytes)


def stream_projected_json_array_iter(
    source: Any,
    spec: ObjectSpec,
    validator: Callable[[bytes], T],
    *,
    root_prefix: str | None = None,
    chunk_size: int = 1_048_576,
) -> Iterator[T]:
    """Yield projected array items through ``validator(item_bytes)``.

    This is an advanced escape hatch for non-model consumers. Most users
    should prefer the model or dataclass mixin methods.
    """
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
    """Async version of ``stream_projected_json_array_iter``."""
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
    """Yield items from a plain JSON array in bounded memory.

    This helper does not perform projection; it validates each item as-is with
    ``adapter.validate_json``. For projection-aware streaming, prefer the mixin
    methods or ``stream_projected_json_array_iter``.
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
