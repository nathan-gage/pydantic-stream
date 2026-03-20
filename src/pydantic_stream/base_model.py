from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar, Self, TypeVar, cast

from pydantic import BaseModel, TypeAdapter
from pydantic_core import ValidationError

from ._native import (
    ObjectSpec,
    StreamingProjectionError,
    project_jsonl,
    project_object,
    validate_array_items_partial,
)
from ._schema import compile_model_spec
from ._streaming import (
    _aiter_jsonl_lines_from_chunks,
    _async_source_to_chunks,
    _has_trailing_array_content_async,
    _recontext_stream_validation_error,
    _source_to_chunks,
    _stream_projected_json_array_blob_batches_aiter,
    _stream_projected_json_array_blob_batches_iter,
    _validate_json_blob_batches_aiter,
    _validate_json_blob_batches_iter,
)
from .stream_array import StreamArray

StreamableModel = TypeVar("StreamableModel", bound="StreamingBaseModelMixin")


def _as_native_bytes(data: bytes | bytearray | memoryview) -> bytes:
    return data if isinstance(data, bytes) else bytes(data)


def _to_bytes(source: Any) -> bytes:
    """Normalize an eager input source to ``bytes``."""
    if isinstance(source, bytes):
        return source
    if isinstance(source, (bytearray, memoryview)):
        return _as_native_bytes(source)
    if isinstance(source, str):
        return source.encode("utf-8")
    if callable(source):
        resolved = source()
        if callable(resolved) and not isinstance(resolved, (bytes, str, bytearray, memoryview)):
            raise StreamingProjectionError(
                f"Callable source returned another callable: {type(resolved).__name__!r}"
            )
        return _to_bytes(resolved)
    if hasattr(source, "read"):
        data = source.read()
        if isinstance(data, str):
            return data.encode("utf-8")
        if isinstance(data, bytes):
            return data
        if isinstance(data, (bytearray, memoryview)):
            return _as_native_bytes(data)
    raise StreamingProjectionError(f"Unsupported source type: {type(source).__name__!r}")


def _to_bytes_jsonl(source: Any) -> bytes:
    """Like ``_to_bytes`` but also handles iterables of JSONL lines."""
    if isinstance(source, (bytes, str, bytearray, memoryview)) or hasattr(source, "read"):
        return _to_bytes(source)
    parts: list[bytes] = []
    for item in source:
        if isinstance(item, str):
            parts.append(item.encode("utf-8"))
        elif isinstance(item, bytes):
            parts.append(item)
        elif isinstance(item, (bytearray, memoryview)):
            parts.append(_as_native_bytes(item))
        else:
            raise StreamingProjectionError(f"Unsupported line type: {type(item).__name__!r}")
    return b"\n".join(parts)


def _is_eager_source(source: Any) -> bool:
    """True if source is bytes-like/str/file-like (not an iterator)."""
    return isinstance(source, (bytes, str, bytearray, memoryview)) or hasattr(source, "read")


def _has_trailing_array_content(remainder: Any, chunks: Iterator[Any]) -> bool:
    if remainder.strip():
        return True
    for chunk in chunks:
        if chunk and bytes(chunk).strip():
            return True
    return False


def compile_spec_for_model_type(typ: type[Any]) -> ObjectSpec:
    adapter: TypeAdapter[Any] = TypeAdapter(typ)
    return compile_model_spec(adapter.core_schema)


class StreamingBaseModelMixin(BaseModel):
    """Mixin that adds streaming JSON helpers to a ``BaseModel``.

    The ``stream_model_validate_*`` methods drop undeclared fields before
    validation, so large payloads can be processed without first materializing
    the full input into Python objects.
    """

    __streaming_type_adapter__: ClassVar[TypeAdapter[Any] | None] = None
    __streaming_list_adapter__: ClassVar[TypeAdapter[Any] | None] = None
    __streaming_object_spec__: ClassVar[ObjectSpec | None] = None

    @classmethod
    def _ensure_rebuilt(cls) -> None:
        cls.model_rebuild()

    @classmethod
    def _streaming_adapter(cls) -> TypeAdapter[Self]:
        cls._ensure_rebuilt()
        adapter = cls.__dict__.get("__streaming_type_adapter__")
        if adapter is None:
            adapter = TypeAdapter(cls)
            cls.__streaming_type_adapter__ = adapter
        return cast(TypeAdapter[Self], adapter)

    @classmethod
    def _streaming_list_adapter(cls) -> TypeAdapter[list[Self]]:
        cls._ensure_rebuilt()
        adapter = cls.__dict__.get("__streaming_list_adapter__")
        if adapter is None:
            adapter = TypeAdapter(list[cls])  # type: ignore[valid-type]
            cls.__streaming_list_adapter__ = adapter
        return cast(TypeAdapter[list[Self]], adapter)

    @classmethod
    def _streaming_spec(cls) -> ObjectSpec:
        cls._ensure_rebuilt()
        spec = cls.__dict__.get("__streaming_object_spec__")
        if spec is None:
            spec = compile_spec_for_model_type(cls)
            cls.__streaming_object_spec__ = spec
        return spec

    @classmethod
    def stream_model_validate_json(
        cls: type[StreamableModel],
        source: Any,
    ) -> StreamableModel:
        """Validate one JSON object after projecting away undeclared fields.

        ``source`` may be bytes, str, bytearray, a file-like object, or a
        callable returning one of those.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()
        projected = project_object(_to_bytes(source), spec)
        return adapter.validate_json(projected)

    @classmethod
    def stream_model_validate_json_array(
        cls: type[StreamableModel],
        source: Any,
        *,
        root_prefix: str | None = None,
    ) -> StreamArray[StreamableModel]:
        """Return a lazy ``StreamArray`` over a JSON array.

        Pass ``root_prefix`` to target an array inside a document envelope, for
        example ``root_prefix="pages"`` for ``{"pages": [...]}``.
        """
        return StreamArray(
            data=_to_bytes(source),
            spec=cls._streaming_spec(),
            adapter=cls._streaming_adapter(),
            list_adapter=cls._streaming_list_adapter(),
            root_prefix=root_prefix,
            prefer_itemwise_iter=True,
            allow_raw_small_iter=cls.model_config.get("extra") in (None, "ignore"),
        )

    @classmethod
    def stream_model_validate_json_array_iter(
        cls: type[StreamableModel],
        source: Any,
        *,
        root_prefix: str | None = None,
        chunk_size: int = 1_048_576,
    ) -> Iterator[StreamableModel]:
        """Yield model instances from a JSON array in bounded memory.

        ``source`` may be bytes, str, a reader, or an iterable of byte chunks.
        Use ``root_prefix`` for nested arrays such as ``{"pages": [...]}``.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()

        if root_prefix is not None:
            yield from _validate_json_blob_batches_iter(
                _stream_projected_json_array_blob_batches_iter(
                    source,
                    spec,
                    root_prefix=root_prefix,
                    chunk_size=chunk_size,
                ),
                adapter,
                cls._streaming_list_adapter(),
                root_prefix=root_prefix,
            )
            return

        validate = adapter.validator.validate_json
        chunks = _source_to_chunks(source, chunk_size)

        buffer = bytearray()
        is_start = True
        saw_input = False
        item_index = 0

        for chunk in chunks:
            if not chunk:
                continue
            saw_input = True
            buffer.extend(chunk)
            items, consumed, finished, error = validate_array_items_partial(
                bytes(buffer),
                spec,
                validate,
                is_start,
            )
            yield from items
            if error is not None:
                if isinstance(error, ValidationError):
                    raise _recontext_stream_validation_error(
                        error,
                        item_index=item_index + len(items),
                        root_prefix=None,
                    ) from error
                raise error
            item_index += len(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                if _has_trailing_array_content(bytes(buffer), chunks):
                    raise StreamingProjectionError("Trailing content after JSON array")
                return

        if saw_input and not buffer:
            raise StreamingProjectionError("Unexpected end of JSON array")

        if buffer:
            items, consumed, finished, error = validate_array_items_partial(
                bytes(buffer),
                spec,
                validate,
                is_start,
            )
            yield from items
            if error is not None:
                if isinstance(error, ValidationError):
                    raise _recontext_stream_validation_error(
                        error,
                        item_index=item_index + len(items),
                        root_prefix=None,
                    ) from error
                raise error
            del buffer[:consumed]
            if not finished:
                raise StreamingProjectionError("Unexpected end of JSON array")
            if bytes(buffer).strip():
                raise StreamingProjectionError("Trailing content after JSON array")

    @classmethod
    async def stream_model_validate_json_array_aiter(
        cls: type[StreamableModel],
        source: Any,
        *,
        root_prefix: str | None = None,
        chunk_size: int = 1_048_576,
    ) -> AsyncIterator[StreamableModel]:
        """Async version of ``stream_model_validate_json_array_iter``.

        ``source`` may be bytes, str, an async reader, or an async iterable of
        byte chunks. Use ``root_prefix`` for nested arrays.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()

        if root_prefix is not None:
            async for item in _validate_json_blob_batches_aiter(
                _stream_projected_json_array_blob_batches_aiter(
                    source,
                    spec,
                    root_prefix=root_prefix,
                    chunk_size=chunk_size,
                ),
                adapter,
                cls._streaming_list_adapter(),
                root_prefix=root_prefix,
            ):
                yield item
            return

        validate = adapter.validator.validate_json
        chunks = _async_source_to_chunks(source, chunk_size)

        buffer = bytearray()
        is_start = True
        saw_input = False
        item_index = 0

        async for chunk in chunks:
            if not chunk:
                continue
            saw_input = True
            buffer.extend(chunk)
            items, consumed, finished, error = validate_array_items_partial(
                bytes(buffer),
                spec,
                validate,
                is_start,
            )
            for item in items:
                yield item
            if error is not None:
                if isinstance(error, ValidationError):
                    raise _recontext_stream_validation_error(
                        error,
                        item_index=item_index + len(items),
                        root_prefix=None,
                    ) from error
                raise error
            item_index += len(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                if await _has_trailing_array_content_async(bytes(buffer), chunks):
                    raise StreamingProjectionError("Trailing content after JSON array")
                return

        if saw_input and not buffer:
            raise StreamingProjectionError("Unexpected end of JSON array")

        if buffer:
            items, consumed, finished, error = validate_array_items_partial(
                bytes(buffer),
                spec,
                validate,
                is_start,
            )
            for item in items:
                yield item
            if error is not None:
                if isinstance(error, ValidationError):
                    raise _recontext_stream_validation_error(
                        error,
                        item_index=item_index + len(items),
                        root_prefix=None,
                    ) from error
                raise error
            del buffer[:consumed]
            if not finished:
                raise StreamingProjectionError("Unexpected end of JSON array")
            if bytes(buffer).strip():
                raise StreamingProjectionError("Trailing content after JSON array")

    @classmethod
    def stream_model_validate_jsonl_iter(
        cls: type[StreamableModel],
        source: Any,
    ) -> Iterator[StreamableModel]:
        """Yield model instances from JSON Lines input.

        Each non-empty line is projected and validated independently.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()

        if not _is_eager_source(source):
            for item_index, raw_line in enumerate(source):
                raw_bytes = _to_bytes(raw_line)
                if not raw_bytes.strip():
                    continue
                projected = project_object(raw_bytes, spec)
                try:
                    yield adapter.validate_json(projected)
                except ValidationError as exc:
                    raise ValueError(f"Validation failed for item {item_index}: {exc}") from exc
            return

        lines = project_jsonl(_to_bytes_jsonl(source), spec)

        for item_index, line in enumerate(lines):
            try:
                yield adapter.validate_json(line)
            except ValidationError as exc:
                raise ValueError(f"Validation failed for item {item_index}: {exc}") from exc

    @classmethod
    async def stream_model_validate_jsonl_aiter(
        cls: type[StreamableModel],
        source: Any,
        *,
        chunk_size: int = 1_048_576,
    ) -> AsyncIterator[StreamableModel]:
        """Async version of ``stream_model_validate_jsonl_iter``."""
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()
        item_index = 0

        async for line_number, raw_line in _aiter_jsonl_lines_from_chunks(
            _async_source_to_chunks(source, chunk_size)
        ):
            if not raw_line.strip():
                continue
            try:
                projected = project_object(raw_line, spec)
            except StreamingProjectionError as exc:
                raise StreamingProjectionError(f"{exc} (line {line_number})") from exc
            try:
                yield adapter.validate_json(projected)
            except ValidationError as exc:
                raise ValueError(f"Validation failed for item {item_index}: {exc}") from exc
            item_index += 1

    @classmethod
    def stream_model_validate_jsonl(
        cls: type[StreamableModel],
        source: Any,
    ) -> list[StreamableModel]:
        """Materialize JSON Lines input into a list of model instances."""
        return list(cls.stream_model_validate_jsonl_iter(source))


__all__ = ["StreamingBaseModelMixin", "StreamingProjectionError", "StreamableModel"]
