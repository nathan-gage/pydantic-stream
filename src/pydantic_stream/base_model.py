from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar, Self, TypeVar, cast

from pydantic import BaseModel, TypeAdapter
from pydantic_core import ValidationError

from ._native import ObjectSpec, StreamingProjectionError, project_jsonl, project_object
from ._schema import compile_model_spec
from ._streaming import (
    _stream_projected_json_array_blob_batches_aiter,
    _stream_projected_json_array_blob_batches_iter,
    _validate_json_blob_batches_aiter,
    _validate_json_blob_batches_iter,
)
from .stream_array import StreamArray

StreamableModel = TypeVar("StreamableModel", bound="StreamingBaseModelMixin")


def _to_bytes(source: Any) -> bytes:
    """Normalize any source to bytes."""
    if isinstance(source, bytes):
        return source
    if isinstance(source, str):
        return source.encode("utf-8")
    if isinstance(source, bytearray):
        return bytes(source)
    if callable(source):
        resolved = source()
        if callable(resolved) and not isinstance(resolved, (bytes, str, bytearray)):
            raise StreamingProjectionError(
                f"Callable source returned another callable: {type(resolved).__name__!r}"
            )
        return _to_bytes(resolved)
    if hasattr(source, "read"):
        data = source.read()
        if isinstance(data, str):
            return data.encode("utf-8")
        return bytes(data) if isinstance(data, bytearray) else data
    raise StreamingProjectionError(f"Unsupported source type: {type(source).__name__!r}")


def _to_bytes_jsonl(source: Any) -> bytes:
    """Like _to_bytes but also handles iterables of str/bytes lines (for JSONL)."""
    if isinstance(source, (bytes, str, bytearray)) or hasattr(source, "read"):
        return _to_bytes(source)
    parts: list[bytes] = []
    for item in source:
        if isinstance(item, str):
            parts.append(item.encode("utf-8"))
        elif isinstance(item, bytearray):
            parts.append(bytes(item))
        elif isinstance(item, bytes):
            parts.append(item)
        else:
            raise StreamingProjectionError(f"Unsupported line type: {type(item).__name__!r}")
    return b"\n".join(parts)


def _is_eager_source(source: Any) -> bool:
    """True if source is bytes/str/bytearray/file-like (not an iterator)."""
    return isinstance(source, (bytes, str, bytearray)) or hasattr(source, "read")


def _has_trailing_array_content(remainder: bytes, chunks: Iterator[Any]) -> bool:
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
        list_adapter = cls._streaming_list_adapter()
        spec = cls._streaming_spec()

        yield from _validate_json_blob_batches_iter(
            _stream_projected_json_array_blob_batches_iter(
                source,
                spec,
                root_prefix=root_prefix,
                chunk_size=chunk_size,
            ),
            adapter,
            list_adapter,
            root_prefix=root_prefix,
        )

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
        list_adapter = cls._streaming_list_adapter()
        spec = cls._streaming_spec()

        async for item in _validate_json_blob_batches_aiter(
            _stream_projected_json_array_blob_batches_aiter(
                source,
                spec,
                root_prefix=root_prefix,
                chunk_size=chunk_size,
            ),
            adapter,
            list_adapter,
            root_prefix=root_prefix,
        ):
            yield item

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
    def stream_model_validate_jsonl(
        cls: type[StreamableModel],
        source: Any,
    ) -> list[StreamableModel]:
        """Materialize JSON Lines input into a list of model instances."""
        return list(cls.stream_model_validate_jsonl_iter(source))


__all__ = ["StreamingBaseModelMixin", "StreamingProjectionError", "StreamableModel"]
