from __future__ import annotations

from collections.abc import Iterator
from typing import Any, ClassVar, Self, TypeVar, cast

from pydantic import BaseModel, TypeAdapter
from pydantic_core import ValidationError

from ._native import (
    ObjectSpec,
    StreamingProjectionError,
    project_array_items_partial,
    project_jsonl,
    project_object,
)
from ._schema import compile_model_spec
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


def compile_spec_for_model_type(typ: type[Any]) -> ObjectSpec:
    adapter: TypeAdapter[Any] = TypeAdapter(typ)
    return compile_model_spec(adapter.core_schema)


class StreamingBaseModelMixin(BaseModel):
    """Minimal schema-aware projection in front of Pydantic validation.

    Uses Rust/jiter projection to emit compact JSON bytes, then calls
    pydantic's validate_json for fast Rust-to-Rust validation.
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
        chunk_size: int = 1_048_576,
    ) -> Iterator[StreamableModel]:
        """Stream-validate a JSON array of objects in bounded memory.

        Uses combined streaming + projection for maximum efficiency.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()

        if hasattr(source, "read"):
            chunks: Iterator[bytes] = iter(lambda: source.read(chunk_size), b"")
        else:
            chunks = iter(source)

        buffer = bytearray()
        is_start = True

        for chunk in chunks:
            if not chunk:
                break
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            for item_bytes in items:
                yield adapter.validate_json(item_bytes)
            del buffer[:consumed]
            is_start = False
            if finished:
                return

        # Drain remaining buffer
        if buffer:
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            for item_bytes in items:
                yield adapter.validate_json(item_bytes)
            if not finished:
                remaining = bytes(buffer[consumed:]).strip()
                if remaining:
                    raise StreamingProjectionError("Unexpected end of JSON array")

    @classmethod
    def stream_model_validate_jsonl_iter(
        cls: type[StreamableModel],
        source: Any,
    ) -> Iterator[StreamableModel]:
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
        return list(cls.stream_model_validate_jsonl_iter(source))


__all__ = ["StreamingBaseModelMixin", "StreamingProjectionError", "StreamableModel"]
