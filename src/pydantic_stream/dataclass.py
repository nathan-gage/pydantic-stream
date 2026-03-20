from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar, Self, TypeVar, cast

from pydantic import TypeAdapter
from pydantic.dataclasses import rebuild_dataclass
from pydantic_core import ValidationError

from ._native import (
    ObjectSpec,
    StreamingProjectionError,
    project_jsonl,
    project_object,
    validate_array_items_partial,
)
from ._schema import compile_object_spec
from ._streaming import (
    _async_source_to_chunks,
    _has_trailing_array_content_async,
    _source_to_chunks,
)
from .base_model import _has_trailing_array_content, _is_eager_source, _to_bytes, _to_bytes_jsonl
from .stream_array import StreamArray

Streamable = TypeVar("Streamable", bound="StreamingDataclassMixin")


def compile_spec_for_type(typ: type[Any]) -> ObjectSpec:
    adapter: TypeAdapter[Any] = TypeAdapter(typ)
    return compile_object_spec(adapter.core_schema)


class StreamingDataclassMixin:
    """Mixin that adds JSON streaming helpers to a Pydantic dataclass.

    The class methods on this mixin validate raw JSON input directly,
    ignore unrelated object fields, and return dataclass instances without
    requiring ``json.loads()`` first.
    """

    __streaming_type_adapter__: ClassVar[TypeAdapter[Any] | None] = None
    __streaming_list_adapter__: ClassVar[TypeAdapter[Any] | None] = None
    __streaming_object_spec__: ClassVar[ObjectSpec | None] = None

    @classmethod
    def _ensure_rebuilt(cls) -> None:
        rebuild_dataclass(cls)  # type: ignore[arg-type]

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
            spec = compile_spec_for_type(cls)
            cls.__streaming_object_spec__ = spec
        return spec

    @classmethod
    def stream_validate_json(
        cls: type[Streamable],
        source: Any,
    ) -> Streamable:
        """Parse a JSON object into an instance of the calling dataclass.

        Extra object fields are ignored before validation.

        Args:
            source: ``bytes``, ``str``, a file-like object with ``.read()``,
                or a zero-argument callable that returns one of those.

        Returns:
            A validated instance of the calling class.
        """
        adapter = cls._streaming_adapter()
        spec = cls._streaming_spec()
        projected = project_object(_to_bytes(source), spec)
        return adapter.validate_json(projected)

    @classmethod
    def stream_validate_json_array(
        cls: type[Streamable],
        source: Any,
        *,
        root_prefix: str | None = None,
    ) -> StreamArray[Streamable]:
        """Return a lazy :class:`StreamArray` over a JSON array of objects.

        The input is only processed when you iterate, index, slice, or call
        :meth:`StreamArray.to_list`.

        Args:
            source: ``bytes``, ``str``, a file-like object, or a callable —
                the same input forms accepted by :meth:`stream_validate_json`.
            root_prefix: Dot-separated path to the array within the JSON
                document, for example ``"data.items"``. Use ``None`` for a
                top-level array.

        Returns:
            A :class:`StreamArray` of validated dataclass instances.
        """
        return StreamArray(
            data=_to_bytes(source),
            spec=cls._streaming_spec(),
            adapter=cls._streaming_adapter(),
            list_adapter=cls._streaming_list_adapter(),
            root_prefix=root_prefix,
            prefer_itemwise_iter=True,
            allow_raw_small_iter=getattr(cls, "__pydantic_config__", {}).get("extra")
            in (None, "ignore"),
        )

    @classmethod
    def stream_validate_json_array_iter(
        cls: type[Streamable],
        source: Any,
        *,
        chunk_size: int = 1_048_576,
    ) -> Iterator[Streamable]:
        """Iterate over validated items from a JSON array.

        The source is consumed incrementally, so you can process large arrays
        without loading the entire document into memory at once.
        """
        adapter = cls._streaming_adapter()
        validate = adapter.validator.validate_json
        spec = cls._streaming_spec()

        chunks = _source_to_chunks(source, chunk_size)

        buffer = bytearray()
        is_start = True
        saw_input = False

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
                raise error
            del buffer[:consumed]
            is_start = False
            if finished:
                if _has_trailing_array_content(buffer, chunks):
                    raise StreamingProjectionError("Trailing content after JSON array")
                return

        if saw_input and not buffer:
            raise StreamingProjectionError("Unexpected end of JSON array")

        # Drain remaining buffer
        if buffer:
            items, consumed, finished, error = validate_array_items_partial(
                bytes(buffer),
                spec,
                validate,
                is_start,
            )
            yield from items
            if error is not None:
                raise error
            del buffer[:consumed]
            if not finished:
                raise StreamingProjectionError("Unexpected end of JSON array")
            if buffer.strip():
                raise StreamingProjectionError("Trailing content after JSON array")

    @classmethod
    async def stream_validate_json_array_aiter(
        cls: type[Streamable],
        source: Any,
        *,
        chunk_size: int = 1_048_576,
    ) -> AsyncIterator[Streamable]:
        """Async variant of :meth:`stream_validate_json_array_iter`.

        Accepts async file-like objects and async iterables of chunks in
        addition to the synchronous input forms supported by the sync method.
        """
        adapter = cls._streaming_adapter()
        validate = adapter.validator.validate_json
        spec = cls._streaming_spec()
        chunks = _async_source_to_chunks(source, chunk_size)

        buffer = bytearray()
        is_start = True
        saw_input = False

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
                raise error
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
                raise error
            del buffer[:consumed]
            if not finished:
                raise StreamingProjectionError("Unexpected end of JSON array")
            if buffer.strip():
                raise StreamingProjectionError("Trailing content after JSON array")

    @classmethod
    def stream_validate_jsonl_iter(
        cls: type[Streamable],
        source: Any,
    ) -> Iterator[Streamable]:
        """Yield validated instances from JSON Lines input.

        Blank lines are ignored.

        Args:
            source: ``bytes``, ``str``, a file-like object, or an iterable of
                ``bytes``/``str`` lines.

        Yields:
            Validated instances of the calling class.
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
    def stream_validate_jsonl(
        cls: type[Streamable],
        source: Any,
    ) -> list[Streamable]:
        """Eagerly validate all JSONL records and return them as a list.

        Equivalent to ``list(cls.stream_validate_jsonl_iter(source))``.
        """
        return list(cls.stream_validate_jsonl_iter(source))


__all__ = ["StreamingDataclassMixin", "StreamingProjectionError", "Streamable"]
