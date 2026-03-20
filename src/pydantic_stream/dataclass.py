from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from typing import Any, ClassVar, Self, TypeVar, cast

from pydantic import TypeAdapter
from pydantic.dataclasses import rebuild_dataclass
from pydantic_core import ValidationError

from ._native import ObjectSpec, StreamingProjectionError, project_jsonl, project_object
from ._schema import compile_object_spec
from ._streaming import (
    _stream_projected_json_array_blob_batches_aiter,
    _stream_projected_json_array_blob_batches_iter,
    _validate_json_blob_batches_aiter,
    _validate_json_blob_batches_iter,
)
from .base_model import _is_eager_source, _to_bytes, _to_bytes_jsonl
from .stream_array import StreamArray

Streamable = TypeVar("Streamable", bound="StreamingDataclassMixin")


def compile_spec_for_type(typ: type[Any]) -> ObjectSpec:
    adapter: TypeAdapter[Any] = TypeAdapter(typ)
    return compile_object_spec(adapter.core_schema)


class StreamingDataclassMixin:
    """Mixin that adds streaming JSON helpers to a Pydantic dataclass.

    The ``stream_validate_*`` methods drop undeclared fields before
    validation, so large payloads can be processed without first materializing
    the full input into Python objects.
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
        """Validate one JSON object after projecting away undeclared fields.

        ``source`` may be bytes, str, bytearray, a file-like object, or a
        callable returning one of those.
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
    def stream_validate_json_array_iter(
        cls: type[Streamable],
        source: Any,
        *,
        root_prefix: str | None = None,
        chunk_size: int = 1_048_576,
    ) -> Iterator[Streamable]:
        """Yield dataclass instances from a JSON array in bounded memory.

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
    async def stream_validate_json_array_aiter(
        cls: type[Streamable],
        source: Any,
        *,
        root_prefix: str | None = None,
        chunk_size: int = 1_048_576,
    ) -> AsyncIterator[Streamable]:
        """Async version of ``stream_validate_json_array_iter``.

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
    def stream_validate_jsonl_iter(
        cls: type[Streamable],
        source: Any,
    ) -> Iterator[Streamable]:
        """Yield dataclass instances from JSON Lines input.

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
    def stream_validate_jsonl(
        cls: type[Streamable],
        source: Any,
    ) -> list[Streamable]:
        """Materialize JSON Lines input into a list of dataclass instances."""
        return list(cls.stream_validate_jsonl_iter(source))


__all__ = ["StreamingDataclassMixin", "StreamingProjectionError", "Streamable"]
