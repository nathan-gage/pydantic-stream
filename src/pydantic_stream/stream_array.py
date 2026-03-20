from __future__ import annotations

from collections.abc import Iterator
from typing import Generic, TypeVar, overload

from pydantic import TypeAdapter
from pydantic_core import ValidationError

from ._native import ObjectSpec, extract_array_items, project_array_items_sliced, project_array_nav
from ._streaming import (
    _stream_projected_json_array_blob_batches_iter,
    _validate_json_blob_batches_iter,
)

T = TypeVar("T")


class StreamArray(Generic[T]):
    """Lazy view over a projected JSON array.

    Supports iteration, indexing, slicing, and ``to_list()`` without eagerly
    validating the entire array up front.
    """

    __slots__ = ("_data", "_spec", "_adapter", "_list_adapter", "_prefix")

    def __init__(
        self,
        data: bytes,
        spec: ObjectSpec,
        adapter: TypeAdapter[T],
        list_adapter: TypeAdapter[list[T]],
        root_prefix: str | None = None,
    ) -> None:
        self._data = data
        self._spec = spec
        self._adapter = adapter
        self._list_adapter = list_adapter
        self._prefix = root_prefix

    def __iter__(self) -> Iterator[T]:
        """Iterate over validated items on demand."""
        if self._prefix:
            yield from _validate_json_blob_batches_iter(
                _stream_projected_json_array_blob_batches_iter(
                    self._data,
                    self._spec,
                    root_prefix=self._prefix,
                ),
                self._adapter,
                self._list_adapter,
                root_prefix=self._prefix,
            )
            return

        # Fast path: project the entire array into one compact blob (single Rust
        # allocation), then validate everything in one pydantic-core call.
        # For the common case of valid data this avoids N per-item Vec<u8>
        # allocations and N separate validate_json calls.
        blob = project_array_nav(self._data, self._spec, self._prefix)
        try:
            yield from self._list_adapter.validate_json(blob)
        except ValidationError:
            # Slow path: re-validate item-by-item from the projected blob so
            # that ValidationError.loc contains ("field",) not (index, "field").
            # This also preserves "yield valid items up to the first error"
            # semantics that per-item validation gives.
            items, _, _ = extract_array_items(blob, is_start=True)
            validate = self._adapter.validate_json
            for item_bytes in items:
                yield validate(item_bytes)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Return one validated item, or a validated list for a slice.

        Negative indices and negative slice steps are not supported.
        """
        if isinstance(index, slice):
            start, stop, step = index.start, index.stop, index.step
            if start is not None and start < 0:
                raise IndexError("StreamArray does not support negative start index")
            if stop is not None and stop < 0:
                raise IndexError("StreamArray does not support negative stop index")
            if step is not None and step == 0:
                raise ValueError("StreamArray slice step cannot be zero")
            if step is not None and step < 0:
                raise IndexError("StreamArray does not support negative step")
            items = project_array_items_sliced(
                self._data,
                self._spec,
                self._prefix,
                start or 0,
                stop,
                step or 1,
            )
            return [self._adapter.validate_json(b) for b in items]

        if index < 0:
            raise IndexError("StreamArray does not support negative indexing")
        items = project_array_items_sliced(
            self._data, self._spec, self._prefix, index, index + 1, 1
        )
        if not items:
            raise IndexError(f"StreamArray index {index} out of range")
        return self._adapter.validate_json(items[0])

    def to_list(self) -> list[T]:
        """Materialize the whole array and validate it in one pass."""
        blob = project_array_nav(self._data, self._spec, self._prefix)
        return self._list_adapter.validate_json(blob)

    def __repr__(self) -> str:
        prefix_part = f", prefix={self._prefix!r}" if self._prefix else ""
        return f"StreamArray({len(self._data)} bytes{prefix_part})"


__all__ = ["StreamArray"]
