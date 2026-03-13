from __future__ import annotations

from collections.abc import Iterator
from typing import Generic, TypeVar, overload

from pydantic import TypeAdapter

from ._native import ObjectSpec, project_array_items_sliced, project_array_nav

T = TypeVar("T")


class StreamArray(Generic[T]):
    """Lazy array container that holds raw bytes and re-parses on demand.

    Supports iteration, indexing, slicing, and bulk materialization via ``to_list()``.
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
        items = project_array_items_sliced(self._data, self._spec, self._prefix, 0, None, 1)
        for item_bytes in items:
            yield self._adapter.validate_json(item_bytes)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
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
        """Bulk materialization fast path.

        Calls Rust once to produce the full projected array, then validates
        everything in a single pydantic-core pass.
        """
        blob = project_array_nav(self._data, self._spec, self._prefix)
        return self._list_adapter.validate_json(blob)

    def __repr__(self) -> str:
        prefix_part = f", prefix={self._prefix!r}" if self._prefix else ""
        return f"StreamArray({len(self._data)} bytes{prefix_part})"


__all__ = ["StreamArray"]
