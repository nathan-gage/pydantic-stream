from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Generic, TypeVar, overload

from pydantic import TypeAdapter
from pydantic_core import ValidationError

from ._native import (
    ObjectSpec,
    extract_array_items,
    project_array_item_at,
    project_array_items_sliced,
    project_array_nav,
    validate_array_items,
    validate_array_items_next_batch,
    validate_array_nav,
    validate_raw_array_item_next,
    validate_raw_array_items,
)

T = TypeVar("T")

# For smaller inputs, item-by-item iteration cuts peak memory substantially by
# avoiding an additional compacted array blob. Very small arrays that do not
# need projection can optionally validate raw item bytes directly.
_ITER_ITEMWISE_BYTES_THRESHOLD = 256 * 1024
_RAW_ITEMWISE_BYTES_THRESHOLD = 128 * 1024
_EAGER_VALIDATED_ITEMS_BYTES_THRESHOLD = 128 * 1024
_PROJECTED_ITER_BATCH_SIZE = 16


def _iter_items_then_raise(items: list[T], error: BaseException) -> Iterator[T]:
    yield from items
    raise error


def _iter_validated_array_item_batches(data: Any, spec: ObjectSpec, validator: Any) -> Iterator[T]:
    pos = 0
    started = False
    while True:
        items, pos, finished, error = validate_array_items_next_batch(
            data,
            spec,
            validator,
            pos,
            started,
            _PROJECTED_ITER_BATCH_SIZE,
        )
        if items:
            yield from items
        if error is not None:
            raise error
        if finished:
            return
        started = True


def _iter_validated_raw_array_items(data: Any, validator: Any) -> Iterator[T]:
    pos = 0
    started = False
    while True:
        item, pos, finished = validate_raw_array_item_next(data, validator, pos, started)
        if finished:
            return
        started = True
        if item is not None:
            yield item


class StreamArray(Generic[T]):
    """Lazy, indexable view over a JSON array of validated items.

    ``StreamArray`` supports iteration, indexing, slicing, and eager
    materialization via :meth:`to_list`.
    """

    __slots__ = (
        "_data",
        "_spec",
        "_adapter",
        "_list_adapter",
        "_prefix",
        "_prefer_itemwise_iter",
        "_allow_raw_small_iter",
    )

    def __init__(
        self,
        data: Any,
        spec: ObjectSpec,
        adapter: TypeAdapter[T],
        list_adapter: TypeAdapter[list[T]],
        root_prefix: str | None = None,
        *,
        prefer_itemwise_iter: bool = False,
        allow_raw_small_iter: bool = False,
    ) -> None:
        """Create a lazy view over a JSON array source.

        Args:
            data: JSON document containing the target array.
            spec: Precompiled field mapping for the item type.
            adapter: ``TypeAdapter[T]`` used for single-item validation.
            list_adapter: ``TypeAdapter[list[T]]`` used when validating the
                full array at once.
            root_prefix: Dot-separated path to the array within the document,
                for example ``"data.items"``. Use ``None`` for a top-level
                array.
            prefer_itemwise_iter: Prefer validating one item at a time while
                iterating.
            allow_raw_small_iter: Allow direct validation for small arrays when
                the item type already accepts extra fields.
        """
        self._data = data if isinstance(data, bytes) else bytes(data)
        self._spec = spec
        self._adapter = adapter
        self._list_adapter = list_adapter
        self._prefix = root_prefix
        self._prefer_itemwise_iter = prefer_itemwise_iter
        self._allow_raw_small_iter = allow_raw_small_iter

    def __iter__(self) -> Iterator[T]:
        # Iteration is the memory-sensitive path (e.g. ``list(stream_array)``).
        # For smaller top-level inputs, validate projected items one-by-one so
        # we do not keep an additional compacted array blob alive alongside the
        # final result objects. Larger inputs keep the faster bulk-validation
        # path and only fall back to per-item validation on errors to preserve
        # error locations and "yield valid items until the first error"
        # semantics.
        fast_validate = self._adapter.validator.validate_json
        data_len = len(self._data)
        if (
            self._prefix is None
            and self._allow_raw_small_iter
            and data_len <= _RAW_ITEMWISE_BYTES_THRESHOLD
        ):
            if data_len <= _EAGER_VALIDATED_ITEMS_BYTES_THRESHOLD:
                items, error = validate_raw_array_items(self._data, fast_validate)
                return iter(items) if error is None else _iter_items_then_raise(items, error)
            return _iter_validated_raw_array_items(self._data, fast_validate)

        if self._prefix is None and (
            self._prefer_itemwise_iter or data_len <= _ITER_ITEMWISE_BYTES_THRESHOLD
        ):
            if data_len <= _EAGER_VALIDATED_ITEMS_BYTES_THRESHOLD:
                items, error = validate_array_items(self._data, self._spec, fast_validate)
                return iter(items) if error is None else _iter_items_then_raise(items, error)
            return _iter_validated_array_item_batches(self._data, self._spec, fast_validate)

        blob = project_array_nav(self._data, self._spec, self._prefix)
        try:
            return iter(self._list_adapter.validate_json(blob))
        except ValidationError:
            items, _, _ = extract_array_items(blob, is_start=True)
            validate = self._adapter.validate_json
            return (validate(item_bytes) for item_bytes in items)

    @overload
    def __getitem__(self, index: int) -> T: ...

    @overload
    def __getitem__(self, index: slice) -> list[T]: ...

    def __getitem__(self, index: int | slice) -> T | list[T]:
        """Return one validated item or a list for a slice.

        Negative indexes are not supported. Slice bounds follow normal Python
        semantics except that negative ``start``, ``stop``, and ``step``
        values are rejected.

        Raises:
            IndexError: If the index is negative or out of range.
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
            validate = self._adapter.validator.validate_json
            return [validate(b) for b in items]

        if index < 0:
            raise IndexError("StreamArray does not support negative indexing")
        item = project_array_item_at(self._data, self._spec, self._prefix, index)
        if item is None:
            raise IndexError(f"StreamArray index {index} out of range")
        return self._adapter.validator.validate_json(item)

    def to_list(self) -> list[T]:
        """Validate the entire array and return the results as a list."""
        return validate_array_nav(
            self._data,
            self._spec,
            self._list_adapter.validator.validate_json,
            self._prefix,
        )

    def __repr__(self) -> str:
        prefix_part = f", prefix={self._prefix!r}" if self._prefix else ""
        return f"StreamArray({len(self._data)} bytes{prefix_part})"


__all__ = ["StreamArray"]
