"""Type stubs for native helpers exposed by pydantic-stream."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

class FieldSpec:
    """Field mapping used when selecting data from JSON objects.

    Args:
        output_key: Field name used in the projected JSON object.
        nested: Nested :class:`ObjectSpec` for object-valued fields, if any.
    """

    def __init__(self, output_key: str, nested: ObjectSpec | None = None) -> None: ...
    @property
    def output_key(self) -> str: ...
    def __repr__(self) -> str: ...

class ObjectSpec:
    """Mapping of accepted input keys for an object type.

    Args:
        fields_by_input_key: Mapping from input JSON key to :class:`FieldSpec`.
    """

    def __init__(self, fields_by_input_key: dict[str, FieldSpec]) -> None: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: str) -> bool: ...

class StreamingProjectionError(RuntimeError):
    """Raised when the input JSON or schema shape is unsupported."""

    ...

# ---------------------------------------------------------------------------
# Streaming — no field filtering required
# ---------------------------------------------------------------------------

def extract_array_items(
    data: bytes,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Extract complete items from a full or partial JSON array buffer.

    Returns:
        ``(items, consumed, finished)`` where ``items`` are raw JSON byte
        strings, ``consumed`` is the number of bytes that can be discarded
        from the front of ``data``, and ``finished`` is ``True`` once the
        closing ``]`` has been seen.
    """
    ...

def validate_raw_array_items(
    data: bytes,
    validator: Callable[[bytes], Any],
) -> tuple[list[Any], BaseException | None]:
    """Validate every item in a JSON array without field filtering.

    Returns:
        ``(validated_items, first_error_or_None)``. Validation stops at the
        first error, but any earlier validated items are still returned.
    """
    ...

def validate_raw_array_item_next(
    data: bytes,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
) -> tuple[Any | None, int, bool]:
    """Validate the next item in a JSON array.

    Returns:
        ``(validated_item_or_None, next_pos, finished)``.
    """
    ...

# ---------------------------------------------------------------------------
# Field filtering — require ObjectSpec
# ---------------------------------------------------------------------------

def project_object(data: bytes, spec: ObjectSpec) -> bytes:
    """Return a JSON object containing only the fields described by ``spec``."""
    ...

def project_array(data: bytes, spec: ObjectSpec) -> bytes:
    """Apply ``spec`` to each object in a JSON array and return the array."""
    ...

def project_array_items(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Return one filtered JSON object per item in a JSON array."""
    ...

def project_array_item_at(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
    index: int = 0,
) -> bytes | None:
    """Return one filtered array item by index.

    Args:
        prefix: Dot-separated path to the array, for example ``"data.items"``.
        index: Zero-based item index.

    Returns:
        Filtered JSON bytes, or ``None`` if ``index`` is out of range.
    """
    ...

def project_array_nav(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
) -> bytes:
    """Navigate to the array at ``prefix`` and apply ``spec`` to each item.

    Args:
        prefix: Dot-separated path to the array, for example ``"data.items"``.
    """
    ...

def project_array_items_sliced(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
) -> list[bytes]:
    """Return a filtered slice of array items.

    Args:
        prefix: Dot-separated path to the array, for example ``"data.items"``.
        start: Inclusive start index.
        stop: Exclusive stop index, or ``None`` for the end of the array.
        step: Step size.
    """
    ...

def project_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Filter all complete items currently available in a partial array buffer.

    Returns:
        ``(items, consumed, finished)``. After each call, the first
        ``consumed`` bytes can be discarded from the buffer.
    """
    ...

def project_jsonl(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Apply ``spec`` to each record in JSON Lines input."""
    ...

# ---------------------------------------------------------------------------
# Field filtering + validation — require ObjectSpec and a validator callable
# ---------------------------------------------------------------------------

def validate_array_items(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
) -> tuple[list[Any], BaseException | None]:
    """Filter and validate every item in a JSON array.

    ``validator`` is called for each filtered item, usually something like
    ``TypeAdapter.validate_json``.

    Returns:
        ``(validated_items, first_error_or_None)``.
    """
    ...

def validate_array_item_next(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
) -> tuple[Any | None, int, bool]:
    """Filter and validate the next item in a JSON array.

    Returns:
        ``(validated_item_or_None, next_pos, finished)``.
    """
    ...

def validate_array_items_next_batch(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
    batch_size: int = 8,
) -> tuple[list[Any], int, bool, BaseException | None]:
    """Filter and validate the next batch of items in a JSON array.

    Returns:
        ``(validated_items, next_pos, finished, first_error_or_None)``.
    """
    ...

def validate_array_items_batched(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    list_validator: Callable[[bytes], Any],
    batch_size: int = 16,
) -> tuple[list[Any], BaseException | None]:
    """Validate filtered items in batches.

    ``list_validator`` is tried on each batch first. If a batch fails,
    validation falls back to ``validator`` on individual items so callers can
    still receive successfully validated items before the first error.

    Returns:
        ``(validated_items, first_error_or_None)``.
    """
    ...

def validate_array_nav(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    prefix: str | None = None,
) -> Any:
    """Navigate to the array at ``prefix``, filter it, and validate it.

    Args:
        prefix: Dot-separated path to the array, for example ``"data.items"``.

    Returns:
        Whatever ``validator`` returns.
    """
    ...

def validate_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    is_start: bool = True,
) -> tuple[list[Any], int, bool, BaseException | None]:
    """Filter and validate all complete items in a partial array buffer.

    Returns:
        ``(validated_items, consumed, finished, first_error_or_None)``.
    """
    ...
