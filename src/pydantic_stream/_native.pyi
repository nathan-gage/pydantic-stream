"""Type stubs for native helpers exposed by pydantic-stream."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

class FieldSpec:
    """Describes one field in an :class:`ObjectSpec`."""

    def __init__(self, output_key: str, nested: ObjectSpec | None = None) -> None: ...
    @property
    def output_key(self) -> str: ...
    def __repr__(self) -> str: ...

class ObjectSpec:
    """Maps input JSON keys to their :class:`FieldSpec` entries."""

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
    """Parse complete items from a full or partial JSON array buffer.

    Returns:
        ``(items, consumed, finished)`` — raw item bytes, bytes consumed from
        the front of ``data``, and whether the closing ``]`` was seen.
    """
    ...

def validate_raw_array_items(
    data: bytes,
    validator: Callable[[bytes], Any],
) -> tuple[list[Any], BaseException | None]:
    """Validate all items in a JSON array without field filtering.

    Returns:
        ``(validated_items, first_error_or_None)``.
    """
    ...

def validate_raw_array_item_next(
    data: bytes,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
) -> tuple[Any | None, int, bool]:
    """Validate the next item in a JSON array starting at ``pos``.

    Returns:
        ``(item_or_None, next_pos, finished)``.
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
    """Return a single filtered array item by index, or ``None`` if out of range.

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
        index: Zero-based item index.
    """
    ...

def project_array_nav(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
) -> bytes:
    """Filter each item in the array at ``prefix`` and return the array.

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
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
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
        start: Inclusive start index.
        stop: Exclusive stop index, or ``None`` for end of array.
        step: Step size.
    """
    ...

def project_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Filter complete items from a partial array buffer.

    Returns:
        ``(items, consumed, finished)``.
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
    """Filter and validate all items in a JSON array.

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
    """Filter and validate the next item in a JSON array starting at ``pos``.

    Returns:
        ``(item_or_None, next_pos, finished)``.
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
    """Filter and validate the next batch of items starting at ``pos``.

    Returns:
        ``(items, next_pos, finished, first_error_or_None)``.
    """
    ...

def validate_array_items_batched(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    list_validator: Callable[[bytes], Any],
    batch_size: int = 16,
) -> tuple[list[Any], BaseException | None]:
    """Filter all items then validate in batches. Falls back to per-item on batch failure.

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
    """Navigate to ``prefix``, filter, and validate the array.

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
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
