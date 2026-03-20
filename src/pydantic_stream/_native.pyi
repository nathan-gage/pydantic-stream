"""Type stubs for the Rust _native extension module."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

class FieldSpec:
    """Spec for a single projected field.

    Args:
        output_key: The key written to the projected JSON output.
        nested: Nested ObjectSpec for nested model/dataclass fields, or None.
    """

    def __init__(self, output_key: str, nested: ObjectSpec | None = None) -> None: ...
    @property
    def output_key(self) -> str: ...
    def __repr__(self) -> str: ...

class ObjectSpec:
    """Compiled projection spec mapping input JSON keys to FieldSpecs.

    Args:
        fields_by_input_key: Mapping from input JSON key to FieldSpec.
    """

    def __init__(self, fields_by_input_key: dict[str, FieldSpec]) -> None: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: str) -> bool: ...

class StreamingProjectionError(RuntimeError):
    """Raised for JSON parse errors or unsupported schema shapes."""

    ...

# ---------------------------------------------------------------------------
# Streaming — no projection, no spec required
# ---------------------------------------------------------------------------

def extract_array_items(
    data: bytes,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Extract complete JSON items from a (possibly incomplete) JSON array buffer.

    Returns:
        (items, consumed, finished) where *items* are raw JSON byte strings,
        *consumed* is the number of bytes that can be discarded from the
        front of *data*, and *finished* is True when the closing ``]`` was seen.
    """
    ...

def validate_raw_array_items(
    data: bytes,
    validator: Callable[[bytes], Any],
) -> tuple[list[Any], BaseException | None]:
    """Validate all items in a raw JSON array without projection.

    Returns:
        (validated_items, first_error_or_None). Stops after the first
        validation error; previously validated items are still returned.
    """
    ...

def validate_raw_array_item_next(
    data: bytes,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
) -> tuple[Any | None, int, bool]:
    """Validate the next raw array item starting at *pos* without projection.

    Returns:
        (validated_item_or_None, next_pos, finished).
    """
    ...

# ---------------------------------------------------------------------------
# Projection — require ObjectSpec
# ---------------------------------------------------------------------------

def project_object(data: bytes, spec: ObjectSpec) -> bytes:
    """Project a single JSON object, keeping only fields declared in *spec*."""
    ...

def project_array(data: bytes, spec: ObjectSpec) -> bytes:
    """Project a JSON array of objects, returning a compacted JSON array."""
    ...

def project_array_items(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Project a JSON array, returning one projected bytes object per item."""
    ...

def project_array_item_at(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
    index: int = 0,
) -> bytes | None:
    """Project the array item at *index*, navigating via *prefix* first.

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
        index: Zero-based item index.

    Returns:
        Projected JSON bytes, or None if *index* is out of range.
    """
    ...

def project_array_nav(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
) -> bytes:
    """Navigate to the array at *prefix* and return a projected JSON array.

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
    """Project a slice of array items after optional prefix navigation.

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.
        start: Inclusive start index (default 0).
        stop: Exclusive stop index, or None for end of array.
        step: Step size (default 1).
    """
    ...

def project_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Project complete items from a (possibly incomplete) JSON array chunk.

    Returns:
        (items, consumed, finished). Safe to discard the first *consumed*
        bytes from the buffer after each call.
    """
    ...

def project_jsonl(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Project each newline-delimited JSON object in *data*."""
    ...

# ---------------------------------------------------------------------------
# Projection + validation — require ObjectSpec and a validator callable
# ---------------------------------------------------------------------------

def validate_array_items(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
) -> tuple[list[Any], BaseException | None]:
    """Project and validate all items in a JSON array.

    Calls *validator* (e.g. ``TypeAdapter.validate_json``) inside Rust for
    each projected item, stopping after the first error.

    Returns:
        (validated_items, first_error_or_None).
    """
    ...

def validate_array_item_next(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    pos: int = 0,
    started: bool = False,
) -> tuple[Any | None, int, bool]:
    """Project and validate the next array item starting at *pos*.

    Returns:
        (validated_item_or_None, next_pos, finished).
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
    """Project and validate a batch of array items starting at *pos*.

    Returns:
        (validated_items, next_pos, finished, first_error_or_None).
    """
    ...

def validate_array_items_batched(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    list_validator: Callable[[bytes], Any],
    batch_size: int = 16,
) -> tuple[list[Any], BaseException | None]:
    """Project all items, then validate in batches for throughput.

    Tries to validate each batch as a JSON array using *list_validator*;
    falls back to per-item validation with *validator* on batch failure so
    "yield valid items until first error" semantics are preserved.

    Returns:
        (validated_items, first_error_or_None).
    """
    ...

def validate_array_nav(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    prefix: str | None = None,
) -> Any:
    """Navigate to the array at *prefix*, project it, and validate in one pass.

    Calls *validator* on the full projected array bytes (e.g.
    ``TypeAdapter[list[T]].validate_json``).

    Args:
        prefix: Dot-separated path to the array, e.g. ``"data.items"``.

    Returns:
        Whatever *validator* returns (typically a list of model instances).
    """
    ...

def validate_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    validator: Callable[[bytes], Any],
    is_start: bool = True,
) -> tuple[list[Any], int, bool, BaseException | None]:
    """Project and validate complete items from a partial JSON array chunk.

    Designed for streaming pipelines: call repeatedly with successive chunks,
    discarding the first *consumed* bytes from the buffer after each call.

    Returns:
        (validated_items, consumed, finished, first_error_or_None).
    """
    ...
