"""Type stubs for the Rust ``_native`` extension module."""

from __future__ import annotations

class FieldSpec:
    """Low-level projection rule for one JSON field."""

    def __init__(self, output_key: str, nested: ObjectSpec | None = None) -> None: ...
    @property
    def output_key(self) -> str: ...
    def __repr__(self) -> str: ...

class ObjectSpec:
    """Low-level projection spec for one JSON object."""

    def __init__(self, fields_by_input_key: dict[str, FieldSpec]) -> None: ...
    def __repr__(self) -> str: ...
    def __len__(self) -> int: ...
    def __contains__(self, key: str) -> bool: ...

class ProjectedArrayBlobStreamer:
    """Incrementally project a top-level or prefixed JSON array."""

    def __init__(self, spec: ObjectSpec, prefix: str | None = None) -> None: ...
    def push(self, chunk: bytes) -> bytes | None: ...
    def finish(self) -> bytes | None: ...

class StreamingProjectionError(RuntimeError): ...

def extract_array_items(
    data: bytes,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Pull complete items from a possibly partial JSON array."""
    ...

def locate_array_start(
    data: bytes,
    prefix: str | None = None,
) -> int | None:
    """Return the byte offset of the ``[`` for a top-level or prefixed array."""
    ...

def project_array(data: bytes, spec: ObjectSpec) -> bytes:
    """Return a compact JSON array containing only fields declared in ``spec``."""
    ...

def project_array_items(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Return one projected JSON object per array item."""
    ...

def project_object(data: bytes, spec: ObjectSpec) -> bytes:
    """Return a compact JSON object containing only fields declared in ``spec``."""
    ...

def project_jsonl(data: bytes, spec: ObjectSpec) -> list[bytes]:
    """Project JSON Lines input and return one projected JSON byte string per line."""
    ...

def project_array_items_sliced(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
    start: int = 0,
    stop: int | None = None,
    step: int = 1,
) -> list[bytes]:
    """Project only the selected items from a top-level or prefixed array."""
    ...

def project_array_nav(
    data: bytes,
    spec: ObjectSpec,
    prefix: str | None = None,
) -> bytes:
    """Project a top-level or prefixed JSON array and return it as compact JSON."""
    ...

def project_array_items_partial(
    data: bytes,
    spec: ObjectSpec,
    is_start: bool = True,
) -> tuple[list[bytes], int, bool]:
    """Project one possibly partial array chunk into complete item bytes."""
    ...

def project_array_blob_partial(
    data: bytes,
    spec: ObjectSpec,
    is_start: bool = True,
) -> tuple[bytes, int, bool]:
    """Project one possibly partial array chunk into one array blob."""
    ...
