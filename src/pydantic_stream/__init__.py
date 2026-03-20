"""Stream and project JSON into Pydantic types in bounded memory.

Start here:
- ``StreamingBaseModelMixin`` for ``BaseModel`` classes
- ``StreamingDataclassMixin`` for Pydantic dataclasses
- ``StreamArray`` for lazy array access

Advanced helpers such as ``stream_projected_json_array_iter`` and the native
projection functions are also re-exported here.
"""

from ._native import (
    FieldSpec,
    ObjectSpec,
    StreamingProjectionError,
    extract_array_items,
    project_array,
    project_array_items,
    project_array_items_partial,
    project_array_items_sliced,
    project_array_nav,
    project_jsonl,
    project_object,
)
from ._schema import compile_model_spec, compile_object_spec
from ._streaming import (
    stream_json_array,
    stream_projected_json_array_aiter,
    stream_projected_json_array_iter,
)
from .base_model import StreamingBaseModelMixin
from .dataclass import StreamingDataclassMixin
from .stream_array import StreamArray

__all__ = [
    # Primary user-facing API
    "StreamingBaseModelMixin",
    "StreamingDataclassMixin",
    "StreamArray",
    "StreamingProjectionError",
    # High-level helpers
    "stream_json_array",
    "stream_projected_json_array_iter",
    "stream_projected_json_array_aiter",
    # Low-level spec helpers
    "compile_model_spec",
    "compile_object_spec",
    "FieldSpec",
    "ObjectSpec",
    # Native projection primitives
    "extract_array_items",
    "project_object",
    "project_array",
    "project_array_items",
    "project_array_items_sliced",
    "project_array_items_partial",
    "project_array_nav",
    "project_jsonl",
]


def __dir__() -> list[str]:
    """Return a focused top-level help surface for interactive use."""
    return sorted(__all__)
