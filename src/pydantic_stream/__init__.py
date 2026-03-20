"""Stream and project JSON into Pydantic types in bounded memory.

Start here:
- ``StreamingBaseModelMixin`` for ``BaseModel`` classes
- ``StreamingDataclassMixin`` for Pydantic dataclasses
- ``StreamArray`` for lazy array access

Advanced helpers such as ``stream_projected_json_array_iter`` are also
available for non-model consumers.
"""

from ._native import FieldSpec, ObjectSpec, StreamingProjectionError
from ._schema import compile_model_spec, compile_object_spec
from ._streaming import (
    stream_json_array,
    stream_json_array_async,
    stream_projected_json_array_aiter,
    stream_projected_json_array_iter,
)
from .base_model import StreamingBaseModelMixin
from .dataclass import StreamingDataclassMixin
from .stream_array import StreamArray

__all__ = [
    "StreamingBaseModelMixin",
    "StreamingDataclassMixin",
    "StreamArray",
    "StreamingProjectionError",
    "stream_json_array",
    "stream_json_array_async",
    "stream_projected_json_array_iter",
    "stream_projected_json_array_aiter",
    "compile_model_spec",
    "compile_object_spec",
    "FieldSpec",
    "ObjectSpec",
]


def __dir__() -> list[str]:
    """Return a focused top-level help surface for interactive use."""
    return sorted(__all__)
