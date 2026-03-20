"""Fast JSON validation and streaming helpers for Pydantic."""

from ._native import FieldSpec, ObjectSpec, StreamingProjectionError
from ._schema import compile_model_spec, compile_object_spec
from ._streaming import stream_json_array, stream_json_array_async
from .base_model import StreamableModel, StreamingBaseModelMixin
from .dataclass import Streamable, StreamingDataclassMixin
from .stream_array import StreamArray

__all__ = [
    "FieldSpec",
    "ObjectSpec",
    "StreamArray",
    "Streamable",
    "StreamableModel",
    "StreamingBaseModelMixin",
    "StreamingDataclassMixin",
    "StreamingProjectionError",
    "compile_model_spec",
    "compile_object_spec",
    "stream_json_array",
    "stream_json_array_async",
]
