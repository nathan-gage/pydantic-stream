"""Rust-accelerated JSON streaming and projection for pydantic."""

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
    "extract_array_items",
    "project_array",
    "project_array_items",
    "project_array_items_partial",
    "project_array_items_sliced",
    "project_array_nav",
    "project_jsonl",
    "project_object",
    "stream_json_array",
    "stream_projected_json_array_aiter",
    "stream_projected_json_array_iter",
]
