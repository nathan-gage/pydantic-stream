"""Unit tests for _schema.py schema compilation functions."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import BaseModel, TypeAdapter
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import rebuild_dataclass
from pydantic_stream import ObjectSpec, StreamingProjectionError
from pydantic_stream._schema import (
    _config_flags,
    compile_model_spec,
    compile_object_spec,
    input_keys_for_field,
    maybe_nested_object_spec,
    unwrap_schema,
)

# ---------------------------------------------------------------------------
# TestUnwrapSchema
# ---------------------------------------------------------------------------


class TestUnwrapSchema:
    def test_default_wrapper_peeled(self) -> None:
        inner = {"type": "int"}
        schema = {"type": "default", "schema": inner}
        assert unwrap_schema(schema) is inner

    def test_nullable_wrapper_peeled(self) -> None:
        inner = {"type": "str"}
        schema = {"type": "nullable", "schema": inner}
        assert unwrap_schema(schema) is inner

    def test_function_after_wrapper_peeled(self) -> None:
        inner = {"type": "int"}
        schema = {"type": "function-after", "schema": inner}
        assert unwrap_schema(schema) is inner

    def test_multiple_layers_peeled(self) -> None:
        innermost = {"type": "str"}
        schema = {
            "type": "default",
            "schema": {
                "type": "nullable",
                "schema": {
                    "type": "function-before",
                    "schema": innermost,
                },
            },
        }
        assert unwrap_schema(schema) is innermost

    def test_stops_at_model_type(self) -> None:
        # "model" is not in UNWRAP_TYPES, so it should not be peeled
        schema = {"type": "model", "schema": {"type": "model-fields", "fields": {}}}
        result = unwrap_schema(schema)
        assert result is schema

    def test_stops_when_no_inner_schema_key(self) -> None:
        schema = {"type": "default"}
        result = unwrap_schema(schema)
        assert result is schema


# ---------------------------------------------------------------------------
# TestConfigFlags
# ---------------------------------------------------------------------------


class TestConfigFlags:
    def test_empty_config_returns_defaults(self) -> None:
        schema: dict[str, Any] = {"type": "model", "config": {}}
        assert _config_flags(schema) == (True, False)

    def test_validate_by_alias_false_explicitly_set(self) -> None:
        schema: dict[str, Any] = {
            "type": "model",
            "config": {"validate_by_alias": False},
        }
        assert _config_flags(schema) == (False, False)

    def test_validate_by_name_true_explicitly_set(self) -> None:
        schema: dict[str, Any] = {
            "type": "model",
            "config": {"validate_by_name": True},
        }
        assert _config_flags(schema) == (True, True)

    def test_both_set(self) -> None:
        schema: dict[str, Any] = {
            "type": "model",
            "config": {"validate_by_alias": True, "validate_by_name": True},
        }
        assert _config_flags(schema) == (True, True)

    def test_none_config_returns_defaults(self) -> None:
        # No "config" key at all
        schema: dict[str, Any] = {"type": "model"}
        assert _config_flags(schema) == (True, False)


# ---------------------------------------------------------------------------
# TestInputKeysForField
# ---------------------------------------------------------------------------


class TestInputKeysForField:
    def test_no_alias_returns_name(self) -> None:
        field = {"name": "my_field", "schema": {"type": "int"}}
        assert input_keys_for_field(field) == ("my_field",)

    def test_str_alias_with_validate_by_alias_true(self) -> None:
        field = {"name": "my_field", "validation_alias": "myAlias", "schema": {"type": "int"}}
        assert input_keys_for_field(field, validate_by_alias=True) == ("myAlias",)

    def test_str_alias_with_validate_by_alias_false_falls_back_to_name(self) -> None:
        field = {"name": "my_field", "validation_alias": "myAlias", "schema": {"type": "int"}}
        result = input_keys_for_field(field, validate_by_alias=False)
        assert result == ("my_field",)

    def test_alias_choices_flat_all_extracted(self) -> None:
        # AliasChoices-style: list of single-element lists
        field = {
            "name": "record_id",
            "validation_alias": [["external_id"], ["legacy_id"]],
            "schema": {"type": "int"},
        }
        result = input_keys_for_field(field, validate_by_alias=True)
        assert result == ("external_id", "legacy_id")

    def test_alias_choices_with_nested_path_falls_back_to_name(self) -> None:
        # Multi-element inner list means path alias — not supported, falls back
        field = {
            "name": "record_id",
            "validation_alias": [["data", "external_id"]],
            "schema": {"type": "int"},
        }
        result = input_keys_for_field(field, validate_by_alias=True)
        assert result == ("record_id",)

    def test_validate_by_name_appends_name(self) -> None:
        field = {"name": "my_field", "validation_alias": "myAlias", "schema": {"type": "int"}}
        result = input_keys_for_field(field, validate_by_alias=True, validate_by_name=True)
        assert "myAlias" in result
        assert "my_field" in result

    def test_no_duplicate_if_name_already_in_aliases(self) -> None:
        # alias list contains the field name
        field = {
            "name": "my_field",
            "validation_alias": [["my_field"], ["alt_key"]],
            "schema": {"type": "int"},
        }
        result = input_keys_for_field(field, validate_by_alias=True, validate_by_name=True)
        assert result.count("my_field") == 1

    def test_empty_alias_list_falls_back_to_name(self) -> None:
        field = {"name": "my_field", "validation_alias": [], "schema": {"type": "int"}}
        result = input_keys_for_field(field, validate_by_alias=True)
        assert result == ("my_field",)


# ---------------------------------------------------------------------------
# TestMaybeNestedObjectSpec
# ---------------------------------------------------------------------------


class TestMaybeNestedObjectSpec:
    def test_model_type_returns_object_spec(self) -> None:
        class SimpleModel(BaseModel):
            x: int

        adapter = TypeAdapter(SimpleModel)
        schema = adapter.core_schema
        result = maybe_nested_object_spec(schema)
        assert isinstance(result, ObjectSpec)

    def test_dataclass_type_returns_object_spec(self) -> None:
        @pydantic_dataclass
        class SimpleDataclass:
            x: int

        rebuild_dataclass(SimpleDataclass)  # type: ignore[arg-type]
        adapter = TypeAdapter(SimpleDataclass)
        schema = adapter.core_schema
        result = maybe_nested_object_spec(schema)
        assert isinstance(result, ObjectSpec)

    def test_int_type_returns_none(self) -> None:
        schema = {"type": "int"}
        assert maybe_nested_object_spec(schema) is None

    def test_list_type_returns_none(self) -> None:
        schema = {"type": "list", "items_schema": {"type": "str"}}
        assert maybe_nested_object_spec(schema) is None

    def test_nullable_wrapping_model_returns_object_spec(self) -> None:
        class NestedModel(BaseModel):
            y: str

        adapter = TypeAdapter(NestedModel)
        inner_schema = adapter.core_schema
        wrapped = {"type": "nullable", "schema": inner_schema}
        result = maybe_nested_object_spec(wrapped)
        assert isinstance(result, ObjectSpec)

    def test_default_wrapping_dataclass_returns_object_spec(self) -> None:
        @pydantic_dataclass
        class WrappedDataclass:
            z: float

        rebuild_dataclass(WrappedDataclass)  # type: ignore[arg-type]
        adapter = TypeAdapter(WrappedDataclass)
        inner_schema = adapter.core_schema
        wrapped = {"type": "default", "schema": inner_schema}
        result = maybe_nested_object_spec(wrapped)
        assert isinstance(result, ObjectSpec)


# ---------------------------------------------------------------------------
# TestCompileSpecErrorPaths
# ---------------------------------------------------------------------------


class TestCompileSpecErrorPaths:
    def test_compile_model_spec_with_non_model_type_raises(self) -> None:
        schema = {"type": "dataclass", "schema": {"type": "dataclass-args", "fields": []}}
        with pytest.raises(StreamingProjectionError):
            compile_model_spec(schema)

    def test_compile_object_spec_with_non_dataclass_type_raises(self) -> None:
        schema = {"type": "model", "schema": {"type": "model-fields", "fields": {}}}
        with pytest.raises(StreamingProjectionError):
            compile_object_spec(schema)

    def test_compile_model_spec_unsupported_shape_raises(self) -> None:
        # model type but inner schema is not "model-fields"
        schema = {
            "type": "model",
            "schema": {"type": "tagged-union"},
            "config": {},
        }
        with pytest.raises(StreamingProjectionError):
            compile_model_spec(schema)
