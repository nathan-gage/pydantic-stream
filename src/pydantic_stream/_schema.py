"""Schema compilation — builds Rust FieldSpec/ObjectSpec from pydantic core schemas."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ._native import FieldSpec, ObjectSpec, StreamingProjectionError

UNWRAP_TYPES = {
    "default",
    "nullable",
    "json-or-python",
    "lax-or-strict",
    "function-after",
    "function-before",
    "function-wrap",
    "function-plain",
}


def unwrap_schema(schema: Mapping[str, Any]) -> Mapping[str, Any]:
    """Peel off simple wrappers that do not affect key selection."""
    current = schema
    while current.get("type") in UNWRAP_TYPES and "schema" in current:
        current = current["schema"]
    return current


def input_keys_for_field(
    field_schema: Mapping[str, Any],
    *,
    validate_by_alias: bool = True,
    validate_by_name: bool = False,
) -> tuple[str, ...]:
    """Return flat input keys accepted for a field."""
    name = field_schema["name"]
    alias = field_schema.get("validation_alias")

    if alias is None:
        return (name,)

    def unsupported_alias_error() -> StreamingProjectionError:
        return StreamingProjectionError(
            f"Unsupported validation_alias for field {name!r}: "
            "nested alias paths are not supported by the streaming projector"
        )

    alias_keys: tuple[str, ...] = ()
    if validate_by_alias:
        if isinstance(alias, str):
            alias_keys = (alias,)
        elif isinstance(alias, list):
            if alias and all(isinstance(item, str) for item in alias):
                raise unsupported_alias_error()

            flat: list[str] = []
            for item in alias:
                if isinstance(item, list) and len(item) == 1 and isinstance(item[0], str):
                    flat.append(item[0])
                else:
                    raise unsupported_alias_error()
            alias_keys = tuple(flat)
        else:
            raise unsupported_alias_error()

    if validate_by_name and name not in alias_keys:
        return alias_keys + (name,)

    return alias_keys if alias_keys else (name,)


def maybe_nested_object_spec(schema: Mapping[str, Any]) -> ObjectSpec | None:
    """Recurse only for nested dataclass/model objects."""
    schema = unwrap_schema(schema)
    schema_type = schema.get("type")
    if schema_type == "dataclass":
        return compile_object_spec(schema)
    if schema_type == "model":
        return compile_model_spec(schema)
    return None


def _config_flags(schema: Mapping[str, Any]) -> tuple[bool, bool]:
    """Extract (validate_by_alias, validate_by_name) from a schema node."""
    config: Any = schema.get("config") or {}
    return bool(config.get("validate_by_alias", True)), bool(config.get("validate_by_name", False))


def _compile_field_spec(
    field_schema: Mapping[str, Any],
    *,
    validate_by_alias: bool = True,
    validate_by_name: bool = False,
) -> tuple[tuple[str, ...], FieldSpec]:
    """Compile a field schema, returning (input_keys, FieldSpec)."""
    keys = input_keys_for_field(
        field_schema,
        validate_by_alias=validate_by_alias,
        validate_by_name=validate_by_name,
    )
    value_schema = unwrap_schema(field_schema["schema"])
    nested = maybe_nested_object_spec(value_schema)
    output_key = keys[0]
    return keys, FieldSpec(output_key=output_key, nested=nested)


def compile_object_spec(dataclass_schema: Mapping[str, Any]) -> ObjectSpec:
    dataclass_schema = unwrap_schema(dataclass_schema)
    if dataclass_schema.get("type") != "dataclass":
        schema_type = dataclass_schema.get("type")
        raise StreamingProjectionError(f"Expected dataclass schema, got {schema_type!r}")

    validate_by_alias, validate_by_name = _config_flags(dataclass_schema)

    args_schema = unwrap_schema(dataclass_schema["schema"])
    if args_schema.get("type") != "dataclass-args":
        raise StreamingProjectionError("Unsupported dataclass schema shape")

    fields_by_input_key: dict[str, FieldSpec] = {}
    for field_schema in args_schema.get("fields", []):
        keys, spec = _compile_field_spec(
            field_schema,
            validate_by_alias=validate_by_alias,
            validate_by_name=validate_by_name,
        )
        for key in keys:
            fields_by_input_key[key] = spec

    return ObjectSpec(fields_by_input_key=fields_by_input_key)


def compile_model_spec(model_schema: Mapping[str, Any]) -> ObjectSpec:
    model_schema = unwrap_schema(model_schema)
    if model_schema.get("type") != "model":
        raise StreamingProjectionError(f"Expected model schema, got {model_schema.get('type')!r}")

    validate_by_alias, validate_by_name = _config_flags(model_schema)

    fields_schema = unwrap_schema(model_schema["schema"])
    if fields_schema.get("type") != "model-fields":
        raise StreamingProjectionError("Unsupported model schema shape")

    fields_by_input_key: dict[str, FieldSpec] = {}
    for field_name, field_schema in fields_schema.get("fields", {}).items():
        keys, spec = _compile_field_spec(
            {**field_schema, "name": field_name},
            validate_by_alias=validate_by_alias,
            validate_by_name=validate_by_name,
        )
        for key in keys:
            fields_by_input_key[key] = spec

    return ObjectSpec(fields_by_input_key=fields_by_input_key)
