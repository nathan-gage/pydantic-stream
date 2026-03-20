"""Tests for error paths, edge cases, and structural validation in the streaming engine."""

from __future__ import annotations

import json
from io import BytesIO, StringIO
from typing import Any

import pytest
from pydantic import AliasPath, ConfigDict, Field, model_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import rebuild_dataclass
from pydantic_core import ValidationError

from pydantic_stream import (
    FieldSpec,
    ObjectSpec,
    StreamingBaseModelMixin,
    StreamingDataclassMixin,
    StreamingProjectionError,
    project_array,
    project_jsonl,
    project_object,
)
from pydantic_stream.base_model import _to_bytes, _to_bytes_jsonl

from .cases import HarnessUserDataclass, HarnessUserModel, json_source, jsonl_source


def _simple_spec(*field_names: str) -> ObjectSpec:
    fields: dict[str, FieldSpec] = {}
    for name in field_names:
        spec = FieldSpec(output_key=name, nested=None)
        fields[name] = spec
    return ObjectSpec(fields_by_input_key=fields)


def _project(spec: ObjectSpec, source: bytes) -> dict[str, Any]:
    """Project a single object and parse the resulting JSON bytes."""
    return json.loads(project_object(source, spec))


# ---------------------------------------------------------------------------
# project_object error paths
# ---------------------------------------------------------------------------


class TestProjectObjectErrors:
    def test_top_level_array_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"[1, 2, 3]", spec)

    def test_top_level_string_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b'"hello"', spec)

    def test_top_level_number_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"42", spec)

    def test_top_level_boolean_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"true", spec)


# ---------------------------------------------------------------------------
# Validation error behavior in array/jsonl iterators
# ---------------------------------------------------------------------------


class TestValidationErrorBehavior:
    def test_array_iter_propagates_field_validation_error_basemodel(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": "not-an-int", "name": "Grace"}]
        source = json_source(payload)
        stream_array = HarnessUserModel.stream_model_validate_json_array(source)
        it = iter(stream_array)
        first = next(it)
        assert first.id == 1
        with pytest.raises(ValidationError) as exc_info:
            next(it)
        assert exc_info.value.errors(include_url=False)[0]["loc"] == ("id",)
        assert exc_info.value.errors(include_url=False)[0]["type"] == "int_parsing"

    def test_array_iter_propagates_field_validation_error_dataclass(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": "not-an-int", "name": "Grace"}]
        source = json_source(payload)
        stream_array = HarnessUserDataclass.stream_validate_json_array(source)
        it = iter(stream_array)
        first = next(it)
        assert first.id == 1
        with pytest.raises(ValidationError) as exc_info:
            next(it)
        assert exc_info.value.errors(include_url=False)[0]["loc"] == ("id",)
        assert exc_info.value.errors(include_url=False)[0]["type"] == "int_parsing"

    def test_jsonl_iter_wraps_validation_error_basemodel(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": "not-an-int", "name": "Grace"}]
        source = jsonl_source(payload)
        it = HarnessUserModel.stream_model_validate_jsonl_iter(source)
        first = next(it)
        assert first.id == 1
        with pytest.raises(ValueError, match="Validation failed for item 1"):
            next(it)

    def test_jsonl_iter_wraps_validation_error_dataclass(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": "not-an-int", "name": "Grace"}]
        source = jsonl_source(payload)
        it = HarnessUserDataclass.stream_validate_jsonl_iter(source)
        first = next(it)
        assert first.id == 1
        with pytest.raises(ValueError, match="Validation failed for item 1"):
            next(it)

    def test_first_array_item_validation_error_points_to_the_invalid_field(self) -> None:
        payload = [{"id": "bad", "name": "Ada"}]
        source = json_source(payload)
        stream_array = HarnessUserModel.stream_model_validate_json_array(source)
        with pytest.raises(ValidationError) as exc_info:
            list(stream_array)
        assert exc_info.value.errors(include_url=False)[0]["loc"] == ("id",)
        assert exc_info.value.errors(include_url=False)[0]["type"] == "int_parsing"


# ---------------------------------------------------------------------------
# Caching behavior
# ---------------------------------------------------------------------------


class TestSpecCaching:
    def test_basemodel_spec_is_cached(self) -> None:
        class CachingModel(StreamingBaseModelMixin):
            x: int

        spec1 = CachingModel._streaming_spec()
        spec2 = CachingModel._streaming_spec()
        assert spec1 is spec2

    def test_basemodel_adapter_is_cached(self) -> None:
        class CachingModel(StreamingBaseModelMixin):
            x: int

        adapter1 = CachingModel._streaming_adapter()
        adapter2 = CachingModel._streaming_adapter()
        assert adapter1 is adapter2

    def test_dataclass_spec_is_cached(self) -> None:
        @pydantic_dataclass
        class CachingDC(StreamingDataclassMixin):
            x: int

        rebuild_dataclass(CachingDC)  # type: ignore[arg-type]

        spec1 = CachingDC._streaming_spec()
        spec2 = CachingDC._streaming_spec()
        assert spec1 is spec2

    def test_subclass_has_own_cache(self) -> None:
        class ParentModel(StreamingBaseModelMixin):
            x: int

        class ChildModel(ParentModel):
            y: int = 0

        parent_spec = ParentModel._streaming_spec()
        child_spec = ChildModel._streaming_spec()
        # Child should have its own spec with both fields
        assert "x" in child_spec
        assert "y" in child_spec
        assert "y" not in parent_spec


# ---------------------------------------------------------------------------
# Unsupported alias-path schemas
# ---------------------------------------------------------------------------


class TestUnsupportedAliasPaths:
    def test_basemodel_alias_path_raises_during_spec_compilation(self) -> None:
        class AliasPathModel(StreamingBaseModelMixin):
            record_id: int = Field(validation_alias=AliasPath("data", "id"))

        with pytest.raises(StreamingProjectionError, match="nested alias paths"):
            AliasPathModel._streaming_spec()

    def test_dataclass_alias_path_raises_during_spec_compilation(self) -> None:
        @pydantic_dataclass
        class AliasPathDataclass(StreamingDataclassMixin):
            record_id: int = Field(validation_alias=AliasPath("data", "id"))

        rebuild_dataclass(AliasPathDataclass)  # type: ignore[arg-type]

        with pytest.raises(StreamingProjectionError, match="nested alias paths"):
            AliasPathDataclass._streaming_spec()


# ---------------------------------------------------------------------------
# Unicode and special characters in values
# ---------------------------------------------------------------------------


class TestUnicodeValues:
    def test_unicode_string_value_preserved(self) -> None:
        spec = _simple_spec("x")
        payload = {"x": "caf\u00e9 \u2603 \U0001f600"}
        source = json.dumps(payload, ensure_ascii=False).encode()
        result = _project(spec, source)
        assert result["x"] == "caf\u00e9 \u2603 \U0001f600"

    def test_escaped_newline_in_json_string(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x":"line1\\nline2"}'
        result = _project(spec, source)
        assert result["x"] == "line1\nline2"

    def test_empty_string_value(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x":""}'
        result = _project(spec, source)
        assert result["x"] == ""


# ---------------------------------------------------------------------------
# Edge cases in projection
# ---------------------------------------------------------------------------


class TestProjectionEdgeCases:
    def test_all_fields_unknown_returns_empty_dict(self) -> None:
        spec = _simple_spec("x")
        source = b'{"a":1,"b":2,"c":3}'
        result = _project(spec, source)
        assert result == {}

    def test_empty_object_returns_empty_dict(self) -> None:
        spec = _simple_spec("x")
        source = b"{}"
        result = _project(spec, source)
        assert result == {}

    def test_duplicate_keys_last_wins(self) -> None:
        """JSON with duplicate keys — the last value wins."""
        spec = _simple_spec("x")
        source = b'{"x":1,"x":2}'
        result = _project(spec, source)
        assert result["x"] == 2

    def test_null_field_value_projected(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x":null}'
        result = _project(spec, source)
        assert result == {"x": None}

    def test_deeply_nested_unknown_field_skipped(self) -> None:
        spec = _simple_spec("x")
        deep = {"a": {"b": {"c": {"d": [1, 2, [3, {"e": 4}]]}}}}
        payload = {"x": 1, **deep}
        source = json.dumps(payload).encode()
        result = _project(spec, source)
        assert result == {"x": 1}

    def test_large_number_of_unknown_fields(self) -> None:
        spec = _simple_spec("target")
        payload: dict[str, Any] = {f"noise_{i}": {"nested": [i, i + 1]} for i in range(50)}
        payload["target"] = "found"
        source = json.dumps(payload).encode()
        result = _project(spec, source)
        assert result == {"target": "found"}

    def test_nested_projection_skips_unknown_in_nested(self) -> None:
        """Nested objects have their unknown fields stripped too."""
        nested_spec = _simple_spec("a")
        nested_field = FieldSpec(output_key="inner", nested=nested_spec)
        spec = ObjectSpec(fields_by_input_key={"inner": nested_field})

        source = b'{"inner":{"a":1,"b":2,"c":3}}'
        result = _project(spec, source)
        assert result == {"inner": {"a": 1}}

    def test_nested_null_for_nested_spec_captured_opaque(self) -> None:
        """When a field has a nested spec but the JSON value is null, it should be captured."""
        nested_spec = _simple_spec("a")
        nested_field = FieldSpec(output_key="inner", nested=nested_spec)
        spec = ObjectSpec(fields_by_input_key={"inner": nested_field})

        source = b'{"inner":null}'
        result = _project(spec, source)
        assert result == {"inner": None}


# ---------------------------------------------------------------------------
# model_validator (pre/post) integration
# ---------------------------------------------------------------------------


class TestModelValidator:
    def test_model_validator_after_applied(self) -> None:
        class ModelWithAfterValidator(StreamingBaseModelMixin):
            x: int
            y: int

            @model_validator(mode="after")
            def check_sum(self) -> ModelWithAfterValidator:
                if self.x + self.y > 100:
                    raise ValueError("sum too large")
                return self

        result = ModelWithAfterValidator.stream_model_validate_json(json_source({"x": 10, "y": 20}))
        assert result.x == 10
        assert result.y == 20

    def test_model_validator_after_rejects(self) -> None:
        class ModelWithAfterValidator(StreamingBaseModelMixin):
            x: int
            y: int

            @model_validator(mode="after")
            def check_sum(self) -> ModelWithAfterValidator:
                if self.x + self.y > 100:
                    raise ValueError("sum too large")
                return self

        with pytest.raises(ValidationError):
            ModelWithAfterValidator.stream_model_validate_json(json_source({"x": 50, "y": 60}))


# ---------------------------------------------------------------------------
# Extra fields config
# ---------------------------------------------------------------------------


class TestExtraFieldsConfig:
    def test_extra_allow_with_projection_does_not_pass_extras(self) -> None:
        """Streaming projection strips unknowns BEFORE Pydantic sees them,
        so even with extra='allow', extras won't appear."""

        class ExtraModel(StreamingBaseModelMixin):
            model_config = ConfigDict(extra="allow")
            x: int

        result = ExtraModel.stream_model_validate_json(json_source({"x": 1, "bonus": 99}))
        assert result.x == 1
        # The projection stripped 'bonus' before Pydantic validation
        assert not hasattr(result, "bonus") or result.model_extra == {}

    def test_extra_forbid_still_works(self) -> None:
        """With extra='forbid', the projection strips unknowns, so validation should pass."""

        class StrictModel(StreamingBaseModelMixin):
            model_config = ConfigDict(extra="forbid")
            x: int

        result = StrictModel.stream_model_validate_json(json_source({"x": 1, "unknown": 2}))
        assert result.x == 1


# ---------------------------------------------------------------------------
# Source type edge cases
# ---------------------------------------------------------------------------


class TestSourceTypes:
    def test_str_source_for_single_object(self) -> None:
        result = HarnessUserModel.stream_model_validate_json('{"id":1,"name":"Ada"}')
        assert result.id == 1

    def test_bytes_source_for_single_object(self) -> None:
        result = HarnessUserModel.stream_model_validate_json(b'{"id":1,"name":"Ada"}')
        assert result.id == 1

    def test_bytearray_source_for_single_object(self) -> None:
        result = HarnessUserModel.stream_model_validate_json(bytearray(b'{"id":1,"name":"Ada"}'))
        assert result.id == 1

    def test_memoryview_source_for_single_object(self) -> None:
        result = HarnessUserModel.stream_model_validate_json(memoryview(b'{"id":1,"name":"Ada"}'))
        assert result.id == 1

    def test_str_source_for_array(self) -> None:
        result = HarnessUserModel.stream_model_validate_json_array('[{"id":1,"name":"Ada"}]')
        assert len(list(result)) == 1

    def test_bytes_source_for_array(self) -> None:
        result = HarnessUserModel.stream_model_validate_json_array(b'[{"id":1,"name":"Ada"}]')
        assert len(list(result)) == 1

    def test_bytearray_source_for_array(self) -> None:
        result = HarnessUserModel.stream_model_validate_json_array(bytearray(b'[{"id":1,"name":"Ada"}]'))
        assert len(list(result)) == 1

    def test_memoryview_source_for_array(self) -> None:
        result = HarnessUserModel.stream_model_validate_json_array(memoryview(b'[{"id":1,"name":"Ada"}]'))
        assert len(list(result)) == 1

    def test_str_source_for_jsonl(self) -> None:
        result = HarnessUserModel.stream_model_validate_jsonl(
            '{"id":1,"name":"Ada"}\n{"id":2,"name":"Grace"}'
        )
        assert len(result) == 2

    def test_bytes_source_for_jsonl(self) -> None:
        result = HarnessUserModel.stream_model_validate_jsonl(
            b'{"id":1,"name":"Ada"}\n{"id":2,"name":"Grace"}'
        )
        assert len(result) == 2

    def test_bytearray_source_for_jsonl(self) -> None:
        result = HarnessUserModel.stream_model_validate_jsonl(
            bytearray(b'{"id":1,"name":"Ada"}\n{"id":2,"name":"Grace"}')
        )
        assert len(result) == 2

    def test_memoryview_source_for_jsonl(self) -> None:
        result = HarnessUserModel.stream_model_validate_jsonl(
            memoryview(b'{"id":1,"name":"Ada"}\n{"id":2,"name":"Grace"}')
        )
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Number handling
# ---------------------------------------------------------------------------


class TestNumberHandling:
    def test_float_preserved_not_decimal(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x": 3.14}'
        result = _project(spec, source)
        assert isinstance(result["x"], float)
        assert result["x"] == pytest.approx(3.14)

    def test_integer_preserved(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x": 42}'
        result = _project(spec, source)
        assert isinstance(result["x"], int)

    def test_negative_float(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x": -1.5}'
        result = _project(spec, source)
        assert result["x"] == -1.5

    def test_zero_float(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x": 0.0}'
        result = _project(spec, source)
        assert isinstance(result["x"], float)

    def test_scientific_notation(self) -> None:
        spec = _simple_spec("x")
        source = b'{"x": 1.5e10}'
        result = _project(spec, source)
        assert result["x"] == 1.5e10


# ---------------------------------------------------------------------------
# Truncated JSON input
# ---------------------------------------------------------------------------


class TestTruncatedInput:
    TRUNCATED_INPUTS: list[tuple[str, bytes]] = [
        ("truncated after colon", b'{"x":'),
        ("truncated nested object", b'{"x": {"a":'),
        ("truncated array", b'[{"x":1'),
        ("open brace only", b"{"),
        ("open bracket only", b"["),
        ("truncated mid-array", b'[{"x":1},'),
        ("truncated string", b'{"x": "hello'),
        ("truncated nested array", b'{"x": [1, 2'),
        ("truncated deep nesting", b'{"a": {"b": {"c":'),
    ]

    @pytest.mark.parametrize("label,data", TRUNCATED_INPUTS, ids=[t[0] for t in TRUNCATED_INPUTS])
    def test_truncated_input_raises(self, label: str, data: bytes) -> None:
        """Truncated inputs must raise, not return partial results."""
        spec = _simple_spec("x", "a", "b", "c")
        with pytest.raises(StreamingProjectionError):
            project_object(data, spec)


# ---------------------------------------------------------------------------
# project_object extended error paths
# ---------------------------------------------------------------------------


class TestProjectObjectErrorsExtended:
    def test_empty_input_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"", spec)

    def test_whitespace_only_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"   ", spec)

    def test_null_top_level_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b"null", spec)

    def test_trailing_content_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_object(b'{"x":1}{"y":2}', spec)


# ---------------------------------------------------------------------------
# project_array error paths
# ---------------------------------------------------------------------------


class TestProjectArrayErrors:
    def test_non_array_input_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_array(b'{"x":1}', spec)

    def test_non_object_first_item_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_array(b"[1,2]", spec)

    def test_non_object_mid_array_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_array(b'[{"x":1},null]', spec)

    def test_truncated_array_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_array(b'[{"x":1}', spec)

    def test_trailing_content_raises(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError):
            project_array(b'[{"x":1}]garbage', spec)


# ---------------------------------------------------------------------------
# project_jsonl error paths
# ---------------------------------------------------------------------------


class TestProjectJsonlErrors:
    def test_non_object_line_raises_with_line_number(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError, match="line 1"):
            project_jsonl(b"42\n", spec)

    def test_non_object_second_line_raises_with_line_2(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError, match="line 2"):
            project_jsonl(b'{"x":1}\n42\n', spec)

    def test_truncated_line_raises_with_line_number(self) -> None:
        spec = _simple_spec("x")
        with pytest.raises(StreamingProjectionError, match="line 1"):
            project_jsonl(b'{"x":\n', spec)

    def test_empty_input_returns_empty_list(self) -> None:
        spec = _simple_spec("x")
        result = project_jsonl(b"", spec)
        assert result == []

    def test_blank_lines_skipped(self) -> None:
        spec = _simple_spec("x")
        result = project_jsonl(b'\n\n{"x":1}\n  \n{"x":2}\n', spec)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# Source normalization helpers
# ---------------------------------------------------------------------------


class TestSourceNormalization:
    # _to_bytes tests
    def test_to_bytes_accepts_bytes(self) -> None:
        assert _to_bytes(b"hello") == b"hello"

    def test_to_bytes_accepts_str(self) -> None:
        assert _to_bytes("hello") == b"hello"

    def test_to_bytes_accepts_bytearray(self) -> None:
        assert _to_bytes(bytearray(b"hello")) == b"hello"

    def test_to_bytes_accepts_memoryview(self) -> None:
        assert _to_bytes(memoryview(b"hello")) == b"hello"

    def test_to_bytes_accepts_bytesio(self) -> None:
        assert _to_bytes(BytesIO(b"hello")) == b"hello"

    def test_to_bytes_accepts_stringio(self) -> None:
        assert _to_bytes(StringIO("hello")) == b"hello"

    def test_to_bytes_accepts_callable_returning_bytes(self) -> None:
        assert _to_bytes(lambda: b"hello") == b"hello"

    def test_to_bytes_accepts_callable_returning_str(self) -> None:
        assert _to_bytes(lambda: "hello") == b"hello"

    def test_to_bytes_callable_returning_callable_raises(self) -> None:
        """Callable that returns another callable should raise, not recurse infinitely."""

        def self_returning() -> Any:
            return self_returning

        with pytest.raises(StreamingProjectionError, match="[Cc]allable"):
            _to_bytes(self_returning)

    def test_to_bytes_callable_returning_another_callable_raises(self) -> None:
        """Two callables that bounce between each other should raise, not recurse infinitely."""

        def ping() -> Any:
            return pong

        def pong() -> Any:
            return ping

        with pytest.raises(StreamingProjectionError, match="[Cc]allable"):
            _to_bytes(ping)

    def test_to_bytes_rejects_int(self) -> None:
        with pytest.raises(StreamingProjectionError):
            _to_bytes(42)

    def test_to_bytes_rejects_list(self) -> None:
        with pytest.raises(StreamingProjectionError):
            _to_bytes([1, 2, 3])

    # _to_bytes_jsonl tests
    def test_to_bytes_jsonl_accepts_str_iterator(self) -> None:
        result = _to_bytes_jsonl(iter(['{"x":1}', '{"x":2}']))
        assert result == b'{"x":1}\n{"x":2}'

    def test_to_bytes_jsonl_accepts_bytes_iterator(self) -> None:
        result = _to_bytes_jsonl(iter([b'{"x":1}', b'{"x":2}']))
        assert result == b'{"x":1}\n{"x":2}'

    def test_to_bytes_jsonl_accepts_bytearray_iterator(self) -> None:
        result = _to_bytes_jsonl(iter([bytearray(b'{"x":1}')]))
        assert result == b'{"x":1}'

    def test_to_bytes_jsonl_accepts_memoryview_iterator(self) -> None:
        result = _to_bytes_jsonl(iter([memoryview(b'{"x":1}')]))
        assert result == b'{"x":1}'

    def test_to_bytes_jsonl_rejects_int_iterator(self) -> None:
        with pytest.raises(StreamingProjectionError):
            _to_bytes_jsonl(iter([42]))

    def test_to_bytes_jsonl_falls_through_to_to_bytes_for_str(self) -> None:
        """str is handled by _to_bytes path, not iterable path."""
        result = _to_bytes_jsonl('{"x":1}\n{"x":2}')
        assert result == b'{"x":1}\n{"x":2}'
