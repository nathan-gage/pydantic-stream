"""Integration tests for the StreamingBaseModelMixin API."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core import ValidationError
from pydantic_stream import StreamingBaseModelMixin

from .cases import (
    HarnessRichModel,
    HarnessUserModel,
    Priority,
    json_bytes,
    json_source,
    jsonl_source,
)

RICH_PAYLOAD = {"id": 1, "status": "active", "priority": 2}
RICH_FULL_PAYLOAD = {
    "id": 1,
    "status": "active",
    "priority": 2,
    "score": 3.14,
    "created_at": "2024-01-01T00:00:00Z",
    "label": "test",
    "extras": ["a", "b"],
    "unique_tags": ["x", "y"],
}


class TestSingleObjectValidationError:
    def test_single_object_validation_error_propagates_raw(self) -> None:
        """stream_model_validate_json raises ValidationError directly, not wrapped in ValueError."""
        with pytest.raises(ValidationError):
            HarnessUserModel.stream_model_validate_json(json_source({"id": "not-an-int", "name": "Ada"}))


class TestIteratorLaziness:
    def test_array_returns_stream_array(self) -> None:
        """stream_model_validate_json_array returns a StreamArray."""
        from pydantic_stream import StreamArray

        source = json_source([{"id": 1, "name": "Ada"}])
        result = HarnessUserModel.stream_model_validate_json_array(source)
        assert isinstance(result, StreamArray)
        assert hasattr(result, "__iter__")

    def test_jsonl_iter_is_lazy(self) -> None:
        """stream_model_validate_jsonl_iter returns an iterator without consuming it."""
        source = jsonl_source([{"id": 1, "name": "Ada"}])
        result = HarnessUserModel.stream_model_validate_jsonl_iter(source)
        assert hasattr(result, "__iter__") and hasattr(result, "__next__")


class TestRichModelFields:
    def test_literal_field_accepts_valid_value(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_PAYLOAD))
        assert result.status == "active"

    def test_literal_field_rejects_invalid_value(self) -> None:
        payload = {**RICH_PAYLOAD, "status": "garbage"}
        with pytest.raises(ValidationError):
            HarnessRichModel.stream_model_validate_json(json_source(payload))

    def test_enum_field_round_trips(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_PAYLOAD))
        assert result.priority == Priority.MEDIUM

    def test_datetime_field_coerced_from_string(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_FULL_PAYLOAD))
        assert isinstance(result.created_at, datetime)

    def test_static_default_used_when_field_missing(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_PAYLOAD))
        assert result.score == 0.0

    def test_optional_with_non_none_default(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_PAYLOAD))
        assert result.label == "unnamed"

    def test_tuple_field_round_trips(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_FULL_PAYLOAD))
        assert result.extras == ("a", "b")

    def test_frozenset_field_round_trips(self) -> None:
        result = HarnessRichModel.stream_model_validate_json(json_source(RICH_FULL_PAYLOAD))
        assert result.unique_tags == frozenset({"x", "y"})


class TestFrozenModel:
    def test_frozen_model_streams_correctly(self) -> None:
        class FrozenModel(StreamingBaseModelMixin):
            model_config = ConfigDict(frozen=True)
            x: int

        result = FrozenModel.stream_model_validate_json(json_source({"x": 1}))
        assert result.x == 1


class TestFieldValidator:
    def test_field_validator_applied_after_projection(self) -> None:
        class ValidatedModel(StreamingBaseModelMixin):
            name: str

            @field_validator("name")
            @classmethod
            def name_must_be_nonempty(cls, v: str) -> str:
                if not v:
                    raise ValueError("name must not be empty")
                return v.upper()

        result = ValidatedModel.stream_model_validate_json(json_source({"name": "ada"}))
        assert result.name == "ADA"

    def test_field_validator_rejects_invalid(self) -> None:
        class ValidatedModel(StreamingBaseModelMixin):
            name: str

            @field_validator("name")
            @classmethod
            def name_must_be_nonempty(cls, v: str) -> str:
                if not v:
                    raise ValueError("name must not be empty")
                return v.upper()

        with pytest.raises(ValidationError):
            ValidatedModel.stream_model_validate_json(json_source({"name": ""}))


class TestDiscriminatedUnion:
    def test_discriminated_union_captured_opaque(self) -> None:
        class CatModel(BaseModel):
            kind: Literal["cat"]
            meow: str

        class DogModel(BaseModel):
            kind: Literal["dog"]
            bark: str

        class PetOwnerModel(StreamingBaseModelMixin):
            name: str
            pet: CatModel | DogModel = Field(discriminator="kind")

        result = PetOwnerModel.stream_model_validate_json(
            json_source({"name": "Ada", "pet": {"kind": "cat", "meow": "loud"}})
        )
        assert result.pet.kind == "cat"
        assert result.pet.meow == "loud"  # type: ignore[union-attr]


class TestGeneratorJsonlSource:
    def test_jsonl_from_generator_source(self) -> None:
        payload1 = {"id": 1, "name": "Ada"}
        payload2 = {"id": 2, "name": "Grace"}
        source = iter(
            [
                json_bytes(payload1).decode(),
                json_bytes(payload2).decode(),
            ]
        )
        results = HarnessUserModel.stream_model_validate_jsonl(source)
        assert len(results) == 2
        assert results[0].id == 1
        assert results[1].id == 2


class TestEmptyInputs:
    def test_empty_json_array_returns_empty_list(self) -> None:
        result = HarnessUserModel.stream_model_validate_json_array(json_source([]))
        assert list(result) == []

    def test_empty_jsonl_returns_empty_list(self) -> None:
        result = HarnessUserModel.stream_model_validate_jsonl(jsonl_source([]))
        assert result == []


class TestDeepNesting:
    def test_three_level_nesting_projects_recursively(self) -> None:
        class InnerModel(StreamingBaseModelMixin):
            value: int

        class MiddleModel(StreamingBaseModelMixin):
            inner: InnerModel

        class OuterModel(StreamingBaseModelMixin):
            middle: MiddleModel

        payload = {"middle": {"inner": {"value": 42}, "noise": 1}, "noise": 2}
        result = OuterModel.stream_model_validate_json(json_source(payload))
        assert result.middle.inner.value == 42
