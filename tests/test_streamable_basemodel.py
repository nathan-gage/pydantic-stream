"""BaseModel-specific integration tests for StreamingBaseModelMixin."""

from __future__ import annotations

from typing import Literal

import pytest
from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic_core import ValidationError

from pydantic_stream import StreamingBaseModelMixin

from .cases import json_source


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
            def name_must_be_nonempty(cls, value: str) -> str:
                if not value:
                    raise ValueError("name must not be empty")
                return value.upper()

        result = ValidatedModel.stream_model_validate_json(json_source({"name": "ada"}))
        assert result.name == "ADA"

    def test_field_validator_rejects_invalid(self) -> None:
        class ValidatedModel(StreamingBaseModelMixin):
            name: str

            @field_validator("name")
            @classmethod
            def name_must_be_nonempty(cls, value: str) -> str:
                if not value:
                    raise ValueError("name must not be empty")
                return value.upper()

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
