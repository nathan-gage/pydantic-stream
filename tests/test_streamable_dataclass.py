"""Dataclass-specific integration tests for StreamingDataclassMixin."""

from __future__ import annotations

import pytest
from pydantic import ConfigDict, field_validator
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import rebuild_dataclass
from pydantic_core import ValidationError

from pydantic_stream import StreamingDataclassMixin

from .cases import json_source


class TestFrozenModel:
    def test_frozen_model_streams_correctly(self) -> None:
        @pydantic_dataclass(config=ConfigDict(frozen=True))
        class FrozenDataclass(StreamingDataclassMixin):
            x: int

        rebuild_dataclass(FrozenDataclass)  # type: ignore[arg-type]

        result = FrozenDataclass.stream_validate_json(json_source({"x": 1}))
        assert result.x == 1


class TestFieldValidator:
    def test_field_validator_applied_after_projection(self) -> None:
        @pydantic_dataclass
        class ValidatedDataclass(StreamingDataclassMixin):
            name: str

            @field_validator("name")
            @classmethod
            def name_must_be_nonempty(cls, value: str) -> str:
                if not value:
                    raise ValueError("name must not be empty")
                return value.upper()

        rebuild_dataclass(ValidatedDataclass)  # type: ignore[arg-type]

        result = ValidatedDataclass.stream_validate_json(json_source({"name": "ada"}))
        assert result.name == "ADA"

    def test_field_validator_rejects_invalid(self) -> None:
        @pydantic_dataclass
        class ValidatedDataclass(StreamingDataclassMixin):
            name: str

            @field_validator("name")
            @classmethod
            def name_must_be_nonempty(cls, value: str) -> str:
                if not value:
                    raise ValueError("name must not be empty")
                return value.upper()

        rebuild_dataclass(ValidatedDataclass)  # type: ignore[arg-type]

        with pytest.raises(ValidationError):
            ValidatedDataclass.stream_validate_json(json_source({"name": ""}))


class TestUnionField:
    def test_union_field_accepts_string(self) -> None:
        @pydantic_dataclass
        class UnionFieldDataclass(StreamingDataclassMixin):
            value: str | int

        rebuild_dataclass(UnionFieldDataclass)  # type: ignore[arg-type]

        result = UnionFieldDataclass.stream_validate_json(json_source({"value": "hello"}))
        assert result.value == "hello"

    def test_union_field_accepts_int(self) -> None:
        @pydantic_dataclass
        class UnionFieldDataclass(StreamingDataclassMixin):
            value: str | int

        rebuild_dataclass(UnionFieldDataclass)  # type: ignore[arg-type]

        result = UnionFieldDataclass.stream_validate_json(json_source({"value": 42}))
        assert result.value == 42


class TestDeepNesting:
    def test_three_level_nesting_projects_recursively(self) -> None:
        @pydantic_dataclass
        class InnerDC(StreamingDataclassMixin):
            value: int

        @pydantic_dataclass
        class MiddleDC(StreamingDataclassMixin):
            inner: InnerDC

        @pydantic_dataclass
        class OuterDC(StreamingDataclassMixin):
            middle: MiddleDC

        rebuild_dataclass(InnerDC)  # type: ignore[arg-type]
        rebuild_dataclass(MiddleDC)  # type: ignore[arg-type]
        rebuild_dataclass(OuterDC)  # type: ignore[arg-type]

        payload = {"middle": {"inner": {"value": 42}, "noise": 1}, "noise": 2}
        result = OuterDC.stream_validate_json(json_source(payload))
        assert result.middle.inner.value == 42
