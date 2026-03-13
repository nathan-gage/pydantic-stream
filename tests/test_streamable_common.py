"""Shared integration coverage for both StreamingBaseModelMixin and StreamingDataclassMixin."""

from __future__ import annotations

from datetime import datetime

import pytest
from pydantic_core import ValidationError

from pydantic_stream import StreamArray

from .cases import Priority, StreamableCase, json_bytes, json_source, jsonl_source

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


def test_single_object_validation_error_propagates_raw(user_case: StreamableCase) -> None:
    with pytest.raises(ValidationError):
        user_case.stream_validate_json(json_source({"id": "not-an-int", "name": "Ada"}))


def test_array_returns_stream_array_and_items_in_order(user_case: StreamableCase) -> None:
    payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]

    result = user_case.stream_validate_json_array(json_source(payload))

    assert isinstance(result, StreamArray)
    assert [item.id for item in result] == [1, 2]
    assert [item.name for item in result.to_list()] == ["Ada", "Grace"]


def test_jsonl_iter_yields_expected_items_in_order(user_case: StreamableCase) -> None:
    payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]

    result = user_case.stream_validate_jsonl_iter(jsonl_source(payload))

    first = next(result)
    second = next(result)

    assert first.id == 1
    assert first.name == "Ada"
    assert second.id == 2
    assert second.name == "Grace"
    with pytest.raises(StopIteration):
        next(result)


def test_rich_literal_field_accepts_valid_value(rich_case: StreamableCase) -> None:
    result = rich_case.stream_validate_json(json_source(RICH_PAYLOAD))
    assert result.status == "active"


def test_rich_literal_field_rejects_invalid_value(rich_case: StreamableCase) -> None:
    payload = {**RICH_PAYLOAD, "status": "garbage"}
    with pytest.raises(ValidationError):
        rich_case.stream_validate_json(json_source(payload))


def test_rich_enum_and_datetime_fields_round_trip(rich_case: StreamableCase) -> None:
    result = rich_case.stream_validate_json(json_source(RICH_FULL_PAYLOAD))
    assert result.priority == Priority.MEDIUM
    assert isinstance(result.created_at, datetime)


def test_rich_defaults_and_collection_fields_round_trip(rich_case: StreamableCase) -> None:
    result_default = rich_case.stream_validate_json(json_source(RICH_PAYLOAD))
    result_full = rich_case.stream_validate_json(json_source(RICH_FULL_PAYLOAD))

    assert result_default.score == 0.0
    assert result_default.label == "unnamed"
    assert result_full.extras == ("a", "b")
    assert result_full.unique_tags == frozenset({"x", "y"})


def test_jsonl_from_generator_source(user_case: StreamableCase) -> None:
    payload1 = {"id": 1, "name": "Ada"}
    payload2 = {"id": 2, "name": "Grace"}
    source = iter(
        [
            json_bytes(payload1).decode(),
            json_bytes(payload2).decode(),
        ]
    )

    results = user_case.stream_validate_jsonl(source)

    assert [item.id for item in results] == [1, 2]
    assert [item.name for item in results] == ["Ada", "Grace"]


def test_empty_json_array_returns_empty_stream_array(user_case: StreamableCase) -> None:
    result = user_case.stream_validate_json_array(json_source([]))
    assert list(result) == []


def test_empty_jsonl_returns_empty_list(user_case: StreamableCase) -> None:
    result = user_case.stream_validate_jsonl(jsonl_source([]))
    assert result == []
