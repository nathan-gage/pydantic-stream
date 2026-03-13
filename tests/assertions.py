"""Shared assertions for streamable direct-vs-streaming parity tests."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from .cases import StreamableCase, json_bytes, json_source, jsonl_bytes, jsonl_source


def assert_single_matches_direct(case: StreamableCase, payload: Any) -> None:
    source_bytes = json_bytes(payload)
    expected = case.direct_validate_json(source_bytes)
    actual = case.stream_validate_json(json_source(payload))
    assert case.dump_python(actual) == case.dump_python(expected)


def assert_json_array_matches_direct(case: StreamableCase, payloads: Sequence[Any]) -> None:
    source_bytes = json_bytes(list(payloads))
    expected = case.direct_validate_json_array(source_bytes)
    stream_array = case.stream_validate_json_array(json_source(list(payloads)))
    iter_actual = list(stream_array)
    list_actual = stream_array.to_list()

    expected_dump = case.dump_python_many(expected)
    assert case.dump_python_many(iter_actual) == expected_dump
    assert case.dump_python_many(list_actual) == expected_dump


def assert_jsonl_matches_direct(case: StreamableCase, payloads: Sequence[Any]) -> None:
    source_bytes = jsonl_bytes(payloads)
    expected = case.direct_validate_jsonl(source_bytes)
    iter_actual = list(case.stream_validate_jsonl_iter(jsonl_source(payloads)))
    list_actual = case.stream_validate_jsonl(jsonl_source(payloads))

    expected_dump = case.dump_python_many(expected)
    assert case.dump_python_many(iter_actual) == expected_dump
    assert case.dump_python_many(list_actual) == expected_dump
