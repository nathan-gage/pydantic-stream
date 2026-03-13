"""Smoke tests proving the shared streamable harness is ready for expansion."""

from __future__ import annotations

from hypothesis import given, settings

from .assertions import (
    assert_json_array_matches_direct,
    assert_jsonl_matches_direct,
    assert_single_matches_direct,
)
from .cases import StreamableCase
from .strategies import (
    alias_choice_payloads,
    populate_by_name_payloads,
    user_payload_lists,
)


def test_user_case_sample_payloads_match_direct_validation(user_case: StreamableCase) -> None:
    payloads = [
        {
            "id": 1,
            "name": "Ada",
            "address": {"city": "NYC", "zip": 10001, "junk": {"nested": [1, 2, 3]}},
            "tags": ["engineer"],
            "metadata": {"keep": True},
            "ignore_me": {"huge": [1, 2, 3]},
        },
        {
            "id": "2",
            "name": "Grace",
        },
    ]

    assert_json_array_matches_direct(user_case, payloads)
    assert_jsonl_matches_direct(user_case, payloads)


@given(payloads=user_payload_lists())
@settings(max_examples=25)
def test_user_case_property_array_matches_direct_validation(
    user_case: StreamableCase,
    payloads: list[dict[str, object]],
) -> None:
    assert_json_array_matches_direct(user_case, payloads)
    assert_jsonl_matches_direct(user_case, payloads)


@given(payload=alias_choice_payloads(include_unknown=True))
@settings(max_examples=25)
def test_alias_choice_case_property_matches_direct_validation(
    alias_choice_case: StreamableCase,
    payload: dict[str, object],
) -> None:
    assert_single_matches_direct(alias_choice_case, payload)


@given(payload=populate_by_name_payloads(include_unknown=True))
@settings(max_examples=25)
def test_populate_by_name_case_property_matches_direct_validation(
    populate_by_name_case: StreamableCase,
    payload: dict[str, object],
) -> None:
    assert_single_matches_direct(populate_by_name_case, payload)
