"""Hypothesis strategies shared across streamable tests."""

from __future__ import annotations

from collections.abc import Collection
from typing import Any

from hypothesis import strategies as st

RESERVED_USER_KEYS = frozenset({"id", "name", "address", "tags", "metadata"})
RESERVED_ADDRESS_KEYS = frozenset({"city", "zip", "zip_code"})
RESERVED_ALIAS_CHOICE_KEYS = frozenset({"label", "external_id", "legacy_id"})
RESERVED_RICH_KEYS = frozenset(
    {
        "id",
        "status",
        "priority",
        "score",
        "created_at",
        "label",
        "extras",
        "unique_tags",
    }
)


def identifier_keys(forbidden: Collection[str] = ()) -> st.SearchStrategy[str]:
    reserved = frozenset(forbidden)
    return st.from_regex(r"[a-z_][a-z0-9_]{0,10}", fullmatch=True).filter(lambda key: key not in reserved)


def json_scalars() -> st.SearchStrategy[Any]:
    return st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-10_000, max_value=10_000),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
        st.text(max_size=24),
    )


def json_values() -> st.SearchStrategy[Any]:
    return st.recursive(
        json_scalars(),
        lambda children: st.one_of(
            st.lists(children, max_size=4),
            st.dictionaries(identifier_keys(), children, max_size=4),
        ),
        max_leaves=12,
    )


def json_objects(forbidden: Collection[str] = ()) -> st.SearchStrategy[dict[str, Any]]:
    return st.dictionaries(identifier_keys(forbidden), json_values(), max_size=4)


@st.composite
def address_payloads(
    draw: st.DrawFn,
    *,
    include_unknown: bool = False,
    allow_field_name: bool = False,
) -> dict[str, Any]:
    payload = {
        "city": draw(st.text(min_size=1, max_size=20)),
        draw(st.sampled_from(["zip", "zip_code"] if allow_field_name else ["zip"])): draw(
            st.integers(min_value=0, max_value=99_999)
        ),
    }
    if include_unknown:
        payload.update(draw(json_objects(payload.keys() | RESERVED_ADDRESS_KEYS)))
    return payload


@st.composite
def user_payloads(
    draw: st.DrawFn,
    *,
    include_unknown: bool = True,
    allow_address_by_name: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": draw(st.integers(min_value=-10_000, max_value=10_000)),
        "name": draw(st.text(min_size=1, max_size=30)),
    }

    if draw(st.booleans()):
        payload["address"] = draw(
            st.one_of(
                st.none(),
                address_payloads(include_unknown=include_unknown, allow_field_name=allow_address_by_name),
            )
        )

    if draw(st.booleans()):
        payload["tags"] = draw(st.lists(st.text(min_size=1, max_size=12), max_size=4))

    if draw(st.booleans()):
        payload["metadata"] = draw(json_objects())

    if include_unknown:
        payload.update(draw(json_objects(payload.keys() | RESERVED_USER_KEYS)))

    return payload


def user_payload_lists(
    *,
    include_unknown: bool = True,
    allow_address_by_name: bool = False,
) -> st.SearchStrategy[list[dict[str, Any]]]:
    return st.lists(
        user_payloads(include_unknown=include_unknown, allow_address_by_name=allow_address_by_name),
        max_size=6,
    )


@st.composite
def alias_choice_payloads(
    draw: st.DrawFn,
    *,
    include_unknown: bool = False,
) -> dict[str, Any]:
    payload = {
        "label": draw(st.text(min_size=1, max_size=24)),
        draw(st.sampled_from(["external_id", "legacy_id"])): draw(st.integers(min_value=0, max_value=10_000)),
    }
    if include_unknown:
        payload.update(draw(json_objects(payload.keys() | RESERVED_ALIAS_CHOICE_KEYS)))
    return payload


@st.composite
def populate_by_name_payloads(
    draw: st.DrawFn,
    *,
    include_unknown: bool = False,
) -> dict[str, Any]:
    payload = {
        "city": draw(st.text(min_size=1, max_size=20)),
        draw(st.sampled_from(["zip", "zip_code"])): draw(st.integers(min_value=0, max_value=99_999)),
    }
    if include_unknown:
        payload.update(draw(json_objects(payload.keys() | RESERVED_ADDRESS_KEYS)))
    return payload


@st.composite
def rich_model_payloads(
    draw: st.DrawFn,
    *,
    include_unknown: bool = False,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "id": draw(st.integers(min_value=-10_000, max_value=10_000)),
        "status": draw(st.sampled_from(["active", "inactive"])),
        "priority": draw(st.sampled_from([1, 2, 3])),
    }

    if draw(st.booleans()):
        payload["score"] = draw(st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False))

    if draw(st.booleans()):
        payload["created_at"] = draw(
            st.sampled_from(
                [
                    "2024-01-01T00:00:00Z",
                    "2025-06-15T12:30:00+00:00",
                    None,
                ]
            )
        )

    if draw(st.booleans()):
        payload["label"] = draw(st.one_of(st.none(), st.text(min_size=1, max_size=20)))

    if draw(st.booleans()):
        payload["extras"] = draw(st.lists(st.text(min_size=1, max_size=10), max_size=4))

    if draw(st.booleans()):
        payload["unique_tags"] = draw(st.lists(st.text(min_size=1, max_size=10), max_size=4))

    if include_unknown:
        payload.update(draw(json_objects(payload.keys() | RESERVED_RICH_KEYS)))

    return payload


def rich_model_payload_lists(
    *,
    include_unknown: bool = False,
) -> st.SearchStrategy[list[dict[str, Any]]]:
    return st.lists(
        rich_model_payloads(include_unknown=include_unknown),
        max_size=6,
    )
