"""Tests for StreamArray across both streamable mixins."""

from __future__ import annotations

import pytest

from pydantic_stream import StreamingProjectionError

from .cases import StreamableCase, json_bytes, json_source


def _stream_array(
    case: StreamableCase, payload: object, *, root_prefix: str | None = None
) -> object:
    source = json_bytes(payload) if root_prefix else json_source(payload)
    if root_prefix is None:
        return case.stream_validate_json_array(source)
    return case.stream_validate_json_array(source, root_prefix=root_prefix)


class TestStreamArrayIteration:
    def test_iter_returns_items_in_order(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = _stream_array(user_case, payload)
        items = list(sa)
        assert [item.id for item in items] == [0, 1, 2, 3, 4]

    def test_iter_can_be_called_multiple_times(self, user_case: StreamableCase) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = _stream_array(user_case, payload)
        first = list(sa)
        second = list(sa)
        assert len(first) == len(second) == 2
        assert [item.id for item in first] == [item.id for item in second] == [1, 2]

    def test_iter_empty_array(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [])
        assert list(sa) == []


class TestStreamArrayGetItem:
    def test_getitem_first(self, user_case: StreamableCase) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = _stream_array(user_case, payload)
        assert sa[0].id == 1

    def test_getitem_last(self, user_case: StreamableCase) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = _stream_array(user_case, payload)
        assert sa[1].id == 2

    def test_getitem_out_of_range_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(IndexError):
            sa[5]

    def test_getitem_negative_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(IndexError, match="negative"):
            sa[-1]


class TestStreamArraySlice:
    def test_slice_start_stop(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = _stream_array(user_case, payload)
        result = sa[1:3]
        assert [item.id for item in result] == [1, 2]

    def test_slice_from_start(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = _stream_array(user_case, payload)
        result = sa[0:2]
        assert [item.id for item in result] == [0, 1]

    def test_slice_empty(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = _stream_array(user_case, payload)
        assert sa[0:0] == []

    def test_slice_negative_start_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(IndexError, match="negative"):
            sa[-1:2]

    def test_slice_negative_stop_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(IndexError, match="negative"):
            sa[0:-1]

    def test_slice_zero_step_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(ValueError, match="zero"):
            sa[::0]

    def test_slice_negative_step_raises(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        with pytest.raises(IndexError, match="negative"):
            sa[0:1:-1]

    def test_slice_with_step(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(6)]
        sa = _stream_array(user_case, payload)
        result = sa[0:6:2]
        assert [item.id for item in result] == [0, 2, 4]


class TestStreamArrayToList:
    def test_to_list_matches_iteration(self, user_case: StreamableCase) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = _stream_array(user_case, payload)
        iter_result = list(sa)
        list_result = sa.to_list()
        assert [item.id for item in iter_result] == [item.id for item in list_result]

    def test_to_list_empty_array(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [])
        assert sa.to_list() == []


class TestStreamArrayRootPrefix:
    def test_root_prefix_one_level(self, user_case: StreamableCase) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = _stream_array(user_case, data, root_prefix="items")
        assert [item.id for item in sa] == [1, 2]

    def test_root_prefix_two_levels(self, user_case: StreamableCase) -> None:
        data = {"data": {"results": [{"id": 1, "name": "Ada"}]}}
        sa = _stream_array(user_case, data, root_prefix="data.results")
        items = list(sa)
        assert len(items) == 1
        assert items[0].id == 1

    def test_root_prefix_key_not_found(self, user_case: StreamableCase) -> None:
        data = {"other": [{"id": 1, "name": "Ada"}]}
        sa = _stream_array(user_case, data, root_prefix="items")
        with pytest.raises(StreamingProjectionError, match="not found"):
            list(sa)

    def test_root_prefix_to_list(self, user_case: StreamableCase) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = _stream_array(user_case, data, root_prefix="items")
        assert [item.id for item in sa.to_list()] == [1, 2]

    def test_root_prefix_getitem(self, user_case: StreamableCase) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = _stream_array(user_case, data, root_prefix="items")
        assert sa[0].id == 1
        assert sa[1].id == 2

    def test_root_prefix_slice(self, user_case: StreamableCase) -> None:
        data = {"items": [{"id": i, "name": f"user{i}"} for i in range(5)]}
        sa = _stream_array(user_case, data, root_prefix="items")
        result = sa[1:3]
        assert [item.id for item in result] == [1, 2]


class TestStreamArrayRepr:
    def test_repr_shows_byte_count(self, user_case: StreamableCase) -> None:
        sa = _stream_array(user_case, [{"id": 1, "name": "Ada"}])
        r = repr(sa)
        assert "StreamArray(" in r
        assert "bytes" in r

    def test_repr_shows_prefix(self, user_case: StreamableCase) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}]}
        sa = _stream_array(user_case, data, root_prefix="items")
        assert "items" in repr(sa)
