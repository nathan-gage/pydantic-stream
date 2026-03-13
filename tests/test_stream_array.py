"""Tests for StreamArray — lazy array container with slicing, indexing, and to_list()."""

from __future__ import annotations

import pytest
from pydantic_stream import StreamingProjectionError

from .cases import HarnessUserModel, json_bytes, json_source


class TestStreamArrayIteration:
    def test_iter_returns_items_in_order(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        items = list(sa)
        assert [item.id for item in items] == [0, 1, 2, 3, 4]

    def test_iter_can_be_called_multiple_times(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        first = list(sa)
        second = list(sa)
        assert len(first) == len(second) == 2
        assert first[0].id == second[0].id

    def test_iter_empty_array(self) -> None:
        sa = HarnessUserModel.stream_model_validate_json_array(json_source([]))
        assert list(sa) == []


class TestStreamArrayGetItem:
    def test_getitem_first(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        assert sa[0].id == 1

    def test_getitem_last(self) -> None:
        payload = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        assert sa[1].id == 2

    def test_getitem_out_of_range_raises(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        with pytest.raises(IndexError):
            sa[5]

    def test_getitem_negative_raises(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        with pytest.raises(IndexError, match="negative"):
            sa[-1]


class TestStreamArraySlice:
    def test_slice_start_stop(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        result = sa[1:3]
        assert [item.id for item in result] == [1, 2]

    def test_slice_from_start(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        result = sa[0:2]
        assert [item.id for item in result] == [0, 1]

    def test_slice_empty(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        result = sa[0:0]
        assert result == []

    def test_slice_negative_start_raises(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        with pytest.raises(IndexError, match="negative"):
            sa[-1:2]

    def test_slice_negative_stop_raises(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        with pytest.raises(IndexError, match="negative"):
            sa[0:-1]

    def test_slice_negative_step_raises(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        with pytest.raises(IndexError, match="negative"):
            sa[0:1:-1]

    def test_slice_with_step(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(6)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        result = sa[0:6:2]
        assert [item.id for item in result] == [0, 2, 4]


class TestStreamArrayToList:
    def test_to_list_matches_iteration(self) -> None:
        payload = [{"id": i, "name": f"user{i}"} for i in range(5)]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        iter_result = list(sa)
        list_result = sa.to_list()
        assert [item.id for item in iter_result] == [item.id for item in list_result]

    def test_to_list_empty_array(self) -> None:
        sa = HarnessUserModel.stream_model_validate_json_array(json_source([]))
        assert sa.to_list() == []


class TestStreamArrayRootPrefix:
    def test_root_prefix_one_level(self) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        items = list(sa)
        assert [item.id for item in items] == [1, 2]

    def test_root_prefix_two_levels(self) -> None:
        data = {"data": {"results": [{"id": 1, "name": "Ada"}]}}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="data.results")
        items = list(sa)
        assert len(items) == 1
        assert items[0].id == 1

    def test_root_prefix_key_not_found(self) -> None:
        data = {"other": [{"id": 1, "name": "Ada"}]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        with pytest.raises(StreamingProjectionError, match="not found"):
            list(sa)

    def test_root_prefix_to_list(self) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        result = sa.to_list()
        assert [item.id for item in result] == [1, 2]

    def test_root_prefix_getitem(self) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        assert sa[0].id == 1
        assert sa[1].id == 2

    def test_root_prefix_slice(self) -> None:
        data = {"items": [{"id": i, "name": f"user{i}"} for i in range(5)]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        result = sa[1:3]
        assert [item.id for item in result] == [1, 2]


class TestStreamArrayRepr:
    def test_repr_shows_byte_count(self) -> None:
        payload = [{"id": 1, "name": "Ada"}]
        sa = HarnessUserModel.stream_model_validate_json_array(json_source(payload))
        r = repr(sa)
        assert "StreamArray(" in r
        assert "bytes" in r

    def test_repr_shows_prefix(self) -> None:
        data = {"items": [{"id": 1, "name": "Ada"}]}
        sa = HarnessUserModel.stream_model_validate_json_array(json_bytes(data), root_prefix="items")
        r = repr(sa)
        assert "items" in r
