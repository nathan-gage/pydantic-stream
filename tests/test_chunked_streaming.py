"""Tests for chunked / partial-array streaming and related public APIs."""

from __future__ import annotations

import io
import json
from collections.abc import Iterator

import pytest
from pydantic import TypeAdapter
from pydantic_core import ValidationError

from pydantic_stream import (
    StreamingProjectionError,
    project_array_items,
    project_array_items_partial,
    stream_json_array,
)

from .cases import HarnessUserDataclass, HarnessUserModel, StreamableCase, json_bytes


def _make_array_bytes(records: list[dict[str, object]]) -> bytes:
    return json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode()


def _chunk(data: bytes, size: int) -> Iterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


def _assert_user_results(results: list[object], records: list[dict[str, object]]) -> None:
    assert [item.id for item in results] == [record["id"] for record in records]
    assert [item.name for item in results] == [record["name"] for record in records]


class TestPartialArrayBasic:
    def test_empty_array(self) -> None:
        items, consumed, finished = project_array_items_partial(
            b"[]", HarnessUserModel._streaming_spec()
        )
        assert items == []
        assert finished is True
        assert consumed == 2

    def test_single_item(self) -> None:
        records = [{"id": 1, "name": "Ada"}]
        data = _make_array_bytes(records)

        items, consumed, finished = project_array_items_partial(
            data, HarnessUserModel._streaming_spec()
        )

        assert [json.loads(item) for item in items] == records
        assert consumed == len(data)
        assert finished is True

    def test_multiple_items(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)

        items, consumed, finished = project_array_items_partial(
            data, HarnessUserModel._streaming_spec()
        )

        assert [json.loads(item) for item in items] == records
        assert consumed == len(data)
        assert finished is True

    def test_strips_extra_fields(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada", "noise": "xxx"}])
        items, _, _ = project_array_items_partial(data, HarnessUserModel._streaming_spec())
        assert json.loads(items[0]) == {"id": 1, "name": "Ada"}

    def test_not_array_error(self) -> None:
        with pytest.raises(StreamingProjectionError, match="Expected"):
            project_array_items_partial(b'{"id": 1}', HarnessUserModel._streaming_spec())

    def test_non_object_in_array_error(self) -> None:
        with pytest.raises(StreamingProjectionError, match="Expected object"):
            project_array_items_partial(b"[42]", HarnessUserModel._streaming_spec())


class TestPartialArrayChunking:
    def test_truncated_item_deferred(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])
        cut = data.index(b'"Grace"')

        items, consumed, finished = project_array_items_partial(
            data[:cut], HarnessUserModel._streaming_spec()
        )

        assert [json.loads(item) for item in items] == [{"id": 1, "name": "Ada"}]
        assert consumed == data.index(b'{"id":2')
        assert finished is False

    def test_chunk_boundary_at_comma(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
        first_obj_end = data.index(b"},") + 1
        chunk1 = data[: first_obj_end + 1]
        chunk2 = data[first_obj_end + 1 :]

        items1, consumed1, fin1 = project_array_items_partial(
            chunk1, HarnessUserModel._streaming_spec(), True
        )
        assert [json.loads(item) for item in items1] == [{"id": 1, "name": "A"}]
        assert consumed1 == first_obj_end + 1
        assert fin1 is False

        remainder = chunk1[consumed1:] + chunk2
        items2, consumed2, fin2 = project_array_items_partial(
            remainder, HarnessUserModel._streaming_spec(), False
        )
        assert [json.loads(item) for item in items2] == [{"id": 2, "name": "B"}]
        assert consumed2 == len(remainder)
        assert fin2 is True

    def test_small_chunks_reassembled(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(10)]
        data = _make_array_bytes(records)

        all_items: list[bytes] = []
        buffer = bytearray()
        is_start = True

        for chunk in _chunk(data, 20):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            all_items.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break

        if buffer:
            items, _, _ = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            all_items.extend(items)

        assert [json.loads(item) for item in all_items] == records

    def test_single_large_item_larger_than_chunk(self) -> None:
        records = [{"id": 1, "name": "A" * 1000}]
        data = _make_array_bytes(records)

        all_items: list[bytes] = []
        buffer = bytearray()
        is_start = True

        for chunk in _chunk(data, 50):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            all_items.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break

        if buffer:
            items, _, _ = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            all_items.extend(items)

        assert [json.loads(item) for item in all_items] == records

    def test_matches_eager_projection(self) -> None:
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(20)]
        data = _make_array_bytes(records)
        eager = project_array_items(data, HarnessUserModel._streaming_spec())

        chunked: list[bytes] = []
        buffer = bytearray()
        is_start = True
        for chunk in _chunk(data, 64):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            chunked.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break
        if buffer:
            items, _, _ = project_array_items_partial(
                bytes(buffer), HarnessUserModel._streaming_spec(), is_start
            )
            chunked.extend(items)

        assert [json.loads(item) for item in chunked] == [json.loads(item) for item in eager]

    def test_empty_input_returns_not_finished(self) -> None:
        items, consumed, finished = project_array_items_partial(
            b"", HarnessUserModel._streaming_spec(), True
        )
        assert items == []
        assert consumed == 0
        assert finished is False

    def test_whitespace_handling(self) -> None:
        data = b'  [  {"id": 1, "name": "A"}  ,  {"id": 2, "name": "B"}  ]  '
        items, consumed, finished = project_array_items_partial(
            data, HarnessUserModel._streaming_spec()
        )
        assert [json.loads(item) for item in items] == [
            {"id": 1, "name": "A"},
            {"id": 2, "name": "B"},
        ]
        assert consumed == len(data) - len(b"  ")
        assert finished is True


class TestStreamValidateJsonArrayIter:
    def test_file_like_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        results = list(
            user_case.stream_validate_json_array_iter(
                io.BytesIO(_make_array_bytes(records)), chunk_size=32
            )
        )
        _assert_user_results(results, records)

    def test_iterable_source(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        results = list(
            user_case.stream_validate_json_array_iter(list(_chunk(_make_array_bytes(records), 32)))
        )
        _assert_user_results(results, records)

    def test_empty_array(self, user_case: StreamableCase) -> None:
        results = list(user_case.stream_validate_json_array_iter(io.BytesIO(b"[]")))
        assert results == []

    def test_single_item(self, user_case: StreamableCase) -> None:
        records = [{"id": 1, "name": "Ada"}]
        results = list(
            user_case.stream_validate_json_array_iter(
                io.BytesIO(_make_array_bytes(records)), chunk_size=16
            )
        )
        _assert_user_results(results, records)

    def test_strips_extra_fields(self, user_case: StreamableCase) -> None:
        records = [{"id": 1, "name": "Ada", "noise": "xxx"}]
        results = list(
            user_case.stream_validate_json_array_iter(io.BytesIO(_make_array_bytes(records)))
        )
        assert user_case.dump_python(results[0]) == {
            "id": 1,
            "name": "Ada",
            "address": None,
            "tags": [],
            "metadata": {},
        }

    def test_large_array_small_chunks(self, user_case: StreamableCase) -> None:
        records = [{"id": i, "name": f"U{i}"} for i in range(100)]
        results = list(
            user_case.stream_validate_json_array_iter(
                io.BytesIO(_make_array_bytes(records)), chunk_size=64
            )
        )
        _assert_user_results(results, records)

    def test_truncated_file_like_source_raises(self, user_case: StreamableCase) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])[:-1]
        with pytest.raises(StreamingProjectionError, match="Unexpected end of JSON array"):
            list(user_case.stream_validate_json_array_iter(io.BytesIO(data), chunk_size=16))

    def test_truncated_iterable_source_raises(self, user_case: StreamableCase) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])[:-1]
        with pytest.raises(StreamingProjectionError, match="Unexpected end of JSON array"):
            list(user_case.stream_validate_json_array_iter(_chunk(data, 16)))

    def test_trailing_garbage_raises(self, user_case: StreamableCase) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}]) + b"garbage"
        with pytest.raises(StreamingProjectionError):
            list(user_case.stream_validate_json_array_iter(io.BytesIO(data), chunk_size=16))

    def test_validation_fallback_preserves_itemwise_semantics(
        self, user_case: StreamableCase
    ) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": "bad", "name": "Grace"}])
        iterator = user_case.stream_validate_json_array_iter(io.BytesIO(data), chunk_size=4096)

        first = next(iterator)
        assert first.id == 1

        with pytest.raises(ValidationError):
            next(iterator)


class TestCallableSource:
    def test_callable_returns_bytes(self, user_case: StreamableCase) -> None:
        result = user_case.stream_validate_json(lambda: json_bytes({"id": 1, "name": "Ada"}))
        assert result.id == 1
        assert result.name == "Ada"

    def test_callable_returns_string(self, user_case: StreamableCase) -> None:
        result = user_case.stream_validate_json(lambda: json.dumps({"id": 1, "name": "Ada"}))
        assert result.id == 1
        assert result.name == "Ada"


class FakeS3StreamingBody:
    """Simulates boto3's StreamingBody with `.read()` and `.iter_chunks()`."""

    def __init__(self, data: bytes) -> None:
        self._stream = io.BytesIO(data)

    def read(self, amt: int | None = None) -> bytes:
        return self._stream.read(amt)  # type: ignore[arg-type]

    def iter_chunks(self, chunk_size: int = 1024) -> Iterator[bytes]:
        while True:
            chunk = self._stream.read(chunk_size)
            if not chunk:
                break
            yield chunk


class TestSimulatedS3Streaming:
    def _make_s3_body(
        self, num_records: int, extra_fields: int = 5
    ) -> tuple[FakeS3StreamingBody, list[dict[str, object]]]:
        records: list[dict[str, object]] = []
        for i in range(num_records):
            record = {"id": i, "name": f"Entity-{i:04d}"}
            for j in range(extra_fields):
                record[f"unused_col_{j}"] = f"noise-{'x' * 50}-{j}"
            records.append(record)
        return FakeS3StreamingBody(_make_array_bytes(records)), records

    def test_file_like_read(self, user_case: StreamableCase) -> None:
        body, expected = self._make_s3_body(50)
        results = list(user_case.stream_validate_json_array_iter(body, chunk_size=256))
        _assert_user_results(results, expected)

    def test_iter_chunks(self, user_case: StreamableCase) -> None:
        body, expected = self._make_s3_body(50)
        results = list(user_case.stream_validate_json_array_iter(body.iter_chunks(chunk_size=256)))
        _assert_user_results(results, expected)

    def test_projection_drops_extra_fields(self, user_case: StreamableCase) -> None:
        body, _ = self._make_s3_body(10, extra_fields=20)
        results = list(user_case.stream_validate_json_array_iter(body, chunk_size=128))
        assert all(
            user_case.dump_python(item)
            == {
                "id": item.id,
                "name": item.name,
                "address": None,
                "tags": [],
                "metadata": {},
            }
            for item in results
        )

    def test_large_payload_small_chunks(self, user_case: StreamableCase) -> None:
        body, expected = self._make_s3_body(200, extra_fields=3)
        results = list(user_case.stream_validate_json_array_iter(body, chunk_size=128))
        _assert_user_results(results, expected)

    def test_truncated_body_raises(self, user_case: StreamableCase) -> None:
        body, _ = self._make_s3_body(5)
        broken = FakeS3StreamingBody(body.read()[:-1])
        with pytest.raises(StreamingProjectionError, match="Unexpected end of JSON array"):
            list(user_case.stream_validate_json_array_iter(broken, chunk_size=32))


class TestStreamingJsonlIter:
    def test_iterator_source_streams(self, user_case: StreamableCase) -> None:
        lines = [
            json_bytes({"id": 1, "name": "Ada"}),
            json_bytes({"id": 2, "name": "Grace"}),
        ]
        results = list(user_case.stream_validate_jsonl_iter(iter(lines)))
        _assert_user_results(results, [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])

    def test_blank_lines_skipped(self, user_case: StreamableCase) -> None:
        lines = [
            json_bytes({"id": 1, "name": "Ada"}),
            b"",
            b"   ",
            json_bytes({"id": 2, "name": "Grace"}),
        ]
        results = list(user_case.stream_validate_jsonl_iter(iter(lines)))
        _assert_user_results(results, [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])

    def test_string_lines(self, user_case: StreamableCase) -> None:
        lines = [
            json.dumps({"id": 1, "name": "Ada"}),
            json.dumps({"id": 2, "name": "Grace"}),
        ]
        results = list(user_case.stream_validate_jsonl_iter(iter(lines)))
        _assert_user_results(results, [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])

    def test_eager_source_still_works(self, user_case: StreamableCase) -> None:
        data = json_bytes({"id": 1, "name": "Ada"}) + b"\n" + json_bytes({"id": 2, "name": "Grace"})
        results = list(user_case.stream_validate_jsonl_iter(data))
        _assert_user_results(results, [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])


@pytest.fixture(
    params=[TypeAdapter(HarnessUserModel), TypeAdapter(HarnessUserDataclass)],
    ids=["basemodel", "dataclass"],
)
def user_adapter(request: pytest.FixtureRequest) -> TypeAdapter[object]:
    return request.param


class TestStreamJsonArray:
    def test_file_like_source(self, user_adapter: TypeAdapter[object]) -> None:
        records = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        results = list(
            stream_json_array(io.BytesIO(_make_array_bytes(records)), user_adapter, chunk_size=8)
        )
        _assert_user_results(results, records)

    def test_iterable_source(self, user_adapter: TypeAdapter[object]) -> None:
        records = [{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}]
        results = list(stream_json_array(_chunk(_make_array_bytes(records), 8), user_adapter))
        _assert_user_results(results, records)

    def test_truncated_source_raises(self, user_adapter: TypeAdapter[object]) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}])[:-1]
        with pytest.raises(StreamingProjectionError, match="Unexpected end of JSON array"):
            list(stream_json_array(io.BytesIO(data), user_adapter, chunk_size=8))

    def test_trailing_garbage_raises(self, user_adapter: TypeAdapter[object]) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}]) + b"garbage"
        with pytest.raises(StreamingProjectionError):
            list(stream_json_array(io.BytesIO(data), user_adapter, chunk_size=8))
