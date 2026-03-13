"""Tests for chunked / partial-array streaming and related features."""

from __future__ import annotations

import io
import json
from typing import Iterator

import pytest
from pydantic_stream import (
    StreamingProjectionError,
    project_array_items,
    project_array_items_partial,
)

from .cases import (
    HarnessUserDataclass,
    HarnessUserModel,
    json_bytes,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_array_bytes(records: list[dict]) -> bytes:
    return json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode()


def _chunk(data: bytes, size: int) -> Iterator[bytes]:
    for i in range(0, len(data), size):
        yield data[i : i + size]


# ---------------------------------------------------------------------------
# Rust: project_array_items_partial
# ---------------------------------------------------------------------------


class TestPartialArrayBasic:
    def test_empty_array(self) -> None:
        items, consumed, finished = project_array_items_partial(b"[]", HarnessUserModel._streaming_spec())
        assert items == []
        assert finished is True
        assert consumed == 2

    def test_single_item(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}])
        spec = HarnessUserModel._streaming_spec()
        items, consumed, finished = project_array_items_partial(data, spec)
        assert len(items) == 1
        assert finished is True
        assert json.loads(items[0]) == {"id": 1, "name": "Ada"}

    def test_multiple_items(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)
        spec = HarnessUserModel._streaming_spec()
        items, consumed, finished = project_array_items_partial(data, spec)
        assert len(items) == 5
        assert finished is True

    def test_strips_extra_fields(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada", "noise": "xxx"}])
        spec = HarnessUserModel._streaming_spec()
        items, _, _ = project_array_items_partial(data, spec)
        parsed = json.loads(items[0])
        assert "noise" not in parsed

    def test_not_array_error(self) -> None:
        spec = HarnessUserModel._streaming_spec()
        with pytest.raises(StreamingProjectionError, match="Expected"):
            project_array_items_partial(b'{"id": 1}', spec)

    def test_non_object_in_array_error(self) -> None:
        spec = HarnessUserModel._streaming_spec()
        with pytest.raises(StreamingProjectionError, match="Expected object"):
            project_array_items_partial(b"[42]", spec)


class TestPartialArrayChunking:
    """Test that chunk boundaries are handled correctly."""

    def test_truncated_item_deferred(self) -> None:
        """When an item is cut off, it's not included; consumed stops before it."""
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])
        spec = HarnessUserModel._streaming_spec()
        # Cut the data mid-way through the second item
        cut = data.index(b'"Grace"')
        partial = data[:cut]
        items, consumed, finished = project_array_items_partial(partial, spec)
        assert len(items) == 1
        assert finished is False
        # consumed should point to the start of the incomplete second item
        assert consumed < len(partial)

    def test_continuation_chunk(self) -> None:
        """is_start=False handles continuation without leading '['."""
        data = _make_array_bytes([{"id": 1, "name": "Ada"}, {"id": 2, "name": "Grace"}])
        spec = HarnessUserModel._streaming_spec()
        # First call: get first item
        items1, consumed1, fin1 = project_array_items_partial(data, spec, True)
        assert not fin1 or len(items1) == 2  # might get both if data fits

        if not fin1:
            # Second call with remainder
            remainder = data[consumed1:]
            items2, consumed2, fin2 = project_array_items_partial(remainder, spec, False)
            all_items = items1 + items2
            assert len(all_items) == 2
            assert fin2 is True

    def test_chunk_boundary_at_comma(self) -> None:
        """Chunk split right at the comma between items."""
        data = _make_array_bytes([{"id": 1, "name": "A"}, {"id": 2, "name": "B"}])
        spec = HarnessUserModel._streaming_spec()
        # Find the comma between items
        first_obj_end = data.index(b"},") + 1  # after the '}'
        chunk1 = data[: first_obj_end + 1]  # includes ','
        chunk2 = data[first_obj_end + 1 :]

        items1, consumed1, fin1 = project_array_items_partial(chunk1, spec, True)
        assert len(items1) == 1
        assert fin1 is False

        remainder = chunk1[consumed1:] + chunk2
        items2, _, fin2 = project_array_items_partial(remainder, spec, False)
        assert len(items2) == 1
        assert fin2 is True

    def test_small_chunks_reassembled(self) -> None:
        """Streaming through very small chunks still yields all items."""
        records = [{"id": i, "name": f"User{i}"} for i in range(10)]
        data = _make_array_bytes(records)
        spec = HarnessUserModel._streaming_spec()

        all_items: list[bytes] = []
        buffer = bytearray()
        is_start = True

        for chunk in _chunk(data, 20):  # very small chunks
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            all_items.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break

        # Drain
        if buffer:
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            all_items.extend(items)

        assert len(all_items) == 10
        for i, item in enumerate(all_items):
            parsed = json.loads(item)
            assert parsed["id"] == i

    def test_single_large_item_larger_than_chunk(self) -> None:
        """A single item larger than chunk_size is handled when buffer grows."""
        big_name = "A" * 1000
        records = [{"id": 1, "name": big_name}]
        data = _make_array_bytes(records)
        spec = HarnessUserModel._streaming_spec()

        all_items: list[bytes] = []
        buffer = bytearray()
        is_start = True

        for chunk in _chunk(data, 50):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            all_items.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break

        if buffer:
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            all_items.extend(items)

        assert len(all_items) == 1
        assert json.loads(all_items[0])["name"] == big_name

    def test_matches_eager_projection(self) -> None:
        """Chunked results match eager project_array_items."""
        records = [{"id": i, "name": f"User{i}", "noise": "drop"} for i in range(20)]
        data = _make_array_bytes(records)
        spec = HarnessUserModel._streaming_spec()

        # Eager
        eager = project_array_items(data, spec)

        # Chunked
        chunked: list[bytes] = []
        buffer = bytearray()
        is_start = True
        for chunk in _chunk(data, 64):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            chunked.extend(items)
            del buffer[:consumed]
            is_start = False
            if finished:
                break
        if buffer:
            items, _, _ = project_array_items_partial(bytes(buffer), spec, is_start)
            chunked.extend(items)

        assert len(chunked) == len(eager)
        for c, e in zip(chunked, eager):
            assert json.loads(c) == json.loads(e)

    def test_empty_input_returns_not_finished(self) -> None:
        spec = HarnessUserModel._streaming_spec()
        items, consumed, finished = project_array_items_partial(b"", spec, True)
        assert items == []
        assert consumed == 0
        assert finished is False

    def test_whitespace_handling(self) -> None:
        data = b'  [  {"id": 1, "name": "A"}  ,  {"id": 2, "name": "B"}  ]  '
        spec = HarnessUserModel._streaming_spec()
        items, _, finished = project_array_items_partial(data, spec)
        assert len(items) == 2
        assert finished is True


# ---------------------------------------------------------------------------
# Python: stream_model_validate_json_array_iter (BaseModel)
# ---------------------------------------------------------------------------


class TestBaseModelArrayIter:
    def test_file_like_source(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)
        f = io.BytesIO(data)
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(f, chunk_size=32))
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.id == i
            assert r.name == f"User{i}"

    def test_iterable_source(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)
        chunks = list(_chunk(data, 32))
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(chunks))
        assert len(results) == 5

    def test_empty_array(self) -> None:
        f = io.BytesIO(b"[]")
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(f))
        assert results == []

    def test_single_item(self) -> None:
        data = _make_array_bytes([{"id": 1, "name": "Ada"}])
        f = io.BytesIO(data)
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(f, chunk_size=16))
        assert len(results) == 1
        assert results[0].id == 1
        assert results[0].name == "Ada"

    def test_strips_extra_fields(self) -> None:
        records = [{"id": 1, "name": "Ada", "noise": "xxx"}]
        data = _make_array_bytes(records)
        f = io.BytesIO(data)
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(f))
        assert len(results) == 1
        assert not hasattr(results[0], "noise")

    def test_returns_iterator(self) -> None:
        f = io.BytesIO(_make_array_bytes([{"id": 1, "name": "A"}]))
        it = HarnessUserModel.stream_model_validate_json_array_iter(f)
        assert hasattr(it, "__iter__") and hasattr(it, "__next__")

    def test_large_array_small_chunks(self) -> None:
        records = [{"id": i, "name": f"U{i}"} for i in range(100)]
        data = _make_array_bytes(records)
        f = io.BytesIO(data)
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(f, chunk_size=64))
        assert len(results) == 100


# ---------------------------------------------------------------------------
# Python: stream_validate_json_array_iter (Dataclass)
# ---------------------------------------------------------------------------


class TestDataclassArrayIter:
    def test_file_like_source(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)
        f = io.BytesIO(data)
        results = list(HarnessUserDataclass.stream_validate_json_array_iter(f, chunk_size=32))
        assert len(results) == 5
        for i, r in enumerate(results):
            assert r.id == i

    def test_iterable_source(self) -> None:
        records = [{"id": i, "name": f"User{i}"} for i in range(5)]
        data = _make_array_bytes(records)
        chunks = list(_chunk(data, 32))
        results = list(HarnessUserDataclass.stream_validate_json_array_iter(chunks))
        assert len(results) == 5

    def test_empty_array(self) -> None:
        f = io.BytesIO(b"[]")
        results = list(HarnessUserDataclass.stream_validate_json_array_iter(f))
        assert results == []


# ---------------------------------------------------------------------------
# Callable source in _to_bytes
# ---------------------------------------------------------------------------


class TestCallableSource:
    def test_callable_returns_bytes(self) -> None:
        payload = {"id": 1, "name": "Ada"}
        result = HarnessUserModel.stream_model_validate_json(lambda: json_bytes(payload))
        assert result.id == 1

    def test_callable_returns_string(self) -> None:
        payload = {"id": 1, "name": "Ada"}
        result = HarnessUserModel.stream_model_validate_json(lambda: json.dumps(payload))
        assert result.id == 1


# ---------------------------------------------------------------------------
# Streaming JSONL (line-by-line iterator path)
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Simulated S3 streaming body
# ---------------------------------------------------------------------------


class FakeS3StreamingBody:
    """Simulates boto3's StreamingBody from s3.get_object()["Body"].

    Supports both `.read(chunk_size)` (file-like) and `.iter_chunks(chunk_size)`
    (explicit iterator), matching the real StreamingBody API.
    """

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
    """Demonstrate chunked streaming with a source that behaves like S3."""

    def _make_s3_body(self, num_records: int, extra_fields: int = 5) -> tuple[FakeS3StreamingBody, list[dict]]:
        """Build a fake S3 body with records containing extra fields to drop."""
        records = []
        for i in range(num_records):
            record: dict = {"id": i, "name": f"Entity-{i:04d}"}
            # Simulate a wide payload where only a few fields are needed
            for j in range(extra_fields):
                record[f"unused_col_{j}"] = f"noise-{'x' * 50}-{j}"
            records.append(record)
        data = _make_array_bytes(records)
        return FakeS3StreamingBody(data), records

    def test_s3_file_like_read(self) -> None:
        """s3_body.read(chunk_size) via stream_model_validate_json_array_iter."""
        body, expected = self._make_s3_body(50)

        results = list(HarnessUserModel.stream_model_validate_json_array_iter(body, chunk_size=256))

        assert len(results) == 50
        for i, item in enumerate(results):
            assert item.id == i
            assert item.name == f"Entity-{i:04d}"

    def test_s3_iter_chunks(self) -> None:
        """s3_body.iter_chunks(chunk_size) as an explicit iterable source."""
        body, expected = self._make_s3_body(50)
        chunks = body.iter_chunks(chunk_size=256)

        results = list(HarnessUserModel.stream_model_validate_json_array_iter(chunks))

        assert len(results) == 50
        for i, item in enumerate(results):
            assert item.id == i

    def test_s3_projection_drops_extra_fields(self) -> None:
        """Only id/name survive projection — extra columns are discarded in Rust."""
        body, _ = self._make_s3_body(10, extra_fields=20)

        results = list(HarnessUserModel.stream_model_validate_json_array_iter(body, chunk_size=128))

        assert len(results) == 10
        for item in results:
            dumped = item.model_dump()
            # HarnessUserModel fields only
            assert set(dumped.keys()) == {"id", "name", "address", "tags", "metadata"}

    def test_s3_large_payload_small_chunks(self) -> None:
        """1000 records streamed in 512-byte chunks — result matches expectations."""
        body, expected = self._make_s3_body(1000, extra_fields=3)

        results = list(HarnessUserModel.stream_model_validate_json_array_iter(body, chunk_size=512))

        assert len(results) == 1000
        assert results[0].id == 0
        assert results[-1].id == 999

    def test_s3_empty_array(self) -> None:
        body = FakeS3StreamingBody(b"[]")
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(body))
        assert results == []

    def test_s3_single_record(self) -> None:
        body = FakeS3StreamingBody(_make_array_bytes([{"id": 1, "name": "Solo"}]))
        results = list(HarnessUserModel.stream_model_validate_json_array_iter(body, chunk_size=16))
        assert len(results) == 1
        assert results[0].name == "Solo"

    def test_s3_dataclass_variant(self) -> None:
        """Same pattern works with dataclass mixin."""
        body, _ = self._make_s3_body(20)

        results = list(HarnessUserDataclass.stream_validate_json_array_iter(body, chunk_size=256))

        assert len(results) == 20
        for i, item in enumerate(results):
            assert item.id == i


class TestStreamingJsonlIter:
    def test_basemodel_iterator_source_streams(self) -> None:
        """When source is an iterator, JSONL processes line-by-line."""
        lines = [
            json_bytes({"id": 1, "name": "Ada"}),
            json_bytes({"id": 2, "name": "Grace"}),
        ]
        results = list(HarnessUserModel.stream_model_validate_jsonl_iter(iter(lines)))
        assert len(results) == 2
        assert results[0].id == 1
        assert results[1].id == 2

    def test_dataclass_iterator_source_streams(self) -> None:
        lines = [
            json_bytes({"id": 1, "name": "Ada"}),
            json_bytes({"id": 2, "name": "Grace"}),
        ]
        results = list(HarnessUserDataclass.stream_validate_jsonl_iter(iter(lines)))
        assert len(results) == 2

    def test_blank_lines_skipped(self) -> None:
        lines = [
            json_bytes({"id": 1, "name": "Ada"}),
            b"",
            b"   ",
            json_bytes({"id": 2, "name": "Grace"}),
        ]
        results = list(HarnessUserModel.stream_model_validate_jsonl_iter(iter(lines)))
        assert len(results) == 2

    def test_string_lines(self) -> None:
        lines = [
            json.dumps({"id": 1, "name": "Ada"}),
            json.dumps({"id": 2, "name": "Grace"}),
        ]
        results = list(HarnessUserModel.stream_model_validate_jsonl_iter(iter(lines)))
        assert len(results) == 2

    def test_eager_source_still_works(self) -> None:
        """bytes/str/BytesIO sources still go through the eager path."""
        data = json_bytes({"id": 1, "name": "Ada"}) + b"\n" + json_bytes({"id": 2, "name": "Grace"})
        results = list(HarnessUserModel.stream_model_validate_jsonl_iter(data))
        assert len(results) == 2
