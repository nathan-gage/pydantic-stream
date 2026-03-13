"""Hypothesis property-based tests for the streaming projection engine."""

from __future__ import annotations

import io
import json

from hypothesis import given, settings
from hypothesis import strategies as st
from pydantic_stream import FieldSpec, ObjectSpec, project_object

from .assertions import assert_json_array_matches_direct, assert_jsonl_matches_direct, assert_single_matches_direct
from .cases import (
    RICH_BASEMODEL_CASE,
    RICH_DATACLASS_CASE,
    USER_CASES,
    json_bytes,
    jsonl_bytes,
)
from .strategies import rich_model_payload_lists, rich_model_payloads, user_payload_lists

# ---------------------------------------------------------------------------
# TestRichModelSingleObjectParity
# ---------------------------------------------------------------------------


class TestRichModelSingleObjectParity:
    @settings(max_examples=25)
    @given(payload=rich_model_payloads())
    def test_rich_dataclass_single_matches_direct(self, payload: dict) -> None:
        assert_single_matches_direct(RICH_DATACLASS_CASE, payload)

    @settings(max_examples=25)
    @given(payload=rich_model_payloads())
    def test_rich_basemodel_single_matches_direct(self, payload: dict) -> None:
        assert_single_matches_direct(RICH_BASEMODEL_CASE, payload)


# ---------------------------------------------------------------------------
# TestRichModelArrayParity
# ---------------------------------------------------------------------------


class TestRichModelArrayParity:
    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists())
    def test_rich_dataclass_array_matches_direct(self, payloads: list) -> None:
        assert_json_array_matches_direct(RICH_DATACLASS_CASE, payloads)

    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists())
    def test_rich_basemodel_array_matches_direct(self, payloads: list) -> None:
        assert_json_array_matches_direct(RICH_BASEMODEL_CASE, payloads)


# ---------------------------------------------------------------------------
# TestRichModelJsonlParity
# ---------------------------------------------------------------------------


class TestRichModelJsonlParity:
    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists())
    def test_rich_dataclass_jsonl_matches_direct(self, payloads: list) -> None:
        assert_jsonl_matches_direct(RICH_DATACLASS_CASE, payloads)

    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists())
    def test_rich_basemodel_jsonl_matches_direct(self, payloads: list) -> None:
        assert_jsonl_matches_direct(RICH_BASEMODEL_CASE, payloads)


# ---------------------------------------------------------------------------
# TestJsonlCrlfInvariance
# ---------------------------------------------------------------------------


class TestJsonlCrlfInvariance:
    @settings(max_examples=25)
    @given(payloads=user_payload_lists(include_unknown=True))
    def test_crlf_jsonl_matches_lf_jsonl(self, payloads: list) -> None:
        for case in USER_CASES:
            lf_bytes = jsonl_bytes(payloads)
            crlf_bytes = lf_bytes.replace(b"\n", b"\r\n")
            lf_result = case.stream_validate_jsonl(io.BytesIO(lf_bytes))
            crlf_result = case.stream_validate_jsonl(io.BytesIO(crlf_bytes))
            assert case.dump_python_many(lf_result) == case.dump_python_many(crlf_result)


# ---------------------------------------------------------------------------
# TestJsonlSourceFormatInvariance
# ---------------------------------------------------------------------------


class TestJsonlSourceFormatInvariance:
    @settings(max_examples=25)
    @given(payloads=user_payload_lists(include_unknown=True))
    def test_jsonl_str_matches_bytes_matches_bytesio(self, payloads: list) -> None:
        for case in USER_CASES:
            raw = jsonl_bytes(payloads)
            from_bytes = case.stream_validate_jsonl(raw)
            from_str = case.stream_validate_jsonl(raw.decode("utf-8"))
            from_io = case.stream_validate_jsonl(io.BytesIO(raw))
            expected = case.dump_python_many(from_bytes)
            assert case.dump_python_many(from_str) == expected
            assert case.dump_python_many(from_io) == expected


# ---------------------------------------------------------------------------
# TestArraySourceFormatInvariance
# ---------------------------------------------------------------------------


class TestArraySourceFormatInvariance:
    @settings(max_examples=25)
    @given(payloads=user_payload_lists(include_unknown=True))
    def test_array_str_matches_bytes_matches_bytesio(self, payloads: list) -> None:
        for case in USER_CASES:
            raw = json_bytes(list(payloads))
            from_bytes = case.stream_validate_json_array(raw)
            from_str = case.stream_validate_json_array(raw.decode("utf-8"))
            from_io = case.stream_validate_json_array(io.BytesIO(raw))
            expected = case.dump_python_many(from_bytes)
            assert case.dump_python_many(from_str) == expected
            assert case.dump_python_many(from_io) == expected


# ---------------------------------------------------------------------------
# TestLargeIntegerRoundTrip
# ---------------------------------------------------------------------------


class TestLargeIntegerRoundTrip:
    @settings(max_examples=50)
    @given(v=st.integers(min_value=-(10**18), max_value=10**18))
    def test_large_integer_captured_exactly(self, v: int) -> None:
        spec = ObjectSpec(fields_by_input_key={"v": FieldSpec(output_key="v")})
        source = json.dumps({"v": v}).encode()
        result = json.loads(project_object(source, spec))
        assert result["v"] == v


# ---------------------------------------------------------------------------
# TestUnicodeKeyHandling
# ---------------------------------------------------------------------------


class TestUnicodeKeyHandling:
    @settings(max_examples=50)
    @given(key=st.text(min_size=1, max_size=10).filter(lambda k: k != "x"))
    def test_unknown_unicode_key_skipped(self, key: str) -> None:
        spec = ObjectSpec(fields_by_input_key={"x": FieldSpec(output_key="x")})
        payload = {"x": 1, key: "noise"}
        source = json.dumps(payload, ensure_ascii=False).encode()
        result = json.loads(project_object(source, spec))
        assert result == {"x": 1}


# ---------------------------------------------------------------------------
# TestRichModelWithUnknownFieldsParity
# ---------------------------------------------------------------------------


class TestRichModelWithUnknownFieldsParity:
    """Rich model tests with include_unknown=True to verify noise stripping."""

    @settings(max_examples=25)
    @given(payload=rich_model_payloads(include_unknown=True))
    def test_rich_dataclass_single_unknown_parity(self, payload: dict) -> None:
        assert_single_matches_direct(RICH_DATACLASS_CASE, payload)

    @settings(max_examples=25)
    @given(payload=rich_model_payloads(include_unknown=True))
    def test_rich_basemodel_single_unknown_parity(self, payload: dict) -> None:
        assert_single_matches_direct(RICH_BASEMODEL_CASE, payload)

    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists(include_unknown=True))
    def test_rich_dataclass_array_unknown_parity(self, payloads: list) -> None:
        assert_json_array_matches_direct(RICH_DATACLASS_CASE, payloads)

    @settings(max_examples=15)
    @given(payloads=rich_model_payload_lists(include_unknown=True))
    def test_rich_basemodel_array_unknown_parity(self, payloads: list) -> None:
        assert_json_array_matches_direct(RICH_BASEMODEL_CASE, payloads)


# ---------------------------------------------------------------------------
# TestOutputKeyEscaping
# ---------------------------------------------------------------------------


class TestOutputKeyEscaping:
    """Property test: aliases with special chars produce valid JSON output."""

    @settings(max_examples=50)
    @given(alias=st.text(min_size=1, max_size=20))
    def test_special_alias_produces_valid_json(self, alias: str) -> None:
        """Create a model with a Field(alias=alias), project through it, verify output parses."""
        # Build a spec manually with the alias as input key and output key
        field_spec = FieldSpec(output_key=alias)
        spec = ObjectSpec(fields_by_input_key={alias: field_spec})

        # Create input JSON with the alias as key
        input_json = json.dumps({alias: 42}, ensure_ascii=False).encode()

        # Project and verify the output is valid JSON with the correct value
        output = project_object(input_json, spec)
        parsed = json.loads(output)
        assert parsed[alias] == 42
