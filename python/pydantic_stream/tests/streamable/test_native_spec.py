"""Tests for FieldSpec and ObjectSpec PyO3 wrapper APIs."""

from __future__ import annotations

from pydantic_stream import FieldSpec, ObjectSpec


class TestFieldSpec:
    def test_output_key_getter(self) -> None:
        fs = FieldSpec(output_key="foo")
        assert fs.output_key == "foo"

    def test_repr_without_nested(self) -> None:
        fs = FieldSpec(output_key="bar")
        r = repr(fs)
        assert "bar" in r
        assert "None" in r

    def test_repr_with_nested(self) -> None:
        inner = ObjectSpec(fields_by_input_key={"a": FieldSpec(output_key="a")})
        fs = FieldSpec(output_key="outer", nested=inner)
        r = repr(fs)
        assert "outer" in r
        assert "ObjectSpec" in r


class TestObjectSpec:
    def test_len_empty(self) -> None:
        spec = ObjectSpec(fields_by_input_key={})
        assert len(spec) == 0

    def test_len_non_empty(self) -> None:
        spec = ObjectSpec(
            fields_by_input_key={
                "a": FieldSpec(output_key="a"),
                "b": FieldSpec(output_key="b"),
            }
        )
        assert len(spec) == 2

    def test_contains_true(self) -> None:
        spec = ObjectSpec(fields_by_input_key={"x": FieldSpec(output_key="x")})
        assert "x" in spec

    def test_contains_false(self) -> None:
        spec = ObjectSpec(fields_by_input_key={"x": FieldSpec(output_key="x")})
        assert "y" not in spec

    def test_repr_contains_keys(self) -> None:
        spec = ObjectSpec(fields_by_input_key={"alpha": FieldSpec(output_key="alpha")})
        r = repr(spec)
        assert "alpha" in r
        assert "ObjectSpec" in r
