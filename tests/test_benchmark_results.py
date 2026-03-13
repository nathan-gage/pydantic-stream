from __future__ import annotations

from dataclasses import dataclass

import pytest

from benchmarks._results import (
    ARTIFACT_KEY,
    build_memory_comparisons,
    build_timing_comparisons,
    load_saved_benchmark_artifact,
    serialize_memory_results,
)


@dataclass
class FakeMemoryStats:
    name: str
    group: str
    samples: list[int]
    result_size: int

    @property
    def min_val(self) -> float:
        return float(min(self.samples))

    @property
    def max_val(self) -> float:
        return float(max(self.samples))

    @property
    def mean_val(self) -> float:
        return sum(self.samples) / len(self.samples)

    @property
    def stddev_val(self) -> float:
        return 0.0

    @property
    def median_val(self) -> float:
        mid = len(self.samples) // 2
        ordered = sorted(self.samples)
        return float(ordered[mid])

    @property
    def iqr_val(self) -> float:
        return 0.0

    @property
    def outliers(self) -> tuple[int, int]:
        return (0, 0)

    @property
    def rounds(self) -> int:
        return len(self.samples)


def test_serialize_memory_results_embeds_context_and_stats() -> None:
    memory_results = {
        "stream-default": FakeMemoryStats(
            name="stream-default",
            group="parse-default",
            samples=[100, 120, 110],
            result_size=64,
        )
    }

    payload = serialize_memory_results(
        memory_results=memory_results,
        metadata={"payload_size_default": 1234},
        context={"no_memory": False, "payload_shapes": ["default"]},
    )

    assert payload["context"]["payload_shapes"] == ["default"]
    assert payload["metadata"]["payload_size_default"] == 1234
    assert payload["memory_results"][0]["name"] == "stream-default"
    assert payload["memory_results"][0]["median"] == 110.0


def test_build_timing_comparisons_matches_on_fullname() -> None:
    current = [
        {
            "group": "parse-default",
            "name": "test_stream_basemodel[default]",
            "fullname": "benchmarks/test_memory_benchmarks.py::test_stream_basemodel[default]",
            "stats": {"median": 0.90},
        }
    ]
    saved = {
        "benchmarks": [
            {
                "group": "parse-default",
                "name": "test_stream_basemodel[default]",
                "fullname": "benchmarks/test_memory_benchmarks.py::test_stream_basemodel[default]",
                "stats": {"median": 1.20},
            }
        ]
    }

    compare = build_timing_comparisons(current, saved)

    assert compare["missing_from_saved"] == []
    assert compare["missing_from_current"] == []
    row = compare["groups"]["parse-default"][0]
    assert row["ratio"] == pytest.approx(0.75)
    assert row["delta_pct"] == pytest.approx(-25.0)


def test_build_memory_comparisons_reads_saved_pydantic_stream_section() -> None:
    current = {
        "stream-default": FakeMemoryStats(
            name="stream-default",
            group="parse-default",
            samples=[90, 95, 100],
            result_size=64,
        )
    }
    saved = {
        "benchmarks": [],
        ARTIFACT_KEY: {
            "memory_results": [
                {
                    "name": "stream-default",
                    "group": "parse-default",
                    "median": 120.0,
                }
            ]
        },
    }

    compare = build_memory_comparisons(current, saved)

    row = compare["groups"]["parse-default"][0]
    assert row["ratio"] == pytest.approx(95.0 / 120.0)
    assert row["delta_pct"] == pytest.approx(((95.0 - 120.0) / 120.0) * 100.0)


def test_load_saved_benchmark_artifact_rejects_invalid_json(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError, match="not valid JSON"):
        load_saved_benchmark_artifact(path)
