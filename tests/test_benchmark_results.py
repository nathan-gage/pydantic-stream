from __future__ import annotations

from dataclasses import dataclass, field

import pytest

from benchmarks._results import (
    build_memory_comparisons,
    load_saved_memory_artifact,
    memory_storage_path,
    serialize_memory_results,
    write_memory_histogram,
)


@dataclass
class FakeMemoryStats:
    name: str
    group: str
    samples: list[int]
    result_size: int
    timeline_rounds: list[list[tuple[float, int]]] = field(default_factory=list)

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


def test_serialize_memory_results_embeds_context_stats_and_metadata() -> None:
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
        context={"markexpr": "not large_payload", "keyword": "wide"},
        commit_info={"id": "abc123def456", "dirty": False},
        created_at="2026-03-13T12:00:00+00:00",
    )

    assert payload["datetime"] == "2026-03-13T12:00:00+00:00"
    assert payload["commit_info"]["id"] == "abc123def456"
    assert payload["context"]["markexpr"] == "not large_payload"
    assert payload["metadata"]["payload_size_default"] == 1234
    assert payload["memory_results"][0]["name"] == "stream-default"
    assert payload["memory_results"][0]["median"] == 110.0
    assert "timeline_rounds" not in payload["memory_results"][0]


def test_serialize_memory_results_includes_timeline_rounds_when_requested() -> None:
    memory_results = {
        "stream-default": FakeMemoryStats(
            name="stream-default",
            group="parse-default",
            samples=[100, 120, 110],
            result_size=64,
            timeline_rounds=[[(0.0, 1024), (0.25, 4096)]],
        )
    }

    payload = serialize_memory_results(
        memory_results=memory_results,
        metadata={},
        context={},
        include_data=True,
        commit_info={"id": "abc123def456", "dirty": False},
        created_at="2026-03-13T12:00:00+00:00",
    )

    assert payload["memory_results"][0]["timeline_rounds"] == [
        [{"seconds": 0.0, "rss": 1024}, {"seconds": 0.25, "rss": 4096}]
    ]


def test_build_memory_comparisons_reads_saved_rows() -> None:
    current = {
        "stream-default": FakeMemoryStats(
            name="stream-default",
            group="parse-default",
            samples=[90, 95, 100],
            result_size=64,
        )
    }
    saved = {
        "memory_results": [
            {
                "name": "stream-default",
                "group": "parse-default",
                "median": 120.0,
            }
        ]
    }

    compare = build_memory_comparisons(current, saved)

    row = compare["groups"]["parse-default"][0]
    assert row["ratio"] == pytest.approx(95.0 / 120.0)
    assert row["delta_pct"] == pytest.approx(((95.0 - 120.0) / 120.0) * 100.0)


def test_load_saved_memory_artifact_rejects_invalid_json(tmp_path) -> None:
    path = tmp_path / "bad.json"
    path.write_text("{not json", encoding="utf-8")

    with pytest.raises(ValueError, match="not valid JSON"):
        load_saved_memory_artifact(path)


def test_memory_storage_path_uses_memory_subdirectory(tmp_path) -> None:
    storage = memory_storage_path(f"file://{tmp_path}")
    assert storage == tmp_path / "memory"


def test_write_memory_histogram_writes_svg(tmp_path) -> None:
    stats = FakeMemoryStats(
        name="stream-default",
        group="parse-default",
        samples=[100, 120, 110],
        result_size=64,
        timeline_rounds=[[(0.0, 1024), (0.1, 4096), (0.2, 2048)]],
    )

    output = write_memory_histogram(str(tmp_path / "memory_plot"), stats, previous_median=150.0)

    contents = output.read_text(encoding="utf-8")
    assert output.name == "memory_plot-stream-default.svg"
    assert "<svg" in contents
    assert "stream-default" in contents
    assert "previous median peak" in contents
