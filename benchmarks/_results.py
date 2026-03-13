"""Helpers for benchmark artifact serialization and comparison."""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path
from typing import Any

ARTIFACT_KEY = "pydantic_stream"
ARTIFACT_SCHEMA_VERSION = 1


def serialize_memory_results(
    memory_results: dict[str, Any],
    metadata: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    """Convert in-memory memray results into JSON-safe artifact data."""
    rows: list[dict[str, Any]] = []
    for name in sorted(memory_results):
        stats = memory_results[name]
        std_outliers, iqr_outliers = stats.outliers
        rows.append(
            {
                "name": stats.name,
                "group": stats.group,
                "samples": list(stats.samples),
                "result_size": stats.result_size,
                "min": stats.min_val,
                "max": stats.max_val,
                "mean": stats.mean_val,
                "stddev": stats.stddev_val,
                "median": stats.median_val,
                "iqr": stats.iqr_val,
                "outliers": {
                    "stddev": std_outliers,
                    "iqr": iqr_outliers,
                },
                "rounds": stats.rounds,
            }
        )

    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "context": dict(context),
        "metadata": dict(metadata),
        "memory_results": rows,
    }


def load_saved_benchmark_artifact(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate a benchmark JSON artifact."""
    artifact_path = Path(path)
    try:
        raw = artifact_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(f"Unable to read benchmark artifact {artifact_path}: {exc}") from exc

    try:
        artifact = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Benchmark artifact {artifact_path} is not valid JSON: {exc}") from exc

    if not isinstance(artifact, dict) or not isinstance(artifact.get("benchmarks"), list):
        raise ValueError(
            f"Benchmark artifact {artifact_path} is missing a top-level 'benchmarks' list"
        )
    return artifact


def build_timing_comparisons(
    current_benchmarks: list[dict[str, Any]],
    saved_artifact: dict[str, Any],
) -> dict[str, Any]:
    """Build comparison rows for current timing stats vs a saved artifact."""
    saved_benchmarks = saved_artifact.get("benchmarks", [])
    current_map = _map_benchmarks(current_benchmarks)
    saved_map = _map_benchmarks(saved_benchmarks)
    return _build_comparisons(current_map, saved_map, value_key="median")


def build_memory_comparisons(
    current_memory_results: dict[str, Any],
    saved_artifact: dict[str, Any],
) -> dict[str, Any]:
    """Build comparison rows for current memory stats vs a saved artifact."""
    saved_section = saved_artifact.get(ARTIFACT_KEY, {})
    saved_rows = saved_section.get("memory_results", [])

    current_map = {
        name: {
            "name": stats.name,
            "group": stats.group,
            "median": stats.median_val,
        }
        for name, stats in current_memory_results.items()
    }
    saved_map = {
        row["name"]: row
        for row in saved_rows
        if isinstance(row, dict) and isinstance(row.get("name"), str) and "median" in row
    }

    return _build_comparisons(current_map, saved_map, value_key="median")


def describe_saved_artifact(saved_artifact: dict[str, Any]) -> dict[str, Any]:
    """Return a short human-friendly summary of a saved artifact."""
    commit_info = saved_artifact.get("commit_info", {})
    commit_id = str(commit_info.get("id", "unknown"))
    return {
        "commit": commit_id[:7],
        "datetime": saved_artifact.get("datetime", "unknown"),
        "has_memory": bool(saved_artifact.get(ARTIFACT_KEY, {}).get("memory_results")),
    }


def _map_benchmarks(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    mapped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = row.get("fullname") or row.get("name")
        if not isinstance(key, str):
            continue
        mapped[key] = row
    return mapped


def _build_comparisons(
    current_map: dict[str, dict[str, Any]],
    saved_map: dict[str, dict[str, Any]],
    *,
    value_key: str,
) -> dict[str, Any]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)

    common_keys = sorted(
        set(current_map) & set(saved_map),
        key=lambda key: (
            str(current_map[key].get("group") or saved_map[key].get("group") or ""),
            str(current_map[key].get("name") or saved_map[key].get("name") or key),
        ),
    )

    for key in common_keys:
        current = current_map[key]
        saved = saved_map[key]
        current_value = _extract_value(current, value_key)
        saved_value = _extract_value(saved, value_key)
        group = str(current.get("group") or saved.get("group") or "ungrouped")

        ratio = current_value / saved_value if saved_value else None
        delta_pct = ((current_value - saved_value) / saved_value * 100.0) if saved_value else None

        groups[group].append(
            {
                "name": str(current.get("name") or saved.get("name") or key),
                "key": key,
                "group": group,
                "previous": saved_value,
                "current": current_value,
                "ratio": ratio,
                "delta_pct": delta_pct,
            }
        )

    missing_from_saved = sorted(
        str(current_map[key].get("name") or key) for key in set(current_map) - set(saved_map)
    )
    missing_from_current = sorted(
        str(saved_map[key].get("name") or key) for key in set(saved_map) - set(current_map)
    )

    return {
        "groups": dict(groups),
        "missing_from_saved": missing_from_saved,
        "missing_from_current": missing_from_current,
    }


def _extract_value(row: dict[str, Any], value_key: str) -> float:
    stats = row.get("stats")
    source = stats if isinstance(stats, dict) else row
    value = source.get(value_key)
    if not isinstance(value, int | float):
        raise ValueError(f"Benchmark row is missing numeric {value_key!r}: {row!r}")
    return float(value)
