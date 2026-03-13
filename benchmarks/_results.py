"""Helpers for memory benchmark artifact serialization, comparison, and plots."""

from __future__ import annotations

import json
import math
import re
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pytest_benchmark.utils import get_commit_info

ARTIFACT_SCHEMA_VERSION = 2
MEMORY_STORAGE_SUBDIR = "memory"

_SAFE_FILENAME_RE = re.compile(r"[^A-Za-z0-9_.-]+")
_PLOT_COLORS = (
    "#0f766e",
    "#b45309",
    "#1d4ed8",
    "#be123c",
    "#4338ca",
    "#166534",
)


def serialize_memory_results(
    memory_results: dict[str, Any],
    metadata: dict[str, Any],
    context: dict[str, Any],
    *,
    include_data: bool = False,
    commit_info: dict[str, Any] | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Convert in-memory memray results into a standalone JSON artifact."""
    rows: list[dict[str, Any]] = []
    for name in sorted(memory_results):
        stats = memory_results[name]
        std_outliers, iqr_outliers = stats.outliers
        row = {
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
        if include_data:
            row["timeline_rounds"] = [
                [{"seconds": seconds, "rss": rss} for seconds, rss in timeline]
                for timeline in getattr(stats, "timeline_rounds", [])
            ]
        rows.append(row)

    return {
        "schema_version": ARTIFACT_SCHEMA_VERSION,
        "datetime": created_at or datetime.now(UTC).isoformat(),
        "commit_info": commit_info or get_commit_info(),
        "context": dict(context),
        "metadata": dict(metadata),
        "memory_results": rows,
    }


def load_saved_memory_artifact(path: str | Path) -> dict[str, Any]:
    """Load and minimally validate a saved memory benchmark artifact."""
    artifact_path = Path(path)
    try:
        raw = artifact_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ValueError(
            f"Unable to read memory benchmark artifact {artifact_path}: {exc}"
        ) from exc

    try:
        artifact = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Memory benchmark artifact {artifact_path} is not valid JSON: {exc}"
        ) from exc

    if not isinstance(artifact, dict) or not isinstance(artifact.get("memory_results"), list):
        raise ValueError(
            f"Memory benchmark artifact {artifact_path} is missing a top-level 'memory_results' list"
        )
    return artifact


def build_memory_comparisons(
    current_memory_results: dict[str, Any],
    saved_artifact: dict[str, Any],
) -> dict[str, Any]:
    """Build comparison rows for current memory stats vs a saved artifact."""
    saved_rows = saved_artifact.get("memory_results", [])

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
        "rows": len(saved_artifact.get("memory_results", [])),
    }


def resolve_file_storage_path(raw_storage: str) -> Path:
    """Resolve pytest-benchmark's storage URI to a local path for memory artifacts."""
    if raw_storage.startswith("file://"):
        path = Path(raw_storage.removeprefix("file://"))
    elif "://" in raw_storage:
        raise ValueError(
            "memory benchmark artifacts only support local file storage; "
            f"got unsupported benchmark storage URI {raw_storage!r}"
        )
    else:
        path = Path(raw_storage)
    return path.expanduser().resolve()


def memory_storage_path(raw_storage: str) -> Path:
    return resolve_file_storage_path(raw_storage) / MEMORY_STORAGE_SUBDIR


def histogram_output_path(prefix: str, name: str) -> Path:
    raw = Path(prefix)
    parent = raw.parent if raw.parent != Path("") else Path(".")
    parent.mkdir(parents=True, exist_ok=True)
    return parent / f"{raw.name}-{_slugify(name)}.svg"


def write_memory_histogram(
    prefix: str,
    stats: Any,
    *,
    previous_median: float | None = None,
) -> Path:
    """Write a simple SVG memory plot for one benchmark result."""
    output_path = histogram_output_path(prefix, stats.name)
    output_path.write_text(
        _render_memory_plot_svg(stats, previous_median=previous_median),
        encoding="utf-8",
    )
    return output_path


def _render_memory_plot_svg(stats: Any, *, previous_median: float | None) -> str:
    timelines = [
        timeline for timeline in getattr(stats, "timeline_rounds", []) if len(timeline) > 1
    ]
    width = 920
    height = 520
    left = 72
    right = 28
    top = 56
    bottom = 72
    chart_width = width - left - right
    chart_height = height - top - bottom

    lines = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}" role="img" aria-label="{_xml_escape(stats.name)} memory plot">',
        '<rect width="100%" height="100%" fill="#ffffff"/>',
        f'<text x="{left}" y="30" font-size="22" font-family="monospace" fill="#111827">{_xml_escape(stats.name)}</text>',
        f'<text x="{left}" y="48" font-size="13" font-family="monospace" fill="#4b5563">median peak {_fmt_bytes(stats.median_val)} | result {_fmt_bytes(stats.result_size)}</text>',
    ]

    for idx in range(6):
        y = top + (chart_height * idx / 5)
        lines.append(
            f'<line x1="{left}" y1="{y:.2f}" x2="{left + chart_width}" y2="{y:.2f}" stroke="#e5e7eb" stroke-width="1"/>'
        )

    lines.append(
        f'<line x1="{left}" y1="{top}" x2="{left}" y2="{top + chart_height}" stroke="#111827" stroke-width="1.5"/>'
    )
    lines.append(
        f'<line x1="{left}" y1="{top + chart_height}" x2="{left + chart_width}" y2="{top + chart_height}" stroke="#111827" stroke-width="1.5"/>'
    )

    if timelines:
        max_time = max(timeline[-1][0] for timeline in timelines) or 1.0
        max_rss = max(rss for timeline in timelines for _, rss in timeline) or 1.0

        for idx, timeline in enumerate(timelines):
            color = _PLOT_COLORS[idx % len(_PLOT_COLORS)]
            points = " ".join(
                f"{left + (seconds / max_time) * chart_width:.2f},{top + chart_height - (rss / max_rss) * chart_height:.2f}"
                for seconds, rss in timeline
            )
            lines.append(
                f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{points}"/>'
            )

        for idx in range(6):
            seconds = max_time * idx / 5
            x = left + chart_width * idx / 5
            lines.append(
                f'<text x="{x:.2f}" y="{height - 28}" text-anchor="middle" font-size="12" font-family="monospace" fill="#374151">{seconds:.3f}s</text>'
            )

        for idx in range(6):
            rss = max_rss * (5 - idx) / 5
            y = top + chart_height * idx / 5
            lines.append(
                f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="monospace" fill="#374151">{_fmt_bytes(rss)}</text>'
            )

        lines.append(
            f'<text x="{left + chart_width / 2:.2f}" y="{height - 8}" text-anchor="middle" font-size="13" font-family="monospace" fill="#111827">Time since start</text>'
        )
        lines.append(
            f'<text x="24" y="{top + chart_height / 2:.2f}" transform="rotate(-90 24 {top + chart_height / 2:.2f})" text-anchor="middle" font-size="13" font-family="monospace" fill="#111827">RSS</text>'
        )
    else:
        sample_count = max(len(stats.samples), 1)
        max_sample = max(stats.samples) if stats.samples else 1.0
        bar_width = chart_width / sample_count
        for idx, sample in enumerate(stats.samples):
            bar_height = (sample / max_sample) * chart_height if max_sample else 0.0
            x = left + idx * bar_width + bar_width * 0.15
            y = top + chart_height - bar_height
            lines.append(
                f'<rect x="{x:.2f}" y="{y:.2f}" width="{bar_width * 0.7:.2f}" height="{bar_height:.2f}" fill="{_PLOT_COLORS[idx % len(_PLOT_COLORS)]}"/>'
            )
            lines.append(
                f'<text x="{left + idx * bar_width + bar_width / 2:.2f}" y="{height - 28}" text-anchor="middle" font-size="12" font-family="monospace" fill="#374151">r{idx + 1}</text>'
            )

        for idx in range(6):
            value = max_sample * (5 - idx) / 5
            y = top + chart_height * idx / 5
            lines.append(
                f'<text x="{left - 10}" y="{y + 4:.2f}" text-anchor="end" font-size="12" font-family="monospace" fill="#374151">{_fmt_bytes(value)}</text>'
            )

        lines.append(
            f'<text x="{left + chart_width / 2:.2f}" y="{height - 8}" text-anchor="middle" font-size="13" font-family="monospace" fill="#111827">Round</text>'
        )
        lines.append(
            f'<text x="24" y="{top + chart_height / 2:.2f}" transform="rotate(-90 24 {top + chart_height / 2:.2f})" text-anchor="middle" font-size="13" font-family="monospace" fill="#111827">Peak memory</text>'
        )

    if previous_median is not None:
        lines.append(
            f'<text x="{width - right}" y="48" text-anchor="end" font-size="13" font-family="monospace" fill="#7c2d12">previous median peak {_fmt_bytes(previous_median)}</text>'
        )

    lines.append("</svg>")
    return "\n".join(lines)


def _slugify(name: str) -> str:
    return _SAFE_FILENAME_RE.sub("-", name).strip("-") or "memory-benchmark"


def _fmt_bytes(value: float) -> str:
    if value < 1024:
        return f"{value:.0f} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value / (1024 * 1024):.1f} MiB"


def _xml_escape(text: str) -> str:
    return (
        text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")
    )


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
    value = row.get(value_key)
    if not isinstance(value, int | float):
        raise ValueError(f"Benchmark row is missing numeric {value_key!r}: {row!r}")
    if not math.isfinite(float(value)):
        raise ValueError(f"Benchmark row has non-finite numeric {value_key!r}: {row!r}")
    return float(value)
