"""Benchmark conftest: CLI flags, memray memory measurement, and result tables.

CLI flags
---------
``--large-payload``
    Enable ``@pytest.mark.large_payload`` tests (~100 MB per shape).

``--no-memory``
    Skip memray memory profiling in benchmarks (timing only).

``--payload-shape``
    Restrict benchmarks to specific shapes.  Repeatable.
    Values: ``default``, ``wide``, ``deep``, ``string-heavy``, ``many-small``, ``all``.
    Default when omitted: ``all``.
"""

from __future__ import annotations

import gc
import os
import statistics
import tempfile
from dataclasses import dataclass, field
from typing import Any, Callable

import memray
import pytest
from memray import FileReader

from ._data_gen import SHAPES, PayloadShape


# ---------------------------------------------------------------------------
# pytest hooks — CLI flags for benchmark shapes
# ---------------------------------------------------------------------------


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("benchmarks", "Streamable memory benchmarks")
    group.addoption(
        "--large-payload",
        action="store_true",
        default=False,
        help="Run large-payload (~100 MB) benchmark tests.",
    )
    group.addoption(
        "--no-memory",
        action="store_true",
        default=False,
        help="Skip memray memory profiling in benchmarks (timing only).",
    )
    group.addoption(
        "--payload-shape",
        action="append",
        default=[],
        metavar="SHAPE",
        help=(
            "Shape(s) to include for large-payload tests. "
            "Repeatable. Values: default, wide, deep, string-heavy, many-small, all. "
            "Default: all."
        ),
    )


def _resolve_shapes(raw: list[str]) -> list[PayloadShape]:
    """Normalise the ``--payload-shape`` CLI values into a concrete list."""
    if not raw or "all" in raw:
        return list(SHAPES)
    resolved: list[PayloadShape] = []
    for v in raw:
        normed = v.strip().lower()
        if normed in SHAPES:
            resolved.append(normed)  # type: ignore[arg-type]
        else:
            raise pytest.UsageError(f"Unknown --payload-shape {v!r}. Choose from: {', '.join(SHAPES)}, all")
    return resolved


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    large = config.getoption("--large-payload")
    raw_shapes = config.getoption("--payload-shape")
    enabled_shapes = _resolve_shapes(raw_shapes)

    skip_large = pytest.mark.skip(reason="need --large-payload to run")
    for item in items:
        # Gate large-payload tests behind --large-payload flag
        if "large_payload" in item.keywords and not large:
            item.add_marker(skip_large)
            continue

        # Filter by --payload-shape for any parametrized shape test
        if hasattr(item, "callspec") and "shape" in item.callspec.params:
            shape = item.callspec.params["shape"]
            if shape not in enabled_shapes:
                item.add_marker(pytest.mark.skip(reason=f"shape {shape!r} not in --payload-shape selection"))

    # Deterministic ordering: sort by node ID so test names are always consistent.
    items.sort(key=lambda item: item.nodeid)

MEMORY_ROUNDS = int(os.environ.get("MEMORY_ROUNDS", "5"))

# Match pytest-benchmark's number formatting
NUMBER_FMT = "{0:,.4f}"
ALIGNED_NUMBER_FMT = "{0:>{1},.4f}{2:<{3}}"


@dataclass
class MemoryStats:
    name: str
    group: str = "memory"
    samples: list[int] = field(default_factory=list)
    result_size: int = 0

    @property
    def min_val(self) -> float:
        return float(min(self.samples))

    @property
    def max_val(self) -> float:
        return float(max(self.samples))

    @property
    def mean_val(self) -> float:
        return statistics.mean(self.samples)

    @property
    def stddev_val(self) -> float:
        return statistics.stdev(self.samples) if len(self.samples) > 1 else 0.0

    @property
    def median_val(self) -> float:
        return statistics.median(self.samples)

    @property
    def iqr_val(self) -> float:
        if len(self.samples) < 4:
            return 0.0
        q = statistics.quantiles(self.samples, n=4)
        return q[2] - q[0]

    @property
    def outliers(self) -> tuple[int, int]:
        """(std_outliers, iqr_outliers) matching pytest-benchmark legend."""
        n = len(self.samples)
        if n < 2:
            return 0, 0
        mean = self.mean_val
        sd = self.stddev_val
        std_out = sum(1 for s in self.samples if sd > 0 and abs(s - mean) > sd)
        if n >= 4:
            q = statistics.quantiles(self.samples, n=4)
            q1, q3 = q[0], q[2]
            iqr = q3 - q1
            iqr_out = sum(1 for s in self.samples if iqr > 0 and (s < q1 - 1.5 * iqr or s > q3 + 1.5 * iqr))
        else:
            iqr_out = 0
        return std_out, iqr_out

    @property
    def rounds(self) -> int:
        return len(self.samples)


memory_results: dict[str, MemoryStats] = {}
metadata: dict[str, Any] = {}


def _tracker(path: str) -> memray.Tracker:
    """Create a memray Tracker that sees individual Python object allocations."""
    return memray.Tracker(path, trace_python_allocators=True)


def _make_tmp_path() -> tuple[str, str]:
    """Create a temp dir + file path suitable for memray (file must not exist yet)."""
    tmp_dir = tempfile.mkdtemp()
    return tmp_dir, os.path.join(tmp_dir, "memray.bin")


def _cleanup_tmp(tmp_dir: str, path: str) -> None:
    os.unlink(path)
    os.rmdir(tmp_dir)


def _measure_peak(fn: Callable[[], Any]) -> int:
    """Run *fn* once under memray and return high-watermark bytes."""
    tmp_dir, path = _make_tmp_path()
    try:
        with _tracker(path):
            fn()
        records = list(FileReader(path).get_high_watermark_allocation_records())
        return sum(r.size for r in records)
    finally:
        _cleanup_tmp(tmp_dir, path)


def _measure_result_size(fn: Callable[[], Any]) -> int:
    """Run *fn* under memray and return bytes still alive at exit (= result objects)."""
    tmp_dir, path = _make_tmp_path()
    try:
        with _tracker(path):
            _result = fn()  # noqa: F841 — kept alive so its allocations are "leaked"
            gc.collect()  # free generator/iterator cycles so only _result's objects remain
        records = list(FileReader(path).get_leaked_allocation_records())
        return sum(r.size for r in records)
    finally:
        _cleanup_tmp(tmp_dir, path)


def measure_memory(name: str, fn: Callable[[], Any], rounds: int = MEMORY_ROUNDS, group: str = "memory") -> None:
    """Run *fn* under memray *rounds* times and record peak memory + result size."""
    stats = MemoryStats(name=name, group=group)
    for _ in range(rounds):
        stats.samples.append(_measure_peak(fn))
    stats.result_size = _measure_result_size(fn)
    memory_results[name] = stats


# ---------------------------------------------------------------------------
# Table rendering (matches pytest-benchmark's visual format)
# ---------------------------------------------------------------------------


def _fmt_size(b: int) -> str:
    if b < 1024:
        return f"{b} B"
    if b < 1024 * 1024:
        return f"{b / 1024:.1f} KiB"
    return f"{b / (1024 * 1024):.1f} MiB"


def _memory_unit(best_val: float) -> tuple[str, float]:
    """Pick display unit based on the smallest (best) value."""
    if best_val < 1024:
        return "B", 1.0
    if best_val < 1024 * 1024:
        return "KiB", 1024.0
    return "MiB", 1024.0 * 1024.0


def _compute_baseline_scale(baseline: float, value: float, width: int) -> str:
    """Ratio suffix matching pytest-benchmark's format."""
    if not width:
        return ""
    if baseline == 0 and value == 0:
        return " (0)".ljust(width)
    if value == baseline:
        return " (1.0)".ljust(width)
    if baseline > 0:
        scale = abs(value / baseline)
    else:
        scale = float("inf")
    if scale > 1000:
        return (" (inf)" if scale == float("inf") else " (>1000.0)").ljust(width)
    return f" ({scale:.2f})".ljust(width)


def _render_memory_table(
    tr: Any,
    group_name: str,
    results: list[MemoryStats],
    payload_size: int,
) -> None:
    """Render one memory comparison table for a group of results."""
    solo = len(results) == 1
    rpadding = 0 if solo else 10

    unit, divisor = _memory_unit(results[0].median_val)
    adjustment = 1.0 / divisor

    ratio_props = ["min_val", "max_val", "mean_val", "stddev_val", "median_val", "iqr_val"]
    all_columns = ratio_props + ["outliers", "rounds", "result_size"]

    labels = {
        "name": f"Name (memory in {unit})",
        "min_val": "Min",
        "max_val": "Max",
        "mean_val": "Mean",
        "stddev_val": "StdDev",
        "median_val": "Median",
        "iqr_val": "IQR",
        "outliers": "Outliers",
        "rounds": "Rounds",
        "result_size": "Result",
    }

    best: dict[str, float] = {}
    for prop in ratio_props:
        positive = [v for v in (getattr(r, prop) for r in results) if v > 0]
        best[prop] = min(positive) if positive else 0.0

    bench_data: list[dict[str, Any]] = []
    for r in results:
        d: dict[str, Any] = {"name": r.name}
        for prop in ratio_props:
            d[prop] = getattr(r, prop)
        std_out, iqr_out = r.outliers
        d["outliers"] = f"{std_out};{iqr_out}"
        d["rounds"] = str(r.rounds)
        d["result_size"] = _fmt_size(r.result_size)
        bench_data.append(d)

    widths: dict[str, int] = {
        "name": 3 + max(len(labels["name"]), max(len(d["name"]) for d in bench_data)),
    }
    for prop in ratio_props:
        widths[prop] = 2 + max(
            len(labels[prop]),
            max(len(NUMBER_FMT.format(d[prop] * adjustment)) for d in bench_data),
        )
    for prop in ("outliers", "rounds", "result_size"):
        widths[prop] = 2 + max(
            len(labels[prop]),
            max(len(str(d[prop])) for d in bench_data),
        )

    header = labels["name"].ljust(widths["name"])
    for prop in all_columns:
        header += labels[prop].rjust(widths[prop])
        if prop in ratio_props:
            header += " " * rpadding

    payload_str = f", payload: {_fmt_size(payload_size)}" if payload_size else ""
    title = f" {group_name} ({len(results)} tests{payload_str}) "
    title_line = title.center(len(header), "-")

    tr.write_line("")
    tr.write_line(title_line, yellow=True)
    tr.write_line(header)
    tr.write_line("-" * len(header), yellow=True)

    tw = tr._tw
    for d in bench_data:
        tw.write(d["name"].ljust(widths["name"]))
        for prop in all_columns:
            if prop in ratio_props:
                cell = ALIGNED_NUMBER_FMT.format(
                    d[prop] * adjustment,
                    widths[prop],
                    _compute_baseline_scale(best[prop], d[prop], rpadding),
                    rpadding,
                )
                is_best = best[prop] > 0 and d[prop] == best[prop]
                tw.write(cell, bold=is_best, green=is_best)
            else:
                tw.write(f"{d[prop]:>{widths[prop]}}")
        tw.line()

    tr.write_line("-" * len(header), yellow=True)


def pytest_terminal_summary(terminalreporter, exitstatus, config):  # noqa: ARG001
    if not memory_results:
        return

    # Group results by their group name
    groups: dict[str, list[MemoryStats]] = {}
    for stats in memory_results.values():
        groups.setdefault(stats.group, []).append(stats)

    for group_name, group_results in groups.items():
        group_results.sort(key=lambda r: r.median_val)
        # Extract shape from group name (e.g. "parse-wide", "large-parse-deep")
        payload_size = 0
        for prefix in ("large-parse-", "parse-"):
            if group_name.startswith(prefix):
                shape = group_name.removeprefix(prefix)
                payload_size = metadata.get(f"payload_size_{shape}", 0)
                break
        _render_memory_table(terminalreporter, group_name, group_results, payload_size)

    terminalreporter.write_line("")
