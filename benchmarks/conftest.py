"""Benchmark support for memray-backed memory measurements.

Selection is intentionally pytest-native:

- Use ``-m "not large_payload"`` for the standard benchmark set.
- Use ``-m large_payload`` for the heavy ~100 MiB inputs.
- Use ``-k wide`` / ``-k deep`` / ``-k stream_basemodel`` to narrow by shape or case.

Timing benchmarks use ``pytest-benchmark`` directly. The memory-only suite
reuses ``--benchmark-save``, ``--benchmark-autosave``, ``--benchmark-compare``,
``--benchmark-storage``, and ``--benchmark-histogram`` but stores its artifacts
under ``<benchmark-storage>/memory`` so the timing and memory data stay separate.
"""

from __future__ import annotations

import gc
import os
import statistics
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import memray
import pytest
from memray import FileReader
from pytest_benchmark.logger import Logger
from pytest_benchmark.storage.file import FileStorage

from ._results import (
    build_memory_comparisons,
    describe_saved_artifact,
    load_saved_memory_artifact,
    memory_storage_path,
    serialize_memory_results,
    write_memory_histogram,
)

MEMORY_ROUNDS = int(os.environ.get("MEMORY_ROUNDS", "5"))

# Match pytest-benchmark's number formatting.
NUMBER_FMT = "{0:,.4f}"
ALIGNED_NUMBER_FMT = "{0:>{1},.4f}{2:<{3}}"


@dataclass
class MemoryStats:
    name: str
    group: str = "memory"
    samples: list[int] = field(default_factory=list)
    result_size: int = 0
    timeline_rounds: list[list[tuple[float, int]]] = field(default_factory=list)

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
        """(std_outliers, iqr_outliers) matching pytest-benchmark's legend."""
        n = len(self.samples)
        if n < 2:
            return 0, 0
        mean = self.mean_val
        sd = self.stddev_val
        std_out = sum(1 for sample in self.samples if sd > 0 and abs(sample - mean) > sd)
        if n >= 4:
            q = statistics.quantiles(self.samples, n=4)
            q1, q3 = q[0], q[2]
            iqr = q3 - q1
            iqr_out = sum(
                1
                for sample in self.samples
                if iqr > 0 and (sample < q1 - 1.5 * iqr or sample > q3 + 1.5 * iqr)
            )
        else:
            iqr_out = 0
        return std_out, iqr_out

    @property
    def rounds(self) -> int:
        return len(self.samples)


memory_results: dict[str, MemoryStats] = {}
metadata: dict[str, Any] = {}


def _is_memory_only_run(config: pytest.Config) -> bool:
    if not config.args:
        return False
    return all(
        os.path.basename(str(target).split("::", 1)[0]) == "test_memory_profiles.py"
        for target in config.args
    )


def _targets_include_memory_suite(config: pytest.Config) -> bool:
    if not config.args:
        return False
    for target in config.args:
        path = str(target).split("::", 1)[0]
        basename = os.path.basename(path.rstrip(os.sep))
        if basename in {"benchmarks", "test_memory_profiles.py"}:
            return True
    return False


def _store_memory_benchmark_options(config: pytest.Config) -> None:
    config._memory_benchmark_save = config.getoption("benchmark_save")
    config._memory_benchmark_autosave = config.getoption("benchmark_autosave")
    config._memory_benchmark_compare = config.getoption("benchmark_compare")
    config._memory_benchmark_histogram = list(config.getoption("benchmark_histogram"))
    config._memory_benchmark_storage = config.getoption("benchmark_storage")
    config._memory_benchmark_save_data = bool(config.getoption("benchmark_save_data"))


def _memory_option(config: pytest.Config, name: str) -> Any:
    return getattr(config, f"_memory_benchmark_{name}")


def _load_memory_compare_artifacts(storage: FileStorage, compare: object) -> list[tuple[Any, Any]]:
    if compare is True:
        candidates = storage.query("[0-9][0-9][0-9][0-9]_")
        return [
            (path.relative_to(storage.path), load_saved_memory_artifact(path))
            for path in candidates[-1:]
        ]

    candidates = storage.query(str(compare))
    if not candidates:
        candidates = storage.query(f"*_{compare}*")
    return [
        (path.relative_to(storage.path), load_saved_memory_artifact(path)) for path in candidates
    ]


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config: pytest.Config) -> None:
    _store_memory_benchmark_options(config)
    memory_results.clear()
    metadata.clear()
    config._memory_compare_artifact = None
    config._memory_compare_path = None
    config._memory_compare_missing = None

    memory_only_run = _is_memory_only_run(config)
    if memory_only_run:
        config.option.benchmark_disable = True
        config.option.benchmark_save = None
        config.option.benchmark_autosave = None
        config.option.benchmark_compare = []
        config.option.benchmark_json = None
        config.option.benchmark_histogram = []

    if not _targets_include_memory_suite(config):
        return

    compare = _memory_option(config, "compare")
    if compare == []:
        return

    storage = _memory_storage(config)
    loaded = _load_memory_compare_artifacts(storage, compare)

    if not loaded:
        message = f"Can't compare memory benchmarks. No memory benchmark files in {str(storage)!r}"
        if compare is True:
            message += ". Can't load the previous memory benchmark."
        else:
            message += f" match {compare!r}."
        if memory_only_run:
            raise pytest.UsageError(message)
        config._memory_compare_missing = message
        return

    compare_path, compare_artifact = loaded[-1]
    if not isinstance(compare_artifact, dict) or not isinstance(
        compare_artifact.get("memory_results"), list
    ):
        raise pytest.UsageError(
            f"Saved memory benchmark artifact {compare_path} is missing a top-level 'memory_results' list"
        )

    config._memory_compare_path = compare_path
    config._memory_compare_artifact = compare_artifact


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    items.sort(key=lambda item: item.nodeid)

    # When the run only contains memray-style memory tests, disable pytest-benchmark's
    # execution/saving path so its save/compare flags can be reused without warnings.
    has_timing_benchmarks = any("benchmark" in getattr(item, "fixturenames", ()) for item in items)
    if not has_timing_benchmarks:
        config.option.benchmark_disable = True


def _benchmark_context(config: pytest.Config) -> dict[str, Any]:
    return {
        "paths": [str(arg) for arg in config.invocation_params.args],
        "keyword": config.option.keyword or "",
        "markexpr": config.option.markexpr or "",
        "memory_rounds": MEMORY_ROUNDS,
    }


def _memory_logger(config: pytest.Config) -> Logger:
    logger = getattr(config, "_memory_logger", None)
    if logger is not None:
        return logger

    if config.getoption("benchmark_verbose"):
        level = Logger.VERBOSE
    elif config.getoption("benchmark_quiet"):
        level = Logger.QUIET
    else:
        level = Logger.NORMAL

    logger = Logger(level=level, config=config)
    config._memory_logger = logger
    return logger


def _memory_storage(config: pytest.Config) -> FileStorage:
    storage = getattr(config, "_memory_storage", None)
    if storage is not None:
        return storage

    try:
        storage_path = memory_storage_path(_memory_option(config, "storage"))
    except ValueError as exc:
        raise pytest.UsageError(str(exc)) from exc

    storage = FileStorage(str(storage_path), logger=_memory_logger(config))
    config._memory_storage = storage
    return storage


def _tracker(path: str) -> memray.Tracker:
    return memray.Tracker(path, trace_python_allocators=True)


def _make_tmp_path() -> tuple[str, str]:
    tmp_dir = tempfile.mkdtemp()
    return tmp_dir, os.path.join(tmp_dir, "memray.bin")


def _cleanup_tmp(tmp_dir: str, path: str) -> None:
    if os.path.exists(path):
        os.unlink(path)
    os.rmdir(tmp_dir)


def _normalize_snapshots(reader: FileReader) -> list[tuple[float, int]]:
    snapshots = list(reader.get_memory_snapshots())
    if not snapshots:
        return []
    start = snapshots[0].time
    return [((snapshot.time - start) / 1000.0, snapshot.rss) for snapshot in snapshots]


def _measure_round(fn: Callable[[], Any]) -> tuple[int, list[tuple[float, int]]]:
    """Run *fn* once under memray and return peak bytes plus RSS snapshots."""
    tmp_dir, path = _make_tmp_path()
    try:
        with _tracker(path):
            fn()
        reader = FileReader(path)
        try:
            peak = sum(record.size for record in reader.get_high_watermark_allocation_records())
            timeline = _normalize_snapshots(reader)
            return peak, timeline
        finally:
            reader.close()
    finally:
        _cleanup_tmp(tmp_dir, path)


def _measure_result_size(fn: Callable[[], Any]) -> int:
    """Run *fn* under memray and return bytes still alive at exit (= result objects)."""
    tmp_dir, path = _make_tmp_path()
    try:
        with _tracker(path):
            _result = fn()  # noqa: F841 - kept alive so its allocations are "leaked"
            gc.collect()
        reader = FileReader(path)
        try:
            records = list(reader.get_leaked_allocation_records())
            return sum(record.size for record in records)
        finally:
            reader.close()
    finally:
        _cleanup_tmp(tmp_dir, path)


def measure_memory(
    name: str,
    fn: Callable[[], Any],
    rounds: int = MEMORY_ROUNDS,
    group: str = "memory",
) -> None:
    """Run *fn* under memray *rounds* times and record peak memory + result size."""
    stats = MemoryStats(name=name, group=group)
    for _ in range(rounds):
        peak, timeline = _measure_round(fn)
        stats.samples.append(peak)
        stats.timeline_rounds.append(timeline)
    stats.result_size = _measure_result_size(fn)
    memory_results[name] = stats


def _fmt_size(value: int | float) -> str:
    if value < 1024:
        return f"{value:.0f} B"
    if value < 1024 * 1024:
        return f"{value / 1024:.1f} KiB"
    return f"{value / (1024 * 1024):.1f} MiB"


def _memory_unit(best_val: float) -> tuple[str, float]:
    if best_val < 1024:
        return "B", 1.0
    if best_val < 1024 * 1024:
        return "KiB", 1024.0
    return "MiB", 1024.0 * 1024.0


def _compute_baseline_scale(baseline: float, value: float, width: int) -> str:
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
        positive = [value for value in (getattr(result, prop) for result in results) if value > 0]
        best[prop] = min(positive) if positive else 0.0

    bench_data: list[dict[str, Any]] = []
    for result in results:
        row: dict[str, Any] = {"name": result.name}
        for prop in ratio_props:
            row[prop] = getattr(result, prop)
        std_out, iqr_out = result.outliers
        row["outliers"] = f"{std_out};{iqr_out}"
        row["rounds"] = str(result.rounds)
        row["result_size"] = _fmt_size(result.result_size)
        bench_data.append(row)

    widths: dict[str, int] = {
        "name": 3 + max(len(labels["name"]), max(len(row["name"]) for row in bench_data)),
    }
    for prop in ratio_props:
        widths[prop] = 2 + max(
            len(labels[prop]),
            max(len(NUMBER_FMT.format(row[prop] * adjustment)) for row in bench_data),
        )
    for prop in ("outliers", "rounds", "result_size"):
        widths[prop] = 2 + max(len(labels[prop]), max(len(str(row[prop])) for row in bench_data))

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
    for row in bench_data:
        tw.write(row["name"].ljust(widths["name"]))
        for prop in all_columns:
            if prop in ratio_props:
                cell = ALIGNED_NUMBER_FMT.format(
                    row[prop] * adjustment,
                    widths[prop],
                    _compute_baseline_scale(best[prop], row[prop], rpadding),
                    rpadding,
                )
                is_best = best[prop] > 0 and row[prop] == best[prop]
                tw.write(cell, bold=is_best, green=is_best)
            else:
                tw.write(f"{row[prop]:>{widths[prop]}}")
        tw.line()

    tr.write_line("-" * len(header), yellow=True)


def _render_memory_compare_table(tr: Any, group_name: str, rows: list[dict[str, Any]]) -> None:
    unit, divisor = _memory_unit(min(min(row["previous"], row["current"]) for row in rows))

    labels = {
        "name": f"Name (median peak in {unit})",
        "previous": "Previous",
        "current": "Current",
        "ratio": "Ratio",
        "delta": "Delta",
    }

    rendered_rows = [
        {
            "name": row["name"],
            "previous": NUMBER_FMT.format(row["previous"] / divisor),
            "current": NUMBER_FMT.format(row["current"] / divisor),
            "ratio": "n/a" if row["ratio"] is None else f"{row['ratio']:.2f}x",
            "delta": "n/a" if row["delta_pct"] is None else f"{row['delta_pct']:+.1f}%",
            "is_improvement": row["current"] < row["previous"],
            "is_regression": row["current"] > row["previous"],
        }
        for row in rows
    ]

    widths = {
        "name": 3 + max(len(labels["name"]), max(len(row["name"]) for row in rendered_rows)),
        "previous": 2
        + max(len(labels["previous"]), max(len(row["previous"]) for row in rendered_rows)),
        "current": 2
        + max(len(labels["current"]), max(len(row["current"]) for row in rendered_rows)),
        "ratio": 2 + max(len(labels["ratio"]), max(len(row["ratio"]) for row in rendered_rows)),
        "delta": 2 + max(len(labels["delta"]), max(len(row["delta"]) for row in rendered_rows)),
    }

    header = (
        labels["name"].ljust(widths["name"])
        + labels["previous"].rjust(widths["previous"])
        + labels["current"].rjust(widths["current"])
        + labels["ratio"].rjust(widths["ratio"])
        + labels["delta"].rjust(widths["delta"])
    )
    title = f" compare memory: {group_name} ({len(rows)} matched) "
    tr.write_line(title.center(len(header), "-"), cyan=True)
    tr.write_line(header)
    tr.write_line("-" * len(header), cyan=True)

    tw = tr._tw
    for row in rendered_rows:
        tw.write(row["name"].ljust(widths["name"]))
        tw.write(f"{row['previous']:>{widths['previous']}}")
        tw.write(f"{row['current']:>{widths['current']}}")
        tw.write(
            f"{row['ratio']:>{widths['ratio']}}",
            green=row["is_improvement"],
            red=row["is_regression"],
            bold=True,
        )
        tw.write(
            f"{row['delta']:>{widths['delta']}}",
            green=row["is_improvement"],
            red=row["is_regression"],
        )
        tw.line()
    tr.write_line("-" * len(header), cyan=True)


def _save_memory_results(config: pytest.Config) -> None:
    save_name = _memory_option(config, "save") or _memory_option(config, "autosave")
    if not save_name or not memory_results:
        return

    artifact = serialize_memory_results(
        memory_results=memory_results,
        metadata=metadata,
        context=_benchmark_context(config),
        include_data=bool(_memory_option(config, "save_data")),
    )
    _memory_storage(config).save(artifact, save_name)


def _render_memory_histograms(
    terminalreporter: Any,
    config: pytest.Config,
    compare_rows: dict[str, dict[str, Any]],
) -> None:
    prefixes = _memory_option(config, "histogram")
    if not prefixes or not memory_results:
        return

    for prefix in prefixes:
        for name in sorted(memory_results):
            stats = memory_results[name]
            compare_row = compare_rows.get(name)
            output_path = write_memory_histogram(
                prefix,
                stats,
                previous_median=None if compare_row is None else compare_row["previous"],
            )
            terminalreporter.write_line(f"generated memory histogram: {output_path}", purple=True)


@pytest.hookimpl(trylast=True)
def pytest_terminal_summary(terminalreporter, exitstatus, config):  # noqa: ARG001
    if not memory_results:
        return

    groups: dict[str, list[MemoryStats]] = {}
    for stats in memory_results.values():
        groups.setdefault(stats.group, []).append(stats)

    for group_name, group_results in groups.items():
        group_results.sort(key=lambda result: result.median_val)
        payload_size = 0
        for prefix in ("large-parse-", "parse-"):
            if group_name.startswith(prefix):
                shape = group_name.removeprefix(prefix)
                payload_size = metadata.get(f"payload_size_{shape}", 0)
                break
        _render_memory_table(terminalreporter, group_name, group_results, payload_size)

    terminalreporter.write_line("")

    compare_artifact = getattr(config, "_memory_compare_artifact", None)
    compare_rows_by_name: dict[str, dict[str, Any]] = {}
    if compare_artifact is not None:
        compare_info = describe_saved_artifact(compare_artifact)
        compare_path = getattr(config, "_memory_compare_path", None)
        terminalreporter.write_line(
            f"memory comparison source: {compare_path} (commit {compare_info['commit']} at {compare_info['datetime']})",
            cyan=True,
        )

        memory_compare = build_memory_comparisons(memory_results, compare_artifact)
        for group_name, rows in memory_compare["groups"].items():
            for row in rows:
                compare_rows_by_name[row["name"]] = row
            _render_memory_compare_table(terminalreporter, group_name, rows)

        if memory_compare["missing_from_saved"] or memory_compare["missing_from_current"]:
            terminalreporter.write_line(
                "memory comparison coverage:"
                f" missing from saved={len(memory_compare['missing_from_saved'])},"
                f" missing from current={len(memory_compare['missing_from_current'])}",
                cyan=True,
            )
    elif getattr(config, "_memory_compare_missing", None):
        terminalreporter.write_line(config._memory_compare_missing, yellow=True)

    _save_memory_results(config)
    _render_memory_histograms(terminalreporter, config, compare_rows_by_name)
