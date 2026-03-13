"""Local pytest and Hypothesis configuration for streamable tests.

CLI flags (benchmark-specific)
------------------------------
``--large-payload``
    Enable ``@pytest.mark.large_payload`` tests (~100 MB per shape).

``--payload-shape``
    Restrict large-payload tests to specific shapes.  Repeatable.
    Values: ``default``, ``wide``, ``deep``, ``string-heavy``, ``many-small``, ``all``.
    Default when omitted: ``all``.
"""

from __future__ import annotations

import os
from typing import Any

import pytest
from hypothesis import HealthCheck, settings

from .benchmarks._data_gen import SHAPES, PayloadShape
from .cases import (
    ALIAS_CHOICE_CASES,
    ALL_CASES,
    POPULATE_BY_NAME_CASES,
    RICH_CASES,
    USER_CASES,
    StreamableCase,
    reset_streaming_harness_caches,
)
from .helpers import SourceFactory

# ---------------------------------------------------------------------------
# pytest hooks — CLI flags for benchmark shapes (must live here so they are
# registered before command-line parsing; sub-directory conftest files are
# loaded too late for pytest_addoption).
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


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line(
        "markers",
        "large_payload: mark test to run only with --large-payload flag",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    large = config.getoption("--large-payload")
    enabled_shapes = _resolve_shapes(config.getoption("--payload-shape"))

    skip_large = pytest.mark.skip(reason="need --large-payload to run")
    for item in items:
        if "large_payload" not in item.keywords:
            continue
        if not large:
            item.add_marker(skip_large)
        elif hasattr(item, "callspec") and "shape" in item.callspec.params:
            shape = item.callspec.params["shape"]
            if shape not in enabled_shapes:
                item.add_marker(pytest.mark.skip(reason=f"shape {shape!r} not in --payload-shape selection"))


# ---------------------------------------------------------------------------
# Hypothesis
# ---------------------------------------------------------------------------


def _configure_hypothesis() -> None:
    try:
        settings.register_profile(
            "streamable",
            deadline=None,
            print_blob=True,
            suppress_health_check=[HealthCheck.function_scoped_fixture],
        )
    except ValueError:
        pass
    settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "streamable"))


_configure_hypothesis()


def _case_id(case: Any) -> str:
    return case.case_id


@pytest.fixture(autouse=True)
def reset_streamable_harness_state() -> None:
    reset_streaming_harness_caches()


@pytest.fixture
def source_factory() -> SourceFactory:
    return SourceFactory()


@pytest.fixture(params=ALL_CASES, ids=_case_id)
def any_stream_case(request: pytest.FixtureRequest) -> StreamableCase:
    return request.param


@pytest.fixture(params=USER_CASES, ids=_case_id)
def user_case(request: pytest.FixtureRequest) -> StreamableCase:
    return request.param


@pytest.fixture(params=ALIAS_CHOICE_CASES, ids=_case_id)
def alias_choice_case(request: pytest.FixtureRequest) -> StreamableCase:
    return request.param


@pytest.fixture(params=POPULATE_BY_NAME_CASES, ids=_case_id)
def populate_by_name_case(request: pytest.FixtureRequest) -> StreamableCase:
    return request.param


@pytest.fixture(params=RICH_CASES, ids=_case_id)
def rich_case(request: pytest.FixtureRequest) -> StreamableCase:
    return request.param
