"""Pytest and Hypothesis configuration for unit/integration tests."""

from __future__ import annotations

import os
from importlib.machinery import EXTENSION_SUFFIXES
from pathlib import Path
from typing import Any

import pytest
from hypothesis import HealthCheck, settings

from .cases import (
    ALIAS_CHOICE_CASES,
    ALL_CASES,
    POPULATE_BY_NAME_CASES,
    RICH_CASES,
    USER_CASES,
    StreamableCase,
    reset_streaming_harness_caches,
)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PACKAGE_DIR = _REPO_ROOT / "src" / "pydantic_stream"


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


# ---------------------------------------------------------------------------
# Native extension freshness checks
# ---------------------------------------------------------------------------


def _native_extension_paths() -> list[Path]:
    return sorted(
        path
        for path in _PACKAGE_DIR.iterdir()
        if path.is_file() and any(path.name.endswith(suffix) for suffix in EXTENSION_SUFFIXES)
    )


def _native_source_paths() -> list[Path]:
    return [
        _REPO_ROOT / "Cargo.toml",
        _REPO_ROOT / "Cargo.lock",
        _REPO_ROOT / "pyproject.toml",
        *sorted(_REPO_ROOT.glob("crates/**/Cargo.toml")),
        *sorted(_REPO_ROOT.glob("crates/**/*.rs")),
    ]


def _stale_native_build_message() -> str | None:
    native_paths = _native_extension_paths()
    if not native_paths:
        return (
            "Native extension is not built. Run `make build` or `uv run maturin develop` "
            "before running pytest."
        )

    native_path = max(native_paths, key=lambda path: path.stat().st_mtime)
    source_path = max(_native_source_paths(), key=lambda path: path.stat().st_mtime)

    if source_path.stat().st_mtime > native_path.stat().st_mtime:
        native_rel = native_path.relative_to(_REPO_ROOT)
        source_rel = source_path.relative_to(_REPO_ROOT)
        return (
            "Native extension is stale: "
            f"{native_rel} is older than {source_rel}. "
            "Run `make build` or `uv run maturin develop` before running pytest."
        )

    return None


def pytest_sessionstart(session: pytest.Session) -> None:
    message = _stale_native_build_message()
    if message is not None:
        pytest.exit(message, returncode=2)


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _case_id(case: Any) -> str:
    return case.case_id


@pytest.fixture(autouse=True)
def reset_streamable_harness_state() -> None:
    reset_streaming_harness_caches()


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
