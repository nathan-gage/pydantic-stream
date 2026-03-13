"""Pytest and Hypothesis configuration for unit/integration tests."""

from __future__ import annotations

import os
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
from .helpers import SourceFactory

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
