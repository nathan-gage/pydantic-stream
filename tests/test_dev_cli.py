from __future__ import annotations

import sys
from argparse import Namespace

from tools.dev import build_bench_commands, build_build_command, build_test_commands


def test_build_test_commands_runs_cargo_and_pytest_by_default() -> None:
    args = Namespace(
        python_only=False,
        rust_only=False,
        target="tests/",
        k="chunked",
        quiet=True,
        pytest_arg=["-x"],
        cargo_arg=["--lib"],
        rust_filter="streaming",
    )

    commands = build_test_commands(args)

    assert commands == [
        ["cargo", "test", "streaming", "--lib"],
        [sys.executable, "-m", "pytest", "tests/", "-k", "chunked", "-q", "-x"],
    ]


def test_build_test_commands_respects_rust_only() -> None:
    args = Namespace(
        python_only=False,
        rust_only=True,
        target="tests/",
        k=None,
        quiet=False,
        pytest_arg=[],
        cargo_arg=[],
        rust_filter=None,
    )

    assert build_test_commands(args) == [["cargo", "test"]]


def test_build_bench_commands_includes_save_compare_and_shape_flags() -> None:
    args = Namespace(
        target="benchmarks/",
        large=True,
        no_memory=True,
        shape=["default", "wide"],
        json="before.json",
        compare_json="after.json",
        k="stream_basemodel",
        quiet=True,
        pytest_arg=["-x"],
    )

    commands = build_bench_commands(args)

    assert commands == [
        [
            sys.executable,
            "-m",
            "pytest",
            "benchmarks/",
            "--benchmark-enable",
            "--large-payload",
            "--no-memory",
            "--payload-shape",
            "default",
            "--payload-shape",
            "wide",
            "--benchmark-json",
            "before.json",
            "--benchmark-compare-json",
            "after.json",
            "-k",
            "stream_basemodel",
            "-q",
            "-x",
        ],
    ]


def test_build_build_command_supports_release_mode() -> None:
    args = Namespace(release=True)

    command = build_build_command(args)

    assert command[-2:] == ["develop", "--release"]
