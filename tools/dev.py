"""Developer command runner for tests and benchmarks."""

from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
_maturin = Path(sys.executable).resolve().with_name("maturin")
MATURIN = str(_maturin) if _maturin.exists() else "maturin"


def build_test_commands(args: argparse.Namespace) -> list[list[str]]:
    commands: list[list[str]] = []

    if not args.python_only:
        cargo = ["cargo", "test"]
        if args.rust_filter:
            cargo.append(args.rust_filter)
        cargo.extend(args.cargo_arg)
        commands.append(cargo)

    if not args.rust_only:
        pytest = [sys.executable, "-m", "pytest", args.target]
        if args.k:
            pytest.extend(["-k", args.k])
        if args.quiet:
            pytest.append("-q")
        pytest.extend(args.pytest_arg)
        commands.append(pytest)

    return commands


def build_bench_commands(args: argparse.Namespace) -> list[list[str]]:
    pytest = [sys.executable, "-m", "pytest", args.target, "--benchmark-enable"]
    if args.large:
        pytest.append("--large-payload")
    if args.no_memory:
        pytest.append("--no-memory")
    for shape in args.shape:
        pytest.extend(["--payload-shape", shape])
    if args.json:
        pytest.extend(["--benchmark-json", args.json])
    if args.compare_json:
        pytest.extend(["--benchmark-compare-json", args.compare_json])
    if args.k:
        pytest.extend(["-k", args.k])
    if args.quiet:
        pytest.append("-q")
    pytest.extend(args.pytest_arg)

    return [pytest]


def build_build_command(args: argparse.Namespace) -> list[str]:
    command = [MATURIN, "develop"]
    if args.release:
        command.append("--release")
    return command


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run common test and benchmark workflows for pydantic-stream."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    test = subparsers.add_parser(
        "test",
        help="Run Rust and/or Python tests.",
        description="Run cargo tests and/or pytest against the tests/ tree.",
    )
    test.set_defaults(handler=run_test)
    mode = test.add_mutually_exclusive_group()
    mode.add_argument("--python-only", action="store_true", help="Run only the Python test suite.")
    mode.add_argument("--rust-only", action="store_true", help="Run only cargo tests.")
    test.add_argument(
        "--target",
        default="tests/",
        help="Pytest target for the Python test suite. Default: %(default)s",
    )
    test.add_argument("-k", default=None, help="Pytest -k expression for the Python test suite.")
    test.add_argument("-q", "--quiet", action="store_true", help="Pass -q to pytest.")
    test.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="Extra argument to pass through to pytest. Repeatable.",
    )
    test.add_argument(
        "--cargo-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="Extra argument to pass through to cargo test. Repeatable.",
    )
    test.add_argument(
        "--rust-filter",
        default=None,
        help="Optional cargo test filter expression.",
    )
    test.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")

    build = subparsers.add_parser(
        "build",
        help="Build the editable extension with maturin.",
        description="Build and install the local extension into the active environment.",
    )
    build.set_defaults(handler=run_build)
    build.add_argument(
        "--release",
        action="store_true",
        help="Build the release extension instead of the debug build.",
    )
    build.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")

    bench = subparsers.add_parser(
        "bench",
        help="Run benchmark suites.",
        description="Run pytest-benchmark suites with optional save/compare support.",
    )
    bench.set_defaults(handler=run_bench)
    bench.add_argument(
        "--target",
        default="benchmarks/",
        help="Benchmark target file or directory. Default: %(default)s",
    )
    bench.add_argument(
        "--shape",
        action="append",
        default=[],
        metavar="SHAPE",
        help="Restrict payload shape. Repeatable.",
    )
    bench.add_argument("--large", action="store_true", help="Enable large-payload benchmark cases.")
    bench.add_argument("--no-memory", action="store_true", help="Skip memray profiling.")
    bench.add_argument("--json", default=None, help="Write the benchmark artifact JSON to this path.")
    bench.add_argument(
        "--compare-json",
        default=None,
        help="Compare against a previous benchmark JSON artifact.",
    )
    bench.add_argument("-k", default=None, help="Pytest -k expression for benchmark selection.")
    bench.add_argument("-q", "--quiet", action="store_true", help="Pass -q to pytest.")
    bench.add_argument(
        "--pytest-arg",
        action="append",
        default=[],
        metavar="ARG",
        help="Extra argument to pass through to pytest. Repeatable.",
    )
    bench.add_argument("--dry-run", action="store_true", help="Print commands without executing them.")

    return parser


def run_test(args: argparse.Namespace) -> int:
    return _run_commands(build_test_commands(args), dry_run=args.dry_run)


def run_bench(args: argparse.Namespace) -> int:
    return _run_commands(build_bench_commands(args), dry_run=args.dry_run)


def run_build(args: argparse.Namespace) -> int:
    return _run_commands([build_build_command(args)], dry_run=args.dry_run)


def _run_commands(commands: list[list[str]], *, dry_run: bool) -> int:
    for command in commands:
        print(f"+ {shlex.join(command)}")
        if dry_run:
            continue
        subprocess.run(command, check=True, cwd=ROOT)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.handler(args)


if __name__ == "__main__":
    raise SystemExit(main())
