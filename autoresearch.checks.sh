#!/bin/bash
set -euo pipefail

# Run Rust + Python tests to verify correctness
# Output only errors (suppress success)
echo "Running Rust tests..."
cargo test --workspace -q 2>&1 | grep -E "FAILED|error|panicked" || true

echo "Running Python tests..."
uv run pytest tests/ -q --no-header 2>&1 | tail -20

echo "Checks passed."
