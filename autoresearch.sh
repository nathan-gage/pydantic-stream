#!/bin/bash
set -euo pipefail

# Build release extension first
uv run maturin develop --release -q 2>&1 | grep -v "^$" || true

# Run the standard timing benchmarks (not large_payload)
# We measure only stream_basemodel and stream_dataclass_slots since those
# are the targets we're optimizing. Output mean times per shape.
RESULTS=$(uv run pytest benchmarks/test_memory_benchmarks.py \
  -m "not large_payload" \
  -k "stream_basemodel or stream_dataclass_slots" \
  --benchmark-enable \
  --benchmark-json=/tmp/autoresearch_bench.json \
  -q --no-header 2>&1 | tail -5)

# Parse the benchmark JSON for mean times
TOTAL_MS=$(python3 - <<'EOF'
import json, sys
with open("/tmp/autoresearch_bench.json") as f:
    data = json.load(f)
benchmarks = data.get("benchmarks", [])
total_ms = sum(b["stats"]["mean"] * 1000 for b in benchmarks)
# Also emit per-benchmark metrics
for b in benchmarks:
    name = b["name"].replace("test_", "").replace("[", "_").replace("]", "")
    mean_ms = b["stats"]["mean"] * 1000
    print(f"METRIC {name}={mean_ms:.4f}")
print(f"METRIC total_ms={total_ms:.4f}")
EOF
)

echo "$TOTAL_MS"
