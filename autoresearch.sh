#!/bin/bash
set -euo pipefail

PGO_DIR=/tmp/pydantic_stream_pgo

# ─── Step 1: Instrumented build for profile generation ────────────────────
rm -rf "$PGO_DIR" && mkdir -p "$PGO_DIR"
RUSTFLAGS="-C profile-generate=$PGO_DIR" uv run maturin develop --release -q 2>&1 | grep -v "^$" || true

# ─── Step 2: Run a representative workload to collect profile data ─────────
uv run pytest benchmarks/test_memory_benchmarks.py \
  -m "not large_payload" \
  -k "stream_basemodel" \
  --benchmark-enable -q --no-header 2>&1 >/dev/null || true

# ─── Step 3: Merge profile data ───────────────────────────────────────────
/Library/Developer/CommandLineTools/usr/bin/llvm-profdata \
  merge -o "$PGO_DIR/merged.profdata" "$PGO_DIR"/*.profraw 2>/dev/null || true

# ─── Step 4: PGO-optimized build ──────────────────────────────────────────
RUSTFLAGS="-C profile-use=$PGO_DIR/merged.profdata" \
  uv run maturin develop --release -q 2>&1 | grep -v "^$" || true

# ─── Step 5: Run the actual timing benchmark ──────────────────────────────
uv run pytest benchmarks/test_memory_benchmarks.py \
  -m "not large_payload" \
  -k "stream_basemodel or stream_dataclass_slots" \
  --benchmark-enable \
  --benchmark-json=/tmp/autoresearch_bench.json \
  -q --no-header 2>&1 | tail -5

# ─── Parse results ────────────────────────────────────────────────────────
python3 - <<'EOF'
import json, sys
with open("/tmp/autoresearch_bench.json") as f:
    data = json.load(f)
benchmarks = data.get("benchmarks", [])
total_ms = sum(b["stats"]["mean"] * 1000 for b in benchmarks)
for b in benchmarks:
    name = b["name"].replace("test_", "").replace("[", "_").replace("]", "")
    mean_ms = b["stats"]["mean"] * 1000
    print(f"METRIC {name}={mean_ms:.4f}")
print(f"METRIC total_ms={total_ms:.4f}")
EOF
