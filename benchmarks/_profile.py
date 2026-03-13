"""Profiling script for py-spy flamegraph generation.

Usage:
    sudo py-spy record -o flamegraph.svg --native -- \
        uv run python -m benchmarks._profile
"""

import random

from ._data_gen import make_benchmark_payload_bytes
from ._models import BenchUser_StreamDC

random.seed(42)
payload = make_benchmark_payload_bytes(500)

# Warm up
list(BenchUser_StreamDC.stream_validate_json_array(payload))

# Hot loop — enough iterations for py-spy to get good samples
for _ in range(200):
    list(BenchUser_StreamDC.stream_validate_json_array(payload))
