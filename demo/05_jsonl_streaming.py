"""JSONL streaming.

Two paths for newline-delimited JSON:
  - Iterator source → projects each line as it arrives, O(1) memory
  - Bytes source    → projects all lines at once, returns a list

Both strip unknown fields per-line through the same Rust projector.

    uv run python demo/05_jsonl_streaming.py
"""

from __future__ import annotations

import json
import random
import string
from typing import Literal

from pydantic_stream import StreamingBaseModelMixin


class Contract(StreamingBaseModelMixin):
    id: int
    title: str
    amount: float
    agency: str
    status: Literal["active", "completed", "pending"]


charset = string.ascii_letters + string.digits


def make_record(i: int) -> dict:
    rec: dict = {
        "id": i,
        "title": f"Contract-{i:05d}",
        "amount": round(random.uniform(10_000, 10_000_000), 2),
        "agency": f"Agency-{i % 20}",
        "status": random.choice(["active", "completed", "pending"]),
    }
    for j in range(15):
        rec[f"_extra_{j}"] = "".join(random.choices(charset, k=200))
    return rec


# -- Streaming path: iterator of lines, O(1) memory ------------------------
# Pass any iterator of JSON bytes/strings. Each line is projected in Rust
# and validated individually — nothing accumulates in memory.


def line_generator():
    """Simulates a log stream or chunked JSONL download."""
    for i in range(200):
        yield json.dumps(make_record(i), separators=(",", ":")).encode()


count = 0
first = last = None
for contract in Contract.stream_model_validate_jsonl_iter(line_generator()):
    if first is None:
        first = contract
    last = contract
    count += 1

print(f"Streaming: {count} records")
print(f"  first: {first}")
print(f"  last:  {last}")

"""
Streaming: 200 records
  first: id=0 title='Contract-00000' amount=6810539.99 agency='Agency-0' status='active'
  last:  id=199 title='Contract-00199' amount=9673906.46 agency='Agency-19' status='pending'
"""

# -- Eager path: full bytes blob → list ------------------------------------
# When you already have all the JSONL in memory, this projects every line
# at once and returns a list.

jsonl_blob = b"\n".join(
    json.dumps(make_record(i), separators=(",", ":")).encode() for i in range(200)
)

results = Contract.stream_model_validate_jsonl(jsonl_blob)

print()
print(f"Eager: {len(results)} records from {len(jsonl_blob) / 1000:.1f} KB")

"""
Eager: 200 records from 662.0 KB
"""
