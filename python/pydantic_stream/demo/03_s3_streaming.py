"""Chunked streaming — the headline feature.

Streams a large JSON array through Rust projection in bounded memory.
Each record is projected + validated as it arrives; the full payload is
never loaded into Python. Works with any source that has `.read(n)` —
boto3 StreamingBody, httpx streams, open files, etc.

    uv run python demo/03_s3_streaming.py
"""

from __future__ import annotations

import io
import json
import random
import string
import time
from typing import Iterator, Literal

from pydantic_stream import StreamingBaseModelMixin


class Contract(StreamingBaseModelMixin):
    id: int
    title: str
    amount: float
    agency: str
    status: Literal["active", "completed", "pending"]


# -- FakeS3Body: drop-in for boto3's StreamingBody -------------------------
# The real StreamingBody has .read(n) — that's all we need.


class FakeS3Body:
    """Simulates s3.get_object()["Body"]. Has .read(n) like the real thing."""

    def __init__(self, data: bytes) -> None:
        self._stream = io.BytesIO(data)

    def read(self, amt: int | None = None) -> bytes:
        return self._stream.read(amt)  # type: ignore[arg-type]

    def iter_chunks(self, chunk_size: int = 1024) -> Iterator[bytes]:
        while True:
            chunk = self._stream.read(chunk_size)
            if not chunk:
                break
            yield chunk


# -- Generate a ~13 MB payload: 2000 records with 30 junk fields each ------

charset = string.ascii_letters + string.digits
payload_records = []
for i in range(2000):
    rec: dict = {
        "id": i,
        "title": f"Contract-{i:05d}",
        "amount": round(random.uniform(10_000, 10_000_000), 2),
        "agency": f"Agency-{i % 20}",
        "status": random.choice(["active", "completed", "pending"]),
    }
    for j in range(30):
        rec[f"_extra_{j}"] = "".join(random.choices(charset, k=200))
    payload_records.append(rec)

data = json.dumps(payload_records, separators=(",", ":")).encode()
body = FakeS3Body(data)

# -- Stream it: 1 MB chunks, bounded memory --------------------------------
# stream_model_validate_json_array_iter reads chunk_size bytes at a time,
# projects each complete record in Rust, and yields validated models.
# Peak memory is bounded by chunk_size, not by file size.

start = time.perf_counter()
count = 0
projected_bytes = 0

for contract in Contract.stream_model_validate_json_array_iter(body, chunk_size=1_048_576):
    projected_bytes += len(contract.model_dump_json().encode())
    count += 1
    if count % 500 == 0:
        print(f"  {count:>5} records  ({time.perf_counter() - start:.2f}s)")

elapsed = time.perf_counter() - start
ratio = len(data) / projected_bytes if projected_bytes else 0

print()
print(f"Records:   {count}")
print(f"Input:     {len(data) / 1_000_000:.1f} MB")
print(f"Projected: {projected_bytes / 1_000:.1f} KB")
print(f"Ratio:     {ratio:.0f}x smaller")
print(f"Time:      {elapsed:.2f}s")

"""
    500 records  (0.00s)
   1000 records  (0.00s)
   1500 records  (0.01s)
   2000 records  (0.01s)

Records:   2000
Input:     13.1 MB
Projected: 190.1 KB
Ratio:     69x smaller
Time:      0.01s
"""

# With real S3, swap in the actual body:
#
#   body = s3.get_object(Bucket="b", Key="contracts.json")["Body"]
#   for c in Contract.stream_model_validate_json_array_iter(body):
#       process(c)
