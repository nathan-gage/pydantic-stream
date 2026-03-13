"""Dataclass mixin — same Rust projection, different Python type.

StreamingDataclassMixin gives pydantic dataclasses the same API as
StreamingBaseModelMixin. Single object, array, chunked streaming,
JSONL — all work identically. Pick whichever type you prefer.

Note the method names drop the "model_" prefix:
  stream_validate_json (not stream_model_validate_json)

    uv run python demo/06_dataclass_api.py
"""

from __future__ import annotations

import io
import json
import random
import string
from typing import Literal

from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic_stream import StreamingDataclassMixin


@pydantic_dataclass
class Contract(StreamingDataclassMixin):
    id: int
    title: str
    amount: float
    agency: str
    status: Literal["active", "completed", "pending"]


charset = string.ascii_letters + string.digits


def make_record(i: int, extra: int = 20) -> dict:
    rec: dict = {
        "id": i,
        "title": f"Contract-{i:05d}",
        "amount": round(random.uniform(10_000, 10_000_000), 2),
        "agency": f"Agency-{i % 20}",
        "status": random.choice(["active", "completed", "pending"]),
    }
    for j in range(extra):
        rec[f"_extra_{j}"] = "".join(random.choices(charset, k=200))
    return rec


def make_array_bytes(n: int, extra: int = 20) -> bytes:
    return json.dumps(
        [make_record(i, extra) for i in range(n)],
        separators=(",", ":"),
    ).encode()


class FakeS3Body:
    def __init__(self, data: bytes) -> None:
        self._stream = io.BytesIO(data)

    def read(self, amt: int | None = None) -> bytes:
        return self._stream.read(amt)  # type: ignore[arg-type]


# -- Single object ----------------------------------------------------------

obj_bytes = json.dumps(make_record(42), separators=(",", ":")).encode()
c = Contract.stream_validate_json(obj_bytes)
print(f"Single:    {c}")
"""Single:    Contract(id=42, title='Contract-00042', amount=8555366.78, agency='Agency-2', status='completed')"""

# -- Array (StreamArray) ----------------------------------------------------

sa = Contract.stream_validate_json_array(make_array_bytes(100, extra=10))
print(f"Array:     {sa!r}  sa[0]={sa[0]}")
"""Array:     StreamArray(223465 bytes)  sa[0]=Contract(id=0, title='Contract-00000', ...)"""

# -- Chunked streaming ------------------------------------------------------

body = FakeS3Body(make_array_bytes(500, extra=15))
count = sum(1 for _ in Contract.stream_validate_json_array_iter(body, chunk_size=65_536))
print(f"Chunked:   {count} records streamed")
"""Chunked:   500 records streamed"""

# -- JSONL ------------------------------------------------------------------

jsonl = b"\n".join(json.dumps(make_record(i, extra=10), separators=(",", ":")).encode() for i in range(50))
results = Contract.stream_validate_jsonl(jsonl)
print(f"JSONL:     {len(results)} records")
"""JSONL:     50 records"""
