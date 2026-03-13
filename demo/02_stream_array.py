"""StreamArray — lazy array access.

Wraps raw JSON bytes and projects/validates only the items you touch.
sa[500] skips straight to record 500 in Rust without parsing 0-499.
Slicing, step-slicing, iteration, and to_list() are all supported.

    uv run python demo/02_stream_array.py
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


# -- Build 1000 fat records ------------------------------------------------

charset = string.ascii_letters + string.digits
records = []
for i in range(1000):
    rec: dict = {
        "id": i,
        "title": f"Contract-{i:05d}",
        "amount": round(random.uniform(10_000, 10_000_000), 2),
        "agency": f"Agency-{i % 20}",
        "status": random.choice(["active", "completed", "pending"]),
    }
    for j in range(20):  # 20 junk fields per record
        rec[f"_extra_{j}"] = "".join(random.choices(charset, k=200))
    records.append(rec)

data = json.dumps(records, separators=(",", ":")).encode()

# -- Create a StreamArray — holds raw bytes, no parsing yet -----------------

sa = Contract.stream_model_validate_json_array(data)

print(repr(sa))
"""StreamArray(4385486 bytes)"""

# -- Index single items — only the requested record is projected ------------

print(f"sa[0]   = {sa[0]}")
print(f"sa[500] = {sa[500]}")
print(f"sa[999] = {sa[999]}")
"""
sa[0]   = id=0 title='Contract-00000' amount=9148681.37 agency='Agency-0' status='completed'
sa[500] = id=500 title='Contract-00500' amount=7900906.76 agency='Agency-0' status='active'
sa[999] = id=999 title='Contract-00999' amount=187265.96 agency='Agency-19' status='completed'
"""

# -- Slicing — only the sliced range is projected ---------------------------

print(f"sa[10:13] = {sa[10:13]}")
"""sa[10:13] = [Contract(id=10, ...), Contract(id=11, ...), Contract(id=12, ...)]"""

every_200th = sa[::200]
print(f"sa[::200] = {len(every_200th)} items")
for c in every_200th:
    print(f"  id={c.id}  {c.title}")
"""
sa[::200] = 5 items
  id=0  Contract-00000
  id=200  Contract-00200
  id=400  Contract-00400
  id=600  Contract-00600
  id=800  Contract-00800
"""

# -- Full materialization: one Rust pass, one pydantic pass -----------------

all_items = sa.to_list()
print(f"to_list() = {len(all_items)} items")
"""to_list() = 1000 items"""
