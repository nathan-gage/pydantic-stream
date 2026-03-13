"""Nested projection + root_prefix navigation.

Projection recurses into nested models — unknown fields are stripped at
every level, not just the top. And root_prefix lets you navigate into
wrapper objects like {"data": {"results": [...]}} before projecting.

    uv run python demo/04_nested_projection.py
"""

from __future__ import annotations

import json
from typing import Literal

from pydantic_stream import StreamingBaseModelMixin

# -- Nested models: Address lives inside Contract ---------------------------


class ContractAddress(StreamingBaseModelMixin):
    city: str
    state: str


class Contract(StreamingBaseModelMixin):
    id: int
    title: str
    amount: float
    agency: str
    status: Literal["active", "completed", "pending"]
    address: ContractAddress | None = None


# -- Part 1: nested field stripping -----------------------------------------
# Both outer junk (_analytics_blob etc.) AND inner junk (zip_code, county,
# lat, lon, census_tract inside address) are stripped in Rust.

record = {
    "id": 1,
    "title": "Contract-00001",
    "amount": 500_000.00,
    "agency": "Agency-5",
    "status": "active",
    "address": {
        "city": "Washington",
        "state": "DC",
        # junk at the nested level — all stripped
        "zip_code": "20001",
        "county": "District of Columbia",
        "lat": 38.9072,
        "lon": -77.0369,
        "census_tract": "000100",
    },
    # junk at the outer level — all stripped
    "_analytics_blob": {"events": list(range(100)), "summary": "x" * 500},
    "_audit_trail": ["created", "modified", "approved"],
    "_embedding": [0.1] * 128,
}

input_bytes = json.dumps(record, separators=(",", ":")).encode()
contract = Contract.stream_model_validate_json(input_bytes)

print("Nested projection")
print(f"  Input address fields: {list(record['address'].keys())}")
print(f"  Output: {contract}")
print(f"  address.city={contract.address.city}, address.state={contract.address.state}")
print("  zip_code, county, lat, lon, census_tract — all gone")

"""
Nested projection
  Input address fields: ['city', 'state', 'zip_code', 'county', 'lat', 'lon', 'census_tract']
  Output: id=1 title='Contract-00001' amount=500000.0 agency='Agency-5' status='active' address=ContractAddress(city='Washington', state='DC')
  address.city=Washington, address.state=DC
  zip_code, county, lat, lon, census_tract — all gone
"""  # noqa: E501

# -- Part 2: root_prefix navigation ----------------------------------------
# Real APIs often wrap arrays: {"data": {"results": [...], "count": N}}.
# root_prefix navigates to the array before projecting.

wrapped = {
    "metadata": {"version": "2.0", "timestamp": "2025-01-15"},
    "data": {
        "results": [
            {
                "id": i,
                "title": f"Contract-{i:05d}",
                "amount": 100_000.0 * i,
                "agency": f"Agency-{i}",
                "status": "active",
                "_noise": "x" * 500,
            }
            for i in range(5)
        ],
        "total_count": 5,
    },
}

wrapped_bytes = json.dumps(wrapped, separators=(",", ":")).encode()
sa = Contract.stream_model_validate_json_array(wrapped_bytes, root_prefix="data.results")

print()
print("root_prefix navigation")
print('  root_prefix="data.results" reaches into {"data": {"results": [...]}}')
for item in sa:
    print(f"  {item}")

"""
root_prefix navigation
  root_prefix="data.results" reaches into {"data": {"results": [...]}}
  id=0 title='Contract-00000' amount=0.0 agency='Agency-0' status='active' address=None
  id=1 title='Contract-00001' amount=100000.0 agency='Agency-1' status='active' address=None
  id=2 title='Contract-00002' amount=200000.0 agency='Agency-2' status='active' address=None
  id=3 title='Contract-00003' amount=300000.0 agency='Agency-3' status='active' address=None
  id=4 title='Contract-00004' amount=400000.0 agency='Agency-4' status='active' address=None
"""
