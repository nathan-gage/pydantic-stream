"""Single object projection.

Define a model with 5 fields, feed it a JSON blob with 35+, get back only
what you asked for. The extra fields are skipped in Rust at parse time —
they're never allocated as Python objects and never reach pydantic.

    uv run python demo/01_basics.py
"""

from __future__ import annotations

import json
import random
import string
from typing import Literal

from pydantic_stream import StreamingBaseModelMixin

# -- Model: only the fields we care about -----------------------------------


class Contract(StreamingBaseModelMixin):
    id: int
    title: str
    amount: float
    agency: str
    status: Literal["active", "completed", "pending"]


# -- Build a fat JSON blob (simulating a real API response) -----------------

charset = string.ascii_letters + string.digits
record: dict = {
    "id": 42,
    "title": "Contract-00042",
    "amount": 1_234_567.89,
    "agency": "Agency-2",
    "status": "active",
    # fields we don't need but the API sends anyway
    "created_at": "2024-01-15T09:30:00Z",
    "updated_at": "2025-03-10T14:22:00Z",
    "contracting_officer": "Jane Smith",
    "contracting_officer_email": "jsmith@agency.gov",
    "place_of_performance_city": "Washington",
    "place_of_performance_state": "DC",
    "place_of_performance_zip": "20001",
    "naics_code": "541512",
    "naics_description": "Computer Systems Design Services",
    "psc_code": "D302",
    "set_aside_type": "Total Small Business",
    "solicitation_id": "SOL-2024-00042",
    "parent_award_id": "CONT_AWD_0001",
    "funding_agency": "Department of Defense",
    "awarding_sub_agency": "Army",
}
# bulk noise — analytics blobs, embeddings, audit trails
for j in range(15):
    record[f"_analytics_field_{j}"] = "".join(random.choices(charset, k=300))

input_bytes = json.dumps(record, separators=(",", ":")).encode()

# -- Project + validate in one call -----------------------------------------
# Rust scans the JSON, copies only the 5 known fields into a new compact
# buffer, and hands that to pydantic. The 30 junk fields are never parsed.

contract = Contract.stream_model_validate_json(input_bytes)
output_bytes = contract.model_dump_json().encode()
reduction = (1 - len(output_bytes) / len(input_bytes)) * 100

print(f"Input:     {len(input_bytes):>6,} bytes ({len(record)} fields)")
print(f"Output:    {len(output_bytes):>6,} bytes ({len(contract.model_dump())} fields)")
print(f"Reduction: {reduction:.1f}%")
print()
print(contract)

"""
Input:      5,496 bytes (35 fields)
Output:        92 bytes (5 fields)
Reduction: 98.3%

id=42 title='Contract-00042' amount=1234567.89 agency='Agency-2' status='active'
"""
