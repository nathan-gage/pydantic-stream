"""Generate benchmark payloads with large unknown fields.

Shapes
------
- ``"default"``       — ~27 KB/record, mixed known + unknown fields (original shape)
- ``"wide"``          — ~27 KB/record, 200 flat unknown keys per record
- ``"deep"``          — ~27 KB/record, 8-level nesting in unknown fields
- ``"string-heavy"``  — ~270 KB/record, few records with huge string blobs
- ``"many-small"``    — ~200 B/record, no unknown fields at all
"""

import json
import os
import random
import string
from typing import Literal

PayloadShape = Literal["default", "wide", "deep", "string-heavy", "many-small"]

SHAPES: list[PayloadShape] = ["default", "wide", "deep", "string-heavy", "many-small"]

# Approximate record counts to reach ~100 MB per shape
LARGE_RECORD_COUNTS: dict[PayloadShape, int] = {
    "default": 3_750,
    "wide": 3_750,
    "deep": 4_500,
    "string-heavy": 390,
    "many-small": 630_000,
}


_CHARSET = string.ascii_letters + string.digits
# Pre-built 1 KB block for fast large-string generation
_BLOCK_SIZE = 1024
_BLOCK = "".join(random.choices(_CHARSET, k=_BLOCK_SIZE))


def _random_string(length: int) -> str:
    if length <= _BLOCK_SIZE:
        # Slice from a pre-built large buffer instead of calling random.choices per-call
        return _FAST_POOL[_fast_pool_offset(length) : _fast_pool_offset(length) + length]
    # Tile the pre-built block for large strings
    full, rem = divmod(length, _BLOCK_SIZE)
    parts = [_BLOCK] * full
    if rem:
        parts.append(_BLOCK[:rem])
    return "".join(parts)


# Pre-built 64 KB random pool — small-string slices come from here via round-robin
_FAST_POOL_SIZE = 65536
_FAST_POOL = "".join(random.choices(_CHARSET, k=_FAST_POOL_SIZE))
_fast_pool_counter = 0


def _fast_pool_offset(length: int) -> int:
    """Return a rotating offset into _FAST_POOL for the given length."""
    global _fast_pool_counter
    off = _fast_pool_counter % (_FAST_POOL_SIZE - length) if length < _FAST_POOL_SIZE else 0
    _fast_pool_counter += 7  # stride by a prime to spread values
    return off


def _known_fields(i: int) -> dict:
    """The known fields that every record shares (matches the benchmark models)."""
    return {
        "id": i,
        "name": f"user_{i}",
        "score": round(random.uniform(0.0, 100.0), 4),
        "active": i % 3 != 0,
        "address": {
            "city": f"City_{i % 500}",
            "country": f"Country_{i % 50}",
        },
        "tags": [f"tag_{j}" for j in range(5)],
    }


def _default_unknown() -> dict:
    """~25 KB of mixed unknown blobs (original shape)."""
    return {
        "_analytics_blob": {
            "raw_events": [
                {"ts": f"2025-01-{(j % 28) + 1:02d}T12:00:00Z", "value": _random_string(200)}
                for j in range(50)
            ],
            "summary": {
                "counts": list(range(200)),
                "labels": [_random_string(50) for _ in range(20)],
            },
        },
        "_audit_trail": [_random_string(200) for _ in range(20)],
        "_embedding_vector": [round(random.gauss(0, 1), 6) for _ in range(128)],
        "_raw_response_cache": {
            f"level1_{a}": {
                f"level2_{b}": {f"level3_{c}": _random_string(100) for c in range(5)}
                for b in range(5)
            }
            for a in range(3)
        },
    }


def _wide_unknown() -> dict:
    """~25 KB spread across 200 flat keys — stresses key-skipping throughput."""
    return {f"_extra_{k}": _random_string(125) for k in range(200)}


def _deep_unknown() -> dict:
    """~25 KB in an 8-level nested structure — stresses depth tracking.

    Uses a linear chain (1 child per level) with payload at each level
    to avoid exponential blowup while still exercising deep nesting.
    """
    node: dict = {"leaf": _random_string(3000)}
    for depth in range(8):
        node = {f"d{depth}": node, f"payload_{depth}": _random_string(2500)}
    return {"_nested": node}


def _string_heavy_unknown() -> dict:
    """~270 KB in a handful of huge string values — stresses string skipping."""
    return {
        "_blob_a": _random_string(100_000),
        "_blob_b": _random_string(100_000),
        "_blob_c": _random_string(70_000),
    }


def make_benchmark_record(i: int, *, include_unknown: bool = True) -> dict:
    """Build a single benchmark record (original ``"default"`` shape).

    Known fields: ~200 bytes (id, name, score, active, address, tags).
    Unknown fields: ~25 KB of nested blobs that streamable will skip.
    """
    record = _known_fields(i)
    if include_unknown:
        record.update(_default_unknown())
    return record


def make_shaped_record(i: int, shape: PayloadShape) -> dict:
    """Build a single benchmark record in the given *shape*."""
    record = _known_fields(i)
    if shape == "default":
        record.update(_default_unknown())
    elif shape == "wide":
        record.update(_wide_unknown())
    elif shape == "deep":
        record.update(_deep_unknown())
    elif shape == "string-heavy":
        record.update(_string_heavy_unknown())
    # "many-small" — no unknown fields
    return record


def make_benchmark_payload_bytes(n: int, *, include_unknown: bool = True) -> bytes:
    """Generate a JSON array of *n* benchmark records as bytes."""
    records = [make_benchmark_record(i, include_unknown=include_unknown) for i in range(n)]
    return json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def make_shaped_payload_bytes(shape: PayloadShape, n: int | None = None) -> bytes:
    """Generate a JSON array of *n* records in the given *shape* as bytes.

    When *n* is ``None``, uses ``LARGE_RECORD_COUNTS[shape]`` (~100 MB).

    For large payloads we build a small set of template records, serialise each
    once, then tile the JSON fragments with varying ``id``/``name`` fields.
    This avoids calling ``_random_string`` millions of times.
    """
    count = n if n is not None else LARGE_RECORD_COUNTS[shape]
    if count <= 500:
        # Small payload — generate each record individually (original path)
        records = [make_shaped_record(i, shape) for i in range(count)]
        return json.dumps(records, ensure_ascii=False, separators=(",", ":")).encode("utf-8")

    return _tile_large_payload(shape, count)


# Number of unique template records to cycle through for large payloads.
# Enough variety for realistic parsing without the cost of generating each one.
_TEMPLATE_POOL_SIZE = int(os.environ.get("BENCH_TEMPLATE_POOL", "8"))


def _tile_large_payload(shape: PayloadShape, count: int) -> bytes:
    """Build a large JSON array by tiling a small pool of serialised templates.

    For each record we only vary ``"id"`` and ``"name"`` (the first two keys);
    the rest of the record (including all unknown blobs) is reused verbatim from
    the pre-serialised template.  This makes generation O(pool) for the expensive
    random data, plus O(n) cheap string substitutions.
    """
    sep = b","
    _dumps = json.dumps

    # Build template pool — generate just _TEMPLATE_POOL_SIZE records
    templates: list[bytes] = []
    for t in range(_TEMPLATE_POOL_SIZE):
        rec = make_shaped_record(t, shape)
        # Serialise without id/name so we can stamp them per-record
        rec.pop("id")
        rec.pop("name")
        # Serialise the rest — we'll prepend {"id":N,"name":"user_N", to it
        body = _dumps(rec, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        # Strip the leading '{' so we can prepend our own key-values
        templates.append(body[1:])  # starts with ,"score":...}

    pool_size = len(templates)
    parts: list[bytes] = [b"["]
    for i in range(count):
        if i:
            parts.append(sep)
        prefix = f'{{"id":{i},"name":"user_{i}",'.encode()
        parts.append(prefix)
        parts.append(templates[i % pool_size])
    parts.append(b"]")
    return b"".join(parts)
