"""HTTP streaming demo — server + client in one script.

Downloads JSON from a local aiohttp server and streams it through every
consumption pattern pydantic-stream offers: StreamArray, chunked streaming,
root_prefix navigation, JSONL, and the projection-free TypeAdapter path.

    uv run --with aiohttp python demo/http_streaming.py
"""
from __future__ import annotations

import asyncio
import json
import random
import string
import time
import uuid
from io import BytesIO
from typing import Any

import aiohttp
from aiohttp import web
from pydantic import TypeAdapter

from pydantic_stream import (
    StreamingBaseModelMixin,
    compile_model_spec,
    project_array_items_partial,
    stream_json_array,
)

# ---------------------------------------------------------------------------
# Model — 5 useful fields, rest is noise
# ---------------------------------------------------------------------------


class Event(StreamingBaseModelMixin):
    id: int
    timestamp: str
    user_id: int
    action: str
    tags: list[str]


# ---------------------------------------------------------------------------
# Data generation (~50 MB with noise)
# ---------------------------------------------------------------------------

NUM_RECORDS = 10_000
CHUNK_SIZE = 256 * 1024  # 256 KB for chunked streaming

ACTIONS = ["click", "view", "purchase", "signup", "logout", "scroll", "search"]
CHARSET = string.ascii_letters + string.digits


def _make_record(i: int) -> dict[str, Any]:
    rec: dict[str, Any] = {
        "id": i,
        "timestamp": f"2026-03-13T{i % 24:02d}:{i % 60:02d}:00Z",
        "user_id": random.randint(1, 50_000),
        "action": random.choice(ACTIONS),
        "tags": random.sample(["web", "mobile", "api", "internal", "beta", "prod"], k=random.randint(1, 4)),
    }
    # ~8 noise fields to make projection dramatic
    rec["_trace_id"] = str(uuid.uuid4())
    rec["_session_id"] = str(uuid.uuid4())
    rec["_metadata"] = {
        "browser": "".join(random.choices(CHARSET, k=60)),
        "os": "".join(random.choices(CHARSET, k=40)),
        "ip": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(0,255)}",
        "geo": {"lat": random.uniform(-90, 90), "lon": random.uniform(-180, 180)},
    }
    rec["_feature_vector"] = [random.random() for _ in range(64)]
    rec["_raw_payload"] = "".join(random.choices(CHARSET, k=800))
    rec["_audit_log"] = [
        {"ts": f"2026-03-13T00:{j:02d}:00Z", "msg": "".join(random.choices(CHARSET, k=120))}
        for j in range(5)
    ]
    rec["_previous_actions"] = [random.choice(ACTIONS) for _ in range(20)]
    rec["_ab_groups"] = {f"exp_{j}": random.choice(["control", "variant_a", "variant_b"]) for j in range(10)}
    return rec


print("Generating test data …")
t0 = time.perf_counter()
records = [_make_record(i) for i in range(NUM_RECORDS)]

ARRAY_BYTES = json.dumps(records, separators=(",", ":")).encode()
NESTED_BYTES = json.dumps({"data": {"results": records}}, separators=(",", ":")).encode()
JSONL_BYTES = b"\n".join(json.dumps(r, separators=(",", ":")).encode() for r in records)

gen_time = time.perf_counter() - t0
print(f"  {NUM_RECORDS:,} records in {gen_time:.2f}s")
print(f"  array.json  = {len(ARRAY_BYTES) / 1e6:.1f} MB")
print(f"  nested.json = {len(NESTED_BYTES) / 1e6:.1f} MB")
print(f"  items.jsonl  = {len(JSONL_BYTES) / 1e6:.1f} MB")

# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------

PORT: int = 0  # assigned by OS


async def handle_array(_req: web.Request) -> web.Response:
    return web.Response(body=ARRAY_BYTES, content_type="application/json")


async def handle_nested(_req: web.Request) -> web.Response:
    return web.Response(body=NESTED_BYTES, content_type="application/json")


async def handle_jsonl(_req: web.Request) -> web.Response:
    return web.Response(body=JSONL_BYTES, content_type="application/x-ndjson")


async def server(ready: asyncio.Event) -> web.AppRunner:
    app = web.Application()
    app.router.add_get("/array.json", handle_array)
    app.router.add_get("/nested.json", handle_nested)
    app.router.add_get("/items.jsonl", handle_jsonl)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()

    global PORT
    sock = site._server.sockets[0]  # type: ignore[union-attr]
    PORT = sock.getsockname()[1]
    ready.set()

    # Keep running until cancelled
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    finally:
        await runner.cleanup()
    return runner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def header(title: str) -> None:
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


# ---------------------------------------------------------------------------
# Client — 5 approaches
# ---------------------------------------------------------------------------


async def approach_1_eager_stream_array(session: aiohttp.ClientSession) -> None:
    """Eager download → StreamArray: random access, slicing, bulk."""
    header("1. Eager download → StreamArray")
    t0 = time.perf_counter()
    async with session.get(f"http://127.0.0.1:{PORT}/array.json") as resp:
        data = await resp.read()
    download_ms = (time.perf_counter() - t0) * 1000

    t1 = time.perf_counter()
    arr = Event.stream_model_validate_json_array(data)
    construct_ms = (time.perf_counter() - t1) * 1000

    print(f"  Downloaded {len(data) / 1e6:.1f} MB in {download_ms:.0f} ms")
    print(f"  StreamArray created in {construct_ms:.1f} ms (lazy — no parsing yet)")
    print(f"  repr: {repr(arr)}")

    # Random access
    print(f"  arr[0]    = {arr[0]}")
    print(f"  arr[9999] = {arr[9999]}")

    # Step-slice
    sample = arr[::2500]
    print(f"  arr[::2500] = {len(sample)} items:")
    for e in sample:
        print(f"    id={e.id}  action={e.action}  tags={e.tags}")

    # Bulk materialization
    t2 = time.perf_counter()
    all_items = arr.to_list()
    mat_ms = (time.perf_counter() - t2) * 1000
    print(f"  to_list() = {len(all_items):,} items in {mat_ms:.0f} ms")


async def approach_2_chunked_streaming(session: aiohttp.ClientSession) -> None:
    """Async chunked streaming with manual project_array_items_partial."""
    header("2. Async chunked streaming (projection + streaming)")
    spec = Event._streaming_spec()
    adapter = Event._streaming_adapter()

    t0 = time.perf_counter()
    count = 0
    first = last = None
    buffer = bytearray()
    is_start = True

    async with session.get(f"http://127.0.0.1:{PORT}/array.json") as resp:
        async for chunk in resp.content.iter_chunked(CHUNK_SIZE):
            buffer.extend(chunk)
            items, consumed, finished = project_array_items_partial(bytes(buffer), spec, is_start)
            for item_bytes in items:
                event = adapter.validate_json(item_bytes)
                if first is None:
                    first = event
                last = event
                count += 1
            del buffer[:consumed]
            is_start = False
            if finished:
                break

    elapsed_ms = (time.perf_counter() - t0) * 1000
    print(f"  Streamed {count:,} events in {elapsed_ms:.0f} ms (chunk_size={CHUNK_SIZE // 1024} KB)")
    print(f"  Peak buffer ≈ {CHUNK_SIZE // 1024} KB (bounded by chunk_size)")
    print(f"  first: {first}")
    print(f"  last:  {last}")


async def approach_3_nested_root_prefix(session: aiohttp.ClientSession) -> None:
    """Nested JSON + root_prefix navigation."""
    header("3. Nested JSON + root_prefix")
    t0 = time.perf_counter()
    async with session.get(f"http://127.0.0.1:{PORT}/nested.json") as resp:
        data = await resp.read()
    download_ms = (time.perf_counter() - t0) * 1000

    t1 = time.perf_counter()
    arr = Event.stream_model_validate_json_array(data, root_prefix="data.results")
    project_ms = (time.perf_counter() - t1) * 1000

    print(f"  Downloaded {len(data) / 1e6:.1f} MB in {download_ms:.0f} ms")
    print(f'  root_prefix="data.results" navigates {{"data": {{"results": [...]}}}}')
    print(f"  StreamArray created in {project_ms:.1f} ms")
    print(f"  arr[0]    = {arr[0]}")
    print(f"  arr[9999] = {arr[9999]}")

    t2 = time.perf_counter()
    items = arr.to_list()
    mat_ms = (time.perf_counter() - t2) * 1000
    print(f"  to_list() = {len(items):,} items in {mat_ms:.0f} ms")


async def approach_4_jsonl(session: aiohttp.ClientSession) -> None:
    """JSONL — both eager (Rust batch) and iterator (per-line) paths."""
    header("4. JSONL streaming")

    async with session.get(f"http://127.0.0.1:{PORT}/items.jsonl") as resp:
        data = await resp.read()

    # 4a — Eager: bytes blob → Rust project_jsonl batch path
    print("  4a. Eager batch (project_jsonl in Rust)")
    t0 = time.perf_counter()
    count_a = 0
    first_a = last_a = None
    for event in Event.stream_model_validate_jsonl_iter(data):
        if first_a is None:
            first_a = event
        last_a = event
        count_a += 1
    elapsed_a = (time.perf_counter() - t0) * 1000
    print(f"      {count_a:,} events in {elapsed_a:.0f} ms")
    print(f"      first: {first_a}")
    print(f"      last:  {last_a}")

    # 4b — Iterator: line-by-line → per-line project_object path
    print("  4b. Line iterator (per-line project_object)")
    lines = data.split(b"\n")
    t1 = time.perf_counter()
    count_b = 0
    first_b = last_b = None
    for event in Event.stream_model_validate_jsonl_iter(iter(lines)):
        if first_b is None:
            first_b = event
        last_b = event
        count_b += 1
    elapsed_b = (time.perf_counter() - t1) * 1000
    print(f"      {count_b:,} events in {elapsed_b:.0f} ms")
    print(f"      first: {first_b}")
    print(f"      last:  {last_b}")


async def approach_5_no_projection(session: aiohttp.ClientSession) -> None:
    """No-projection streaming via stream_json_array + TypeAdapter."""
    header("5. No-projection streaming (TypeAdapter path)")
    async with session.get(f"http://127.0.0.1:{PORT}/array.json") as resp:
        data = await resp.read()

    adapter: TypeAdapter[Event] = TypeAdapter(Event)

    t0 = time.perf_counter()
    count = 0
    first = last = None
    for event in stream_json_array(BytesIO(data), adapter, chunk_size=CHUNK_SIZE):
        if first is None:
            first = event
        last = event
        count += 1
    elapsed_ms = (time.perf_counter() - t0) * 1000

    print(f"  stream_json_array (no projection, pydantic drops extras)")
    print(f"  {count:,} events in {elapsed_ms:.0f} ms")
    print(f"  first: {first}")
    print(f"  last:  {last}")


# ---------------------------------------------------------------------------
# Projection summary
# ---------------------------------------------------------------------------


def projection_summary() -> None:
    header("Projection summary")
    spec = Event._streaming_spec()
    from pydantic_stream import project_array, project_object

    raw_size = len(ARRAY_BYTES)
    projected_bytes = project_array(ARRAY_BYTES, spec)
    projected_size = len(projected_bytes)

    sample_raw = json.dumps(records[0], separators=(",", ":")).encode()
    sample_projected = project_object(sample_raw, spec)

    print(f"  Single record: {len(sample_raw):,} B raw → {len(sample_projected):,} B projected")
    print(f"  Raw payload:       {raw_size / 1e6:6.1f} MB")
    print(f"  Projected:         {projected_size / 1e6:6.1f} MB")
    print(f"  Reduction:         {(1 - projected_size / raw_size) * 100:.1f}%")
    print(f"\n  Raw:       {sample_raw[:120]}…")
    print(f"  Projected: {sample_projected}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def main() -> None:
    ready = asyncio.Event()
    server_task = asyncio.create_task(server(ready))
    await ready.wait()
    print(f"\nServer listening on http://127.0.0.1:{PORT}")

    async with aiohttp.ClientSession() as session:
        await approach_1_eager_stream_array(session)
        await approach_2_chunked_streaming(session)
        await approach_3_nested_root_prefix(session)
        await approach_4_jsonl(session)
        await approach_5_no_projection(session)

    projection_summary()

    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
