"""Usage-oriented smoke tests that show the public streaming API directly."""

from __future__ import annotations

import asyncio
import io

from pydantic_stream import StreamArray

from .cases import HarnessUserDataclass, HarnessUserModel, json_bytes, json_source, jsonl_bytes

PAYLOADS = [
    {
        "id": 1,
        "name": "Ada",
        "address": {"city": "NYC", "zip": 10001, "junk": {"nested": [1, 2, 3]}},
        "ignore_me": {"huge": [1, 2, 3]},
    },
    {
        "id": "2",
        "name": "Grace",
    },
]


def test_basemodel_usage_example_covers_all_streaming_entry_points() -> None:
    async def collect_array_async() -> list[HarnessUserModel]:
        return [
            item
            async for item in HarnessUserModel.stream_model_validate_json_array_aiter(
                io.BytesIO(json_bytes(PAYLOADS)), chunk_size=16
            )
        ]

    async def collect_jsonl_async() -> list[HarnessUserModel]:
        return [
            item
            async for item in HarnessUserModel.stream_model_validate_jsonl_aiter(
                io.BytesIO(jsonl_bytes(PAYLOADS)), chunk_size=16
            )
        ]

    single = HarnessUserModel.stream_model_validate_json(json_source(PAYLOADS[0]))
    array = HarnessUserModel.stream_model_validate_json_array(json_source(PAYLOADS))
    array_iter = list(
        HarnessUserModel.stream_model_validate_json_array_iter(
            io.BytesIO(json_bytes(PAYLOADS)), chunk_size=16
        )
    )
    array_aiter = asyncio.run(collect_array_async())
    jsonl_iter = list(
        HarnessUserModel.stream_model_validate_jsonl_iter(io.BytesIO(jsonl_bytes(PAYLOADS)))
    )
    jsonl_aiter = asyncio.run(collect_jsonl_async())

    assert single.id == 1
    assert single.address is not None
    assert single.address.city == "NYC"
    assert isinstance(array, StreamArray)
    assert [item.id for item in array] == [1, 2]
    assert [item.id for item in array.to_list()] == [1, 2]
    assert [item.name for item in array_iter] == ["Ada", "Grace"]
    assert [item.name for item in array_aiter] == ["Ada", "Grace"]
    assert [item.name for item in jsonl_iter] == ["Ada", "Grace"]
    assert [item.name for item in jsonl_aiter] == ["Ada", "Grace"]


def test_dataclass_usage_example_covers_all_streaming_entry_points() -> None:
    async def collect_array_async() -> list[HarnessUserDataclass]:
        return [
            item
            async for item in HarnessUserDataclass.stream_validate_json_array_aiter(
                io.BytesIO(json_bytes(PAYLOADS)), chunk_size=16
            )
        ]

    async def collect_jsonl_async() -> list[HarnessUserDataclass]:
        return [
            item
            async for item in HarnessUserDataclass.stream_validate_jsonl_aiter(
                io.BytesIO(jsonl_bytes(PAYLOADS)), chunk_size=16
            )
        ]

    single = HarnessUserDataclass.stream_validate_json(json_source(PAYLOADS[0]))
    array = HarnessUserDataclass.stream_validate_json_array(json_source(PAYLOADS))
    array_iter = list(
        HarnessUserDataclass.stream_validate_json_array_iter(
            io.BytesIO(json_bytes(PAYLOADS)), chunk_size=16
        )
    )
    array_aiter = asyncio.run(collect_array_async())
    jsonl_iter = list(
        HarnessUserDataclass.stream_validate_jsonl_iter(io.BytesIO(jsonl_bytes(PAYLOADS)))
    )
    jsonl_aiter = asyncio.run(collect_jsonl_async())

    assert single.id == 1
    assert single.address is not None
    assert single.address.city == "NYC"
    assert isinstance(array, StreamArray)
    assert [item.id for item in array] == [1, 2]
    assert [item.id for item in array.to_list()] == [1, 2]
    assert [item.name for item in array_iter] == ["Ada", "Grace"]
    assert [item.name for item in array_aiter] == ["Ada", "Grace"]
    assert [item.name for item in jsonl_iter] == ["Ada", "Grace"]
    assert [item.name for item in jsonl_aiter] == ["Ada", "Grace"]


def test_array_usage_example_supports_root_prefix() -> None:
    data = {"items": PAYLOADS}

    result = HarnessUserModel.stream_model_validate_json_array(
        json_bytes(data), root_prefix="items"
    )

    assert [item.id for item in result[0:2]] == [1, 2]
