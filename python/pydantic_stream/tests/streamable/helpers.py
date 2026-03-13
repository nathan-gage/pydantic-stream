"""Low-level helpers for streamable tests."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Sequence

from .cases import json_bytes, jsonl_bytes


@dataclass(frozen=True)
class SourceFactory:
    """Build multiple source shapes from the same logical payload."""

    def object_str(self, payload: Any) -> str:
        return json_bytes(payload).decode("utf-8")

    def object_bytes(self, payload: Any) -> bytes:
        return json_bytes(payload)

    def object_io(self, payload: Any) -> io.BytesIO:
        return io.BytesIO(self.object_bytes(payload))

    def array_str(self, payloads: Sequence[Any]) -> str:
        return self.object_str(list(payloads))

    def array_bytes(self, payloads: Sequence[Any]) -> bytes:
        return self.object_bytes(list(payloads))

    def array_io(self, payloads: Sequence[Any]) -> io.BytesIO:
        return io.BytesIO(self.array_bytes(payloads))

    def jsonl_str(self, payloads: Sequence[Any]) -> str:
        return jsonl_bytes(payloads).decode("utf-8")

    def jsonl_bytes(self, payloads: Sequence[Any]) -> bytes:
        return jsonl_bytes(payloads)

    def jsonl_io(self, payloads: Sequence[Any]) -> io.BytesIO:
        return io.BytesIO(self.jsonl_bytes(payloads))

    def jsonl_text_lines(self, payloads: Sequence[Any]) -> list[str]:
        return [self.object_str(payload) for payload in payloads]

    def jsonl_byte_lines(self, payloads: Sequence[Any]) -> list[bytes]:
        return [self.object_bytes(payload) for payload in payloads]
