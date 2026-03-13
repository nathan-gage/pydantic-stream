"""Seven model variant families for benchmarking (same schema, different backends).

Do NOT add ``from __future__ import annotations`` — slots=True pydantic
dataclasses can fail at construction with postponed annotations on Python 3.11.
"""

import dataclasses

from pydantic import BaseModel, ConfigDict, TypeAdapter
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import rebuild_dataclass
from pydantic_stream import StreamingBaseModelMixin, StreamingDataclassMixin

# ---------------------------------------------------------------------------
# Approach 1: StreamingBaseModelMixin
# ---------------------------------------------------------------------------


class BenchAddress_StreamModel(StreamingBaseModelMixin):
    city: str
    country: str


class BenchUser_StreamModel(StreamingBaseModelMixin):
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_StreamModel
    tags: list[str]


# ---------------------------------------------------------------------------
# Approach 2: StreamingDataclassMixin, slots=False (default pydantic_dataclass)
# ---------------------------------------------------------------------------


@pydantic_dataclass
class BenchAddress_StreamDC(StreamingDataclassMixin):
    city: str
    country: str


@pydantic_dataclass
class BenchUser_StreamDC(StreamingDataclassMixin):
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_StreamDC
    tags: list[str]


rebuild_dataclass(BenchAddress_StreamDC)  # type: ignore[arg-type]
rebuild_dataclass(BenchUser_StreamDC)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Approach 3: StreamingDataclassMixin, slots=True
# ---------------------------------------------------------------------------


@pydantic_dataclass(config=ConfigDict(slots=True))
class BenchAddress_StreamDCSlots(StreamingDataclassMixin):
    city: str
    country: str


@pydantic_dataclass(config=ConfigDict(slots=True))
class BenchUser_StreamDCSlots(StreamingDataclassMixin):
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_StreamDCSlots
    tags: list[str]


rebuild_dataclass(BenchAddress_StreamDCSlots)  # type: ignore[arg-type]
rebuild_dataclass(BenchUser_StreamDCSlots)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Approach 4: Standard Pydantic BaseModel (no mixin)
# ---------------------------------------------------------------------------


class BenchAddress_PydanticModel(BaseModel):
    city: str
    country: str


class BenchUser_PydanticModel(BaseModel):
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_PydanticModel
    tags: list[str]


pydantic_model_list_adapter: TypeAdapter[list[BenchUser_PydanticModel]] = TypeAdapter(list[BenchUser_PydanticModel])


# ---------------------------------------------------------------------------
# Approach 5: Standard Pydantic dataclass (no mixin)
# ---------------------------------------------------------------------------


@pydantic_dataclass
class BenchAddress_PydanticDC:
    city: str
    country: str


@pydantic_dataclass
class BenchUser_PydanticDC:
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_PydanticDC
    tags: list[str]


rebuild_dataclass(BenchAddress_PydanticDC)  # type: ignore[arg-type]
rebuild_dataclass(BenchUser_PydanticDC)  # type: ignore[arg-type]

pydantic_dc_list_adapter: TypeAdapter[list[BenchUser_PydanticDC]] = TypeAdapter(list[BenchUser_PydanticDC])


# ---------------------------------------------------------------------------
# Approach 6: Standard Pydantic dataclass with slots=True (no mixin)
# ---------------------------------------------------------------------------


@pydantic_dataclass(config=ConfigDict(slots=True))
class BenchAddress_PydanticDCSlots:
    city: str
    country: str


@pydantic_dataclass(config=ConfigDict(slots=True))
class BenchUser_PydanticDCSlots:
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_PydanticDCSlots
    tags: list[str]


rebuild_dataclass(BenchAddress_PydanticDCSlots)  # type: ignore[arg-type]
rebuild_dataclass(BenchUser_PydanticDCSlots)  # type: ignore[arg-type]

pydantic_dc_slots_list_adapter: TypeAdapter[list[BenchUser_PydanticDCSlots]] = TypeAdapter(
    list[BenchUser_PydanticDCSlots]
)


# ---------------------------------------------------------------------------
# Approach 7: stdlib dataclass with __slots__
# ---------------------------------------------------------------------------


@dataclasses.dataclass(slots=True)
class BenchAddress_StdlibDC:
    city: str
    country: str


@dataclasses.dataclass(slots=True)
class BenchUser_StdlibDC:
    id: int
    name: str
    score: float
    active: bool
    address: BenchAddress_StdlibDC
    tags: list[str]
