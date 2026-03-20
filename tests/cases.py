"""Reusable streamable model cases for integration and property tests."""

from __future__ import annotations

import enum
import io
import json
from collections.abc import AsyncIterator, Callable, Iterator, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, TypeAdapter
from pydantic.dataclasses import dataclass as pydantic_dataclass
from pydantic.dataclasses import rebuild_dataclass

from pydantic_stream import StreamingBaseModelMixin, StreamingDataclassMixin

JsonObject = dict[str, Any]


def json_bytes(payload: Any) -> bytes:
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def json_source(payload: Any) -> io.BytesIO:
    return io.BytesIO(json_bytes(payload))


def jsonl_bytes(records: Sequence[Any]) -> bytes:
    if not records:
        return b""
    return b"\n".join(json_bytes(record) for record in records) + b"\n"


def jsonl_source(records: Sequence[Any]) -> io.BytesIO:
    return io.BytesIO(jsonl_bytes(records))


def iter_non_empty_jsonl_lines(
    source: str | bytes | Sequence[str] | Sequence[bytes],
) -> Iterator[str | bytes]:
    if isinstance(source, str):
        raw_lines: Iterator[str | bytes] = iter(source.splitlines())
    elif isinstance(source, bytes):
        raw_lines = iter(source.splitlines())
    else:
        raw_lines = iter(source)

    for raw_line in raw_lines:
        stripped = raw_line.strip()
        if stripped:
            yield stripped


@dataclass(frozen=True)
class StreamableCase:
    case_id: str
    model_type: type[Any]
    stream_validate_json_fn: Callable[[Any], Any]
    stream_validate_json_array_fn: Callable[[Any], Any]
    stream_validate_json_array_iter_fn: Callable[[Any], Iterator[Any]]
    stream_validate_jsonl_iter_fn: Callable[[Any], Iterator[Any]]
    stream_validate_jsonl_fn: Callable[[Any], list[Any]]

    @property
    def adapter(self) -> TypeAdapter[Any]:
        return TypeAdapter(self.model_type)

    @property
    def list_adapter(self) -> TypeAdapter[Any]:
        return TypeAdapter(list[self.model_type])

    def stream_validate_json(self, source: Any, *args: Any, **kwargs: Any) -> Any:
        return self.stream_validate_json_fn(source, *args, **kwargs)

    def stream_validate_json_array(self, source: Any, *args: Any, **kwargs: Any) -> Any:
        return self.stream_validate_json_array_fn(source, *args, **kwargs)

    def stream_validate_json_array_iter(
        self, source: Any, *args: Any, **kwargs: Any
    ) -> Iterator[Any]:
        return self.stream_validate_json_array_iter_fn(source, *args, **kwargs)

    def stream_validate_json_array_aiter(
        self, source: Any, *args: Any, **kwargs: Any
    ) -> AsyncIterator[Any]:
        if hasattr(self.model_type, "stream_validate_json_array_aiter"):
            return self.model_type.stream_validate_json_array_aiter(source, *args, **kwargs)
        return self.model_type.stream_model_validate_json_array_aiter(source, *args, **kwargs)

    def stream_validate_jsonl_iter(self, source: Any, *args: Any, **kwargs: Any) -> Iterator[Any]:
        return self.stream_validate_jsonl_iter_fn(source, *args, **kwargs)

    def stream_validate_jsonl_aiter(
        self, source: Any, *args: Any, **kwargs: Any
    ) -> AsyncIterator[Any]:
        if hasattr(self.model_type, "stream_validate_jsonl_aiter"):
            return self.model_type.stream_validate_jsonl_aiter(source, *args, **kwargs)
        return self.model_type.stream_model_validate_jsonl_aiter(source, *args, **kwargs)

    def stream_validate_jsonl(self, source: Any, *args: Any, **kwargs: Any) -> list[Any]:
        return self.stream_validate_jsonl_fn(source, *args, **kwargs)

    def direct_validate_python(self, payload: Any) -> Any:
        return self.adapter.validate_python(payload)

    def direct_validate_json(self, source: str | bytes) -> Any:
        return self.adapter.validate_json(source)

    def direct_validate_json_array(self, source: str | bytes) -> list[Any]:
        return self.list_adapter.validate_json(source)

    def direct_validate_jsonl(self, source: str | bytes) -> list[Any]:
        return [self.direct_validate_json(line) for line in iter_non_empty_jsonl_lines(source)]

    def dump_python(self, value: Any) -> Any:
        return self.adapter.dump_python(value, mode="json", by_alias=False)

    def dump_python_many(self, values: Sequence[Any]) -> list[Any]:
        return [self.dump_python(value) for value in values]

    def compile_spec(self) -> Any:
        return self.model_type._streaming_spec()  # type: ignore[attr-defined]


@pydantic_dataclass
class HarnessAddressDataclass(StreamingDataclassMixin):
    city: str
    zip_code: int = Field(alias="zip")


@pydantic_dataclass
class HarnessUserDataclass(StreamingDataclassMixin):
    id: int
    name: str
    address: HarnessAddressDataclass | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: JsonObject = Field(default_factory=dict)


@pydantic_dataclass
class HarnessAliasChoiceDataclass(StreamingDataclassMixin):
    label: str
    record_id: int = Field(validation_alias=AliasChoices("external_id", "legacy_id"))


@pydantic_dataclass(config=ConfigDict(validate_by_name=True))
class HarnessPopulateByNameDataclass(StreamingDataclassMixin):
    city: str
    zip_code: int = Field(alias="zip")


rebuild_dataclass(HarnessAddressDataclass)  # type: ignore[arg-type]
rebuild_dataclass(HarnessUserDataclass)  # type: ignore[arg-type]
rebuild_dataclass(HarnessAliasChoiceDataclass)  # type: ignore[arg-type]
rebuild_dataclass(HarnessPopulateByNameDataclass)  # type: ignore[arg-type]


class HarnessAddressModel(StreamingBaseModelMixin):
    city: str
    zip_code: int = Field(alias="zip")


class HarnessUserModel(StreamingBaseModelMixin):
    id: int
    name: str
    address: HarnessAddressModel | None = None
    tags: list[str] = Field(default_factory=list)
    metadata: JsonObject = Field(default_factory=dict)


class HarnessAliasChoiceModel(StreamingBaseModelMixin):
    label: str
    record_id: int = Field(validation_alias=AliasChoices("external_id", "legacy_id"))


class HarnessPopulateByNameModel(StreamingBaseModelMixin):
    model_config = ConfigDict(validate_by_name=True)
    city: str
    zip_code: int = Field(alias="zip")


class Priority(enum.IntEnum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3


@pydantic_dataclass
class HarnessRichDataclass(StreamingDataclassMixin):
    id: int
    status: Literal["active", "inactive"]
    priority: Priority
    score: float = 0.0
    created_at: datetime | None = None
    label: str | None = "unnamed"
    extras: tuple[str, ...] = ()
    unique_tags: frozenset[str] = frozenset()


class HarnessRichModel(StreamingBaseModelMixin):
    id: int
    status: Literal["active", "inactive"]
    priority: Priority
    score: float = 0.0
    created_at: datetime | None = None
    label: str | None = "unnamed"
    extras: tuple[str, ...] = ()
    unique_tags: frozenset[str] = frozenset()


rebuild_dataclass(HarnessRichDataclass)  # type: ignore[arg-type]


USER_DATACLASS_CASE = StreamableCase(
    case_id="dataclass-user",
    model_type=HarnessUserDataclass,
    stream_validate_json_fn=HarnessUserDataclass.stream_validate_json,
    stream_validate_json_array_fn=HarnessUserDataclass.stream_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessUserDataclass.stream_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessUserDataclass.stream_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessUserDataclass.stream_validate_jsonl,
)

USER_BASEMODEL_CASE = StreamableCase(
    case_id="basemodel-user",
    model_type=HarnessUserModel,
    stream_validate_json_fn=HarnessUserModel.stream_model_validate_json,
    stream_validate_json_array_fn=HarnessUserModel.stream_model_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessUserModel.stream_model_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessUserModel.stream_model_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessUserModel.stream_model_validate_jsonl,
)

ALIAS_CHOICE_DATACLASS_CASE = StreamableCase(
    case_id="dataclass-alias-choice",
    model_type=HarnessAliasChoiceDataclass,
    stream_validate_json_fn=HarnessAliasChoiceDataclass.stream_validate_json,
    stream_validate_json_array_fn=HarnessAliasChoiceDataclass.stream_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessAliasChoiceDataclass.stream_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessAliasChoiceDataclass.stream_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessAliasChoiceDataclass.stream_validate_jsonl,
)

ALIAS_CHOICE_BASEMODEL_CASE = StreamableCase(
    case_id="basemodel-alias-choice",
    model_type=HarnessAliasChoiceModel,
    stream_validate_json_fn=HarnessAliasChoiceModel.stream_model_validate_json,
    stream_validate_json_array_fn=HarnessAliasChoiceModel.stream_model_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessAliasChoiceModel.stream_model_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessAliasChoiceModel.stream_model_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessAliasChoiceModel.stream_model_validate_jsonl,
)

POPULATE_BY_NAME_DATACLASS_CASE = StreamableCase(
    case_id="dataclass-validate-by-name",
    model_type=HarnessPopulateByNameDataclass,
    stream_validate_json_fn=HarnessPopulateByNameDataclass.stream_validate_json,
    stream_validate_json_array_fn=HarnessPopulateByNameDataclass.stream_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessPopulateByNameDataclass.stream_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessPopulateByNameDataclass.stream_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessPopulateByNameDataclass.stream_validate_jsonl,
)

POPULATE_BY_NAME_BASEMODEL_CASE = StreamableCase(
    case_id="basemodel-validate-by-name",
    model_type=HarnessPopulateByNameModel,
    stream_validate_json_fn=HarnessPopulateByNameModel.stream_model_validate_json,
    stream_validate_json_array_fn=HarnessPopulateByNameModel.stream_model_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessPopulateByNameModel.stream_model_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessPopulateByNameModel.stream_model_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessPopulateByNameModel.stream_model_validate_jsonl,
)

RICH_DATACLASS_CASE = StreamableCase(
    case_id="dataclass-rich",
    model_type=HarnessRichDataclass,
    stream_validate_json_fn=HarnessRichDataclass.stream_validate_json,
    stream_validate_json_array_fn=HarnessRichDataclass.stream_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessRichDataclass.stream_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessRichDataclass.stream_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessRichDataclass.stream_validate_jsonl,
)

RICH_BASEMODEL_CASE = StreamableCase(
    case_id="basemodel-rich",
    model_type=HarnessRichModel,
    stream_validate_json_fn=HarnessRichModel.stream_model_validate_json,
    stream_validate_json_array_fn=HarnessRichModel.stream_model_validate_json_array,
    stream_validate_json_array_iter_fn=HarnessRichModel.stream_model_validate_json_array_iter,
    stream_validate_jsonl_iter_fn=HarnessRichModel.stream_model_validate_jsonl_iter,
    stream_validate_jsonl_fn=HarnessRichModel.stream_model_validate_jsonl,
)

USER_CASES = (USER_DATACLASS_CASE, USER_BASEMODEL_CASE)
ALIAS_CHOICE_CASES = (ALIAS_CHOICE_DATACLASS_CASE, ALIAS_CHOICE_BASEMODEL_CASE)
POPULATE_BY_NAME_CASES = (POPULATE_BY_NAME_DATACLASS_CASE, POPULATE_BY_NAME_BASEMODEL_CASE)
RICH_CASES = (RICH_DATACLASS_CASE, RICH_BASEMODEL_CASE)
ALL_CASES = USER_CASES + ALIAS_CHOICE_CASES + POPULATE_BY_NAME_CASES + RICH_CASES
HARNESS_MODEL_TYPES = tuple(case.model_type for case in ALL_CASES)


def reset_streaming_harness_caches() -> None:
    for model_type in HARNESS_MODEL_TYPES:
        if hasattr(model_type, "__streaming_type_adapter__"):
            model_type.__streaming_type_adapter__ = None
        if hasattr(model_type, "__streaming_list_adapter__"):
            model_type.__streaming_list_adapter__ = None
        if hasattr(model_type, "__streaming_object_spec__"):
            model_type.__streaming_object_spec__ = None

        if issubclass(model_type, BaseModel):
            model_type.model_rebuild(force=True)
        else:
            rebuild_dataclass(model_type)  # type: ignore[arg-type]
