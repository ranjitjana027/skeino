"""Shared type aliases and the checkpoint selector model."""

from typing import Any, Final, Literal, TypeAlias

from pydantic import BaseModel

JsonObject: TypeAlias = dict[str, Any]
JsonArray: TypeAlias = list[Any]
JsonValue: TypeAlias = JsonObject | JsonArray | str | int | float | bool | None

ThreadStatus = Literal["idle", "busy", "interrupted", "error"]
RunStatus = Literal["pending", "running", "error", "success", "timeout", "interrupted"]
MultitaskStrategy = Literal["reject", "rollback", "interrupt", "enqueue"]
CancelAction = Literal["interrupt", "rollback"]
ThreadIfExists = Literal["raise", "do_nothing"]
RunIfNotExists = Literal["create", "reject"]
StreamMode = Literal[
    "values",
    "messages",
    "messages-tuple",
    "tasks",
    "checkpoints",
    "updates",
    "events",
    "debug",
    "custom",
]

DEFAULT_STREAM_MODES: Final[tuple[StreamMode, ...]] = ("values",)


class CheckpointConfigModel(BaseModel):
    """Checkpoint selector for thread state or run resumption."""

    thread_id: str | None = None
    checkpoint_ns: str | None = None
    checkpoint_id: str | None = None
    checkpoint_map: dict[str, JsonValue] | None = None
