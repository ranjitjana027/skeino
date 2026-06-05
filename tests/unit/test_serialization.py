"""Tests for inbound/outbound payload normalisation."""

from datetime import UTC, datetime
from uuid import UUID, uuid4

from langchain_core.messages import AIMessage, HumanMessage
from skeino.schemas import CheckpointConfigModel, CommandModel
from skeino.serialization import (
    build_thread_config,
    coerce_stream_modes,
    normalize_command_payload,
    normalize_input_payload,
    serialize_message,
    serialize_value,
)


def test_serialize_value_handles_primitives() -> None:
    assert serialize_value(None) is None
    assert serialize_value(True) is True
    assert serialize_value(42) == 42
    assert serialize_value(3.14) == 3.14
    assert serialize_value("hi") == "hi"


def test_serialize_value_converts_uuid_and_datetime() -> None:
    uid = uuid4()
    when = datetime(2026, 1, 1, tzinfo=UTC)
    assert serialize_value(uid) == str(uid)
    assert serialize_value(when) == when.isoformat()


def test_serialize_value_recurses_dict_and_list() -> None:
    payload = {"id": uuid4(), "items": [1, 2, {"nested": True}]}
    out = serialize_value(payload)
    assert isinstance(out, dict)
    assert isinstance(out["id"], str)
    assert out["items"][2] == {"nested": True}


def test_serialize_message_ai_basic() -> None:
    msg = AIMessage(content="hello")
    out = serialize_message(msg)
    assert out["type"] == "ai"
    assert out["content"] == "hello"
    assert "id" in out


def test_serialize_message_flattens_content_blocks() -> None:
    msg = AIMessage(content=[{"type": "text", "text": "foo"}, "bar"])
    out = serialize_message(msg)
    assert out["content"] == "foobar"


def test_normalize_input_payload_converts_messages() -> None:
    payload = {"messages": [{"role": "user", "content": "hi"}]}
    out = normalize_input_payload(payload)
    assert isinstance(out["messages"], list)
    assert isinstance(out["messages"][0], HumanMessage)


def test_normalize_command_payload_passthrough_none() -> None:
    assert normalize_command_payload(None) is None


def test_normalize_command_payload_builds_command() -> None:
    cmd = normalize_command_payload(CommandModel(resume="ok"))
    assert cmd is not None
    assert cmd.resume == "ok"


def test_coerce_stream_modes_accepts_str_or_list() -> None:
    assert coerce_stream_modes("values") == ["values"]
    assert coerce_stream_modes(["values", "messages"]) == ["values", "messages"]


def test_build_thread_config_sets_thread_id_and_run_id() -> None:
    run_id = str(uuid4())
    cfg = build_thread_config(
        "abc",
        {"metadata": {"x": 1}},
        checkpoint=CheckpointConfigModel(checkpoint_ns="ns", checkpoint_id="cp1"),
        run_id=run_id,
    )
    configurable = cfg["configurable"]
    assert configurable["thread_id"] == "abc"
    assert configurable["checkpoint_ns"] == "ns"
    assert configurable["checkpoint_id"] == "cp1"
    assert cfg["run_id"] == UUID(run_id)
    assert cfg["metadata"]["run_id"] == run_id
    assert cfg["metadata"]["thread_id"] == "abc"
