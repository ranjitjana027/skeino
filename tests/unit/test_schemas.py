"""Sanity checks for the public schema surface."""

from uuid import uuid4

import pytest
from skeino.schemas import (
    AssistantSearchRequest,
    CheckpointConfigModel,
    RunCreateRequest,
    ThreadCreateRequest,
    ThreadSearchRequest,
)


def test_thread_create_request_defaults() -> None:
    """ThreadCreateRequest accepts an empty payload."""
    req = ThreadCreateRequest()
    assert req.metadata == {}
    assert req.if_exists == "raise"
    assert req.supersteps == []


def test_thread_search_request_limit_validation() -> None:
    """ThreadSearchRequest enforces sensible limit/offset bounds."""
    ThreadSearchRequest(limit=1)
    ThreadSearchRequest(limit=1000)
    with pytest.raises(ValueError):
        ThreadSearchRequest(limit=0)
    with pytest.raises(ValueError):
        ThreadSearchRequest(limit=1001)


def test_run_create_request_durability_default() -> None:
    """RunCreateRequest defaults durability='exit' (one checkpoint per run)."""
    req = RunCreateRequest(assistant_id="agent")
    assert req.durability == "exit"
    assert req.multitask_strategy == "enqueue"
    assert req.stream_mode == ["values"]


def test_assistant_search_request_defaults() -> None:
    """AssistantSearchRequest defaults limit=10, offset=0."""
    req = AssistantSearchRequest()
    assert req.limit == 10
    assert req.offset == 0


def test_checkpoint_config_model_optional_fields() -> None:
    """CheckpointConfigModel allows every field to be optional."""
    cfg = CheckpointConfigModel(thread_id=str(uuid4()))
    assert cfg.checkpoint_ns is None
    assert cfg.checkpoint_id is None
    assert cfg.checkpoint_map is None
