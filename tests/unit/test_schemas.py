"""Sanity checks for the public schema surface."""

from uuid import uuid4

import pytest
from pydantic import BaseModel

import skeino.schemas as schemas
from skeino.schemas import (
    AssistantSearchRequest,
    CheckpointConfigModel,
    RunCreateRequest,
    ThreadCreateRequest,
    ThreadSearchRequest,
)


def _public_models() -> list[type[BaseModel]]:
    """Every BaseModel re-exported from the public schemas package."""
    return [
        obj
        for name in schemas.__all__
        if isinstance(obj := getattr(schemas, name), type)
        and issubclass(obj, BaseModel)
        and obj is not BaseModel
    ]


@pytest.mark.parametrize("model", _public_models(), ids=lambda m: m.__name__)
def test_every_field_has_a_description(model: type[BaseModel]) -> None:
    """Every request/response field carries a description.

    Descriptions are the single source of the field-level API docs that render
    in the OpenAPI schema, ``/docs``, the API explorer, and the Python API
    reference. A field added without one would silently ship undocumented, so
    this fails loud instead.
    """
    missing = [
        name
        for name, field in model.model_fields.items()
        if not (field.description and field.description.strip())
    ]
    assert not missing, f"{model.__name__} fields without a description: {missing}"


def test_public_models_discovered() -> None:
    """The guard above is non-vacuous: it actually found models to check."""
    assert len(_public_models()) >= 15


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
