"""Assistant schema routes against real graphs (``/schemas``, ``/subgraphs``).

Upstream (``langgraph_api/api/assistants.py`` ``_graph_schemas``) builds each
schema independently — one that cannot be generated degrades to ``None`` with
a warning — and derives ``state_schema`` from every state channel, not from
the input schema. Gaps are strict xfails tied to #133.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest
from fastapi.testclient import TestClient
from langgraph.graph import START, StateGraph
from typing_extensions import TypedDict

from skeino import SkeinoSettings, create_app
from tests.real_graphs import ASSISTANT_ID


class Opaque:
    """A type pydantic cannot build a JSON schema for."""


class Question(TypedDict):
    question: str


class Answer(TypedDict):
    answer: str


class ScratchState(TypedDict):
    question: str
    scratch: str
    answer: str


class OpaqueState(TypedDict):
    question: str
    blob: Opaque
    answer: str


def _build(state: Any, **schemas: Any) -> Any:
    def factory(checkpointer: Any) -> Any:
        graph = StateGraph(state, **schemas)
        graph.add_node("answer", lambda _: {"answer": "42"})
        graph.add_edge(START, "answer")
        return graph.compile(checkpointer=checkpointer)

    return factory


@contextmanager
def _client(factory: Any) -> Iterator[TestClient]:
    app = create_app(
        graphs={ASSISTANT_ID: factory},
        settings=SkeinoSettings(default_assistant_id=ASSISTANT_ID),
    )
    with TestClient(app) as client:
        yield client


def _properties(schema: dict[str, Any] | None) -> set[str]:
    return set((schema or {}).get("properties", {}))


def test_input_and_output_schemas_follow_the_declared_schemas() -> None:
    graph = _build(ScratchState, input_schema=Question, output_schema=Answer)
    with _client(graph) as client:
        schemas = client.get(f"/assistants/{ASSISTANT_ID}/schemas").json()
    assert _properties(schemas["input_schema"]) == {"question"}
    assert _properties(schemas["output_schema"]) == {"answer"}


@pytest.mark.xfail(strict=True, reason="#133: state_schema is the input schema")
def test_state_schema_lists_every_state_channel() -> None:
    graph = _build(ScratchState, input_schema=Question, output_schema=Answer)
    with _client(graph) as client:
        schemas = client.get(f"/assistants/{ASSISTANT_ID}/schemas").json()
    assert _properties(schemas["state_schema"]) == {"question", "scratch", "answer"}


@pytest.mark.xfail(strict=True, reason="#133: a failing schema 500s the route")
@pytest.mark.parametrize("route", ["schemas", "subgraphs"])
def test_unbuildable_schema_degrades_to_none(route: str) -> None:
    # ``blob: Opaque`` breaks only the input schema; the rest must still render.
    with _client(_build(OpaqueState, output_schema=Answer)) as client:
        response = client.get(f"/assistants/{ASSISTANT_ID}/{route}")
    assert response.status_code == 200, response.text
    if route == "schemas":
        assert response.json()["input_schema"] is None
        assert _properties(response.json()["output_schema"]) == {"answer"}
