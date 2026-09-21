"""LangGraph Studio tracing parity: ``/info`` flags and per-run LangSmith sessions.

Studio shows in-app traces only when ``GET /info`` advertises
``langsmith_tracing_session_on_runs``, and it then expects each run it sends with
``langsmith_tracer.project_name`` to be traced into that project. These tests
assert both halves: what the server claims, and where a run's trace is routed
(read off the LangSmith tracing context the graph actually executed under).
"""

import json
from typing import Any

import pytest
from fastapi.testclient import TestClient
from langsmith import run_helpers

import skeino.ops.assistants as assistants_ops
import skeino.tracing as tracing
from tests.conftest import FakeGraph, build_test_app

STUDIO_PROJECT = "studio-session"
DEFAULT_PROJECT = "server-default"
EXAMPLE_ID = "5b8f5f6e-2b4e-4d3a-9c55-0d1f7f1b9a11"


@pytest.fixture
def tracing_on(monkeypatch: pytest.MonkeyPatch) -> None:
    """Tracing configured, with a known default project."""
    monkeypatch.setattr(tracing, "tracing_enabled", lambda: True)
    monkeypatch.setattr(assistants_ops, "tracing_enabled", lambda: True)
    monkeypatch.setattr(tracing, "_default_project", lambda: DEFAULT_PROJECT)


@pytest.fixture
def tracing_off(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(tracing, "tracing_enabled", lambda: False)
    monkeypatch.setattr(assistants_ops, "tracing_enabled", lambda: False)


@pytest.fixture
def app_and_graph() -> Any:
    app, graph = build_test_app()
    with TestClient(app) as client:
        yield client, graph


def _thread(client: TestClient) -> str:
    return client.post("/threads", json={}).json()["thread_id"]


def _studio_run(**extra: Any) -> dict[str, Any]:
    return {
        "assistant_id": "test_agent",
        "input": {"messages": []},
        "langsmith_tracer": {"project_name": STUDIO_PROJECT, "example_id": EXAMPLE_ID},
        **extra,
    }


def _replicas(graph: FakeGraph) -> Any:
    assert graph.tracing_seen, "the graph never executed"
    return graph.tracing_seen[-1].get("replicas")


_EXPECTED_REPLICAS: list[dict[str, Any]] = [
    {
        "project_name": STUDIO_PROJECT,
        "updates": {"reference_example_id": EXAMPLE_ID},
    },
    {"project_name": DEFAULT_PROJECT, "updates": None},
]


# --- /info --------------------------------------------------------------------


def test_info_advertises_session_tracing(app_and_graph: Any, tracing_on: None) -> None:
    client, _ = app_and_graph
    info = client.get("/info").json()

    assert info["flags"] == {
        "assistants": True,
        "crons": False,
        "langsmith": True,
        "langsmith_tracing_replicas": True,
        "langsmith_tracing_session_on_runs": True,
    }
    assert info["langgraph_py_version"]
    assert info["host"]["kind"] == "self-hosted"
    # Existing fields are unchanged.
    assert info["status"] == "ok"
    assert info["version"] == "0.0.1-test"


def test_info_reports_langsmith_off_when_tracing_is_not_configured(
    app_and_graph: Any, tracing_off: None
) -> None:
    client, _ = app_and_graph
    flags = client.get("/info").json()["flags"]
    assert flags["langsmith"] is False
    # Runs still accept a session; with tracing off it is simply not used.
    assert flags["langsmith_tracing_session_on_runs"] is True


# --- where a run's trace goes ---------------------------------------------------


def test_wait_run_traces_into_the_requested_and_default_projects(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    response = client.post(f"/threads/{thread_id}/runs/wait", json=_studio_run())

    assert response.status_code == 200
    assert _replicas(graph) == _EXPECTED_REPLICAS


def test_streaming_run_traces_into_the_requested_and_default_projects(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    response = client.post(f"/threads/{thread_id}/runs/stream", json=_studio_run())

    assert response.status_code == 200
    assert _replicas(graph) == _EXPECTED_REPLICAS
    # The run announced in the stream's metadata event carries its session.
    metadata = next(
        json.loads(line[len("data: ") :])
        for chunk in response.text.split("\n\n")
        if "event: metadata" in chunk.splitlines()
        for line in chunk.splitlines()
        if line.startswith("data: ")
    )
    assert metadata["run"]["langsmith_session_name"] == STUDIO_PROJECT


def test_stateless_run_traces_into_the_requested_project(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph

    response = client.post("/runs/wait", json=_studio_run())

    assert response.status_code == 200
    assert _replicas(graph) == _EXPECTED_REPLICAS


def test_background_run_reports_its_session(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    created = client.post(f"/threads/{thread_id}/runs", json=_studio_run()).json()
    assert created["langsmith_session_name"] == STUDIO_PROJECT

    client.get(f"/threads/{thread_id}/runs/{created['run_id']}/join")
    fetched = client.get(f"/threads/{thread_id}/runs/{created['run_id']}").json()
    assert fetched["langsmith_session_name"] == STUDIO_PROJECT
    assert _replicas(graph) == _EXPECTED_REPLICAS


def test_run_without_a_session_traces_to_the_default_project_only(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    run = client.post(
        f"/threads/{thread_id}/runs",
        json={"assistant_id": "test_agent", "input": {"messages": []}},
    ).json()
    client.get(f"/threads/{thread_id}/runs/{run['run_id']}/join")

    assert run["langsmith_session_name"] == DEFAULT_PROJECT
    # No per-run destination, so no replicas: ordinary tracing, as before.
    assert not _replicas(graph)


def test_a_session_is_ignored_when_tracing_is_off(
    app_and_graph: Any, tracing_off: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    run = client.post(f"/threads/{thread_id}/runs", json=_studio_run()).json()
    client.get(f"/threads/{thread_id}/runs/{run['run_id']}/join")

    assert run["langsmith_session_name"] is None
    assert not _replicas(graph)


def test_the_tracing_context_does_not_outlive_the_run(
    app_and_graph: Any, tracing_on: None
) -> None:
    client, graph = app_and_graph
    thread_id = _thread(client)

    client.post(f"/threads/{thread_id}/runs/wait", json=_studio_run())
    assert _replicas(graph) == _EXPECTED_REPLICAS

    # A later run with no session must not inherit the previous run's replicas.
    client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": "test_agent", "input": {"messages": []}},
    )
    assert not _replicas(graph)
    assert not run_helpers.get_tracing_context().get("replicas")
