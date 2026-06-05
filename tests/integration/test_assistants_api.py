"""Integration tests for the assistants API."""

from uuid import uuid4

from fastapi.testclient import TestClient


def test_search_returns_singleton(skeino_client: TestClient) -> None:
    r = skeino_client.post("/assistants/search", json={})
    assert r.status_code == 200
    body = r.json()
    assert len(body) == 1
    assert body[0]["graph_id"] == "test_agent"


def test_get_assistant_by_graph_id(skeino_client: TestClient) -> None:
    r = skeino_client.get("/assistants/test_agent")
    assert r.status_code == 200
    assert r.json()["graph_id"] == "test_agent"


def test_get_assistant_by_random_uuid(skeino_client: TestClient) -> None:
    """Any valid UUID resolves to the singleton in v1."""
    r = skeino_client.get(f"/assistants/{uuid4()}")
    assert r.status_code == 200
    assert r.json()["graph_id"] == "test_agent"


def test_get_assistant_invalid_id_returns_404(skeino_client: TestClient) -> None:
    r = skeino_client.get("/assistants/not-a-uuid-or-known-id")
    assert r.status_code == 404


def test_get_assistant_schemas(skeino_client: TestClient) -> None:
    r = skeino_client.get("/assistants/test_agent/schemas")
    assert r.status_code == 200
    body = r.json()
    assert body["graph_id"] == "test_agent"
    assert body["state_schema"]["type"] == "object"


def test_get_assistant_graph(skeino_client: TestClient) -> None:
    r = skeino_client.get("/assistants/test_agent/graph")
    assert r.status_code == 200
    body = r.json()
    assert "nodes" in body and "edges" in body


def test_get_assistant_subgraphs(skeino_client: TestClient) -> None:
    r = skeino_client.get("/assistants/test_agent/subgraphs")
    assert r.status_code == 200
    body = r.json()
    assert "test_agent" in body
    assert "state_schema" in body["test_agent"]
