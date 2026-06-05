"""Integration tests for /api/health, /info, /api/initial-message."""

from fastapi.testclient import TestClient


def test_health_endpoint(skeino_client: TestClient) -> None:
    r = skeino_client.get("/api/health")
    assert r.status_code == 200
    assert r.json() == {"status": "healthy", "version": "0.0.1-test"}


def test_server_info(skeino_client: TestClient) -> None:
    r = skeino_client.get("/info")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "ok"
    assert body["name"] == "test_agent"
    assert body["version"] == "0.0.1-test"


def test_initial_message(skeino_client: TestClient) -> None:
    r = skeino_client.get("/api/initial-message")
    assert r.status_code == 200
    assert r.json() == {"message": "hello", "version": "0.0.1-test"}
