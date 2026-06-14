"""Thread reads must degrade safely, not mask failures or leak internal state.

A checkpoint-read failure should preserve the stored thread status (not fabricate
``error``), and an unintrospectable output schema should drop all values rather
than leak internal pipeline fields.
"""

from collections.abc import Iterator
from typing import Any

from fastapi.testclient import TestClient

from tests.conftest import build_test_app


def _client_and_graph() -> Iterator[tuple[TestClient, Any]]:
    app, graph = build_test_app()
    with TestClient(app) as client:
        yield client, graph


def test_checkpoint_read_failure_preserves_stored_status() -> None:
    gen = _client_and_graph()
    client, graph = next(gen)
    try:
        thread_id = client.post("/threads", json={}).json()["thread_id"]
        before = client.get(f"/threads/{thread_id}").json()["status"]

        async def _boom(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("checkpoint backend down")

        graph.aget_state = _boom
        after = client.get(f"/threads/{thread_id}")

        # The read failure is handled (no 500), the stored status is preserved
        # rather than fabricated as "error", and no stale values are returned.
        assert after.status_code == 200
        assert after.json()["status"] == before
        assert after.json()["values"] == {}
    finally:
        gen.close()


def test_thread_state_fails_closed_on_unintrospectable_schema() -> None:
    gen = _client_and_graph()
    client, graph = next(gen)
    try:
        thread_id = client.post("/threads", json={}).json()["thread_id"]
        graph.state_by_thread[thread_id] = {
            "messages": [],
            "secret_internal": "leak",
        }
        graph.output_schema = object()  # no model_fields → fail closed

        body = client.get(f"/threads/{thread_id}").json()
        assert body["values"] == {}  # internal field is not leaked
    finally:
        gen.close()
