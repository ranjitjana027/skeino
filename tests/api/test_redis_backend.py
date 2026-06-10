"""Redis split-brain semantics: durable checkpoints, ephemeral metadata.

Redis has no native metadata store, so running it requires the explicit
``allow_ephemeral_metadata`` opt-in: graph state survives restarts in redis
while thread/run rows evaporate. These tests pin down exactly that contract.
"""

import pytest
from fastapi.testclient import TestClient

from skeino import SkeinoSettings, create_app
from tests.api.conftest import (
    Backend,
    api_client,
    build_echo_graph,
    create_thread,
    message_contents,
    run_to_completion,
)


def test_checkpoints_survive_restart_but_metadata_does_not(
    redis_backend: Backend,
) -> None:
    with api_client(redis_backend) as client:
        thread_id = create_thread(client)
        run_to_completion(client, thread_id, "stick around")

    with api_client(redis_backend) as client:
        # Metadata was in-memory: the thread row is gone after restart.
        assert client.get(f"/threads/{thread_id}").status_code == 404

        # ...but the checkpoint survived in redis: re-creating the thread row
        # with the same id resurfaces the persisted graph state.
        recreated = client.post("/threads", json={"thread_id": thread_id})
        assert recreated.status_code == 200
        state = client.get(f"/threads/{thread_id}/state")
        assert state.status_code == 200
        contents = message_contents(state.json())
        assert "stick around" in contents
        assert "echo: stick around" in contents


def test_redis_requires_ephemeral_metadata_optin(redis_backend: Backend) -> None:
    app = create_app(
        graphs={"echo_agent": build_echo_graph},
        settings=SkeinoSettings(
            default_assistant_id="echo_agent",
            checkpointer_scheme=redis_backend.scheme,
            checkpointer_uri=redis_backend.uri,
            # allow_ephemeral_metadata deliberately NOT set.
        ),
    )
    with pytest.raises(ValueError, match="durable"):
        with TestClient(app):
            pass
