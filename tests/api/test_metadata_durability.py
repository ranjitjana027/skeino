"""Durability across app restarts — the behaviour the in-memory suite cannot test.

An app is built, used, and torn down; a second app on the same backend URI must
see everything the first one persisted (``metadata_backend``: postgres + mongo).
"""

from tests.api.conftest import (
    Backend,
    api_client,
    build_failing_graph,
    create_thread,
    message_contents,
    run_to_completion,
)


def test_metadata_and_state_survive_restart(metadata_backend: Backend) -> None:
    with api_client(metadata_backend) as client:
        thread_id = create_thread(client, metadata={"topic": "durability"})
        run = run_to_completion(client, thread_id, "remember me")
        run_id = run["run_id"]

    # A brand-new app over the same backend: rows and checkpoints must survive.
    with api_client(metadata_backend) as client:
        thread = client.get(f"/threads/{thread_id}")
        assert thread.status_code == 200
        assert thread.json()["metadata"]["topic"] == "durability"

        persisted_run = client.get(f"/threads/{thread_id}/runs/{run_id}")
        assert persisted_run.status_code == 200
        assert persisted_run.json()["status"] == "success"

        state = client.get(f"/threads/{thread_id}/state")
        assert state.status_code == 200
        contents = message_contents(state.json())
        assert "remember me" in contents
        assert "echo: remember me" in contents

        found = client.post(
            "/threads/search", json={"metadata": {"topic": "durability"}}
        ).json()
        assert [t["thread_id"] for t in found] == [thread_id]


def test_failed_run_error_status_survives_restart(metadata_backend: Backend) -> None:
    with api_client(metadata_backend, graph_builder=build_failing_graph) as client:
        thread_id = create_thread(client)
        r = client.post(
            f"/threads/{thread_id}/runs",
            json={
                "assistant_id": "echo_agent",
                "input": {"messages": [{"role": "user", "content": "boom"}]},
            },
        )
        assert r.status_code == 500

    with api_client(metadata_backend) as client:
        errored = client.get(f"/threads/{thread_id}/runs?status=error").json()
        assert len(errored) == 1

    if metadata_backend.name == "postgres":
        # RunModel does not expose the error message over the wire; check the
        # column directly to prove the failure detail was persisted too.
        import psycopg

        with psycopg.connect(metadata_backend.uri) as conn:
            row = conn.execute(
                "SELECT error FROM app_runs WHERE thread_id = %s", (thread_id,)
            ).fetchone()
        assert row is not None
        assert row[0] and "node boom" in row[0]
