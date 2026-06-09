"""End-to-end API behaviour shared by all real backends.

Each test runs against postgres, mongodb, and redis (``any_backend``): a real
graph executes behind the API and its state round-trips through the real
checkpointer.
"""

import json

from tests.api.conftest import (
    Backend,
    api_client,
    create_thread,
    message_contents,
    run_to_completion,
)


def _sse_chunks(text: str) -> list[str]:
    return [chunk for chunk in text.split("\n\n") if chunk.strip()]


def _event_names(text: str) -> list[str]:
    return [
        next(line for line in chunk.splitlines() if line.startswith("event: "))[7:]
        for chunk in _sse_chunks(text)
    ]


def _event_payload(text: str, event: str) -> dict:
    chunk = next(c for c in _sse_chunks(text) if f"event: {event}" in c)
    data_line = next(line for line in chunk.splitlines() if line.startswith("data: "))
    return json.loads(data_line[6:])


def test_run_lifecycle_end_to_end(any_backend: Backend) -> None:
    with api_client(any_backend) as client:
        thread_id = create_thread(client)
        run = run_to_completion(client, thread_id, "hello")
        assert run["thread_id"] == thread_id

        state = client.get(f"/threads/{thread_id}/state")
        assert state.status_code == 200
        contents = message_contents(state.json())
        assert "hello" in contents
        assert "echo: hello" in contents

        thread = client.get(f"/threads/{thread_id}")
        assert thread.json()["status"] == "idle"


def test_streaming_sse_from_real_graph(any_backend: Backend) -> None:
    with api_client(any_backend) as client:
        thread_id = create_thread(client)
        with client.stream(
            "POST",
            f"/threads/{thread_id}/runs/stream",
            json={
                "assistant_id": "echo_agent",
                "input": {"messages": [{"role": "user", "content": "stream me"}]},
                "stream_mode": "values",
            },
        ) as r:
            assert r.status_code == 200
            text = "".join(r.iter_text())

        events = _event_names(text)
        assert events[0] == "metadata"
        assert "values" in events
        assert events[-1] == "end"

        metadata = _event_payload(text, "metadata")
        assert metadata["thread_id"] == thread_id
        run_id = metadata["run_id"]

        # The last values event carries the echoed message from the real graph.
        values_chunks = [c for c in _sse_chunks(text) if "event: values" in c]
        final_values = json.loads(
            next(
                line
                for line in values_chunks[-1].splitlines()
                if line.startswith("data: ")
            )[6:]
        )
        final_contents = [m["content"] for m in final_values.get("messages", [])]
        assert "echo: stream me" in final_contents

        run = client.get(f"/threads/{thread_id}/runs/{run_id}")
        assert run.status_code == 200
        assert run.json()["status"] == "success"


def test_checkpoint_history_and_time_travel(any_backend: Backend) -> None:
    with api_client(any_backend) as client:
        thread_id = create_thread(client)
        for text in ("one", "two"):
            run_to_completion(client, thread_id, text)

        history = client.get(f"/threads/{thread_id}/history?limit=20")
        assert history.status_code == 200
        snapshots = history.json()
        assert len(snapshots) >= 2

        # Postgres checkpoints are stamped with the run id by skeino's
        # run-enriching wrapper; mongo's saver merges config metadata itself.
        # The redis builder has neither, so its snapshots carry no run_id —
        # pinned here as the current (known) gap.
        run_ids = {r["run_id"] for r in client.get(f"/threads/{thread_id}/runs").json()}
        if any_backend.name == "redis":
            assert all("run_id" not in s["metadata"] for s in snapshots)
        else:
            assert {s["metadata"]["run_id"] for s in snapshots} <= run_ids

        # History is newest-first: the latest state has run two's echo, the
        # oldest checkpoint predates it. Exact counts are durability-dependent,
        # so assert content, not cardinality.
        latest = client.get(f"/threads/{thread_id}/state").json()
        assert "echo: two" in message_contents(latest)

        oldest_checkpoint = snapshots[-1]["checkpoint"]["checkpoint_id"]
        past = client.get(f"/threads/{thread_id}/state/{oldest_checkpoint}")
        assert past.status_code == 200
        assert "echo: two" not in message_contents(past.json())
