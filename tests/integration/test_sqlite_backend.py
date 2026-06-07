"""SQLite backend wiring: durability guard + end-to-end through create_app."""

import pytest
from fastapi.testclient import TestClient

from skeino import SkeinoSettings, create_app
from skeino.app import _check_metadata_durability
from tests.conftest import FakeGraph


def test_durability_guard_rejects_durable_checkpointer_without_durable_metadata() -> (
    None
):
    # Durable checkpointer scheme, no postgres_uri/sqlite_path → split-brain.
    with pytest.raises(ValueError, match="durable"):
        _check_metadata_durability(SkeinoSettings(checkpointer_scheme="postgres"))


def test_durability_guard_allows_opt_in_and_safe_combos() -> None:
    # Explicit opt-in.
    _check_metadata_durability(
        SkeinoSettings(checkpointer_scheme="postgres", allow_ephemeral_metadata=True)
    )
    # Memory checkpointer is not durable.
    _check_metadata_durability(SkeinoSettings(checkpointer_scheme="memory"))
    # SQLite provides durable metadata too.
    _check_metadata_durability(
        SkeinoSettings(sqlite_path=":memory:", checkpointer_scheme="sqlite")
    )
    # Nothing configured.
    _check_metadata_durability(SkeinoSettings())


def test_sqlite_backend_end_to_end() -> None:
    # sqlite_path drives both the checkpointer and the SqliteMetadataStore.
    app = create_app(
        graphs={"test_agent": lambda _ckpt: FakeGraph()},
        settings=SkeinoSettings(
            default_assistant_id="test_agent",
            assistant_name="Test Agent",
            sqlite_path=":memory:",
        ),
    )
    with TestClient(app) as client:
        thread_id = client.post("/threads", json={"metadata": {"t": "x"}}).json()[
            "thread_id"
        ]
        # Metadata persisted via the SQLite store.
        got = client.get(f"/threads/{thread_id}")
        assert got.status_code == 200
        assert got.json()["metadata"]["t"] == "x"

        run = client.post(
            f"/threads/{thread_id}/runs",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": [{"role": "user", "content": "hi"}]},
            },
        )
        assert run.status_code == 200
        assert run.json()["status"] == "success"

        listed = client.get(f"/threads/{thread_id}/runs").json()
        assert any(r["run_id"] == run.json()["run_id"] for r in listed)
