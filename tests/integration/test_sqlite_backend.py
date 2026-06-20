"""SQLite backend wiring: durability guard + end-to-end through create_app."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from skeino import SkeinoSettings, create_app
from skeino.app import _check_metadata_durability
from tests.conftest import FakeGraph


def test_durability_guard_rejects_durable_scheme_without_native_metadata() -> None:
    # A durable scheme with no native metadata store (redis) → split-brain.
    with pytest.raises(ValueError, match="durable"):
        _check_metadata_durability(
            SkeinoSettings(
                checkpointer_scheme="redis", checkpointer_uri="redis://localhost"
            )
        )


def test_durability_guard_allows_opt_in_and_safe_combos() -> None:
    # Explicit opt-in.
    _check_metadata_durability(
        SkeinoSettings(
            checkpointer_scheme="redis",
            checkpointer_uri="redis://localhost",
            allow_ephemeral_metadata=True,
        )
    )
    # Memory checkpointer is not durable.
    _check_metadata_durability(SkeinoSettings(checkpointer_scheme="memory"))
    # SQLite has a native metadata store — usable even without an explicit URI
    # (defaults to :memory:, matching the SQLite checkpointer builder).
    _check_metadata_durability(
        SkeinoSettings(checkpointer_scheme="sqlite", checkpointer_uri=":memory:")
    )
    _check_metadata_durability(SkeinoSettings(checkpointer_scheme="sqlite"))
    # Mongo has a native metadata store.
    _check_metadata_durability(
        SkeinoSettings(
            checkpointer_scheme="mongodb", checkpointer_uri="mongodb://localhost"
        )
    )
    # Default (memory).
    _check_metadata_durability(SkeinoSettings())


def test_sqlite_backend_end_to_end() -> None:
    # scheme=sqlite + checkpointer_uri drives both checkpointer and metadata store.
    app = create_app(
        graphs={"test_agent": lambda _ckpt: FakeGraph()},
        settings=SkeinoSettings(
            default_assistant_id="test_agent",
            assistant_name="Test Agent",
            checkpointer_scheme="sqlite",
            checkpointer_uri=":memory:",
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

        out = client.post(
            f"/threads/{thread_id}/runs/wait",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": [{"role": "user", "content": "hi"}]},
            },
        )
        assert out.status_code == 200

        listed = client.get(f"/threads/{thread_id}/runs").json()
        assert len(listed) == 1
        assert listed[0]["status"] == "success"


def test_sqlite_scheme_without_uri_defaults_to_memory() -> None:
    # scheme="sqlite" with no checkpointer_uri must work (both default :memory:),
    # not fail the durability guard.
    app = create_app(
        graphs={"test_agent": lambda _ckpt: FakeGraph()},
        settings=SkeinoSettings(
            default_assistant_id="test_agent", checkpointer_scheme="sqlite"
        ),
    )
    with TestClient(app) as client:
        thread_id = client.post("/threads", json={}).json()["thread_id"]
        assert client.get(f"/threads/{thread_id}").status_code == 200


def test_sqlite_file_shared_by_checkpointer_and_metadata_store(tmp_path: Path) -> None:
    # Checkpointer and metadata store on the SAME file — the contention
    # scenario the metadata store's WAL + busy-timeout hardening targets.
    db_path = tmp_path / "skeino.db"
    app = create_app(
        graphs={"test_agent": lambda _ckpt: FakeGraph()},
        settings=SkeinoSettings(
            default_assistant_id="test_agent",
            checkpointer_scheme="sqlite",
            checkpointer_uri=str(db_path),
        ),
    )
    with TestClient(app) as client:
        thread_id = client.post("/threads", json={"metadata": {"t": "x"}}).json()[
            "thread_id"
        ]
        out = client.post(
            f"/threads/{thread_id}/runs/wait",
            json={
                "assistant_id": "test_agent",
                "input": {"messages": [{"role": "user", "content": "hi"}]},
            },
        )
        assert out.status_code == 200
    assert db_path.exists()
