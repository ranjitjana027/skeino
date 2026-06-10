"""Postgres-only assertions at the SQL level: rows really land in the tables."""

import psycopg

from tests.api.conftest import (
    Backend,
    api_client,
    create_thread,
    run_to_completion,
)


def _scalar(uri: str, query: str, *params: object) -> object:
    with psycopg.connect(uri) as conn:
        row = conn.execute(query, params).fetchone()
    return row[0] if row else None


def test_rows_actually_in_postgres_and_delete_cascades(
    postgres_backend: Backend,
) -> None:
    uri = postgres_backend.uri
    with api_client(postgres_backend) as client:
        thread_id = create_thread(client, metadata={"check": "sql"})
        run_to_completion(client, thread_id, "persist me")

        status = _scalar(
            uri, "SELECT status FROM app_threads WHERE thread_id = %s", thread_id
        )
        assert status == "idle"
        run_status = _scalar(
            uri, "SELECT status FROM app_runs WHERE thread_id = %s", thread_id
        )
        assert run_status == "success"
        checkpoint_count = _scalar(
            uri, "SELECT COUNT(*) FROM checkpoints WHERE thread_id = %s", thread_id
        )
        assert isinstance(checkpoint_count, int) and checkpoint_count >= 1

        # Deleting via the API removes metadata rows (FK cascade) AND the
        # langgraph checkpoints (real adelete_thread — FakeGraph fakes this).
        assert client.delete(f"/threads/{thread_id}").status_code == 204
        assert (
            _scalar(
                uri,
                "SELECT COUNT(*) FROM app_threads WHERE thread_id = %s",
                thread_id,
            )
            == 0
        )
        assert (
            _scalar(
                uri,
                "SELECT COUNT(*) FROM app_runs WHERE thread_id = %s",
                thread_id,
            )
            == 0
        )
        assert (
            _scalar(
                uri,
                "SELECT COUNT(*) FROM checkpoints WHERE thread_id = %s",
                thread_id,
            )
            == 0
        )
