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


async def test_release_busy_thread_waits_for_a_run_being_inserted(
    postgres_backend: Backend,
) -> None:
    # #140: a run another worker is inserting (not yet committed) when the
    # release runs must still keep the thread busy. A single UPDATE ... WHERE
    # NOT EXISTS reads one snapshot and would miss it; the release locks the
    # thread row, so it waits for that insert and then sees the run.
    import asyncio
    from uuid import uuid4

    from skeino.persistence import MetadataStore

    store = MetadataStore(postgres_backend.uri)
    await store.setup()
    try:
        thread_id = str(uuid4())
        await store.create_thread(
            thread_id, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.update_thread(thread_id, status_value="busy")
        async with await psycopg.AsyncConnection.connect(postgres_backend.uri) as other:
            await other.execute(
                "INSERT INTO app_runs (run_id, thread_id, assistant_id, status, "
                "multitask_strategy) VALUES (%s, %s, 'agent', 'pending', 'enqueue')",
                (str(uuid4()), thread_id),
            )
            release = asyncio.create_task(store.release_busy_thread(thread_id, "error"))
            await asyncio.sleep(0.3)
            assert not release.done()  # blocked on the uncommitted insert
            await other.commit()
            assert await asyncio.wait_for(release, timeout=5) is False
        row = await store.fetch_thread_row(thread_id)
        assert row is not None and row["status"] == "busy"
    finally:
        await store.aclose()
