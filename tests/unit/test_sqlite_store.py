"""Unit tests for the SQLite-backed metadata store.

Exercises the same contract as the in-memory/Postgres stores against a
``:memory:`` SQLite database, and checks that returned row shapes match
(UUID ids, datetime timestamps, dict JSON columns).
"""

from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from skeino.persistence import SqliteMetadataStore
from skeino.persistence.sqlite_store import _BUSY_TIMEOUT_MS
from skeino.schemas import ThreadSearchRequest, ThreadTtlConfig


async def _store() -> SqliteMetadataStore:
    store = SqliteMetadataStore(":memory:")
    await store.setup()
    return store


async def test_create_fetch_thread_row_shapes() -> None:
    store = await _store()
    try:
        tid = str(uuid4())
        row = await store.create_thread(
            tid,
            metadata={"topic": "swing"},
            config={"configurable": {"thread_id": tid}},
            ttl=ThreadTtlConfig(strategy="delete", ttl=5.0),
            if_exists="raise",
        )
        assert row["thread_id"] == UUID(tid)
        assert isinstance(row["created_at"], datetime)
        assert row["metadata"] == {"topic": "swing"}
        assert row["status"] == "idle"
        assert row["ttl"]["ttl_minutes"] == 5.0

        fetched = await store.fetch_thread_row(tid)
        assert fetched is not None
        assert fetched["thread_id"] == UUID(tid)
        assert isinstance(fetched["created_at"], datetime)
        assert fetched["metadata"] == {"topic": "swing"}
        assert fetched["ttl"]["strategy"] == "delete"
    finally:
        await store.aclose()


async def test_create_thread_conflict_and_do_nothing() -> None:
    store = await _store()
    try:
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        # Second create with raise → 409.
        with pytest.raises(Exception) as exc_info:
            await store.create_thread(
                tid, metadata={}, config={}, ttl=None, if_exists="raise"
            )
        assert getattr(exc_info.value, "status_code", None) == 409
        # do_nothing → returns the existing row.
        existing = await store.create_thread(
            tid, metadata={"x": 1}, config={}, ttl=None, if_exists="do_nothing"
        )
        assert existing["thread_id"] == UUID(tid)
    finally:
        await store.aclose()


async def test_update_thread_and_search() -> None:
    store = await _store()
    try:
        a, b = str(uuid4()), str(uuid4())
        await store.create_thread(
            a, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.create_thread(
            b, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.update_thread(a, status_value="busy", metadata={"k": "v"})

        busy = await store.search_thread_rows(
            ThreadSearchRequest(status="busy", limit=10, offset=0)
        )
        assert [str(r["thread_id"]) for r in busy] == [a]
        assert busy[0]["metadata"] == {"k": "v"}

        all_rows = await store.search_thread_rows(
            ThreadSearchRequest(limit=10, offset=0)
        )
        assert {str(r["thread_id"]) for r in all_rows} == {a, b}
    finally:
        await store.aclose()


async def test_runs_lifecycle_and_scoping() -> None:
    store = await _store()
    try:
        tid = str(uuid4())
        other = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        rid = str(uuid4())
        run = await store.create_run(
            rid,
            tid,
            assistant_id="agent",
            metadata={},
            kwargs={"stream_mode": "values"},
            multitask_strategy="enqueue",
        )
        assert run["run_id"] == UUID(rid)
        assert run["status"] == "pending"
        assert run["kwargs"] == {"stream_mode": "values"}

        await store.update_run_status(rid, "success")
        fetched = await store.fetch_run_row(tid, rid)
        assert fetched is not None and fetched["status"] == "success"

        # Run is scoped to its thread.
        assert await store.fetch_run_row(other, rid) is None

        rows = await store.list_run_rows(tid, limit=10, offset=0, status_value=None)
        assert [str(r["run_id"]) for r in rows] == [rid]
        assert (
            await store.list_run_rows(tid, limit=10, offset=0, status_value="error")
            == []
        )
    finally:
        await store.aclose()


async def test_delete_thread_cascades_runs() -> None:
    store = await _store()
    try:
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.create_run(
            str(uuid4()),
            tid,
            assistant_id="a",
            metadata={},
            kwargs={},
            multitask_strategy="enqueue",
        )
        await store.delete_thread(tid)
        assert await store.fetch_thread_row(tid) is None
        assert (
            await store.list_run_rows(tid, limit=10, offset=0, status_value=None) == []
        )
    finally:
        await store.aclose()


async def test_file_backed_store_enables_wal_and_busy_timeout(tmp_path: Path) -> None:
    store = SqliteMetadataStore(str(tmp_path / "meta.db"))
    await store.setup()
    try:
        cursor = await store._conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "wal"
        cursor = await store._conn.execute("PRAGMA busy_timeout")
        assert (await cursor.fetchone())[0] == _BUSY_TIMEOUT_MS
    finally:
        await store.aclose()


async def test_memory_store_setup_unaffected_by_wal_pragma() -> None:
    # journal_mode=WAL is a documented no-op on ":memory:" databases.
    store = await _store()
    try:
        cursor = await store._conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "memory"
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        assert await store.fetch_thread_row(tid) is not None
    finally:
        await store.aclose()
