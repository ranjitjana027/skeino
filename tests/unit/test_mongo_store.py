"""Mongo-specific metadata-store behaviour (database-name resolution).

Protocol behaviour — row shapes, conflicts, updates, search, run scoping —
lives in the shared contract (``tests/store_contract.py``), run against
mongomock by ``tests/unit/test_store_contract.py`` and against a real server
by ``tests/api/test_store_contract_backends.py``.
"""

from typing import Any
from uuid import uuid4

import pytest
from mongomock_motor import AsyncMongoMockClient

from skeino.persistence import MongoMetadataStore


def test_db_name_derived_from_uri_path() -> None:
    assert MongoMetadataStore("mongodb://host:27017/customdb")._db_name == "customdb"


def test_explicit_db_name_wins_over_uri_path() -> None:
    store = MongoMetadataStore("mongodb://host:27017/customdb", db_name="explicit")
    assert store._db_name == "explicit"


def test_pathless_uri_falls_back_to_default_db() -> None:
    assert MongoMetadataStore("mongodb://host:27017")._db_name == "skeino"


async def test_setup_uses_uri_database(monkeypatch: pytest.MonkeyPatch) -> None:
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock/customdb")
    await store.setup()
    try:
        assert store._threads.database.name == "customdb"
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        assert await store.fetch_thread_row(tid) is not None
    finally:
        await store.aclose()


@pytest.mark.parametrize("marks_busy", [True, False])
async def test_release_busy_thread_loses_to_a_run_started_after_its_check(
    monkeypatch: pytest.MonkeyPatch, marks_busy: bool
) -> None:
    # #140: threads and runs are separate collections, so the in-flight check
    # and the write are two operations. A run another worker starts between
    # them bumps the thread's version (on insert, and again when it marks the
    # thread ``busy``): the compare-and-set must then fail. Without the busy
    # write too: a queued run waiting for the thread is still in flight.
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock", db_name=f"r{uuid4().hex}")
    await store.setup()
    try:
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.update_thread(tid, status_value="busy")
        find_run = store._runs.find_one

        async def racing(*args: Any, **kwargs: Any) -> Any:
            found = await find_run(*args, **kwargs)  # nothing in flight yet
            run_id = str(uuid4())
            await store.create_run(
                run_id,
                tid,
                "agent",
                metadata={},
                kwargs={},
                multitask_strategy="enqueue",
            )
            if marks_busy:
                await store.update_thread(tid, status_value="busy")
            return found

        store._runs.find_one = racing
        assert await store.release_busy_thread(tid, "error") is False
        row = await store.fetch_thread_row(tid)
        assert row is not None and row["status"] == "busy"
    finally:
        await store.aclose()


async def test_run_creation_failing_after_its_insert_leaves_no_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The version bump follows the insert: if it fails, the inserted run has
    # no owner and must not linger ``pending`` until the orphan timeout.
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock", db_name=f"r{uuid4().hex}")
    await store.setup()
    try:
        tid, run_id = str(uuid4()), str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )

        async def broken(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("connection reset")

        monkeypatch.setattr(store._threads, "update_one", broken)
        with pytest.raises(RuntimeError, match="connection reset"):
            await store.create_run(
                run_id,
                tid,
                "agent",
                metadata={},
                kwargs={},
                multitask_strategy="enqueue",
            )
        assert await store.fetch_run_row(tid, run_id) is None
    finally:
        await store.aclose()


async def test_release_busy_thread_sees_a_run_whose_insert_straddles_its_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Why ``create_run`` bumps the version after its insert, not before: a
    # bump-first run could bump before the release reads the version, then
    # insert between the release's in-flight check and its write, and the
    # compare-and-set would release a thread with that run in flight.
    import asyncio

    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock", db_name=f"r{uuid4().hex}")
    await store.setup()
    try:
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.update_thread(tid, status_value="busy")
        insert, find_run = store._runs.insert_one, store._runs.find_one
        go = asyncio.Event()

        async def slow_insert(*args: Any, **kwargs: Any) -> Any:
            await go.wait()  # held until the release has checked
            return await insert(*args, **kwargs)

        monkeypatch.setattr(store._runs, "insert_one", slow_insert)
        creating = asyncio.create_task(
            store.create_run(
                str(uuid4()),
                tid,
                "agent",
                metadata={},
                kwargs={},
                multitask_strategy="enqueue",
            )
        )
        await asyncio.sleep(0)  # the run's creation is under way

        async def racing(*args: Any, **kwargs: Any) -> Any:
            found = await find_run(*args, **kwargs)  # not inserted yet
            go.set()
            await creating  # inserted (and bumped) before the release writes
            return found

        monkeypatch.setattr(store._runs, "find_one", racing)
        assert await store.release_busy_thread(tid, "error") is False
        row = await store.fetch_thread_row(tid)
        assert row is not None and row["status"] == "busy"
    finally:
        await store.aclose()
