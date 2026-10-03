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


async def _mongo_store(monkeypatch: pytest.MonkeyPatch) -> MongoMetadataStore:
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    store = MongoMetadataStore("mongodb://mock", db_name=f"r{uuid4().hex}")
    await store.setup()
    return store


async def _busy_thread(store: MongoMetadataStore) -> str:
    tid = str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    await store.update_thread(tid, status_value="busy")
    return tid


def _create_run(store: MongoMetadataStore, tid: str, run_id: str | None = None) -> Any:
    return store.create_run(
        run_id or str(uuid4()),
        tid,
        "agent",
        metadata={},
        kwargs={},
        multitask_strategy="enqueue",
    )


async def test_release_busy_thread_backs_off_while_a_run_is_being_created(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The worst interleaving: the release's in-flight check runs before the
    # run's insert, and its write after the insert but before the run's
    # creation has finished. The thread was already reserved for that insert
    # when the release read it, so the release must back off.
    import asyncio

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        insert, find_run = store._runs.insert_one, store._runs.find_one
        update = store._threads.update_one
        go, inserted, finish = asyncio.Event(), asyncio.Event(), asyncio.Event()
        entered = asyncio.Event()

        async def held_insert(*args: Any, **kwargs: Any) -> Any:
            entered.set()  # reserved, about to insert
            await go.wait()
            result = await insert(*args, **kwargs)
            inserted.set()
            return result

        async def held_finish(
            filter_: Any, change: Any, *args: Any, **kwargs: Any
        ) -> Any:
            if "$unset" in change and "status" not in filter_:
                await finish.wait()  # the run's creation not finished yet
            return await update(filter_, change, *args, **kwargs)

        async def racing(*args: Any, **kwargs: Any) -> Any:
            found = await find_run(*args, **kwargs)  # before the insert
            go.set()
            await inserted.wait()  # the write comes after it
            return found

        monkeypatch.setattr(store._runs, "insert_one", held_insert)
        monkeypatch.setattr(store._threads, "update_one", held_finish)
        monkeypatch.setattr(store._runs, "find_one", racing)
        creating = asyncio.create_task(_create_run(store, tid))
        await asyncio.wait_for(entered.wait(), timeout=5)
        releasing = asyncio.create_task(store.release_busy_thread(tid, "error"))
        assert await asyncio.wait_for(releasing, timeout=5) is False
        go.set()  # a release that backed off early never let the insert through
        finish.set()
        await creating
        row = await store.fetch_thread_row(tid)
        assert row is not None and row["status"] == "busy"
    finally:
        await store.aclose()


async def test_release_busy_thread_ignores_an_expired_creation_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A process that died mid-creation leaves its reservation behind: it must
    # not block the thread's release for good.
    from datetime import UTC, datetime, timedelta

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        dead = datetime.now(UTC) - timedelta(minutes=5)
        await store._threads.update_one(
            {"_id": tid}, {"$set": {f"creating.{uuid4()}": dead}}
        )
        assert await store.release_busy_thread(tid, "error") is True
        doc = await store._threads.find_one({"_id": tid})
        assert doc is not None and not doc.get("creating")
    finally:
        await store.aclose()


async def test_slow_run_creation_renews_its_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A live creator stuck in its insert past the reservation's lifetime keeps
    # it fresh, so a release meanwhile still backs off. A fake clock makes the
    # lifetime pass, so the test waits on the renewal itself, not on time.
    import asyncio
    from datetime import UTC, datetime, timedelta

    import skeino.persistence.mongo_store as mongo_store

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        clock = datetime.now(UTC)
        monkeypatch.setattr(mongo_store, "utcnow", lambda: clock)
        monkeypatch.setattr(mongo_store, "_CREATION_RESERVATION_RENEWAL", timedelta(0))
        insert, update = store._runs.insert_one, store._threads.update_one
        go, entered, renewed = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def slow_insert(*args: Any, **kwargs: Any) -> Any:
            entered.set()  # reserved, about to insert
            await go.wait()
            return await insert(*args, **kwargs)

        async def watched(filter_: Any, change: Any, *args: Any, **kwargs: Any) -> Any:
            result = await update(filter_, change, *args, **kwargs)
            stamps = change.get("$set", {}).values()
            if len(filter_) > 1 and clock in stamps:
                renewed.set()  # renewed after the clock moved past the lifetime
            return result

        monkeypatch.setattr(store._runs, "insert_one", slow_insert)
        monkeypatch.setattr(store._threads, "update_one", watched)
        creating = asyncio.create_task(_create_run(store, tid))
        await asyncio.wait_for(entered.wait(), timeout=5)
        clock += timedelta(minutes=10)  # well past the reservation's lifetime
        await asyncio.wait_for(renewed.wait(), timeout=5)
        assert await store.release_busy_thread(tid, "idle") is False
        go.set()
        await asyncio.wait_for(creating, timeout=5)
        row = await store.fetch_thread_row(tid)
        assert row is not None and row["status"] == "busy"
    finally:
        await store.aclose()


async def test_reservation_renewal_survives_a_failed_write_and_stops_once_taken(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    import asyncio
    from datetime import timedelta

    import skeino.persistence.mongo_store as mongo_store

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        monkeypatch.setattr(mongo_store, "_CREATION_RESERVATION_RENEWAL", timedelta(0))
        update, calls = store._threads.update_one, 0

        async def flaky(*args: Any, **kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                raise RuntimeError("connection reset")
            return await update(*args, **kwargs)

        monkeypatch.setattr(store._threads, "update_one", flaky)
        # No reservation on the thread: a release took it, so renewal stops.
        await asyncio.wait_for(
            store._renew_reservation(tid, f"creating.{uuid4()}"), timeout=5
        )
        assert calls == 2
        assert "Failed to renew run-creation reservation" in caplog.text
    finally:
        await store.aclose()


async def test_slow_run_creation_undoes_its_run_once_its_reservation_expired(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A live creator stuck in its insert past the reservation's expiry: a
    # release meanwhile ignores the reservation and frees the thread, so the
    # creator, finding its reservation gone, must not leave its run behind.
    import asyncio
    from datetime import timedelta

    import skeino.persistence.mongo_store as mongo_store

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        monkeypatch.setattr(mongo_store, "_CREATION_RESERVATION_TTL", timedelta(0))
        insert, go, entered = store._runs.insert_one, asyncio.Event(), asyncio.Event()

        async def slow_insert(*args: Any, **kwargs: Any) -> Any:
            entered.set()  # reserved, about to insert
            await go.wait()
            return await insert(*args, **kwargs)

        monkeypatch.setattr(store._runs, "insert_one", slow_insert)
        run_id = str(uuid4())
        creating = asyncio.create_task(_create_run(store, tid, run_id))
        await asyncio.wait_for(entered.wait(), timeout=5)
        assert await store.release_busy_thread(tid, "idle") is True
        go.set()
        with pytest.raises(RuntimeError, match="outlived its creation reservation"):
            await asyncio.wait_for(creating, timeout=5)
        assert await store._runs.find_one({"_id": run_id}) is None
    finally:
        await store.aclose()


async def test_release_busy_thread_loses_to_a_slow_creation_finishing_first(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The same expired reservation, but the creator inserts and finishes
    # between the release's run check and its write: the release must fail.
    import asyncio
    from datetime import timedelta

    import skeino.persistence.mongo_store as mongo_store

    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)
        monkeypatch.setattr(mongo_store, "_CREATION_RESERVATION_TTL", timedelta(0))
        insert, find_run = store._runs.insert_one, store._runs.find_one
        go, created, entered = asyncio.Event(), asyncio.Event(), asyncio.Event()

        async def slow_insert(*args: Any, **kwargs: Any) -> Any:
            entered.set()  # reserved, about to insert
            await go.wait()
            return await insert(*args, **kwargs)

        async def racing(*args: Any, **kwargs: Any) -> Any:
            found = await find_run(*args, **kwargs)  # before the insert
            go.set()
            await created.wait()  # the write comes after the whole creation
            return found

        monkeypatch.setattr(store._runs, "insert_one", slow_insert)
        monkeypatch.setattr(store._runs, "find_one", racing)
        run_id = str(uuid4())
        creating = asyncio.create_task(_create_run(store, tid, run_id))
        await asyncio.wait_for(entered.wait(), timeout=5)
        releasing = asyncio.create_task(store.release_busy_thread(tid, "idle"))
        await asyncio.wait_for(creating, timeout=5)
        created.set()
        assert await asyncio.wait_for(releasing, timeout=5) is False
        row = await store.fetch_thread_row(tid)
        assert row is not None and row["status"] == "busy"
        assert await find_run({"_id": run_id}) is not None
    finally:
        await store.aclose()


async def test_run_insert_failing_drops_its_reservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = await _mongo_store(monkeypatch)
    try:
        tid = await _busy_thread(store)

        async def broken(*_args: Any, **_kwargs: Any) -> Any:
            raise RuntimeError("connection reset")

        monkeypatch.setattr(store._runs, "insert_one", broken)
        with pytest.raises(RuntimeError, match="connection reset"):
            await _create_run(store, tid)
        # No reservation left to hold off the release.
        assert await store.release_busy_thread(tid, "error") is True
    finally:
        await store.aclose()


async def test_run_creation_failing_after_its_insert_leaves_no_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Clearing the reservation follows the insert: if it fails, the inserted
    # run has no owner and must not linger ``pending`` until the orphan timeout.
    store = await _mongo_store(monkeypatch)
    try:
        tid, run_id = await _busy_thread(store), str(uuid4())
        update = store._threads.update_one

        async def broken(filter_: Any, change: Any, *args: Any, **kwargs: Any) -> Any:
            if "$unset" in change:
                raise RuntimeError("connection reset")
            return await update(filter_, change, *args, **kwargs)

        monkeypatch.setattr(store._threads, "update_one", broken)
        with pytest.raises(RuntimeError, match="connection reset"):
            await _create_run(store, tid, run_id)
        assert await store.fetch_run_row(tid, run_id) is None
    finally:
        await store.aclose()
