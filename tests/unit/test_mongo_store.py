"""Unit tests for the MongoDB-backed metadata store.

Runs against an in-memory ``mongomock-motor`` client (no MongoDB server) by
patching motor's ``AsyncIOMotorClient``, and checks the row shapes match the
other stores (UUID ids, datetime timestamps, dict fields).
"""

from collections.abc import AsyncIterator
from datetime import datetime
from uuid import UUID, uuid4

import pytest
from mongomock_motor import AsyncMongoMockClient

from skeino.persistence import MongoMetadataStore
from skeino.schemas import ThreadSearchRequest, ThreadTtlConfig


@pytest.fixture
async def store(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[MongoMetadataStore]:
    import motor.motor_asyncio

    monkeypatch.setattr(motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient)
    s = MongoMetadataStore("mongodb://mock", db_name="skeino_test")
    await s.setup()
    try:
        yield s
    finally:
        await s.aclose()


async def test_create_fetch_thread_row_shapes(store: MongoMetadataStore) -> None:
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


async def test_create_thread_conflict_and_do_nothing(store: MongoMetadataStore) -> None:
    tid = str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    with pytest.raises(Exception) as exc_info:
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
    assert getattr(exc_info.value, "status_code", None) == 409
    existing = await store.create_thread(
        tid, metadata={"x": 1}, config={}, ttl=None, if_exists="do_nothing"
    )
    assert existing["thread_id"] == UUID(tid)


async def test_update_thread_and_search(store: MongoMetadataStore) -> None:
    a, b = str(uuid4()), str(uuid4())
    await store.create_thread(a, metadata={}, config={}, ttl=None, if_exists="raise")
    await store.create_thread(b, metadata={}, config={}, ttl=None, if_exists="raise")
    await store.update_thread(a, status_value="busy", metadata={"k": "v"})

    busy = await store.search_thread_rows(
        ThreadSearchRequest(status="busy", limit=10, offset=0)
    )
    assert [str(r["thread_id"]) for r in busy] == [a]
    assert busy[0]["metadata"] == {"k": "v"}

    all_rows = await store.search_thread_rows(ThreadSearchRequest(limit=10, offset=0))
    assert {str(r["thread_id"]) for r in all_rows} == {a, b}


async def test_runs_lifecycle_and_scoping(store: MongoMetadataStore) -> None:
    tid, other = str(uuid4()), str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    rid = str(uuid4())
    run = await store.create_run(
        rid,
        tid,
        assistant_id="agent",
        metadata={},
        kwargs={"k": 1},
        multitask_strategy="enqueue",
    )
    assert run["run_id"] == UUID(rid)
    assert run["status"] == "pending"

    await store.update_run_status(rid, "success")
    fetched = await store.fetch_run_row(tid, rid)
    assert fetched is not None and fetched["status"] == "success"
    assert await store.fetch_run_row(other, rid) is None

    rows = await store.list_run_rows(tid, limit=10, offset=0, status_value=None)
    assert [str(r["run_id"]) for r in rows] == [rid]


async def test_delete_thread_cascades_runs(store: MongoMetadataStore) -> None:
    tid = str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
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
    assert await store.list_run_rows(tid, limit=10, offset=0, status_value=None) == []


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
