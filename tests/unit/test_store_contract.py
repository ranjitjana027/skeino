"""Cross-store contract test: identical row key sets across backends.

Every metadata store must return exactly the :class:`ThreadRow` /
:class:`RunRow` key sets declared next to ``MetadataStoreProtocol``. The
parametrized fixture pins InMemory/SQLite/Mongo to that contract (and thereby
to each other); Postgres is excluded — it needs a server — but its
SELECT/RETURNING column lists are written against the same TypedDicts.
"""

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
from mongomock_motor import AsyncMongoMockClient

from skeino.persistence import (
    InMemoryMetadataStore,
    MetadataStoreProtocol,
    MongoMetadataStore,
    RunRow,
    SqliteMetadataStore,
    ThreadRow,
)
from skeino.schemas import ThreadSearchRequest

THREAD_KEYS = frozenset(ThreadRow.__required_keys__)
RUN_KEYS = frozenset(RunRow.__required_keys__)


def test_contract_has_no_optional_keys() -> None:
    # Guards the key-set assertions below against becoming vacuous if the
    # TypedDicts ever gain NotRequired keys.
    assert not ThreadRow.__optional_keys__
    assert not RunRow.__optional_keys__


@pytest.fixture(params=["in_memory", "sqlite", "mongo"])
async def store(
    request: pytest.FixtureRequest, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[MetadataStoreProtocol]:
    if request.param == "in_memory":
        memory_store = InMemoryMetadataStore()
        await memory_store.setup()
        yield memory_store
    elif request.param == "sqlite":
        sqlite_store = SqliteMetadataStore(":memory:")
        await sqlite_store.setup()
        try:
            yield sqlite_store
        finally:
            await sqlite_store.aclose()
    else:
        import motor.motor_asyncio

        monkeypatch.setattr(
            motor.motor_asyncio, "AsyncIOMotorClient", AsyncMongoMockClient
        )
        mongo_store = MongoMetadataStore("mongodb://mock", db_name="skeino_contract")
        await mongo_store.setup()
        try:
            yield mongo_store
        finally:
            await mongo_store.aclose()


async def test_thread_rows_match_contract_keys(store: MetadataStoreProtocol) -> None:
    tid = str(uuid4())
    created = await store.create_thread(
        tid, metadata={"k": "v"}, config={}, ttl=None, if_exists="raise"
    )
    assert set(created) == THREAD_KEYS

    fetched = await store.fetch_thread_row(tid)
    assert fetched is not None
    assert set(fetched) == THREAD_KEYS

    searched = await store.search_thread_rows(ThreadSearchRequest(limit=10, offset=0))
    assert [set(row) for row in searched] == [THREAD_KEYS]


async def test_run_rows_match_contract_keys_through_error_lifecycle(
    store: MetadataStoreProtocol,
) -> None:
    tid, rid = str(uuid4()), str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    created = await store.create_run(
        rid, tid, "agent", metadata={}, kwargs={}, multitask_strategy="reject"
    )
    assert set(created) == RUN_KEYS
    assert created["error"] is None  # present-but-empty at create

    await store.update_run_status(rid, "error", error="boom")
    failed = await store.fetch_run_row(tid, rid)
    assert failed is not None
    assert set(failed) == RUN_KEYS
    assert failed["error"] == "boom"

    # A later update without an error must clear it, not leave it stale.
    await store.update_run_status(rid, "running")
    rows = await store.list_run_rows(tid, limit=10, offset=0, status_value=None)
    assert [set(row) for row in rows] == [RUN_KEYS]
    assert rows[0]["error"] is None


async def test_delete_run_removes_only_the_target(
    store: MetadataStoreProtocol,
) -> None:
    tid, keep, drop = str(uuid4()), str(uuid4()), str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    for rid in (keep, drop):
        await store.create_run(
            rid, tid, "agent", metadata={}, kwargs={}, multitask_strategy="enqueue"
        )

    await store.delete_run(tid, drop)
    assert await store.fetch_run_row(tid, drop) is None
    assert await store.fetch_run_row(tid, keep) is not None

    # Wrong thread scope must not delete the row.
    await store.delete_run(str(uuid4()), keep)
    assert await store.fetch_run_row(tid, keep) is not None
