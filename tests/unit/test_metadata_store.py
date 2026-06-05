"""Tests for the in-memory metadata store."""

from uuid import uuid4

import pytest
from skeino.persistence import InMemoryMetadataStore
from skeino.schemas import ThreadSearchRequest, ThreadTtlConfig


@pytest.mark.asyncio
async def test_create_and_fetch_thread() -> None:
    store = InMemoryMetadataStore()
    await store.setup()
    tid = str(uuid4())
    await store.create_thread(
        tid,
        metadata={"topic": "swing"},
        config={"configurable": {"thread_id": tid}},
        ttl=None,
        if_exists="raise",
    )
    row = await store.fetch_thread_row(tid)
    assert row is not None
    assert row["status"] == "idle"
    assert row["metadata"]["topic"] == "swing"


@pytest.mark.asyncio
async def test_create_thread_with_ttl_records_expiry() -> None:
    store = InMemoryMetadataStore()
    tid = str(uuid4())
    await store.create_thread(
        tid,
        metadata={},
        config={},
        ttl=ThreadTtlConfig(strategy="delete", ttl=15.0),
        if_exists="raise",
    )
    row = await store.fetch_thread_row(tid)
    assert row is not None
    ttl_payload = row["ttl"]
    assert ttl_payload is not None
    assert ttl_payload["ttl_minutes"] == 15.0
    assert "expires_at" in ttl_payload


@pytest.mark.asyncio
async def test_update_thread_status_and_search_filter() -> None:
    store = InMemoryMetadataStore()
    t1, t2 = str(uuid4()), str(uuid4())
    await store.create_thread(t1, metadata={}, config={}, ttl=None, if_exists="raise")
    await store.create_thread(t2, metadata={}, config={}, ttl=None, if_exists="raise")
    await store.update_thread(t1, status_value="busy")

    results = await store.search_thread_rows(ThreadSearchRequest(status="busy"))
    assert {str(r["thread_id"]) for r in results} == {t1}


@pytest.mark.asyncio
async def test_run_lifecycle() -> None:
    store = InMemoryMetadataStore()
    tid = str(uuid4())
    await store.create_thread(tid, metadata={}, config={}, ttl=None, if_exists="raise")
    rid = str(uuid4())
    await store.create_run(
        rid, tid, "agent", metadata={}, kwargs={}, multitask_strategy="enqueue"
    )
    await store.update_run_status(rid, "running")
    row = await store.fetch_run_row(tid, rid)
    assert row is not None
    assert row["status"] == "running"

    rows = await store.list_run_rows(tid, limit=10, offset=0, status_value="running")
    assert [str(r["run_id"]) for r in rows] == [rid]


@pytest.mark.asyncio
async def test_fetch_run_for_wrong_thread_returns_none() -> None:
    store = InMemoryMetadataStore()
    t1, t2 = str(uuid4()), str(uuid4())
    for t in (t1, t2):
        await store.create_thread(
            t, metadata={}, config={}, ttl=None, if_exists="raise"
        )
    rid = str(uuid4())
    await store.create_run(
        rid, t1, "agent", metadata={}, kwargs={}, multitask_strategy="enqueue"
    )
    assert await store.fetch_run_row(t2, rid) is None
