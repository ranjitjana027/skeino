"""Mongo-specific metadata-store behaviour (database-name resolution).

Protocol behaviour — row shapes, conflicts, updates, search, run scoping —
lives in the shared contract (``tests/store_contract.py``), run against
mongomock by ``tests/unit/test_store_contract.py`` and against a real server
by ``tests/api/test_store_contract_backends.py``.
"""

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
