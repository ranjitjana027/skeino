"""The metadata-store contract (``tests/store_contract.py``) on real servers.

The default suite runs the contract against InMemory, SQLite and mongomock;
this runs the identical class against the Postgres and MongoDB services from
``docker-compose.yml``, so a backend can't pass on a fake and diverge on the
real driver (BSON datetime precision, Postgres ``timestamptz`` round-trips).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import ClassVar
from uuid import uuid4

import pytest

from skeino.persistence import (
    MetadataStore,
    MetadataStoreProtocol,
    MongoMetadataStore,
)
from tests.api.conftest import MONGODB_URI, POSTGRES_URI
from tests.store_contract import MONGO_TIMESTAMP_GAPS, StoreContract


class TestPostgresStoreContract(StoreContract):
    @pytest.fixture
    async def store(self) -> AsyncIterator[MetadataStoreProtocol]:
        postgres_store = MetadataStore(POSTGRES_URI)
        await postgres_store.setup()
        import psycopg

        # The contract assumes an empty store; the tables are shared.
        async with await psycopg.AsyncConnection.connect(POSTGRES_URI) as conn:
            await conn.execute("TRUNCATE app_runs, app_threads")
        try:
            yield postgres_store
        finally:
            await postgres_store.aclose()


class TestMongoStoreContract(StoreContract):
    KNOWN_GAPS: ClassVar[dict[str, str]] = MONGO_TIMESTAMP_GAPS

    @pytest.fixture
    async def store(self) -> AsyncIterator[MetadataStoreProtocol]:
        db_name = f"skeino_contract_{uuid4().hex}"
        mongo_store = MongoMetadataStore(MONGODB_URI, db_name=db_name)
        await mongo_store.setup()
        try:
            yield mongo_store
        finally:
            from pymongo import MongoClient

            MongoClient(MONGODB_URI).drop_database(db_name)
            await mongo_store.aclose()
