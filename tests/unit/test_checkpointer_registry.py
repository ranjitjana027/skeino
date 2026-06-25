"""Tests for the pluggable checkpointer registry."""

from contextlib import asynccontextmanager, contextmanager

import pytest

from skeino.persistence import open_checkpointer, register_checkpointer
from skeino.persistence.checkpointer import CheckpointerSpec


@pytest.mark.asyncio
async def test_memory_resolution_no_uri() -> None:
    """No URI → memory checkpointer."""
    async with open_checkpointer() as ckpt:
        assert ckpt.__class__.__name__ in {"MemorySaver", "InMemorySaver"}


@pytest.mark.asyncio
async def test_explicit_memory_scheme() -> None:
    async with open_checkpointer(scheme="memory") as ckpt:
        assert ckpt.__class__.__name__ in {"MemorySaver", "InMemorySaver"}


@pytest.mark.asyncio
async def test_unknown_scheme_raises() -> None:
    with pytest.raises(ValueError, match="No checkpointer registered"):
        async with open_checkpointer(uri="cassandra://localhost"):
            pass


@pytest.mark.asyncio
async def test_register_checkpointer_adds_scheme() -> None:
    sentinel = object()

    @register_checkpointer("skeino-test-scheme")
    @asynccontextmanager
    async def _builder(_spec: CheckpointerSpec):
        yield sentinel  # type: ignore[misc]

    async with open_checkpointer(scheme="skeino-test-scheme") as ckpt:
        assert ckpt is sentinel


class _FakeMongoSaver:
    """Records from_conn_string kwargs; yields a saver with no setup hooks."""

    calls: dict = {}

    @classmethod
    @contextmanager
    def from_conn_string(cls, conn_string=None, **kwargs):
        cls.calls = {"conn_string": conn_string, **kwargs}
        yield object()


@pytest.mark.asyncio
async def test_mongodb_builder_derives_db_name_from_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import langgraph.checkpoint.mongodb as mongodb_mod

    monkeypatch.setattr(mongodb_mod, "MongoDBSaver", _FakeMongoSaver)
    async with open_checkpointer("mongodb://host:27017/mydb", scheme="mongodb"):
        pass
    assert _FakeMongoSaver.calls["db_name"] == "mydb"


@pytest.mark.asyncio
async def test_mongodb_builder_keeps_default_db_for_pathless_uri(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import langgraph.checkpoint.mongodb as mongodb_mod

    monkeypatch.setattr(mongodb_mod, "MongoDBSaver", _FakeMongoSaver)
    async with open_checkpointer("mongodb://host:27017", scheme="mongodb"):
        pass
    # No db in the URI path → the saver's own default must be left alone.
    assert "db_name" not in _FakeMongoSaver.calls


class _FakePool:
    """Records construction kwargs; opens/closes are tracked, no real DB."""

    instances: list["_FakePool"] = []

    def __class_getitem__(cls, _item: object) -> type["_FakePool"]:
        # Support the generic subscription AsyncConnectionPool[AsyncConnection[...]].
        return cls

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.opened = False
        self.closed = False
        _FakePool.instances.append(self)

    async def open(self, wait: bool = False) -> None:
        self.opened = True

    async def close(self) -> None:
        self.closed = True


class _FakePGSaver:
    """Stand-in AsyncPostgresSaver that records the connection it was given."""

    def __init__(self, conn: object) -> None:
        self.conn = conn
        self.setup_called = False

    async def setup(self) -> None:
        self.setup_called = True


@pytest.mark.asyncio
async def test_postgres_builder_uses_checked_pool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The postgres saver is built over a liveness-checked connection pool.

    A single long-lived connection silently dies when a pooler (e.g. Supabase/
    pgbouncer) recycles it, wedging every later read with "the connection is
    closed". The builder must instead hand the saver a pool that validates and
    replaces connections on checkout.
    """
    import langgraph.checkpoint.postgres.aio as pg_aio
    import psycopg_pool

    from skeino.persistence import checkpointer as ckpt_mod

    monkeypatch.setattr(psycopg_pool, "AsyncConnectionPool", _FakePool)
    monkeypatch.setattr(pg_aio, "AsyncPostgresSaver", _FakePGSaver)
    monkeypatch.setattr(
        ckpt_mod, "build_run_enriching_checkpointer", lambda inner: inner
    )
    _FakePool.instances.clear()

    uri = "postgres://u:p@host:5432/db"
    async with open_checkpointer(uri, scheme="postgres") as ckpt:
        assert isinstance(ckpt, _FakePGSaver)
        assert ckpt.setup_called is True
        assert isinstance(ckpt.conn, _FakePool)

    assert len(_FakePool.instances) == 1
    pool = _FakePool.instances[0]
    assert pool.opened is True  # opened on enter
    assert pool.closed is True  # released on exit via the exit stack
    assert pool.kwargs["conninfo"] == uri
    # Resilience: a liveness check is configured so dropped sockets are replaced.
    assert callable(pool.kwargs["check"])
    # Pooler-safety: prepared statements off, autocommit on (saver requirement).
    conn_kwargs = pool.kwargs["kwargs"]
    assert conn_kwargs["prepare_threshold"] == 0
    assert conn_kwargs["autocommit"] is True


@pytest.mark.asyncio
async def test_postgres_builder_pool_max_size_option(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """pool_max_size flows from spec.options into the pool."""
    import langgraph.checkpoint.postgres.aio as pg_aio
    import psycopg_pool

    from skeino.persistence import checkpointer as ckpt_mod

    monkeypatch.setattr(psycopg_pool, "AsyncConnectionPool", _FakePool)
    monkeypatch.setattr(pg_aio, "AsyncPostgresSaver", _FakePGSaver)
    monkeypatch.setattr(
        ckpt_mod, "build_run_enriching_checkpointer", lambda inner: inner
    )
    _FakePool.instances.clear()

    async with open_checkpointer(
        "postgres://u:p@host:5432/db",
        scheme="postgres",
        options={"pool_max_size": 25},
    ):
        pass

    assert _FakePool.instances[0].kwargs["max_size"] == 25
