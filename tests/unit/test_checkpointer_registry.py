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
