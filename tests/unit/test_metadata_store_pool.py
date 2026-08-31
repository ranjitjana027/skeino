"""Connection reuse in the Postgres metadata store.

Every operation used to open its own connection — a TCP connect, TLS handshake
and SCRAM exchange per query. Against a managed Postgres in another region that
dominates the latency of any request that issues more than one query. These
tests pin the pooling without needing a live server: psycopg and psycopg_pool
are swapped for fakes that count what gets opened.
"""

from typing import Any
from uuid import uuid4

import pytest

from skeino.persistence import metadata_store as ms
from skeino.schemas import ThreadSearchRequest


class _FakeCursor:
    def __init__(self, conn: "_FakeConnection") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeCursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def execute(self, query: str, values: Any = None) -> None:
        self._conn.queries.append(query)

    async def fetchone(self) -> None:
        return None

    async def fetchall(self) -> list[Any]:
        return []


class _FakeConnection:
    def __init__(self) -> None:
        self.queries: list[str] = []
        self.commits = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(self)

    async def commit(self) -> None:
        self.commits += 1


class _ConnectionCheckout:
    def __init__(self, pool: "_FakePool") -> None:
        self._pool = pool

    async def __aenter__(self) -> _FakeConnection:
        self._pool.checkouts += 1
        return self._pool.conn

    async def __aexit__(self, *exc: Any) -> None:
        return None


class _FakePool:
    """Stands in for psycopg_pool.AsyncConnectionPool."""

    instances: list["_FakePool"] = []

    def __init__(self, **kwargs: Any) -> None:
        self.kwargs = kwargs
        self.opened = 0
        self.closed = 0
        self.checkouts = 0
        self.conn = _FakeConnection()
        _FakePool.instances.append(self)

    async def open(self, wait: bool = False) -> None:
        self.opened += 1

    async def close(self) -> None:
        self.closed += 1

    def connection(self) -> _ConnectionCheckout:
        return _ConnectionCheckout(self)


class _FakeDirectCursor:
    """Cursor on the standalone connection used for index builds."""

    def __init__(self, conn: "_FakeDirectConnection") -> None:
        self._conn = conn

    async def __aenter__(self) -> "_FakeDirectCursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def execute(self, query: str, values: Any = None) -> None:
        self._conn.queries.append(query)

    async def fetchone(self) -> Any:
        # Stands in for the pg_index invalid-index probe.
        return (1,) if self._conn.invalid_indexes else None


class _FakeDirectConnection:
    """Stands in for psycopg.AsyncConnection.connect()."""

    instances: list["_FakeDirectConnection"] = []

    def __init__(self, conninfo: str, **kwargs: Any) -> None:
        self.conninfo = conninfo
        self.kwargs = kwargs
        self.queries: list[str] = []
        self.closed = 0
        self.invalid_indexes = False
        _FakeDirectConnection.instances.append(self)

    def cursor(self) -> _FakeDirectCursor:
        return _FakeDirectCursor(self)

    async def close(self) -> None:
        self.closed += 1


class _FakeAsyncConnection:
    @staticmethod
    async def connect(conninfo: str, **kwargs: Any) -> _FakeDirectConnection:
        return _FakeDirectConnection(conninfo, **kwargs)


class _FakePsycopg:
    AsyncConnection = _FakeAsyncConnection


@pytest.fixture
def pooled_store(monkeypatch: pytest.MonkeyPatch) -> ms.MetadataStore:
    """A MetadataStore whose psycopg imports are replaced with fakes."""
    _FakePool.instances.clear()
    _FakeDirectConnection.instances.clear()
    monkeypatch.setattr(ms, "_pg_pool", lambda: _FakePool)
    monkeypatch.setattr(ms, "_pg", lambda: (_FakePsycopg, object()))
    return ms.MetadataStore("postgresql://fake/db")


async def test_operations_share_one_pool(pooled_store: ms.MetadataStore) -> None:
    tid = str(uuid4())
    await pooled_store.setup()
    await pooled_store.fetch_thread_row(tid)
    await pooled_store.fetch_thread_row(tid)
    await pooled_store.search_thread_rows(ThreadSearchRequest(limit=10, offset=0))
    await pooled_store.delete_thread(tid)

    assert len(_FakePool.instances) == 1, (
        f"{len(_FakePool.instances)} pools built — each operation is still "
        "opening its own connection"
    )
    pool = _FakePool.instances[0]
    assert pool.opened == 1
    assert pool.checkouts == 5, "every operation should borrow from the pool"


async def test_pool_is_configured_for_a_transaction_mode_pooler(
    pooled_store: ms.MetadataStore,
) -> None:
    # Behind pgbouncer/Supabase in transaction mode a later query can land on a
    # different server session, so client-side prepared statements must be off;
    # `check` discards a connection the server has already dropped instead of
    # handing it out closed.
    await pooled_store.fetch_thread_row(str(uuid4()))

    kwargs = _FakePool.instances[0].kwargs
    # None disables prepared statements; 0 would prepare on the FIRST
    # execution, which is what breaks behind a transaction-mode pooler.
    assert kwargs["kwargs"]["prepare_threshold"] is None
    assert kwargs["check"] is not None
    assert kwargs["max_size"] == 10


async def test_aclose_closes_the_pool_and_allows_reopen(
    pooled_store: ms.MetadataStore,
) -> None:
    await pooled_store.fetch_thread_row(str(uuid4()))
    await pooled_store.aclose()

    assert _FakePool.instances[0].closed == 1

    # A store reused after shutdown must not hand out a closed pool.
    await pooled_store.fetch_thread_row(str(uuid4()))
    assert len(_FakePool.instances) == 2


async def test_aclose_is_a_noop_when_nothing_was_opened(
    pooled_store: ms.MetadataStore,
) -> None:
    # app.py registers aclose *before* setup() so a failed setup still unwinds.
    await pooled_store.aclose()
    assert _FakePool.instances == []


async def test_indexes_are_built_concurrently_outside_a_transaction(
    pooled_store: ms.MetadataStore,
) -> None:
    # A plain CREATE INDEX locks out writes for the whole scan, so rolling this
    # out against an existing large app_threads would stall traffic on every
    # instance's startup. CONCURRENTLY cannot run in a transaction, so it must
    # go over its own autocommit connection rather than a pooled one.
    await pooled_store.setup()

    pooled = _FakePool.instances[0].conn.queries
    assert not any("INDEX" in q for q in pooled), (
        "indexes were built on the pooled (transactional) connection"
    )

    assert len(_FakeDirectConnection.instances) == 1
    direct = _FakeDirectConnection.instances[0]
    assert direct.kwargs.get("autocommit") is True, (
        "CREATE INDEX CONCURRENTLY cannot run inside a transaction"
    )
    created = [q for q in direct.queries if q.startswith("CREATE INDEX")]
    assert len(created) == 2
    assert all("CONCURRENTLY" in q for q in created), created
    assert direct.closed == 1, "the index connection was not closed"


async def test_invalid_leftover_index_is_dropped_and_rebuilt(
    monkeypatch: pytest.MonkeyPatch, pooled_store: ms.MetadataStore
) -> None:
    # A concurrent build that fails leaves an INVALID index behind. CREATE INDEX
    # IF NOT EXISTS then skips it forever, so the table stays silently
    # unindexed — the failure mode this repair exists to prevent.
    original = _FakeDirectConnection.__init__

    def _init(self: Any, conninfo: str, **kwargs: Any) -> None:
        original(self, conninfo, **kwargs)
        self.invalid_indexes = True

    monkeypatch.setattr(_FakeDirectConnection, "__init__", _init)

    await pooled_store.setup()

    queries = _FakeDirectConnection.instances[0].queries
    dropped = [q for q in queries if q.startswith("DROP INDEX")]
    assert len(dropped) == 2, f"invalid indexes were not dropped: {queries}"
    assert all("CONCURRENTLY" in q for q in dropped), dropped
    # Every drop must be followed by a rebuild, else the repair loses the index.
    assert len([q for q in queries if q.startswith("CREATE INDEX")]) == 2
