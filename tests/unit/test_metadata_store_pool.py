"""Connection reuse in the Postgres metadata store.

Every operation used to open its own connection — a TCP connect, TLS handshake
and SCRAM exchange per query. Against a managed Postgres in another region that
dominates the latency of any request that issues more than one query. These
tests pin the pooling without needing a live server: psycopg and psycopg_pool
are swapped for fakes that count what gets opened.
"""

import asyncio
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
        # Must actually suspend: bringing a real pool up awaits, and that is the
        # window in which concurrent first callers each see `_pool is None`.
        # Without a yield here the interleaving never happens and any test of
        # it passes vacuously.
        await asyncio.sleep(0)
        self.opened += 1

    async def close(self) -> None:
        self.closed += 1

    def connection(self) -> _ConnectionCheckout:
        return _ConnectionCheckout(self)


class _FakeDirectCursor:
    """Cursor on the standalone connection used for index builds."""

    def __init__(self, conn: "_FakeDirectConnection") -> None:
        self._conn = conn
        self._last = ""

    async def __aenter__(self) -> "_FakeDirectCursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        return None

    async def execute(self, query: str, values: Any = None) -> None:
        self._last = query
        self._conn.queries.append(query)

    async def fetchone(self) -> Any:
        if "pg_try_advisory_lock" in self._last:
            return (self._conn.lock_acquired,)
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
        self.lock_acquired = True
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


async def test_index_maintenance_holds_an_advisory_lock(
    pooled_store: ms.MetadataStore,
) -> None:
    # indisvalid = false also describes an index a peer is building right now:
    # CREATE INDEX CONCURRENTLY publishes its catalog row invalid and flips it
    # only at the end. Probing and dropping without a lock lets a second replica
    # drop a first replica's live build.
    await pooled_store.setup()

    queries = _FakeDirectConnection.instances[0].queries
    assert "pg_try_advisory_lock" in queries[0], (
        f"index maintenance did not take the lock first: {queries[:2]}"
    )
    probe = next(i for i, q in enumerate(queries) if "indisvalid" in q)
    assert probe > 0, "the invalid-index probe ran outside the lock"
    assert any("pg_advisory_unlock" in q for q in queries), "lock never released"
    assert (
        queries.index(next(q for q in queries if "pg_advisory_unlock" in q)) > probe
    ), "lock released before the probe/create sequence finished"


async def test_index_maintenance_defers_to_the_lock_holder(
    monkeypatch: pytest.MonkeyPatch, pooled_store: ms.MetadataStore
) -> None:
    # A peer already holds the lock and is creating these exact indexes.
    # Touching the catalog anyway is what would kill its in-flight build.
    original = _FakeDirectConnection.__init__

    def _init(self: Any, conninfo: str, **kwargs: Any) -> None:
        original(self, conninfo, **kwargs)
        self.lock_acquired = False
        self.invalid_indexes = True

    monkeypatch.setattr(_FakeDirectConnection, "__init__", _init)

    await pooled_store.setup()

    queries = _FakeDirectConnection.instances[0].queries
    assert not any("DROP INDEX" in q for q in queries), (
        f"dropped an index while a peer held the build lock: {queries}"
    )
    assert not any(q.startswith("CREATE INDEX") for q in queries), queries
    assert _FakeDirectConnection.instances[0].closed == 1


async def test_concurrent_first_callers_open_exactly_one_pool(
    pooled_store: ms.MetadataStore,
) -> None:
    # Opening awaits, so the `_pool is None` check and the assignment are not
    # atomic: every concurrent first caller would build its own pool and all but
    # the last would be unreachable — and so never closed by aclose().
    tid = str(uuid4())
    await asyncio.gather(*(pooled_store.fetch_thread_row(tid) for _ in range(8)))

    assert len(_FakePool.instances) == 1, (
        f"{len(_FakePool.instances)} pools opened concurrently — all but one "
        "are unreachable and leak their connections"
    )

    await pooled_store.aclose()
    assert _FakePool.instances[0].closed == 1


async def test_failed_open_closes_the_pool_it_abandoned(
    monkeypatch: pytest.MonkeyPatch, pooled_store: ms.MetadataStore
) -> None:
    # open() can bring connections up before failing. That pool is about to go
    # out of scope, so if it is not closed here nothing can ever close it.
    # Fail only the first open. monkeypatch.undo() is not usable here: this
    # fixture shares its monkeypatch with pooled_store, so undoing would also
    # restore the real psycopg_pool.
    opens = {"n": 0}

    async def _boom_once(self: Any, wait: bool = False) -> None:
        opens["n"] += 1
        self.opened += 1
        if opens["n"] == 1:
            raise TimeoutError("pool did not come up in time")

    monkeypatch.setattr(_FakePool, "open", _boom_once)

    with pytest.raises(TimeoutError):
        await pooled_store.fetch_thread_row(str(uuid4()))

    assert _FakePool.instances[0].closed == 1, "abandoned pool was left open"

    # A failed open must not poison the store: a later call retries cleanly.
    await pooled_store.fetch_thread_row(str(uuid4()))
    assert len(_FakePool.instances) == 2
    assert _FakePool.instances[1].opened == 1
