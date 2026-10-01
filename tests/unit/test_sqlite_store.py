"""SQLite-specific metadata-store behaviour (connection pragmas).

Protocol behaviour — row shapes, conflicts, updates, search, run scoping —
lives in the shared contract (``tests/store_contract.py``), run against SQLite
by ``tests/unit/test_store_contract.py``.
"""

from pathlib import Path
from uuid import uuid4

from skeino.persistence import SqliteMetadataStore
from skeino.persistence.sqlite_store import _BUSY_TIMEOUT_MS


async def test_file_backed_store_enables_wal_and_busy_timeout(tmp_path: Path) -> None:
    store = SqliteMetadataStore(str(tmp_path / "meta.db"))
    await store.setup()
    try:
        cursor = await store._conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "wal"
        cursor = await store._conn.execute("PRAGMA busy_timeout")
        assert (await cursor.fetchone())[0] == _BUSY_TIMEOUT_MS
    finally:
        await store.aclose()


async def test_memory_store_setup_unaffected_by_wal_pragma() -> None:
    # journal_mode=WAL is a documented no-op on ":memory:" databases.
    store = SqliteMetadataStore(":memory:")
    await store.setup()
    try:
        cursor = await store._conn.execute("PRAGMA journal_mode")
        assert (await cursor.fetchone())[0] == "memory"
        tid = str(uuid4())
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        assert await store.fetch_thread_row(tid) is not None
    finally:
        await store.aclose()
