"""Behavioural contract every ``MetadataStoreProtocol`` implementation obeys.

``StoreContract`` holds the tests; a subclass supplies a ``store`` fixture
yielding a fresh, set-up, empty store. The in-process backends (InMemory,
SQLite, mongomock) run in the default suite (``tests/unit``); real Postgres
and Mongo run the same class under ``tests/api``. A behaviour one backend gets
wrong is a strict xfail scoped to that backend via ``KNOWN_GAPS``.

The contract is behaviour at the protocol surface — row values, ordering,
scoping, no-op and missing-row semantics — not driver-level value types
beyond what ``ThreadRow``/``RunRow`` declare.
"""

from __future__ import annotations

import asyncio
import copy
from datetime import datetime
from typing import Any, ClassVar
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from skeino.persistence import MetadataStoreProtocol, RunRow, ThreadRow
from skeino.schemas import ThreadSearchRequest, ThreadTtlConfig

SORT_KEYS = ("thread_id", "status", "created_at", "updated_at", "state_updated_at")

# Short pause so successive writes get distinct timestamps on every backend.
TICK = 0.005


def _tid() -> str:
    return str(uuid4())


def _search(**kwargs: Any) -> ThreadSearchRequest:
    return ThreadSearchRequest(**{"limit": 100, "offset": 0, **kwargs})


def _ids(rows: list[ThreadRow] | list[RunRow], key: str = "thread_id") -> list[str]:
    return [str(row[key]) for row in rows]  # type: ignore[literal-required]


MONGO_TIMESTAMP_GAPS = {
    name: "#135: Mongo returns naive, ms-truncated timestamps"
    for name in (
        "test_create_thread_returns_the_stored_row",
        "test_row_types_match_the_declared_contract",
        "test_create_existing_thread_do_nothing_returns_original",
        "test_update_thread_sets_fields_and_bumps_updated_at",
        "test_delete_thread_removes_it_and_only_its_runs",
        "test_create_run_returns_a_pending_row",
        "test_update_run_status_sets_and_clears_error",
    )
}


class StoreContract:
    """Mixin of contract tests; subclasses provide the ``store`` fixture."""

    #: ``{test name or parametrized id: reason}`` — strict xfails for this
    #: backend's known gaps, e.g. ``"test_x"`` or ``"test_x[created_at-asc]"``.
    KNOWN_GAPS: ClassVar[dict[str, str]] = {}

    @pytest.fixture(autouse=True)
    def _known_gap(self, request: pytest.FixtureRequest) -> None:
        for key in (request.node.name, request.node.originalname):
            if key in self.KNOWN_GAPS:
                request.applymarker(
                    pytest.mark.xfail(strict=True, reason=self.KNOWN_GAPS[key])
                )
                return

    async def _thread(
        self, store: MetadataStoreProtocol, thread_id: str | None = None, **kwargs: Any
    ) -> ThreadRow:
        # Deep copies throughout: a store that hands out its live rows must
        # not make a before/after comparison pass by comparing a row to itself
        # (``test_returned_rows_are_snapshots`` pins that behaviour directly).
        return copy.deepcopy(
            await store.create_thread(
                thread_id or _tid(),
                metadata=kwargs.pop("metadata", {}),
                config=kwargs.pop("config", {}),
                ttl=kwargs.pop("ttl", None),
                if_exists=kwargs.pop("if_exists", "raise"),
            )
        )

    async def _run(
        self, store: MetadataStoreProtocol, thread_id: str, **kwargs: Any
    ) -> RunRow:
        return copy.deepcopy(
            await store.create_run(
                kwargs.pop("run_id", _tid()),
                thread_id,
                kwargs.pop("assistant_id", "agent"),
                kwargs.pop("metadata", {}),
                kwargs.pop("kwargs", {}),
                kwargs.pop("multitask_strategy", "enqueue"),
            )
        )

    # --- create / fetch thread ---------------------------------------------

    async def test_create_thread_returns_the_stored_row(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        row = await self._thread(store, tid, metadata={"a": 1}, config={"c": "x"})
        assert row["thread_id"] == UUID(tid)
        assert row["metadata"] == {"a": 1}
        assert row["config"] == {"c": "x"}
        assert row["status"] == "idle"
        assert row["state_updated_at"] is None
        assert row["ttl"] is None
        assert row["created_at"] == row["updated_at"]
        assert await store.fetch_thread_row(tid) == row

    async def test_row_types_match_the_declared_contract(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        run = await self._run(store, tid)
        thread = await store.fetch_thread_row(tid)
        assert thread is not None
        assert isinstance(thread["thread_id"], UUID)
        assert isinstance(run["run_id"], UUID)
        assert isinstance(run["thread_id"], UUID)
        for stamp in (
            thread["created_at"],
            thread["updated_at"],
            run["created_at"],
            run["updated_at"],
        ):
            assert isinstance(stamp, datetime)
            assert stamp.utcoffset() is not None  # timezone-aware

    async def test_create_thread_with_ttl_records_expiry(
        self, store: MetadataStoreProtocol
    ) -> None:
        row = await self._thread(store, ttl=ThreadTtlConfig(strategy="delete", ttl=30))
        ttl = row["ttl"]
        assert ttl is not None
        assert ttl["strategy"] == "delete"
        assert ttl["ttl_minutes"] == 30
        expires = datetime.fromisoformat(str(ttl["expires_at"]))
        assert (expires - row["created_at"]).total_seconds() == pytest.approx(
            1800, abs=1
        )

    async def test_create_existing_thread_raises_409(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        with pytest.raises(HTTPException) as excinfo:
            await self._thread(store, tid)
        assert excinfo.value.status_code == 409

    async def test_create_existing_thread_do_nothing_returns_original(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        original = await self._thread(store, tid, metadata={"v": 1})
        again = await self._thread(
            store, tid, metadata={"v": 2}, if_exists="do_nothing"
        )
        assert again == original
        fetched = await store.fetch_thread_row(tid)
        assert fetched is not None and fetched["metadata"] == {"v": 1}

    async def test_fetch_missing_thread_is_none(
        self, store: MetadataStoreProtocol
    ) -> None:
        assert await store.fetch_thread_row(_tid()) is None

    # --- update thread -----------------------------------------------------

    async def test_update_thread_sets_fields_and_bumps_updated_at(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        created = await self._thread(store, tid, metadata={"a": 1}, config={"c": 1})
        await asyncio.sleep(TICK)
        await store.update_thread(
            tid, status_value="busy", config={"c": 2}, metadata={"b": 2}
        )
        row = await store.fetch_thread_row(tid)
        assert row is not None
        assert row["status"] == "busy"
        assert row["config"] == {"c": 2}
        assert row["metadata"] == {"b": 2}  # replaced, not merged, at this layer
        assert row["created_at"] == created["created_at"]
        assert row["updated_at"] > created["updated_at"]
        assert row["state_updated_at"] is None

    async def test_update_thread_leaves_unset_fields_alone(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid, metadata={"a": 1}, config={"c": 1})
        await store.update_thread(tid, status_value="busy")
        row = await store.fetch_thread_row(tid)
        assert row is not None
        assert (row["metadata"], row["config"]) == ({"a": 1}, {"c": 1})

    async def test_mark_state_updated_stamps_state_updated_at(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        await store.update_thread(tid, mark_state_updated=True)
        row = await store.fetch_thread_row(tid)
        assert row is not None
        assert row["state_updated_at"] is not None
        assert row["state_updated_at"] >= row["created_at"]

    async def test_empty_update_changes_nothing(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        before = copy.deepcopy(await store.fetch_thread_row(tid))
        await asyncio.sleep(TICK)
        await store.update_thread(tid)
        assert await store.fetch_thread_row(tid) == before

    async def test_update_missing_thread_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await store.update_thread(tid, status_value="busy", metadata={"a": 1})
        assert await store.fetch_thread_row(tid) is None

    # --- search ------------------------------------------------------------

    async def test_search_defaults_to_updated_at_desc(
        self, store: MetadataStoreProtocol
    ) -> None:
        first, second, third = [await self._thread(store) for _ in range(3)]
        for row in (first, second, third):
            await asyncio.sleep(TICK)
            await store.update_thread(str(row["thread_id"]), status_value="idle")
        await asyncio.sleep(TICK)
        await store.update_thread(str(first["thread_id"]), status_value="idle")
        found = _ids(await store.search_thread_rows(_search()))
        assert found == _ids([first, third, second])

    async def test_search_filters_by_ids_and_status(
        self, store: MetadataStoreProtocol
    ) -> None:
        a, b, c = [str((await self._thread(store))["thread_id"]) for _ in range(3)]
        await store.update_thread(b, status_value="busy")
        by_ids = await store.search_thread_rows(_search(ids=[a, b]))
        assert sorted(_ids(by_ids)) == sorted([a, b])
        assert _ids(await store.search_thread_rows(_search(status="busy"))) == [b]
        both = await store.search_thread_rows(_search(ids=[a, c], status="busy"))
        assert both == []

    async def test_search_pages_partition_the_results(
        self, store: MetadataStoreProtocol
    ) -> None:
        for _ in range(5):
            await self._thread(store)
            await asyncio.sleep(TICK)
        everything = _ids(await store.search_thread_rows(_search()))
        pages = [
            _ids(await store.search_thread_rows(_search(limit=2, offset=offset)))
            for offset in (0, 2, 4, 6)
        ]
        assert [len(p) for p in pages] == [2, 2, 1, 0]
        assert sum(pages, []) == everything

    @pytest.mark.parametrize("sort_order", ["asc", "desc"])
    @pytest.mark.parametrize("sort_by", SORT_KEYS)
    async def test_search_sorts_by_every_key(
        self, store: MetadataStoreProtocol, sort_by: str, sort_order: str
    ) -> None:
        # Same arrangement as the HTTP threads suite: the default order
        # (updated_at desc) is monotonic in no other key, in either direction.
        ids = [f"00000000-0000-0000-0000-00000000000{n}" for n in "31524"]
        for tid in ids:
            await self._thread(store, tid)
            await asyncio.sleep(TICK)
        t1, t2, t3, t4, _ = ids
        for tid in (t2, t3, t4):
            await store.update_thread(tid, status_value="busy", mark_state_updated=True)
            await asyncio.sleep(TICK)
        for tid in (t2, t1):
            await store.update_thread(tid, metadata={"bump": True})
            await asyncio.sleep(TICK)
        rows = await store.search_thread_rows(
            _search(sort_by=sort_by, sort_order=sort_order)
        )
        assert len(rows) == 5
        values = [
            str(row[sort_by])  # type: ignore[literal-required]
            for row in rows
            if row[sort_by] is not None  # type: ignore[literal-required]
        ]
        assert len(values) >= 3
        assert values == sorted(values, reverse=sort_order == "desc")

    # --- delete thread -----------------------------------------------------

    async def test_delete_thread_removes_it_and_only_its_runs(
        self, store: MetadataStoreProtocol
    ) -> None:
        doomed, kept = _tid(), _tid()
        await self._thread(store, doomed)
        await self._thread(store, kept)
        doomed_run = await self._run(store, doomed)
        kept_run = await self._run(store, kept)
        await store.delete_thread(doomed)
        assert await store.fetch_thread_row(doomed) is None
        assert await store.fetch_run_row(doomed, str(doomed_run["run_id"])) is None
        assert await store.fetch_thread_row(kept) is not None
        assert await store.fetch_run_row(kept, str(kept_run["run_id"])) == kept_run

    async def test_delete_missing_thread_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        await store.delete_thread(_tid())

    # --- runs --------------------------------------------------------------

    async def test_create_run_returns_a_pending_row(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid, rid = _tid(), _tid()
        await self._thread(store, tid)
        run = await self._run(
            store,
            tid,
            run_id=rid,
            assistant_id="agent",
            metadata={"m": 1},
            kwargs={"input": {"x": [1, 2]}},
            multitask_strategy="reject",
        )
        assert run["run_id"] == UUID(rid)
        assert run["thread_id"] == UUID(tid)
        assert run["assistant_id"] == "agent"
        assert run["status"] == "pending"
        assert run["metadata"] == {"m": 1}
        assert run["kwargs"] == {"input": {"x": [1, 2]}}
        assert run["multitask_strategy"] == "reject"
        assert run["error"] is None
        assert run["created_at"] == run["updated_at"]
        assert await store.fetch_run_row(tid, rid) == run

    async def test_update_run_status_sets_and_clears_error(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        run = await self._run(store, tid)
        rid = str(run["run_id"])
        await asyncio.sleep(TICK)
        await store.update_run_status(rid, "error", error="boom")
        failed = await store.fetch_run_row(tid, rid)
        assert failed is not None
        assert (failed["status"], failed["error"]) == ("error", "boom")
        assert failed["updated_at"] > run["updated_at"]
        assert failed["created_at"] == run["created_at"]
        await store.update_run_status(rid, "success")
        retried = await store.fetch_run_row(tid, rid)
        assert retried is not None
        assert (retried["status"], retried["error"]) == ("success", None)

    async def test_update_missing_run_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        await store.update_run_status(_tid(), "success")

    async def test_run_lookups_are_scoped_to_their_thread(
        self, store: MetadataStoreProtocol
    ) -> None:
        owner, other = _tid(), _tid()
        await self._thread(store, owner)
        await self._thread(store, other)
        rid = str((await self._run(store, owner))["run_id"])
        assert await store.fetch_run_row(other, rid) is None
        assert (
            await store.list_run_rows(other, limit=10, offset=0, status_value=None)
            == []
        )
        await store.delete_run(other, rid)  # wrong thread: must not delete
        assert await store.fetch_run_row(owner, rid) is not None
        await store.delete_run(owner, rid)
        assert await store.fetch_run_row(owner, rid) is None

    async def test_list_runs_newest_first_with_paging_and_status(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        created = []
        for _ in range(4):
            created.append(str((await self._run(store, tid))["run_id"]))
            await asyncio.sleep(TICK)
        await store.update_run_status(created[1], "success")
        newest_first = list(reversed(created))

        def listed(**kwargs: Any) -> Any:
            return store.list_run_rows(
                tid,
                limit=kwargs.get("limit", 10),
                offset=kwargs.get("offset", 0),
                status_value=kwargs.get("status_value"),
            )

        assert _ids(await listed(), "run_id") == newest_first
        pages = [
            _ids(await listed(limit=3, offset=offset), "run_id") for offset in (0, 3)
        ]
        assert pages == [newest_first[:3], newest_first[3:]]
        assert _ids(await listed(status_value="success"), "run_id") == [created[1]]
        pending = _ids(await listed(status_value="pending"), "run_id")
        assert pending == [r for r in newest_first if r != created[1]]

    # --- isolation ---------------------------------------------------------

    async def test_returned_rows_are_snapshots(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        created = await self._thread(store, tid, metadata={"a": 1})
        fetched = await store.fetch_thread_row(tid)
        assert fetched is not None
        fetched["metadata"]["leak"] = True  # caller mutates what it was given
        await store.update_thread(tid, status_value="busy")
        assert created["status"] == "idle"  # earlier result not rewritten
        again = await store.fetch_thread_row(tid)
        assert again is not None and again["metadata"] == {"a": 1}
