"""Behavioural contract every ``MetadataStoreProtocol`` implementation obeys.

``StoreContract`` holds the tests; a subclass supplies a ``store`` fixture
yielding a fresh, set-up, empty store. The in-process backends (InMemory,
SQLite, mongomock) run in the default suite (``tests/unit``); real Postgres
and Mongo run the same class under ``tests/api``. A behaviour one backend gets
wrong is a strict xfail scoped to that backend via ``KNOWN_GAPS``.

The contract is behaviour at the protocol surface — row values, ordering,
scoping, no-op and missing-row semantics — not driver-level value types
beyond what ``ThreadRow``/``RunRow`` declare. Behavioural comparisons go
through ``_norm`` (UTC, millisecond precision) so a timestamp-precision gap
fails only the dedicated timestamp tests, never masking an unrelated
regression; ``test_timestamps_round_trip_exactly`` and
``test_timestamps_are_timezone_aware`` pin exactness and offsets.
"""

from __future__ import annotations

import asyncio
import copy
from datetime import UTC, datetime
from typing import Any, ClassVar
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException

from skeino.persistence import MetadataStoreProtocol, RunRow, ThreadRow
from skeino.schemas import ThreadSearchRequest, ThreadTtlConfig

SORT_KEYS = ("thread_id", "status", "created_at", "updated_at", "state_updated_at")

# Short pause so successive writes get distinct timestamps on every backend.
TICK = 0.005
# Heartbeat window for the orphan-sweep tests: well above TICK and Mongo's
# millisecond timestamp precision, short enough to keep the suite fast.
STALE = 0.25


def _tid() -> str:
    return str(uuid4())


def _search(**kwargs: Any) -> ThreadSearchRequest:
    return ThreadSearchRequest(**{"limit": 100, "offset": 0, **kwargs})


def _ids(rows: list[ThreadRow] | list[RunRow], key: str = "thread_id") -> list[str]:
    return [str(row[key]) for row in rows]  # type: ignore[literal-required]


def _norm(value: Any) -> Any:
    """Deep copy with every datetime as UTC-aware, truncated to milliseconds.

    Used for behavioural comparisons only; the copy also guarantees a store
    that hands out live rows cannot make a before/after comparison pass by
    comparing a row to itself.
    """
    if isinstance(value, datetime):
        aware = value if value.utcoffset() is not None else value.replace(tzinfo=UTC)
        aware = aware.astimezone(UTC)
        return aware.replace(microsecond=aware.microsecond // 1000 * 1000)
    if isinstance(value, dict):
        return {key: _norm(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_norm(item) for item in value]
    return copy.deepcopy(value)


def _sort_key(value: Any) -> tuple[int, Any]:
    """Contract ordering: ``None`` sorts before every value (upstream precedent:
    ``x or datetime.min``), so null placement is identical on every backend."""
    return (0, "") if value is None else (1, str(_norm(value)))


MONGO_TIMESTAMP_GAPS = {
    name: "#135: Mongo returns naive, ms-truncated timestamps"
    for name in (
        "test_timestamps_are_timezone_aware",
        "test_timestamps_round_trip_exactly",
    )
}
POSTGRES_NULL_ORDER_GAPS = {
    f"test_search_sorts_by_every_key[state_updated_at-{order}]": (
        "#138: Postgres sorts NULL state_updated_at as the largest value"
    )
    for order in ("asc", "desc")
}
IN_MEMORY_SNAPSHOT_GAPS = {
    name: "#136: in-memory returns its live rows"
    for name in (
        "test_create_results_are_snapshots",
        "test_do_nothing_create_result_is_a_snapshot",
        "test_read_results_are_snapshots",
        "test_run_results_are_snapshots",
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
        return _norm(  # type: ignore[no-any-return]
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
        return _norm(  # type: ignore[no-any-return]
            await store.create_run(
                kwargs.pop("run_id", _tid()),
                thread_id,
                kwargs.pop("assistant_id", "agent"),
                kwargs.pop("metadata", {}),
                kwargs.pop("kwargs", {}),
                kwargs.pop("multitask_strategy", "enqueue"),
            )
        )

    async def _get(self, store: MetadataStoreProtocol, thread_id: str) -> Any:
        return _norm(await store.fetch_thread_row(thread_id))

    async def _get_run(
        self, store: MetadataStoreProtocol, thread_id: str, run_id: str
    ) -> Any:
        return _norm(await store.fetch_run_row(thread_id, run_id))

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
        assert await self._get(store, tid) == row

    async def _typed_rows(
        self, store: MetadataStoreProtocol
    ) -> tuple[ThreadRow, RunRow, RunRow]:
        """A thread with every timestamp set, plus a run as created and as read."""
        tid, rid = _tid(), _tid()
        await self._thread(store, tid)
        created = await store.create_run(rid, tid, "agent", {}, {}, "enqueue")
        await store.update_thread(tid, mark_state_updated=True)
        thread = await store.fetch_thread_row(tid)
        run = await store.fetch_run_row(tid, rid)
        assert thread is not None and run is not None
        return thread, created, run

    async def test_row_ids_are_uuids(self, store: MetadataStoreProtocol) -> None:
        thread, created, run = await self._typed_rows(store)
        assert isinstance(thread["thread_id"], UUID)
        for row in (created, run):
            assert isinstance(row["run_id"], UUID)
            assert isinstance(row["thread_id"], UUID)

    async def test_timestamps_are_timezone_aware(
        self, store: MetadataStoreProtocol
    ) -> None:
        thread, created, run = await self._typed_rows(store)
        for stamp in (
            thread["created_at"],
            thread["updated_at"],
            thread["state_updated_at"],
            created["created_at"],
            created["updated_at"],
            run["created_at"],
            run["updated_at"],
        ):
            assert isinstance(stamp, datetime)
            assert stamp.utcoffset() is not None  # timezone-aware

    async def test_timestamps_round_trip_exactly(
        self, store: MetadataStoreProtocol
    ) -> None:
        # What a create returns is what every later read returns, to the
        # microsecond — callers echo the create response straight to clients.
        tid, rid = _tid(), _tid()
        thread = await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        run = await store.create_run(rid, tid, "agent", {}, {}, "enqueue")
        stored_thread = await store.fetch_thread_row(tid)
        stored_run = await store.fetch_run_row(tid, rid)
        assert stored_thread is not None and stored_run is not None
        for key in ("created_at", "updated_at"):
            assert stored_thread[key] == thread[key]
            assert stored_run[key] == run[key]

    async def test_create_thread_with_ttl_records_expiry(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        row = await self._thread(
            store, tid, ttl=ThreadTtlConfig(strategy="delete", ttl=30)
        )
        stored = await self._get(store, tid)
        assert stored is not None
        # What was persisted, not just what the create echoed back.
        assert stored["ttl"] == row["ttl"]
        ttl = stored["ttl"]
        assert ttl is not None
        assert ttl["strategy"] == "delete"
        assert ttl["ttl_minutes"] == 30
        expires = datetime.fromisoformat(str(ttl["expires_at"]))
        assert (expires - stored["created_at"]).total_seconds() == pytest.approx(
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
        fetched = await self._get(store, tid)
        assert fetched is not None and fetched["metadata"] == {"v": 1}

    async def test_fetch_missing_thread_is_none(
        self, store: MetadataStoreProtocol
    ) -> None:
        assert await self._get(store, _tid()) is None

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
        row = await self._get(store, tid)
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
        row = await self._get(store, tid)
        assert row is not None
        assert (row["metadata"], row["config"]) == ({"a": 1}, {"c": 1})

    async def test_mark_state_updated_stamps_state_updated_at(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        await store.update_thread(tid, mark_state_updated=True)
        row = await self._get(store, tid)
        assert row is not None
        assert row["state_updated_at"] is not None
        assert row["state_updated_at"] >= row["created_at"]

    async def test_empty_update_changes_nothing(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        before = await self._get(store, tid)
        await asyncio.sleep(TICK)
        await store.update_thread(tid)
        assert await self._get(store, tid) == before

    async def test_update_missing_thread_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await store.update_thread(tid, status_value="busy", metadata={"a": 1})
        assert await self._get(store, tid) is None

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
        keys = [_sort_key(row[sort_by]) for row in rows]  # type: ignore[literal-required]
        assert sum(key[0] for key in keys) >= 3  # enough real values to order
        assert keys == sorted(keys, reverse=sort_order == "desc")

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
        assert await self._get(store, doomed) is None
        assert await self._get_run(store, doomed, str(doomed_run["run_id"])) is None
        assert await self._get(store, kept) is not None
        assert await self._get_run(store, kept, str(kept_run["run_id"])) == kept_run

    async def test_delete_missing_thread_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid, rid = _tid(), _tid()
        await self._thread(store, tid, metadata={"keep": True})
        await self._run(store, tid, run_id=rid)
        thread_before = await self._get(store, tid)
        run_before = await self._get_run(store, tid, rid)
        await store.delete_thread(_tid())
        # Unrelated rows are untouched, not merely "no exception raised".
        assert await self._get(store, tid) == thread_before
        assert await self._get_run(store, tid, rid) == run_before

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
        assert await self._get_run(store, tid, rid) == run

    async def test_update_run_status_sets_and_clears_error(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        run = await self._run(store, tid)
        rid = str(run["run_id"])
        await asyncio.sleep(TICK)
        await store.update_run_status(rid, "running", error="boom")
        flagged = await self._get_run(store, tid, rid)
        assert flagged is not None
        assert (flagged["status"], flagged["error"]) == ("running", "boom")
        assert flagged["updated_at"] > run["updated_at"]
        assert flagged["created_at"] == run["created_at"]
        await store.update_run_status(rid, "success")
        finished = await self._get_run(store, tid, rid)
        assert finished is not None
        assert (finished["status"], finished["error"]) == ("success", None)

    @pytest.mark.parametrize("terminal", ["success", "error", "interrupted"])
    async def test_terminal_run_status_is_final(
        self, store: MetadataStoreProtocol, terminal: str
    ) -> None:
        # Another worker's orphan sweep can fail a run whose owner is merely
        # late; the owner's later write must not flip it back.
        tid = _tid()
        await self._thread(store, tid)
        rid = str((await self._run(store, tid))["run_id"])
        assert await store.update_run_status(rid, "running") is True
        assert await store.update_run_status(rid, terminal, error="first") is True
        before = await self._get_run(store, tid, rid)
        await asyncio.sleep(TICK)
        # The late owner is told its write lost, so it can report the winner.
        for later in ("running", "success", "interrupted", "error"):
            assert await store.update_run_status(rid, later, error="late") is False
        assert await self._get_run(store, tid, rid) == before

    async def test_update_missing_run_is_a_silent_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid, rid = _tid(), _tid()
        await self._thread(store, tid)
        assert await store.update_run_status(rid, "success") is False
        # No upsert: the id stays unknown and a later create starts it fresh.
        assert await store.fetch_run_row(tid, rid) is None
        assert (
            await store.list_run_rows(tid, limit=10, offset=0, status_value=None) == []
        )
        run = await self._run(store, tid, run_id=rid)
        assert (run["status"], run["error"]) == ("pending", None)

    # --- run liveness (heartbeat + orphan sweep) -------------------------------

    async def test_fail_stale_runs_claims_only_unheartbeated_in_flight_runs(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        ids = {
            name: str((await self._run(store, tid))["run_id"])
            for name in ("pending", "running", "fresh", "excluded", "done")
        }
        await store.update_run_status(ids["running"], "running")
        await store.update_run_status(ids["done"], "success")
        await asyncio.sleep(STALE + TICK)
        await store.touch_runs([ids["fresh"]])  # heartbeat keeps it alive

        failed = await store.fail_stale_runs(
            stale_after_seconds=STALE,
            exclude_run_ids=[ids["excluded"]],
            error="orphaned",
        )
        assert sorted(_ids(failed, "run_id")) == sorted(
            [ids["pending"], ids["running"]]
        )
        assert all((r["status"], r["error"]) == ("error", "orphaned") for r in failed)
        # The sweep releases each claimed run's thread, so rows must name it.
        assert all(str(r["thread_id"]) == str(tid) for r in failed)
        for name in ("pending", "running"):
            row = await self._get_run(store, tid, ids[name])
            assert (row["status"], row["error"]) == ("error", "orphaned")
        for name, expected in (
            ("fresh", "pending"),
            ("excluded", "pending"),
            ("done", "success"),
        ):
            assert (await self._get_run(store, tid, ids[name]))["status"] == expected

        # Claimed once: a second sweep (another worker) reports nothing.
        again = await store.fail_stale_runs(
            stale_after_seconds=STALE, exclude_run_ids=[], error="orphaned"
        )
        assert _ids(again, "run_id") == [ids["excluded"]]

    async def test_release_busy_thread_only_when_nothing_is_in_flight(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        await store.update_thread(tid, status_value="busy")
        run_id = str((await self._run(store, tid))["run_id"])

        # A pending (and then a running) run keeps the thread busy.
        assert await store.release_busy_thread(tid, "error") is False
        await store.update_run_status(run_id, "running")
        assert await store.release_busy_thread(tid, "error") is False
        assert (await store.fetch_thread_row(tid))["status"] == "busy"

        await store.update_run_status(run_id, "success")
        assert await store.release_busy_thread(tid, "idle") is True
        row = await store.fetch_thread_row(tid)
        assert row["status"] == "idle"
        assert row["state_updated_at"] is None

        # Only a busy thread is released: a settled one keeps its status.
        assert await store.release_busy_thread(tid, "error") is False
        assert (await store.fetch_thread_row(tid))["status"] == "idle"

    async def test_release_busy_thread_can_stamp_the_state_update(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        await store.update_thread(tid, status_value="busy")
        assert await store.release_busy_thread(
            tid, "interrupted", mark_state_updated=True
        )
        row = await store.fetch_thread_row(tid)
        assert row["status"] == "interrupted"
        assert row["state_updated_at"] is not None

    async def test_release_busy_thread_ignores_other_threads_runs(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid, other = _tid(), _tid()
        for thread_id in (tid, other):
            await self._thread(store, thread_id)
            await store.update_thread(thread_id, status_value="busy")
        await self._run(store, other)  # in flight, but on another thread
        assert await store.release_busy_thread(tid, "error") is True
        assert await store.release_busy_thread(other, "error") is False

    async def test_release_missing_thread_is_a_no_op(
        self, store: MetadataStoreProtocol
    ) -> None:
        assert await store.release_busy_thread(_tid(), "error") is False

    async def test_touch_runs_leaves_terminal_and_unknown_runs_alone(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        rid = str((await self._run(store, tid))["run_id"])
        await store.update_run_status(rid, "success")
        before = await self._get_run(store, tid, rid)
        await asyncio.sleep(TICK)
        await store.touch_runs([rid, _tid()])
        await store.touch_runs([])
        assert await self._get_run(store, tid, rid) == before

    async def test_touch_runs_bumps_in_flight_updated_at(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await self._thread(store, tid)
        run = await self._run(store, tid)
        await asyncio.sleep(TICK)
        await store.touch_runs([str(run["run_id"])])
        touched = await self._get_run(store, tid, str(run["run_id"]))
        assert touched["updated_at"] > run["updated_at"]
        assert touched["status"] == "pending"

    async def test_run_lookups_are_scoped_to_their_thread(
        self, store: MetadataStoreProtocol
    ) -> None:
        owner, other = _tid(), _tid()
        await self._thread(store, owner)
        await self._thread(store, other)
        rid = str((await self._run(store, owner))["run_id"])
        assert await self._get_run(store, other, rid) is None
        assert (
            await store.list_run_rows(other, limit=10, offset=0, status_value=None)
            == []
        )
        await store.delete_run(other, rid)  # wrong thread: must not delete
        assert await self._get_run(store, owner, rid) is not None
        await store.delete_run(owner, rid)
        assert await self._get_run(store, owner, rid) is None

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

    async def test_create_results_are_snapshots(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        thread = await store.create_thread(
            tid, metadata={"a": 1}, config={}, ttl=None, if_exists="raise"
        )
        rid = _tid()
        run = await store.create_run(rid, tid, "agent", {"m": 1}, {}, "enqueue")
        # Mutating what a create returned must not write into the store...
        thread["metadata"]["leak"] = True
        run["metadata"]["leak"] = True
        stored_thread = await store.fetch_thread_row(tid)
        stored_run = await store.fetch_run_row(tid, rid)
        assert stored_thread is not None and stored_thread["metadata"] == {"a": 1}
        assert stored_run is not None and stored_run["metadata"] == {"m": 1}
        del thread["metadata"]["leak"], run["metadata"]["leak"]
        await store.update_thread(tid, status_value="busy", metadata={"b": 2})
        await store.update_run_status(rid, "success")
        # Later writes must not rewrite results the caller already holds.
        assert (thread["status"], thread["metadata"]) == ("idle", {"a": 1})
        assert run["status"] == "pending"

    async def test_do_nothing_create_result_is_a_snapshot(
        self, store: MetadataStoreProtocol
    ) -> None:
        # ``if_exists="do_nothing"`` returns the existing row on its own
        # branch, so it needs isolating separately from a fresh insert.
        tid = _tid()
        await store.create_thread(
            tid, metadata={"a": 1}, config={}, ttl=None, if_exists="raise"
        )
        existing = await store.create_thread(
            tid, metadata={"x": 2}, config={}, ttl=None, if_exists="do_nothing"
        )
        existing["metadata"]["leak"] = True
        stored = await store.fetch_thread_row(tid)
        assert stored is not None and stored["metadata"] == {"a": 1}
        del existing["metadata"]["leak"]
        await store.update_thread(tid, status_value="busy", metadata={"b": 2})
        assert (existing["status"], existing["metadata"]) == ("idle", {"a": 1})

    async def test_read_results_are_snapshots(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid = _tid()
        await store.create_thread(
            tid, metadata={"a": 1}, config={}, ttl=None, if_exists="raise"
        )
        fetched = await store.fetch_thread_row(tid)
        (searched,) = await store.search_thread_rows(_search())
        assert fetched is not None
        # Mutating what a read returned must not write into the store.
        fetched["metadata"]["leak"] = True
        searched["metadata"]["leak"] = True
        await store.update_thread(tid, status_value="busy")
        assert fetched["status"] == "idle"
        again = await store.fetch_thread_row(tid)
        assert again is not None and again["metadata"] == {"a": 1}

    async def test_run_results_are_snapshots(
        self, store: MetadataStoreProtocol
    ) -> None:
        tid, rid = _tid(), _tid()
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
        await store.create_run(rid, tid, "agent", {"m": 1}, {}, "enqueue")
        fetched = await store.fetch_run_row(tid, rid)
        (listed,) = await store.list_run_rows(
            tid, limit=10, offset=0, status_value=None
        )
        assert fetched is not None
        fetched["metadata"]["leak"] = True
        listed["metadata"]["leak"] = True
        await store.update_run_status(rid, "success")
        assert fetched["status"] == "pending"
        again = await store.fetch_run_row(tid, rid)
        assert again is not None and again["metadata"] == {"m": 1}
