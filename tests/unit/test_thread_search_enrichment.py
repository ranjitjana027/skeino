"""Cost characteristics of ``ThreadOps.search``.

Enriching a page of threads is the most expensive read in the API: one graph
state read per row, on top of the metadata query that produced the page. Two
properties keep that bounded, and both are invisible in a response body — a
regression here shows up only as latency, so it is pinned here instead.
"""

import asyncio
from types import SimpleNamespace
from typing import Any
from uuid import uuid4

import pytest

from skeino.ops.threads import ThreadOps
from skeino.persistence import InMemoryMetadataStore
from skeino.schemas import ThreadSearchRequest


class _CountingStore(InMemoryMetadataStore):
    """In-memory store that records how often each row is read back."""

    def __init__(self) -> None:
        super().__init__()
        self.fetch_calls: list[str] = []

    async def fetch_thread_row(self, thread_id: str) -> Any:
        self.fetch_calls.append(thread_id)
        return await super().fetch_thread_row(thread_id)


class _StateGraph:
    """Graph whose ``aget_state`` is slow, and which tracks its own concurrency."""

    def __init__(self, *, delay: float = 0.0) -> None:
        self._delay = delay
        self.in_flight = 0
        self.peak_in_flight = 0
        self.state_by_thread: dict[str, dict[str, Any]] = {}

    async def aget_state(
        self, config: dict[str, Any], *, subgraphs: bool = False
    ) -> SimpleNamespace:
        del subgraphs
        thread_id = str(config["configurable"]["thread_id"])
        self.in_flight += 1
        self.peak_in_flight = max(self.peak_in_flight, self.in_flight)
        try:
            if self._delay:
                await asyncio.sleep(self._delay)
            return SimpleNamespace(
                values=self.state_by_thread.get(thread_id, {}),
                next=(),
                config={"configurable": {"thread_id": thread_id}},
                metadata={},
                created_at=None,
                parent_config=None,
                tasks=(),
                interrupts=(),
            )
        finally:
            self.in_flight -= 1


async def _seed(store: InMemoryMetadataStore, count: int) -> list[str]:
    ids = [str(uuid4()) for _ in range(count)]
    for tid in ids:
        await store.create_thread(
            tid, metadata={}, config={}, ttl=None, if_exists="raise"
        )
    return ids


async def test_search_does_not_re_read_rows_it_already_has() -> None:
    # Each row returned by search_thread_rows is its own existence proof. Going
    # back to the store per row doubles the metadata round trips for a listing —
    # the N+1 this asserts against.
    store = _CountingStore()
    ids = await _seed(store, 5)
    ops = ThreadOps(graph=_StateGraph(), metadata_store=store)

    results = await ops.search(ThreadSearchRequest(limit=10, offset=0))

    assert {str(r.thread_id) for r in results} == set(ids)
    assert store.fetch_calls == [], (
        "search re-read thread rows it was already handed: "
        f"{len(store.fetch_calls)} extra metadata reads for {len(ids)} threads"
    )


async def test_search_enriches_rows_concurrently() -> None:
    # Serial enrichment makes a page cost limit x (one round trip). With 12 rows
    # and a per-read delay, a serial implementation peaks at 1 in flight.
    store = InMemoryMetadataStore()
    await _seed(store, 12)
    graph = _StateGraph(delay=0.01)
    ops = ThreadOps(graph=graph, metadata_store=store)

    await ops.search(ThreadSearchRequest(limit=20, offset=0))

    assert graph.peak_in_flight > 1, (
        f"state reads ran one at a time (peak in flight {graph.peak_in_flight})"
    )


async def test_search_bounds_its_concurrency() -> None:
    # Unbounded gather over a large page would open one checkpoint read per row
    # at once and exhaust the checkpointer's connection pool, starving live runs.
    store = InMemoryMetadataStore()
    await _seed(store, 40)
    graph = _StateGraph(delay=0.01)
    ops = ThreadOps(graph=graph, metadata_store=store)

    await ops.search(ThreadSearchRequest(limit=50, offset=0))

    assert graph.peak_in_flight <= 8, (
        f"state reads ran {graph.peak_in_flight} deep — expected a bounded burst"
    )


async def test_search_preserves_store_ordering() -> None:
    # Concurrent enrichment must not reorder the page: the store decides sort
    # order, and clients paginate on it.
    store = InMemoryMetadataStore()
    await _seed(store, 6)
    graph = _StateGraph(delay=0.005)
    ops = ThreadOps(graph=graph, metadata_store=store)

    rows = await store.search_thread_rows(ThreadSearchRequest(limit=10, offset=0))
    results = await ops.search(ThreadSearchRequest(limit=10, offset=0))

    assert [str(r.thread_id) for r in results] == [str(r["thread_id"]) for r in rows]


async def test_concurrent_searches_share_one_bound() -> None:
    # The bound exists to keep enrichment inside the checkpointer's connection
    # pool. A semaphore built per call does not do that: three simultaneous
    # searches would each get their own allowance and run 3 x the bound deep,
    # saturating the pool and starving live runs. The bound must be aggregate.
    store = InMemoryMetadataStore()
    await _seed(store, 12)
    graph = _StateGraph(delay=0.01)
    ops = ThreadOps(graph=graph, metadata_store=store, search_enrich_concurrency=4)

    await asyncio.gather(
        *(ops.search(ThreadSearchRequest(limit=20, offset=0)) for _ in range(3))
    )

    assert graph.peak_in_flight <= 4, (
        f"three concurrent searches peaked at {graph.peak_in_flight} state "
        "reads against a bound of 4 — the semaphore is not shared across calls"
    )


async def test_concurrent_searches_still_run_concurrently() -> None:
    # Guard the fix against being "passed" by serialising everything: sharing
    # one semaphore must not collapse enrichment back to one read at a time.
    store = InMemoryMetadataStore()
    await _seed(store, 12)
    graph = _StateGraph(delay=0.01)
    ops = ThreadOps(graph=graph, metadata_store=store, search_enrich_concurrency=4)

    await asyncio.gather(
        *(ops.search(ThreadSearchRequest(limit=20, offset=0)) for _ in range(3))
    )

    assert graph.peak_in_flight > 1, (
        f"state reads ran one at a time (peak in flight {graph.peak_in_flight})"
    )


async def test_zero_enrichment_bound_is_rejected() -> None:
    # asyncio.Semaphore rejects negatives but accepts 0, and Semaphore(0) is
    # never acquirable — every search would wait on it forever with no error
    # raised and nothing logged. A silent hang is the worst failure mode here,
    # so construction must reject it.
    store = InMemoryMetadataStore()

    with pytest.raises(ValueError, match="at least 1"):
        ThreadOps(
            graph=_StateGraph(), metadata_store=store, search_enrich_concurrency=0
        )


async def test_a_bound_of_one_is_still_allowed() -> None:
    # 1 is degenerate but legitimate (fully serial enrichment); only values
    # below 1 are unusable, so the guard must not over-reject.
    store = InMemoryMetadataStore()
    await _seed(store, 3)
    graph = _StateGraph()
    ops = ThreadOps(graph=graph, metadata_store=store, search_enrich_concurrency=1)

    results = await ops.search(ThreadSearchRequest(limit=10, offset=0))

    assert len(results) == 3
    assert graph.peak_in_flight == 1
