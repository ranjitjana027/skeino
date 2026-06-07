"""Unit tests for ThreadOps.copy with non-dict (list) graph state.

The shared ``FakeGraph`` coerces state to a dict, so a small list-state stub is
used here to prove that a thread whose state is a list is copied faithfully
rather than coming out empty.
"""

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from skeino.ops.threads import ThreadOps
from skeino.persistence import InMemoryMetadataStore
from skeino.schemas import ThreadCreateRequest


class _ListStateGraph:
    """Minimal graph whose per-thread state is a list, not a dict."""

    def __init__(self) -> None:
        self.state: dict[str, list[Any]] = {}

    async def aget_state(
        self, config: dict[str, Any], *, subgraphs: bool = False
    ) -> SimpleNamespace:
        del subgraphs
        thread_id = str(config["configurable"]["thread_id"])
        return SimpleNamespace(
            values=list(self.state.get(thread_id, [])),
            next=(),
            tasks=(),
            config={"configurable": {"thread_id": thread_id}},
            metadata={},
            created_at=datetime.now(UTC),
            parent_config=None,
            interrupts=(),
        )

    async def aupdate_state(
        self,
        config: dict[str, Any],
        values: Any,
        as_node: str | None = None,
        task_id: str | None = None,
    ) -> dict[str, Any]:
        del as_node, task_id
        thread_id = str(config["configurable"]["thread_id"])
        self.state[thread_id] = list(values)
        return config


async def test_copy_seeds_list_valued_state() -> None:
    graph = _ListStateGraph()
    store = InMemoryMetadataStore()
    await store.setup()
    ops = ThreadOps(graph=graph, metadata_store=store)

    source = await ops.create(ThreadCreateRequest(metadata={}))
    source_id = str(source.thread_id)
    graph.state[source_id] = ["a", "b", "c"]

    copy = await ops.copy(source_id)
    copy_id = str(copy.thread_id)

    # The list state was seeded into the copy (not left empty), and provenance
    # is recorded.
    assert copy_id != source_id
    assert graph.state[copy_id] == ["a", "b", "c"]
    assert copy.metadata["forked_from"] == source_id
