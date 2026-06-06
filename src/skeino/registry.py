"""Graph registry.

v1 holds compiled graphs by name. The signature stays multi-graph-shaped so
multi-graph routing can land later without breaking consumers; today every
ops layer is wired against the *default* graph (first registered, or
explicitly selected via ``SkeinoSettings.default_assistant_id``).
"""

from typing import Iterator, Mapping

from langgraph.graph.state import CompiledStateGraph


class GraphRegistry:
    """Immutable mapping of assistant id to compiled graph."""

    def __init__(
        self,
        graphs: Mapping[str, CompiledStateGraph],
        *,
        default: str | None = None,
    ) -> None:
        """Validate the input mapping and freeze the default selection."""
        if not graphs:
            raise ValueError("GraphRegistry requires at least one graph.")
        if default is not None and default not in graphs:
            raise ValueError(
                f"default={default!r} is not present in graphs={list(graphs)!r}"
            )
        self._graphs: dict[str, CompiledStateGraph] = dict(graphs)
        self._default: str = default or next(iter(self._graphs))

    @property
    def default_id(self) -> str:
        """Identifier of the graph used when none is specified."""
        return self._default

    @property
    def default_graph(self) -> CompiledStateGraph:
        """Compiled graph instance for the default assistant."""
        return self._graphs[self._default]

    def get(self, assistant_id: str) -> CompiledStateGraph | None:
        """Return the graph for ``assistant_id`` (or None when absent)."""
        return self._graphs.get(assistant_id)

    def __iter__(self) -> Iterator[str]:
        """Iterate over registered assistant ids."""
        return iter(self._graphs)

    def __contains__(self, key: object) -> bool:
        """Membership test against assistant ids."""
        return key in self._graphs

    def __len__(self) -> int:
        """Return the number of registered graphs."""
        return len(self._graphs)
