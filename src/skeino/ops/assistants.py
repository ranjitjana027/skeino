"""Assistant search/lookup and graph-schema introspection.

The v1 surface assumes a single-graph deployment per ``AssistantOps`` instance.
A simple in-memory registry can be layered on top to support multi-graph
deployments; for now ``default_assistant_id`` names the only assistant
exposed.
"""

from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import HTTPException, status

from skeino.schemas import (
    AssistantModel,
    AssistantSearchRequest,
    GraphSchemaModel,
    ServerInfoModel,
)
from skeino.serialization import (
    serialize_mapping,
    serialize_optional_mapping,
    serialize_value,
)


class AssistantOps:
    """Singleton assistant facade backed by a single compiled graph."""

    def __init__(
        self,
        *,
        graph: Any,
        default_assistant_id: str,
        assistant_name: str | None = None,
        assistant_description: str | None = None,
        supported_assistant_ids: frozenset[str] | None = None,
        assistant_namespace: str = "https://skeino.local/assistants",
        now: datetime,
    ) -> None:
        """Capture the graph and immutable assistant metadata."""
        self._graph = graph
        self._default_assistant_id = default_assistant_id
        self._assistant_name = assistant_name
        self._assistant_description = assistant_description
        self._supported_ids: frozenset[str] = (
            supported_assistant_ids
            if supported_assistant_ids is not None
            else frozenset({default_assistant_id})
        )
        self._assistant_uuid: UUID = uuid5(
            NAMESPACE_URL, f"{assistant_namespace}/{default_assistant_id}"
        )
        self._created_at: datetime = now

    @property
    def assistant_uuid(self) -> UUID:
        """Stable deterministic UUID derived from the assistant id."""
        return self._assistant_uuid

    @property
    def default_assistant_id(self) -> str:
        """Configured default assistant identifier."""
        return self._default_assistant_id

    def get_server_info(self, server_version: str) -> ServerInfoModel:
        """Return minimal system information for the standalone server."""
        return ServerInfoModel(
            status="ok",
            name=self._default_assistant_id,
            version=server_version,
        )

    def matches(self, assistant_id: str) -> bool:
        """Return True when ``assistant_id`` should resolve to the singleton."""
        if assistant_id in self._supported_ids:
            return True
        if assistant_id == self._default_assistant_id:
            return True
        if assistant_id == str(self._assistant_uuid):
            return True
        try:
            UUID(assistant_id)
        except ValueError:
            return False
        return True

    def ensure_supported(self, assistant_id: str) -> None:
        """Validate that the requested assistant is available."""
        if self.matches(assistant_id):
            return
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Assistant {assistant_id!r} is not available.",
        )

    def search(self, request: AssistantSearchRequest) -> list[AssistantModel]:
        """Return the singleton assistant when it matches the filters."""
        assistant = self._build_assistant_model()

        if request.graph_id is not None and request.graph_id != assistant.graph_id:
            return []
        if request.name is not None:
            assistant_name = (assistant.name or "").lower()
            if request.name.lower() not in assistant_name:
                return []
        if request.metadata is not None:
            for key, value in request.metadata.items():
                if assistant.metadata.get(key) != value:
                    return []

        return [assistant][request.offset : request.offset + request.limit]

    def get(self, assistant_id: str) -> AssistantModel:
        """Return the singleton assistant for compatible IDs."""
        self.ensure_supported(assistant_id)
        return self._build_assistant_model()

    def get_schemas(self, assistant_id: str) -> GraphSchemaModel:
        """Return graph schemas for the singleton assistant."""
        self.ensure_supported(assistant_id)
        return GraphSchemaModel(
            graph_id=self._default_assistant_id,
            input_schema=serialize_optional_mapping(self._graph.get_input_jsonschema()),
            output_schema=serialize_optional_mapping(
                self._graph.get_output_jsonschema()
            ),
            state_schema=serialize_mapping(self._graph.get_input_jsonschema()),
            config_schema=serialize_optional_mapping(
                self._graph.get_config_jsonschema()
            ),
            context_schema=serialize_optional_mapping(
                self._graph.get_context_jsonschema()
            ),
        )

    def get_graph(
        self, assistant_id: str, *, xray: bool | int = False
    ) -> dict[str, Any]:
        """Return the graph structure (nodes, edges) for visualization."""
        del xray
        self.ensure_supported(assistant_id)
        graph_obj = self._graph.get_graph()
        raw = graph_obj.to_json()
        return serialize_mapping(raw)

    def get_subgraphs(
        self, assistant_id: str, *, recurse: bool = False
    ) -> dict[str, Any]:
        """Return subgraph schemas, or the main graph schema if no subgraphs."""
        del recurse
        self.ensure_supported(assistant_id)
        subgraphs_raw = dict(self._graph.get_subgraphs())
        if not subgraphs_raw:
            schema = self._graph_schema_dict(self._graph)
            return {self._default_assistant_id: schema}
        return {
            str(name): self._graph_schema_dict(subgraph)
            for name, subgraph in subgraphs_raw.items()
        }

    def _graph_schema_dict(self, graph: Any) -> dict[str, Any]:
        """Build a JSON-serializable schema record for a graph."""
        return {
            "input_schema": serialize_value(graph.get_input_jsonschema()),
            "output_schema": serialize_value(graph.get_output_jsonschema()),
            "state_schema": serialize_value(graph.get_input_jsonschema()),
            "config_schema": serialize_value(graph.get_config_jsonschema()),
            "context_schema": serialize_value(graph.get_context_jsonschema()),
        }

    def _build_assistant_model(self) -> AssistantModel:
        """Build the singleton assistant descriptor."""
        created_at = self._created_at.isoformat()
        return AssistantModel(
            assistant_id=self._assistant_uuid,
            graph_id=self._default_assistant_id,
            config={},
            context={},
            created_at=created_at,
            updated_at=created_at,
            metadata={"runtime": "skeino", "created_by": "system"},
            version=1,
            name=self._assistant_name,
            description=self._assistant_description,
        )
