"""Thread CRUD, state, and history operations."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException, status

from skeino.persistence import MetadataStore
from skeino.schemas import (
    CheckpointConfigModel,
    JsonValue,
    ThreadCreateRequest,
    ThreadModel,
    ThreadSearchRequest,
    ThreadStateModel,
    ThreadStatus,
    ThreadTtlInfo,
)
from skeino.serialization import (
    build_thread_config,
    normalize_input_payload,
    serialize_state_snapshot,
    serialize_value,
)

_THREAD_STATUS_ERROR: ThreadStatus = "error"


def _to_isoformat(value: datetime | None) -> str | None:
    """Convert an optional datetime to ISO 8601."""
    if value is None:
        return None
    return value.isoformat()


def _match_metadata_filters(
    record_metadata: dict[str, JsonValue], filters: dict[str, JsonValue] | None
) -> bool:
    """Return True when all requested metadata key-value pairs match."""
    if not filters:
        return True
    for key, value in filters.items():
        if record_metadata.get(key) != value:
            return False
    return True


def _match_value_filters(
    record_values: dict[str, JsonValue] | None, filters: dict[str, JsonValue] | None
) -> bool:
    """Return True when all requested state values match."""
    if not filters:
        return True
    if record_values is None:
        return False
    for key, value in filters.items():
        if record_values.get(key) != value:
            return False
    return True


class ThreadOps:
    """Thread lifecycle: create, get, search, state, history."""

    def __init__(
        self,
        *,
        graph: Any,
        metadata_store: MetadataStore,
        logger: Any | None = None,
    ) -> None:
        """Capture the graph and metadata store backing this ops layer."""
        self._graph = graph
        self._metadata_store = metadata_store
        self._logger = logger

    async def create(self, request: ThreadCreateRequest) -> ThreadModel:
        """Create a thread and optionally seed it with initial state."""
        thread_id = str(request.thread_id or uuid4())
        config = {"configurable": {"thread_id": thread_id}}
        row = await self._metadata_store.create_thread(
            thread_id,
            metadata=request.metadata,
            config=config,
            ttl=request.ttl,
            if_exists=request.if_exists,
        )

        for superstep in request.supersteps:
            for update in superstep.updates:
                if update.command is not None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Command-based supersteps are not supported.",
                    )
                await self._graph.aupdate_state(
                    {"configurable": {"thread_id": thread_id}},
                    normalize_input_payload(update.values or {}),
                    as_node=update.as_node,
                )

        if request.supersteps:
            await self._metadata_store.update_thread(thread_id, mark_state_updated=True)
            row = await self.require_row(thread_id)

        return await self.build_model_from_row(row)

    async def get(self, thread_id: str) -> ThreadModel:
        """Return a single thread by ID."""
        row = await self.require_row(thread_id)
        return await self.build_model_from_row(row)

    async def search(self, request: ThreadSearchRequest) -> list[ThreadModel]:
        """Return threads enriched with their latest LangGraph state."""
        rows = await self._metadata_store.search_thread_rows(request)
        results: list[ThreadModel] = []
        for row in rows:
            if not _match_metadata_filters(row["metadata"], request.metadata):
                continue
            model = await self.build_model_from_row(row)
            if not _match_value_filters(model.values, request.values):
                continue
            results.append(model)
        return results

    async def get_state(
        self, thread_id: str, *, subgraphs: bool = False
    ) -> ThreadStateModel:
        """Return the latest checkpoint state for a thread."""
        await self.ensure_exists(thread_id)
        config = {"configurable": {"thread_id": thread_id}}
        snapshot = await self._graph.aget_state(config, subgraphs=subgraphs)
        return serialize_state_snapshot(snapshot)

    async def get_history(
        self,
        thread_id: str,
        *,
        limit: int,
        before: CheckpointConfigModel | None = None,
        metadata: dict[str, JsonValue] | None = None,
        checkpoint: CheckpointConfigModel | None = None,
    ) -> list[ThreadStateModel]:
        """Return prior checkpoint states for a thread."""
        await self.ensure_exists(thread_id)
        config = build_thread_config(thread_id, {}, checkpoint)
        before_config: dict[str, Any] | None = None
        if before is not None:
            before_config = build_thread_config(thread_id, {}, before)

        allowed = self._output_keys()
        snapshots: list[ThreadStateModel] = []
        async for snapshot in self._graph.aget_state_history(
            config,
            filter=metadata,
            before=before_config,
            limit=limit,
        ):
            model = serialize_state_snapshot(snapshot)
            if allowed is not None and isinstance(model.values, dict):
                model.values = {k: v for k, v in model.values.items() if k in allowed}
            snapshots.append(model)
        return snapshots

    async def ensure_exists(self, thread_id: str) -> None:
        """Raise 404 when a thread does not exist."""
        row = await self._metadata_store.fetch_thread_row(thread_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Thread {thread_id} not found.",
            )

    async def require_row(self, thread_id: str) -> dict[str, Any]:
        """Return an existing thread row or raise 404."""
        row = await self._metadata_store.fetch_thread_row(thread_id)
        if row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Thread {thread_id} not found.",
            )
        return row

    async def ensure_thread_for_run(self, thread_id: str, if_not_exists: str) -> None:
        """Ensure a thread exists before a run starts, creating it on demand."""
        row = await self._metadata_store.fetch_thread_row(thread_id)
        if row is not None:
            return
        if if_not_exists == "create":
            await self.create(
                ThreadCreateRequest(
                    thread_id=UUID(thread_id),
                    metadata={},
                    if_exists="do_nothing",
                )
            )
            return
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Thread {thread_id} not found.",
        )

    def _output_keys(self) -> frozenset[str] | None:
        """Return the graph's output schema field names, or None if not defined."""
        schema = getattr(self._graph, "output_schema", None)
        if schema is None:
            return None
        try:
            return frozenset(schema.model_fields.keys())
        except Exception:
            return None

    async def build_model_from_row(self, row: dict[str, Any]) -> ThreadModel:
        """Combine stored metadata with the latest graph state."""
        thread_id = str(row["thread_id"])
        try:
            state = await self.get_state(thread_id)
            values: dict[str, Any] = (
                state.values
                if isinstance(state.values, dict)
                else {"data": state.values}
            )
            # Filter to graph's declared output schema so internal pipeline
            # fields (evidence, analyst_reports, routing state, etc.) are never
            # returned to API clients.
            allowed = self._output_keys()
            if allowed is not None:
                values = {k: v for k, v in values.items() if k in allowed}
            interrupts = serialize_value(state.interrupts)
            thread_status: ThreadStatus = row["status"]
        except Exception as exc:
            if self._logger is not None:
                self._logger.warning(
                    "Failed to load checkpoint for thread %s: %s; returning placeholder",
                    thread_id,
                    exc,
                )
            values = {}
            interrupts = []
            thread_status = _THREAD_STATUS_ERROR
        ttl_payload = row.get("ttl")
        ttl_info = (
            ThreadTtlInfo(
                strategy=str(ttl_payload["strategy"]),
                ttl_minutes=float(ttl_payload["ttl_minutes"]),
                expires_at=str(ttl_payload["expires_at"]),
            )
            if isinstance(ttl_payload, dict)
            else None
        )
        return ThreadModel(
            thread_id=UUID(thread_id),
            created_at=row["created_at"].isoformat(),
            updated_at=row["updated_at"].isoformat(),
            state_updated_at=_to_isoformat(row["state_updated_at"]),
            metadata=serialize_value(row["metadata"]),
            config=serialize_value(row["config"]),
            status=thread_status,
            values=values,
            interrupts=interrupts,
            ttl=ttl_info,
        )
