"""Thread CRUD, state, and history operations."""

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import HTTPException, status

from skeino.persistence import MetadataStoreProtocol
from skeino.schemas import (
    CheckpointConfigModel,
    JsonValue,
    RunIfNotExists,
    ThreadCreateRequest,
    ThreadModel,
    ThreadPatchRequest,
    ThreadSearchRequest,
    ThreadStateModel,
    ThreadStateUpdateRequest,
    ThreadStatus,
    ThreadTtlInfo,
)
from skeino.serialization import (
    build_thread_config,
    normalize_input_payload,
    serialize_mapping,
    serialize_state_snapshot,
    serialize_value,
)


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
        metadata_store: MetadataStoreProtocol,
        logger: Any | None = None,
    ) -> None:
        """Capture the graph and metadata store backing this ops layer."""
        self._graph = graph
        self._metadata_store = metadata_store
        self._logger = logger

    async def create(self, request: ThreadCreateRequest) -> ThreadModel:
        """Create a thread and optionally seed it with initial state."""
        thread_id = str(request.thread_id or uuid4())
        config: dict[str, JsonValue] = {"configurable": {"thread_id": thread_id}}
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

    async def update(self, thread_id: str, request: ThreadPatchRequest) -> ThreadModel:
        """Update a thread's mutable metadata and return the updated record."""
        await self.ensure_exists(thread_id)
        await self._metadata_store.update_thread(thread_id, metadata=request.metadata)
        return await self.build_model_from_row(await self.require_row(thread_id))

    async def delete(self, thread_id: str) -> None:
        """Delete a thread, its run rows, and its checkpoint history."""
        await self.ensure_exists(thread_id)
        checkpointer = getattr(self._graph, "checkpointer", None)
        delete_checkpoints = getattr(checkpointer, "adelete_thread", None)
        if delete_checkpoints is not None:
            await delete_checkpoints(thread_id)
        await self._metadata_store.delete_thread(thread_id)

    async def update_state(
        self, thread_id: str, request: ThreadStateUpdateRequest
    ) -> CheckpointConfigModel:
        """Write/patch thread state (human-in-the-loop edit).

        Applies ``values`` as an update through the graph (optionally as a
        specific node and/or from a specific checkpoint) and returns the
        checkpoint config produced by the write.
        """
        await self.ensure_exists(thread_id)
        config = build_thread_config(thread_id, {}, request.checkpoint)
        # Explicit None check: an empty list payload (`[]`) must stay a list,
        # not be coerced to `{}` by truthiness.
        values = request.values if request.values is not None else {}
        new_config = await self._graph.aupdate_state(
            config,
            normalize_input_payload(values),
            as_node=request.as_node,
        )
        await self._metadata_store.update_thread(thread_id, mark_state_updated=True)
        configurable = (new_config or {}).get("configurable", {})
        return CheckpointConfigModel(
            thread_id=str(configurable.get("thread_id", thread_id)),
            checkpoint_ns=configurable.get("checkpoint_ns"),
            checkpoint_id=configurable.get("checkpoint_id"),
            checkpoint_map=configurable.get("checkpoint_map"),
        )

    async def copy(self, source_thread_id: str) -> ThreadModel:
        """Fork a thread into an independent copy seeded with its latest state.

        The new thread gets a fresh id, copies the source's metadata (stamped
        with ``forked_from``), and is seeded with the source's *latest*
        checkpoint state via ``aupdate_state`` so callers can branch and explore
        without mutating the original.

        This is a **shallow** copy: only the current state carries over, not the
        full checkpoint history. Replaying history through the graph's reducers
        (e.g. message-appending channels) would not faithfully reconstruct it, so
        v1 copies the latest state only. Because it goes through the graph's
        public state API rather than checkpointer internals, it behaves
        identically across the in-memory and Postgres backends.
        """
        source_row = await self.require_row(source_thread_id)
        new_thread_id = str(uuid4())
        new_config: dict[str, JsonValue] = {
            "configurable": {"thread_id": new_thread_id}
        }
        metadata: dict[str, JsonValue] = {
            **source_row["metadata"],
            "forked_from": source_thread_id,
        }
        row = await self._metadata_store.create_thread(
            new_thread_id,
            metadata=metadata,
            config=new_config,
            ttl=None,
            if_exists="raise",
        )

        source_snapshot = await self._graph.aget_state(
            {"configurable": {"thread_id": source_thread_id}}
        )
        source_values = getattr(source_snapshot, "values", None)
        # LangGraph state may be a dict or a list (or other non-dict shape), so
        # seed the copy from any non-empty state rather than dicts only.
        if source_values:
            await self._graph.aupdate_state(new_config, source_values)
            await self._metadata_store.update_thread(
                new_thread_id, mark_state_updated=True
            )
            row = await self.require_row(new_thread_id)

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
        self,
        thread_id: str,
        *,
        subgraphs: bool = False,
        checkpoint: CheckpointConfigModel | None = None,
    ) -> ThreadStateModel:
        """Return thread state — the latest checkpoint, or a specific one.

        When ``checkpoint`` is provided, state is read at that checkpoint
        (time travel); otherwise the latest checkpoint is returned.
        """
        await self.ensure_exists(thread_id)
        config = build_thread_config(thread_id, {}, checkpoint)
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

    async def ensure_thread_for_run(
        self, thread_id: str, if_not_exists: RunIfNotExists
    ) -> None:
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
        except AttributeError:
            # Fail closed: drop all values rather than leak internal fields when
            # the declared output schema cannot be introspected.
            if self._logger is not None:
                self._logger.warning(
                    "Could not resolve output schema fields; returning an empty "
                    "allow-set to avoid leaking internal fields to clients"
                )
            return frozenset()

    async def build_model_from_row(self, row: dict[str, Any]) -> ThreadModel:
        """Combine stored metadata with the latest graph state."""
        thread_id = str(row["thread_id"])
        thread_status: ThreadStatus = row["status"]
        values: dict[str, Any] = {}
        interrupts: Any = []
        try:
            state = await self.get_state(thread_id)
            resolved_values: dict[str, Any] = (
                state.values
                if isinstance(state.values, dict)
                else {"data": state.values}
            )
            # Filter to graph's declared output schema so internal pipeline
            # fields (evidence, analyst_reports, routing state, etc.) are never
            # returned to API clients.
            allowed = self._output_keys()
            if allowed is not None:
                resolved_values = {
                    k: v for k, v in resolved_values.items() if k in allowed
                }
            values = resolved_values
            interrupts = serialize_value(state.interrupts)
        except Exception as exc:
            # A checkpoint-read failure is not the same as the thread being in
            # an error state: keep the stored status and surface the failure
            # with a traceback rather than masking it as status="error". Caught
            # broadly on purpose — this runs once per row in search(), so one
            # unreadable checkpoint must not 500 the entire listing.
            if self._logger is not None:
                self._logger.error(
                    "Failed to load checkpoint for thread %s; returning stored "
                    "status %r with empty values",
                    thread_id,
                    thread_status,
                    exc_info=exc,
                )
        ttl_payload = row.get("ttl")
        ttl_info = (
            ThreadTtlInfo(
                strategy=ttl_payload["strategy"],
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
            metadata=serialize_mapping(row["metadata"]),
            config=serialize_mapping(row["config"]),
            status=thread_status,
            values=values,
            interrupts=interrupts,
            ttl=ttl_info,
        )
