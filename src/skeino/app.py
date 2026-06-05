"""The ``create_app`` factory.

Use this when wiring a skeino-backed FastAPI app programmatically. For
``langgraph.json``-driven setups, prefer
:func:`skeino.langgraph_json.from_langgraph_json`, which calls into this factory
after parsing the manifest.
"""

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from datetime import UTC, datetime
from typing import AsyncIterator, Awaitable, Callable, Mapping

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph.state import CompiledStateGraph

from skeino.api import (
    assistants_router,
    health_router,
    runs_router,
    threads_router,
)
from skeino.api._request import SkeinoState
from skeino.concurrency import ThreadLockManager
from skeino.config import SkeinoSettings
from skeino.ops import AssistantOps, RunOps, ThreadOps
from skeino.persistence import InMemoryMetadataStore, MetadataStore, open_checkpointer
from skeino.registry import GraphRegistry
from skeino.streaming import Streamer

GraphBuilder = Callable[
    [BaseCheckpointSaver | None],
    CompiledStateGraph | Awaitable[CompiledStateGraph],
]
GraphInput = GraphBuilder | CompiledStateGraph

logger = logging.getLogger(__name__)


async def _materialise_graph(
    entry: GraphInput, checkpointer: BaseCheckpointSaver | None
) -> CompiledStateGraph:
    """Resolve a graph entry into a compiled graph instance."""
    if callable(entry) and not isinstance(entry, CompiledStateGraph):
        result = entry(checkpointer)
        if isinstance(result, Awaitable):
            result = await result  # type: ignore[assignment]
        return result  # type: ignore[return-value]
    return entry  # type: ignore[return-value]


def _resolve_default_id(
    graphs: Mapping[str, GraphInput], settings: SkeinoSettings
) -> str:
    """Choose the assistant id used by ops layers in single-graph mode."""
    if settings.default_assistant_id is not None:
        if settings.default_assistant_id not in graphs:
            raise ValueError(
                f"default_assistant_id={settings.default_assistant_id!r} "
                f"is not in graphs={list(graphs)!r}"
            )
        return settings.default_assistant_id
    return next(iter(graphs))


def create_app(
    *,
    graphs: Mapping[str, GraphInput],
    settings: SkeinoSettings,
) -> FastAPI:
    """Build a FastAPI application that exposes the skeino HTTP surface.

    ``graphs`` maps assistant id → either a precompiled :class:`CompiledStateGraph`
    or a builder ``(checkpointer) -> CompiledStateGraph`` (sync or async). When
    a builder is supplied, skeino resolves a checkpointer via
    :func:`skeino.persistence.open_checkpointer` and passes it in.
    """
    if not graphs:
        raise ValueError("create_app requires at least one graph entry.")

    default_id = _resolve_default_id(graphs, settings)

    @asynccontextmanager
    async def lifespan(app_instance: FastAPI) -> AsyncIterator[None]:
        async with AsyncExitStack() as stack:
            checkpointer: BaseCheckpointSaver | None = None
            if settings.postgres_uri or settings.checkpointer_scheme:
                checkpointer = await stack.enter_async_context(
                    open_checkpointer(
                        settings.postgres_uri,
                        scheme=settings.checkpointer_scheme,
                        options=dict(settings.checkpointer_options),
                    )
                )

            compiled: dict[str, CompiledStateGraph] = {}
            for name, entry in graphs.items():
                compiled[name] = await _materialise_graph(entry, checkpointer)
            registry = GraphRegistry(compiled, default=default_id)
            default_graph = registry.default_graph

            metadata_store: MetadataStore | InMemoryMetadataStore
            if settings.postgres_uri:
                metadata_store = MetadataStore(settings.postgres_uri)
            else:
                metadata_store = InMemoryMetadataStore()
                logger.info(
                    "POSTGRES_URI not set — using in-memory metadata store "
                    "(thread/run metadata will not persist across restarts)."
                )
            await metadata_store.setup()

            streamer = Streamer(
                default_graph,
                agent_nodes=settings.agent_nodes,
                status_field=settings.status_field,
            )
            assistant_ops = AssistantOps(
                graph=default_graph,
                default_assistant_id=default_id,
                assistant_name=settings.assistant_name,
                assistant_description=settings.assistant_description,
                supported_assistant_ids=settings.supported_assistant_ids,
                assistant_namespace=settings.assistant_namespace,
                now=datetime.now(UTC),
            )
            thread_ops = ThreadOps(
                graph=default_graph,
                metadata_store=metadata_store,
                logger=logger,
            )
            run_ops = RunOps(
                graph=default_graph,
                metadata_store=metadata_store,
                streamer=streamer,
                thread_ops=thread_ops,
                assistant_ops=assistant_ops,
                lock_manager=ThreadLockManager(),
                logger=logger,
            )

            app_instance.state.skeino = SkeinoState(
                thread_ops=thread_ops,
                run_ops=run_ops,
                assistant_ops=assistant_ops,
                settings=settings,
            )
            app_instance.state.registry = registry
            logger.info("skeino runtime initialised (graphs=%s)", list(compiled))
            try:
                yield
            finally:
                logger.info("skeino runtime shutting down")

    fastapi_app = FastAPI(
        title=settings.server_title,
        description=settings.server_description,
        version=settings.server_version,
        lifespan=lifespan,
    )
    fastapi_app.add_middleware(
        CORSMiddleware,
        allow_origins=list(settings.cors_origins),
        allow_methods=list(settings.cors_methods),
        allow_headers=list(settings.cors_headers),
    )
    fastapi_app.include_router(health_router)
    fastapi_app.include_router(assistants_router)
    fastapi_app.include_router(threads_router)
    fastapi_app.include_router(runs_router)
    return fastapi_app
