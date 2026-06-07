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
from skeino.persistence import (
    InMemoryMetadataStore,
    MetadataStore,
    MetadataStoreProtocol,
    SqliteMetadataStore,
    open_checkpointer,
)
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
    if isinstance(entry, CompiledStateGraph):
        return entry
    result = entry(checkpointer)
    if isinstance(result, Awaitable):
        return await result
    return result


_MEMORY_SCHEMES: frozenset[str] = frozenset({"memory"})


def _resolve_checkpointer_target(
    settings: SkeinoSettings,
) -> tuple[str | None, str | None] | None:
    """Return ``(uri, scheme)`` for the checkpointer, or None when none is needed.

    Precedence: ``postgres_uri`` → ``sqlite_path`` → an explicit
    ``checkpointer_scheme`` (e.g. a custom/registered backend or ``memory``).
    """
    if settings.postgres_uri:
        return settings.postgres_uri, settings.checkpointer_scheme
    if settings.sqlite_path:
        return settings.sqlite_path, settings.checkpointer_scheme or "sqlite"
    if settings.checkpointer_scheme:
        return None, settings.checkpointer_scheme
    return None


def _resolve_metadata_store(settings: SkeinoSettings) -> MetadataStoreProtocol:
    """Pick the metadata store backend (Postgres → SQLite → in-memory)."""
    if settings.postgres_uri:
        return MetadataStore(settings.postgres_uri)
    if settings.sqlite_path:
        return SqliteMetadataStore(settings.sqlite_path)
    return InMemoryMetadataStore()


def _check_metadata_durability(settings: SkeinoSettings) -> None:
    """Fail loudly when a durable checkpointer would pair with ephemeral metadata.

    A durable graph state alongside an in-memory thread/run list is a confusing
    split-brain (state persists, the run list evaporates on restart). This only
    arises with an explicit durable ``checkpointer_scheme`` and no
    ``postgres_uri``/``sqlite_path``; ``allow_ephemeral_metadata`` opts out.
    """
    scheme = settings.checkpointer_scheme
    durable_checkpointer = scheme is not None and scheme.lower() not in _MEMORY_SCHEMES
    durable_metadata = bool(settings.postgres_uri or settings.sqlite_path)
    if (
        durable_checkpointer
        and not durable_metadata
        and not (settings.allow_ephemeral_metadata)
    ):
        raise ValueError(
            f"checkpointer_scheme={scheme!r} configures a durable checkpointer, "
            "but no durable metadata store is set (no postgres_uri / sqlite_path) "
            "— thread/run metadata would be in-memory and lost on restart. Set "
            "postgres_uri or sqlite_path, or pass allow_ephemeral_metadata=True "
            "to accept ephemeral metadata."
        )


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
        _check_metadata_durability(settings)
        async with AsyncExitStack() as stack:
            checkpointer: BaseCheckpointSaver | None = None
            target = _resolve_checkpointer_target(settings)
            if target is not None:
                uri, scheme = target
                checkpointer = await stack.enter_async_context(
                    open_checkpointer(
                        uri,
                        scheme=scheme,
                        options=dict(settings.checkpointer_options),
                    )
                )

            compiled: dict[str, CompiledStateGraph] = {}
            for name, entry in graphs.items():
                compiled[name] = await _materialise_graph(entry, checkpointer)
            registry = GraphRegistry(compiled, default=default_id)
            default_graph = registry.default_graph

            metadata_store: MetadataStoreProtocol = _resolve_metadata_store(settings)
            if isinstance(metadata_store, InMemoryMetadataStore):
                logger.info(
                    "Using in-memory metadata store — thread/run metadata will "
                    "not persist across restarts. Set postgres_uri or sqlite_path "
                    "for durable metadata."
                )
            await metadata_store.setup()
            aclose = getattr(metadata_store, "aclose", None)
            if aclose is not None:
                stack.push_async_callback(aclose)

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
