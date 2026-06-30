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
from skeino.api._openapi import install_request_body_openapi
from skeino.api._request import SkeinoState
from skeino.concurrency import BackgroundRunRegistry, ThreadLockManager
from skeino.config import SkeinoSettings
from skeino.ops import AssistantOps, RunOps, ThreadOps
from skeino.persistence import (
    InMemoryMetadataStore,
    MetadataStore,
    MetadataStoreProtocol,
    MongoMetadataStore,
    SqliteMetadataStore,
    open_checkpointer,
)
from skeino.persistence.uri import (
    MEMORY_SCHEMES,
    SQLITE_SCHEMES,
    normalize_sqlite_uri,
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


# Schemes whose metadata store has a native implementation. Other durable
# checkpointers (redis, custom) have no native metadata store and trip the
# fail-loud guard unless allow_ephemeral_metadata is set.
_NATIVE_METADATA_STORES: dict[str, Callable[[str], MetadataStoreProtocol]] = {
    "postgres": MetadataStore,
    "postgresql": MetadataStore,
    "sqlite": SqliteMetadataStore,
    "sqlite3": SqliteMetadataStore,
    "mongodb": MongoMetadataStore,
    "mongo": MongoMetadataStore,
}


def _scheme(settings: SkeinoSettings) -> str:
    return (settings.checkpointer_scheme or "memory").lower()


def _resolve_metadata_store(settings: SkeinoSettings) -> MetadataStoreProtocol:
    """Pick the metadata store from the scheme (native for pg/sqlite/mongo).

    SQLite defaults to ``:memory:`` when no URI is given — mirroring the SQLite
    checkpointer builder — so the checkpointer and metadata store never split.
    Postgres/Mongo require a URI (the checkpointer builder raises otherwise).
    """
    scheme = _scheme(settings)
    factory = _NATIVE_METADATA_STORES.get(scheme)
    if factory is None:
        return InMemoryMetadataStore()
    uri = settings.checkpointer_uri
    if uri is None and scheme in SQLITE_SCHEMES:
        uri = normalize_sqlite_uri(uri)  # ":memory:" — same default as the builder
    if uri is None:
        # postgres/mongo need a URI. The checkpointer builder normally raises
        # first; raise here too so a durable scheme never silently downgrades to
        # ephemeral metadata regardless of call order.
        raise ValueError(
            f"checkpointer_scheme={settings.checkpointer_scheme!r} requires "
            "checkpointer_uri to be set."
        )
    return factory(uri)


def _check_metadata_durability(settings: SkeinoSettings) -> None:
    """Fail loudly when a durable checkpointer would pair with ephemeral metadata.

    Durable graph state alongside an in-memory thread/run list is a confusing
    split-brain (state persists, the run list evaporates on restart). This
    arises for a durable scheme with **no native metadata store** (e.g. ``redis``
    or a custom checkpointer); ``allow_ephemeral_metadata`` opts out. (A missing
    URI for postgres/mongo is caught separately by the checkpointer builder.)
    """
    scheme = _scheme(settings)
    durable_checkpointer = scheme not in MEMORY_SCHEMES
    has_native_metadata = scheme in _NATIVE_METADATA_STORES
    if (
        durable_checkpointer
        and not has_native_metadata
        and not settings.allow_ephemeral_metadata
    ):
        raise ValueError(
            f"checkpointer_scheme={settings.checkpointer_scheme!r} is a durable "
            "checkpointer with no native metadata store — thread/run metadata "
            "would be in-memory and lost on restart. Use a scheme with a native "
            "metadata store (postgres/sqlite/mongodb), "
            "or pass allow_ephemeral_metadata=True to accept ephemeral metadata."
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
            # The scheme alone selects the backend (default "memory"); the URI is
            # a connection detail. A URI without a matching scheme is ignored
            # (memory). Durable schemes that need a URI (postgres/mongodb/redis)
            # raise here if it's missing; sqlite defaults to ":memory:".
            checkpointer = await stack.enter_async_context(
                open_checkpointer(
                    settings.checkpointer_uri,
                    scheme=_scheme(settings),
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
                    "not persist across restarts. Use a durable scheme "
                    "(postgres/sqlite/mongodb) with checkpointer_uri to persist."
                )
            # Register cleanup BEFORE setup() so a setup failure that has already
            # opened resources (sqlite connection, motor client) is still closed
            # by the exit stack. aclose() is a no-op when nothing was opened.
            aclose = getattr(metadata_store, "aclose", None)
            if aclose is not None:
                stack.push_async_callback(aclose)
            await metadata_store.setup()

            streamer = Streamer(default_graph)
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
                registry=BackgroundRunRegistry(),
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
                # Cancel any in-flight background runs so their tasks unwind
                # (persist ``interrupted``, release locks) before resources close.
                await run_ops.shutdown()

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
    # Routers parse bodies by hand (text/plain tolerance), so FastAPI can't see
    # their request models — patch the schema to document them (see #67).
    install_request_body_openapi(fastapi_app)
    return fastapi_app
