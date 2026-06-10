"""Infra-backed API tests: real Postgres/Mongo/Redis from docker-compose.yml.

This suite is deliberately NOT collected by plain ``pytest`` (``testpaths`` in
pyproject.toml excludes it) so the default suite stays in-memory and
sub-second. Run it explicitly:

    docker compose up -d --wait
    poetry install --all-extras --with redis
    poetry run pytest tests/api
    docker compose down -v

Unlike the in-memory suite, tests here drive a *real* tiny LangGraph graph
compiled with the *real* checkpointer, so they exercise API -> ops -> graph
execution -> durable checkpoints + metadata rows end to end.
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Callable, Iterator

import pytest
from fastapi.testclient import TestClient
from langchain_core.messages import AIMessage
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.graph.state import CompiledStateGraph

from skeino import SkeinoSettings, create_app

POSTGRES_URI = os.environ.get(
    "SKEINO_TEST_POSTGRES_URI", "postgresql://skeino:skeino@localhost:5433/skeino_test"
)
MONGODB_URI = os.environ.get(
    "SKEINO_TEST_MONGODB_URI", "mongodb://localhost:27018/skeino_test"
)
REDIS_URI = os.environ.get("SKEINO_TEST_REDIS_URI", "redis://localhost:6380/0")

# Where mongo data actually lands. Neither consumer honours the db named in
# the URI path: MongoMetadataStore defaults to db "skeino" and langgraph's
# MongoDBSaver to "checkpointing_db".
MONGO_METADATA_DB = "skeino"
MONGO_CHECKPOINT_DB = "checkpointing_db"

# Tables owned by skeino (app_*) and by the langgraph postgres saver.
POSTGRES_TABLES = (
    "app_runs",
    "app_threads",
    "checkpoint_writes",
    "checkpoint_blobs",
    "checkpoints",
    "checkpoint_migrations",
)

_INFRA_HINT = (
    "tests/api needs the local docker services and the redis backend installed.\n"
    "Start/install with:\n"
    "    docker compose up -d --wait\n"
    "    poetry install --all-extras --with redis"
)


@dataclass(frozen=True)
class Backend:
    """One real persistence backend the suite runs against."""

    name: str  # "postgres" | "mongodb" | "redis"
    scheme: str
    uri: str
    # Redis has no native metadata store: metadata is in-memory (opt-in via
    # allow_ephemeral_metadata) while checkpoints are durable.
    allow_ephemeral_metadata: bool


BACKENDS: dict[str, Backend] = {
    "postgres": Backend("postgres", "postgres", POSTGRES_URI, False),
    "mongodb": Backend("mongodb", "mongodb", MONGODB_URI, False),
    "redis": Backend("redis", "redis", REDIS_URI, True),
}


@pytest.fixture(scope="session", autouse=True)
def _require_infra() -> None:
    """Fail loud (never skip) when the docker services or deps are missing."""
    problems: list[str] = []
    try:
        import psycopg

        with psycopg.connect(POSTGRES_URI, connect_timeout=3):
            pass
    except Exception as exc:
        problems.append(f"postgres @ {POSTGRES_URI}: {exc}")
    try:
        from pymongo import MongoClient

        MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000).admin.command("ping")
    except Exception as exc:
        problems.append(f"mongodb @ {MONGODB_URI}: {exc}")
    try:
        import redis as redis_lib

        client = redis_lib.Redis.from_url(REDIS_URI, socket_connect_timeout=3)
        client.ping()
        modules = {
            (m.get(b"name") or m.get("name", b"")).decode().lower()
            if isinstance(m.get(b"name") or m.get("name"), bytes)
            else str(m.get(b"name") or m.get("name", "")).lower()
            for m in client.module_list()
        }
        if not {"search", "rejson"} <= modules:
            problems.append(
                f"redis @ {REDIS_URI}: missing RediSearch/RedisJSON modules "
                f"(found: {sorted(modules)}) — use the redis:8 image "
                "(or redis/redis-stack-server)"
            )
    except ImportError as exc:
        problems.append(f"redis client not installed: {exc}")
    except Exception as exc:
        problems.append(f"redis @ {REDIS_URI}: {exc}")
    try:
        import langgraph.checkpoint.redis  # noqa: F401
    except ImportError:
        problems.append(
            "langgraph-checkpoint-redis missing "
            "(poetry install --all-extras --with redis)"
        )
    if problems:
        pytest.fail(
            _INFRA_HINT + "\n\nUnreachable/missing:\n- " + "\n- ".join(problems)
        )


def _echo_node(state: MessagesState) -> dict[str, Any]:
    last = state["messages"][-1]
    return {"messages": [AIMessage(content=f"echo: {last.content}")]}


def _boom_node(state: MessagesState) -> dict[str, Any]:
    raise RuntimeError("node boom")


def build_echo_graph(
    checkpointer: BaseCheckpointSaver | None,
) -> CompiledStateGraph:
    """A real one-node graph: appends a deterministic AI echo message."""
    builder = StateGraph(MessagesState)
    builder.add_node("echo", _echo_node)
    builder.add_edge(START, "echo")
    builder.add_edge("echo", END)
    return builder.compile(checkpointer=checkpointer)


def build_failing_graph(
    checkpointer: BaseCheckpointSaver | None,
) -> CompiledStateGraph:
    """A real graph whose only node raises — for durable error-status tests."""
    builder = StateGraph(MessagesState)
    builder.add_node("boom", _boom_node)
    builder.add_edge(START, "boom")
    builder.add_edge("boom", END)
    return builder.compile(checkpointer=checkpointer)


def reset_backend(backend: Backend) -> None:
    """Wipe a backend BEFORE a test (failed-test data stays inspectable).

    Uses sync drivers on purpose: the app's async drivers live only inside the
    TestClient portal loop, so cleanup never races an event loop.
    """
    if backend.name == "postgres":
        import psycopg

        with psycopg.connect(backend.uri, autocommit=True) as conn:
            conn.execute(f"DROP TABLE IF EXISTS {', '.join(POSTGRES_TABLES)} CASCADE")
    elif backend.name == "mongodb":
        from pymongo import MongoClient

        # The URI path is ignored by both mongo consumers: MongoMetadataStore
        # hardcodes db "skeino" and the langgraph MongoDBSaver defaults to
        # "checkpointing_db". Drop both (see MONGO_METADATA_DB/MONGO_CHECKPOINT_DB).
        client: Any = MongoClient(backend.uri)
        client.drop_database(MONGO_METADATA_DB)
        client.drop_database(MONGO_CHECKPOINT_DB)
    else:
        import redis as redis_lib

        # FLUSHALL also drops RediSearch indices; the next app lifespan's
        # asetup() recreates them.
        redis_lib.Redis.from_url(backend.uri).flushall()


def _settings_for(backend: Backend) -> SkeinoSettings:
    return SkeinoSettings(
        default_assistant_id="echo_agent",
        assistant_name="Echo Agent",
        assistant_description="skeino infra test agent",
        checkpointer_scheme=backend.scheme,
        checkpointer_uri=backend.uri,
        allow_ephemeral_metadata=backend.allow_ephemeral_metadata,
    )


@contextmanager
def api_client(
    backend: Backend,
    *,
    graph_builder: Callable[
        [BaseCheckpointSaver | None], CompiledStateGraph
    ] = build_echo_graph,
) -> Iterator[TestClient]:
    """A TestClient over a freshly built app (lifespan opens/closes the backend).

    A context manager rather than a fixture so restart tests can open two
    consecutive apps against the same backend within one test. Never reuse an
    app across TestClient contexts: async drivers bind to the portal loop.
    """
    app = create_app(
        graphs={"echo_agent": graph_builder}, settings=_settings_for(backend)
    )
    with TestClient(app) as client:
        yield client


@pytest.fixture(params=["postgres", "mongodb", "redis"])
def any_backend(request: pytest.FixtureRequest) -> Backend:
    backend = BACKENDS[request.param]
    reset_backend(backend)
    return backend


@pytest.fixture(params=["postgres", "mongodb"])
def metadata_backend(request: pytest.FixtureRequest) -> Backend:
    """Backends with a durable native metadata store (excludes redis)."""
    backend = BACKENDS[request.param]
    reset_backend(backend)
    return backend


@pytest.fixture
def postgres_backend() -> Backend:
    backend = BACKENDS["postgres"]
    reset_backend(backend)
    return backend


@pytest.fixture
def mongodb_backend() -> Backend:
    backend = BACKENDS["mongodb"]
    reset_backend(backend)
    return backend


@pytest.fixture
def redis_backend() -> Backend:
    backend = BACKENDS["redis"]
    reset_backend(backend)
    return backend


def create_thread(client: TestClient, **body: Any) -> str:
    response = client.post("/threads", json=body)
    assert response.status_code == 200, response.text
    return str(response.json()["thread_id"])


def run_to_completion(client: TestClient, thread_id: str, content: str) -> dict:
    response = client.post(
        f"/threads/{thread_id}/runs",
        json={
            "assistant_id": "echo_agent",
            "input": {"messages": [{"role": "user", "content": content}]},
        },
    )
    assert response.status_code == 200, response.text
    body = response.json()
    assert body["status"] == "success"
    return body


def message_contents(state: dict) -> list[str]:
    return [m["content"] for m in state["values"].get("messages", [])]
