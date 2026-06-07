"""Scheme-authoritative persistence resolution + langgraph.json scheme derivation.

These pin the PR's headline contract: the *scheme* selects the backend (a URI
without a matching scheme is ignored), and `langgraph.json`'s `store.uri` maps to
`checkpointer_uri` with the scheme derived from the URI prefix.
"""

import pytest

from skeino.app import _resolve_metadata_store
from skeino.config import SkeinoSettings
from skeino.langgraph_json import _scheme_from_uri, _settings_from_manifest
from skeino.persistence import (
    InMemoryMetadataStore,
    MetadataStore,
    MongoMetadataStore,
    SqliteMetadataStore,
    open_checkpointer,
)


@pytest.mark.parametrize(
    "uri,scheme",
    [
        ("postgresql://h/db", "postgres"),
        ("postgres://h/db", "postgres"),
        ("mongodb://h/db", "mongodb"),
        ("mongodb+srv://h/db", "mongodb"),
        ("sqlite:///x.db", "sqlite"),
        ("sqlite3:///x.db", "sqlite"),
        ("redis://h:6379/0", "redis"),
        ("/var/lib/skeino.db", "sqlite"),  # bare path → sqlite fallback
        ("cassandra://h", "cassandra"),  # unknown scheme → passthrough
    ],
)
def test_scheme_from_uri(uri: str, scheme: str) -> None:
    assert _scheme_from_uri(uri) == scheme


def test_manifest_store_uri_sets_checkpointer_uri_and_scheme() -> None:
    settings = _settings_from_manifest(
        {"store": {"uri": "postgresql://host/db"}}, overrides=None
    )
    assert settings.checkpointer_uri == "postgresql://host/db"
    assert settings.checkpointer_scheme == "postgres"


def test_manifest_without_store_keeps_memory_default() -> None:
    settings = _settings_from_manifest({}, overrides=None)
    assert settings.checkpointer_scheme == "memory"
    assert settings.checkpointer_uri is None


# --- scheme decides the backend; a URI without a matching scheme is ignored ---


def test_memory_scheme_ignores_a_postgres_uri() -> None:
    store = _resolve_metadata_store(
        SkeinoSettings(
            checkpointer_scheme="memory", checkpointer_uri="postgresql://x/d"
        )
    )
    assert isinstance(store, InMemoryMetadataStore)


def test_native_schemes_resolve_their_store() -> None:
    assert isinstance(
        _resolve_metadata_store(
            SkeinoSettings(checkpointer_scheme="sqlite", checkpointer_uri=":memory:")
        ),
        SqliteMetadataStore,
    )
    assert isinstance(
        _resolve_metadata_store(
            SkeinoSettings(
                checkpointer_scheme="postgres", checkpointer_uri="postgresql://x/d"
            )
        ),
        MetadataStore,
    )
    assert isinstance(
        _resolve_metadata_store(
            SkeinoSettings(
                checkpointer_scheme="mongodb", checkpointer_uri="mongodb://x/d"
            )
        ),
        MongoMetadataStore,
    )


def test_postgres_scheme_without_uri_raises() -> None:
    with pytest.raises(ValueError, match="checkpointer_uri"):
        _resolve_metadata_store(SkeinoSettings(checkpointer_scheme="postgres"))


# --- durable checkpointer builders require a URI ---


@pytest.mark.parametrize("scheme", ["postgres", "mongodb", "redis"])
async def test_durable_builder_without_uri_raises(scheme: str) -> None:
    with pytest.raises(ValueError, match="requires a connection URI"):
        async with open_checkpointer(scheme=scheme):
            pass
