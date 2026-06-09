"""Scheme and URI helpers shared across the persistence layer.

Single source of truth for the SQLite ``:memory:`` default and ``sqlite://``
prefix stripping — used by the checkpointer builder, the SQLite metadata
store, and ``create_app``'s store resolution, so the "checkpointer and
metadata store never split" invariant lives in one place — plus MongoDB
database-from-URI derivation.

Distinct from ``skeino.langgraph_json._scheme_from_uri``, which derives a
checkpointer *scheme* from a manifest ``store.uri``; this module normalises
URIs *within* an already-chosen scheme.
"""

from urllib.parse import unquote, urlsplit

MEMORY_SCHEMES: frozenset[str] = frozenset({"memory", ""})
SQLITE_SCHEMES: frozenset[str] = frozenset({"sqlite", "sqlite3"})


def normalize_sqlite_uri(uri: str | None) -> str:
    """Normalise a SQLite URI/path to what aiosqlite/AsyncSqliteSaver expect."""
    if not uri:
        return ":memory:"
    for prefix in ("sqlite:///", "sqlite://"):
        if uri.startswith(prefix):
            return uri[len(prefix) :] or ":memory:"
    return uri


def mongo_db_from_uri(uri: str) -> str | None:
    """Return the database named in a MongoDB URI's path, or ``None``.

    Parses with ``urlsplit`` rather than pymongo: pymongo is an optional
    extra, and its parser resolves ``mongodb+srv://`` hosts over DNS — network
    I/O this helper must never do. Multi-host netlocs and query strings are
    handled; a degenerate multi-segment path (``/db/extra``) is returned
    verbatim so the driver rejects it loudly.
    """
    if "://" not in uri:
        return None
    db = urlsplit(uri).path.lstrip("/")
    return unquote(db) if db else None
