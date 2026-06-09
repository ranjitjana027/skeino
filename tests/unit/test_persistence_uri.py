"""Unit tests for the shared persistence URI helpers."""

import pytest

from skeino.persistence.uri import mongo_db_from_uri, normalize_sqlite_uri


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        (None, ":memory:"),
        ("", ":memory:"),
        (":memory:", ":memory:"),
        ("sqlite://", ":memory:"),
        ("sqlite:///", ":memory:"),
        ("sqlite:///skeino.db", "skeino.db"),
        ("sqlite:////abs/path/skeino.db", "/abs/path/skeino.db"),
        ("sqlite://skeino.db", "skeino.db"),
        ("/abs/path/skeino.db", "/abs/path/skeino.db"),
        ("relative/skeino.db", "relative/skeino.db"),
    ],
)
def test_normalize_sqlite_uri(uri: str | None, expected: str) -> None:
    assert normalize_sqlite_uri(uri) == expected


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("mongodb://localhost:27017/mydb", "mydb"),
        ("mongodb://localhost:27017/mydb?replicaSet=rs0", "mydb"),
        ("mongodb://h1:27017,h2:27017/mydb?replicaSet=rs0", "mydb"),
        ("mongodb+srv://cluster.example.com/mydb", "mydb"),
        ("mongodb://user:pass@localhost:27017/mydb", "mydb"),
        ("mongodb://localhost:27017/my%20db", "my db"),
        # No database in the path → None (callers keep their defaults).
        ("mongodb://localhost:27017", None),
        ("mongodb://localhost:27017/", None),
        ("mongodb://localhost:27017/?replicaSet=rs0", None),
        ("not-a-uri", None),
    ],
)
def test_mongo_db_from_uri(uri: str, expected: str | None) -> None:
    assert mongo_db_from_uri(uri) == expected
