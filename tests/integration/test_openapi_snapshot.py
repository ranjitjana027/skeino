"""Wire-contract snapshot: the served OpenAPI schema must not drift silently.

skeino's product *is* its HTTP surface; an accidental route, schema, or
status-code change is a regression even when every behavioural test still
passes. This test pins the full ``/openapi.json`` document to a checked-in
snapshot, so any contract change has to be made consciously — by regenerating
the snapshot in the same PR — and shows reviewers an exact diff of the change:

    UPDATE_SNAPSHOTS=1 poetry run pytest tests/integration/test_openapi_snapshot.py
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from tests.conftest import build_test_app

SNAPSHOT_PATH = Path(__file__).parent / "snapshots" / "openapi.json"


def _served_schema() -> dict[str, Any]:
    app, _ = build_test_app()
    with TestClient(app) as client:
        response = client.get("/openapi.json")
    assert response.status_code == 200
    schema: dict[str, Any] = response.json()
    return schema


def test_openapi_schema_matches_snapshot() -> None:
    schema = _served_schema()

    if os.environ.get("UPDATE_SNAPSHOTS"):
        SNAPSHOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        SNAPSHOT_PATH.write_text(
            json.dumps(schema, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )

    assert SNAPSHOT_PATH.is_file(), (
        "OpenAPI snapshot missing. Generate it with: "
        "UPDATE_SNAPSHOTS=1 poetry run pytest "
        "tests/integration/test_openapi_snapshot.py"
    )
    expected = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))
    assert schema == expected, (
        "Served OpenAPI schema differs from tests/integration/snapshots/"
        "openapi.json. If the wire-contract change is intentional, regenerate "
        "the snapshot in this PR with: UPDATE_SNAPSHOTS=1 poetry run pytest "
        "tests/integration/test_openapi_snapshot.py"
    )


def test_openapi_schema_is_deterministic() -> None:
    """Guard the snapshot itself: two fresh apps must serve identical schemas.

    If schema generation ever becomes order- or state-dependent, the snapshot
    test above would flake; fail loudly here instead.
    """
    assert _served_schema() == _served_schema()
