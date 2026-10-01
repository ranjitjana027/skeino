"""Endpoint matrix: ``docs/api-reference/http.md`` is the route contract.

Every route row in the doc's tables is checked three ways:

* the docs, the app's OpenAPI schema and this matrix list the **same** routes
  (an undocumented route, a stale doc row, or a route with no matrix entry
  fails here rather than drifting silently);
* a happy-path request against a real graph returns the **documented** status
  (``204`` where the doc says so, ``200`` otherwise — see its Conventions);
* every route taking a ``{thread_id}``/``{assistant_id}``/``{run_id}`` answers
  an unknown id with **404**, per the doc's Conventions.

Behaviour per route lives in the ``*_conformance`` suites; this file only
pins the surface.
"""

from __future__ import annotations

import re
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from tests.real_graphs import (
    ASSISTANT_ID,
    gate,
    real_client,
    user_input,
    wait_until_gated,
)

DOC = Path(__file__).parents[2] / "docs" / "api-reference" / "http.md"
ROW = re.compile(r"^\| `(GET|POST|PUT|PATCH|DELETE)` \| `([^`]+)` \|(.*)$")
MISSING = "00000000-0000-0000-0000-000000000000"
RUN = {"assistant_id": ASSISTANT_ID, "input": user_input()}

Route = tuple[str, str]


def _documented() -> dict[Route, int]:
    """``(method, path) -> documented success status`` from the doc's tables."""
    rows: dict[Route, int] = {}
    for line in DOC.read_text().splitlines():
        if match := ROW.match(line):
            method, path, rest = match.groups()
            rows[(method, path)] = 204 if "| `204` |" in rest else 200
    return rows


DOCUMENTED = _documented()


@dataclass(frozen=True)
class World:
    """A thread with one finished run, so every id in a path resolves."""

    client: TestClient
    thread_id: str
    run_id: str
    checkpoint_id: str


@pytest.fixture
def world() -> Iterator[World]:
    # ``gated`` is open by default, so it behaves like ``echo`` until a case
    # closes the gate to get an in-flight run.
    with real_client("gated") as client:
        thread_id = client.post("/threads", json={}).json()["thread_id"]
        run_id = client.post(f"/threads/{thread_id}/runs", json=RUN).json()["run_id"]
        client.get(f"/threads/{thread_id}/runs/{run_id}/join")
        state = client.get(f"/threads/{thread_id}/state").json()
        yield World(client, thread_id, run_id, state["checkpoint"]["checkpoint_id"])


def _cancel_in_flight(w: World) -> httpx.Response:
    with gate():
        thread_id = w.client.post("/threads", json={}).json()["thread_id"]
        run_id = w.client.post(f"/threads/{thread_id}/runs", json=RUN).json()["run_id"]
        wait_until_gated()
        return w.client.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel", params={"wait": "true"}
        )


T = "/threads/{thread_id}"

# One happy-path request per documented route.
HAPPY: dict[Route, Callable[[World], httpx.Response]] = {
    ("GET", "/api/health"): lambda w: w.client.get("/api/health"),
    ("GET", "/info"): lambda w: w.client.get("/info"),
    ("GET", "/api/initial-message"): lambda w: w.client.get("/api/initial-message"),
    ("POST", "/assistants/search"): lambda w: w.client.post(
        "/assistants/search", json={}
    ),
    ("GET", "/assistants/{assistant_id}"): lambda w: w.client.get(
        f"/assistants/{ASSISTANT_ID}"
    ),
    ("GET", "/assistants/{assistant_id}/schemas"): lambda w: w.client.get(
        f"/assistants/{ASSISTANT_ID}/schemas"
    ),
    ("GET", "/assistants/{assistant_id}/graph"): lambda w: w.client.get(
        f"/assistants/{ASSISTANT_ID}/graph"
    ),
    ("GET", "/assistants/{assistant_id}/subgraphs"): lambda w: w.client.get(
        f"/assistants/{ASSISTANT_ID}/subgraphs"
    ),
    ("POST", "/threads"): lambda w: w.client.post("/threads", json={}),
    ("POST", "/threads/search"): lambda w: w.client.post("/threads/search", json={}),
    ("GET", T): lambda w: w.client.get(f"/threads/{w.thread_id}"),
    ("PATCH", T): lambda w: w.client.patch(
        f"/threads/{w.thread_id}", json={"metadata": {"k": "v"}}
    ),
    ("DELETE", T): lambda w: w.client.delete(f"/threads/{w.thread_id}"),
    ("POST", f"{T}/copy"): lambda w: w.client.post(f"/threads/{w.thread_id}/copy"),
    ("GET", f"{T}/state"): lambda w: w.client.get(f"/threads/{w.thread_id}/state"),
    ("POST", f"{T}/state"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/state",
        json={"values": {"messages": [{"role": "ai", "content": "edited"}]}},
    ),
    ("GET", f"{T}/state/{{checkpoint_id}}"): lambda w: w.client.get(
        f"/threads/{w.thread_id}/state/{w.checkpoint_id}"
    ),
    ("POST", f"{T}/state/checkpoint"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/state/checkpoint",
        json={"checkpoint_id": w.checkpoint_id},
    ),
    ("GET", f"{T}/history"): lambda w: w.client.get(f"/threads/{w.thread_id}/history"),
    ("POST", f"{T}/history"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/history", json={"limit": 2}
    ),
    ("POST", f"{T}/runs"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/runs", json=RUN
    ),
    ("POST", f"{T}/runs/wait"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/runs/wait", json=RUN
    ),
    ("POST", f"{T}/runs/stream"): lambda w: w.client.post(
        f"/threads/{w.thread_id}/runs/stream", json=RUN
    ),
    ("GET", f"{T}/runs"): lambda w: w.client.get(f"/threads/{w.thread_id}/runs"),
    ("GET", f"{T}/runs/{{run_id}}"): lambda w: w.client.get(
        f"/threads/{w.thread_id}/runs/{w.run_id}"
    ),
    ("GET", f"{T}/runs/{{run_id}}/join"): lambda w: w.client.get(
        f"/threads/{w.thread_id}/runs/{w.run_id}/join"
    ),
    ("POST", f"{T}/runs/{{run_id}}/cancel"): _cancel_in_flight,
    ("DELETE", f"{T}/runs/{{run_id}}"): lambda w: w.client.delete(
        f"/threads/{w.thread_id}/runs/{w.run_id}"
    ),
    ("POST", "/runs"): lambda w: w.client.post("/runs", json=RUN),
    ("POST", "/runs/wait"): lambda w: w.client.post("/runs/wait", json=RUN),
    ("POST", "/runs/stream"): lambda w: w.client.post("/runs/stream", json=RUN),
    ("POST", "/runs/batch"): lambda w: w.client.post("/runs/batch", json=[RUN, RUN]),
}

STREAMING = {("POST", f"{T}/runs/stream"), ("POST", "/runs/stream")}


def _openapi_routes(client: TestClient) -> dict[Route, set[int]]:
    paths = client.get("/openapi.json").json()["paths"]
    return {
        (method.upper(), path): {int(code) for code in op["responses"]}
        for path, ops in paths.items()
        for method, op in ops.items()
    }


# --- surface agreement -----------------------------------------------------


def test_doc_lists_every_route() -> None:
    # Guards the parser too: a table-format change must not silently empty it.
    assert len(DOCUMENTED) >= 30


def test_docs_and_openapi_list_the_same_routes(world: World) -> None:
    served = set(_openapi_routes(world.client))
    assert served - set(DOCUMENTED) == set(), "served but undocumented"
    assert set(DOCUMENTED) - served == set(), "documented but not served"


def test_openapi_advertises_the_documented_success_status(world: World) -> None:
    served = _openapi_routes(world.client)
    wrong = {
        route: (status, served[route])
        for route, status in DOCUMENTED.items()
        if status not in served.get(route, set())
    }
    assert wrong == {}


def test_matrix_covers_every_documented_route() -> None:
    assert set(HAPPY) == set(DOCUMENTED)


# --- documented status per route -------------------------------------------


# ``tests/real_graphs.py`` states are ``typing.TypedDict``, which pydantic cannot
# schema on Python < 3.12; skeino 500s instead of degrading that schema (#133).
_PY311_SCHEMA_GAP = pytest.mark.xfail(
    sys.version_info < (3, 12),
    strict=True,
    reason="#133: a failing schema 500s the route (typing.TypedDict on py<3.12)",
)
_SCHEMA_ROUTES = {
    ("GET", "/assistants/{assistant_id}/schemas"),
    ("GET", "/assistants/{assistant_id}/subgraphs"),
}
_STATUS_CASES = [
    pytest.param(route, marks=_PY311_SCHEMA_GAP, id=" ".join(route))
    if route in _SCHEMA_ROUTES
    else pytest.param(route, id=" ".join(route))
    for route in sorted(DOCUMENTED)
]


@pytest.mark.parametrize("route", _STATUS_CASES)
def test_route_returns_documented_status(route: Route, world: World) -> None:
    response = HAPPY[route](world)
    assert response.status_code == DOCUMENTED[route], response.text
    if route in STREAMING:
        assert response.headers["content-type"].startswith("text/event-stream")
        assert "event: end" in response.text
    elif response.status_code == 200:
        response.json()  # a JSON body, not an error page


# --- unknown ids -----------------------------------------------------------

_ID_ROUTES = sorted(
    route
    for route in DOCUMENTED
    if any(p in route[1] for p in ("{thread_id}", "{assistant_id}", "{run_id}"))
)


def _missing_request(
    route: Route, client: TestClient, thread_id: str
) -> httpx.Response:
    method, path = route
    # Unknown thread everywhere a thread is named; on thread-scoped run routes
    # a real thread with an unknown run, so the run lookup itself is exercised.
    run_scoped = "{run_id}" in path
    url = (
        path.replace("{thread_id}", thread_id if run_scoped else MISSING)
        .replace("{run_id}", MISSING)
        .replace("{assistant_id}", "no-such-assistant")
        .replace("{checkpoint_id}", MISSING)
    )
    body: object = None
    if method in {"POST", "PATCH"}:
        if path.endswith(("/runs", "/runs/wait", "/runs/stream")):
            body = RUN
        elif path.endswith("/state/checkpoint"):
            body = {"checkpoint_id": MISSING}
        elif path.endswith("/state"):
            body = {"values": {}}
        else:
            body = {}
    return client.request(method, url, json=body)


@pytest.mark.parametrize("route", _ID_ROUTES, ids=" ".join)
def test_unknown_id_is_404(route: Route, world: World) -> None:
    response = _missing_request(route, world.client, world.thread_id)
    assert response.status_code == 404, response.text
    assert response.json()["detail"]


@pytest.mark.xfail(strict=True, reason="#129: cancel of a finished run is 409, not 404")
def test_cancel_of_a_finished_run_is_404(world: World) -> None:
    # Upstream: "No matching runs to cancel" — a finished run is not cancellable.
    response = world.client.post(
        f"/threads/{world.thread_id}/runs/{world.run_id}/cancel"
    )
    assert response.status_code == 404
