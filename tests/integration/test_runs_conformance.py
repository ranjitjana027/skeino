"""Run endpoint conformance against real LangGraph graphs.

Covers the thread-scoped and stateless run routes in
``docs/api-reference/http.md``: background create + join, wait, list/get/
delete, every ``multitask_strategy`` against a genuinely busy thread (the
``gated`` graph holds a run open until the test releases it), cancel with
both actions, interrupt + resume, graph failures, and stateless/batch runs
leaving no thread behind. The contract is the docs plus upstream LangGraph
server behaviour. Known gaps are strict xfails tied to their issues.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.real_graphs import (
    ASSISTANT_ID,
    gate,
    real_client,
    user_input,
)

MISSING = "00000000-0000-0000-0000-000000000000"
RUN_FIELDS = {
    "run_id",
    "thread_id",
    "assistant_id",
    "created_at",
    "updated_at",
    "status",
    "metadata",
    "kwargs",
    "multitask_strategy",
}

XFAIL_129 = pytest.mark.xfail(
    strict=True, reason="#129: rollback/delete keeps the run's checkpoints"
)


@pytest.fixture
def client() -> Iterator[TestClient]:
    with real_client("echo") as c:
        yield c


@pytest.fixture
def gated() -> Iterator[TestClient]:
    with real_client("gated") as c:
        yield c


def _thread(client: TestClient) -> str:
    response = client.post("/threads", json={})
    assert response.status_code == 200, response.text
    return str(response.json()["thread_id"])


def _start(client: TestClient, thread_id: str, text: str = "hi", **body: Any) -> Any:
    response = client.post(
        f"/threads/{thread_id}/runs",
        json={"assistant_id": ASSISTANT_ID, "input": user_input(text), **body},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _wait(client: TestClient, thread_id: str, text: str = "hi", **body: Any) -> Any:
    return client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": ASSISTANT_ID, "input": user_input(text), **body},
    )


def _run(client: TestClient, thread_id: str, run_id: str) -> Any:
    return client.get(f"/threads/{thread_id}/runs/{run_id}")


def _until_status(
    client: TestClient, thread_id: str, run_id: str, want: str, timeout: float = 3.0
) -> None:
    """Poll the run until it reaches ``want``; fail loud on timeout."""
    deadline = time.monotonic() + timeout
    status = None
    while time.monotonic() < deadline:
        status = _run(client, thread_id, run_id).json()["status"]
        if status == want:
            return
        time.sleep(0.01)
    pytest.fail(f"run {run_id} stuck in {status!r}, wanted {want!r}")


def _busy(client: TestClient, text: str = "one", **body: Any) -> tuple[str, str]:
    """A thread whose run is in flight (call inside ``gate()``)."""
    thread_id = _thread(client)
    run_id = _start(client, thread_id, text, **body)["run_id"]
    _until_status(client, thread_id, run_id, "running")
    return thread_id, run_id


def _messages(client: TestClient, thread_id: str) -> list[str]:
    values = client.get(f"/threads/{thread_id}/state").json()["values"]
    return [m["content"] for m in values.get("messages", [])]


def _thread_count(client: TestClient) -> int:
    return len(client.post("/threads/search", json={"limit": 1000}).json())


# --- background create / join / wait ---------------------------------------


def test_background_run_is_pending_then_joins_to_final_values(
    client: TestClient,
) -> None:
    thread_id = _thread(client)
    response = client.post(
        f"/threads/{thread_id}/runs",
        json={"assistant_id": ASSISTANT_ID, "input": user_input()},
    )
    run = response.json()
    assert RUN_FIELDS <= set(run)
    assert run["status"] in {"pending", "running"}
    assert run["thread_id"] == thread_id
    assert response.headers["location"] == f"/threads/{thread_id}/runs/{run['run_id']}"

    joined = client.get(f"/threads/{thread_id}/runs/{run['run_id']}/join")
    assert joined.status_code == 200
    assert [m["content"] for m in joined.json()["messages"]] == ["hi", "echo: hi"]
    assert _run(client, thread_id, run["run_id"]).json()["status"] == "success"


def test_wait_returns_final_values_and_records_a_successful_run(
    client: TestClient,
) -> None:
    thread_id = _thread(client)
    response = _wait(client, thread_id, "ping")
    assert response.status_code == 200
    assert [m["content"] for m in response.json()["messages"]] == [
        "ping",
        "echo: ping",
    ]
    (run,) = client.get(f"/threads/{thread_id}/runs").json()
    assert run["status"] == "success"
    assert client.get(f"/threads/{thread_id}").json()["status"] == "idle"


def test_run_metadata_and_default_multitask_strategy(client: TestClient) -> None:
    thread_id = _thread(client)
    run = _start(client, thread_id, metadata={"source": "test"})
    stored = _run(client, thread_id, run["run_id"]).json()
    assert stored["metadata"] == {"source": "test"}
    # Upstream default (langgraph_api/models/run.py: ``or "enqueue"``).
    assert stored["multitask_strategy"] == "enqueue"


def test_runs_accumulate_state_across_turns(client: TestClient) -> None:
    thread_id = _thread(client)
    _wait(client, thread_id, "one")
    _wait(client, thread_id, "two")
    assert _messages(client, thread_id) == ["one", "echo: one", "two", "echo: two"]


# --- missing resources -----------------------------------------------------


def test_run_on_missing_thread_is_404_by_default(client: TestClient) -> None:
    response = _wait(client, MISSING)
    assert response.status_code == 404
    assert response.json() == {"detail": f"Thread {MISSING} not found."}


def test_if_not_exists_create_makes_the_thread(client: TestClient) -> None:
    response = _wait(client, MISSING, if_not_exists="create")
    assert response.status_code == 200
    assert client.get(f"/threads/{MISSING}").status_code == 200
    assert _messages(client, MISSING) == ["hi", "echo: hi"]


def test_unknown_assistant_is_404(client: TestClient) -> None:
    response = client.post(
        f"/threads/{_thread(client)}/runs/wait",
        json={"assistant_id": "nope", "input": user_input()},
    )
    assert response.status_code == 404


def test_missing_run_is_404(client: TestClient) -> None:
    thread_id = _thread(client)
    for response in (
        _run(client, thread_id, MISSING),
        client.post(f"/threads/{thread_id}/runs/{MISSING}/cancel"),
        client.delete(f"/threads/{thread_id}/runs/{MISSING}"),
    ):
        assert response.status_code == 404


# --- list / delete ---------------------------------------------------------


def test_list_runs_newest_first_with_paging_and_status(client: TestClient) -> None:
    thread_id = _thread(client)
    for text in ("a", "b", "c", "d"):
        _wait(client, thread_id, text)
    runs = client.get(f"/threads/{thread_id}/runs").json()
    created = [r["created_at"] for r in runs]
    assert len(runs) == 4
    assert created == sorted(created, reverse=True)

    first = client.get(f"/threads/{thread_id}/runs", params={"limit": 2}).json()
    rest = client.get(
        f"/threads/{thread_id}/runs", params={"limit": 2, "offset": 2}
    ).json()
    assert [r["run_id"] for r in first + rest] == [r["run_id"] for r in runs]

    by_status = {
        status: len(
            client.get(f"/threads/{thread_id}/runs", params={"status": status}).json()
        )
        for status in ("success", "pending", "error")
    }
    assert by_status == {"success": 4, "pending": 0, "error": 0}
    assert (
        client.get(f"/threads/{thread_id}/runs", params={"limit": 0}).status_code == 422
    )


def test_delete_terminal_run_removes_the_row(client: TestClient) -> None:
    thread_id = _thread(client)
    run = _start(client, thread_id)
    client.get(f"/threads/{thread_id}/runs/{run['run_id']}/join")
    assert (
        client.delete(f"/threads/{thread_id}/runs/{run['run_id']}").status_code == 204
    )
    assert _run(client, thread_id, run["run_id"]).status_code == 404
    assert client.get(f"/threads/{thread_id}/runs").json() == []


def test_delete_active_run_is_409(gated: TestClient) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        assert gated.delete(f"/threads/{thread_id}/runs/{run_id}").status_code == 409
        assert _run(gated, thread_id, run_id).json()["status"] == "running"


@XFAIL_129
def test_delete_run_removes_its_checkpoints(client: TestClient) -> None:
    thread_id = _thread(client)
    _wait(client, thread_id, "one")
    run = _start(client, thread_id, "two")
    client.get(f"/threads/{thread_id}/runs/{run['run_id']}/join")
    client.delete(f"/threads/{thread_id}/runs/{run['run_id']}")
    assert _messages(client, thread_id) == ["one", "echo: one"]


# --- multitask strategies on a busy thread ---------------------------------


def test_thread_is_busy_while_a_run_is_in_flight(gated: TestClient) -> None:
    with gate() as release:
        thread_id, run_id = _busy(gated)
        assert gated.get(f"/threads/{thread_id}").json()["status"] == "busy"
        release.set()
        _until_status(gated, thread_id, run_id, "success")
    assert gated.get(f"/threads/{thread_id}").json()["status"] == "idle"


def test_multitask_reject_is_409_and_leaves_first_run(gated: TestClient) -> None:
    with gate() as release:
        thread_id, first = _busy(gated)
        response = gated.post(
            f"/threads/{thread_id}/runs",
            json={
                "assistant_id": ASSISTANT_ID,
                "input": user_input("two"),
                "multitask_strategy": "reject",
            },
        )
        assert response.status_code == 409
        assert _run(gated, thread_id, first).json()["status"] == "running"
        release.set()
        _until_status(gated, thread_id, first, "success")
    assert _messages(gated, thread_id) == ["one", "echo: one"]
    assert len(gated.get(f"/threads/{thread_id}/runs").json()) == 1


def test_multitask_enqueue_runs_after_the_first(gated: TestClient) -> None:
    with gate() as release:
        thread_id, first = _busy(gated)
        second = _start(gated, thread_id, "two", multitask_strategy="enqueue")
        assert second["status"] == "pending"
        assert _run(gated, thread_id, first).json()["status"] == "running"
        release.set()
        gated.get(f"/threads/{thread_id}/runs/{second['run_id']}/join")
    assert _run(gated, thread_id, first).json()["status"] == "success"
    assert _messages(gated, thread_id) == ["one", "echo: one", "two", "echo: two"]


def test_multitask_interrupt_stops_the_first_and_runs_the_second(
    gated: TestClient,
) -> None:
    with gate() as release:
        thread_id, first = _busy(gated)
        second = _start(gated, thread_id, "two", multitask_strategy="interrupt")
        _until_status(gated, thread_id, first, "interrupted")
        release.set()
        gated.get(f"/threads/{thread_id}/runs/{second['run_id']}/join")
    assert _run(gated, thread_id, second["run_id"]).json()["status"] == "success"
    # Interrupt keeps what the first run already wrote (upstream semantics).
    assert _messages(gated, thread_id) == ["one", "two", "echo: two"]


def test_multitask_rollback_deletes_the_first_run(gated: TestClient) -> None:
    with gate() as release:
        thread_id, first = _busy(gated)
        second = _start(gated, thread_id, "two", multitask_strategy="rollback")
        release.set()
        gated.get(f"/threads/{thread_id}/runs/{second['run_id']}/join")
    assert _run(gated, thread_id, first).status_code == 404
    assert [r["run_id"] for r in gated.get(f"/threads/{thread_id}/runs").json()] == [
        second["run_id"]
    ]


@XFAIL_129
def test_multitask_rollback_discards_the_first_runs_state(gated: TestClient) -> None:
    with gate() as release:
        thread_id, _ = _busy(gated)
        second = _start(gated, thread_id, "two", multitask_strategy="rollback")
        release.set()
        gated.get(f"/threads/{thread_id}/runs/{second['run_id']}/join")
    assert _messages(gated, thread_id) == ["two", "echo: two"]


# --- cancel ----------------------------------------------------------------


def test_cancel_interrupt_marks_run_interrupted(gated: TestClient) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "interrupt", "wait": "true"},
        )
        assert _run(gated, thread_id, run_id).json()["status"] == "interrupted"
        assert gated.get(f"/threads/{thread_id}").json()["status"] == "idle"


def test_cancel_rollback_deletes_the_run(gated: TestClient) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "rollback", "wait": "true"},
        )
        assert _run(gated, thread_id, run_id).status_code == 404
        assert gated.get(f"/threads/{thread_id}").json()["status"] == "idle"


@XFAIL_129
def test_cancel_rollback_discards_checkpoints_written_mid_run(
    gated: TestClient,
) -> None:
    thread_id = _thread(gated)
    _wait(gated, thread_id, "one")
    with gate():
        run_id = _start(gated, thread_id, "two", durability="async")["run_id"]
        _until_status(gated, thread_id, run_id, "running")
        gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "rollback", "wait": "true"},
        )
    assert _messages(gated, thread_id) == ["one", "echo: one"]


@pytest.mark.xfail(strict=True, reason="#129: cancel without wait is 204, not 202")
def test_cancel_without_wait_is_accepted(gated: TestClient) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        response = gated.post(f"/threads/{thread_id}/runs/{run_id}/cancel")
        assert response.status_code == 202


@pytest.mark.xfail(
    strict=True, reason="#129: cancel wait=true is 204, not the run's final body"
)
def test_cancel_with_wait_returns_final_body_and_join_location(
    gated: TestClient,
) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        response = gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "interrupt", "wait": "true"},
        )
    assert response.status_code == 200
    assert response.headers["location"] == f"/threads/{thread_id}/runs/{run_id}/join"
    assert "messages" in response.json()


def test_join_after_cancel_returns_the_threads_values(gated: TestClient) -> None:
    with gate():
        thread_id, run_id = _busy(gated)
        gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "interrupt", "wait": "true"},
        )
        joined = gated.get(f"/threads/{thread_id}/runs/{run_id}/join")
    assert joined.status_code == 200
    assert "messages" in joined.json()


# --- interrupt + resume ----------------------------------------------------


def test_interrupt_then_resume_with_command() -> None:
    with real_client("interrupting") as client:
        thread_id = _thread(client)
        paused = _wait(client, thread_id).json()
        assert [i["value"] for i in paused["__interrupt__"]] == ["approve?"]
        (run,) = client.get(f"/threads/{thread_id}/runs").json()
        assert run["status"] == "success"  # pausing is not a failure upstream
        assert client.get(f"/threads/{thread_id}").json()["status"] == "interrupted"

        resumed = client.post(
            f"/threads/{thread_id}/runs/wait",
            json={"assistant_id": ASSISTANT_ID, "command": {"resume": "yes"}},
        ).json()
        assert "__interrupt__" not in resumed
        assert resumed["messages"][-1]["content"] == "approved: yes"
        assert client.get(f"/threads/{thread_id}").json()["status"] == "idle"
        assert len(client.get(f"/threads/{thread_id}/runs").json()) == 2


# --- failures --------------------------------------------------------------


def test_failing_graph_marks_run_and_thread_error() -> None:
    with real_client("failing") as client:
        thread_id = _thread(client)
        response = _wait(client, thread_id)
        # Only "the caller is told it failed": the error body's shape and
        # wording are deliberately not part of this contract.
        assert response.status_code >= 400 or "__error__" in response.json()
        (run,) = client.get(f"/threads/{thread_id}/runs").json()
        assert run["status"] == "error"
        assert client.get(f"/threads/{thread_id}").json()["status"] == "error"


def test_failing_background_run_is_error_after_join() -> None:
    with real_client("failing") as client:
        thread_id = _thread(client)
        run_id = _start(client, thread_id)["run_id"]
        client.get(f"/threads/{thread_id}/runs/{run_id}/join")
        assert _run(client, thread_id, run_id).json()["status"] == "error"


def test_thread_accepts_runs_after_a_cancelled_one(gated: TestClient) -> None:
    # A cancel must not wedge the thread: the next run on it still works.
    thread_id = _thread(gated)
    with gate() as release:
        run_id = _start(gated, thread_id, "one")["run_id"]
        _until_status(gated, thread_id, run_id, "running")
        gated.post(
            f"/threads/{thread_id}/runs/{run_id}/cancel",
            params={"action": "interrupt", "wait": "true"},
        )
        release.set()
    assert _wait(gated, thread_id, "two").status_code == 200
    assert _messages(gated, thread_id)[-1] == "echo: two"


# --- stateless -------------------------------------------------------------


def test_stateless_run_returns_run_model_and_keeps_no_thread(
    client: TestClient,
) -> None:
    before = _thread_count(client)
    response = client.post(
        "/runs", json={"assistant_id": ASSISTANT_ID, "input": user_input()}
    )
    assert response.status_code == 200
    assert RUN_FIELDS <= set(response.json())
    assert response.json()["status"] == "success"
    assert "location" not in response.headers
    assert _thread_count(client) == before


def test_stateless_wait_returns_values_and_keeps_no_thread(
    client: TestClient,
) -> None:
    before = _thread_count(client)
    response = client.post(
        "/runs/wait", json={"assistant_id": ASSISTANT_ID, "input": user_input("x")}
    )
    assert [m["content"] for m in response.json()["messages"]] == ["x", "echo: x"]
    assert _thread_count(client) == before


def test_batch_runs_each_payload_in_order_on_its_own_thread(
    client: TestClient,
) -> None:
    before = _thread_count(client)
    payloads = [
        {"assistant_id": ASSISTANT_ID, "input": user_input(text)}
        for text in ("a", "b", "c")
    ]
    outputs = client.post("/runs/batch", json=payloads).json()
    # Each output holds only its own turn: no state leaks between payloads.
    assert [[m["content"] for m in o["messages"]] for o in outputs] == [
        ["a", "echo: a"],
        ["b", "echo: b"],
        ["c", "echo: c"],
    ]
    assert _thread_count(client) == before
