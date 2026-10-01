"""Thread endpoint conformance against real LangGraph graphs.

Covers every thread route in ``docs/api-reference/http.md`` (create, search,
get, patch, delete, copy, state read/write/time-travel, history) with enough
data to exercise the edges: pagination beyond one page, sort ties, nested
filters, multi-run history. The contract is the docs plus upstream LangGraph
server behaviour. Known gaps are strict xfails tied to their issues.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from tests.real_graphs import ASSISTANT_ID, real_client, user_input

MISSING = "00000000-0000-0000-0000-000000000000"


@pytest.fixture
def client() -> Any:
    with real_client("echo") as c:
        yield c


def _thread(client: TestClient, **body: Any) -> dict[str, Any]:
    response = client.post("/threads", json=body)
    assert response.status_code == 200, response.text
    return dict(response.json())


def _run(client: TestClient, thread_id: str, text: str = "hi", **body: Any) -> Any:
    response = client.post(
        f"/threads/{thread_id}/runs/wait",
        json={"assistant_id": ASSISTANT_ID, "input": user_input(text), **body},
    )
    assert response.status_code == 200, response.text
    return response.json()


def _search(client: TestClient, **body: Any) -> list[dict[str, Any]]:
    response = client.post("/threads/search", json=body)
    assert response.status_code == 200, response.text
    return list(response.json())


def _ids(threads: list[dict[str, Any]]) -> list[str]:
    return [t["thread_id"] for t in threads]


# --- create ----------------------------------------------------------------


def test_create_with_explicit_id_and_metadata(client: TestClient) -> None:
    thread = _thread(client, thread_id=MISSING, metadata={"team": "a"})
    assert thread["thread_id"] == MISSING
    assert thread["metadata"] == {"team": "a"}
    assert thread["status"] == "idle"
    assert client.get(f"/threads/{MISSING}").json()["metadata"] == {"team": "a"}


def test_create_existing_id_conflicts_by_default(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    response = client.post("/threads", json={"thread_id": thread_id})
    assert response.status_code == 409
    assert thread_id in response.json()["detail"]


def test_create_existing_id_do_nothing_returns_original(client: TestClient) -> None:
    thread_id = _thread(client, metadata={"v": 1})["thread_id"]
    again = _thread(
        client, thread_id=thread_id, if_exists="do_nothing", metadata={"v": 2}
    )
    assert again["thread_id"] == thread_id
    assert again["metadata"] == {"v": 1}


def test_create_with_supersteps_seeds_state(client: TestClient) -> None:
    seeded = {"messages": [{"role": "user", "content": "seeded"}]}
    thread = _thread(
        client,
        # A node-attributed update writes state directly. (``__input__`` only
        # queues input for START; LangGraph itself leaves state empty then.)
        supersteps=[{"updates": [{"values": seeded, "as_node": "reply"}]}],
    )
    state = client.get(f"/threads/{thread['thread_id']}/state").json()
    assert [m["content"] for m in state["values"]["messages"]] == ["seeded"]


# --- search ----------------------------------------------------------------


def test_search_pages_through_every_thread_exactly_once(client: TestClient) -> None:
    created = {_thread(client)["thread_id"] for _ in range(7)}
    seen: list[str] = []
    for offset in range(0, 9, 3):
        seen += _ids(_search(client, limit=3, offset=offset))
    assert len(seen) == len(set(seen)) == 7
    assert set(seen) == created


def test_search_offset_past_end_is_empty(client: TestClient) -> None:
    _thread(client)
    assert _search(client, offset=50) == []


def test_search_by_ids_respects_limit(client: TestClient) -> None:
    ids = [_thread(client)["thread_id"] for _ in range(4)]
    _thread(client)  # not requested
    found = _ids(_search(client, ids=ids, limit=10))
    assert sorted(found) == sorted(ids)
    assert len(_search(client, ids=ids, limit=2)) == 2


def test_search_by_status() -> None:
    with real_client("interrupting") as client:
        idle = _thread(client)["thread_id"]
        paused = _thread(client)["thread_id"]
        _run(client, paused)
        assert _ids(_search(client, status="interrupted")) == [paused]
        assert _ids(_search(client, status="idle")) == [idle]


def test_search_metadata_match_on_first_page(client: TestClient) -> None:
    wanted = _thread(client, metadata={"team": "a"})["thread_id"]
    _thread(client, metadata={"team": "b"})
    assert _ids(_search(client, metadata={"team": "a"})) == [wanted]


@pytest.mark.xfail(strict=True, reason="#112: metadata filtered after pagination")
def test_search_metadata_match_beyond_first_page(client: TestClient) -> None:
    wanted = {_thread(client, metadata={"team": "a"})["thread_id"] for _ in range(5)}
    for _ in range(5):
        _thread(client, metadata={"team": "b"})  # newer, fills page one
    assert set(_ids(_search(client, metadata={"team": "a"}, limit=5))) == wanted


@pytest.mark.xfail(strict=True, reason="#112: metadata filter is not containment")
def test_search_metadata_nested_containment(client: TestClient) -> None:
    wanted = _thread(client, metadata={"owner": {"id": "u1", "org": "x"}})["thread_id"]
    assert _ids(_search(client, metadata={"owner": {"id": "u1"}})) == [wanted]


@pytest.mark.xfail(
    strict=True, reason="#112: values filter is exact-match, not containment"
)
def test_search_by_values(client: TestClient) -> None:
    hit, miss = _thread(client)["thread_id"], _thread(client)["thread_id"]
    _run(client, hit, "needle")
    _run(client, miss, "hay")
    found = _ids(_search(client, values={"messages": [{"content": "needle"}]}))
    assert found == [hit]


def test_search_sort_by_updated_at(client: TestClient) -> None:
    first, second, third = (_thread(client)["thread_id"] for _ in range(3))
    client.patch(f"/threads/{first}", json={"metadata": {"touched": True}})
    ids = [first, second, third]
    newest_first = _ids(
        _search(client, ids=ids, sort_by="updated_at", sort_order="desc")
    )
    assert newest_first == [first, third, second]
    oldest_first = _ids(
        _search(client, ids=ids, sort_by="updated_at", sort_order="asc")
    )
    assert oldest_first == [second, third, first]


@pytest.mark.xfail(strict=True, reason="#112: in-memory store ignores sort_by")
def test_search_sort_by_created_at(client: TestClient) -> None:
    first, second, third = (_thread(client)["thread_id"] for _ in range(3))
    client.patch(f"/threads/{first}", json={"metadata": {"touched": True}})
    found = _ids(
        _search(
            client, ids=[first, second, third], sort_by="created_at", sort_order="asc"
        )
    )
    assert found == [first, second, third]


@pytest.mark.xfail(strict=True, reason="#127: search select ignored")
def test_search_select_limits_fields(client: TestClient) -> None:
    _thread(client)
    (thread,) = _search(client, select=["thread_id", "status"])
    assert set(thread) == {"thread_id", "status"}


def test_search_rejects_out_of_range_limit(client: TestClient) -> None:
    assert client.post("/threads/search", json={"limit": 0}).status_code == 422
    assert client.post("/threads/search", json={"limit": 1001}).status_code == 422


# --- get / patch / delete --------------------------------------------------


def test_get_returns_latest_values_after_run(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id, "ping")
    thread = client.get(f"/threads/{thread_id}").json()
    assert thread["values"]["messages"][-1]["content"] == "echo: ping"
    assert thread["state_updated_at"] is not None


@pytest.mark.parametrize(
    "path",
    ["", "/state", "/history", "/runs"],
    ids=["thread", "state", "history", "runs"],
)
def test_unknown_thread_is_404_with_detail(client: TestClient, path: str) -> None:
    response = client.get(f"/threads/{MISSING}{path}")
    assert response.status_code == 404
    assert response.json() == {"detail": f"Thread {MISSING} not found."}


def test_patch_replaces_metadata_and_bumps_updated_at(client: TestClient) -> None:
    thread = _thread(client, metadata={"a": 1})
    patched = client.patch(
        f"/threads/{thread['thread_id']}", json={"metadata": {"a": 2}}
    ).json()
    assert patched["metadata"]["a"] == 2
    assert patched["updated_at"] > thread["updated_at"]


@pytest.mark.xfail(strict=True, reason="#114: PATCH replaces instead of merging")
def test_patch_merges_metadata(client: TestClient) -> None:
    thread_id = _thread(client, metadata={"a": 1, "b": 1})["thread_id"]
    patched = client.patch(f"/threads/{thread_id}", json={"metadata": {"b": 2}}).json()
    assert patched["metadata"] == {"a": 1, "b": 2}


@pytest.mark.xfail(strict=True, reason="#114: graph_id/assistant_id not stamped")
def test_run_stamps_graph_and_assistant_on_thread_metadata(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    metadata = client.get(f"/threads/{thread_id}").json()["metadata"]
    assert metadata["graph_id"] == ASSISTANT_ID
    assert metadata["assistant_id"] == ASSISTANT_ID
    assert _ids(_search(client, metadata={"graph_id": ASSISTANT_ID})) == [thread_id]


def test_delete_removes_thread_state_and_runs(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    assert client.delete(f"/threads/{thread_id}").status_code == 204
    for path in ("", "/state", "/runs"):
        assert client.get(f"/threads/{thread_id}{path}").status_code == 404
    assert thread_id not in _ids(_search(client))
    # The 404s above only prove the row is gone; recreating the id proves the
    # checkpoints and run rows went with it.
    _thread(client, thread_id=thread_id)
    assert client.get(f"/threads/{thread_id}/state").json()["values"] in ({}, None)
    assert client.get(f"/threads/{thread_id}/runs").json() == []


# --- copy ------------------------------------------------------------------


def test_copy_forks_state_and_metadata_independently(client: TestClient) -> None:
    source = _thread(client, metadata={"team": "a"})["thread_id"]
    _run(client, source, "one")
    source_metadata = client.get(f"/threads/{source}").json()["metadata"]
    copy = client.post(f"/threads/{source}/copy").json()
    assert copy["thread_id"] != source
    assert source_metadata["team"] == "a"
    assert copy["metadata"] == {**source_metadata, "forked_from": source}
    assert copy["values"] == client.get(f"/threads/{source}").json()["values"]

    _run(client, copy["thread_id"], "two")
    source_messages = client.get(f"/threads/{source}").json()["values"]["messages"]
    copy_messages = client.get(f"/threads/{copy['thread_id']}").json()["values"][
        "messages"
    ]
    assert len(source_messages) == 2
    assert len(copy_messages) == 4


# --- state -----------------------------------------------------------------


def test_state_reports_latest_checkpoint(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    state = client.get(f"/threads/{thread_id}/state").json()
    assert state["next"] == []
    assert state["tasks"] == []
    assert state["checkpoint"]["thread_id"] == thread_id
    assert state["checkpoint"]["checkpoint_id"]
    assert [m["type"] for m in state["values"]["messages"]] == ["human", "ai"]


def test_state_update_writes_new_checkpoint(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    before = client.get(f"/threads/{thread_id}/state").json()["checkpoint"]
    written = client.post(
        f"/threads/{thread_id}/state",
        json={"values": {"messages": [{"role": "ai", "content": "edited"}]}},
    ).json()
    assert written["checkpoint_id"] != before["checkpoint_id"]
    state = client.get(f"/threads/{thread_id}/state").json()
    assert state["checkpoint"]["checkpoint_id"] == written["checkpoint_id"]
    assert state["values"]["messages"][-1]["content"] == "edited"


def test_state_at_checkpoint_time_travels(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id, "first")
    first_cp = client.get(f"/threads/{thread_id}/state").json()["checkpoint"]
    _run(client, thread_id, "second")

    by_path = client.get(
        f"/threads/{thread_id}/state/{first_cp['checkpoint_id']}"
    ).json()
    by_body = client.post(
        f"/threads/{thread_id}/state/checkpoint",
        json={"checkpoint_id": first_cp["checkpoint_id"]},
    ).json()
    for state in (by_path, by_body):
        assert [m["content"] for m in state["values"]["messages"]] == [
            "first",
            "echo: first",
        ]


def test_interrupted_thread_state_exposes_pending_task() -> None:
    with real_client("interrupting") as client:
        thread_id = _thread(client)["thread_id"]
        _run(client, thread_id)
        state = client.get(f"/threads/{thread_id}/state").json()
    assert state["next"] == ["ask"]
    (task,) = state["tasks"]
    assert task["name"] == "ask"
    assert len(task["interrupts"]) == 1


@pytest.mark.xfail(strict=True, reason="#121: non-dict interrupt value wrapped")
def test_interrupted_task_carries_raw_interrupt_value() -> None:
    with real_client("interrupting") as client:
        thread_id = _thread(client)["thread_id"]
        _run(client, thread_id)
        (task,) = client.get(f"/threads/{thread_id}/state").json()["tasks"]
    assert task["interrupts"][0]["value"] == "approve?"


# --- history ---------------------------------------------------------------


def _history(client: TestClient, thread_id: str, **params: Any) -> list[dict[str, Any]]:
    response = client.get(f"/threads/{thread_id}/history", params=params)
    assert response.status_code == 200, response.text
    return list(response.json())


def test_history_is_newest_first_and_paginates(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    for text in ("one", "two", "three"):
        _run(client, thread_id, text, durability="async")
    history = _history(client, thread_id)
    sizes = [len(h["values"].get("messages", [])) for h in history]
    assert sizes == sorted(sizes, reverse=True)
    assert sizes[0] == 6

    assert len(_history(client, thread_id, limit=2)) == 2
    newest = history[0]["checkpoint"]["checkpoint_id"]
    older = _history(client, thread_id, before=newest)
    assert newest not in [h["checkpoint"]["checkpoint_id"] for h in older]
    assert len(older) == len(history) - 1


def test_history_post_matches_get(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    posted = client.post(f"/threads/{thread_id}/history", json={"limit": 5}).json()
    assert posted == _history(client, thread_id, limit=5)


def test_explicit_async_durability_checkpoints_every_step(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id, durability="async")
    # input step, reply step (plus LangGraph's initial empty checkpoint)
    assert len(_history(client, thread_id)) >= 3


@pytest.mark.xfail(strict=True, reason="#127: default durability is exit, not async")
def test_default_durability_checkpoints_every_step(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id)
    assert len(_history(client, thread_id)) >= 3


@pytest.mark.xfail(strict=True, reason="#127: checkpoint_during is ignored")
def test_checkpoint_during_checkpoints_every_step(client: TestClient) -> None:
    thread_id = _thread(client)["thread_id"]
    _run(client, thread_id, checkpoint_during=True)
    assert len(_history(client, thread_id)) >= 3
