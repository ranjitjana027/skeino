"""Request bodies of hand-parsed routes must reach the served OpenAPI schema.

skeino's routers parse JSON bodies manually (to tolerate ``text/plain``), so
FastAPI doesn't document their request models on its own. ``create_app`` patches
the schema to add them; these tests pin that behaviour at the wire level so a
regression (e.g. dropping the ``@request_model`` tag or the install call) fails
loudly rather than silently emptying ``/docs``. See #67.
"""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from tests.conftest import build_test_app

# (path, method, expected request model component) for every hand-parsed body.
EXPECTED_BODIES = [
    ("/threads", "post", "ThreadCreateRequest"),
    ("/threads/search", "post", "ThreadSearchRequest"),
    ("/threads/{thread_id}", "patch", "ThreadPatchRequest"),
    ("/threads/{thread_id}/state", "post", "ThreadStateUpdateRequest"),
    ("/threads/{thread_id}/state/checkpoint", "post", "CheckpointConfigModel"),
    ("/threads/{thread_id}/history", "post", "ThreadStateSearchRequest"),
    ("/threads/{thread_id}/runs", "post", "RunCreateRequest"),
    ("/threads/{thread_id}/runs/stream", "post", "RunCreateRequest"),
    ("/assistants/search", "post", "AssistantSearchRequest"),
]


def _schema() -> dict[str, Any]:
    app, _ = build_test_app()
    with TestClient(app) as client:
        response = client.get("/openapi.json")
    assert response.status_code == 200
    return response.json()


def test_every_hand_parsed_route_documents_its_request_body() -> None:
    schema = _schema()
    components = schema["components"]["schemas"]
    for path, method, model in EXPECTED_BODIES:
        operation = schema["paths"][path][method]
        body = operation.get("requestBody")
        assert body is not None, f"{method.upper()} {path} has no requestBody"
        ref = body["content"]["application/json"]["schema"]["$ref"]
        assert ref == f"#/components/schemas/{model}", (
            f"{method.upper()} {path} body refs {ref}, expected {model}"
        )
        assert model in components, f"{model} missing from components/schemas"


def test_request_body_required_flag_tracks_required_fields() -> None:
    schema = _schema()
    # RunCreateRequest requires assistant_id → body required.
    run_body = schema["paths"]["/threads/{thread_id}/runs"]["post"]["requestBody"]
    assert run_body["required"] is True
    # ThreadCreateRequest is all-optional → body not required (empty body is {}).
    thread_body = schema["paths"]["/threads"]["post"]["requestBody"]
    assert thread_body["required"] is False


def test_request_model_fields_carry_descriptions_in_schema() -> None:
    """The #66 field descriptions must now reach the served schema, not just
    the Python reference — that is the whole point of #67."""
    schema = _schema()
    props = schema["components"]["schemas"]["RunCreateRequest"]["properties"]
    assert props["assistant_id"]["description"]
    # every property of a request model should be documented
    assert all("description" in field for field in props.values())


def test_no_dangling_component_refs() -> None:
    """Every injected $ref must resolve to a registered component."""
    import json
    import re

    schema = _schema()
    components = set(schema["components"]["schemas"])
    refs = set(re.findall(r"#/components/schemas/([A-Za-z0-9_]+)", json.dumps(schema)))
    assert refs <= components, f"dangling refs: {sorted(refs - components)}"
