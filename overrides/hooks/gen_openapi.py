"""MkDocs hook: emit skeino's OpenAPI schema for the API explorer.

skeino's HTTP surface is defined by FastAPI, so the authoritative OpenAPI
document is whatever ``create_app`` produces. Rather than hand-maintaining a
copy, this hook builds the app against a throwaway stub graph and writes the
generated schema into the built site, where ``api-reference/explorer.md`` renders
it with Scalar. Generating it at build time keeps the explorer in lockstep with
the code.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, TypedDict

log = logging.getLogger("mkdocs.plugins.skeino")

_REL_OUTPUT = Path("api-reference") / "openapi.json"


class _StubState(TypedDict):
    """Minimal graph state used only to materialise an app for schema export."""

    messages: list[Any]


def _build_openapi() -> dict[str, Any]:
    """Build a skeino app over a stub graph and return its OpenAPI document."""
    from langgraph.graph import END, START, StateGraph

    from skeino import SkeinoSettings, create_app

    builder: StateGraph = StateGraph(_StubState)
    builder.add_node("agent", lambda state: state)
    builder.add_edge(START, "agent")
    builder.add_edge("agent", END)

    app = create_app(graphs={"agent": builder.compile()}, settings=SkeinoSettings())
    schema: dict[str, Any] = app.openapi()
    schema.setdefault("info", {})["title"] = "skeino HTTP API"
    return schema


def on_post_build(config: Any, **_kwargs: Any) -> None:
    """Write the generated OpenAPI schema into the built site directory.

    A failure here means the API explorer would ship with no (or a stale)
    schema. In CI we fail the build so a broken explorer can never publish; for
    local builds we only warn, so iterating on prose doesn't require the package
    to import cleanly.
    """
    try:
        schema = _build_openapi()
    except Exception as exc:
        if os.environ.get("CI"):
            raise
        log.warning(
            "skeino: could not generate OpenAPI schema (%s); the API explorer "
            "will be empty in this local build.",
            exc,
        )
        return

    out_path = Path(config["site_dir"]) / _REL_OUTPUT
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(schema), encoding="utf-8")
    log.info("skeino: wrote OpenAPI schema to %s", out_path)
