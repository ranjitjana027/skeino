"""Surface manually-parsed request bodies in the generated OpenAPI schema.

skeino's routers parse JSON bodies by hand (see
:func:`skeino.api._request.parse_request_model`) so they accept
``application/json`` payloads sent with a ``text/plain`` content-type (the
LangGraph SDK does this to dodge CORS preflight). The cost is that FastAPI's
OpenAPI generator never sees those request models, so they are absent from
``/openapi.json``, ``/docs``, and the Scalar explorer — only response models
(declared via ``response_model=``) are documented.

This module closes that gap without touching the tolerant runtime parsing or
the 422 contract: a router tags each hand-parsing handler with
``@request_model(Model)``, and :func:`install_request_body_openapi` injects each
model into ``components/schemas`` and the operation's ``requestBody`` at
schema-build time.
"""

from __future__ import annotations

from typing import Any, Callable, Iterable, Iterator, TypeVar

from fastapi import FastAPI
from fastapi.routing import APIRoute
from pydantic import BaseModel
from pydantic.json_schema import models_json_schema
from starlette.routing import BaseRoute

F = TypeVar("F", bound=Callable[..., Any])

_REQUEST_MODEL_ATTR = "__skeino_request_model__"
_REF_TEMPLATE = "#/components/schemas/{model}"


def request_model(model: type[BaseModel]) -> Callable[[F], F]:
    """Tag a route handler with the request body model it parses by hand.

    Read by :func:`install_request_body_openapi` at schema-build time; it does
    **not** change runtime behaviour (the handler still calls
    ``parse_request_model`` itself). Place it directly under the router
    decorator so the tag lands on the registered endpoint::

        @router.post("/threads")
        @request_model(ThreadCreateRequest)
        async def create_thread(request: Request) -> ThreadModel: ...
    """

    def decorate(func: F) -> F:
        setattr(func, _REQUEST_MODEL_ATTR, model)
        return func

    return decorate


def _iter_api_routes(routes: Iterable[BaseRoute]) -> Iterator[APIRoute]:
    """Yield every ``APIRoute`` reachable from ``routes``.

    FastAPI 0.138 includes routers lazily: ``app.routes`` holds opaque wrappers
    (``_IncludedRouter``) exposing the real router via ``original_router``
    instead of flattening its ``APIRoute``s. Recurse through those so this keeps
    working whether routes are flattened (older FastAPI) or wrapped.
    """
    for route in routes:
        if isinstance(route, APIRoute):
            yield route
        original = getattr(route, "original_router", None)
        if original is not None:
            yield from _iter_api_routes(original.routes)


def _tagged_routes(app: FastAPI) -> list[tuple[APIRoute, type[BaseModel]]]:
    """Collect ``(route, model)`` pairs for every ``@request_model`` handler."""
    tagged: list[tuple[APIRoute, type[BaseModel]]] = []
    for route in _iter_api_routes(app.routes):
        model = getattr(route.endpoint, _REQUEST_MODEL_ATTR, None)
        if model is not None:
            tagged.append((route, model))
    return tagged


def install_request_body_openapi(app: FastAPI) -> None:
    """Patch ``app.openapi`` to document the bodies of ``@request_model`` routes.

    Idempotent in effect: the enriched schema is cached on ``app.openapi_schema``
    like FastAPI's own, so generation is deterministic and runs once.
    """
    tagged = _tagged_routes(app)
    if not tagged:
        return

    base_openapi = app.openapi

    def custom_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema

        schema = base_openapi()
        components = schema.setdefault("components", {}).setdefault("schemas", {})

        # Sort by name so the generated components are deterministic regardless
        # of route registration order (the determinism guard depends on this).
        models = sorted({model for _, model in tagged}, key=lambda m: m.__name__)
        refs, defs = models_json_schema(
            [(model, "validation") for model in models],
            ref_template=_REF_TEMPLATE,
        )

        # Register each request model's definitions without clobbering existing
        # components: a model already rendered by FastAPI for a response (e.g.
        # CheckpointConfigModel) wins, so the doc keeps one definition per name
        # and request bodies just $ref it.
        for name, definition in defs.get("$defs", {}).items():
            components.setdefault(name, definition)

        for route, model in tagged:
            ref = refs[(model, "validation")]["$ref"]
            # A body is required only when the model has a field with no default;
            # the tolerant parser treats an empty body as ``{}``.
            required = bool(model.model_json_schema().get("required"))
            request_body = {
                "required": required,
                "content": {"application/json": {"schema": {"$ref": ref}}},
            }
            operations = schema["paths"][route.path_format]
            for method in (route.methods or set()) - {"HEAD", "OPTIONS"}:
                # setdefault: never override a body FastAPI already documented.
                operations[method.lower()].setdefault("requestBody", request_body)

        app.openapi_schema = schema
        return schema

    app.openapi = custom_openapi  # type: ignore[method-assign]
