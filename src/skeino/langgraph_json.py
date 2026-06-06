"""Loader that builds a skeino app from a ``langgraph.json`` manifest.

This is the high-level entry point that mirrors how ``langgraph dev`` boots:

1. Read the JSON manifest.
2. Apply the declared ``env`` file (via python-dotenv).
3. Resolve each ``graphs[name]`` entry as ``path:attribute``.
4. Build a :class:`SkeinoSettings` from the env / manifest.
5. Hand off to :func:`skeino.create_app`.

v1 ignores ``http.app`` overrides, ``store``, and ``auth`` (warns when present).
``graphs[name]`` may resolve to either a precompiled
``CompiledStateGraph`` (used as-is) or a callable
``(checkpointer) -> CompiledStateGraph`` (called by skeino with its resolved
checkpointer).
"""

import importlib.util
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any, cast

from fastapi import FastAPI

from skeino.app import GraphInput, create_app
from skeino.config import SkeinoSettings

logger = logging.getLogger(__name__)

_ENV_VAR_RE = re.compile(r"\$\{([A-Z_][A-Z0-9_]*)\}")


def _expand_env(value: str) -> str:
    """Replace ``${VAR}`` placeholders with current env-var values (empty if unset)."""

    def _sub(match: re.Match[str]) -> str:
        return os.environ.get(match.group(1), "")

    return _ENV_VAR_RE.sub(_sub, value)


def _expanded_str(value: Any) -> str | None:
    """Return an env-expanded string when ``value`` is a string, else None."""
    if isinstance(value, str):
        return _expand_env(value)
    return None


def _load_module(path: Path) -> Any:
    """Import a Python module from a filesystem path, cached by absolute path."""
    abs_path = path.resolve()
    cache_key = f"_skeino_langgraph_json::{abs_path}"
    if cache_key in sys.modules:
        return sys.modules[cache_key]
    spec = importlib.util.spec_from_file_location(cache_key, abs_path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot import module from {abs_path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[cache_key] = module
    spec.loader.exec_module(module)
    return module


def _resolve_graph_target(spec: str, manifest_dir: Path) -> GraphInput:
    """Resolve ``./path/to/file.py:attr`` to the referenced Python object."""
    if ":" not in spec:
        raise ValueError(f"Invalid graph target {spec!r}; expected 'path:attribute'.")
    path_part, attr = spec.split(":", 1)
    target_path = (manifest_dir / path_part).resolve()
    if not target_path.exists():
        raise FileNotFoundError(f"Graph module not found: {target_path}")
    module = _load_module(target_path)
    if not hasattr(module, attr):
        raise AttributeError(f"Module {target_path} has no attribute {attr!r}.")
    return cast(GraphInput, getattr(module, attr))


def _maybe_load_dotenv(manifest_dir: Path, env_spec: Any) -> None:
    """Apply python-dotenv when the manifest names an env file."""
    if not isinstance(env_spec, str) or not env_spec:
        return
    env_path = (manifest_dir / env_spec).resolve()
    if not env_path.exists():
        logger.warning("env file %s declared in manifest but missing", env_path)
        return
    try:
        from dotenv import load_dotenv
    except ImportError:  # pragma: no cover — python-dotenv is a declared dep
        logger.warning("python-dotenv not installed; skipping env file load")
        return
    load_dotenv(env_path, override=False)


def _settings_from_manifest(
    manifest: dict[str, Any],
    *,
    overrides: SkeinoSettings | None,
) -> SkeinoSettings:
    """Build a ``SkeinoSettings`` from the manifest, allowing caller overrides."""
    base_kwargs: dict[str, Any] = {}

    http_section = manifest.get("http")
    if isinstance(http_section, dict):
        cors = http_section.get("cors")
        if isinstance(cors, dict):
            origins = cors.get("allow_origins")
            methods = cors.get("allow_methods")
            headers = cors.get("allow_headers")
            if isinstance(origins, list):
                base_kwargs["cors_origins"] = [str(o) for o in origins]
            if isinstance(methods, list):
                base_kwargs["cors_methods"] = [str(m) for m in methods]
            if isinstance(headers, list):
                base_kwargs["cors_headers"] = [str(h) for h in headers]
        if "app" in http_section:
            logger.warning(
                "manifest http.app is set to %s — skeino builds its own FastAPI "
                "app and does not merge user-supplied apps in v1.",
                http_section["app"],
            )

    store_section = manifest.get("store")
    if isinstance(store_section, dict):
        store_uri = _expanded_str(store_section.get("uri"))
        if store_uri:
            base_kwargs["postgres_uri"] = store_uri

    # If the caller passed overrides, they win field-by-field for any explicit
    # value. Use model_dump(exclude_unset=True) so we don't clobber manifest
    # data with default SkeinoSettings field values.
    if overrides is not None:
        for key, value in overrides.model_dump(exclude_unset=True).items():
            base_kwargs[key] = value

    return SkeinoSettings(**base_kwargs)


def from_langgraph_json(
    manifest_path: str | Path,
    *,
    settings: SkeinoSettings | None = None,
) -> FastAPI:
    """Build a skeino-backed FastAPI app from a ``langgraph.json`` manifest.

    Parameters
    ----------
    manifest_path:
        Path to the ``langgraph.json`` file.
    settings:
        Optional :class:`SkeinoSettings` whose explicit fields override anything
        derived from the manifest. Useful for setting graph-specific options
        (``agent_nodes``, ``status_field``) that ``langgraph.json`` does not
        describe.

    """
    path = Path(manifest_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"Manifest not found: {path}")

    with path.open("r", encoding="utf-8") as fh:
        manifest = json.load(fh)

    manifest_dir = path.parent
    _maybe_load_dotenv(manifest_dir, manifest.get("env"))

    graphs_section = manifest.get("graphs") or {}
    if not isinstance(graphs_section, dict) or not graphs_section:
        raise ValueError(f"Manifest {path} declares no graphs.")
    resolved: dict[str, GraphInput] = {
        name: _resolve_graph_target(str(spec), manifest_dir)
        for name, spec in graphs_section.items()
    }

    for label in ("auth", "ui"):
        if label in manifest:
            logger.warning("manifest section %r is not implemented in skeino v1", label)

    effective_settings = _settings_from_manifest(manifest, overrides=settings)
    return create_app(graphs=resolved, settings=effective_settings)
