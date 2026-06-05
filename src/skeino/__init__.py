"""skeino — reusable LangGraph dev-server replacement.

Public surface:

* :func:`create_app` — assemble a FastAPI app over user-supplied graphs.
* :class:`SkeinoSettings` — typed configuration.
* :class:`GraphRegistry` — multi-graph registry (single-graph routed in v1).
* :func:`from_langgraph_json` — load ``langgraph.json`` and call ``create_app``.

Sub-modules (``api``, ``ops``, ``persistence``, ``serialization``, ``schemas``,
``streaming``, ``concurrency``) are intentionally importable for advanced
users — but the contract above is the supported surface.
"""

__version__ = "0.1.0"

from skeino.app import create_app
from skeino.config import SkeinoSettings
from skeino.langgraph_json import from_langgraph_json
from skeino.registry import GraphRegistry

__all__ = [
    "GraphRegistry",
    "SkeinoSettings",
    "__version__",
    "create_app",
    "from_langgraph_json",
]
