"""Per-run LangSmith tracing, as LangGraph Studio expects it.

Studio sends each run with ``langsmith_tracer: {project_name, example_id}`` and
then looks for that run's trace in ``project_name``. It only does so once the
server advertises ``langsmith_tracing_session_on_runs`` in ``GET /info`` —
without it, Studio shows no traces at all.

Mirrors langgraph-api (``tracing_session.py`` and the tracing block in
``stream.py``): the run executes inside a LangSmith tracing context whose
*replicas* write the trace to the requested project **and** to the server's
default project, so Studio gets its copy without the deployment losing its own.
LangChain's tracer reads those replicas from the context, so every model, tool,
and node call inside the run is covered with no per-call wiring.

Everything here is a no-op when tracing is not enabled for the process.
"""

from collections.abc import Iterator
from contextlib import contextmanager, nullcontext
from typing import Any

from langsmith import run_helpers
from langsmith import utils as ls_utils

from skeino.schemas import LangSmithTracerModel

__all__ = ["resolve_session_name", "run_tracing_context", "tracing_enabled"]


def tracing_enabled() -> bool:
    """Whether LangSmith tracing is configured for this process.

    Reads the same environment switches LangChain does (``LANGSMITH_TRACING``,
    ``LANGCHAIN_TRACING_V2``, …), so it agrees with whether traces are
    actually written.
    """
    return bool(ls_utils.tracing_is_enabled())


def _default_project() -> str:
    """Return the project runs trace into when no per-run project is given."""
    return str(ls_utils.get_tracer_project())


def resolve_session_name(tracer: LangSmithTracerModel | None) -> str | None:
    """Return the project a run's trace is written to, or ``None`` if untraced.

    A per-run ``project_name`` wins over the deployment default
    (``LANGSMITH_PROJECT`` / ``LANGCHAIN_PROJECT``).
    """
    if not tracing_enabled():
        return None
    if tracer is not None and tracer.project_name:
        return tracer.project_name
    return _default_project()


@contextmanager
def _replica_context(project_name: str, example_id: str | None) -> Iterator[None]:
    updates: dict[str, Any] | None = (
        {"reference_example_id": example_id} if example_id else None
    )
    replicas: list[Any] = [
        {"project_name": project_name, "updates": updates},
        {"project_name": _default_project(), "updates": None},
    ]
    with run_helpers.tracing_context(replicas=replicas):
        yield


def run_tracing_context(
    tracer: LangSmithTracerModel | None,
) -> Any:
    """Return a context manager that routes a run's trace to its projects.

    Enter it around graph execution. When tracing is off or the run named no
    project, it is a ``nullcontext`` and the run traces exactly as before.
    """
    if tracer is None or not tracer.project_name or not tracing_enabled():
        return nullcontext()
    return _replica_context(tracer.project_name, tracer.example_id)
