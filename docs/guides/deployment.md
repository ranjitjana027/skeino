# Deploy skeino

skeino is a standard ASGI (FastAPI) application, so it deploys like any other
FastAPI service. This guide covers the production essentials.

## Serve with an ASGI server

For development, uvicorn with `--reload` is fine:

```bash
uvicorn app:app --reload --port 8000
```

For production, run uvicorn without reload (optionally behind Gunicorn for
process management):

```bash
uvicorn app:app --host 0.0.0.0 --port 8000 --workers 4
```

!!! warning "Workers and the per-thread lock"
    skeino enforces *one run per thread* using **in-process** locks. With
    multiple worker processes, that guarantee holds only *within* a worker, not
    across them, since two workers don't share the lock map. If you rely on the
    single-run-per-thread invariant across a cluster, either run a single worker
    process or front the deployment with routing that pins a thread to one
    worker. A shared, cross-process lock service is out of scope for v1.

## Use Postgres

Production deployments should set `postgres_uri` so threads, runs, and
checkpoints survive restarts and are shared across workers. See
[Set up Postgres persistence](postgres.md). The in-memory default is for
development and tests only.

Provide the connection string through your environment and read it with
`pydantic-settings` (or your config system), then pass it into `SkeinoSettings`
— skeino itself does not read environment variables (see
[Configuration](../concepts/configuration.md)).

## Lock down CORS

The default CORS settings (`["*"]`) are permissive for convenience. In
production, set `cors_origins` to the explicit list of front-end origins that
should be allowed to call the API.

## Add authentication

skeino has no built-in auth in v1. Put authentication in front of it — an API
gateway, a reverse proxy, or an ASGI middleware / FastAPI dependency on the app
you control. See
[Embed in an existing FastAPI app](embedding-fastapi.md#adding-authentication).

## Containerise

A minimal image:

```dockerfile title="Dockerfile"
FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml poetry.lock ./
RUN pip install poetry && poetry config virtualenvs.create false \
    && poetry install --only main --no-interaction --no-root
COPY . .
RUN poetry install --only main --no-interaction

EXPOSE 8000
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

If you only depend on the published package, a plain
`pip install skeino` plus your app code is enough — Poetry is not required at
runtime.

## Health checks

Wire your orchestrator's liveness/readiness probe to `GET /api/health`, which
returns `{"status": "ok", "version": "..."}`. `GET /info` returns the server
name and version for SDK clients.

## Observability

skeino logs through the standard `logging` module under the `skeino` logger
namespace. Configure handlers/levels in your application to capture startup
diagnostics (checkpointer/metadata-store selection, manifest warnings) and run
errors.
