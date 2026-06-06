# Embed in an existing FastAPI app

`create_app` returns a standard `FastAPI` instance, so embedding skeino into a
larger application is straightforward. There are two common patterns.

## Pattern 1 — mount skeino as a sub-application

Keep your own app as the root and mount the skeino app under a path prefix. This
isolates skeino's routes, lifespan, and middleware cleanly.

```python title="app.py"
from fastapi import FastAPI
from skeino import create_app, SkeinoSettings
from my_project.graph import graph

skeino_app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(postgres_uri="postgresql://localhost/skeino"),
)

root = FastAPI()

@root.get("/")
def home():
    return {"service": "my-product"}

# skeino's endpoints are now under /agent (e.g. /agent/threads, /agent/info)
root.mount("/agent", skeino_app)
```

```bash
uvicorn app:root --port 8000
```

!!! note "Lifespan runs on mount"
    skeino opens its checkpointer and metadata store in the sub-app's lifespan.
    When you `mount()` a sub-application, FastAPI/Starlette runs its lifespan as
    part of the parent's — so persistence is initialised correctly. Prefer
    `mount()` over copying routers for this reason.

## Pattern 2 — run skeino as the root, add your own routes

If skeino's surface is the bulk of your API, make it the root app and attach your
own routes and middleware to it:

```python title="app.py"
from skeino import create_app, SkeinoSettings
from my_project.graph import graph

app = create_app(
    graphs={"my_agent": graph},
    settings=SkeinoSettings(),
)

@app.get("/healthz")
def healthz():
    return {"ok": True}
```

This works because `app` is a normal `FastAPI` object. Be mindful that skeino
already mounts routes under `/`, `/assistants`, and `/threads`, so avoid path
collisions.

## Adding authentication

skeino does not ship auth in v1. Add it as middleware or a dependency on the app
you control. With the mount pattern, you can guard the whole sub-app:

```python
from fastapi import Depends, HTTPException, Request

async def require_api_key(request: Request):
    if request.headers.get("x-api-key") != EXPECTED_KEY:
        raise HTTPException(status_code=401, detail="Unauthorized")

root.mount("/agent", skeino_app)
# Apply the dependency at the router/route level on `root`, or wrap skeino_app
# with an ASGI middleware that enforces the check before forwarding.
```

For production, prefer an ASGI authentication middleware (or an API gateway in
front of the process) so every skeino route is covered uniformly.

## CORS

skeino configures CORS from [`SkeinoSettings`](../concepts/configuration.md#cors).
If you embed skeino under your own app and also configure CORS at the root, make
sure the two don't conflict — typically you set CORS in one place only.
