---
title: API explorer
hide:
  - toc
---

# API explorer

An interactive, always-current view of skeino's HTTP API, rendered from the
OpenAPI schema generated directly from [`create_app`][skeino.create_app] at
build time. Browse every endpoint, expand request/response models, and copy
example calls.

!!! tip "Run it against your own server"
    This explorer documents the schema. To send live requests, point a client at
    your running server — FastAPI also serves Swagger UI at `/docs` and the raw
    schema at `/openapi.json` on the app itself.

<div class="skeino-scalar">
<script
  id="api-reference"
  data-url="../openapi.json"
  data-configuration='{"theme":"purple","hideDownloadButton":false,"darkMode":false}'>
</script>
<script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
</div>
