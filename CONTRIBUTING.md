# Contributing to skeino

Thanks for your interest in contributing! This document covers local setup and
the checks your change must pass.

## Development setup

skeino uses [Poetry](https://python-poetry.org/) and targets Python 3.11+.

```bash
git clone https://github.com/ranjitjana027/skeino.git
cd skeino
poetry install
```

## Running the checks

All of these run in CI and must pass before a PR can be merged:

```bash
poetry run ruff format --check .   # formatting
poetry run ruff check .            # lint
poetry run mypy src                # static types (strict)
poetry run bandit -r src           # security scan
poetry run pytest                  # unit + integration tests
```

To auto-fix formatting and lint issues:

```bash
poetry run ruff format .
poetry run ruff check --fix .
```

### Infra-backed API tests (optional)

`tests/api/` exercises the HTTP API end to end against **real** Postgres,
MongoDB, and Redis (a real LangGraph graph behind the real checkpointer). It
is excluded from plain `pytest` and CI — run it locally when touching
persistence or streaming code:

```bash
docker compose up -d --wait
poetry install --all-extras --with redis
poetry run pytest tests/api
docker compose down -v
```

## Guidelines

- Keep the public surface small. The supported API is `create_app`,
  `SkeinoSettings`, `from_langgraph_json`, and `GraphRegistry`
  (see `src/skeino/__init__.py`). Submodules are importable for advanced use
  but are not part of the stability contract.
- Add tests for new behaviour. Tests are self-contained — see
  `tests/conftest.py` for the `FakeGraph` test double; no external services
  are required for the unit/integration suites.
- Add a [changelog fragment](changelog.d/README.md) for any user-facing change —
  a file `changelog.d/<id>.<type>.md` (e.g. `changelog.d/42.added.md`). **Do not**
  edit `CHANGELOG.md` directly; fragments are collated on release and avoid the
  merge conflicts a shared `[Unreleased]` section causes.
- New code must be fully typed (the mypy config is strict).

## Submitting changes

1. Fork and create a feature branch.
2. Make your change with tests and a changelog fragment (`changelog.d/`).
3. Ensure all checks pass locally.
4. Open a pull request describing the change and its motivation.

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, consistent with the project's [LICENSE](LICENSE).
