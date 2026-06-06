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

## Guidelines

- Keep the public surface small. The supported API is `create_app`,
  `SkeinoSettings`, `from_langgraph_json`, and `GraphRegistry`
  (see `src/skeino/__init__.py`). Submodules are importable for advanced use
  but are not part of the stability contract.
- Add tests for new behaviour. Tests are self-contained — see
  `tests/conftest.py` for the `FakeGraph` test double; no external services
  are required for the unit/integration suites.
- Update `CHANGELOG.md` under `[Unreleased]` for any user-facing change.
- New code must be fully typed (the mypy config is strict).

## Submitting changes

1. Fork and create a feature branch.
2. Make your change with tests and a changelog entry.
3. Ensure all checks pass locally.
4. Open a pull request describing the change and its motivation.

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, consistent with the project's [LICENSE](LICENSE).
