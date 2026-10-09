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

## Working with Codex and Claude Code

Both agents share `AGENTS.md` and the workflows in `.agents/skills/`.
Current Claude Code versions load `AGENTS.md` directly when no `CLAUDE.md`
is present. Claude discovers the shared workflows through symlinks in
`.claude/skills/`. Edit the shared sources to keep them aligned.

Claude hooks are registered in `.claude/settings.json`; Codex hooks are in
`.codex/hooks.json`. Both use the scripts in `.claude/hooks/` to guard direct
lockfile/secrets edits and run pytest after Python changes. The file guard
covers edit/patch tools, not arbitrary shell writes. The stop hook stops
blocking after three consecutive failures so the agent can report the problem.

For Codex, start a new session in this repository and review/trust the project
and its hooks through `/hooks` when prompted. Hooks require a Codex version
that supports project hooks; inspect `/hooks` to confirm they loaded. See the
[official hooks documentation](https://learn.chatgpt.com/docs/hooks).
Claude marketplace plugins remain configured in `.claude/settings.json`;
Codex Python/LangGraph skills must be installed separately in your user setup.

For simultaneous work, give each agent its own branch and git worktree.
For example, from a clean checkout:

```bash
git worktree add ../skeino-codex -b feat/codex-task
git worktree add ../skeino-claude -b feat/claude-task
```

Open each agent in its assigned worktree and install dependencies there with
`poetry install`. Both agents must run the required checks below.

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
