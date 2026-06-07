## Summary

<!-- What does this PR do and why? Link any related issues, e.g. "Closes #123". -->

## Type of change

- [ ] Bug fix (non-breaking change that fixes an issue)
- [ ] New feature (non-breaking change that adds functionality)
- [ ] Breaking change (fix or feature that changes existing behavior)
- [ ] Documentation / chore / CI

## Checklist

- [ ] `poetry run ruff format --check .` passes
- [ ] `poetry run ruff check .` passes
- [ ] `poetry run mypy src` passes
- [ ] `poetry run bandit -r src` passes
- [ ] `poetry run pytest` passes
- [ ] Added/updated tests for the change
- [ ] Added a changelog fragment in `changelog.d/` (not a direct `CHANGELOG.md` edit)
- [ ] Bumped `version` in `pyproject.toml` (patch = bug fix, minor = feature, major = breaking) — bug-fix/feature PRs only
- [ ] Updated docs/README where relevant

## Notes for reviewers

<!-- Anything reviewers should focus on, trade-offs made, or follow-ups. -->
