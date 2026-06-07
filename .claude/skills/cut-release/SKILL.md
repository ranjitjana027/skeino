---
name: cut-release
description: >-
  Cut and publish a new skeino release: pick the semver bump, bump the version,
  collate changelog fragments with towncrier, build, open and merge the release
  PR, tag, and verify the PyPI publish. Use when asked to "release", "cut a
  release", "publish to PyPI", or ship the accumulated unreleased changes.
---

# Cut a skeino release

skeino publishes to PyPI via **Trusted Publishing**: pushing a `vX.Y.Z` tag
triggers `.github/workflows/publish.yml`, which builds and uploads. The release
itself is just a version bump + changelog collation merged to `main`, then a tag.

## 1. Decide the version (semver)

Feature/fix PRs bump `pyproject.toml`'s `version` themselves (patch=fix,
minor=feature, major=breaking), so **`main` usually already carries the target
release version** — confirm it matches the pending `changelog.d/` fragments
(`poetry run towncrier build --draft --version 0.0.0` previews them):

- Any `added`/`changed`/`removed` fragment → at least **minor** (major if the
  fragment is breaking / a `removed`).
- Only `fixed`/docs/chore fragments → **patch**.

If `main`'s version already reflects this, reuse it; otherwise bump to it. If
unsure between two, ask the user. Confirm whether they want a real PyPI publish
(default yes) or a dry run.

## 2. Branch and bump

```bash
git checkout main && git pull
git checkout -b release/X.Y.Z
```

- `pyproject.toml`: set `version = "X.Y.Z"`.

## 3. Collate the changelog

```bash
poetry run towncrier build --yes --version X.Y.Z   # writes CHANGELOG.md, deletes fragments
```

This inserts `## [X.Y.Z] - <today>` under the towncrier marker and removes the
consumed `changelog.d/*.md`. Then **add the compare links** at the bottom of
`CHANGELOG.md` by hand (towncrier doesn't manage them):

```
[Unreleased]: https://github.com/ranjitjana027/skeino/compare/vX.Y.Z...HEAD
[X.Y.Z]: https://github.com/ranjitjana027/skeino/compare/v<prev>...vX.Y.Z
```

Update the existing `[Unreleased]` line to compare from the new tag, and keep the
older links. Sanity-check the rendered section.

## 4. Verify the build

```bash
poetry run ruff check . && poetry run mypy src && poetry run pytest
rm -rf dist && poetry build      # expect skeino-X.Y.Z.tar.gz + ...-py3-none-any.whl
```

## 5. PR, merge, tag

```bash
git add pyproject.toml CHANGELOG.md changelog.d
git commit -m "chore(release): X.Y.Z"
git push -u origin release/X.Y.Z
gh pr create --title "chore(release): X.Y.Z" --body "<summary of the release>"
```

Wait for green CI (`gh pr checks <N> --watch`). Merge **squash** (the repo allows
squash only) — admin override is needed because of the solo-maintainer ruleset
(see `review-and-merge-prs`):

```bash
gh pr merge <N> --squash --delete-branch --admin
git checkout main && git pull
```

Tag the merge commit and push (this is what triggers publishing):

```bash
git tag -a vX.Y.Z -m "skeino X.Y.Z" && git push origin vX.Y.Z
```

## 6. Watch publish + verify PyPI

```bash
gh run list --workflow=publish.yml --limit 1          # find the run for the tag
gh run watch <run-id> --interval 15 --exit-status
curl -s https://pypi.org/pypi/skeino/json | \
  python -c "import sys,json;d=json.load(sys.stdin);print(d['info']['version'])"
```

Confirm the latest version matches and both wheel + sdist are present.

## 7. Offer a GitHub Release

The tag alone doesn't create a GitHub *Release* page. Offer to run
`gh release create vX.Y.Z --notes-from-tag` (or notes from the changelog
section).
