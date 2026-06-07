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

The version is **not** bumped per PR — decide it here at release time from the
pending `changelog.d/` fragments (`poetry run towncrier build --draft --version
0.0.0` previews them), bumping from `main`'s current `pyproject.toml` version:

- Any breaking change / a `removed` fragment → **major**.
- Any `added`/`changed` fragment → **minor**.
- Only `fixed`/docs/chore fragments → **patch**.

If unsure between two, ask the user. Confirm whether they want a real PyPI
publish (default yes) or a dry run.

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

## 8. Sync the consumer skill in the marketplace

skeino has a **consumer-facing skill** in the `ranjitjana027/skills` marketplace
(`skills/skeino/SKILL.md`, plugin `skeino` in `.claude-plugin/marketplace.json`)
that documents the public surface for agents helping users *use* skeino. If this
release changed anything user-visible — settings, endpoints, install/extras,
defaults, or scope — update that skill so it doesn't drift.

```bash
gh repo clone ranjitjana027/skills /tmp/skills-repo
cd /tmp/skills-repo
# Update on the open skeino-skill PR branch if one exists, else branch off main:
git checkout add-skeino-skill 2>/dev/null || git checkout -b chore/skeino-vX.Y.Z main
# edit skills/skeino/SKILL.md to match this release (settings table, endpoints,
# install extras, the "Targets skeino X.Y.Z+" note, v1 scope/gotchas), then:
python3 -c "import json;json.load(open('.claude-plugin/marketplace.json'))"  # validate
git commit -am "Update skeino skill for vX.Y.Z" && git push
```

Open/refresh the PR (or comment on the existing one) so the marketplace stays in
lockstep with the released surface. Cross-check against `README.md`,
`docs/concepts/configuration.md`, and `docs/concepts/persistence.md`.
