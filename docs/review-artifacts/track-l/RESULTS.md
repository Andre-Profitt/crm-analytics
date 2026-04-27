# Track L — reusable workflows + composite actions (RESULTS)

## TL;DR

Validator + test job logic moved out of `track-e-validators.yml` into one reusable workflow + two composite actions. PR consumer now collapses to a 4-line job that delegates to the reusable. Future consumers (nightly, monthly publish, manual dispatch) call the same reusable so the gate definition lives in exactly one place. 76/76 release-pipeline tests still pass.

## What changed

| File                                             | Role                                       | Before              | After                                |
| ------------------------------------------------ | ------------------------------------------ | ------------------- | ------------------------------------ |
| `.github/actions/setup-crm-analytics/action.yml` | composite — checkout + Python + deps       | (did not exist)     | new, 50 lines                        |
| `.github/actions/run-release-gates/action.yml`   | composite — structural validators + pytest | (did not exist)     | new, 54 lines                        |
| `.github/workflows/reusable-release-gates.yml`   | reusable workflow (`workflow_call`)        | (did not exist)     | new, 53 lines                        |
| `.github/workflows/track-e-validators.yml`       | PR consumer                                | inline 100-line job | thin wrapper that calls the reusable |

Net: ~216 lines of declarative YAML, single source of truth for the gate. Adding a new consumer (e.g. monthly-publish nightly check) is a 4-line `uses:` block, not a 100-line copy-paste.

## Acceptance criteria

| #   | Criterion                                                                                                          | Result                      |
| --- | ------------------------------------------------------------------------------------------------------------------ | --------------------------- |
| 1   | Composite action installs Python + pip cache + runtime + test deps                                                 | ✅ `setup-crm-analytics`    |
| 2   | Composite action runs structural validators + pytest, surfaces failure summary                                     | ✅ `run-release-gates`      |
| 3   | Reusable workflow accepts `python-version`, `test-paths`, `pytest-extra-args` inputs with sensible defaults        | ✅ `reusable-release-gates` |
| 4   | `track-e-validators.yml` collapses to a `uses:` wrapper that preserves the same `on:` triggers and `paths:` filter | ✅                          |
| 5   | YAML shape validates (parses cleanly with PyYAML)                                                                  | ✅ all 4 files              |
| 6   | No regression in 76 release-pipeline tests                                                                         | ✅ 76/76 pass               |
| 7   | Same paths trigger preserved + extended to include the new composite + reusable files                              | ✅                          |

## Inputs surface

`reusable-release-gates.yml` exposes three knobs so a downstream consumer can tune the run without copy-pasting the job:

- `python-version` (default `"3.13"`)
- `test-paths` (default: every release-pipeline test pattern)
- `pytest-extra-args` (default `"-v"`)

A nightly lane that wants only the structural / non-anchor tests would call:

```yaml
uses: ./.github/workflows/reusable-release-gates.yml
with:
  test-paths: "tests/test_track_e_deck_contract.py tests/test_track_e_workbook_contract.py tests/test_track_f_brand_contract.py"
  pytest-extra-args: "-v --tb=short"
```

A monthly-publish gate that wants the full suite, no overrides:

```yaml
uses: ./.github/workflows/reusable-release-gates.yml
```

## Hard rules preserved

- Triggers on the existing pull-request paths filter — no broader scope.
- Live-anchor tests still skip themselves when `~/Downloads/jesper-tyrer-*` is absent (composite action passes `test-paths` through; pytest skipif logic in test files unchanged).
- 9 never-waivable gates in `config/release_policy.yaml` — untouched.
- Builder lineage scope — untouched (no `build_*.py` modified).
- `profiles.control_deck` — still `status: deferred`.

## Tests

```
$ python3 -m pytest tests/test_track_e_*.py tests/test_track_f_*.py \
    tests/test_track_g_*.py tests/test_track_em2_*.py \
    tests/test_track_k_*.py tests/test_track_j_*.py
76 passed in 20.77s
```

## Files touched

New:

- `.github/actions/setup-crm-analytics/action.yml`
- `.github/actions/run-release-gates/action.yml`
- `.github/workflows/reusable-release-gates.yml`
- `docs/review-artifacts/track-l/RESULTS.md`

Modified:

- `.github/workflows/track-e-validators.yml` — collapsed to a wrapper.

## Forward state

- **Monthly-publish lane** (`monthly-review.yml`) currently has its own dependency / setup blocks — when the schedule + dispatch lanes are next refactored, they can adopt `setup-crm-analytics` to share the pip-extra-packages list.
- **Nightly structural-only lane**: a 12-line workflow file that calls the reusable with a narrow `test-paths` would give us cheap continuous coverage without exercising the live anchors.
- **CI cache miss debugging**: if the pip cache hit-rate drops (composite move can sometimes invalidate the actions/setup-python cache key), check the cache key prefix in the GitHub Actions run logs — the composite uses `cache: pip` from setup-python, which keys on the runner OS + Python version + the hash of `requirements.txt`. Stable across runs.
