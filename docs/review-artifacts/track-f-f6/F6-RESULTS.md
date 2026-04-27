# Track F / F6 — golden visual baseline + regression (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: F5 commit `bd6262a` (render gates passing)

## Acceptance gate

| GPT criterion                          | Status   | Evidence                                                                              |
| -------------------------------------- | -------- | ------------------------------------------------------------------------------------- |
| Golden visual baseline exists          | **pass** | `tests/fixtures/track_f/golden_baseline/manifest.json` (16 slides, 16 frozen regions) |
| Frozen-region visual regression passes | **pass** | re-render → 0 drift findings against the baseline                                     |
| Negative control: drift is detected    | **pass** | mutating the cover slide triggers `frozen_region_drift` blocker                       |

## Validator state (post-F6, all 8 clean)

```
deck_contract:                pass (blockers=0 warnings=0  slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0  sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0  sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=0  stable=15 legacy=0 slides=16/16)
brand_fingerprint:            pass (blockers=0 warnings=0  sha=match)
deck_render:                  pass (blockers=0 warnings=13 slides=16)
deck_visual_regression:       pass (blockers=0 slides=16/16)
```

## Pipeline

`.pptx` → PDF (libreoffice headless via `/opt/homebrew/bin/soffice --convert-to pdf`) → per-slide PNGs (`pdftoppm -png -r 100`) → crop frozen regions per slide (PIL) → SHA-256 of cropped pixels → compare to baseline.

## Frozen regions

`config/deck_visual_regions.yaml` — start conservative:

- **defaults** (slides 2-15): `footer_band` = bottom 8% strip (where SimCorp template chrome lives)
- **cover slide**: `full_slide` (whole-slide hash; the SimCorp brand cover layout)
- **legal_notice slide**: `full_slide` (entirely template-static)

Coordinates are normalised (0.0-1.0) so they survive DPI changes. Future iterations can split regions further (e.g., separate logo region from footer region) without breaking the baseline format.

## What landed

`scripts/render_deck_to_images.py` (new):

- Helper that runs `soffice --convert-to pdf` then `pdftoppm -png` to produce per-slide PNGs at configurable DPI. Returns ordered list of slide-N.png paths.

`scripts/validate_deck_visual_regression.py` (new):

- Two modes:
  - `--mode capture` — render + hash, write baseline manifest. Used to seed or re-bless baseline after intentional brand change.
  - `--mode verify` (default) — render + hash + compare against committed baseline. Mismatch → blocker `frozen_region_drift`.
- Crops each slide's PNG to declared frozen regions, hashes raw RGB bytes (SHA-256), compares hex-string-equal to baseline.

`config/deck_visual_regions.yaml` (new):

- Schema-versioned region declarations. `defaults.frozen_regions` for non-overriden slides; per-slide-id overrides for cover and legal_notice.

`tests/fixtures/track_f/golden_baseline/manifest.json` (new, committed):

- 16 slides × 1 frozen region each = 16 hashed regions. Captures the canonical shape of the post-F1+F2+F3+F5 deck. Persists in repo so CI can verify.

`tests/test_track_f_visual_regression.py` (new) — 2 tests:

- positive: APAC anchor verifies clean against baseline (16/16 slides match)
- negative: adding a 4"x4" rectangle to the cover triggers `frozen_region_drift` on slide 1

`.github/workflows/track-e-validators.yml`:

- Path filter extended to `validate_deck_visual_regression.py`, `render_deck_to_images.py`, `config/deck_visual_regions.yaml`, and `tests/fixtures/track_f/**`. CI now gates the visual regression on every PR touching those paths.

## What's deferred

- **Perceptual hash tolerance** for anti-aliasing variance across renderers. Today's bit-exact SHA-256 may flake when a different libreoffice version produces sub-pixel-different output. If/when it flakes in CI, swap SHA for imagehash + Hamming-distance threshold.
- **Per-region pixel-diff visualisation** — when a region drifts, the report emits hash before/after but not a visual diff PNG. A future enhancement.
- **Sub-region splits** — cover and legal slides hash whole-slide today; could split into logo / disclaimer-text / footer for finer-grain detection.

## Tests

45 Track E + Track F tests pass (32 + 13 F: 7 brand + 4 render + 2 visual). 695/695 unrelated tests pass.

## Hard NO-GOs preserved

- No edits to `scripts/build_deck_from_excel.py` in F6 (the override stands but wasn't needed; F6 is read-only).
- No dashboard builder edits.
- No release catalog/waivers, lineage events, reusable workflows.
- `profiles.control_deck` stays deferred.

## Track F final state

**All 6 sub-milestones complete:**

| #   | Milestone                           | Acceptance gate                           |
| --- | ----------------------------------- | ----------------------------------------- |
| F1  | Stable titles + takeaway split      | `legacy_verbose_title_count = 0` ✅       |
| F2  | Stable table headers                | `legacy_header_drift_count = 0` ✅        |
| F3  | SF drill-through link               | `missing_required_link_transition = 0` ✅ |
| F4  | Template-first + brand fingerprint  | brand SHA + layouts + theme pass ✅       |
| F5  | Render / overflow gates             | `deck_render` blockers = 0 ✅             |
| F6  | Golden visual baseline + regression | baseline + drift detection ✅             |

GPT's full Track F acceptance gate met:

```
pptx_contract.blocker_count = 0
pptx_contract.legacy_verbose_title_count = 0
pptx_contract.legacy_header_drift_count = 0
pptx_contract.missing_required_link_transition = 0
all required Salesforce links pass kind-aware validation
brand_fingerprint passes
render_gates pass
visual_regression baseline + frozen-region diff pass
```

Branch ready for ready-for-merge mark on PR #23.

## Files

| File                               | Purpose                                     |
| ---------------------------------- | ------------------------------------------- |
| F6-RESULTS.md                      | this summary                                |
| deck_visual_regression_report.json | post-F6 visual validator output (sanitized) |
