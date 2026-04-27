# Track F render-gate tightening — footer + legal disclaimer now blockers

Branch: `integration/track-f-template-first-builder`
Predecessor: M3 ticket refresh `7f5004b`

## Summary

`footer_missing` and `legal_disclaimer_missing` were warnings in F5 (transition policy while the builder didn't yet emit them). With this commit:

- Builder emits a source-note footer on every non-static slide.
- Validator detects template-layout disclaimer text on the legal slide.
- Both checks flip from warning → blocker.

## Validator state (all 8 clean)

```
deck_contract:                pass (blockers=0 warnings=0 slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0 sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0 sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=0 stable=15 legacy=0 slides=16/16)
brand_fingerprint:            pass (blockers=0 warnings=0 sha=match)
deck_render:                  pass (blockers=0 warnings=0 slides=16)
deck_visual_regression:       pass (blockers=0 slides=16/16)
```

**Zero warnings across all 8 validators.** Pre-flip state was 13 footer-missing + 1 legal-disclaimer-missing warning.

## What landed

`scripts/build_deck_from_excel.py`:

- New helper `_apply_source_note_footer(prs, director, territory, snapshot_date)` runs once at the end of `build_deck()` (just before `prs.save()`).
- For every slide except the cover (index 0) and end slide (last index), it adds a small italic textbox at `top=7.05"` with: `"Source: Salesforce live extract  |  Snapshot {snapshot_date}  |  {director}, {territory}"`.
- Idempotent: skips slides that already have a text frame in the footer band (`top >= 6.5"`) so the existing pushed_deals SF-link footer doesn't get double-stamped.

`scripts/validate_deck_render.py`:

- Legal disclaimer check now reads slide-shape text AND slide-layout text. The SimCorp `'End slide with disclaimer 1'` template carries the disclaimer on layout-level placeholders that python-pptx does NOT surface via `slide.shapes`. Layout-text fallback is the correct check.
- `footer_missing` severity flipped from `warning` → `blocker`.
- `legal_disclaimer_missing` severity flipped from `warning` → `blocker`.
- When either fires, slide_overall is set to `fail`.

`tests/fixtures/track_f/golden_baseline/manifest.json`:

- Re-captured against the post-footer build; the bottom 8% strip on slides 2-15 now includes the source-note text, so the per-slide hashes shifted. New baseline locks the footer-bearing chrome.

`~/Downloads/jesper-tyrer-LAND.pptx`:

- Refreshed to the post-footer build.

## Tests

45 Track E + Track F tests pass. 699/699 unrelated tests pass.

## Hard NO-GOs preserved

- Only `scripts/build_deck_from_excel.py` was edited from the override allowlist.
- No dashboard builder edits.
- No release catalog/waivers, lineage events, reusable workflows.
- `profiles.control_deck` stays deferred.

## Forward-state debt closed

- ~~Render gate: tighten `footer_missing` from warning → blocker once builder adds explicit footers on slides 2-15.~~ **Done.**
- ~~Render gate: tighten `legal_disclaimer_missing` from warning → blocker.~~ **Done** (via layout-text fallback in the validator).

## Files

| File                    | Purpose                             |
| ----------------------- | ----------------------------------- |
| RESULTS.md              | this summary                        |
| deck_render_report.json | post-flip render report (sanitized) |
| deck_render_report.md   | post-flip render report (markdown)  |
