# Track F / F5 — render / overflow gates (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: F4 commit `383ef79` (brand fingerprint passing)

## Acceptance gate

| GPT criterion                          | Status                   | Evidence                                                                 |
| -------------------------------------- | ------------------------ | ------------------------------------------------------------------------ |
| `deck_render` blockers == 0            | **pass**                 | 0 blockers                                                               |
| no off-slide tables                    | **pass**                 | every table fits the 13.333" × 7.5" slide bounds                         |
| title-region drift caught when present | **pass (negative test)** | synth deck with title pushed to 5" triggers `title_drift_outside_region` |

## Validator state (post-F5, all 7 clean)

```
deck_contract:                pass (blockers=0 warnings=0  slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0  sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0  sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=0  stable=15 legacy=0 slides=16/16)
brand_fingerprint:            pass (blockers=0 warnings=0  sha=match)
deck_render:                  pass (blockers=0 warnings=13 slides=16)
```

The 13 render warnings are forward-state debt:

- 12 `footer_missing` — slides 2-15 (excl. cover/legal) don't yet emit a text frame in the bottom band (top ≥ 6.5"). Builder convergence work, separate ticket.
- 1 `legal_disclaimer_missing` — slide 16 legal_notice doesn't contain "SimCorp"/"Confidential"/"disclaimer" tokens. The slide is empty (template-only). Builder fix or template-default content.

These are warnings, not blockers, mirroring the F1/F3 transition policy.

## What landed

`scripts/validate_deck_render.py` (new):

- `validate_render(pptx_path, contract)` opens the .pptx, walks every slide, and runs four geometry-level checks:
  - **title region**: title-bearing shape (ph 144 → TITLE/CTR_TITLE type → first text frame in title band fallback) must have `top ≤ 1.5"`. Cover slide is exempt — its `'Title 1'` layout is center-cover by design.
  - **table off-slide**: every table's bounding box must stay within `13.333" × 7.5"`. Off-slide → blocker.
  - **footer presence**: at least one text frame at `top ≥ 6.5"` on every non-static slide. Missing → warning.
  - **legal disclaimer**: slide marked `static: true` or `id: legal_notice` must carry tokens like "SimCorp" / "Confidential" / "disclaimer". Missing → warning.
- Read-only — never modifies the .pptx or builder.
- Geometry-only — does NOT detect rendered text overflow inside a shape (would require rasterising the slide; F6 will use that pipeline for visual regression).

`tests/test_track_f_render.py` (new) — 4 tests:

- canonical anchor passes (0 blockers, 13 warnings expected)
- synth deck with off-slide table (12" left + 5" wide → off the right edge) triggers `table_off_slide`
- synth deck with title pushed to 5" (way below the 1.5" region cap) triggers `title_drift_outside_region`
- cover slide is exempt — no title-related findings on slide 1

`.github/workflows/track-e-validators.yml`:

- Path filter extended to `scripts/validate_deck_render.py`. The new validator runs as part of the Track E validator workflow on every PR touching the relevant paths.

## What's deferred

GPT's spec listed five render gates: text overflow, table overflow, missing footer, missing source note, title drift. Of those:

- table overflow → blocker (implemented)
- title drift → blocker (implemented)
- missing footer → warning (implemented; will tighten when builder converges)
- text overflow → deferred to F6 (needs rasterisation; not pure geometry)
- missing source note → currently subsumed by footer-missing (both check for text in the bottom band). Could split if needed.

## Tests

43 Track E + Track F tests pass (32 + 7 + 4 new). 693/693 unrelated tests pass (was 689 pre-F5; +4 from F5).

## Hard NO-GOs preserved

- No edits to `scripts/build_deck_from_excel.py` in F5 (the override stands but wasn't needed; F5 is read-only).
- No dashboard builder edits.
- No golden visual baseline (F6 — coming next).
- No release catalog/waivers, lineage events, reusable workflows.
- `profiles.control_deck` stays deferred.

## Next: F6

Golden visual baseline + frozen-region regression. Render every slide to PNG (likely via libreoffice headless), freeze per-slide bounding boxes for static brand regions (logo, footer, legal disclaimer), then assert future runs match within tolerance for those regions while letting dynamic data regions vary.

This is the final Track F sub-milestone. After F6, Track F's acceptance gate is fully met and the integration PR can be marked ready-for-merge.

## Files

| File                         | Purpose                                     |
| ---------------------------- | ------------------------------------------- |
| F5-RESULTS.md                | this summary                                |
| deck_render_report.{json,md} | post-F5 render validator output (sanitized) |
