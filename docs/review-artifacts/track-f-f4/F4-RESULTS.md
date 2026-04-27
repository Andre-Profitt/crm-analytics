# Track F / F4 — template-first builder + brand fingerprint (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: F3 commit `8a70b0f` (every M1 transition warning closed)

## Acceptance gate

| GPT criterion                                                   | Status           | Evidence                                                                                                    |
| --------------------------------------------------------------- | ---------------- | ----------------------------------------------------------------------------------------------------------- |
| Deck loads approved SimCorp template first                      | **already true** | `build_deck_from_excel.py:3685` does `Presentation(template_path)`; F4 just adds the validator that pins it |
| `brand_fingerprint` passes template SHA / layout / theme checks | **pass**         | `brand_fingerprint_report.json` — `status: pass`, 0 blockers, 0 warnings                                    |

## Validator state (post-F4)

```
deck_contract:                pass (blockers=0 warnings=0 slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0 sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0 sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=0 stable=15 legacy=0 slides=16/16)
brand_fingerprint:            pass (blockers=0 warnings=0 sha=match layouts_missing=0)
```

All 6 Track E/F validators clean.

## What landed

`scripts/monthly_platform/brand_contract.py` (new):

- `validate_brand(contract)` reads the template file at `brand.template`, computes SHA-256, verifies it matches `brand.expected_template_sha256`, plus `expected_slide_master_count`, every `required_layouts` entry, and theme color hex syntax.
- 7 finding codes (blockers: `template_missing`, `template_sha256_mismatch`, `slide_master_count_mismatch`, `required_layout_missing`, `template_parse_error`, `missing_expected_sha256`; warnings: `template_size_mismatch`, `theme_color_invalid_hex`).

`scripts/validate_deck_brand.py` (new): CLI wrapper.

`config/deck_contract.yaml::brand`:

- `expected_slide_master_count: 1`
- `required_layouts: [Title 1, Title and Content, 2 x content w/ gradient line, 4 x content w/ gradient line, End slide with disclaimer 1]` (the 5 layouts the builder uses)

`schemas/deck_contract.schema.json`: new optional `brand.expected_slide_master_count` + `brand.required_layouts`.

`tests/test_track_f_brand_contract.py` (new) — 7 tests covering canonical pass + 6 negative controls (SHA mismatch, missing template, missing layout, master-count mismatch, invalid color warns, size mismatch warns).

`.github/workflows/track-e-validators.yml`: extended path filter + new CI step `python3 scripts/validate_deck_brand.py`; pytest now runs both `test_track_e_*.py` and `test_track_f_*.py`.

## Live state

- template: `assets/SimCorp_PPT_Template.pptx`
- SHA-256: `7834561e83403c4c2f7b1150953a860c23c8e1caf81ef3c7f2fefb0446514195` (matches contract)
- size: 9,058,051 bytes
- slide_master_count: 1
- required_layouts: 5/5 present
- theme_color_count: 13 (all valid hex)

## Tests

39 Track E+F tests pass (32+7). 689 unrelated tests pass.

## Why F4 was small

GPT's design doc had F4 as "deck loads approved SimCorp template first" + "brand fingerprint validator." The builder _already_ loaded the SimCorp template via `Presentation(template_path)` (the design doc explicitly noted this). So F4 boiled down to building the validator + wiring it into the contract & CI. No builder edit was required.

## Hard NO-GOs preserved

- No edits to `build_deck_from_excel.py` in F4 (override stands but wasn't needed).
- No dashboard builder edits.
- No render gates (F5).
- No golden visual baseline (F6).
- No release catalog/waivers, lineage events, reusable workflows.
- `profiles.control_deck` stays deferred.

## What stays open for F5 — F6

- F5 render / overflow gates: detect text/table overflow, missing footer, missing source note, title drift outside layout's title region.
- F6 golden visual baseline + regression: render slides to PNG, freeze per-slide regions, assert future runs match for static brand regions while letting dynamic data regions vary.

## Files

| File                          | Purpose                                    |
| ----------------------------- | ------------------------------------------ |
| F4-RESULTS.md                 | this summary                               |
| brand_fingerprint_report.json | post-F4 brand validator output (sanitized) |
