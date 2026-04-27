# Track F / F3 — Salesforce drill-through hyperlink (results)

Branch: `integration/track-f-template-first-builder`
Predecessor: F2 commit `fdcaf1f` (legacy_header_drift_count = 0)

## Acceptance gate

| GPT criterion                                                                            | Status   | Evidence                                                                              |
| ---------------------------------------------------------------------------------------- | -------- | ------------------------------------------------------------------------------------- |
| `pptx_contract.missing_required_link_transition == 0`                                    | **pass** | 1 → 0                                                                                 |
| Pushed Deals slide hyperlink passes `_link_satisfies_kind(addr, "salesforce_list_view")` | **pass** | URL matches `simcorp.lightning.force.com/lightning/o/Opportunity/list?filterName=...` |

## Validator state (post-F3)

```
deck_contract:                pass (blockers=0 warnings=0 slides=16)
director_workbook_contract:   pass (blockers=0 warnings=0 sheets=13 roles=9)
director_workbook_validation: pass (blockers=0 warnings=0 sheets=13/13 roles_resolved=9/9)
deck_bindings:                pass (64/64 pass, warn=0 fail=0)
pptx_contract:                pass (blockers=0 warnings=0 stable=15 legacy=0 slides=16/16)
```

**All 5 validators clean. 0 blockers, 0 warnings.** All three M1 transition warning categories closed: 15 verbose titles + 13 header drifts + 1 missing link → 0.

## Approach

`scripts/build_deck_from_excel.py::slide_pushed_deals_with_link` already had the per-territory `PI_LINKS` URL map in place (9 territories × Lightning Opportunity list URLs). The function was emitting the URL as plain _text_ in a footer textbox, not as a hyperlink. F3 split the textbox content into two runs:

- prefix run: `"Open Pipeline Inspection in Salesforce: "` (no hyperlink)
- link run: the URL itself, with `run.hyperlink.address = link` set

The PPTX checker's M1 Cond 2 logic walks every run on every shape and checks `run.hyperlink.address` against the kind-specific URL pattern. The existing `salesforce_list_view` regex (`simcorp(\.lightning\.force\.com|\.my\.salesforce\.com)/(lightning/)?o/Opportunity/list`) already accepts the production Lightning URLs.

## File changes

- `scripts/build_deck_from_excel.py` — split footer-link textbox into prefix run + link run with `hyperlink.address`. ~14 lines, slide_pushed_deals_with_link only.
- `tests/test_track_e_pptx_checker.py::test_apac_anchor_passes_in_legacy_mode`:
  - flipped `assert any(missing_required_link_transition)` → `assert not any(...)`
  - added `assert report["warning_count"] == 0` (F1+F2+F3 close every transition warning class)

## Hard NO-GOs preserved

- No edits outside the override allowlist.
- No template-first builder (F4).
- No render gates (F5), golden visuals (F6).
- No release catalog/waivers, OpenLineage, reusable workflows.
- `profiles.control_deck` stays deferred.

## What stays open for F4 — F6

- F4 template-first builder + brand fingerprint: not started. Builder already loads from `assets/SimCorp_PPT_Template.pptx` via `Presentation(template_path)` (line 3685); F4 adds the brand-fingerprint validator that pins template SHA + required layouts + theme tokens.
- F5 render / overflow gates: not started.
- F6 golden visual baseline + regression: not started.

## Files

| File                           | Purpose                       |
| ------------------------------ | ----------------------------- |
| F3-RESULTS.md                  | this summary                  |
| pptx_contract_report.{json,md} | post-F3 PPTX validator output |
