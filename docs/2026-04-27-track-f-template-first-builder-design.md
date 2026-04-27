# Track F — Template-first builder + builder convergence (design)

- **Integration branch:** `integration/track-f-template-first-builder`
- **Anchored predecessor:** Track E M1, merged on `main` as squash commit `295df21` on 2026-04-27.
- **Builder lineage in scope:** `scripts/build_deck_from_excel.py` (Sales Director Monthly LAND deck builder, PowerPoint output, 4129 LOC). Invoked from `scripts/run_monthly_director_review.py:907` with `--land-only`; output lands at `output/simcorp_director_decks/<date>/land-only/<director>-LAND.pptx`. **Note:** the design originally named `build_sd_monthly_deck_v2.py`, which is orphaned (no production caller). User expanded the override 2026-04-27 to the actual production builder.
- **Status:** scoping → awaiting explicit go-ahead before any code edits to the builder land on this branch.

## Why this milestone exists

Track E M1 governs the director-facing Sales Director Monthly deck through `config/deck_contract.yaml`, `config/director_workbook_contract.yaml`, and five validators. Against the live APAC anchors the validators currently report:

```
pptx_contract: pass (blockers=0 warnings=31 stable=1 legacy=16 slides=18/18)
                    breakdown: 16 legacy_verbose_title
                             + 14 legacy_header_drift
                             + 1  missing_required_link_transition
```

Every one of the 31 warnings is **forward-state debt the builder owes**, not a contract bug. The contract describes the target output; the production builder hasn't caught up. Track F closes that gap by converging the builder onto the contract.

This is a **builder-convergence milestone, not another contract milestone** (per GPT 2026-04-27).

## Important: builder-edit policy override

The project's general rule (CLAUDE.md, `feedback_no_python_builders.md`) is:

> Never edit Python builder files (`build_*.py`).

That rule was scoped to **CRM Analytics dashboard builders** — the `build_<dashboard>.py` files that duplicate Wave API dashboard state. The reasoning: patch live dashboards via Wave API, don't maintain a parallel Python tree.

`scripts/build_deck_from_excel.py` is a different category:

- It produces PowerPoint, not a Salesforce Wave dashboard.
- There is no Wave API equivalent to "patch instead."
- It is the canonical production lane for the LAND deck Track E governs: `run_monthly_director_review.py --land-only` → `build_deck_from_excel.py` → `output/simcorp_director_decks/<date>/land-only/<director>-LAND.pptx`. (The cadence-lane Shell deck under `output/sales_director_canonical_shells/` is a different artifact, produced by `build_sales_director_monthly_shell_v2.js` from Node, and is NOT in Track F scope.)

Track F **must** edit this file to deliver the GPT-defined acceptance gate. This is a deliberate exception to the general no-touch rule, scoped to the deck builder lineage only. All other `build_*.py` files (dashboard builders) remain untouchable under the standing rule.

**Confirm-before-edit gate:** every sub-milestone below that touches `build_deck_from_excel.py` lands as its own commit on this branch. The first commit was the design doc seed; this commit corrects the file name. Builder edits start at F1 (next commit).

## Acceptance gate (Track F done means)

From GPT:

```
pptx_contract_report:
  blocker_count = 0
  legacy_verbose_title_count = 0
  legacy_header_drift_count = 0
  missing_required_link_transition = 0
  all required Salesforce links pass kind-aware validation
```

Plus:

```
brand_fingerprint:
  template_sha256_matches_contract = true
  required_layouts_present = true
  required_theme_colors_present = true
  required_theme_fonts_present = true

render_gates:
  no_text_overflow
  no_table_overflow
  no_missing_footer
  no_missing_source_note
  no_title_drift_outside_allowed_region

visual_regression:
  golden_baseline_exists = true
  per_slide_frozen_region_diff_within_tolerance = true
```

## Sub-milestones (F1 — F6)

Each sub-milestone is one named PR-sized unit on this branch. Acceptance criteria gate the next one starting.

### F1 — Stable titles + takeaway split

**Scope:** convert the production builder's dynamic sentence-style slide titles into the stable short titles defined in `deck_contract.yaml::profiles.director_monthly.slides[*].title`. Move the dynamic narrative into a takeaway block underneath the title, populated from `slides[*].required_takeaway.template` + `required_metrics`.

**Files touched:** `scripts/build_deck_from_excel.py` (slide-construction functions for slides 2–17; cover and legal stay as-is).

**Builder change pattern:**

```python
# before (current production)
slide.title = f"Q2 {territory}: {q2_open_deal_count} deals, {q2_open_arr_eur}. ..."

# after (Track F1)
slide.title = "Q2 Deal Readiness"  # from deck_contract.yaml
slide.add_takeaway(
    template=contract.slides["q2_deal_readiness"].required_takeaway.template,
    metrics={
        "territory": territory,
        "q2_open_deal_count": q2_open_deal_count,
        "q2_open_arr_unweighted_eur": format_eur(q2_open_arr_unweighted),
        "q2_readiness_summary": q2_readiness_summary,
    },
)
```

**Acceptance:**

- `pptx_contract_report.legacy_verbose_title_count == 0` against the live APAC anchor
- `stable_title_count == 17` (16 narrative slides + cover; legal slide has no title)
- 16 `legacy_title_patterns` entries can be **removed** from `deck_contract.yaml` (forward-state target reached); leave a deprecation comment for one cycle
- All Track E M1 tests still pass, the legacy-title test pivots from "matches legacy pattern → warning" to "stable_title is the only path"

**Out of scope for F1:** table headers (F2), Salesforce link (F3), template-first (F4), render gates (F5), visual regression (F6).

---

### F2 — Stable table headers

**Scope:** builder emits the stable contract `tables[].columns[].header` strings instead of the production-current column names.

**Files touched:** `scripts/build_deck_from_excel.py` (table-rendering functions for the 14 tables tracked under `legacy_header_sets`).

**Acceptance:**

- `pptx_contract_report.legacy_header_drift_count == 0` against the live APAC anchor
- 14 `legacy_header_sets` entries removed from `deck_contract.yaml`
- A new `tests/test_track_f_*.py` suite asserts: every table on every non-static slide emits `tables[].columns[].header` exactly
- Q1 Forecast Variance bridge keeps its `derived_table` shape; the named transform `q1_forecast_variance_bridge` lands as a real function this milestone (currently registered as a string)

**Note:** F2 is the largest sub-milestone — table-rendering touches more code than title-rendering. Worth its own PR.

---

### F3 — Pushed Deals Salesforce drill-through hyperlink

**Scope:** slide 12 (`pushed_deals`) emits a real hyperlink to the Salesforce Lightning Opportunity list view, scoped to the director's pushed deals.

**Files touched:** `scripts/build_deck_from_excel.py` (slide 12 construction); possibly `config/sales_director_md1_presets.json` to add a per-director `pushed_deals_list_view_url` if not already present.

**Acceptance:**

- `pptx_contract` no longer emits `missing_required_link_transition` for slide 12
- The link's hyperlink address satisfies `_link_satisfies_kind(addr, "salesforce_list_view")` (matches `simcorp.{lightning.force,my.salesforce}.com/(lightning/)?o/Opportunity/list`)
- Test: synthesized PPTX with the new builder output passes the kind-aware validation

---

### F4 — Template-first builder + brand fingerprint

**Scope:** stop the builder from constructing decks via `Presentation()` and switch it to `Presentation(deck_contract.brand.template)` so every slide inherits the SimCorp master. Add a brand fingerprint validator that asserts the loaded template matches the SHA-256 declared in the contract and that all required layouts/theme tokens are present.

**Files touched:**

- `scripts/build_deck_from_excel.py` (template loading)
- `scripts/monthly_platform/brand_contract.py` (new)
- `scripts/validate_deck_brand.py` (new)

**Acceptance:**

- `assets/SimCorp_PPT_Template.pptx` SHA-256 matches `deck_contract.yaml::brand.expected_template_sha256` (`7834561e…4195`)
- All required slide_master layouts referenced by `slides[*].layout` are present on the template (currently: `cover`, `kpi_cards`, `kpi_cards_with_link`, `table_with_takeaway`, `dual_table`, `quad_table`, `legal`)
- All theme colors and fonts under `brand.theme.*` resolve against the template
- Existing six (now seventeen) slide titles still render
- Brand fingerprint report emits `output/track_f/brand_fingerprint_report.json` with `status: pass`

---

### F5 — Render / overflow gates

**Scope:** add a render-time validator that takes a produced PPTX and checks for: text overflow inside its bounding box, table overflow off-slide, missing footer text, missing source-note text, slide title drift outside the title region defined by the layout.

**Files touched:**

- `scripts/validate_director_monthly_pptx_render.py` (new)
- `tests/test_track_f_render_gates.py` (new)

**Acceptance:**

- Render report `output/track_f/pptx_render_report.json` has `status: pass`, zero overflow/missing-element findings
- A negative control: synthesizing a too-long takeaway → render gate emits `text_overflow` blocker

---

### F6 — Golden visual baseline + regression

**Scope:** render every slide of the produced PPTX to PNG, freeze a per-slide "frozen-region" set of bounding boxes (title, footer, logo, source-note) as the golden baseline, and assert future runs match the golden within tolerance for those frozen regions while letting dynamic data regions vary.

**Files touched:**

- `scripts/render_deck_to_images.py` (new)
- `scripts/validate_deck_visual_regression.py` (new)
- `config/deck_visual_regions.yaml` (new — frozen vs dynamic region map)
- `tests/fixtures/track_f/golden_baseline/` (PNG/JSON of the canonical slide images)

**Acceptance:**

- First run captures + commits the golden baseline against the post-F1-through-F5 builder output
- Visual regression report has `status: pass`
- A negative control: deliberately changing the SimCorp logo position triggers a frozen-region diff finding

## Hard NO-GOs (preserved from M1, refreshed for F)

- No edits to **dashboard builders** (`build_<dashboard>.py` files that duplicate Wave API state). The standing rule still applies; only `build_deck_from_excel.py` (and helpers it imports) is in scope.
- No release catalog or waivers (Track K).
- No OpenLineage events (Track J).
- No reusable workflows (Track L).
- No `monthly_director_bundle_contract.json` `optional_empty` policy refresh — that's Track E/M2 (`docs/review-artifacts/track-e-m1/M2-CLEANUP-TICKET.md`), out of scope for F.
- No `profiles.control_deck` implementation; remains `status: deferred`.
- No warehouse-only rewrite of the director-deck data path; the deck still consumes the live workbook.

## Operating model

- One integration branch (`integration/track-f-template-first-builder`).
- One sub-PR per F1 / F2 / F3 / F4 / F5 / F6, each landing as its own commit on the branch (or its own PR opened against this branch then squashed in).
- Acceptance criteria are gates: F2 doesn't start until F1's `legacy_verbose_title_count == 0` is observed against the live anchor.
- Each sub-milestone updates the relevant `pptx_contract_report` evidence in `docs/review-artifacts/track-f-mN/` (where N = F1, F2, …) so the convergence is visible in the repo, not just locally.
- The Track E CI workflow (`.github/workflows/track-e-validators.yml`) keeps protecting the contract; a new `.github/workflows/track-f-validators.yml` will be added in F4/F5 to gate brand and render checks on PRs touching the builder.

## Implementation order

1. **This commit (now):** design doc only, no code.
2. F1 PR: stable titles + takeaway split.
3. F2 PR: stable table headers + Q1 forecast variance transform realised.
4. F3 PR: Pushed Deals SF drill-through link.
5. F4 PR: template-first + brand fingerprint.
6. F5 PR: render / overflow gates.
7. F6 PR: golden visual baseline.
8. Squash-merge `integration/track-f-template-first-builder` to `main`.
9. Hand back to GPT for Track G (full v20d release pass) sign-off planning.

## Decision required from user before F1 starts

Before any builder code edits land on this branch:

1. ~~Confirm Track F's narrow override of the no-touch rule is acceptable.~~ **CLEARED 2026-04-27** — override granted for `scripts/build_deck_from_excel.py` (and its imported helpers).
2. Confirm sub-milestone order F1 → F6 (or pick a different sequence — e.g. F4 template-first first, then converge titles/headers under the new template).
3. Confirm scope of the per-sub-milestone PR boundary — is each F-sub-PR opened as a separate GitHub PR, or all squashed onto this branch and merged once?
