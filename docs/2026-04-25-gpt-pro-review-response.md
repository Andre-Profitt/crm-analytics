# GPT Pro Review Response — v2 Cycle (2026-04-25)

Source: ChatGPT/GPT Pro
Brief reviewed: [`docs/2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md`](./2026-04-25-gpt-pro-handoff-v2-etl-build-sharpness.md)
Baseline: `live-all-sources-pipeline-open-v20c` (snapshot `2026-04-30`)
Captured: 2026-04-25

> **Bottom line:** v20c is publish-green but not change-safe. The brief is right to push on whether the gates are strong enough, not whether they are currently green.

> **Caveat from reviewer:** the verbatim review below was based on GitHub-readable Markdown + JSON evidence. Binary `.pptx` / `.xlsx` artifacts in `docs/review-artifacts/v20c/` were not inspected inline — the repo's own README flags those as not inline-readable.

---

## Five-Track Verdict

### 1. Source registry governance — green evidence, weak promotion control

The current source-backed lane proves all selected Salesforce sources extracted cleanly in v20c, but the brief correctly identifies missing "row-bound teeth," freshness SLA, quarter-roll source promotion, and owner notification. That is a **governance gap, not an extraction gap**.

The sharpest change is to add a source promotion workflow with these fields per registry requirement:

- `owner_email`
- `freshness_sla_days`
- `expected_quarter_role`
- `promotion_state`
- `baseline_group`
- `last_reviewed_at`

The gate should fail on unapproved promotion drift, stale source metadata, or missing owner notification.

| Risk before                                  | Risk after                                                                | Cost   |
| -------------------------------------------- | ------------------------------------------------------------------------- | ------ |
| A stale-but-valid report/list view can pass. | A stale or unreviewed source can still extract, but cannot silently ship. | Medium |

### 2. Period semantics split — canonical lane is right; scheduled legacy lane is dangerous

The source-backed branch has canonical period handling, including quarter mapping evidence (`FY26 Q1` → `Q2 2026` display, approved). **The problem is that the GitHub Actions monthly workflow still schedules the legacy runner monthly and defaults the snapshot date to "today" when no manual date is supplied.** The workflow invokes `scripts/run_monthly_director_review.py`, while the brief calls out legacy `datetime.now()` usage in `build_deck_from_excel.py`.

The right move is **not another abstraction layer**. Repoint or disable the old cron immediately. The scheduled workflow should call the source-backed runner with an explicit snapshot date, and the legacy builder should be frozen behind an explicit "legacy only" path.

| Risk before                                 | Risk after                                                 | Cost |
| ------------------------------------------- | ---------------------------------------------------------- | ---- |
| Running on a different day can shift scope. | Every run has a declared snapshot date and quarter policy. | Low  |

### 3. Extraction quality audit — real structure, conservative thresholds

The audit code has the right bones: zero-row, min/max row count, max-record cap, required-field presence, and null thresholds. But the policy defaults are permissive: `allow_zero=True`, `min_rows=0`, and null issues default to warning behavior. The current code also escalates below-min-row severity through `zero_row_action`, which **blurs separate concepts**: zero-row acceptability and minimum-row breach severity.

The next gate should add **historical baselines and semantic distribution checks**. For example, "Stage 5 deals disappeared" should be detected by comparing stage distribution against a prior approved baseline, not by row count alone. Add `min_rows_action`, `distribution_baselines`, `expected_empty_conditions`; do not let "optional empty" remain implicit.

| Risk before                                                                         | Risk after                                                                   | Cost   |
| ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | ------ |
| A filter can silently drop an important subset while total extraction still passes. | Row-count, null-rate, and stage-mix anomalies become reviewable or blocking. | Medium |

### 4. Presentation contract — currently embedded, should be externalized

The source-backed deck builder procedurally creates a six-slide deck and **embeds the visual/manifest expectations inside the builder**. Separately, the table validator hard-codes expected tables, slide numbers, headers, and style rules. The contract exists, but it is **split across code paths rather than declared once**.

Create `config/deck_contract.yaml` and make builder, validators, release manifest, and think-cell source generation read from it. Should include slide IDs, required text, table IDs, headers, JSON bindings, formatting rules, source-footnote expectations, overflow policy, named ranges.

| Risk before                                                                                                          | Risk after                                                                            | Cost                        |
| -------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | --------------------------- |
| Validators pass for today's six slides, but new slides require synchronized edits across builder and validator code. | Adding/changing a slide is a contract edit plus implementation, not a scavenger hunt. | Medium-high (high leverage) |

### 5. Branding and builder sprawl — must collapse around one template-first builder

**The current source-backed builder starts with `Presentation()` and sets slide size directly, rather than loading the SimCorp template as the presentation source. It also hard-codes colors and fonts in code.** That makes the lane functionally reliable but brand-fragile.

Keep `build_source_backed_deck.py` as canonical, but change it to load the template first, consume design tokens from the deck contract, and deprecate the shell builder. Legacy builder should remain only for one parity/migration checkpoint, then be frozen.

| Risk before                                                              | Risk after                                                                       | Cost   |
| ------------------------------------------------------------------------ | -------------------------------------------------------------------------------- | ------ |
| Deck can pass content gates while drifting visually from SimCorp master. | Template inheritance, token checks, golden render regression catch visual drift. | Medium |

---

## Top 5 ETL Changes (Ordered By ROI)

1. **Cut over the scheduled workflow to the source-backed runner.** Current monthly workflow is still cron-based on the legacy path; should be disabled or replaced with the source-backed pipeline using an explicit snapshot date.
2. **Add calibrated source-quality baselines.** Store historical row-count and null-rate baselines per `source_key` × `dataset` × `period_role` × `territory`. Gate compares current extraction against approved baselines; require explicit `--promote-baselines` step for legitimate changes.
3. **Add distribution checks for business-critical fields.** For pipeline sources, compare stage mix, owner/territory mix, quarter mix, open/closed segmentation. Smallest useful gate that catches "Stage 5 deals disappeared" even when row counts still look plausible.
4. **Separate policy actions.** Distinct: `zero_row_action`, `min_rows_action`, `max_rows_action`, `null_threshold_action`, `distribution_action`. Current row-count severity path is too coarse.
5. **Add source owner/freshness promotion evidence.** Registry items should carry owner, approval date, freshness SLA, quarter-roll promotion status. v20c proof says extraction is clean; this proves the **selected sources** are still the right ones.

## Top 5 Build/Deck Changes (Ordered By ROI)

1. **Create `deck_contract.yaml` as single source of truth.** Move slide titles, table headers, required text, style expectations, source notes, and data bindings out of builder/validator constants. Current table contract is explicitly hard-coded in the validator.
2. **Make the source-backed builder template-first.** Replace `Presentation()` with template loading; fail if required SimCorp layouts or design tokens are missing.
3. **Drive table generation and validation from the same contract.** Today builder creates tables procedurally and validator checks a separate hard-coded list. Single contract should feed both.
4. **Add golden visual regression.** Render slides to images and compare against approved baseline for template fidelity, title/footer placement, overflow, brand drift. Current gates good at "deck built," not yet sufficient for executive-grade visual consistency.
5. **Collapse the builder set.** Canonicalize `build_source_backed_deck.py`, deprecate shell builder, freeze legacy Excel builder after one parity run.

---

## Proposed `deck_contract.yaml` Shape

Minimum useful schema:

```yaml
schema_version: monthly_platform.deck_contract.v1
brand:
  template: assets/SimCorp_PPT_Template.pptx
  aspect_ratio: 16:9
  allowed_fonts:
    - Aptos
    - Microsoft Sans Serif
  colors:
    navy: "011946"
    primary_blue: "0E3788"
    aqua: "00AEEF"
    white: "FFFFFF"
data_bindings:
  truth_packet: output/source_backed_pipeline/{snapshot_date}/truth_packet.json
  release_packet: output/source_backed_pipeline/{snapshot_date}/release_packet.json
  bundle_manifest: output/source_backed_pipeline/{snapshot_date}/pipeline_run_manifest.json
slides:
  - id: director_book
    slide_number: 4
    title: "Director book view"
    layout: simcorp_content
    purpose: "Show director-level open book, risk, and source issue posture."
    required_text:
      - "Director book view"
      - "Source-backed monthly review"
    tables:
      - id: tbl.director_book
        source: truth_packet
        json_path: "$.directors[*]"
        min_rows: 1
        max_rows: 12
        columns:
          - id: director
            header: "Director"
            json_path: "$.director"
            format: text
          - id: territory
            header: "Territory"
            json_path: "$.territory"
            format: text
          - id: open_arr
            header: "Open ARR"
            json_path: "$.open_arr"
            format: eur_compact
          - id: open_deals
            header: "Deals"
            json_path: "$.open_deals"
            format: integer
          - id: risk_rows
            header: "Risk rows"
            json_path: "$.risk_rows"
            format: integer
          - id: tieout
            header: "Tie-out"
            json_path: "$.tieout_mismatch_count"
            format: integer
          - id: source_issues
            header: "Source issues"
            json_path: "$.source_issue_count"
            format: integer
        style:
          header_fill: brand.colors.navy
          header_font: brand.colors.white
          body_font: brand.colors.navy
          allow_alternating_fills: true
    render_expectations:
      no_text_overflow: true
      no_table_overflow: true
      min_body_font_pt: 8
      required_footer_source: true
    thinkcell:
      range_name: DirectorBookTable
      include_headers: true
      columns_ref: tbl.director_book.columns
```

The exact YAML syntax is not the point. The point is that **builder, validator, think-cell exporter, and release manifest all consume the same contract**.

---

## Polished Deck Deterministic Gate (Two-Tier)

### Hard Blockers

| Gate                  | What It Proves                                                                                            |
| --------------------- | --------------------------------------------------------------------------------------------------------- |
| Artifact completeness | Deck, workbook, think-cell workbook, release packet, and manifest exist.                                  |
| Contract compliance   | Slide IDs, titles, tables, headers, required text, named ranges, source notes match `deck_contract.yaml`. |
| Data binding          | Every displayed metric maps to a JSON path in the truth/release packet.                                   |
| Brand inheritance     | Deck was built from approved template; theme tokens match expected SimCorp values.                        |
| Layout integrity      | No text overflow, table overflow, placeholder remnants, cropped legends, missing footers.                 |
| Render integrity      | Every slide renders successfully to image/PDF.                                                            |
| Executive readability | Slide title, takeaway, and action owner are present where required.                                       |

### Advisory LLM Critique (Non-Blocking Initially)

- "Can a VP understand the action in 30 seconds?"
- "Does any slide read like an internal audit artifact instead of a decision surface?"
- "Are abbreviations unexplained?"
- "Is there false precision or unsupported confidence?"

Once stable, repeated high-severity critique findings can become blockers.

---

## One-Week Action Plan

| Day | Action                                                                                                                                                               |
| --- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Repoint or disable the legacy monthly cron. Make source-backed lane the scheduled path; require explicit snapshot date.                                              |
| 2   | Add `min_rows_action`, `distribution_action`, `expected_empty_conditions` to source quality policy.                                                                  |
| 3   | Build a baseline calibrator from v20c plus available historical outputs. Store read-only baselines first; do not auto-promote.                                       |
| 4   | Add stage/quarter/territory distribution checks for pipeline sources.                                                                                                |
| 5   | Draft `deck_contract.yaml` for current six slides; update table validator to read from it.                                                                           |
| 6   | Start template-first builder work: load SimCorp template, assert required layouts/tokens, remove most obvious hard-coded brand constants.                            |
| 7   | Run a v20d evidence pass and compare against v20c: source count, extraction quality, quarter mapping, deck contract, render, release packet, SharePoint plan/upload. |

## One-Month Action Plan

| Week | Outcome                                                                                                                                                                        |
| ---- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 1    | Workflow cutover, source-quality calibration, first deck contract.                                                                                                             |
| 2    | Refactor builder + validators to consume the contract; add think-cell range generation from same contract.                                                                     |
| 3    | Deprecate shell builder, freeze legacy, run one parity comparison for historical confidence.                                                                                   |
| 4    | Golden visual regression, source promotion approvals, owner notifications, release-summary catalog explaining what changed, what passed, what was waived, and who approved it. |

---

## Theater Audit (per existing gate)

| Gate                        | Verdict                                                                                                                                            |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| Quarter mapping             | **Real.** Explicit, approved, release-relevant.                                                                                                    |
| Truth / tie-out blockers    | **Real.** Keep as hard blockers.                                                                                                                   |
| Salesforce extraction count | Useful but incomplete. Proves extraction happened, not that the selected source is fresh or semantically correct.                                  |
| Fingerprints                | Useful but underpowered. High signal for drift, but needs owner/freshness/promotion workflow.                                                      |
| Extract quality             | **Partly theater today.** Audit exists, but permissive defaults and no historical baselines make "0 high" less meaningful than it looks.           |
| Visual/polish/table gates   | Good smoke tests. Catch obvious breakage, not enough for brand fidelity or executive polish.                                                       |
| Semantic score 100          | **Potential false comfort.** Useful as consistency check, should not substitute for metric-to-source binding and human-readable narrative quality. |
| Render gate                 | Necessary, not sufficient. Proves deck opens/renders; does not prove deck looks like the approved SimCorp master.                                  |
| Release artifacts / upload  | **Real for operational readiness.** Proves the packet can ship, not that the content is maximally sharp.                                           |

---

## Strongest Recommendation

> Do the workflow cutover and quality-baseline work **before** touching more deck polish. Right now, the largest hidden risk is not that the deck fails to render; it is that a green run can still be green for the wrong source slice.
