# Sales Director ETL Audit And Surgical Fix Playbook

Date: 2026-04-22

## Purpose

Define the enterprise-grade audit and fix workflow for the Sales Director monthly pipeline.

The goal is to tighten the ETL contract without destabilizing trusted outputs. Every change must be attributable to one layer, re-runnable on one director, and provable through staged gates before batch rollout.

## Canonical Lane

This is the semantic owner for Sales Director monthly:

1. `scripts/extract_director_data.py`
2. `output/director_data_dumps/<snapshot-date>/`
3. `scripts/extract_director_workbook_snapshot.py`
4. `output/director_workbook_snapshots/<snapshot-date>/`
5. `scripts/build_validated_director_brief.py`
6. `output/sales_director_monthly_master_builder/<snapshot-date>/<timestamp>/.../validated_bridge/`
7. `scripts/build_sales_director_monthly_shell.py`
8. `scripts/run_sales_director_monthly_master_builder.py`
9. `scripts/audit_sales_director_preview.py`
10. `scripts/validate_sales_director_shell_contract.py`

This lane already has explicit manifests, typed payloads, deterministic preview builds, and preview audits. It is the safest place to audit and repair.

## Non-Canonical Lane

These scripts are still operationally useful, but they are not allowed to become the semantic owner:

- `scripts/extract_director_live.py`
- `scripts/build_sharepoint_analysis.py`
- `scripts/build_deck_from_excel.py`
- `scripts/audit_deck_scope.py`

Use them for parity checks, regression detection, or legacy output support. Do not put new business-scope logic here unless the intent is explicitly to align legacy behavior to the canonical lane.

## Operative Rules

- Change one semantic layer at a time.
- Reproduce on one director before touching batch.
- Prefer fixing the highest authoritative layer that can explain the issue.
- Do not patch both canonical and legacy semantics in the same change unless the second edit is a strict adapter.
- Do not let `datetime.now()` define business scope.
- Do not accept a green preview audit as a publish signal if tie-out is red.
- If a semantic owner has no direct regression test, add one in the same patch.

## Change Classes

### 1. Source filter or extract error

Allowed files:

- `scripts/extract_director_data.py`
- `scripts/director_data_helpers.py`

Symptoms:

- bad Salesforce row set
- bad period windows in cached JSON
- missing or extra records already visible in `_sources.json`

### 2. Workbook-to-snapshot normalization error

Allowed files:

- `scripts/extract_director_workbook_snapshot.py`

Symptoms:

- snapshot facts disagree with workbook or hidden cache
- Q1 semantics are globally scoped in workbook but should be director-safe in snapshot
- approval, renewals, or pipeline scope gets reinterpreted incorrectly during normalization

### 3. Fact-pack or payload contract error

Allowed files:

- `scripts/build_validated_director_brief.py`
- `config/sales_director_monthly_shell.json`

Symptoms:

- snapshot is correct, but payload fields are wrong, missing, mislabeled, or mixed across ARR and ACV

### 4. Presentation-only error

Allowed files:

- `scripts/build_sales_director_monthly_shell.py`
- `scripts/build_sales_director_monthly_shell_v2.js`
- `scripts/audit_sales_director_preview.py`

Symptoms:

- payload is correct, but deck titles, placeholders, layout, or visual mapping are wrong

### 5. Legacy parity or comparator error

Allowed files:

- `scripts/validate_tie_out.py`
- legacy adapters only when needed

Symptoms:

- canonical lane is semantically correct, but the legacy tie-out contract is counting a different scope

## Standard Working Variables

Use one director until the final batch gate:

```bash
export SNAPSHOT_DATE=2026-04-10
export DECK_DATE=2026-04-10
export DIRECTOR="Jesper Tyrer"
export WORK_ROOT=/tmp/sd-etl-surgery-$SNAPSHOT_DATE
mkdir -p "$WORK_ROOT"
```

## Gate 0: Contract And Planner Readiness

Validate the shell contract and resolve the run plan before any code edit:

```bash
python3 scripts/validate_sales_director_shell_contract.py
```

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date "$SNAPSHOT_DATE" \
  --deck-date "$DECK_DATE" \
  --director "$DIRECTOR" \
  --plan-only \
  --json
```

Success:

- shell contract returns `ok: true`
- target resolution succeeds
- no missing workbook, snapshot, or shell prerequisites for the chosen director

## Gate 1: Source Truth And Lineage

Inspect the cached extract and lineage before diagnosing downstream behavior:

```bash
python3 -m json.tool \
  "output/director_data_dumps/$SNAPSHOT_DATE/.cache/tyrer-jesper/_sources.json" \
  | sed -n '1,220p'
```

If the scope itself is in question, replay the underlying Salesforce query with the `sf` CLI or inspect the cached JSON payloads for the affected source file.

Recommended checks:

- confirm the query window matches the reporting month or quarter
- confirm omitted, stage, type, and territory filters are explicit
- confirm record counts are plausible for the director and date

Success:

- the source query and cached rows match the intended business scope
- if not, fix at the extract layer and stop there

## Gate 2: Snapshot Contract

Rebuild the normalized snapshot for one director:

```bash
python3 scripts/extract_director_workbook_snapshot.py \
  --snapshot-date "$SNAPSHOT_DATE" \
  --director "$DIRECTOR"
```

Inspect the resulting snapshot:

```bash
sed -n '1,240p' \
  "output/director_workbook_snapshots/$SNAPSHOT_DATE/jesper-tyrer.json"
```

Focus fields:

- `snapshot_date`
- `sources`
- `pipeline_detail`
- `q1_review`
- `commercial_approval`
- `renewals`
- `factual_bullets`

Success:

- the snapshot holds the intended business truth without downstream repair
- Q1, approvals, renewals, and scope-sensitive metrics are correct here before proceeding

## Gate 3: Validated Bridge And Payload

Build the validated artifacts from the snapshot.

If no Excel brief is under review, use an empty placeholder so the bridge still materializes the fact pack and payload:

```bash
: > "$WORK_ROOT/empty-brief.txt"
python3 scripts/build_validated_director_brief.py \
  --snapshot "output/director_workbook_snapshots/$SNAPSHOT_DATE/jesper-tyrer.json" \
  --excel-brief "$WORK_ROOT/empty-brief.txt" \
  --output-dir "$WORK_ROOT/validated-bridge" \
  --snapshot-date "$SNAPSHOT_DATE"
```

Inspect:

```bash
sed -n '1,240p' "$WORK_ROOT/validated-bridge/validation-report.json"
```

```bash
sed -n '1,260p' "$WORK_ROOT/validated-bridge/powerpoint-fill-payload.json"
```

Success:

- no unexpected validation errors
- payload fields remain type-safe and horizon-safe
- ARR and ACV are not mixed
- omitted is separated from active pipeline

## Gate 4: Deterministic Preview

Run the canonical monthly builder without Office dependence:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date "$SNAPSHOT_DATE" \
  --deck-date "$DECK_DATE" \
  --director "$DIRECTOR" \
  --skip-excel-brief \
  --skip-powerpoint-review \
  --json
```

Inspect the run manifest in:

- `output/sales_director_monthly_master_builder/<snapshot-date>/<timestamp>/manifest.json`

Success:

- `validated_bridge` is `ok`
- `deterministic_preview` is `ok`
- `deterministic_preview_render` is `ok`
- `deterministic_preview_audit` is `ok`
- `deterministic_preview_layout_audit` is `ok`

## Gate 5: Preview Audit

If the preview deck was rebuilt manually or outside the master runner, audit it directly:

```bash
python3 scripts/audit_sales_director_preview.py \
  --deck "<deck-path>.pptx" \
  --fill-payload "$WORK_ROOT/validated-bridge/powerpoint-fill-payload.json"
```

Success:

- no shell leakage
- no placeholder leakage
- no unrevised shell titles on populated slides

## Gate 6: Business Tie-Out

Run the legacy truth gate after the canonical lane is stable:

```bash
python3 scripts/validate_tie_out.py --date 2026-04-22
```

Interpretation:

- if source, snapshot, and payload are correct but tie-out is red, this is usually a comparator or legacy-scope problem
- if tie-out and canonical facts both disagree with source, the issue is upstream

Success:

- mismatches are zero for the touched metrics and directors, or every residual mismatch is documented as a known legacy-scope exception

## Gate 7: Regression Tests

Minimum regression suite for canonical monthly work:

```bash
python3 -m pytest -q \
  tests/test_sales_director_monthly_master_builder.py \
  tests/test_sales_director_monthly_deck_builder_contract.py \
  tests/test_validate_sales_director_shell_contract.py \
  tests/test_audit_sales_director_preview.py
```

If the patch touches snapshot or bridge semantics, also add or update a focused test that asserts the changed field path or metric rule directly.

## Required Diff Discipline

For every semantic change, capture three diffs:

1. source diff
2. snapshot diff
3. fill payload diff

The rule is simple:

- if the source diff is surprising, stop and fix extract scope
- if the source diff is expected but the snapshot diff is surprising, fix normalization
- if snapshot is right but payload diff is surprising, fix bridge or shell contract
- if payload is right but deck is wrong, fix presentation only

## Publish Packet For Review

Before promoting a batch fix, collect:

- `manifest.json`
- `validated-fact-pack.md`
- `validation-report.json`
- `powerpoint-fill-payload.json`
- preview audit report
- preview layout audit report
- `tie-out.md`
- targeted pytest output

This is the minimum review packet for a SimCorp-grade signoff.

## Batch Promotion Rule

Only move from one director to all directors when:

1. the semantic owner is identified
2. the one-director gates are green
3. the change is covered by a focused regression test
4. tie-out is green or documented for the touched contract

Then run the batch lane:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date "$SNAPSHOT_DATE" \
  --deck-date "$DECK_DATE" \
  --all \
  --skip-excel-brief \
  --skip-powerpoint-review \
  --json
```

## What Not To Do

- do not patch `build_deck_from_excel.py` first because it is easy to observe
- do not fix a scope problem in both snapshot and tie-out on the same pass unless the second change is a strict adapter
- do not accept `audit_deck_scope.py` as a publish gate
- do not let hardcoded FY windows spread across extract, snapshot, bridge, and legacy builders
- do not batch-run first and debug later

## Decision Rule

When a metric breaks, ask in this order:

1. Is the Salesforce row set correct?
2. Is the normalized snapshot correct?
3. Is the payload contract correct?
4. Is the rendered deck wrong even though the payload is right?
5. Is the remaining problem only legacy parity?

Fix the first layer that answers `no`.
