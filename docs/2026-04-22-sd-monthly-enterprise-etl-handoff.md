# SD Monthly Enterprise ETL Handoff

Date: 2026-04-22

## Mission

Tighten the full Sales Director Monthly ETL without breaking the outputs the business already trusts.

The target is enterprise-grade:

- one production ETL lane
- reproducible month-end behavior
- SimCorp branding preserved
- sidecar and tie-out contracts trusted
- safe, surgical fixes with one-director repro before batch rollout

## Repo Truth

Read [architecture.md](/Users/test/crm-analytics/docs/architecture.md:1) first.
Honor [/Users/test/AGENTS.md](/Users/test/AGENTS.md:1) as the operating instruction set for the next session.

The repo has two SD Monthly lanes:

1. Legacy production lane
   `scripts/run_monthly_director_review.py`
   This is the current live pipeline.
   It has GitHub Actions scheduling and produces the decks used for monthly review.

2. Modular future-state lane
   `scripts/run_sales_director_monthly_master_builder.py`
   This is the safer architecture for typed payloads, manifests, audits, and publish packets.
   It is not the committed production scheduler lane yet.

Do not confuse "better architecture" with "current production."

## Actual Goal

The goal is not to rewrite the whole system.

The goal is:

1. keep the legacy ETL outputs stable
2. remove drift in branding, period logic, and scope logic
3. use the modular lane as the audit and hardening surface
4. cut over only after parity is proven

## Current Critical Problems

### 1. Director branding drift

The strongest current defect is in the director render path.

- `scripts/build_sales_director_monthly_shell_v2.js`
- `scripts/build_sales_director_monthly_shell.py`

These build a filled director deck with their own palette, font, and table styling instead of truly inheriting the SimCorp master in a publish-safe way.

This is the reason the business reported losing SimCorp branding and the expected table style.

### 2. Mixed quarter logic

The legacy lane is not uniformly dynamic.

- `scripts/build_deck_from_excel.py` computes quarter state from `datetime.now()` at import time.
- `scripts/build_sharepoint_analysis.py` still contains hardcoded `Q1 2026` / `Q2 FY26` logic.

That means "dynamic quarter logic" is only partially true today.

### 3. Two competing orchestration stories

The repo has both the legacy orchestrator and the modular orchestrator.

Without a clear handoff boundary, engineers can accidentally harden the wrong lane, or worse, patch the future-state lane while the business is still running the legacy lane.

### 4. Office automation risk

Some runs can leave Excel or PowerPoint open if they are started and not allowed to finish cleanly.

Do not start Office-backed flows casually.
Prefer plan-only, deterministic preview, payload inspection, and one-director repro first.

## Production Lane

This is the business-critical ETL today:

1. `scripts/run_monthly_director_review.py`
2. `scripts/extract_director_live.py`
3. `scripts/extract_historical_trending.py`
4. `scripts/audit_data_quality.py`
5. `scripts/build_sharepoint_analysis.py`
6. `scripts/build_dashboard_analysis_excel.py`
7. `scripts/build_deck_from_excel.py`
8. `scripts/build_exec_rollup_deck.py`
9. `scripts/validate_tie_out.py`
10. `scripts/audit_deck_scope.py`
11. `scripts/generate_obsidian_notes.py`

This is the lane to protect.

## Audit And Surgical Fix Toolkit

### Need Now

These are the existing tools to use before changing code:

1. `docs/architecture.md`
   Lane map, production status, and boundaries.

2. `AGENTS.md`
   Default-to-action, anti-drift rules, CRM restrictions, and output discipline.

3. `docs/2026-04-22-sales-director-etl-audit-fix-playbook.md`
   Canonical audit sequence and gate order.

4. Source-truth probes
   `sf org display --target-org my-org --verbose --json`
   `sf data query --query "..."`
   Wave API via `curl` or `python3 requests`
   Use these to verify source filters and report/dashboard lineage before blaming ETL stages.

5. Source inventory docs
   `obsidian/sf-reports-index.md`
   `obsidian/api-reference.md`
   `CLAUDE_HANDOFF.md`
   These contain the live dashboard, report, Pipeline Inspection, and historical trending IDs.

6. `scripts/validate_tie_out.py`
   Business truth gate.
   Sidecar vs live SF vs extract workbook vs regional workbook.

7. `scripts/audit_deck_scope.py`
   Legacy slide claim check.

8. `scripts/audit_data_quality.py`
   Extract-time source hygiene.

9. `scripts/run_sales_director_monthly_master_builder.py --plan-only`
   Safest way to resolve targets and prerequisites without opening Office lanes.

10. `scripts/extract_director_workbook_snapshot.py`
   Workbook-to-snapshot semantic boundary.

11. `scripts/build_validated_director_brief.py`
   Typed fact-pack and fill-payload builder.

12. `scripts/audit_sales_director_preview.py`
   Deterministic preview audit.

13. `scripts/validate_sales_director_shell_contract.py`
    Contract gate for the modular payload/shell lane.

14. Sidecar JSON and manifests
    `output/.../*.json`
    These are the real audit artifacts, not slide screenshots.

15. Local verification tools
    `rg`
    `git diff --stat`
    `python3 -m py_compile`
    focused `pytest`
    Use these before broader ETL runs.

16. Slide inspection tools
    `~/.codex/skills/slides/scripts/render_slides.py`
    `~/.codex/skills/slides/scripts/create_montage.py`
    `~/.codex/skills/slides/scripts/detect_font.py`

### Need Next

These are the missing tools worth building after the immediate fixes:

1. Snapshot diff tool
   Field-level diff between two normalized snapshots.

2. Fill-payload diff tool
   Show semantic deck changes without opening PowerPoint.

3. Golden deck regression pack
   3 directors:
   clean book
   omitted-heavy book
   approval-heavy book

4. Visual deck diff harness
   Render two decks and compare key slides side-by-side.

5. Single-flight lock for scheduled runs
   Prevent overlapping month-end execution.

## Safe Working Rules

1. Reproduce on one director first.
2. Use the modular lane to inspect truth before touching the legacy lane.
3. Fix one semantic layer at a time.
4. Do not start Excel/PowerPoint automation unless the previous gate is green.
5. Do not treat the JS shell path as publish-safe until branding parity is proven.
6. Do not accept a clean preview audit if tie-out is red.
7. Do not patch deck copy or visuals before proving the number contract.

## Recommended Surgical Order

### Phase 1. Restore production render trust

Goal:
Restore SimCorp branding and table styling in the director render path.

Work:

- trace the filled director render path
- compare the JS shell path with the real template-native renderer
- either:
  - route deterministic preview through a true SimCorp template-native builder, or
  - make the current builder inherit the actual template styling contract instead of re-implementing it

Success:

- wordmark, palette, fonts, and table treatment match the SimCorp master
- no regression in payload contract or preview audit

### Phase 2. Unify period semantics

Goal:
Remove quarter/date drift from the legacy lane.

Work:

- remove hardcoded quarter references from analysis scripts
- stop letting `datetime.now()` silently define business scope in the deck lane
- centralize period logic on one resolver contract

Success:

- same workbook run in a later month does not silently change the business story

### Phase 3. Tighten the legacy ETL gates

Goal:
Make the current production lane safer without rewriting it.

Work:

- keep sidecar JSON as the deck contract
- keep tie-out as the publish gate
- keep deck scope audit for claim drift
- add one-director regression commands before batch

Success:

- numbers tie out
- decks render correctly
- no manual reconciliation needed to trust the output

### Phase 4. Cut over only after parity

Goal:
Move from legacy production to modular production only after the outputs match.

Work:

- run one-director parity
- run 3-director parity
- run 9-director batch parity
- only then wire scheduler to the modular lane

Success:

- one production orchestrator
- one render path
- one set of publish gates

## Practical Command Set

### One-director audit

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --director "Jesper Tyrer" \
  --plan-only
```

```bash
python3 scripts/extract_director_workbook_snapshot.py \
  --snapshot-date 2026-04-10 \
  --director "Jesper Tyrer"
```

```bash
python3 scripts/build_validated_director_brief.py \
  --snapshot output/director_workbook_snapshots/2026-04-10/jesper-tyrer.json \
  --excel-brief /tmp/empty-brief.txt \
  --output-dir /tmp/jesper-validated \
  --snapshot-date 2026-04-10
```

### Production audit gates

```bash
python3 scripts/audit_data_quality.py --date 2026-04-22
python3 scripts/audit_deck_scope.py --date 2026-04-22
python3 scripts/validate_tie_out.py --date 2026-04-22
```

### Visual inspection

```bash
python3 ~/.codex/skills/slides/scripts/render_slides.py deck.pptx --output_dir rendered
python3 ~/.codex/skills/slides/scripts/create_montage.py --input_dir rendered --output_file montage.png
python3 ~/.codex/skills/slides/scripts/detect_font.py deck.pptx --json
```

## Definition Of Done

The system is "tightened" when all of these are true:

1. Production decks preserve SimCorp branding and expected table style.
2. Tie-out is trusted again as the final business gate.
3. Quarter logic is not split between dynamic and hardcoded paths.
4. One-director fixes can be proven before batch rollout.
5. The modular lane is used as the audit and hardening surface, not as an ungoverned parallel pipeline.

## Read Next

1. [architecture.md](/Users/test/crm-analytics/docs/architecture.md:1)
2. [2026-04-22-sales-director-etl-audit-fix-playbook.md](/Users/test/crm-analytics/docs/2026-04-22-sales-director-etl-audit-fix-playbook.md:1)
3. [2026-04-22-sales-director-modular-platform-delivery-plan.md](/Users/test/crm-analytics/docs/2026-04-22-sales-director-modular-platform-delivery-plan.md:1)
