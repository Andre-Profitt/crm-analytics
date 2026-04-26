# Handoff Prompt — SD Monthly Enterprise ETL Reset

Paste this into the next clean session.

---

You are picking up the SimCorp Sales Director Monthly ETL. The goal is to tighten the full pipeline to enterprise-grade quality without breaking the production outputs the business already trusts.
Honor `/Users/test/AGENTS.md` as the operating instruction set for this session.

## Read First

In order:

1. `/Users/test/AGENTS.md`
2. `docs/architecture.md`
3. `docs/2026-04-22-sd-monthly-enterprise-etl-handoff.md`
4. `docs/2026-04-22-sales-director-etl-audit-fix-playbook.md`

## Repo Truth

- The repo has two SD Monthly lanes.
- The **legacy lane** is the current production ETL:
  `scripts/run_monthly_director_review.py`
- The **modular lane** is the future-state audit and hardening surface:
  `scripts/run_sales_director_monthly_master_builder.py`
- Do not confuse "better architecture" with "current production."

## Primary Objective

Tighten the full ETL in this order:

1. restore production render trust
2. unify period/scope semantics
3. harden the gates
4. cut over only after parity

## Current Critical Issue

The most important active defect is branding drift in the director render path.

The likely seam is:

- `scripts/build_sales_director_monthly_shell.py`
- `scripts/build_sales_director_monthly_shell_v2.js`

The business reported loss of SimCorp branding and the expected table styling.
Treat that as a real regression.

## What To Use

Audit and surgical-fix tools:

- source-truth probes:
  - `sf org display --target-org my-org --verbose --json`
  - `sf data query --query "..."`
  - Wave API via `curl` or `python3 requests`
- source inventory docs:
  - `obsidian/sf-reports-index.md`
  - `obsidian/api-reference.md`
  - `CLAUDE_HANDOFF.md`
- `scripts/validate_tie_out.py`
- `scripts/audit_deck_scope.py`
- `scripts/audit_data_quality.py`
- `scripts/extract_director_workbook_snapshot.py`
- `scripts/build_validated_director_brief.py`
- `scripts/audit_sales_director_preview.py`
- `scripts/validate_sales_director_shell_contract.py`
- sidecar JSON files
- manifests under `output/`
- local verification:
  - `rg`
  - `git diff --stat`
  - `python3 -m py_compile`
  - focused `pytest`
- slide render tools:
  - `~/.codex/skills/slides/scripts/render_slides.py`
  - `~/.codex/skills/slides/scripts/create_montage.py`
  - `~/.codex/skills/slides/scripts/detect_font.py`

## What Not To Do

- Do not treat the JS shell render path as publish-safe until branding parity is proven.
- Do not start Office automation casually.
- Do not patch multiple semantic layers at once.
- Do not wire the modular lane into production scheduling yet.
- Do not waste time on unrelated CRM Analytics dashboard scripts.

## Safe Operating Rules

1. Reproduce on one director first.
2. Use the modular lane to inspect truth before patching the legacy lane.
3. Fix one layer at a time.
4. Preserve current business outputs unless the change is explicitly correcting a proven defect.
5. Keep changes minimal and directly traceable.

## Recommended First Work Slice

1. Prove the director branding regression on one director.
2. Compare the filled director render path with the SimCorp template-native path.
3. Restore template-native branding/table treatment in the director render path.
4. Re-run one-director gates:
   - tie-out
   - preview audit
   - visual render review
5. Only then touch quarter/date semantics.

## Commands

One-director planning and payload audit:

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
: > /tmp/empty-brief.txt
python3 scripts/build_validated_director_brief.py \
  --snapshot output/director_workbook_snapshots/2026-04-10/jesper-tyrer.json \
  --excel-brief /tmp/empty-brief.txt \
  --output-dir /tmp/jesper-validated \
  --snapshot-date 2026-04-10
```

Production audit gates:

```bash
python3 scripts/audit_data_quality.py --date 2026-04-22
python3 scripts/audit_deck_scope.py --date 2026-04-22
python3 scripts/validate_tie_out.py --date 2026-04-22
```

Visual inspection:

```bash
python3 ~/.codex/skills/slides/scripts/render_slides.py deck.pptx --output_dir rendered
python3 ~/.codex/skills/slides/scripts/create_montage.py --input_dir rendered --output_file montage.png
python3 ~/.codex/skills/slides/scripts/detect_font.py deck.pptx --json
```

## Definition Of Done

Do not call the ETL tightened until:

- SimCorp branding and table styling are restored in the director output
- tie-out is trusted again as the final business gate
- quarter logic is not split between dynamic and hardcoded paths
- one-director fixes are proven before batch rollout
- the production lane is cleaner without destabilizing monthly review outputs

Work directly. No option menus. No broad rewrite. Use the architecture doc and enterprise handoff as the operating boundary.
