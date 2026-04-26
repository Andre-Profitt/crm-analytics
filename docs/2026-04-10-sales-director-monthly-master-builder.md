# Sales Director Monthly Master Builder

Date: 2026-04-10

## Purpose

This is the monthly control-plane runner for the Sales Director deck workflow.

It turns the current one-off sequence into a single dated run:

1. refresh or reuse workbook JSON snapshots
2. run Excel Claude on the workbook for a draft operating brief
3. validate that draft against the factual snapshot
4. emit a validated fact pack and PowerPoint review prompt
5. optionally run PowerPoint Claude against an existing deck or a workbook-native fallback deck

Codex is the validator in the middle. Excel Claude and PowerPoint Claude are workers, not sources of truth.

## File

- [run_sales_director_monthly_master_builder.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_master_builder.py)

## Default Contract

- source-of-truth workbook: `output/director_data_dumps/<snapshot-date>/`
- factual contract: `output/director_workbook_snapshots/<snapshot-date>/`
- preferred review deck: `output/sales_director_monthly_runs/<deck-date>/`
- fallback review deck: `output/director_workbook_deck_runs/<snapshot-date>/`
- master run artifacts: `output/sales_director_monthly_master_builder/<snapshot-date>/<timestamp>/`

## Commands

Plan the monthly run without launching Office:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --plan-only
```

Run one director end to end against an existing deck:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --director "Sarah Pittroff"
```

Run one director in native-template rewrite mode:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --director "Sarah Pittroff" \
  --powerpoint-mode build
```

Run all directors and fall back to workbook-native decks if a review deck is missing:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --fallback-workbook-deck
```

Run all directors without PowerPoint review, but still generate validated fact packs:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-10 \
  --skip-powerpoint-review
```

## Outputs Per Director

- `excel_brief/`
  - raw Excel-Claude prompt, transcript, and copied message
- `validated_bridge/`
  - `validated-fact-pack.md`
  - `validation-report.json`
  - `powerpoint-validated-prompt.txt`
- `powerpoint_review/`
  - copied editable deck
  - PowerPoint-Claude prompt, transcript, and copied message
  - in `build` mode, the editable deck is the in-place rewrite target

Run-level artifact:

- `manifest.json`

## Why This Exists

The repo now has three different lanes:

- deterministic workbook snapshot extraction
- deterministic workbook-native deck rendering
- Claude Office automation

The master builder owns the sequencing between them so monthly execution is repeatable and auditable. It also keeps the validated fact pack as the central handoff contract between Excel Claude and PowerPoint Claude.

## Current Bias

- Excel Claude: draft factual readout from the workbook
- Codex: validate, normalize, and freeze the fact pack
- PowerPoint Claude: review or polish a deck against that fact pack

The native SimCorp deck renderer is still the preferred final rendering target. The workbook-native deck is only a fallback review surface until the native-template lane is fully rebased onto the corrected workbook contract.

## Claude Skills

Custom Claude Skills for this workflow live in:

- [claude_skills](/Users/test/crm-analytics/claude_skills/README.md)

Package them for upload with:

```bash
python3 scripts/package_claude_skills.py
```

Current skill set:

- `SD Workbook Fact Pack`
- `SD PowerPoint Builder`
- `SD Deck Audit`

The master builder prompts now hint Claude to use the Excel and PowerPoint audit skills when they are enabled.
