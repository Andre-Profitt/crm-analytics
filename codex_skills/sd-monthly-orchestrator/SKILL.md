---
name: sd-monthly-orchestrator
description: Use when running, planning, or troubleshooting the Sales Director monthly workbook-to-deck workflow in this repo. Covers snapshot refresh, Excel-Claude briefing, validated fact-pack generation, PowerPoint-Claude review, and monthly run manifests.
---

# SD Monthly Orchestrator

Use this skill for the repo's monthly Sales Director presentation workflow.

## Start Here

1. Read `references/run-modes.md`.
2. Use `scripts/run_sales_director_monthly_master_builder.py` as the primary entrypoint.
3. Prefer `--plan-only` first, then one-director pilot, then wider batch runs.

## Operating Rules

- Codex is the control plane and validator.
- Excel Claude drafts, but the validated fact pack is the source of truth.
- PowerPoint Claude should review or rewrite against the validated fact pack, not raw workbook guesses.
- Prefer existing native SimCorp decks for review. Use workbook-native fallback decks only when an existing deck is unavailable.

## Escalate To Other Skills

- Use `sd-fact-gate` when the question is whether a claim or number is trustworthy.
- Use `sd-deck-publish-gate` when the question is whether a deck is ready for leadership.
- Use `sd-regional-rollup` for CRO or regional tie-outs.
