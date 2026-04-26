# Codex Sales Director Operating System

Date: 2026-04-10

## Purpose

This is the Codex-side scaffolding above the monthly master builder.

It gives the desktop app a reusable operating system for the Sales Director deck workflow:

- named Codex skills
- a local installer into `~/.codex/skills`
- standard cadence commands for plan, pilot, batch, and publish-gate

## Repo Skills

Source skill folders:

- [sd-monthly-orchestrator](/Users/test/crm-analytics/codex_skills/sd-monthly-orchestrator/SKILL.md)
- [sd-fact-gate](/Users/test/crm-analytics/codex_skills/sd-fact-gate/SKILL.md)
- [sd-deck-publish-gate](/Users/test/crm-analytics/codex_skills/sd-deck-publish-gate/SKILL.md)
- [sd-regional-rollup](/Users/test/crm-analytics/codex_skills/sd-regional-rollup/SKILL.md)

Install them locally for Codex Desktop:

```bash
python3 scripts/install_repo_codex_skills.py --link
```

`--link` is the preferred mode while we are still iterating.

## Cadence Wrapper

Use:

- [run_sales_director_monthly_cadence.py](/Users/test/crm-analytics/scripts/run_sales_director_monthly_cadence.py)

Examples:

```bash
python3 scripts/run_sales_director_monthly_cadence.py plan \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10
```

```bash
python3 scripts/run_sales_director_monthly_cadence.py pilot \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --director "Sarah Pittroff"
```

```bash
python3 scripts/run_sales_director_monthly_cadence.py pilot \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --director "Sarah Pittroff" \
  --powerpoint-mode build
```

```bash
python3 scripts/run_sales_director_monthly_cadence.py batch \
  --snapshot-date 2026-04-10 \
  --deck-date 2026-04-10 \
  --fallback-workbook-deck
```

```bash
python3 scripts/run_sales_director_monthly_cadence.py publish-gate \
  --manifest output/sales_director_monthly_master_builder/2026-04-10/<timestamp>/manifest.json
```

## Operating Split

- Codex orchestrates, validates, and gates
- Claude Excel drafts workbook analysis
- Claude PowerPoint rewrites and audits the live deck

This is the enterprise split:

- deterministic control plane
- Office-native deck editing
- monthly repeatability
- clear publish blockers
