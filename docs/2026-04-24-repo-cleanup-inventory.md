# Repo Cleanup Inventory - 2026-04-24

## Current State

This cleanup pass was reversible: no files were deleted, moved, staged, or committed.

Visible `git status --short --untracked-files=all` noise was reduced from 2,463 paths to 414 paths by ignoring generated/tool output only. This inventory adds one more visible governance doc. The remaining visible set is real source, tests, Salesforce metadata, docs, or split-governance material.

After updating `config/repo_split_manifest.json`, the remaining untracked paths classify as:

| Lane | Paths | Meaning |
| --- | ---: | --- |
| CRM Analytics | 212 | Dashboard builders, Wave/CRMA patching, Salesforce metadata, dashboard intelligence, source-truth/profile/audit tooling. |
| Monthly decks | 199 | Sales Director monthly cadence, semantic bundles, workbooks, decks, shell builders, release packets, publish gates, skills, and deck tests. |
| Shared governance | 2 | `config/repo_split_manifest.json` and this cleanup inventory; copy forward until both repos have their own ownership docs. |
| Unclassified | 0 | Anything new in this bucket needs an explicit lane decision before physical split. |

The full non-ignored source tree now also has a complete split classification:

| Lane | Paths |
| --- | ---: |
| CRM Analytics | 283 |
| Monthly decks | 266 |
| Shared between both cutover repos | 9 |
| Legacy parking lot | 70 |
| Unclassified | 0 |

## Ignored As Generated Or Local

The `.gitignore` cleanup hides local and generated noise:

- Tool/runtime caches: `.pytest_cache/`, `.ruff_cache/`, `.deepeval/`, `.playwright-cli/`, `.venv*/`, `__pycache__/`, `*.pyc`.
- Local agent state: `.claude/`, `.mcp.json`, `claude-progress.json`.
- Generated artifacts: `output/`, `exports/`, `snapshots/`, `runs/*/*.json`, `docs/generated/`, `obsidian/`, `assets/rebekka-screenshots/`, `*.csv`, `*.log`.

## Split Boundary

Use `config/repo_split_manifest.json` as the machine-readable contract.

CRM Analytics owns:

- Root dashboard builders: `build_*.py`, `commercial_operating_model.py`, `portfolio_foundation.py`, `fix_all_dashboards.py`.
- Salesforce/CRMA runtime: `force-app/`, `sfdx-project.json`, Wave patch executors, dashboard autopilot, dashboard/report intelligence, harness planner/evaluator.
- CRM docs: CRMA architecture, dashboard standards, Salesforce metadata/runbooks, sales-ops dashboard contracts, source-truth/profile/audit docs.

Monthly decks owns:

- Sales Director/global/region monthly cadence scripts, shell builders, source contracts, semantic models, workbook/deck renderers, release packets, SharePoint/deck gates.
- Repo skills under `codex_skills/sd-*` and `claude_skills/`.
- Monthly docs: Sales Director deck handoffs, master builder workflows, modular platform spec, historical trending, validated fact-pack/deck workflow, and publish-gate references.

Legacy parking lot:

- `scripts/_archive/` and old `run_workflow.sh` stay out of the first cutover unless a specific file is revived.

## Split Preview

An ignored, reversible copy preview was materialized at:

`/Users/test/crm-analytics/.worktrees/repo-split-preview-20260423-204611`

Preview copy counts:

| Preview tree | Files copied |
| --- | ---: |
| `crm-analytics/` | 292 |
| `monthly-decks/` | 275 |
| `legacy-parking-lot/` | 70 |
| Unclassified skipped | 0 |

The CRM and monthly counts include the 9 shared files copied into both previews.

## Next Physical Cleanup

1. Keep this repo frozen except for cleanup patches.
2. Create two clean working trees from manifest allowlists: `crm-analytics` and `monthly-decks`.
3. Copy `config/repo_split_manifest.json` into both until cutover is complete.
4. Leave the legacy parking lot out of both repos unless a file has an active owner.
5. Extract shared Salesforce auth/REST/period helpers only after import graph checks show both repos need them.
6. Run gates before declaring the split done:
   - CRM: `make verify-static`
   - Monthly: targeted monthly tests plus `python3 scripts/run_sales_director_monthly_master_builder.py --snapshot-date <YYYY-MM-DD> --deck-date <YYYY-MM-DD> --director "Jesper Tyrer" --plan-only --json`
   - Monthly data trust: `python3 scripts/validate_director_workbook_contract.py --snapshot-date <YYYY-MM-DD>` and `python3 scripts/validate_tie_out.py --date <YYYY-MM-DD>`
