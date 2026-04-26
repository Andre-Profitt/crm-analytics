# CRM Analytics / Monthly Decks Split

Date: 2026-04-24

## Decision

Split the current repo into two operating products:

1. `crm-analytics`
   - Salesforce CRM Analytics dashboard builders.
   - Wave API patching and dashboard/report surface governance.
   - CRMA-specific builder brain, autopilot, contracts, and live asset export.

2. `monthly-decks`
   - Sales Director monthly cadence and evidence chain.
   - DirectorBundle semantic data layer, Excel render artifacts, SimCorp decks, regional/global rollups, release packets, SharePoint publish, and publish gates.

Keep a small `shared-salesforce-core` package only if needed. It must stay secret-free and boring: Salesforce CLI auth wrapper, retrying REST helper, API version defaults, and possibly period utilities.

## Why

The current repo is mixing two jobs with different blast radii:

- CRM Analytics builders mutate live dashboards and Wave assets.
- Monthly decks produce leadership-ready workbooks/decks with a month-end control plane.

The overlap is mostly Salesforce auth and a few helper patterns. The operational risks are different enough that sharing one script forest makes audits noisy, onboarding slow, and release gates ambiguous.

## Hard Boundary

CRM Analytics must not import monthly deck modules.

Monthly decks must not import dashboard builder modules.

Monthly decks can consume CRM Analytics outputs only as explicit artifacts, not as Python imports:

- exported dashboard JSON
- source-contract JSON
- report/list-view IDs in config
- release packet references

## Target Ownership

### CRM Analytics Repo

Owns:

- root dashboard builders: `build_*.py`
- `crm_analytics_helpers.py`
- `crm_analytics_runtime.py`
- `portfolio_foundation.py`
- `reporting-layer/`
- dashboard/source executors:
  - `scripts/wave_patch_executor.py`
  - `scripts/wave_patch_policy.py`
  - `scripts/salesforce_dashboard_executor.py`
  - `scripts/salesforce_dashboard_filter_automation.py`
  - `scripts/materialize_wave_patch_contract.py`
  - `scripts/export_live_crma_assets.py`
  - `scripts/run_dashboard_autopilot.py`
  - `scripts/report_surface_intelligence.py`
- CRM/dashboard tests.

Primary gates:

```bash
make verify-static
python3 scripts/contract_lint.py
python3 scripts/export_live_crma_assets.py "Executive Revenue & Forecast"
```

### Monthly Decks Repo

Owns:

- `scripts/run_sales_director_monthly_cadence.py`
- `scripts/run_sales_director_monthly_master_builder.py`
- `scripts/extract_director_live.py`
- `scripts/extract_historical_trending.py`
- `scripts/monthly_platform/`
- `scripts/build_sales_director_*.py`
- `scripts/build_sales_global_*.py`
- `scripts/build_sales_region_*.py`
- `scripts/build_validated_*.py`
- `scripts/build_deck_from_excel.py`
- `scripts/build_sharepoint_analysis.py`
- `scripts/validate_tie_out.py`
- `scripts/validate_director_workbook_contract.py`
- deck/shell/release/SharePoint/publish scripts
- `codex_skills/sd-*`
- Sales Director shell configs and territory registry.

Primary gates:

```bash
python3 scripts/run_sales_director_monthly_master_builder.py \
  --snapshot-date 2026-04-23 \
  --deck-date 2026-04-23 \
  --director "Jesper Tyrer" \
  --plan-only \
  --json

python3 -m pytest \
  tests/test_models.py \
  tests/test_bundle_validation.py \
  tests/test_excel_renderer.py \
  tests/test_extract_director_live_period.py \
  tests/test_sales_director_monthly_master_builder.py \
  tests/test_sales_director_monthly_cadence.py
```

Publish gates stay monthly-deck only:

```bash
python3 scripts/validate_director_workbook_contract.py --snapshot-date <date>
python3 scripts/validate_tie_out.py --date <date>
```

## Migration Sequence

### Phase 0: Freeze The Boundary

Add and maintain:

- `config/repo_split_manifest.json`
- this split note

No file moves yet while the repo has active untracked work and generated run artifacts.

### Phase 1: Create New Repo Skeletons

Create:

- `/Users/test/code/apps/crm-analytics/`
- `/Users/test/code/apps/monthly-decks/`

Each gets:

- clean `README.md`
- minimal `Makefile`
- `requirements.txt`
- `.gitignore`
- CI workflow scoped to its product only

### Phase 2: Copy, Do Not Move

Use the manifest to copy files into each repo. Keep the current repo as the source of truth until both new repos pass their gates.

Do not copy:

- `.env`
- access tokens
- `.sf/`, `.sfdx/`
- `.playwright-cli/`
- `__pycache__/`
- generated `output/` except curated fixtures
- local Obsidian content except templates/contracts explicitly needed by monthly-decks

### Phase 3: Cut Cross Imports

Run import scans in both repos:

```bash
rg -n "from scripts\\.|import scripts\\.|crm_analytics_helpers|monthly_platform|build_deck_from_excel|build_sales_director" .
```

Allowed after split:

- CRM repo can import CRM helper modules.
- Monthly repo can import monthly modules.
- Both can import `shared-salesforce-core`, if created.

Not allowed:

- CRM importing `monthly_platform`.
- Monthly importing root dashboard builders.

### Phase 4: Move Scheduler

The GitHub Actions monthly scheduler belongs in `monthly-decks`.

CRM Analytics CI should not run deck tie-outs or deck publish gates.

Monthly deck CI should not run dashboard build/patch verification except artifact contract checks.

### Phase 5: Archive This Mixed Repo

After both repos pass two clean runs:

- mark this repo read-only or rename it `crm-analytics-monolith-archive`
- preserve commit history
- keep a final pointer README to the two active repos

## Immediate Risk From Claude's Recent Work

The semantic bundle layer is fine as a data contract, but monthly publish still fails if the live extract rerender overwrites workbook sheets appended by historical trending.

That issue belongs to `monthly-decks`, not CRM Analytics.

Current red monthly gates:

- missing `Q1 Snapshot Trend` and `Q2 Snapshot Trend` after a direct Jesper live extract rerender
- tie-out mismatches in Q1 land loss / deck-regional scope

Those must not block CRM Analytics dashboard work once the split is complete.

## Machine Manifest

The machine-readable split inventory is:

```text
config/repo_split_manifest.json
```

Use it as the source list for copy scripts and CI path filters.
