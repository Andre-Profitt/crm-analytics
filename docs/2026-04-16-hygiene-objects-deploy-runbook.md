---
title: Hygiene Custom Objects — Deployment Runbook
type: runbook
audience: Salesforce admin with ModifyMetadata or ModifyAllData
prerequisites: sf CLI authed to target org with admin profile
deploy_time: ~5 minutes
---

# Deploying the Hygiene\_\* Custom Objects

This deploys 5 custom objects that store the output of `scripts/audit_data_quality.py`. Once deployed, the audit script's `--write-to-sf` flag starts pushing data in; dashboards can then chart it natively.

## What gets created

| Object                         | Fields | Purpose                                                                  |
| ------------------------------ | -----: | ------------------------------------------------------------------------ |
| `Hygiene_Snapshot__c`          |     12 | Aggregate count per (run_date, metric). Drives KPI tiles + trend charts. |
| `Hygiene_Deal_Flag__c`         |     16 | Per-opportunity hygiene flag. Deal-level drill.                          |
| `Hygiene_Account_Flag__c`      |     12 | Per-account hygiene flag. KYC/NDA/Short Code/etc.                        |
| `Hygiene_Installation_Flag__c` |      7 | Per-installation flag. Ghost / overlapping / expired.                    |
| `Hygiene_Quote_Flag__c`        |      9 | Per-Apttus-proposal flag. Stuck quotes.                                  |

**Total: 5 objects, 56 fields.** All set to `Private` OWD + ReadWrite share, bulk-API-enabled, reportable, feeds off.

All metadata files already generated at `force-app/main/default/objects/Hygiene_*__c/`.

## Prerequisites

- Admin profile user (has `ModifyMetadata` or `ModifyAllData`)
- `sf` CLI installed + authed to the target org
- Repo cloned at `/Users/test/crm-analytics` (or path where the SFDX project lives)

## Step 1 — Validate (dry run)

```bash
cd /Users/test/crm-analytics

sf project deploy validate \
  --source-dir force-app/main/default/objects/Hygiene_Snapshot__c \
               force-app/main/default/objects/Hygiene_Deal_Flag__c \
               force-app/main/default/objects/Hygiene_Account_Flag__c \
               force-app/main/default/objects/Hygiene_Installation_Flag__c \
               force-app/main/default/objects/Hygiene_Quote_Flag__c \
  --target-org <admin-alias> \
  --wait 10
```

Expected output: `Status: Succeeded` and `Components: 61/61`.

If any field fails validation, SF prints the exact error. Common issues:

- Referenced object doesn't exist (e.g. `Apttus_Proposal__Proposal__c` not installed → fails `Hygiene_Quote_Flag__c` deploy — resolve by removing that object OR confirming Apttus is installed)
- Field API name collision (unlikely — these are all brand new names prefixed with `Hygiene_`)

## Step 2 — Deploy

```bash
sf project deploy start \
  --source-dir force-app/main/default/objects/Hygiene_Snapshot__c \
               force-app/main/default/objects/Hygiene_Deal_Flag__c \
               force-app/main/default/objects/Hygiene_Account_Flag__c \
               force-app/main/default/objects/Hygiene_Installation_Flag__c \
               force-app/main/default/objects/Hygiene_Quote_Flag__c \
  --target-org <admin-alias> \
  --wait 10
```

~1-2 minutes. Success message with deployment ID.

## Step 3 — Grant object + field access to Sales Ops profile

By default the profile doesn't have access to new custom objects. Either:

- **Permission set (recommended):** create `Hygiene_Read_Only` with Read on all 5 objects + all fields. Assign to Sales Ops users. Reversible.
- **Profile edit:** modify the Sales Ops profile directly. Less reversible.

## Step 4 — Tab (optional)

For operator drill-in, create a Lightning tab on `Hygiene_Snapshot__c` and `Hygiene_Deal_Flag__c` so users can search/filter them directly. Skip if you want the data to stay "backend" and only surface through dashboards.

## Step 5 — First writeback

From the SFDX project root:

```bash
python3 scripts/audit_data_quality.py --date 2026-04-16 --write-to-sf
```

Expected tail:

```
SF writeback: sent=32 failed=0 errors=0
```

Verify in SF: `Setup → Object Manager → Hygiene Snapshot → List Views → All → Show rows`. Should see 32 rows, one per metric_key with the 2026-04-16 run date.

## Step 6 — Wire into the monthly pipeline

Add `--write-to-sf` to the orchestrator stage (already named `1c_data_quality_audit`):

```python
# scripts/run_monthly_director_review.py, line ~155
step = run_step(
    "1c_data_quality_audit",
    [
        sys.executable,
        "scripts/audit_data_quality.py",
        "--date",
        date_stamp,
        "--write-to-sf",     # ← add this
    ],
    log_dir / "1c_data_quality_audit.log",
)
```

After that, every monthly pipeline run writes a fresh snapshot row per metric to SF.

## Step 7 — Build the Sales Ops dashboard components

Open `01ZTb00000FSP9JMAX` (or clone to Andre folder first), add these components:

| Component                   | Source                                                                                               | Viz                                                              |
| --------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| Critical alerts table       | Report: `Hygiene Snapshots WHERE Severity = Critical AND Run_Date = THIS_MONTH` sorted by Count desc | FlexTable, columns: Metric_Label, Count, Delta, SF_Logic, Action |
| Trend: Stage 3+ no NextStep | Report: `Hygiene Snapshots WHERE Metric_Key = 'mid_stage_no_next_step'` grouped by Run_Date          | Line chart                                                       |
| Hero alert banner           | Report: `Hygiene Snapshots WHERE Is_Hero_Alert = true`                                               | Metric (shows label + count)                                     |
| Trend: all Critical counts  | `Hygiene Snapshots WHERE Severity = Critical` grouped by Run_Date, stacked by Metric_Key             | Stacked line chart                                               |
| Severity mix                | `Hygiene Snapshots WHERE Run_Date = THIS_MONTH` grouped by Severity, summed Count                    | Donut                                                            |

Once 2+ snapshot runs exist, trend charts start rendering with real deltas.

## Rollback

If you need to roll back:

```bash
sf project delete source \
  --metadata CustomObject:Hygiene_Snapshot__c \
             CustomObject:Hygiene_Deal_Flag__c \
             CustomObject:Hygiene_Account_Flag__c \
             CustomObject:Hygiene_Installation_Flag__c \
             CustomObject:Hygiene_Quote_Flag__c \
  --target-org <admin-alias>
```

All data in the objects is lost. Reports and dashboards referencing them will break. Do this only if you're retiring the capability.

## Re-generating the metadata after a field change

If we add a new check that needs a new field, edit `scripts/generate_hygiene_metadata.py`, then:

```bash
python3 scripts/generate_hygiene_metadata.py
sf project deploy validate --source-dir force-app/main/default/objects --target-org <admin-alias>
sf project deploy start    --source-dir force-app/main/default/objects --target-org <admin-alias>
```

The generator is idempotent — re-running just overwrites the metadata files.
