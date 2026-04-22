# Q3 PI Surface Attachment — Playwright Runbook

**Date:** 2026-04-22
**Agent:** Codex via Playwright
**Org:** simcorp.my.salesforce.com (apro@simcorp.com)
**Why:** 9 Q3 Land-only PI list views were created via UI API, but PipelineInspectionListView records couldn't be attached via API ("filter type must be a supported type"). The views work for our extract pipeline (ui-api/list-records), but they won't appear in the Pipeline Inspection interface for directors until attached through Setup.

## Pre-conditions

- Logged into Salesforce as `apro@simcorp.com`
- Lightning Experience (not Classic)

## The 9 Q3 List Views

| Territory            | List View Name                      | View ID            |
| -------------------- | ----------------------------------- | ------------------ |
| APAC                 | PI ARR Forecast APAC Q3 2026 Land   | 00BTb00000LEdNBMA1 |
| Central Europe       | PI Land Q3 CE                       | 00BTb00000LEdmzMAD |
| UK & Ireland         | PI Land Q3 UKI                      | 00BTb00000LEdobMAD |
| Southern Europe      | PI Land Q3 SWE                      | 00BTb00000LEdqDMAT |
| NL & Nordics         | PI Land Q3 NE                       | 00BTb00000LEdrpMAD |
| Middle East & Africa | PI Land Q3 MEA                      | 00BTb00000LEdtRMAT |
| Canada               | PI ARR Forecast Canada Q3 2026 Land | 00BTb00000LEdgXMAT |
| NA Asset Management  | PI Land Q3 NA_AM                    | 00BTb00000LEbwUMAT |
| Pension & Insurance  | PI Land Q3 P_I                      | 00BTb00000LEdv3MAD |

## Steps (repeat for each of the 9 views)

### 1. Navigate to Pipeline Inspection setup

```
https://simcorp.lightning.force.com/lightning/setup/PipelineInspection/home
```

Or: Setup > Feature Settings > Sales > Pipeline Inspection

### 2. Add a new Pipeline Inspection view

- Click "New Pipeline Inspection" or "Add View" (the button to create a new PI surface)
- In the configuration:
  - **List View:** Select the Q3 view from the dropdown (e.g., "PI Land Q3 CE")
  - **Summary Field:** Select the same field used by existing PI views (should be `APTS_Forecast_ARR__c` or the field labeled "Forecast ARR")
  - **Start Date:** 2026-07-01
  - **End Date:** 2026-09-30
- Save

### 3. Verify

- Navigate to Opportunities > Pipeline Inspection
- The new Q3 view should appear in the view picker
- Confirm it shows Land-only deals with Q3 close dates

## Alternative: clone existing PI view

If the Setup UI allows cloning:

1. Find an existing PI view (e.g., "PI ARR Forecast APAC")
2. Clone it
3. Change the list view to the Q3 version
4. Update date range to Jul-Sep 2026
5. Save

## Verification script

After all 9 are done, run from terminal:

```bash
cd /Users/test/crm-analytics
python3 -c "
import subprocess, json
from urllib.parse import quote
import urllib.request

result = subprocess.check_output(['sf', 'org', 'display', '--target-org', 'apro@simcorp.com', '--json'], stderr=subprocess.DEVNULL)
org = json.loads(result)['result']
token = org['accessToken']
base = org['instanceUrl']

q3_views = {
    'APAC': '00BTb00000LEdNBMA1',
    'CE': '00BTb00000LEdmzMAD',
    'UKI': '00BTb00000LEdobMAD',
    'SWE': '00BTb00000LEdqDMAT',
    'NE': '00BTb00000LEdrpMAD',
    'MEA': '00BTb00000LEdtRMAT',
    'Canada': '00BTb00000LEdgXMAT',
    'NA AM': '00BTb00000LEbwUMAT',
    'P&I': '00BTb00000LEdv3MAD',
}

for name, lv_id in q3_views.items():
    check = json.loads(urllib.request.urlopen(urllib.request.Request(
        f'{base}/services/data/v66.0/query?q=' + quote(f\"SELECT Id FROM PipelineInspectionListView WHERE ListViewId = '{lv_id}' LIMIT 1\"),
        headers={'Authorization': f'Bearer {token}'}
    )).read())
    status = 'PASS' if check.get('totalSize', 0) > 0 else 'MISSING'
    print(f'  {name}: {status}')
"
```

Expected: all 9 show PASS.

## Notes

- Do NOT modify or delete existing PI views (the legacy Q1/Q2 ones)
- The summary field must match what existing views use
- If Pipeline Inspection setup doesn't allow adding views for UI-API-created list views, note this and we'll create the views through Setup directly instead
