# PI ARR Forecast List Views — Add Type = Land Filter

**Date:** 2026-04-20
**Agent:** Codex via Playwright
**Org:** simcorp.my.salesforce.com (apro@simcorp.com)
**Why:** All 9 PI list views include Expand + Renewal deals. They should be Land-only to match the deck pipeline and Pipeline Forecast Review reports (already fixed via API).

---

## Context

There are 9 Pipeline Inspection list views on the Opportunity object, one per sales territory. None of them have a `Type` filter — they pull Land, Expand, and Renewal deals. The extract script now filters post-fetch, but the views themselves still show all types when directors open them in Salesforce.

We couldn't fix this via API (ModifyMetadata permission required). Playwright can do it through the UI.

## Pre-conditions

- Logged into Salesforce as `apro@simcorp.com` at `https://simcorp.my.salesforce.com`
- You're on Lightning Experience (not Classic)

## The 9 List Views

| Territory            | List View Name         | Developer Name         |
| -------------------- | ---------------------- | ---------------------- |
| APAC                 | PI ARR Forecast APAC   | PI_ARR_Forecast_APAC   |
| Central Europe       | PI ARR Forecast CE     | PI_ARR_Forecast_CE     |
| UK & Ireland         | PI ARR Forecast UKI    | PI_ARR_Forecast_UKI    |
| Southern Europe      | PI ARR Forecast SWE    | PI_ARR_Forecast_SWE    |
| NL & Nordics         | PI ARR Forecast NE     | PI_ARR_Forecast_NE     |
| Middle East & Africa | PI ARR Forecast MEA    | PI_ARR_Forecast_MEA    |
| Canada               | PI ARR Forecast Canada | PI_ARR_Forecast_Canada |
| NA Asset Management  | PI ARR Forecast NA AM  | PI_ARR_Forecast_NA_AM  |
| Pension & Insurance  | PI ARR Forecast P&I    | PI_ARR_Forecast_P_I    |

## Steps (repeat for each of the 9 views)

### 1. Navigate to the list view

```
https://simcorp.lightning.force.com/lightning/o/Opportunity/list?filterName={developer_name}
```

Example for APAC:

```
https://simcorp.lightning.force.com/lightning/o/Opportunity/list?filterName=PI_ARR_Forecast_APAC
```

### 2. Open the filter panel

- Click the funnel/filter icon in the list view toolbar (top-right area, looks like a funnel)
- The filter panel slides open on the right side

### 3. Add the Type = Land filter

- Click "Add Filter" (or the "+" button in the filter panel)
- In the "Field" dropdown, search for and select **"Type"** (it's a standard Opportunity field)
- Operator: **equals**
- Value: select **"Land"** from the picklist
- Click "Done" or "Save" on the filter row

### 4. Save the list view

- Click "Save" on the list view (not "Save As" — we're updating the existing view)
- Confirm if prompted

### 5. Verify

- The list should reload showing fewer records (Land only)
- Check that the filter panel shows: `Type equals Land` alongside the existing filters

## Verification after all 9 are done

Run this from the terminal to confirm all views now return Land-only data:

```bash
cd /Users/test/crm-analytics
python3 -c "
import subprocess, json, urllib.request

result = subprocess.check_output(['sf', 'org', 'display', '--target-org', 'apro@simcorp.com', '--json'], stderr=subprocess.DEVNULL)
org = json.loads(result)['result']
token = org['accessToken']
base = org['instanceUrl']

pi_views = {
    'APAC': '00BTb00000Ksa4bMAB',
    'CE': '00BTb00000Kr3YvMAJ',
    'UKI': '00BTb00000Kr3yjMAB',
    'SWE': '00BTb00000Kr3sHMAR',
    'NE': '00BTb00000Kr4DFMAZ',
    'MEA': '00BTb00000KsTXmMAN',
    'Canada': '00BTb00000Kr4ErMAJ',
    'NA AM': '00BTb00000Kr4JhMAJ',
    'P&I': '00BTb00000Kr4OXMAZ',
}

for name, lv_id in pi_views.items():
    url = f'{base}/services/data/v66.0/ui-api/list-records/{lv_id}?pageSize=200'
    req = urllib.request.Request(url, headers={'Authorization': f'Bearer {token}'})
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    records = data.get('records', [])
    non_land = [r for r in records if r.get('fields', {}).get('Type', {}).get('value') != 'Land']
    status = 'PASS' if not non_land else f'FAIL ({len(non_land)} non-Land)'
    print(f'  {name}: {len(records)} records, {status}')
"
```

Expected: all 9 show PASS.

## Rollback

If something goes wrong, the only change is an added filter. To undo: open the list view, click the filter panel, remove the `Type equals Land` filter, save.

## Notes

- Do NOT rename the list views or change any other filters
- Do NOT use "Save As" — update the existing view in place
- The existing filters (Stage, Region, ForecastCategory) must remain untouched
- If a list view prompts about sharing/visibility, keep the current setting (should be "All users can see this list view")
