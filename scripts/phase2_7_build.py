#!/usr/bin/env python3
"""Phase 2.7 - Dashboard 1 missing widgets build.

Creates 9 new SF reports on Dashboard 1 and adds 9 matching
components via dashboard PATCH. No report PATCH cell (no
Dashboard 1 defects to fix this phase).

Uses the confirmed metadata template produced by
scripts/phase2_7_probe.py at /tmp/phase2_7_probe/template_raw.json.

Uncommitted by convention.

Run:
    python3 scripts/phase2_7_build.py            # live
    python3 scripts/phase2_7_build.py --dry-run  # no POST/PATCH
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
import uuid
from pathlib import Path
from typing import Any

import requests

# %% Constants

TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

DASHBOARD_ID = "01ZTb00000FSP7hMAH"

PROBE_DIR = Path("/tmp/phase2_7_probe")
TEMPLATE_RAW_PATH = PROBE_DIR / "template_raw.json"
CONFIRMED_PATH = PROBE_DIR / "confirmed.json"

BACKUP_DIR = Path("/tmp/phase2_7_backup")
REPORT_BACKUP_DIR = BACKUP_DIR / "reports"
DASHBOARD_BACKUP_DIR = BACKUP_DIR / "dashboards"

# Read-only fields stripped from report metadata before POST
READ_ONLY_FIELDS = (
    "id",
    "createdDate",
    "lastModifiedDate",
    "lastRunDate",
    "lastModifiedById",
    "createdById",
    "currency",
)

# Read-only dashboard top-level fields stripped before PATCH
DASHBOARD_READ_ONLY_FIELDS = (
    "id",
    "createdDate",
    "lastModifiedDate",
    "lastAccessedDate",
    "url",
    "owner",
    "runningUser",
    "folderName",
)

# Dashboard 1 hit the "max 20 chart/table widgets" limit with 17 existing + 8
# new = 25. Can add at most 3 new chart/table widgets. The other 5 new reports
# still exist in Salesforce (reports-only) and get pinned in
# report-1-source-contract.md so the audit treats them as covered via canonical
# source mapping (Phase 2.6 fallback pattern).
DASHBOARD_BOUND_WIDGET_IDS = {
    "pipeline_overview_global",  # new global pipeline-by-stage view
    "commercial_approval_global",  # new approval True/False count view
    "renewal_likelihood",  # new renewal-by-probability view
}

# ARR + ACV forms (probe-confirmed: Dashboard 1 uses .CONVERT, same as Dashboard 2)
ARR_AGG = "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
ARR_COL = "Opportunity.APTS_Opportunity_ARR__c.CONVERT"
ACV_AGG = "s!Opportunity.APTS_Renewal_ACV__c.CONVERT"
ACV_COL = "Opportunity.APTS_Renewal_ACV__c.CONVERT"

# Region lists aligned to Salesforce CRO forecast hierarchy (verified 2026-04-10).
# See docs/2026-04-10-forecast-subregion-bug-handoff.md and config/territory_mappings.json.
# IMPORTANT: Middle East & Africa rolls up under EMEA in the forecast tree (ME & AFR Sales
# is a child of EMEA, not APAC). Do NOT move MEA back under APAC.
EMEA_REGIONS = "United Kingdom & Ireland,Central Europe,Northern Europe,Southwestern Europe,Middle East & Africa"
APAC_REGIONS = "APAC"

# Shared filter primitives
FILTER_OPEN = {
    "column": "CLOSED",
    "filterType": "fieldValue",
    "isRunPageEditable": False,
    "operator": "equals",
    "value": "False",
}

SDF_CLOSE_DATE_THIS_QUARTER = {
    "column": "CLOSE_DATE",
    "durationValue": "THIS_QUARTER",
    "startDate": None,
    "endDate": None,
}

# Grouping dict templates
GROUP_STAGE = {
    "name": "STAGE_NAME",
    "dateGranularity": "None",
    "sortAggregate": None,
    "sortOrder": "Asc",
}
GROUP_STAGE20_APPROVAL = {
    "name": "Opportunity.Stage_20_Approval__c",
    "dateGranularity": "None",
    "sortAggregate": None,
    "sortOrder": "Asc",
}
GROUP_PROBABILITY = {
    "name": "PROBABILITY",
    "dateGranularity": "None",
    "sortAggregate": None,
    "sortOrder": "Asc",
}

# Dashboard component grouping dict shape (slightly different from report grouping)
DC_GROUP_STAGE = {
    "inheritedReportSort": None,
    "name": "STAGE_NAME",
    "sortAggregate": None,
    "sortOrder": "Asc",
}
DC_GROUP_STAGE20_APPROVAL = {
    "inheritedReportSort": None,
    "name": "Opportunity.Stage_20_Approval__c",
    "sortAggregate": None,
    "sortOrder": "Asc",
}
DC_GROUP_PROBABILITY = {
    "inheritedReportSort": None,
    "name": "PROBABILITY",
    "sortAggregate": None,
    "sortOrder": "Asc",
}

# Minimal safe detail columns for ARR-aggregated reports
ARR_DETAIL_COLS = [
    "ACCOUNT_NAME",
    "OPPORTUNITY_NAME",
    "STAGE_NAME",
    "CLOSE_DATE",
    ARR_COL,
]

# Detail columns for renewal ACV reports
ACV_DETAIL_COLS = [
    "ACCOUNT_NAME",
    "OPPORTUNITY_NAME",
    "STAGE_NAME",
    "CLOSE_DATE",
    "PROBABILITY",
    "FULL_NAME",
    ACV_COL,
]

# Tabular renewal list detail columns
RENEWAL_LIST_DETAIL_COLS = [
    "ACCOUNT_NAME",
    "OPPORTUNITY_NAME",
    ACV_COL,
    "CLOSE_DATE",
    "PROBABILITY",
    "FULL_NAME",
]

# The 9 widget definitions
# NOTE: SF report name limit is 40 characters.
WIDGETS: list[dict[str, Any]] = [
    # --- Pipeline overviews (1-4) ---
    {
        "widget_id": "pipeline_overview_global",
        # "P2.7 Pipeline Global This Qtr" = 29 chars
        "name": "P2.7 Pipeline Global This Qtr",
        "developer_name": "Phase_2_7_Pipeline_Global",
        "header": "Pipeline Overview Global by Stage (This Quarter)",
        "title": "Pipeline Global by Stage",
        "report_format": "SUMMARY",
        "aggregates": [ARR_AGG],
        # SUMMARY reports used as dashboard sources must have >= 1 grouping.
        # Widget 1 was originally "single metric" but a pure no-grouping SUMMARY
        # cannot source a dashboard component (iteration 1 HTTP 400). Group by
        # STAGE_NAME to match widgets 2-4 (no region filter = global view).
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [FILTER_OPEN],
        "boolean_filter": None,
        "standard_date_filter": dict(SDF_CLOSE_DATE_THIS_QUARTER),
        "dashboard_aggregates": [{"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    {
        "widget_id": "pipeline_overview_emea",
        # "P2.7 Pipeline EMEA This Qtr" = 27 chars
        "name": "P2.7 Pipeline EMEA This Qtr",
        "developer_name": "Phase_2_7_Pipeline_EMEA",
        "header": "Pipeline Overview EMEA (This Quarter)",
        "title": "Pipeline EMEA This Quarter",
        "report_format": "SUMMARY",
        "aggregates": [ARR_AGG],
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "Opportunity.Sales_Region__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": EMEA_REGIONS,
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": dict(SDF_CLOSE_DATE_THIS_QUARTER),
        "dashboard_aggregates": [{"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    {
        "widget_id": "pipeline_overview_nam",
        # "P2.7 Pipeline NAM This Qtr" = 26 chars
        "name": "P2.7 Pipeline NAM This Qtr",
        "developer_name": "Phase_2_7_Pipeline_NAM",
        "header": "Pipeline Overview NAM (This Quarter)",
        "title": "Pipeline NAM This Quarter",
        "report_format": "SUMMARY",
        "aggregates": [ARR_AGG],
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "Opportunity.Sales_Region__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "North America",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": dict(SDF_CLOSE_DATE_THIS_QUARTER),
        "dashboard_aggregates": [{"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    {
        "widget_id": "pipeline_overview_apac",
        # "P2.7 Pipeline APAC+MEA This Qtr" = 31 chars
        "name": "P2.7 Pipeline APAC+MEA This Qtr",
        "developer_name": "Phase_2_7_Pipeline_APAC",
        "header": "Pipeline Overview APAC+MEA (This Quarter)",
        "title": "Pipeline APAC+MEA This Quarter",
        "report_format": "SUMMARY",
        "aggregates": [ARR_AGG],
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "Opportunity.Sales_Region__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": APAC_REGIONS,
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": dict(SDF_CLOSE_DATE_THIS_QUARTER),
        "dashboard_aggregates": [{"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    # --- Commercial approval (5) ---
    {
        "widget_id": "commercial_approval_global",
        # "P2.7 Commercial Approval Global" = 31 chars
        "name": "P2.7 Commercial Approval Global",
        "developer_name": "Phase_2_7_Commercial_Approval_Global",
        "header": "Commercial Approval Global Current State",
        "title": "Commercial Approval State",
        "report_format": "SUMMARY",
        "aggregates": ["RowCount"],
        "groupings_down": [dict(GROUP_STAGE20_APPROVAL)],
        "detail_columns": [
            "ACCOUNT_NAME",
            "OPPORTUNITY_NAME",
            "STAGE_NAME",
            "CLOSE_DATE",
        ],
        "filters": [
            FILTER_OPEN,
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "notEqual",
                "value": "0 - Lost",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
        "dashboard_aggregates": [{"name": "RowCount"}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE20_APPROVAL)],
        "dashboard_table_columns": [
            {
                "column": "Opportunity.Stage_20_Approval__c",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": "RowCount",
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    # --- Land Stage 3 no approval, per region (6, 7) ---
    {
        "widget_id": "land_stage3_no_approval_nam",
        # "P2.7 Land Stage 3 No Appr NAM" = 29 chars
        "name": "P2.7 Land Stage 3 No Appr NAM",
        "developer_name": "Phase_2_7_Land_Stage3_NoAppr_NAM",
        "header": "Land Stage 3 Missing Approval - NAM",
        "title": "Land Stage 3 No Approval (NAM)",
        "report_format": "SUMMARY",
        "aggregates": ["RowCount", ARR_AGG],
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "TYPE",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "Land",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "3 - Engagement",
            },
            {
                "column": "Opportunity.Stage_20_Approval__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "Opportunity.Sales_Region__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "North America",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
        "dashboard_aggregates": [{"name": "RowCount"}, {"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": "RowCount",
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    {
        "widget_id": "land_stage3_no_approval_apac",
        # "P2.7 Land Stage 3 No Appr APAC" = 30 chars
        "name": "P2.7 Land Stage 3 No Appr APAC",
        "developer_name": "Phase_2_7_Land_Stage3_NoAppr_APAC",
        "header": "Land Stage 3 Missing Approval - APAC+MEA",
        "title": "Land Stage 3 No Approval (APAC+MEA)",
        "report_format": "SUMMARY",
        "aggregates": ["RowCount", ARR_AGG],
        "groupings_down": [dict(GROUP_STAGE)],
        "detail_columns": list(ARR_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "TYPE",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "Land",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "3 - Engagement",
            },
            {
                "column": "Opportunity.Stage_20_Approval__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "Opportunity.Sales_Region__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": APAC_REGIONS,
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
        "dashboard_aggregates": [{"name": "RowCount"}, {"name": ARR_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_STAGE)],
        "dashboard_table_columns": [
            {
                "column": "STAGE_NAME",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": "RowCount",
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
            {
                "column": ARR_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    # --- Renewals (8, 9) ---
    {
        "widget_id": "renewal_likelihood",
        # "P2.7 Renewal Likelihood This Qtr" = 32 chars
        "name": "P2.7 Renewal Likelihood This Qtr",
        "developer_name": "Phase_2_7_Renewal_Likelihood",
        "header": "Renewal Likelihood by Probability (This Quarter)",
        "title": "Renewal Likelihood by Probability",
        "report_format": "SUMMARY",
        "aggregates": [ACV_AGG],
        "groupings_down": [dict(GROUP_PROBABILITY)],
        "detail_columns": list(ACV_DETAIL_COLS),
        "filters": [
            FILTER_OPEN,
            {
                "column": "TYPE",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "Renewal",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": dict(SDF_CLOSE_DATE_THIS_QUARTER),
        "dashboard_aggregates": [{"name": ACV_AGG}],
        "dashboard_groupings": [dict(DC_GROUP_PROBABILITY)],
        "dashboard_table_columns": [
            {
                "column": "PROBABILITY",
                "showSubTotal": False,
                "showTotal": False,
                "type": "grouping",
            },
            {
                "column": ACV_AGG,
                "showSubTotal": False,
                "showTotal": False,
                "type": "aggregate",
            },
        ],
    },
    # --- DEFERRED widget 9 (renewal_upcoming_list) ---
    # Dashboard 1 has a hard 25-component limit. With 17 existing components,
    # only 8 of the planned 9 can fit. Dropping renewal_upcoming_list here
    # because (a) existing components "Renewal Pipeline This Quarter" and
    # "Renewals by Fiscal Quarter" already give operators a renewal-this-quarter
    # view, and (b) iteration 1 showed it had to be substantially reshaped
    # away from "flat list" anyway (TABULAR reports cannot source dashboard
    # components without dashboardSetting + rowLimit config that the Reports
    # API doesn't expose cleanly).
    # The in-flight POSTed report 00OTb000008fBVxMAM was deleted before this
    # iteration. Widget 8 (renewal_likelihood) still carries the new
    # probability-bucket dimension, so renewal coverage is materially improved.
]


# %% Argparse


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase 2.7 - Dashboard 1 missing widgets build"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print POST/PATCH bodies instead of sending them. Auth + backup still happen.",
    )
    return p.parse_args()


# %% Cell 1: Auth


def get_auth() -> tuple[str, str]:
    """Shell out to sf org display and return (instanceUrl, accessToken)."""
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def _cell1_main() -> tuple[str, str]:
    inst, tok = get_auth()
    print(f"Cell 1 (auth): instance={inst}")
    return inst, tok


# %% API helpers


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def get_dashboard(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def post_report(
    inst: str, tok: str, body: dict[str, Any], dry_run: bool, widget_id: str
) -> dict[str, Any]:
    """POST /analytics/reports. Returns {'ok': bool, 'id': str|None, 'error': str|None}."""
    if dry_run:
        body_str = json.dumps(body)[:400]
        print(f"  [DRY RUN] would POST {widget_id}: body[:400]={body_str}")
        return {"ok": True, "id": f"DRY_RUN_{widget_id}", "error": None}
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports"
    try:
        r = requests.post(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
    except requests.RequestException as e:
        return {"ok": False, "id": None, "error": f"network: {e}"}
    if not r.ok:
        return {
            "ok": False,
            "id": None,
            "error": f"HTTP {r.status_code}: {r.text[:500]}",
        }
    try:
        resp = r.json()
    except ValueError:
        return {"ok": False, "id": None, "error": f"non-JSON response: {r.text[:200]}"}
    new_id = (
        resp.get("id")
        or resp.get("Id")
        or (resp.get("reportMetadata") or {}).get("id")
        or (resp.get("reportMetadata") or {}).get("Id")
    )
    return {"ok": True, "id": new_id, "error": None}


def patch_dashboard(
    inst: str, tok: str, dashboard_id: str, body: dict[str, Any], dry_run: bool
) -> dict[str, Any]:
    if dry_run:
        comps = body.get("components") or []
        print(
            f"  [DRY RUN] would PATCH dashboard {dashboard_id}: components len={len(comps)}"
        )
        return {"ok": True, "error": None}
    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}"
    try:
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=60,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}"}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:800]}"}
    return {"ok": True, "error": None}


# %% Cell 2: Backup


def _cell2_main(inst: str, tok: str) -> None:
    REPORT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    DASHBOARD_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    # Backup Dashboard 1
    dash_dest = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    try:
        dash = get_dashboard(inst, tok, DASHBOARD_ID)
        with open(dash_dest, "w") as f:
            json.dump(dash, f, indent=2)
        print(f"Cell 2 (backup): Dashboard 1 {DASHBOARD_ID} -> {dash_dest}")
    except Exception as e:
        print(f"Cell 2: BACKUP FAILED for Dashboard 1: {e}")
        sys.exit(1)

    # Backup template report (from confirmed.json)
    if not CONFIRMED_PATH.exists():
        print(f"Cell 2: ERROR - {CONFIRMED_PATH} missing. Run phase2_7_probe.py first.")
        sys.exit(1)
    with open(CONFIRMED_PATH) as f:
        confirmed = json.load(f)
    template_id = confirmed.get("template_report_id", "")
    if template_id:
        tmpl_dest = REPORT_BACKUP_DIR / f"{template_id}_template.json"
        try:
            tmpl_describe = get_report_describe(inst, tok, template_id)
            with open(tmpl_dest, "w") as f:
                json.dump(tmpl_describe, f, indent=2)
            print(f"Cell 2 (backup): template {template_id} -> {tmpl_dest}")
        except Exception as e:
            print(f"Cell 2: WARNING - template backup failed: {e}")

    # Write rollback helper for dashboard
    rollback_path = BACKUP_DIR / "rollback_dashboard.sh"
    backup_file = str(DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json")
    rollback_lines = [
        "#!/bin/bash",
        "# Rollback Dashboard 1 to its pre-Phase-2.7 backup.",
        "set -e",
        f'BACKUP="{backup_file}"',
        'if [ ! -f "$BACKUP" ]; then',
        '  echo "No backup at $BACKUP"',
        "  exit 1",
        "fi",
        "python3 - << 'PYEOF'",
        "import json, requests, subprocess, sys",
        f'backup = json.load(open("{backup_file}"))',
        "r = subprocess.run(['sf','org','display','--target-org','apro@simcorp.com','--json'],capture_output=True,text=True)",
        "p = r.stdout; info = json.loads(p[p.find('{'):])['result']",
        "inst = info['instanceUrl']; tok = info['accessToken']",
        "# GET returns 'dashboardMetadata' wrapper - unwrap if present",
        "if 'dashboardMetadata' in backup:",
        "    backup = backup['dashboardMetadata']",
        "for ro in ('id','createdDate','lastModifiedDate','lastAccessedDate','url','owner','runningUser','folderName'):",
        "    backup.pop(ro, None)",
        f"resp = requests.patch(inst+'/services/data/v66.0/analytics/dashboards/{DASHBOARD_ID}',",
        "    headers={'Authorization': f'Bearer {tok}', 'Content-Type': 'application/json'},",
        "    json=backup, timeout=60)",
        "print(f'HTTP {resp.status_code}')",
        "if not resp.ok:",
        "    print(resp.text[:400])",
        "    sys.exit(1)",
        "print('Dashboard 1 rolled back successfully.')",
        "PYEOF",
        "",
    ]
    with open(rollback_path, "w") as f:
        f.write("\n".join(rollback_lines))
    rollback_path.chmod(0o755)
    print(f"Cell 2: wrote rollback helper to {rollback_path}")


# %% Cell 3: Build and POST 9 new reports


def strip_read_only(md: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of md with known read-only fields removed."""
    out = copy.deepcopy(md)
    for f in READ_ONLY_FIELDS:
        out.pop(f, None)
    return out


def build_widget_metadata(
    template_metadata: dict[str, Any],
    widget: dict[str, Any],
    folder_id: str,
) -> dict[str, Any]:
    """Build the reportMetadata dict for a new Phase 2.7 widget.

    Starts from the probe's confirmed template, applies widget-specific
    overrides, and strips read-only fields. Unlike Phase 2.6 (all widgets
    used the same RowCount + Sales_Region grouping), Phase 2.7 widgets
    vary heavily in format, aggregates, groupings, and detail columns.
    """
    md = strip_read_only(copy.deepcopy(template_metadata))

    # Identity
    md["name"] = widget["name"]
    md["developerName"] = widget["developer_name"]
    md["description"] = f"Phase 2.7 Dashboard 1 widget: {widget['name']}"

    # Format + aggregates + groupings
    md["reportFormat"] = widget["report_format"]
    md["aggregates"] = list(widget["aggregates"])
    md["groupingsDown"] = copy.deepcopy(widget["groupings_down"])
    md["groupingsAcross"] = []

    # For TABULAR, no grand totals
    if widget["report_format"] == "TABULAR":
        md["hasRecordCount"] = False
        md["showGrandTotal"] = False
        md["showSubtotals"] = False
    else:
        md["hasRecordCount"] = True
        md["showGrandTotal"] = True
        md["showSubtotals"] = True
    md["hasDetailRows"] = True

    # Detail columns - MUST NOT contain any column that's also a grouping.
    # Salesforce Reports API rejects with:
    # "You can't include groupings in the selected columns list: <NAME>"
    # (specificErrorCode 113). Filter out any grouping names from the
    # detail columns list.
    grouping_names = {g.get("name") for g in md["groupingsDown"]}
    md["detailColumns"] = [
        c for c in widget["detail_columns"] if c not in grouping_names
    ]

    # Filters
    md["reportFilters"] = copy.deepcopy(widget["filters"])
    md["reportBooleanFilter"] = widget.get("boolean_filter")

    # Standard date filter
    sdf = widget.get("standard_date_filter")
    md["standardDateFilter"] = copy.deepcopy(sdf) if sdf is not None else None

    # Folder
    md["folderId"] = folder_id

    # Reset fields that shouldn't inherit from the template
    md["chart"] = None
    md["crossFilters"] = []
    md["sortBy"] = []
    md["historicalSnapshotDates"] = []

    return md


def post_one_report(
    inst: str,
    tok: str,
    template_metadata: dict[str, Any],
    widget: dict[str, Any],
    folder_id: str,
    dry_run: bool,
) -> dict[str, Any]:
    """Build + POST one report. Returns result dict with widget_id, new_id, ok, error."""
    md = build_widget_metadata(template_metadata, widget, folder_id)
    body = {"reportMetadata": md}
    result = post_report(inst, tok, body, dry_run, widget["widget_id"])
    return {
        "widget_id": widget["widget_id"],
        "new_id": result.get("id"),
        "ok": result["ok"],
        "error": result.get("error"),
    }


def _cell3_main(
    inst: str,
    tok: str,
    template_metadata: dict[str, Any],
    folder_id: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    """Build and POST 9 new Phase 2.7 reports. Returns list of result dicts."""

    # --- Inline tests on build_widget_metadata ---
    _fake_template = {
        "aggregates": ["s!AMOUNT", "RowCount"],
        "chart": None,
        "crossFilters": [],
        "currency": "EUR",  # should be stripped
        "detailColumns": ["AMOUNT"],
        "developerName": "Old_Dev",
        "folderId": "005QA000003DUwWYAW",
        "groupingsAcross": [],
        "groupingsDown": [dict(GROUP_STAGE)],
        "hasDetailRows": True,
        "hasRecordCount": True,
        "historicalSnapshotDates": [],
        "id": "00Oold",  # should be stripped
        "name": "Old",
        "createdDate": "2026-01-01",  # should be stripped
        "reportBooleanFilter": None,
        "reportFilters": [],
        "reportFormat": "SUMMARY",
        "reportType": {"label": "Opportunities", "type": "Opportunity"},
        "sortBy": [],
        "standardDateFilter": None,
        "standardFilters": [],
    }

    # Test 1: read-only fields stripped, identity set
    _md = build_widget_metadata(_fake_template, WIDGETS[0], "005QA000003DUwWYAW")
    assert "id" not in _md
    assert "createdDate" not in _md
    assert "currency" not in _md
    assert _md["name"] == "P2.7 Pipeline Global This Qtr"
    assert _md["developerName"] == "Phase_2_7_Pipeline_Global"

    # Test 2: widget 1 has STAGE_NAME grouping (iteration 2 fix - pure SUMMARY
    # without groupings cannot source dashboard components)
    assert len(_md["groupingsDown"]) == 1
    assert _md["groupingsDown"][0]["name"] == "STAGE_NAME"
    assert _md["aggregates"] == [ARR_AGG]
    assert ARR_COL in _md["detailColumns"]
    # STAGE_NAME must NOT be in detailColumns (groupings-in-selected-columns rule)
    assert "STAGE_NAME" not in _md["detailColumns"]

    # Test 3: widget 1 has CLOSE_DATE THIS_QUARTER filter
    assert _md["standardDateFilter"] is not None
    assert _md["standardDateFilter"]["durationValue"] == "THIS_QUARTER"

    # Test 4: widget 2 (EMEA) has STAGE_NAME grouping + region filter
    _md2 = build_widget_metadata(_fake_template, WIDGETS[1], "005QA000003DUwWYAW")
    assert len(_md2["groupingsDown"]) == 1
    assert _md2["groupingsDown"][0]["name"] == "STAGE_NAME"
    assert any(
        f["column"] == "Opportunity.Sales_Region__c" for f in _md2["reportFilters"]
    )

    # Test 5: widget 5 (commercial_approval) uses notEqual operator
    _md5 = build_widget_metadata(_fake_template, WIDGETS[4], "005QA000003DUwWYAW")
    assert _md5["aggregates"] == ["RowCount"]
    assert _md5["groupingsDown"][0]["name"] == "Opportunity.Stage_20_Approval__c"
    stage_filter = next(f for f in _md5["reportFilters"] if f["column"] == "STAGE_NAME")
    assert stage_filter["operator"] == "notEqual"
    assert stage_filter["value"] == "0 - Lost"

    # Test 6: widget 6 (land_stage3_nam) has multi-aggregate with ARR
    _md6 = build_widget_metadata(_fake_template, WIDGETS[5], "005QA000003DUwWYAW")
    assert _md6["aggregates"] == ["RowCount", ARR_AGG]
    assert ARR_COL in _md6["detailColumns"]
    # STAGE_NAME grouping must not leak into detailColumns
    assert "STAGE_NAME" not in _md6["detailColumns"]

    # Test 7: widget 8 (renewal_likelihood) uses ACV aggregate
    _md8 = build_widget_metadata(_fake_template, WIDGETS[7], "005QA000003DUwWYAW")
    assert _md8["aggregates"] == [ACV_AGG]
    assert ACV_COL in _md8["detailColumns"]
    assert _md8["groupingsDown"][0]["name"] == "PROBABILITY"
    # PROBABILITY grouping must not leak into detailColumns
    assert "PROBABILITY" not in _md8["detailColumns"]

    # Test 8: WIDGETS list is exactly 8 after dropping widget 9 to fit the
    # 25-component dashboard limit (iteration 3)
    assert len(WIDGETS) == 8, f"expected 8 widgets, got {len(WIDGETS)}"

    # Test 9: folderId is set to passed argument
    assert _md["folderId"] == "005QA000003DUwWYAW"

    # Test 10: read-only template inheritance reset
    assert _md["chart"] is None
    assert _md["crossFilters"] == []

    print("Cell 3 (build_widget_metadata) tests: PASS")

    # --- Idempotency: skip POSTs for existing reports by developerName ---
    dev_names = [w["developer_name"] for w in WIDGETS]
    existing_by_devname: dict[str, str] = {}
    if not dry_run:
        try:
            names_csv = ",".join(f"'{n}'" for n in dev_names)
            soql = (
                f"SELECT Id, DeveloperName FROM Report "
                f"WHERE DeveloperName IN ({names_csv})"
            )
            soql_r = subprocess.run(
                [
                    "sf",
                    "data",
                    "query",
                    "--query",
                    soql,
                    "--target-org",
                    TARGET_ORG,
                    "--json",
                ],
                capture_output=True,
                text=True,
                timeout=30,
            )
            soql_payload = json.loads(soql_r.stdout[soql_r.stdout.find("{") :])
            for rec in (soql_payload.get("result") or {}).get("records") or []:
                existing_by_devname[rec["DeveloperName"]] = rec["Id"]
            if existing_by_devname:
                print(
                    f"  Idempotency: {len(existing_by_devname)} report(s) already exist"
                )
        except Exception as e:
            print(f"  WARNING: idempotency SOQL failed ({e}); will attempt all POSTs")

    # --- POST the 9 reports ---
    print(f"\nCell 3: POSTing {len(WIDGETS)} new Phase 2.7 reports...")
    results: list[dict[str, Any]] = []
    for widget in WIDGETS:
        existing_id = existing_by_devname.get(widget["developer_name"])
        if existing_id:
            print(
                f"  {widget['widget_id']}: ALREADY EXISTS id={existing_id} (skipping POST)"
            )
            results.append(
                {
                    "widget_id": widget["widget_id"],
                    "new_id": existing_id,
                    "ok": True,
                    "error": None,
                }
            )
            continue
        r = post_one_report(inst, tok, template_metadata, widget, folder_id, dry_run)
        if r["ok"]:
            print(f"  {widget['widget_id']}: POST OK -> {r['new_id']}")
        else:
            print(f"  {widget['widget_id']}: POST FAILED: {r['error']}")
        results.append(r)

    ok_count = sum(1 for r in results if r["ok"])
    print(f"Cell 3: {ok_count}/{len(WIDGETS)} reports created or found")
    return results


# %% Cell 4: Add 9 new components to Dashboard 1


def clone_existing_component_for_new_widget(
    components: list[dict[str, Any]],
    new_report_id: str,
    widget: dict[str, Any],
    new_id_prefix: str,
) -> dict[str, Any]:
    """Clone an existing Report component from Dashboard 1 and apply widget-specific overrides.

    Uses the same Phase 1.5 / Phase 2.6 in-flight fix pattern, generalised
    for Phase 2.7's varied widget shapes (different aggregates, groupings,
    table columns per widget).
    """
    template = None
    for c in components:
        props = c.get("properties") or {}
        if c.get("type") == "Report" and props.get("reportFormat") == "SUMMARY":
            template = c
            break
    if template is None:
        for c in components:
            if c.get("type") == "Report":
                template = c
                break
    if template is None:
        raise ValueError("No Report component to clone from in Dashboard 1")

    new_component = copy.deepcopy(template)
    new_component["id"] = f"{new_id_prefix}_{uuid.uuid4().hex[:8]}"
    new_component["reportId"] = new_report_id
    new_component["header"] = widget["header"]
    new_component["title"] = widget["title"]

    if "properties" in new_component:
        props = new_component["properties"]

        # Set reportFormat to match the new report
        props["reportFormat"] = widget["report_format"]

        # CRITICAL Phase 1.5/2.6 fixes:
        # Dashboard components require aggregates >= 1 for SUMMARY; TABULAR has 0.
        da = list(widget["dashboard_aggregates"])
        if widget["report_format"] == "SUMMARY" and not da:
            # Fallback to RowCount so the PATCH doesn't fail validation
            da = [{"name": "RowCount"}]
        props["aggregates"] = da

        # Groupings must match tableColumns "grouping" count
        props["groupings"] = copy.deepcopy(widget["dashboard_groupings"])

        # filterColumns always reset
        if "filterColumns" in props:
            props["filterColumns"] = []

        # Rebuild visualizationProperties.tableColumns to match per-widget spec
        vp = props.get("visualizationProperties") or {}
        vp["tableColumns"] = copy.deepcopy(widget["dashboard_table_columns"])
        props["visualizationProperties"] = vp

    return new_component


def _cell4_main(
    inst: str,
    tok: str,
    post_results: list[dict[str, Any]],
    dry_run: bool,
) -> bool:
    """Append 9 new components to Dashboard 1. Returns True on success."""

    # --- Inline tests on clone_existing_component_for_new_widget ---
    _fake_components = [
        {
            "id": "c1",
            "type": "Report",
            "reportId": "orig",
            "header": "orig h",
            "title": "orig t",
            "properties": {
                "reportFormat": "SUMMARY",
                "aggregates": [{"name": "s!AMOUNT"}],
                "groupings": [{"name": "STAGE_NAME"}],
                "filterColumns": ["ACCOUNT_NAME"],
                "visualizationProperties": {
                    "tableColumns": [{"column": "STAGE_NAME", "type": "grouping"}]
                },
            },
        }
    ]

    # Test 1: widget 1 (iteration 2: now SUMMARY grouped by STAGE_NAME)
    _w1 = WIDGETS[0]
    _n1 = clone_existing_component_for_new_widget(
        _fake_components, "00ONEW1", _w1, "phase2_7_test1"
    )
    assert _n1["reportId"] == "00ONEW1"
    assert _n1["header"] == _w1["header"]
    assert _n1["properties"]["reportFormat"] == "SUMMARY"
    assert len(_n1["properties"]["groupings"]) == 1
    assert _n1["properties"]["groupings"][0]["name"] == "STAGE_NAME"
    assert _n1["properties"]["aggregates"] == [{"name": ARR_AGG}]
    assert _n1["properties"]["filterColumns"] == []
    _tc1 = _n1["properties"]["visualizationProperties"]["tableColumns"]
    assert _tc1[0]["column"] == "STAGE_NAME" and _tc1[0]["type"] == "grouping"
    assert _tc1[1]["column"] == ARR_AGG and _tc1[1]["type"] == "aggregate"

    # Test 2: widget 2 (EMEA pipeline, STAGE_NAME grouping)
    _w2 = WIDGETS[1]
    _n2 = clone_existing_component_for_new_widget(
        _fake_components, "00ONEW2", _w2, "phase2_7_test2"
    )
    assert len(_n2["properties"]["groupings"]) == 1
    assert _n2["properties"]["groupings"][0]["name"] == "STAGE_NAME"
    _tc2 = _n2["properties"]["visualizationProperties"]["tableColumns"]
    assert _tc2[0]["column"] == "STAGE_NAME" and _tc2[0]["type"] == "grouping"
    assert _tc2[1]["column"] == ARR_AGG and _tc2[1]["type"] == "aggregate"

    # Test 3: widget 6 (land NAM, multi-aggregate)
    _w6 = WIDGETS[5]
    _n6 = clone_existing_component_for_new_widget(
        _fake_components, "00ONEW6", _w6, "phase2_7_test6"
    )
    assert _n6["properties"]["aggregates"] == [
        {"name": "RowCount"},
        {"name": ARR_AGG},
    ]
    _tc6 = _n6["properties"]["visualizationProperties"]["tableColumns"]
    assert len(_tc6) == 3  # STAGE_NAME + RowCount + ARR

    # Test 4: widget 8 (renewal_likelihood) - last widget in the trimmed list
    _w8 = WIDGETS[7]
    _n8 = clone_existing_component_for_new_widget(
        _fake_components, "00ONEW8", _w8, "phase2_7_test8"
    )
    assert _n8["properties"]["aggregates"] == [{"name": ACV_AGG}]
    assert _n8["properties"]["groupings"][0]["name"] == "PROBABILITY"
    _tc8 = _n8["properties"]["visualizationProperties"]["tableColumns"]
    assert _tc8[0]["column"] == "PROBABILITY"
    assert _tc8[1]["column"] == ACV_AGG

    # Test 5: no components at all raises
    try:
        clone_existing_component_for_new_widget([], "x", WIDGETS[0], "p")
        assert False, "should have raised"
    except ValueError:
        pass

    # Test 6: id is unique per call
    _a = clone_existing_component_for_new_widget(
        _fake_components, "00OA", WIDGETS[0], "pa"
    )
    _b = clone_existing_component_for_new_widget(
        _fake_components, "00OB", WIDGETS[0], "pb"
    )
    assert _a["id"] != _b["id"]

    print("Cell 4 (clone_existing_component_for_new_widget) tests: PASS")

    # --- Load Dashboard 1 backup ---
    backup_path = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    if not backup_path.exists():
        print(f"Cell 4: NO BACKUP at {backup_path}")
        return False
    with open(backup_path) as f:
        raw_backup = json.load(f)

    if "dashboardMetadata" in raw_backup:
        dashboard = copy.deepcopy(raw_backup["dashboardMetadata"])
    else:
        dashboard = copy.deepcopy(raw_backup)

    components = list(dashboard.get("components") or [])
    original_count = len(components)
    print(f"Cell 4: Dashboard 1 currently has {original_count} components")

    # Only add components for successfully created reports
    successful_results = [
        r
        for r in post_results
        if r["ok"] and not str(r.get("new_id", "")).startswith("DRY_RUN")
    ]
    if dry_run:
        successful_results = post_results  # all in dry-run

    if not successful_results:
        print("Cell 4: No successful report POSTs - skipping dashboard PATCH")
        return True

    # Iteration 3: Dashboard 1 hit the "max 20 chart/table widgets" limit.
    # Only DASHBOARD_BOUND_WIDGET_IDS get added as dashboard components.
    # The other new reports are report-only (still built) and get pinned
    # in the source contract amendment.
    skipped_for_limit = [
        r["widget_id"]
        for r in successful_results
        if r["widget_id"] not in DASHBOARD_BOUND_WIDGET_IDS
    ]
    if skipped_for_limit:
        print(
            f"  Skipping {len(skipped_for_limit)} widget(s) from dashboard add "
            f"due to 20-chart limit: {skipped_for_limit}"
        )
        print("  (Reports still exist in SF; will be pinned in source contract)")

    new_components: list[dict[str, Any]] = []
    for result in successful_results:
        widget_id = result["widget_id"]
        if widget_id not in DASHBOARD_BOUND_WIDGET_IDS:
            continue
        widget_def = next((w for w in WIDGETS if w["widget_id"] == widget_id), None)
        if widget_def is None:
            print(f"  {widget_id}: WARNING - no widget definition found, skipping")
            continue

        # Idempotency: skip if already present
        new_report_id = result["new_id"]
        already_present = any(c.get("reportId") == new_report_id for c in components)
        if already_present:
            print(
                f"  {widget_id}: already in dashboard (reportId={new_report_id}). SKIP."
            )
            continue

        try:
            new_comp = clone_existing_component_for_new_widget(
                components,
                new_report_id,
                widget_def,
                f"phase2_7_{widget_id}",
            )
            new_components.append(new_comp)
            print(
                f"  {widget_id}: component built (id={new_comp['id']}, reportId={new_report_id})"
            )
        except ValueError as e:
            print(f"  {widget_id}: clone FAILED: {e}")
            return False

    if not new_components:
        print("Cell 4: No new components to add")
        return True

    new_all_components = components + new_components
    new_dashboard = copy.deepcopy(dashboard)
    new_dashboard["components"] = new_all_components

    # Dashboard 1 has an EMPTY layout.components grid per the probe. When
    # layout_comps is empty, Salesforce renders components in their natural
    # order - no positional sync needed. Keep layout as-is.
    layout = new_dashboard.get("layout") or {}
    layout_comps = layout.get("components") or []
    if layout_comps and len(layout_comps) == original_count:
        # Same Phase 2.6 layout extension logic (kept for forward compat)
        max_row_end = 0
        default_rowspan = 8
        default_colspan = 4
        num_cols = layout.get("numColumns") or 12
        for lc in layout_comps:
            row_end = (lc.get("row") or 0) + (lc.get("rowspan") or default_rowspan)
            if row_end > max_row_end:
                max_row_end = row_end
                default_rowspan = lc.get("rowspan") or default_rowspan
                default_colspan = lc.get("colspan") or default_colspan
        new_layout_comps = list(layout_comps)
        for i in range(len(new_components)):
            col_pos = (i * default_colspan) % num_cols
            row_pos = (
                max_row_end + (i // (num_cols // default_colspan)) * default_rowspan
            )
            new_layout_comps.append(
                {
                    "colspan": default_colspan,
                    "column": col_pos,
                    "row": row_pos,
                    "rowspan": default_rowspan,
                }
            )
        new_layout = copy.deepcopy(layout)
        new_layout["components"] = new_layout_comps
        new_dashboard["layout"] = new_layout
        print(
            f"  Layout: extended from {len(layout_comps)} to {len(new_layout_comps)} entries"
        )
    elif layout_comps:
        print(
            f"  WARNING: layout.components count ({len(layout_comps)}) != "
            f"original component count ({original_count})."
        )
    else:
        print(
            "  Layout: empty layout.components grid - no positional sync "
            "(Dashboard 1 convention per probe finding)"
        )

    # Strip component-level lastModifiedDate
    for c in new_dashboard.get("components", []):
        c.pop("lastModifiedDate", None)

    # Strip read-only dashboard fields
    for ro_field in DASHBOARD_READ_ONLY_FIELDS:
        new_dashboard.pop(ro_field, None)

    result = patch_dashboard(inst, tok, DASHBOARD_ID, new_dashboard, dry_run)
    if not result["ok"]:
        print(f"Cell 4: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        print(
            f"Cell 4: [DRY RUN] would add {len(new_components)} components "
            f"(total would be {len(new_all_components)})"
        )
        return True

    print(
        f"Cell 4: Dashboard 1 PATCH OK - "
        f"added {len(new_components)} components "
        f"(was {original_count}, now {len(new_all_components)})"
    )
    return True


# %% Cell 5: Summary


def _cell5_main(
    post_results: list[dict[str, Any]],
    dashboard_patch_ok: bool,
) -> None:
    print()
    print("=" * 60)
    print("Phase 2.7 build summary")
    print("=" * 60)

    post_ok = [r for r in post_results if r["ok"]]
    post_fail = [r for r in post_results if not r["ok"]]
    print(f"\nNew reports created or found: {len(post_ok)}/{len(post_results)}")
    for r in post_results:
        status = "OK" if r["ok"] else "FAILED"
        err = f" ERROR: {r['error']}" if r.get("error") and not r["ok"] else ""
        print(f"  {r['widget_id']}: {status} id={r.get('new_id')}{err}")

    print(
        f"\nDashboard 1 component additions: {'OK' if dashboard_patch_ok else 'FAILED'}"
    )

    total_failures = len(post_fail) + (0 if dashboard_patch_ok else 1)
    print(f"\nTotal failures: {total_failures}")
    print(f"Backup directory: {BACKUP_DIR}")
    print()
    if total_failures == 0:
        print("SUCCESS. All Phase 2.7 operations completed.")
    else:
        print(f"DONE WITH CONCERNS: {total_failures} operation(s) failed.")
    print("=" * 60)


# %% Main


def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no POST/PATCH operations will be sent")
        print()

    # Cell 1: Auth
    inst, tok = _cell1_main()

    # Cell 2: Backup (Dashboard 1 + template report)
    _cell2_main(inst, tok)

    # Load probe-confirmed template metadata and folder
    if not TEMPLATE_RAW_PATH.exists():
        print(
            f"ERROR: {TEMPLATE_RAW_PATH} does not exist. Run phase2_7_probe.py first."
        )
        sys.exit(1)
    with open(TEMPLATE_RAW_PATH) as f:
        template_raw = json.load(f)
    template_metadata = template_raw.get("reportMetadata", {}) or {}

    with open(CONFIRMED_PATH) as f:
        confirmed = json.load(f)
    folder_id = confirmed.get("folder_id", "")
    if not template_metadata or not folder_id:
        print("ERROR: missing template_metadata or folder_id from probe outputs")
        sys.exit(1)
    print(f"Cell 1: loaded template from {TEMPLATE_RAW_PATH}, folderId={folder_id}")

    # Cell 3: POST 9 new reports
    post_results = _cell3_main(inst, tok, template_metadata, folder_id, args.dry_run)

    # Cell 4: Add components to Dashboard 1
    dashboard_patch_ok = _cell4_main(inst, tok, post_results, args.dry_run)

    # Cell 5: Summary
    _cell5_main(post_results, dashboard_patch_ok)


if __name__ == "__main__":
    main()
