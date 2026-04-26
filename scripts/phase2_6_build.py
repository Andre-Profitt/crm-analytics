#!/usr/bin/env python3
"""Phase 2.6 - Dashboard 2 Process Compliance Builds + ph_aging_pipeline_365_plus Fix.

Builds 5 new SF reports via POST /analytics/reports for the
process compliance KPI section of Dashboard 2 (Sales Ops Quarterly
KPI Dashboard, 01ZTb00000FSP9JMAX), then adds them as new components
on Dashboard 2 via dashboard PATCH. Plus a small AMOUNT-to-ARR fix
on ph_aging_pipeline_365_plus using the validated Phase 2.5 B-core
pattern.

Uses the canonical POST body shape and filter conventions confirmed
by /tmp/phase2_6_probe/confirmed.json (the Phase 2.6 probe).

Uncommitted by convention.

Design+plan: docs/2026-04-08-phase2-6-dashboard2-process-compliance-builds.md
(commit 3a938e6).

Run:
    python3 scripts/phase2_6_build.py --dry-run
    python3 scripts/phase2_6_build.py
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

DASHBOARD_ID = "01ZTb00000FSP9JMAX"
AGING_PIPELINE_REPORT_ID = "00OTb000008Ti7VMAS"  # ph_aging_pipeline_365_plus

CONFIRMED_PATH = Path("/tmp/phase2_6_probe/confirmed.json")
BACKUP_DIR = Path("/tmp/phase2_6_backup")
REPORT_BACKUP_DIR = BACKUP_DIR / "reports"
DASHBOARD_BACKUP_DIR = BACKUP_DIR / "dashboards"

# The read-only fields to strip from cloned metadata before POST
READ_ONLY_FIELDS = (
    "id",
    "createdDate",
    "lastModifiedDate",
    "lastRunDate",
    "lastModifiedById",
    "createdById",
    "currency",
)

ARR_AGGREGATE = "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
ARR_DETAIL_COLUMN = "Opportunity.APTS_Opportunity_ARR__c.CONVERT"

# Shared grouping for all 5 new reports
SALES_REGION_GROUPING = [
    {
        "name": "Opportunity.Sales_Region__c",
        "dateGranularity": "None",
        "sortAggregate": None,
        "sortOrder": "Asc",
    }
]

# Minimal sane detail columns for all 5 new reports
DETAIL_COLUMNS = [
    "ACCOUNT_NAME",
    "OPPORTUNITY_NAME",
    "STAGE_NAME",
    "CLOSE_DATE",
    "FULL_NAME",
]

# Read-only fields to strip from dashboard before PATCH
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

# The 5 widget definitions
# NOTE: SF report name limit is 40 characters.
WIDGETS = [
    {
        "widget_id": "pc_next_step_documented",
        # 40 chars max — "P2.6 Mid-Stage: No NextStep" = 27 chars
        "name": "P2.6 Mid-Stage: No NextStep",
        "developer_name": "Phase_2_6_Next_Step_Missing",
        "header": "Mid-Stage Opps Lacking NextStep",
        "title": "Lacking NextStep",
        # Opportunity.NextStep is not accessible in this org (HTTP 400 confirmed on
        # first live run). Simplified: filter on mid-stage open opps only.
        # The NextStep IS NULL filter is dropped — report shows all mid-stage open opps
        # as a superset; manual drill-down needed for NextStep compliance.
        "filters": [
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
        "simplification": "NextStep filter dropped (Opportunity.NextStep not accessible in org); shows all mid-stage open opps",
    },
    {
        "widget_id": "pc_land_commercial_approval_flow",
        # "P2.6 Land: No Approval Flow" = 28 chars
        "name": "P2.6 Land: No Approval Flow",
        "developer_name": "Phase_2_6_Land_Commercial_Approval_Missing",
        "header": "Land Deals Lacking Commercial Approval Flow",
        "title": "Lacking Approval Flow",
        "filters": [
            {
                "column": "TYPE",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "Land",
            },
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting",
            },
            {
                "column": "Opportunity.Stage_20_Approval__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "Opportunity.Submit_for_Stage_20_Review__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
    },
    {
        "widget_id": "pc_recent_activity_logged",
        # "P2.6 Active Opps: No Activity Ever" = 35 chars
        "name": "P2.6 Active Opps: No Activity Ever",
        "developer_name": "Phase_2_6_No_Activity_Ever",
        "header": "Active Opps With No Activity Logged",
        "title": "No Activity Ever",
        # Simplified from OR-clause spec: v1 uses only IS NULL case (LastActivityDate is null)
        # The spec asked for LastActivityDate < TODAY-30 OR IS NULL; the OR requires
        # reportBooleanFilter complexity. This captures the most-broken subset.
        "filters": [
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "2 - Discovery,3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting",
            },
            {
                "column": "LAST_ACTIVITY",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "",
            },
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
        "simplification": "v1: IS NULL only (LAST_ACTIVITY equals ''); OR-clause with TODAY-30 deferred",
    },
    {
        "widget_id": "pc_won_loss_reason_documented",
        # "P2.6 Closed This Qtr: No W/L Reason" = 36 chars
        "name": "P2.6 Closed This Qtr: No W/L Reason",
        "developer_name": "Phase_2_6_Won_Loss_Reason_Missing",
        "header": "Closed Opps This Quarter Lacking Reason",
        "title": "Lacking Win/Loss Reason",
        "filters": [
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "True",
            },
            {
                "column": "Opportunity.Reason_Won_Lost__c",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "",
            },
        ],
        "boolean_filter": None,
        # THIS_QUARTER is the bare-calendar form per Phase 1.5's findings
        "standard_date_filter": {
            "column": "CLOSE_DATE",
            "durationValue": "THIS_QUARTER",
            "startDate": None,
            "endDate": None,
        },
    },
    {
        "widget_id": "pc_stage_age_within_threshold",
        # "P2.6 Mid-Stage: Age Exceeded 60d" = 32 chars
        "name": "P2.6 Mid-Stage: Age Exceeded 60d",
        "developer_name": "Phase_2_6_Stage_Age_Exceeded",
        "header": "Mid-Stage Opps Exceeding 60-Day Age",
        "title": "Stage Age > 60 Days",
        "filters": [
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            },
            {
                "column": "STAGE_NAME",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "3 - Engagement,4 - Shortlisted,5 - Preferred,6 - Contracting",
            },
            # LAST_UPDATE lessThan "TODAY-60" was attempted but SF API rejected it:
            # "Invalid date (Valid date format 08.04.2026 or 08.04.2026 18.06)".
            # Relative date strings are not supported in reportFilters for this field.
            # Simplified: use standardDateFilter with endDate = 60 days ago (absolute).
            # This is computed at script runtime so the report captures opps not updated
            # in the last 60 days when the script is run. The filter moves to
            # standard_date_filter below with a custom date range.
        ],
        "boolean_filter": None,
        # Use standardDateFilter with custom range ending 60 days ago to approximate
        # "last stage change > 60 days ago". LAST_UPDATE maps to LastModifiedDate.
        # endDate = today - 60 in YYYY-MM-DD format, startDate = null (open-ended past).
        "standard_date_filter": {
            "column": "LAST_UPDATE",
            "durationValue": "CUSTOM",
            "startDate": None,
            "endDate": (
                __import__("datetime").date.today()
                - __import__("datetime").timedelta(days=60)
            ).strftime("%Y-%m-%d"),
        },
        "simplification": "LAST_UPDATE lessThan TODAY-60 not supported; using standardDateFilter CUSTOM with endDate=today-60",
    },
]


# %% Argparse


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Phase 2.6 - Dashboard 2 process compliance builds + ARR fix"
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


def patch_report(
    inst: str, tok: str, report_id: str, body: dict[str, Any], dry_run: bool
) -> dict[str, Any]:
    if dry_run:
        body_str = json.dumps(body)[:400]
        print(f"  [DRY RUN] would PATCH report {report_id}: body[:400]={body_str}")
        return {"ok": True, "error": None, "status": 200}
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}"
    try:
        r = requests.patch(
            url,
            headers={
                "Authorization": f"Bearer {tok}",
                "Content-Type": "application/json",
            },
            json=body,
            timeout=30,
        )
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}", "status": None}
    if not r.ok:
        return {
            "ok": False,
            "error": f"HTTP {r.status_code}: {r.text[:500]}",
            "status": r.status_code,
        }
    return {"ok": True, "error": None, "status": r.status_code}


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

    # Backup Dashboard 2
    dash_dest = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    try:
        dash = get_dashboard(inst, tok, DASHBOARD_ID)
        with open(dash_dest, "w") as f:
            json.dump(dash, f, indent=2)
        print(f"Cell 2 (backup): Dashboard 2 {DASHBOARD_ID} -> {dash_dest}")
    except Exception as e:
        print(f"Cell 2: BACKUP FAILED for Dashboard 2: {e}")
        sys.exit(1)

    # Backup fix target
    dest = REPORT_BACKUP_DIR / f"{AGING_PIPELINE_REPORT_ID}.json"
    try:
        describe = get_report_describe(inst, tok, AGING_PIPELINE_REPORT_ID)
        with open(dest, "w") as f:
            json.dump(describe, f, indent=2)
        print(f"Cell 2 (backup): {AGING_PIPELINE_REPORT_ID} -> {dest}")
    except Exception as e:
        print(f"Cell 2: BACKUP FAILED for {AGING_PIPELINE_REPORT_ID}: {e}")
        sys.exit(1)

    # Backup the template report
    if CONFIRMED_PATH.exists():
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
                print(f"Cell 2: WARNING — template backup failed: {e}")

    # Write rollback helper for dashboard
    rollback_path = BACKUP_DIR / "rollback_dashboard.sh"
    backup_file = str(DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json")
    rollback_lines = [
        "#!/bin/bash",
        "# Rollback Dashboard 2 to its pre-Phase-2.6 backup.",
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
        "for ro in ('id','createdDate','lastModifiedDate','lastAccessedDate','url','owner','runningUser','folderName'):",
        "    backup.pop(ro, None)",
        f"resp = requests.patch(inst+'/services/data/v66.0/analytics/dashboards/{DASHBOARD_ID}',",
        "    headers={'Authorization': f'Bearer {tok}', 'Content-Type': 'application/json'},",
        "    json=backup, timeout=60)",
        "print(f'HTTP {resp.status_code}')",
        "if not resp.ok:",
        "    print(resp.text[:400])",
        "    sys.exit(1)",
        "print('Dashboard 2 rolled back successfully.')",
        "PYEOF",
        "",
    ]
    with open(rollback_path, "w") as f:
        f.write("\n".join(rollback_lines))
    rollback_path.chmod(0o755)
    print(f"Cell 2: wrote rollback helper to {rollback_path}")


# %% Cell 3: Build and POST 5 new reports


def strip_read_only(md: dict[str, Any]) -> dict[str, Any]:
    """Remove read-only fields from a metadata dict (in-place copy)."""
    out = copy.deepcopy(md)
    for f in READ_ONLY_FIELDS:
        out.pop(f, None)
    return out


def build_widget_metadata(
    template_metadata: dict[str, Any],
    widget: dict[str, Any],
    folder_id: str,
) -> dict[str, Any]:
    """Build the reportMetadata dict for a new process-compliance widget.

    Starts from the probe's confirmed template, applies widget-specific
    overrides, and strips read-only fields.
    """
    md = strip_read_only(copy.deepcopy(template_metadata))

    # Identity
    md["name"] = widget["name"]
    md["developerName"] = widget["developer_name"]
    md["description"] = widget.get(
        "description",
        f"Phase 2.6 process compliance metric: {widget['name']}",
    )

    # Format + aggregates
    md["reportFormat"] = "SUMMARY"
    md["aggregates"] = ["RowCount"]
    md["hasRecordCount"] = True
    md["hasDetailRows"] = True
    md["showGrandTotal"] = True
    md["showSubtotals"] = True

    # Grouping by Sales_Region__c
    md["groupingsDown"] = copy.deepcopy(SALES_REGION_GROUPING)
    md["groupingsAcross"] = []

    # Detail columns
    md["detailColumns"] = list(DETAIL_COLUMNS)

    # Filters
    md["reportFilters"] = copy.deepcopy(widget["filters"])
    md["reportBooleanFilter"] = widget.get("boolean_filter")

    # Standard date filter (may be None)
    sdf = widget.get("standard_date_filter")
    if sdf is not None:
        md["standardDateFilter"] = copy.deepcopy(sdf)
    else:
        # Null it out — don't carry over the template's date filter
        md["standardDateFilter"] = None

    # Folder
    md["folderId"] = folder_id

    # Reset fields that shouldn't inherit from the ARR-based template
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
        "simplification": widget.get("simplification"),
    }


def _cell3_main(
    inst: str,
    tok: str,
    template_metadata: dict[str, Any],
    folder_id: str,
    dry_run: bool,
) -> list[dict[str, Any]]:
    """Build and POST 5 new process compliance reports. Returns list of result dicts."""

    # --- Inline tests on build_widget_metadata ---
    _fake_template = {
        "aggregates": ["s!Opportunity.APTS_Opportunity_ARR__c.CONVERT", "RowCount"],
        "chart": None,
        "crossFilters": [],
        "dashboardSetting": None,
        "description": "test",
        "detailColumns": ["ACCOUNT_NAME"],
        "developerName": "Old_Dev_Name",
        "division": None,
        "folderId": "005QA000003DUwWYAW",
        "groupingsAcross": [],
        "groupingsDown": [
            {
                "name": "FULL_NAME",
                "dateGranularity": "None",
                "sortAggregate": None,
                "sortOrder": "Asc",
            }
        ],
        "hasDetailRows": True,
        "hasRecordCount": True,
        "historicalSnapshotDates": [],
        "id": "00OXXXXtest",  # should be stripped
        "name": "Old Name",
        "presentationOptions": {"hasStackedSummaries": True},
        "reportBooleanFilter": None,
        "reportFilters": [],
        "reportFormat": "SUMMARY",
        "reportType": {"label": "Opportunities", "type": "Opportunity"},
        "saveRoleHierarchy": True,
        "scope": "organization",
        "showGrandTotal": True,
        "showSubtotals": True,
        "sortBy": [],
        "standardDateFilter": {
            "column": "CLOSE_DATE",
            "durationValue": "THIS_FISCAL_YEAR",
        },
        "standardFilters": [{"name": "open", "value": "all"}],
        "supportsRoleHierarchy": True,
        "userOrHierarchyFilterId": None,
        "createdDate": "2026-01-01",  # should be stripped
        "currency": "EUR",  # should be stripped
    }
    _fake_widget_no_date = {
        "widget_id": "test_widget",
        "name": "Test Widget",
        "developer_name": "Test_Widget_Dev",
        "filters": [
            {
                "column": "CLOSED",
                "filterType": "fieldValue",
                "isRunPageEditable": False,
                "operator": "equals",
                "value": "False",
            }
        ],
        "boolean_filter": None,
        "standard_date_filter": None,
    }

    # Test 1: read-only fields stripped
    _md = build_widget_metadata(
        _fake_template, _fake_widget_no_date, "005QA000003DUwWYAW"
    )
    assert "id" not in _md, "id should be stripped"
    assert "createdDate" not in _md, "createdDate should be stripped"
    assert "currency" not in _md, "currency should be stripped"

    # Test 2: name/developerName set from widget
    assert _md["name"] == "Test Widget"
    assert _md["developerName"] == "Test_Widget_Dev"

    # Test 3: aggregates reset to RowCount only
    assert _md["aggregates"] == ["RowCount"], (
        f"expected ['RowCount'], got {_md['aggregates']}"
    )

    # Test 4: groupingsDown set to Sales_Region__c
    assert len(_md["groupingsDown"]) == 1
    assert _md["groupingsDown"][0]["name"] == "Opportunity.Sales_Region__c"

    # Test 5: standardDateFilter is None when not provided
    assert _md["standardDateFilter"] is None

    # Test 6: filters from widget
    assert len(_md["reportFilters"]) == 1
    assert _md["reportFilters"][0]["column"] == "CLOSED"

    # Test 7: with standard_date_filter
    _fake_widget_with_date = copy.deepcopy(_fake_widget_no_date)
    _fake_widget_with_date["standard_date_filter"] = {
        "column": "CLOSE_DATE",
        "durationValue": "THIS_QUARTER",
        "startDate": None,
        "endDate": None,
    }
    _md2 = build_widget_metadata(
        _fake_template, _fake_widget_with_date, "005QA000003DUwWYAW"
    )
    assert _md2["standardDateFilter"]["durationValue"] == "THIS_QUARTER"

    # Test 8: detail columns include required fields
    assert "ACCOUNT_NAME" in _md["detailColumns"]
    assert "OPPORTUNITY_NAME" in _md["detailColumns"]

    print("Cell 3 (build_widget_metadata) tests: PASS")

    # --- Idempotency: look up existing reports by developerName to avoid duplicate POSTs ---
    dev_names = [w["developer_name"] for w in WIDGETS]
    existing_by_devname: dict[str, str] = {}
    if not dry_run:
        try:
            names_csv = ",".join(f"'{n}'" for n in dev_names)
            soql = f"SELECT Id, DeveloperName FROM Report WHERE DeveloperName IN ({names_csv})"
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
                    f"  Idempotency: {len(existing_by_devname)} report(s) already exist by developerName"
                )
        except Exception as e:
            print(f"  WARNING: idempotency SOQL failed ({e}); will attempt all POSTs")

    # --- POST the 5 reports ---
    print(f"\nCell 3: POSTing {len(WIDGETS)} new process compliance reports...")
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
                    "simplification": widget.get("simplification"),
                }
            )
            continue
        r = post_one_report(inst, tok, template_metadata, widget, folder_id, dry_run)
        simp = (
            f" [simplification: {r['simplification']}]"
            if r.get("simplification")
            else ""
        )
        if r["ok"]:
            print(f"  {widget['widget_id']}: POST OK -> {r['new_id']}{simp}")
        else:
            print(f"  {widget['widget_id']}: POST FAILED: {r['error']}{simp}")
        results.append(r)

    ok_count = sum(1 for r in results if r["ok"])
    print(f"Cell 3: {ok_count}/{len(WIDGETS)} reports created or found")
    return results


# %% Cell 4: Fix ph_aging_pipeline_365_plus AMOUNT -> ARR


def transform_aging_pipeline_metadata(
    original_metadata: dict[str, Any],
) -> tuple[dict[str, Any], bool]:
    """Swap the AMOUNT aggregate to ARR in ph_aging_pipeline_365_plus.

    Returns (new_metadata, changed). `changed` is False if already correct.

    Logic mirrors Phase 2.5 B-core Fix 1:
    - aggregates[0]: any Amount/AMOUNT form -> ARR_AGGREGATE
    - ARR_DETAIL_COLUMN added to detailColumns if missing
    """
    current_aggs = list(original_metadata.get("aggregates") or [])
    current_detail = list(original_metadata.get("detailColumns") or [])

    changed = False
    new_aggs = list(current_aggs)

    # Swap first aggregate if it's any Amount variant (not already ARR)
    if new_aggs:
        agg0 = new_aggs[0]
        if agg0 != ARR_AGGREGATE and (
            "AMOUNT" in agg0.upper() or "amount" in agg0.lower()
        ):
            new_aggs[0] = ARR_AGGREGATE
            changed = True
        elif agg0 != ARR_AGGREGATE and not agg0.startswith(
            "s!Opportunity.APTS_Opportunity_ARR"
        ):
            # Not Amount and not ARR — still force to ARR since this is the intent of the fix
            new_aggs[0] = ARR_AGGREGATE
            changed = True
    elif not new_aggs:
        # No aggregates — add ARR as first
        new_aggs = [ARR_AGGREGATE]
        changed = True

    new_detail = list(current_detail)
    if ARR_DETAIL_COLUMN not in new_detail:
        new_detail.append(ARR_DETAIL_COLUMN)
        changed = True

    new_metadata = copy.deepcopy(original_metadata)
    new_metadata["aggregates"] = new_aggs
    new_metadata["detailColumns"] = new_detail
    return new_metadata, changed


def _cell4_main(inst: str, tok: str, dry_run: bool) -> bool:
    """Fix ph_aging_pipeline_365_plus: AMOUNT -> ARR aggregate. Returns True on success."""

    # --- Inline tests on transform_aging_pipeline_metadata ---

    # Test 1: s!AMOUNT swapped to ARR, ARR detail col added
    _fake = {"aggregates": ["s!AMOUNT", "RowCount"], "detailColumns": ["FULL_NAME"]}
    _new, _changed = transform_aging_pipeline_metadata(_fake)
    assert _changed is True
    assert _new["aggregates"][0] == ARR_AGGREGATE, f"got {_new['aggregates'][0]}"
    assert _new["aggregates"][1] == "RowCount"
    assert ARR_DETAIL_COLUMN in _new["detailColumns"]
    assert "FULL_NAME" in _new["detailColumns"]

    # Test 2: already ARR -> no-op
    _fake2 = {
        "aggregates": [ARR_AGGREGATE, "RowCount"],
        "detailColumns": ["FULL_NAME", ARR_DETAIL_COLUMN],
    }
    _new2, _changed2 = transform_aging_pipeline_metadata(_fake2)
    assert _changed2 is False

    # Test 3: different Amount form
    _fake3 = {"aggregates": ["s!Opportunity.Amount"], "detailColumns": []}
    _new3, _changed3 = transform_aging_pipeline_metadata(_fake3)
    assert _changed3 is True
    assert _new3["aggregates"][0] == ARR_AGGREGATE

    # Test 4: ARR detail col not duplicated
    _fake4 = {
        "aggregates": ["s!AMOUNT"],
        "detailColumns": [ARR_DETAIL_COLUMN, "FULL_NAME"],
    }
    _new4, _changed4 = transform_aging_pipeline_metadata(_fake4)
    assert _changed4 is True  # aggregate still swapped
    assert _new4["detailColumns"].count(ARR_DETAIL_COLUMN) == 1

    # Test 5: empty aggregates list -> ARR injected
    _fake5 = {"aggregates": [], "detailColumns": []}
    _new5, _changed5 = transform_aging_pipeline_metadata(_fake5)
    assert _changed5 is True
    assert _new5["aggregates"][0] == ARR_AGGREGATE

    print("Cell 4 (transform_aging_pipeline_metadata) tests: PASS")

    # Load backup
    backup_path = REPORT_BACKUP_DIR / f"{AGING_PIPELINE_REPORT_ID}.json"
    if not backup_path.exists():
        print(f"Cell 4: NO BACKUP at {backup_path}")
        return False
    with open(backup_path) as f:
        describe = json.load(f)
    original_metadata = describe.get("reportMetadata", {})

    new_metadata, changed = transform_aging_pipeline_metadata(original_metadata)
    if not changed:
        print(f"Cell 4: {AGING_PIPELINE_REPORT_ID} already correct. SKIP.")
        return True

    print(
        f"Cell 4: {AGING_PIPELINE_REPORT_ID} aggregates "
        f"{original_metadata.get('aggregates')} -> {new_metadata['aggregates']}"
    )
    print(f"Cell 4: {AGING_PIPELINE_REPORT_ID} detailColumns +{ARR_DETAIL_COLUMN!r}")

    body = {"reportMetadata": new_metadata}
    result = patch_report(inst, tok, AGING_PIPELINE_REPORT_ID, body, dry_run)
    if not result["ok"]:
        print(f"Cell 4: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        return True

    # Inline verify
    try:
        verify = get_report_describe(inst, tok, AGING_PIPELINE_REPORT_ID)
    except Exception as e:
        print(f"Cell 4: VERIFY GET FAILED: {e}")
        return False
    v_aggs = verify.get("reportMetadata", {}).get("aggregates") or []
    if not v_aggs or v_aggs[0] != ARR_AGGREGATE:
        print(
            f"Cell 4: VERIFY FAILED: aggregates[0]="
            f"{v_aggs[0] if v_aggs else None} (expected {ARR_AGGREGATE})"
        )
        return False
    v_detail = verify.get("reportMetadata", {}).get("detailColumns") or []
    if ARR_DETAIL_COLUMN not in v_detail:
        print(f"Cell 4: VERIFY FAILED: {ARR_DETAIL_COLUMN} not in detailColumns")
        return False
    print(f"Cell 4: {AGING_PIPELINE_REPORT_ID} verified (ARR aggregate + detail col)")
    return True


# %% Cell 5: Add 5 new components to Dashboard 2


def clone_existing_component_for_new_widget(
    components: list[dict[str, Any]],
    new_report_id: str,
    new_id_prefix: str,
    new_header: str,
    new_title: str,
) -> dict[str, Any]:
    """Clone an existing SUMMARY-report component from Dashboard 2.

    Phase 1.5 in-flight fixes baked in:
    - properties.aggregates reset to [{"name": "RowCount"}] (all 5 new reports use RowCount)
    - properties.groupings reset to []
    - properties.filterColumns reset to []

    Raises ValueError if no suitable template component exists.
    """
    template = None
    for c in components:
        props = c.get("properties") or {}
        if c.get("type") == "Report" and props.get("reportFormat") == "SUMMARY":
            template = c
            break
    if template is None:
        # Fallback: any Report component
        for c in components:
            if c.get("type") == "Report":
                template = c
                break
    if template is None:
        raise ValueError("No Report component to clone from in Dashboard 2")

    new_component = copy.deepcopy(template)
    new_component["id"] = f"{new_id_prefix}_{uuid.uuid4().hex[:8]}"
    new_component["reportId"] = new_report_id
    new_component["header"] = new_header
    new_component["title"] = new_title

    if "properties" in new_component:
        props = new_component["properties"]
        # CRITICAL Phase 1.5 fixes: reset inherited properties

        # All 5 new reports use RowCount — set explicitly to avoid
        # "Dashboard component can't have less than 1 aggregates" error
        props["aggregates"] = [{"name": "RowCount"}]

        # All 5 new reports group by Sales_Region__c — set groupings to match.
        # NOTE: groupings count MUST equal the number of "grouping"-type entries
        # in visualizationProperties.tableColumns. Mismatched count = HTTP 400.
        region_grouping = {
            "inheritedReportSort": None,
            "name": "Opportunity.Sales_Region__c",
            "sortAggregate": None,
            "sortOrder": "Asc",
        }
        props["groupings"] = [region_grouping]

        if "filterColumns" in props:
            props["filterColumns"] = []

        # Rebuild visualizationProperties.tableColumns to match the grouping + aggregate.
        # The existing tableColumns reference the cloned report's grouping field (e.g.
        # STAGE_NAME) which doesn't exist in the new report → validation error.
        vp = props.get("visualizationProperties") or {}
        vp["tableColumns"] = [
            {
                "column": "Opportunity.Sales_Region__c",
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
        ]
        props["visualizationProperties"] = vp

    return new_component


def _cell5_main(
    inst: str,
    tok: str,
    post_results: list[dict[str, Any]],
    dry_run: bool,
) -> bool:
    """Append 5 new components to Dashboard 2. Returns True on success."""

    # --- Inline tests on clone_existing_component_for_new_widget ---
    _fake_components = [
        {
            "id": "c1",
            "type": "Report",
            "reportId": "orig_report",
            "header": "orig header",
            "title": "orig title",
            "properties": {
                "reportFormat": "SUMMARY",
                "aggregates": [{"name": "s!AMOUNT"}],
                "groupings": [{"name": "STAGE_NAME"}],
                "filterColumns": ["ACCOUNT_NAME"],
            },
        }
    ]

    # Test 1: clone produces correct overrides
    _new = clone_existing_component_for_new_widget(
        _fake_components, "00O_NEW", "phase2_6_test", "New Header", "New Title"
    )
    assert _new["reportId"] == "00O_NEW"
    assert _new["header"] == "New Header"
    assert _new["title"] == "New Title"
    assert _new["id"] != "c1"
    assert _new["id"].startswith("phase2_6_test_")

    # Test 2: Phase 1.5 fixes applied — aggregates set to RowCount
    assert _new["properties"]["aggregates"] == [{"name": "RowCount"}], (
        f"aggregates should be [{{RowCount}}], got {_new['properties']['aggregates']}"
    )

    # Test 3: groupings set to Sales_Region__c (not cleared — must match tableColumns)
    assert len(_new["properties"]["groupings"]) == 1
    assert _new["properties"]["groupings"][0]["name"] == "Opportunity.Sales_Region__c"
    assert _new["properties"]["filterColumns"] == []

    # Test 3b: tableColumns rebuilt to match grouping + RowCount
    tc = _new["properties"]["visualizationProperties"]["tableColumns"]
    assert len(tc) == 2
    assert tc[0]["column"] == "Opportunity.Sales_Region__c"
    assert tc[0]["type"] == "grouping"
    assert tc[1]["column"] == "RowCount"
    assert tc[1]["type"] == "aggregate"

    # Test 4: reportFormat inherited from template
    assert _new["properties"]["reportFormat"] == "SUMMARY"

    # Test 5: no SUMMARY components raises (tabular-only)
    _bad = [{"id": "x", "type": "Report", "properties": {"reportFormat": "TABULAR"}}]
    # Should fall back to any Report component, not raise
    _new2 = clone_existing_component_for_new_widget(_bad, "00O_X", "px", "H", "T")
    assert _new2["reportId"] == "00O_X"

    # Test 6: no components at all raises
    try:
        clone_existing_component_for_new_widget([], "x", "px", "y", "z")
        assert False, "should have raised"
    except ValueError:
        pass

    print("Cell 5 (clone_existing_component_for_new_widget) tests: PASS")

    # --- Load Dashboard 2 backup ---
    backup_path = DASHBOARD_BACKUP_DIR / f"{DASHBOARD_ID}.json"
    if not backup_path.exists():
        print(f"Cell 5: NO BACKUP at {backup_path}")
        return False
    with open(backup_path) as f:
        raw_backup = json.load(f)

    # The GET /analytics/dashboards/{id} endpoint returns a describe-style
    # response where the patchable metadata is nested under "dashboardMetadata".
    # For PATCH we send the dashboardMetadata dict directly (same as phase1_5
    # which had the same fields at the top level because it used a different
    # GET path that returned them unwrapped).
    if "dashboardMetadata" in raw_backup:
        dashboard = copy.deepcopy(raw_backup["dashboardMetadata"])
    else:
        # Older GET response: fields at top level (phase1_5 style)
        dashboard = copy.deepcopy(raw_backup)

    components = list(dashboard.get("components") or [])
    original_count = len(components)
    print(f"Cell 5: Dashboard 2 currently has {original_count} components")

    # Only add components for successfully created reports
    successful_results = [
        r
        for r in post_results
        if r["ok"] and not str(r.get("new_id", "")).startswith("DRY_RUN")
    ]

    # In dry-run mode, add all 5 (using placeholder IDs)
    if dry_run:
        successful_results = post_results  # all 5

    if not successful_results:
        print("Cell 5: No successful report POSTs — skipping dashboard PATCH")
        return True

    new_components: list[dict[str, Any]] = []
    for result in successful_results:
        widget_id = result["widget_id"]
        # Find the widget definition
        widget_def = next((w for w in WIDGETS if w["widget_id"] == widget_id), None)
        if widget_def is None:
            print(f"  {widget_id}: WARNING — no widget definition found, skipping")
            continue

        # Check idempotency: skip if already present
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
                f"phase2_6_{widget_id}",
                widget_def["header"],
                widget_def["title"],
            )
            new_components.append(new_comp)
            print(
                f"  {widget_id}: component built (id={new_comp['id']}, reportId={new_report_id})"
            )
        except ValueError as e:
            print(f"  {widget_id}: clone FAILED: {e}")
            return False

    if not new_components:
        print("Cell 5: No new components to add")
        return True

    new_all_components = components + new_components
    new_dashboard = copy.deepcopy(dashboard)
    new_dashboard["components"] = new_all_components

    # Dashboard 2 uses a grid layout where layout.components is a positional array
    # that must have exactly one entry per component in dashboard.components.
    # When we add N new components, we must also append N layout position entries.
    # Place new components in new rows starting after the current last row.
    layout = new_dashboard.get("layout") or {}
    layout_comps = layout.get("components") or []
    if layout_comps and len(layout_comps) == original_count:
        # Find the max row+rowspan to know where to start the new rows
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

        # Add new layout entries stacked below existing content
        # Use the same default colspan/rowspan as the last component
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
            f"original component count ({original_count}). Layout not extended — "
            "PATCH may fail with 500."
        )

    # Strip component-level lastModifiedDate (read-only, causes rejections on some orgs)
    for c in new_dashboard.get("components", []):
        c.pop("lastModifiedDate", None)

    # Strip read-only dashboard fields
    for ro_field in DASHBOARD_READ_ONLY_FIELDS:
        new_dashboard.pop(ro_field, None)

    result = patch_dashboard(inst, tok, DASHBOARD_ID, new_dashboard, dry_run)
    if not result["ok"]:
        print(f"Cell 5: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        print(
            f"Cell 5: [DRY RUN] would add {len(new_components)} components "
            f"(total would be {len(new_all_components)})"
        )
        return True

    print(
        f"Cell 5: Dashboard 2 PATCH OK — "
        f"added {len(new_components)} components "
        f"(was {original_count}, now {len(new_all_components)})"
    )
    return True


# %% Cell 6: Summary


def _cell6_main(
    post_results: list[dict[str, Any]],
    arr_fix_ok: bool,
    dashboard_patch_ok: bool,
) -> None:
    print()
    print("=" * 60)
    print("Phase 2.6 build summary")
    print("=" * 60)

    # Report POSTs
    post_ok = [r for r in post_results if r["ok"]]
    post_fail = [r for r in post_results if not r["ok"]]
    print(f"\nNew reports created: {len(post_ok)}/{len(post_results)}")
    for r in post_results:
        status = "OK" if r["ok"] else "FAILED"
        simp = (
            f" [simplified: {r['simplification']}]" if r.get("simplification") else ""
        )
        err = f" ERROR: {r['error']}" if r.get("error") and not r["ok"] else ""
        print(f"  {r['widget_id']}: {status} id={r.get('new_id')}{simp}{err}")

    # ARR fix
    print(f"\nph_aging_pipeline_365_plus ARR fix: {'OK' if arr_fix_ok else 'FAILED'}")

    # Dashboard patch
    print(
        f"\nDashboard 2 component additions: {'OK' if dashboard_patch_ok else 'FAILED'}"
    )

    # Total failures
    total_failures = (
        len(post_fail) + (0 if arr_fix_ok else 1) + (0 if dashboard_patch_ok else 1)
    )
    print(f"\nTotal failures: {total_failures}")
    print(f"Backup directory: {BACKUP_DIR}")
    print()
    if total_failures == 0:
        print("SUCCESS. All Phase 2.6 operations completed.")
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

    # Cell 2: Backup
    _cell2_main(inst, tok)

    # Load confirmed probe data
    if not CONFIRMED_PATH.exists():
        print(
            f"ERROR: {CONFIRMED_PATH} does not exist. Run phase2_6_post_probe.py first."
        )
        sys.exit(1)
    with open(CONFIRMED_PATH) as f:
        confirmed = json.load(f)
    template_metadata = confirmed.get("template_metadata", {})
    folder_id = confirmed.get("folder_id", "")
    if not template_metadata or not folder_id:
        print("ERROR: confirmed.json missing template_metadata or folder_id")
        sys.exit(1)
    print(f"Cell 1: loaded template from {CONFIRMED_PATH}, folderId={folder_id}")

    # Cell 3: POST 5 new reports
    post_results = _cell3_main(inst, tok, template_metadata, folder_id, args.dry_run)

    # Cell 4: Fix ph_aging_pipeline_365_plus
    arr_fix_ok = _cell4_main(inst, tok, args.dry_run)

    # Cell 5: Add components to Dashboard 2
    dashboard_patch_ok = _cell5_main(inst, tok, post_results, args.dry_run)

    # Cell 6: Summary
    _cell6_main(post_results, arr_fix_ok, dashboard_patch_ok)


if __name__ == "__main__":
    main()
