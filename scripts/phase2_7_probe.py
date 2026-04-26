#!/usr/bin/env python3
"""Phase 2.7 - Dashboard 1 pre-flight probe.

Dashboard-1-specific probe. Targets 01ZTb00000FSP7hMAH. Fetches an
existing Dashboard 1 report to extract folder ID, report type API name,
filter convention, ARR aggregate form (bare vs .CONVERT), and clone
template shape.

Also fetches Dashboard 1 itself to capture layout.components positional
grid shape for the build script's component-append step.

Writes confirmed metadata to /tmp/phase2_7_probe/confirmed.json.

Uncommitted by convention.

Run:
    python3 scripts/phase2_7_probe.py
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path
from typing import Any

import requests

# Constants
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

DASHBOARD_1_ID = "01ZTb00000FSP7hMAH"

# Candidate template reports on Dashboard 1 (confirmed OK post-Phase-1.5)
# Primary: Commercial Approval Candidates by Stage (SUMMARY format, stage grouping)
PRIMARY_TEMPLATE_ID = "00OTb000008ekp7MAA"
# Fallback: Forecast Accuracy after Phase 1.5 fix (SUMMARY format)
FALLBACK_TEMPLATE_ID = "00OTb000008TZsDMAW"

PROBE_DIR = Path("/tmp/phase2_7_probe")
CONFIRMED_PATH = PROBE_DIR / "confirmed.json"
DASHBOARD_RAW_PATH = PROBE_DIR / "dashboard_raw.json"
TEMPLATE_RAW_PATH = PROBE_DIR / "template_raw.json"

# Fields that are read-only / server-assigned and must be stripped from POST bodies
READ_ONLY_FIELDS = (
    "id",
    "createdDate",
    "lastModifiedDate",
    "lastRunDate",
    "lastModifiedById",
    "createdById",
    "currency",  # org-level, let it inherit
)


def get_auth() -> tuple[str, str]:
    """Shell out to `sf org display` and extract (instanceUrl, accessToken).

    Uses the trim-to-first-brace pattern to handle any leading log lines.
    """
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    """GET /analytics/reports/{id}/describe. Returns the full JSON body."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def get_dashboard(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    """GET /analytics/dashboards/{id}/describe. Returns the full JSON body."""
    url = (
        f"{inst}/services/data/{API_VERSION}/analytics/dashboards/"
        f"{dashboard_id}/describe"
    )
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def extract_arr_aggregate_form(md: dict[str, Any]) -> dict[str, Any]:
    """Search aggregates and detailColumns for ARR fields and classify CONVERT vs bare.

    Returns a dict with:
    - aggregates: list of ARR-like aggregates found
    - detail_columns: list of ARR-like detail column refs found
    - inferred_form: "bare" | "convert" | "mixed" | "unknown"
    """
    aggs = md.get("aggregates", []) or []
    cols = md.get("detailColumns", []) or []

    arr_aggs = [a for a in aggs if "APTS_Opportunity_ARR__c" in str(a)]
    arr_cols = [c for c in cols if "APTS_Opportunity_ARR__c" in str(c)]

    has_convert = any(".CONVERT" in str(a) for a in arr_aggs) or any(
        ".CONVERT" in str(c) for c in arr_cols
    )
    has_bare = any(
        "APTS_Opportunity_ARR__c" in str(a) and ".CONVERT" not in str(a)
        for a in arr_aggs
    ) or any(
        "APTS_Opportunity_ARR__c" in str(c) and ".CONVERT" not in str(c)
        for c in arr_cols
    )

    if has_convert and has_bare:
        form = "mixed"
    elif has_convert:
        form = "convert"
    elif has_bare:
        form = "bare"
    else:
        form = "unknown"

    return {
        "aggregates": arr_aggs,
        "detail_columns": arr_cols,
        "inferred_form": form,
    }


def extract_filter_shape(md: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the first few reportFilters entries to show the column-ref convention."""
    filters = md.get("reportFilters") or []
    return filters[:5]


def extract_layout_shape(dashboard: dict[str, Any]) -> dict[str, Any]:
    """Pull key fields that describe the Dashboard 1 layout / component conventions."""
    components = dashboard.get("components", []) or []
    layout = dashboard.get("layout", {}) or {}

    sample_component = components[0] if components else {}
    sample_component_keys = list(sample_component.keys())

    # layout.components is the positional grid mapped by component id
    layout_components = layout.get("components", [])
    sample_layout_entry = layout_components[0] if layout_components else {}

    return {
        "component_count": len(components),
        "sample_component_keys": sample_component_keys,
        "sample_component_type": sample_component.get("type"),
        "sample_visualization_type": sample_component.get("visualizationType")
        or sample_component.get("properties", {}).get("visualizationType"),
        "layout_has_components_grid": bool(layout_components),
        "layout_component_count": len(layout_components),
        "sample_layout_entry_keys": list(sample_layout_entry.keys()),
        "sample_layout_entry": sample_layout_entry,
        "dashboard_top_level_keys": list(dashboard.keys()),
    }


def pick_summary_component_for_clone(
    dashboard: dict[str, Any],
) -> dict[str, Any] | None:
    """Find a dashboard component that's a good clone source for new SUMMARY widgets.

    Priority:
      1. a component referencing the primary template report
      2. any component with a SUMMARY-style aggregate (has properties.aggregates)
    """
    components = dashboard.get("components", []) or []

    for c in components:
        if c.get("reportId") == PRIMARY_TEMPLATE_ID:
            return c

    for c in components:
        props = c.get("properties", {}) or {}
        if props.get("aggregates"):
            return c

    return None


def main() -> None:  # noqa: C901 - intentionally linear flow
    PROBE_DIR.mkdir(parents=True, exist_ok=True)

    print("Phase 2.7 Dashboard 1 pre-flight probe")
    print("=" * 60)

    # Step 1: Auth
    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    # Step 2: Fetch Dashboard 1
    print(f"\nFetching Dashboard 1: {DASHBOARD_1_ID}")
    dashboard = get_dashboard(inst, tok, DASHBOARD_1_ID)
    with open(DASHBOARD_RAW_PATH, "w") as f:
        json.dump(dashboard, f, indent=2)
    print(f"  Dashboard raw saved: {DASHBOARD_RAW_PATH}")

    layout_shape = extract_layout_shape(dashboard)
    print(f"  component_count:           {layout_shape['component_count']}")
    print(f"  sample_component_type:     {layout_shape['sample_component_type']!r}")
    print(f"  sample_visualization_type: {layout_shape['sample_visualization_type']!r}")
    print(f"  layout_has_components_grid:{layout_shape['layout_has_components_grid']}")
    print(f"  layout_component_count:    {layout_shape['layout_component_count']}")
    print(f"  sample_layout_entry_keys:  {layout_shape['sample_layout_entry_keys']}")

    # Step 3: Fetch primary template report
    print(f"\nFetching primary template: {PRIMARY_TEMPLATE_ID}")
    try:
        template_desc = get_report_describe(inst, tok, PRIMARY_TEMPLATE_ID)
        template_used = PRIMARY_TEMPLATE_ID
    except requests.HTTPError as e:
        print(f"  Primary template failed: {e}")
        print(f"  Falling back to {FALLBACK_TEMPLATE_ID}")
        template_desc = get_report_describe(inst, tok, FALLBACK_TEMPLATE_ID)
        template_used = FALLBACK_TEMPLATE_ID

    with open(TEMPLATE_RAW_PATH, "w") as f:
        json.dump(template_desc, f, indent=2)
    print(f"  Template raw saved: {TEMPLATE_RAW_PATH}")

    template_md = template_desc.get("reportMetadata", {}) or {}

    report_type = template_md.get("reportType", {}) or {}
    report_type_name = (
        report_type.get("type") if isinstance(report_type, dict) else str(report_type)
    )
    folder_id = template_md.get("folderId", "")
    template_name = template_md.get("name", "")
    template_format = template_md.get("reportFormat", "")

    print(f"  Template name:    {template_name!r}")
    print(f"  reportType:       {report_type_name!r}")
    print(f"  folderId:         {folder_id!r}")
    print(f"  reportFormat:     {template_format!r}")

    # Step 4: Extract ARR aggregate form (CRITICAL for Dashboard 1)
    arr_form = extract_arr_aggregate_form(template_md)
    print(f"\nARR form in template: {arr_form['inferred_form']!r}")
    print(f"  ARR aggregates:    {arr_form['aggregates']}")
    print(f"  ARR detail cols:   {arr_form['detail_columns']}")

    # If primary template doesn't have ARR, scan Dashboard 1 components for
    # another report that does, to confirm the form empirically.
    if arr_form["inferred_form"] == "unknown":
        print(
            "  (Primary template has no ARR aggregate - scanning other "
            "Dashboard 1 component reports for ARR form confirmation)"
        )
        seen_reports: set[str] = set()
        for comp in dashboard.get("components", []) or []:
            rid = comp.get("reportId")
            if not rid or rid in seen_reports:
                continue
            seen_reports.add(rid)
            if len(seen_reports) > 8:  # cap scan to avoid runaway
                break
            try:
                scan_desc = get_report_describe(inst, tok, rid)
            except requests.HTTPError:
                continue
            scan_md = scan_desc.get("reportMetadata", {}) or {}
            scan_form = extract_arr_aggregate_form(scan_md)
            if scan_form["inferred_form"] in ("bare", "convert", "mixed"):
                print(f"    Found ARR in {rid}: form={scan_form['inferred_form']}")
                arr_form = scan_form
                break

    # Step 5: Summarize filter / grouping / column conventions
    filter_samples = extract_filter_shape(template_md)
    print(f"\nFilter shape samples (first {len(filter_samples)}):")
    for i, fs in enumerate(filter_samples, 1):
        print(
            f"  [{i}] column={fs.get('column')!r} op={fs.get('operator')!r} "
            f"value={fs.get('value')!r}"
        )

    groupings = template_md.get("groupingsDown", []) or []
    print(f"\ngroupingsDown (count={len(groupings)}):")
    for g in groupings[:3]:
        print(f"  {g}")

    detail_cols = template_md.get("detailColumns", []) or []
    print(f"\ndetailColumns (count={len(detail_cols)}, showing first 10):")
    for c in detail_cols[:10]:
        print(f"  {c}")

    print(f"\naggregates: {template_md.get('aggregates', [])}")

    # Step 6: Pick a clone-source component from Dashboard 1
    clone_source = pick_summary_component_for_clone(dashboard)
    if clone_source:
        print(
            f"\nClone-source component found: id={clone_source.get('id')} "
            f"type={clone_source.get('type')} "
            f"reportId={clone_source.get('reportId')}"
        )
        clone_source_summary: dict[str, Any] = {
            "id": clone_source.get("id"),
            "type": clone_source.get("type"),
            "reportId": clone_source.get("reportId"),
            "header": clone_source.get("header", {}),
            "properties_keys": list((clone_source.get("properties", {}) or {}).keys()),
            "visualizationProperties_keys": list(
                (clone_source.get("visualizationProperties", {}) or {}).keys()
            ),
            "component_top_level_keys": list(clone_source.keys()),
        }
    else:
        print("\nWARNING: No clone-source component found on Dashboard 1")
        clone_source_summary = {}

    # Step 7: Write confirmed.json
    confirmed: dict[str, Any] = {
        "dashboard_id": DASHBOARD_1_ID,
        "template_report_id": template_used,
        "template_metadata_keys": list(template_md.keys()),
        "report_type_api_name": report_type_name,
        "folder_id": folder_id,
        "report_format": template_format,
        "arr_form": arr_form,
        "filter_samples": filter_samples,
        "groupings_down": groupings,
        "detail_columns_sample": detail_cols[:15],
        "aggregates": template_md.get("aggregates", []),
        "layout_shape": layout_shape,
        "clone_source_component": clone_source_summary,
        "read_only_fields_stripped": list(READ_ONLY_FIELDS),
    }

    with open(CONFIRMED_PATH, "w") as f:
        json.dump(confirmed, f, indent=2)
    print(f"\nConfirmed metadata saved: {CONFIRMED_PATH}")

    # Step 8: Print SUMMARY
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Dashboard:           {DASHBOARD_1_ID}")
    print(f"Template report:     {template_used}")
    print(f"reportType API name: {report_type_name!r}")
    print(f"folderId:            {folder_id!r}")
    print(f"reportFormat:        {template_format!r}")
    print(f"ARR form:            {arr_form['inferred_form']!r}")
    print(f"Layout grid present: {layout_shape['layout_has_components_grid']}")
    print(f"Layout grid count:   {layout_shape['layout_component_count']}")
    print(f"Component count:     {layout_shape['component_count']}")
    if clone_source_summary:
        print(
            f"Clone-source comp:   id={clone_source_summary['id']} "
            f"reportId={clone_source_summary['reportId']}"
        )
    else:
        print("Clone-source comp:   (not found)")
    print()
    print(f"confirmed.json:      {CONFIRMED_PATH}")
    print()
    print("Phase 2.7 Dashboard 1 probe COMPLETE.")


if __name__ == "__main__":
    main()
