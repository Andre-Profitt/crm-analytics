#!/usr/bin/env python3
"""Phase 2.5 B-core - Pre-flight shape probe.

Determines empirically:
1. The canonical ARR aggregate form for Dashboard 2's reports by
   inspecting ph_no_activity_30_plus (target of Fix 1) and a reference
   Dashboard 2 widget that already uses ARR correctly
   (dq_missing_decision_reason per the Phase 2 audit).
2. The calendar-quarter grouping shape by scanning up to 100 reports for
   groupingsDown entries that use a CloseDate-derived calendar quarter.
3. That the wrapped_full PATCH body shape still works for Dashboard 2's
   reports by round-tripping a cosmetic name change on one Dashboard 2
   widget (NOT a target of either Fix).

Writes /tmp/phase2_5_probe/confirmed.json with the results. Exits non-zero
if any step fails catastrophically.

Uncommitted by convention.

Run:
    python3 scripts/phase2_5_shape_probe.py
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
PROBE_DIR = Path("/tmp/phase2_5_probe")
CONFIRMED_PATH = PROBE_DIR / "confirmed.json"

# Targets of the two fixes
FIX1_REPORT_ID = "00OTb000008TaEnMAK"  # ph_no_activity_30_plus
FIX2_REPORT_ID = "00OTb000008SrmLMAS"  # ph_overdue_opportunities

# Reference Dashboard 2 widget that uses ARR correctly per the Phase 2 audit
ARR_REFERENCE_REPORT_ID = "00OTb000008el0PMAQ"  # dq_missing_decision_reason

# Probe target for the body-shape round-trip (a third Dashboard 2 widget
# that is NOT a target of either fix). Using dq_missing_decision_reason
# would work but pick another to avoid touching the reference twice.
BODY_SHAPE_PROBE_REPORT_ID = "00OTb000008TZqcMAG"  # dq_missing_amount


def get_auth() -> tuple[str, str]:
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
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def probe_arr_convention(inst: str, tok: str) -> dict[str, Any]:
    """Check the ARR aggregate form on the reference Dashboard 2 widget."""
    print(f"Probing ARR convention on {ARR_REFERENCE_REPORT_ID}...")
    d = get_report_describe(inst, tok, ARR_REFERENCE_REPORT_ID)
    rm = d.get("reportMetadata", {})
    aggs = rm.get("aggregates", [])
    detail = rm.get("detailColumns", [])
    print(f"  aggregates: {aggs}")
    print(
        f"  detailColumns (ARR-relevant): {[c for c in detail if 'APTS_Opportunity_ARR' in c]}"
    )
    # Find the ARR aggregate form actually used
    arr_agg = None
    for a in aggs:
        if isinstance(a, str) and "APTS_Opportunity_ARR__c" in a:
            arr_agg = a
            break
    return {
        "reference_report": ARR_REFERENCE_REPORT_ID,
        "confirmed_arr_aggregate": arr_agg,  # e.g., "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
        "reference_detail_columns": detail,
    }


def probe_fix1_current_state(inst: str, tok: str) -> dict[str, Any]:
    """Capture the current state of the Fix 1 target."""
    print(f"Probing Fix 1 target {FIX1_REPORT_ID}...")
    d = get_report_describe(inst, tok, FIX1_REPORT_ID)
    rm = d.get("reportMetadata", {})
    return {
        "report_id": FIX1_REPORT_ID,
        "current_aggregates": rm.get("aggregates", []),
        "current_detail_columns": rm.get("detailColumns", []),
    }


def probe_fix2_current_state(inst: str, tok: str) -> dict[str, Any]:
    """Capture the current state of the Fix 2 target."""
    print(f"Probing Fix 2 target {FIX2_REPORT_ID}...")
    d = get_report_describe(inst, tok, FIX2_REPORT_ID)
    rm = d.get("reportMetadata", {})
    return {
        "report_id": FIX2_REPORT_ID,
        "current_groupings_down": rm.get("groupingsDown", []),
        "current_standard_date_filter": rm.get("standardDateFilter"),
    }


def probe_calendar_quarter_grouping(inst: str, tok: str) -> dict[str, Any]:
    """Scan up to 100 reports for a calendar-quarter grouping shape."""
    print("Scanning 100 reports for a calendar-quarter grouping shape...")
    query = "SELECT Id FROM Report LIMIT 100"
    r = requests.get(
        f"{inst}/services/data/{API_VERSION}/query?q={query.replace(' ', '+')}",
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    if not r.ok:
        return {
            "found": False,
            "error": f"query HTTP {r.status_code}",
            "sample_groupings": [],
        }
    recs = r.json().get("records", [])

    sample_groupings: list[dict[str, Any]] = []
    calendar_quarter_shape: dict[str, Any] | None = None

    for rec in recs:
        rid = rec["Id"]
        try:
            d = get_report_describe(inst, tok, rid)
            gd = d.get("reportMetadata", {}).get("groupingsDown", []) or []
            for g in gd:
                if not isinstance(g, dict):
                    continue
                name = g.get("name", "")
                granularity = g.get("dateGranularity")
                # Look for a CloseDate or similar date-field grouping with Quarter granularity
                # that is NOT a fiscal computed field
                if "FISCAL" in name.upper():
                    continue
                if granularity and "QUARTER" in granularity.upper():
                    sample_groupings.append({"report_id": rid, "grouping": g})
                    if calendar_quarter_shape is None:
                        calendar_quarter_shape = g
                # Also look for standalone non-fiscal-Quarter name entries
                elif name.upper() in ("CLOSE_DATE", "CLOSEDATE"):
                    if granularity and "QUARTER" in (granularity or "").upper():
                        sample_groupings.append({"report_id": rid, "grouping": g})
                        if calendar_quarter_shape is None:
                            calendar_quarter_shape = g
        except Exception:
            continue

        if len(sample_groupings) >= 5:
            break

    found = calendar_quarter_shape is not None
    print(f"  found calendar-quarter shape: {found}")
    if found:
        print(f"  shape: {calendar_quarter_shape}")
    return {
        "found": found,
        "calendar_quarter_shape": calendar_quarter_shape,
        "sample_groupings": sample_groupings,
    }


def probe_body_shape_roundtrip(inst: str, tok: str) -> dict[str, Any]:
    """Round-trip a cosmetic name change on BODY_SHAPE_PROBE_REPORT_ID to
    confirm wrapped_full works on Dashboard 2's reports."""
    print(f"Body shape round-trip on {BODY_SHAPE_PROBE_REPORT_ID}...")
    d = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    original_rm = d.get("reportMetadata", {})
    original_name = original_rm.get("name", "")

    probe_name = f"{original_name} (phase2_5_probe)"
    new_rm = copy.deepcopy(original_rm)
    new_rm["name"] = probe_name
    body = {"reportMetadata": new_rm}

    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{BODY_SHAPE_PROBE_REPORT_ID}"
    hdr = {"Authorization": f"Bearer {tok}", "Content-Type": "application/json"}

    try:
        r = requests.patch(url, headers=hdr, json=body, timeout=30)
    except requests.RequestException as e:
        return {"ok": False, "error": f"network: {e}"}
    if not r.ok:
        return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:300]}"}

    # Verify
    v = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    verify_name = v.get("reportMetadata", {}).get("name", "")
    if verify_name != probe_name:
        return {"ok": False, "error": f"verify mismatch: {verify_name!r}"}

    # Restore
    restore_rm = copy.deepcopy(original_rm)
    restore_body = {"reportMetadata": restore_rm}
    r2 = requests.patch(url, headers=hdr, json=restore_body, timeout=30)
    if not r2.ok:
        return {
            "ok": False,
            "error": f"restore FAILED HTTP {r2.status_code}: {r2.text[:300]}",
            "MANUAL_ACTION": f"restore {BODY_SHAPE_PROBE_REPORT_ID} name to {original_name!r}",
        }
    # Re-verify restore
    v2 = get_report_describe(inst, tok, BODY_SHAPE_PROBE_REPORT_ID)
    if v2.get("reportMetadata", {}).get("name") != original_name:
        return {
            "ok": False,
            "error": "restore verify mismatch",
            "MANUAL_ACTION": f"restore {BODY_SHAPE_PROBE_REPORT_ID} name to {original_name!r}",
        }

    print("  body shape wrapped_full confirmed working")
    return {"ok": True, "shape": "wrapped_full"}


def main() -> None:
    PROBE_DIR.mkdir(parents=True, exist_ok=True)

    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    result: dict[str, Any] = {
        "arr_convention": None,
        "fix1_current": None,
        "fix2_current": None,
        "calendar_quarter_probe": None,
        "body_shape_probe": None,
    }

    result["arr_convention"] = probe_arr_convention(inst, tok)
    result["fix1_current"] = probe_fix1_current_state(inst, tok)
    result["fix2_current"] = probe_fix2_current_state(inst, tok)
    result["calendar_quarter_probe"] = probe_calendar_quarter_grouping(inst, tok)
    result["body_shape_probe"] = probe_body_shape_roundtrip(inst, tok)

    with open(CONFIRMED_PATH, "w") as f:
        json.dump(result, f, indent=2)
    print()
    print(f"Probe results written to {CONFIRMED_PATH}")
    print()
    print("SUMMARY")
    print("=" * 60)
    arr = result["arr_convention"]
    print(f"ARR aggregate form: {arr.get('confirmed_arr_aggregate')!r}")
    cq = result["calendar_quarter_probe"]
    print(f"Calendar-quarter shape found: {cq.get('found')}")
    if cq.get("found"):
        print(f"  shape: {cq.get('calendar_quarter_shape')}")
    bs = result["body_shape_probe"]
    print(f"Body shape probe: {'OK' if bs.get('ok') else 'FAIL'}")
    if not bs.get("ok"):
        print(f"  error: {bs.get('error')}")
        if bs.get("MANUAL_ACTION"):
            print(f"  MANUAL ACTION REQUIRED: {bs.get('MANUAL_ACTION')}")
        sys.exit(1)


if __name__ == "__main__":
    main()
