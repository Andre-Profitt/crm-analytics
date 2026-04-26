#!/usr/bin/env python3
"""Phase 2.5 B-core - Dashboard 2 defect patcher.

Applies two mechanical fixes to Dashboard 2 (01ZTb00000FSP9JMAX) reports:

- Fix 1: ph_no_activity_30_plus - swap s!AMOUNT to the canonical ARR
  aggregate (form determined by the probe) and ensure the ARR field
  is in detailColumns.
- Fix 2: ph_overdue_opportunities - swap FISCAL_QUARTER in groupingsDown
  for the calendar-quarter shape from the probe. Deferred if the probe
  did not find a usable shape.

Backs up both reports to /tmp/phase2_5_backup/reports/ before any PATCH.
Supports --dry-run.

Uncommitted by convention.

Design: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-design.md
Plan: docs/2026-04-08-phase2-5-core-dashboard2-defect-fixes-plan.md

Run:
    python3 scripts/phase2_5_core_patch.py --dry-run
    python3 scripts/phase2_5_core_patch.py
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

# %% Constants

TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"

FIX1_REPORT_ID = "00OTb000008TaEnMAK"  # ph_no_activity_30_plus
FIX2_REPORT_ID = "00OTb000008SrmLMAS"  # ph_overdue_opportunities

ARR_DETAIL_COLUMN = "Opportunity.APTS_Opportunity_ARR__c.CONVERT"

PROBE_CONFIRMED_PATH = Path("/tmp/phase2_5_probe/confirmed.json")
BACKUP_DIR = Path("/tmp/phase2_5_backup")
REPORT_BACKUP_DIR = BACKUP_DIR / "reports"


def _parse_args():
    p = argparse.ArgumentParser(
        description="Phase 2.5 B-core - Dashboard 2 defect patcher"
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Print PATCH bodies instead of sending them. Auth + backup still happen.",
    )
    return p.parse_args()


# %% Cell 1: Auth


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


def _cell1_main() -> tuple[str, str]:
    inst, tok = get_auth()
    print(f"Cell 1 (auth): instance={inst}")
    return inst, tok


def load_confirmed() -> dict[str, Any]:
    if not PROBE_CONFIRMED_PATH.exists():
        print(f"ERROR: {PROBE_CONFIRMED_PATH} does not exist. Run Task 1 probe first.")
        sys.exit(1)
    with open(PROBE_CONFIRMED_PATH) as f:
        return json.load(f)


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def patch_report(
    inst: str, tok: str, report_id: str, body: dict[str, Any], dry_run: bool
) -> dict[str, Any]:
    if dry_run:
        body_str = json.dumps(body)[:400]
        print(f"  [DRY RUN] would PATCH {report_id}: body[:400]={body_str}")
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


# %% Cell 2: Backup


def _cell2_main(inst: str, tok: str) -> None:
    REPORT_BACKUP_DIR.mkdir(parents=True, exist_ok=True)

    for rid in (FIX1_REPORT_ID, FIX2_REPORT_ID):
        dest = REPORT_BACKUP_DIR / f"{rid}.json"
        try:
            describe = get_report_describe(inst, tok, rid)
            with open(dest, "w") as f:
                json.dump(describe, f, indent=2)
            print(f"Cell 2 (backup): {rid} -> {dest}")
        except Exception as e:
            print(f"Cell 2: BACKUP FAILED for {rid}: {e}")
            sys.exit(1)

    # Write rollback helper
    rollback_path = BACKUP_DIR / "rollback_one.sh"
    with open(rollback_path, "w") as f:
        f.write("""#!/bin/bash
# Usage: rollback_one.sh <report_id>
set -e
REPORT_ID="$1"
if [ -z "$REPORT_ID" ]; then
  echo "Usage: $0 <report_id>"
  exit 1
fi
BACKUP="/tmp/phase2_5_backup/reports/${REPORT_ID}.json"
if [ ! -f "$BACKUP" ]; then
  echo "No backup at $BACKUP"
  exit 1
fi
SF_JSON=$(sf org display --target-org apro@simcorp.com --json 2>/dev/null)
INST=$(python3 -c "import json,sys; t='''$SF_JSON'''; print(json.loads(t[t.find('{'):])['result']['instanceUrl'])")
TOK=$(python3 -c "import json,sys; t='''$SF_JSON'''; print(json.loads(t[t.find('{'):])['result']['accessToken'])")
BODY=$(python3 -c "import json; d=json.load(open('$BACKUP')); print(json.dumps({'reportMetadata': d['reportMetadata']}))")
curl -s -X PATCH "${INST}/services/data/v66.0/analytics/reports/${REPORT_ID}" \\
  -H "Authorization: Bearer ${TOK}" \\
  -H "Content-Type: application/json" \\
  -d "$BODY"
echo
echo "Restored ${REPORT_ID} from ${BACKUP}"
""")
    rollback_path.chmod(0o755)
    print(f"Cell 2: wrote rollback helper to {rollback_path}")


# %% Cell 3: Fix 1 - ARR aggregate swap + detailColumns fix


def transform_no_activity_metadata(
    original_metadata: dict[str, Any], target_arr_aggregate: str
) -> tuple[dict[str, Any], bool]:
    """Build the new reportMetadata for Fix 1.

    Returns (new_metadata, changed). `changed` is False if both the
    aggregate and the detailColumn are already correct (idempotent).
    """
    current_aggs = list(original_metadata.get("aggregates") or [])
    current_detail = list(original_metadata.get("detailColumns") or [])

    changed = False
    new_aggs = list(current_aggs)
    if new_aggs and new_aggs[0] != target_arr_aggregate:
        new_aggs[0] = target_arr_aggregate
        changed = True

    new_detail = list(current_detail)
    if ARR_DETAIL_COLUMN not in new_detail:
        new_detail.append(ARR_DETAIL_COLUMN)
        changed = True

    new_metadata = copy.deepcopy(original_metadata)
    new_metadata["aggregates"] = new_aggs
    new_metadata["detailColumns"] = new_detail
    return new_metadata, changed


def _cell3_main(inst: str, tok: str, target_arr_aggregate: str, dry_run: bool) -> bool:
    """Fix 1: ph_no_activity_30_plus aggregate + detailColumns. Returns True on success."""
    # Inline tests

    # Test 1: swap happens and ARR added to detailColumns
    _fake = {
        "aggregates": ["s!AMOUNT", "RowCount"],
        "detailColumns": ["FULL_NAME"],
    }
    _new, _changed = transform_no_activity_metadata(
        _fake, "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    )
    assert _changed is True
    assert _new["aggregates"][0] == "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    assert _new["aggregates"][1] == "RowCount"
    assert "Opportunity.APTS_Opportunity_ARR__c.CONVERT" in _new["detailColumns"]
    assert "FULL_NAME" in _new["detailColumns"]

    # Test 2: idempotent when already correct
    _fake2 = {
        "aggregates": ["s!Opportunity.APTS_Opportunity_ARR__c.CONVERT", "RowCount"],
        "detailColumns": ["FULL_NAME", "Opportunity.APTS_Opportunity_ARR__c.CONVERT"],
    }
    _new, _changed = transform_no_activity_metadata(
        _fake2, "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    )
    assert _changed is False

    # Test 3: detailColumns not duplicated
    _fake3 = {
        "aggregates": ["s!AMOUNT"],
        "detailColumns": ["Opportunity.APTS_Opportunity_ARR__c.CONVERT", "FULL_NAME"],
    }
    _new, _changed = transform_no_activity_metadata(
        _fake3, "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    )
    assert _changed is True  # aggregate still needed
    assert (
        _new["detailColumns"].count("Opportunity.APTS_Opportunity_ARR__c.CONVERT") == 1
    )

    # Test 4: different target aggregate (with .CONVERT)
    _fake4 = {
        "aggregates": ["s!AMOUNT"],
        "detailColumns": [],
    }
    _new, _changed = transform_no_activity_metadata(
        _fake4, "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    )
    assert _changed is True
    assert _new["aggregates"][0] == "s!Opportunity.APTS_Opportunity_ARR__c.CONVERT"
    assert "Opportunity.APTS_Opportunity_ARR__c.CONVERT" in _new["detailColumns"]

    print("Cell 3 (Fix 1) tests: PASS")

    backup_path = REPORT_BACKUP_DIR / f"{FIX1_REPORT_ID}.json"
    with open(backup_path) as f:
        describe = json.load(f)
    original_metadata = describe.get("reportMetadata", {})

    new_metadata, changed = transform_no_activity_metadata(
        original_metadata, target_arr_aggregate
    )
    if not changed:
        print(f"Cell 3: {FIX1_REPORT_ID} already correct. SKIP.")
        return True

    print(
        f"Cell 3: {FIX1_REPORT_ID} aggregates {original_metadata.get('aggregates')} -> {new_metadata['aggregates']}"
    )
    print(f"Cell 3: {FIX1_REPORT_ID} detailColumns +{ARR_DETAIL_COLUMN!r}")

    body = {"reportMetadata": new_metadata}
    result = patch_report(inst, tok, FIX1_REPORT_ID, body, dry_run)
    if not result["ok"]:
        print(f"Cell 3: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        return True

    # Inline verify
    try:
        verify = get_report_describe(inst, tok, FIX1_REPORT_ID)
    except Exception as e:
        print(f"Cell 3: VERIFY GET FAILED: {e}")
        return False
    v_aggs = verify.get("reportMetadata", {}).get("aggregates") or []
    if not v_aggs or v_aggs[0] != target_arr_aggregate:
        print(
            f"Cell 3: VERIFY FAILED: aggregates[0]={v_aggs[0] if v_aggs else None} (expected {target_arr_aggregate})"
        )
        return False
    v_detail = verify.get("reportMetadata", {}).get("detailColumns") or []
    if ARR_DETAIL_COLUMN not in v_detail:
        print(
            f"Cell 3: VERIFY FAILED: {ARR_DETAIL_COLUMN} not in detailColumns {v_detail}"
        )
        return False
    print(f"Cell 3: {FIX1_REPORT_ID} verified")
    return True


# %% Cell 4: Fix 2 - FISCAL_QUARTER grouping swap (or defer)


def transform_overdue_metadata(
    original_metadata: dict[str, Any], calendar_quarter_shape: dict[str, Any] | None
) -> tuple[dict[str, Any] | None, str]:
    """Build the new reportMetadata for Fix 2.

    Returns (new_metadata, status). Status is one of:
    - "changed" - fix applied, PATCH should proceed
    - "noop" - already correct, no PATCH needed
    - "deferred" - no calendar shape available, skip Fix 2 entirely
    """
    if calendar_quarter_shape is None:
        return None, "deferred"

    current_gd = list(original_metadata.get("groupingsDown") or [])
    # Find the FISCAL_QUARTER entry
    fiscal_idx = None
    for i, g in enumerate(current_gd):
        if isinstance(g, dict) and "FISCAL_QUARTER" in (g.get("name") or "").upper():
            fiscal_idx = i
            break

    if fiscal_idx is None:
        return copy.deepcopy(original_metadata), "noop"

    new_gd = copy.deepcopy(current_gd)
    new_gd[fiscal_idx] = copy.deepcopy(calendar_quarter_shape)

    new_metadata = copy.deepcopy(original_metadata)
    new_metadata["groupingsDown"] = new_gd
    return new_metadata, "changed"


def _cell4_main(
    inst: str, tok: str, calendar_quarter_shape: dict[str, Any] | None, dry_run: bool
) -> bool:
    """Fix 2: ph_overdue_opportunities grouping swap. Returns True on success or deferral."""
    # Inline tests

    # Test 1: FISCAL_QUARTER replaced when shape is provided
    _shape = {"name": "CLOSE_DATE", "dateGranularity": "Quarter", "sortOrder": "Asc"}
    _fake = {
        "groupingsDown": [
            {"name": "Opportunity.Account_Unit_Group__c", "sortOrder": "Asc"},
            {"name": "FISCAL_QUARTER", "sortOrder": "Asc"},
        ]
    }
    _new, _status = transform_overdue_metadata(_fake, _shape)
    assert _status == "changed"
    assert _new is not None
    assert _new["groupingsDown"][0]["name"] == "Opportunity.Account_Unit_Group__c"
    assert _new["groupingsDown"][1]["name"] == "CLOSE_DATE"
    assert _new["groupingsDown"][1]["dateGranularity"] == "Quarter"

    # Test 2: noop if FISCAL_QUARTER not present
    _fake2 = {
        "groupingsDown": [
            {"name": "Opportunity.Account_Unit_Group__c", "sortOrder": "Asc"},
            {"name": "STAGE_NAME", "sortOrder": "Asc"},
        ]
    }
    _new, _status = transform_overdue_metadata(_fake2, _shape)
    assert _status == "noop"

    # Test 3: deferred when no shape provided
    _new, _status = transform_overdue_metadata(_fake, None)
    assert _status == "deferred"
    assert _new is None

    print("Cell 4 (Fix 2) tests: PASS")

    backup_path = REPORT_BACKUP_DIR / f"{FIX2_REPORT_ID}.json"
    with open(backup_path) as f:
        describe = json.load(f)
    original_metadata = describe.get("reportMetadata", {})

    new_metadata, status = transform_overdue_metadata(
        original_metadata, calendar_quarter_shape
    )
    if status == "deferred":
        print(
            "Cell 4: Fix 2 DEFERRED - no calendar-quarter shape available from probe"
        )
        return True  # Deferral is a successful outcome of B-core
    if status == "noop":
        print(f"Cell 4: {FIX2_REPORT_ID} already correct. SKIP.")
        return True

    print(
        f"Cell 4: {FIX2_REPORT_ID} groupingsDown FISCAL_QUARTER -> {calendar_quarter_shape}"
    )

    body = {"reportMetadata": new_metadata}
    result = patch_report(inst, tok, FIX2_REPORT_ID, body, dry_run)
    if not result["ok"]:
        print(f"Cell 4: PATCH FAILED: {result['error']}")
        return False

    if dry_run:
        return True

    # Inline verify
    try:
        verify = get_report_describe(inst, tok, FIX2_REPORT_ID)
    except Exception as e:
        print(f"Cell 4: VERIFY GET FAILED: {e}")
        return False
    v_gd = verify.get("reportMetadata", {}).get("groupingsDown") or []
    for g in v_gd:
        if isinstance(g, dict) and "FISCAL_QUARTER" in (g.get("name") or "").upper():
            print(
                "Cell 4: VERIFY FAILED: FISCAL_QUARTER still present in groupingsDown"
            )
            return False
    print(f"Cell 4: {FIX2_REPORT_ID} verified")
    return True


# %% Cell 5: Summary


def _cell5_main(fix1_ok: bool, fix2_ok: bool) -> None:
    print()
    print("=" * 60)
    print("Phase 2.5 B-core patch summary")
    print("=" * 60)
    print(f"Fix 1 (ph_no_activity_30_plus ARR): {'OK' if fix1_ok else 'FAILED'}")
    print(
        f"Fix 2 (ph_overdue_opportunities grouping): {'OK or DEFERRED' if fix2_ok else 'FAILED'}"
    )
    print()
    print(f"Backup directory: {BACKUP_DIR}")
    print(f"Rollback helper: {BACKUP_DIR}/rollback_one.sh")
    print()
    if fix1_ok and fix2_ok:
        print("SUCCESS. Next step: re-run the audit script and commit the output.")
    else:
        print("FAILED: one or more fixes failed. Inspect backups before rollback.")
    print("=" * 60)


# %% Main


def main() -> None:
    args = _parse_args()
    if args.dry_run:
        print("DRY RUN MODE - no PATCH operations will be sent")
        print()

    inst, tok = _cell1_main()
    _cell2_main(inst, tok)

    confirmed = load_confirmed()
    target_arr = confirmed.get("arr_convention", {}).get("confirmed_arr_aggregate")
    if not target_arr:
        print("ERROR: confirmed ARR aggregate missing from probe results.")
        sys.exit(1)
    calendar_shape = confirmed.get("calendar_quarter_probe", {}).get(
        "calendar_quarter_shape"
    )

    fix1_ok = _cell3_main(inst, tok, target_arr, args.dry_run)
    fix2_ok = _cell4_main(inst, tok, calendar_shape, args.dry_run)

    _cell5_main(fix1_ok, fix2_ok)


if __name__ == "__main__":
    main()
