#!/usr/bin/env python3
"""Phase 1.5 - PATCH shape probe.

Empirically confirms the Analytics REST API PATCH body shape for standard
SF reports in this org. Picks a low-risk OK widget from the Phase 2 audit
(via /tmp/phase1_5_probe/targets.json written by Task 1), GETs its
metadata, attempts a PATCH with a cosmetic name change, verifies, and
restores.

Uncommitted by convention (matches audit_*.py family pattern).

Success: writes the confirmed PATCH body shape to
/tmp/phase1_5_probe/confirmed_shape.json and prints it.
Failure: up to 5 retry attempts with progressively refined bodies.
If all 5 fail, exits non-zero with the last error body.

Run:
    python3 scripts/phase1_5_patch_shape_probe.py
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

TARGETS_PATH = Path("/tmp/phase1_5_probe/targets.json")
CONFIRMED_SHAPE_PATH = Path("/tmp/phase1_5_probe/confirmed_shape.json")
ORIGINAL_PATH = Path("/tmp/phase1_5_probe/original.json")
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"


def get_auth() -> tuple[str, str]:
    """Shell out to `sf org display` and extract (instanceUrl, accessToken)."""
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def load_probe_target() -> dict[str, Any]:
    """Read targets.json from Task 1 and return the probe_target dict."""
    if not TARGETS_PATH.exists():
        print(f"ERROR: {TARGETS_PATH} does not exist. Run Task 1 first.")
        sys.exit(1)
    with open(TARGETS_PATH) as f:
        targets = json.load(f)
    probe = targets.get("probe_target")
    if not probe or not probe.get("report_id"):
        print("ERROR: no probe_target in targets.json")
        sys.exit(1)
    return probe


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


def try_patch_report(
    inst: str, tok: str, report_id: str, body: dict[str, Any]
) -> tuple[bool, int, str]:
    """PATCH /analytics/reports/{id} with the given body. Return (ok, status, body)."""
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
        return False, 0, f"network error: {e}"
    return r.ok, r.status_code, r.text[:1000]


def main() -> None:
    print("Phase 1.5 PATCH shape probe")
    print("=" * 60)

    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    probe = load_probe_target()
    report_id = probe["report_id"]
    print(f"Probe target: {report_id}  ({probe.get('widget_title', '')})")

    # Step 1: GET original
    describe = get_report_describe(inst, tok, report_id)
    with open(ORIGINAL_PATH, "w") as f:
        json.dump(describe, f, indent=2)
    print(f"Original saved to {ORIGINAL_PATH}")

    original_metadata = describe.get("reportMetadata", {})
    original_name = original_metadata.get("name", "")
    print(f"Original name: {original_name!r}")

    # Build progressively-refined PATCH body attempts
    probe_name = f"{original_name} (probe)"
    attempts: list[tuple[str, dict[str, Any]]] = []

    # Attempt 1: wrapped in reportMetadata envelope, full metadata, name modified
    md1 = copy.deepcopy(original_metadata)
    md1["name"] = probe_name
    attempts.append(("wrapped_full", {"reportMetadata": md1}))

    # Attempt 2: wrapped, but strip known read-only fields (id, createdDate, lastModifiedDate, etc.)
    md2 = copy.deepcopy(original_metadata)
    md2["name"] = probe_name
    for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
        md2.pop(ro_field, None)
    attempts.append(("wrapped_stripped", {"reportMetadata": md2}))

    # Attempt 3: bare body (no envelope)
    md3 = copy.deepcopy(original_metadata)
    md3["name"] = probe_name
    attempts.append(("bare_full", md3))

    # Attempt 4: bare, stripped
    md4 = copy.deepcopy(original_metadata)
    md4["name"] = probe_name
    for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
        md4.pop(ro_field, None)
    attempts.append(("bare_stripped", md4))

    # Attempt 5: only name change, wrapped (sparse update)
    attempts.append(("sparse_name_only", {"reportMetadata": {"name": probe_name}}))

    confirmed_shape = None
    confirmed_label = None
    for label, body in attempts:
        print(f"\nAttempt {label}...")
        ok, status, resp = try_patch_report(inst, tok, report_id, body)
        print(f"  HTTP {status}")
        if ok:
            # Verify the change actually landed
            verify = get_report_describe(inst, tok, report_id)
            verify_name = verify.get("reportMetadata", {}).get("name", "")
            if verify_name == probe_name:
                print(f"  Verified: name changed to {probe_name!r}")
                confirmed_shape = body
                confirmed_label = label
                break
            else:
                print(
                    f"  WARNING: PATCH returned 2xx but name did not change (got {verify_name!r})"
                )
                continue
        else:
            print(f"  Response: {resp[:300]}")

    if confirmed_shape is None or confirmed_label is None:
        print("\nALL 5 ATTEMPTS FAILED. Escalate to controller.")
        sys.exit(1)

    # After this point, confirmed_label is guaranteed str (pyright type narrowing)
    print(f"\nCONFIRMED body shape: {confirmed_label}")

    # Restore the original name using the SAME body shape
    print("\nRestoring original name...")
    restore_body: dict[str, Any]
    if confirmed_label.startswith("wrapped"):
        md_restore = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md_restore.pop(ro_field, None)
        restore_body = {"reportMetadata": md_restore}
    elif confirmed_label.startswith("bare"):
        md_restore = copy.deepcopy(original_metadata)
        if "stripped" in confirmed_label:
            for ro_field in ("id", "createdDate", "lastModifiedDate", "lastRunDate"):
                md_restore.pop(ro_field, None)
        restore_body = md_restore
    elif confirmed_label == "sparse_name_only":
        restore_body = {"reportMetadata": {"name": original_name}}
    else:
        print(f"ERROR: unknown confirmed_label {confirmed_label!r}")
        sys.exit(1)

    ok, status, resp = try_patch_report(inst, tok, report_id, restore_body)
    if not ok:
        print(f"RESTORE FAILED: HTTP {status}: {resp[:300]}")
        print(f"Manual action required: PATCH {report_id} with name={original_name!r}")
        sys.exit(1)

    verify = get_report_describe(inst, tok, report_id)
    verify_name = verify.get("reportMetadata", {}).get("name", "")
    if verify_name != original_name:
        print(
            f"RESTORE VERIFY FAILED: name is {verify_name!r}, expected {original_name!r}"
        )
        sys.exit(1)
    print(f"Verified: name restored to {original_name!r}")

    # Write the confirmed shape to disk
    shape_info = {
        "label": confirmed_label,
        "probe_report_id": report_id,
        "probe_timestamp": None,  # Not critical
        "description": (
            "Use this body shape for subsequent PATCH operations against "
            "standard SF reports. 'wrapped_*' means wrap in {\"reportMetadata\": ...}. "
            "'bare_*' means the metadata is the root of the body. "
            "'stripped' means remove read-only fields (id, createdDate, lastModifiedDate, lastRunDate) "
            "before sending."
        ),
    }
    with open(CONFIRMED_SHAPE_PATH, "w") as f:
        json.dump(shape_info, f, indent=2)
    print(f"\nConfirmed shape written to {CONFIRMED_SHAPE_PATH}")
    print(f"Label: {confirmed_label}")


if __name__ == "__main__":
    main()
