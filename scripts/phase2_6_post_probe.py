#!/usr/bin/env python3
"""Phase 2.6 - POST shape probe.

Validates POST /analytics/reports works in this org by cloning
Dashboard 2's "Missing Decision Reason" report (00OTb000008el0PMAQ)
as a test report named "Phase 2.6 POST Probe Test (delete me)".

Saves the confirmed metadata template to
/tmp/phase2_6_probe/confirmed.json for the Phase 2.6 build script.

Uncommitted by convention.

Run:
    python3 scripts/phase2_6_post_probe.py
"""

from __future__ import annotations

import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

# Constants
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
TEMPLATE_REPORT_ID = "00OTb000008el0PMAQ"
PROBE_DIR = Path("/tmp/phase2_6_probe")
CONFIRMED_PATH = PROBE_DIR / "confirmed.json"
TEST_REPORT_ID_PATH = PROBE_DIR / "test_report_id.txt"

PROBE_NAME = "Phase 2.6 POST Probe Test (delete me)"
PROBE_DEV_NAME = "Phase_2_6_POST_Probe_Test"

# Fields that are read-only / server-assigned and must be stripped from POST bodies
READ_ONLY_FIELDS = (
    "id",
    "createdDate",
    "lastModifiedDate",
    "lastRunDate",
    "lastModifiedById",
    "createdById",
    "currency",  # org-level — let it inherit
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


def try_post_report(
    inst: str, tok: str, body: dict[str, Any]
) -> tuple[bool, int, dict[str, Any]]:
    """POST /analytics/reports. Returns (ok, status_code, response_dict)."""
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
        return False, 0, {"error": f"network error: {e}"}

    try:
        resp_dict = r.json()
    except ValueError:
        resp_dict = {"raw_text": r.text[:1000]}

    return r.ok, r.status_code, resp_dict


def try_delete_report(inst: str, tok: str, report_id: str) -> tuple[bool, int, str]:
    """DELETE /analytics/reports/{id}. Returns (ok, status_code, text)."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}"
    try:
        r = requests.delete(
            url,
            headers={"Authorization": f"Bearer {tok}"},
            timeout=30,
        )
    except requests.RequestException as e:
        return False, 0, f"network error: {e}"
    return r.ok, r.status_code, r.text[:500]


def strip_read_only(md: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of md with known read-only fields removed."""
    out = copy.deepcopy(md)
    for f in READ_ONLY_FIELDS:
        out.pop(f, None)
    return out


def build_probe_metadata(
    template_md: dict[str, Any],
    strip_ro: bool = True,
    strip_folder: bool = False,
) -> dict[str, Any]:
    """Clone the template metadata and apply probe name/devName overrides."""
    md = copy.deepcopy(template_md)
    md["name"] = PROBE_NAME
    md["developerName"] = PROBE_DEV_NAME
    if strip_ro:
        for f in READ_ONLY_FIELDS:
            md.pop(f, None)
    if strip_folder:
        md.pop("folderId", None)
        md.pop("folderName", None)
    return md


def extract_filter_shape(md: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the first few reportFilters entries to show the column-ref convention."""
    filters = md.get("reportFilters") or []
    return filters[:5]  # representative sample


def extract_groupings_shape(md: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract groupingsDown entries."""
    return md.get("groupingsDown") or []


def extract_detail_columns(md: dict[str, Any]) -> list[str]:
    """Extract detailColumns entries."""
    return md.get("detailColumns") or []


def main() -> None:  # noqa: C901 — intentionally linear flow
    PROBE_DIR.mkdir(parents=True, exist_ok=True)

    print("Phase 2.6 POST shape probe")
    print("=" * 60)

    # Step 1: Auth
    inst, tok = get_auth()
    print(f"Auth OK: {inst}")

    # Step 2: GET template
    print(f"\nFetching template: {TEMPLATE_REPORT_ID}")
    describe = get_report_describe(inst, tok, TEMPLATE_REPORT_ID)
    template_md = describe.get("reportMetadata", {})

    # Save the raw template for reference
    template_raw_path = PROBE_DIR / "template_raw.json"
    with open(template_raw_path, "w") as f:
        json.dump(describe, f, indent=2)
    print(f"Template raw saved: {template_raw_path}")

    # Extract key fields for reporting
    report_type = template_md.get("reportType", {})
    report_type_name = (
        report_type.get("type") if isinstance(report_type, dict) else str(report_type)
    )
    folder_id = template_md.get("folderId", "")
    template_name = template_md.get("name", "")
    template_format = template_md.get("reportFormat", "")

    print(f"Template name:    {template_name!r}")
    print(f"reportType:       {report_type_name!r}")
    print(f"folderId:         {folder_id!r}")
    print(f"reportFormat:     {template_format!r}")
    print(f"groupingsDown:    {extract_groupings_shape(template_md)}")
    print(f"reportFilters:    {extract_filter_shape(template_md)}")
    print(f"detailColumns:    {extract_detail_columns(template_md)[:8]}")
    print(f"aggregates:       {template_md.get('aggregates', [])}")

    # Step 3: Build progressively-refined POST body attempts
    # ---
    # Attempt 1: full template + name/devName changed + read-only stripped
    md1 = build_probe_metadata(template_md, strip_ro=True, strip_folder=False)
    attempt1 = ("full_strip_ro", {"reportMetadata": md1})

    # Attempt 2: same but also strip folderId (let API default it)
    md2 = build_probe_metadata(template_md, strip_ro=True, strip_folder=True)
    attempt2 = ("full_strip_ro_and_folder", {"reportMetadata": md2})

    # Attempt 3: minimal body — only reportType, name, developerName, reportFormat, filters
    md3: dict[str, Any] = {
        "name": PROBE_NAME,
        "developerName": PROBE_DEV_NAME,
        "reportFormat": template_format,
        "reportType": report_type,
    }
    # Include filters if they exist
    if template_md.get("reportFilters"):
        md3["reportFilters"] = copy.deepcopy(template_md["reportFilters"])
    attempt3 = ("minimal_essential", {"reportMetadata": md3})

    # Attempt 4: try reportType as string (in case the nested dict is wrong)
    md4: dict[str, Any] = {
        "name": PROBE_NAME,
        "developerName": PROBE_DEV_NAME,
        "reportFormat": template_format,
        "reportType": {"type": report_type_name} if report_type_name else report_type,
    }
    # Also include folderId since Salesforce sometimes requires it for non-personal folders
    if folder_id:
        md4["folderId"] = folder_id
    attempt4 = ("minimal_with_folder_string_type", {"reportMetadata": md4})

    # Attempt 5: bare metadata (no envelope) — some orgs accept this form
    md5 = build_probe_metadata(template_md, strip_ro=True, strip_folder=False)
    attempt5 = ("bare_metadata_no_envelope", md5)

    attempts = [attempt1, attempt2, attempt3, attempt4, attempt5]

    # Step 4: Run POST attempts until one succeeds
    new_id: str | None = None
    confirmed_attempt_label: str | None = None
    confirmed_body: dict[str, Any] | None = None
    last_error: dict[str, Any] = {}

    for idx, (label, body) in enumerate(attempts, 1):
        print(f"\n--- POST Attempt {idx}: {label} ---")
        ok, status, resp = try_post_report(inst, tok, body)
        print(f"  HTTP {status}")
        if ok:
            # Try to extract the new report Id from the response
            candidate_id = (
                resp.get("id")
                or resp.get("Id")
                or resp.get("reportMetadata", {}).get("id")
                or resp.get("reportMetadata", {}).get("Id")
            )
            if candidate_id:
                new_id = candidate_id
                confirmed_attempt_label = label
                confirmed_body = body
                print(f"  New report Id: {new_id}")
                break
            else:
                # 2xx but no id — treat as unexpected
                print(
                    f"  2xx but no id in response. Response keys: {list(resp.keys())}"
                )
                # Still might be usable — search the response dict recursively
                resp_str = json.dumps(resp)
                # Look for a Salesforce Id-like value (15 or 18 chars starting with 00O)
                import re

                id_match = re.search(r'"(00O[A-Za-z0-9]{12,15})"', resp_str)
                if id_match:
                    new_id = id_match.group(1)
                    confirmed_attempt_label = label
                    confirmed_body = body
                    print(f"  Extracted id via regex: {new_id}")
                    break
                last_error = resp
        else:
            err_text = json.dumps(resp)[:400]
            print(f"  Response: {err_text}")
            last_error = resp

    if new_id is None:
        print("\n" + "=" * 60)
        print("ALL 5 POST ATTEMPTS FAILED.")
        print(f"Last error: {json.dumps(last_error, indent=2)[:600]}")
        sys.exit(1)

    print(f"\nPOST SUCCEEDED — attempt: {confirmed_attempt_label!r}")
    print(f"New report Id: {new_id}")

    # Step 5: GET the new report to verify name
    print(f"\nVerifying new report {new_id}...")
    try:
        verify_desc = get_report_describe(inst, tok, new_id)
        verify_name = verify_desc.get("reportMetadata", {}).get("name", "")
        print(f"  Verified name: {verify_name!r}")
        if verify_name != PROBE_NAME:
            print(
                f"  WARNING: expected {PROBE_NAME!r}, got {verify_name!r} — "
                "report was created but name may have been sanitized"
            )
    except Exception as e:
        print(f"  WARNING: GET verify failed: {e}")
        print("  Continuing — the POST succeeded so the report was created.")

    # Step 6: Save the test report Id for manual cleanup fallback
    TEST_REPORT_ID_PATH.write_text(new_id + "\n")
    print(f"Test report Id saved: {TEST_REPORT_ID_PATH}")

    # Step 7: DELETE cleanup
    print(f"\nAttempting DELETE of {new_id}...")
    del_ok, del_status, del_text = try_delete_report(inst, tok, new_id)
    if del_ok:
        print(f"  DELETE OK (HTTP {del_status}) — test report cleaned up.")
        delete_result = "DELETED"
    else:
        print(f"  DELETE returned HTTP {del_status}: {del_text[:200]}")
        print(f"  Manual cleanup needed: delete report Id {new_id} via Salesforce UI.")
        delete_result = f"MANUAL_CLEANUP_NEEDED:{new_id}"

    # Step 8: Build the post_body_shape descriptor
    # Describes what the successful body contained so the build script can clone it
    post_body_shape: dict[str, Any] = {
        "attempt_label": confirmed_attempt_label,
        "envelope": "reportMetadata" in (confirmed_body or {}),
        "include_read_only": False,  # always stripped in our attempts
        "include_folder_id": "folderId"
        in ((confirmed_body or {}).get("reportMetadata", confirmed_body or {})),
        "body_top_level_keys": list((confirmed_body or {}).keys()),
        "reportMetadata_keys": list(
            (confirmed_body or {})
            .get(
                "reportMetadata",
                (confirmed_body or {}),
            )
            .keys()
        ),
    }

    # Step 9: Write confirmed.json
    # Identify filter shape convention
    filter_samples = extract_filter_shape(template_md)
    filter_shape_notes: dict[str, Any] = {}
    if filter_samples:
        sample = filter_samples[0]
        filter_shape_notes = {
            "column_ref_example": sample.get("column"),
            "operator_example": sample.get("operator"),
            "value_example": sample.get("value"),
            "full_sample": sample,
        }

    confirmed: dict[str, Any] = {
        "template_report_id": TEMPLATE_REPORT_ID,
        "template_metadata": template_md,
        "report_type_api_name": report_type_name,
        "folder_id": folder_id,
        "report_format": template_format,
        "groupings_down_shape": extract_groupings_shape(template_md),
        "detail_columns_sample": extract_detail_columns(template_md),
        "filter_shape": filter_shape_notes,
        "aggregates": template_md.get("aggregates", []),
        "post_body_shape": post_body_shape,
        "post_test_result": {
            "new_report_id": new_id,
            "attempt_label": confirmed_attempt_label,
            "delete_result": delete_result,
        },
    }

    with open(CONFIRMED_PATH, "w") as f:
        json.dump(confirmed, f, indent=2)
    print(f"\nConfirmed metadata template saved: {CONFIRMED_PATH}")

    # Step 10: Print SUMMARY
    print()
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Template report:      {TEMPLATE_REPORT_ID}")
    print(f"reportType API name:  {report_type_name!r}")
    print(f"folderId:             {folder_id!r}")
    print(f"reportFormat:         {template_format!r}")
    print(f"groupingsDown:        {extract_groupings_shape(template_md)}")
    print()
    print("Filter shape convention:")
    if filter_shape_notes:
        print(f"  column ref:  {filter_shape_notes.get('column_ref_example')!r}")
        print(f"  operator:    {filter_shape_notes.get('operator_example')!r}")
        print(f"  value:       {filter_shape_notes.get('value_example')!r}")
    else:
        print("  (no reportFilters on template — no filter shape available)")
    print()
    print(f"POST succeeded:       YES — attempt {confirmed_attempt_label!r}")
    print(f"New report Id:        {new_id}")
    print(f"DELETE result:        {delete_result}")
    print()
    print(f"confirmed.json:       {CONFIRMED_PATH}")
    print(f"test_report_id.txt:   {TEST_REPORT_ID_PATH}")
    print()
    print("Phase 2.6 POST probe COMPLETE.")


if __name__ == "__main__":
    main()
