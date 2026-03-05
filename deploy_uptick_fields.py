#!/usr/bin/env python3
"""
Deploy Uptick/Downtick formula fields to Opportunity and update the
'2026 Renewal Book + Expand Upticks' report with Uptick % Custom Summary Formula.

Usage:
    # Step 1: Re-authenticate with an admin user (needs ModifyMetadata)
    sf org login web --alias admin --instance-url https://simcorp.my.salesforce.com

    # Step 2: Deploy formula fields
    python3 deploy_uptick_fields.py --deploy-fields --target-org admin

    # Step 3: Update the report (can use the regular user)
    python3 deploy_uptick_fields.py --update-report --target-org apro@simcorp.com

    # Or do both at once with admin:
    python3 deploy_uptick_fields.py --deploy-fields --update-report --target-org admin
"""

import argparse
import json
import subprocess
import sys
import urllib.parse
import urllib.request

REPORT_ID = "00OTb000008YFUfMAO"


def get_sf_auth(target_org: str) -> tuple[str, str]:
    """Get instance URL and access token from sf CLI."""
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        capture_output=True,
        text=True,
    )
    data = json.loads(result.stdout)["result"]
    return data["instanceUrl"], data["accessToken"]


def deploy_formula_fields(target_org: str) -> bool:
    """Deploy Renewal_Forecast_ACV__c and Expand_Forecast_ACV__c via sf project deploy."""
    import os

    project_dir = os.path.join(os.path.dirname(__file__), "salesforce-reports")

    print("=== Deploying Uptick Formula Fields ===")
    print(f"Target org: {target_org}")
    print()

    # Validate first
    print("Step 1: Validating deployment...")
    result = subprocess.run(
        [
            "sf",
            "project",
            "deploy",
            "start",
            "--source-dir",
            "force-app/main/default/objects/Opportunity/fields/Renewal_Forecast_ACV__c.field-meta.xml",
            "--source-dir",
            "force-app/main/default/objects/Opportunity/fields/Expand_Forecast_ACV__c.field-meta.xml",
            "--target-org",
            target_org,
            "--dry-run",
            "--wait",
            "10",
        ],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Validation failed:\n{result.stderr}")
        return False
    print("  Validation passed.")

    # Deploy
    print("Step 2: Deploying fields...")
    result = subprocess.run(
        [
            "sf",
            "project",
            "deploy",
            "start",
            "--source-dir",
            "force-app/main/default/objects/Opportunity/fields/Renewal_Forecast_ACV__c.field-meta.xml",
            "--source-dir",
            "force-app/main/default/objects/Opportunity/fields/Expand_Forecast_ACV__c.field-meta.xml",
            "--target-org",
            target_org,
            "--wait",
            "10",
        ],
        cwd=project_dir,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        print(f"Deployment failed:\n{result.stderr}")
        return False

    print("  Fields deployed successfully!")
    print("    - Opportunity.Renewal_Forecast_ACV__c (Currency formula)")
    print("    - Opportunity.Expand_Forecast_ACV__c (Currency formula)")
    return True


def update_report(target_org: str) -> bool:
    """Add formula fields + Uptick % Custom Summary Formula to the report."""

    instance_url, access_token = get_sf_auth(target_org)

    print()
    print("=== Updating Report with Uptick % ===")
    print(f"Report ID: {REPORT_ID}")
    print()

    # Step 1: Get current report metadata
    print("Step 1: Fetching current report metadata...")
    url = f"{instance_url}/services/data/v66.0/analytics/reports/{REPORT_ID}/describe"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
        },
    )
    with urllib.request.urlopen(req) as resp:
        report_describe = json.loads(resp.read().decode())

    report_metadata = report_describe["reportMetadata"]
    current_columns = report_metadata["detailColumns"]
    current_aggregates = report_metadata.get("aggregates", [])

    print(f"  Current columns: {len(current_columns)}")
    print(f"  Current aggregates: {len(current_aggregates)}")

    # Step 2: Add the new columns if not already present
    new_columns = [
        "Opportunity.Renewal_Forecast_ACV__c.CONVERT",
        "Opportunity.Expand_Forecast_ACV__c.CONVERT",
    ]
    new_aggregates = [
        "s!Opportunity.Renewal_Forecast_ACV__c.CONVERT",
        "s!Opportunity.Expand_Forecast_ACV__c.CONVERT",
    ]

    columns_added = False
    for col in new_columns:
        if col not in current_columns:
            # Insert after Forecast ACV column
            try:
                idx = current_columns.index(
                    "Opportunity.APTS_Forecast_ACV_AVG__c.CONVERT"
                )
                current_columns.insert(idx + 1, col)
            except ValueError:
                current_columns.append(col)
            columns_added = True
            print(f"  Adding column: {col}")
        else:
            print(f"  Column already exists: {col}")

    for agg in new_aggregates:
        if agg not in current_aggregates:
            current_aggregates.append(agg)
            print(f"  Adding aggregate: {agg}")

    # Step 3: Add Custom Summary Formula for Uptick %
    csf = {
        "label": "Uptick %",
        "description": "Expand pipeline as a percentage of the Renewal base (Expand ACV SUM / Renewal ACV SUM)",
        "formulaType": "PERCENT",
        "decimalPlaces": 1,
        "downGroupingContext": "ACCOUNT_NAME",  # Display at Account grouping level
        "formula": "Opportunity.Expand_Forecast_ACV__c.CONVERT:SUM / Opportunity.Renewal_Forecast_ACV__c.CONVERT:SUM",
    }

    # Step 4: PATCH the report
    print()
    print("Step 2: Updating report...")

    patch_body = {
        "reportMetadata": {
            "detailColumns": current_columns,
            "aggregates": current_aggregates,
            "customSummaryFormulas": {
                "csf_uptick_pct": csf,
            },
        }
    }

    url = f"{instance_url}/services/data/v66.0/analytics/reports/{REPORT_ID}"
    data = json.dumps(patch_body).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
    )

    try:
        with urllib.request.urlopen(req) as resp:
            result = json.loads(resp.read().decode())
            print("  Report updated successfully!")
            print()
            print("=== Report Now Shows ===")
            rm = result.get("reportMetadata", {})
            print(f"  Name: {rm.get('name')}")
            print(f"  Columns: {len(rm.get('detailColumns', []))}")
            for col in rm.get("detailColumns", []):
                marker = (
                    " <-- NEW"
                    if "Renewal_Forecast" in col or "Expand_Forecast" in col
                    else ""
                )
                print(f"    - {col}{marker}")
            print()
            csfs = rm.get("customSummaryFormulas", {})
            if csfs:
                print("  Custom Summary Formulas:")
                for key, val in csfs.items():
                    print(f"    - {key}: {val.get('label')} = {val.get('formula')}")
            return True
    except urllib.error.HTTPError as e:
        error_body = e.read().decode()
        print(f"  Failed to update report (HTTP {e.code}):")
        print(f"  {error_body}")
        return False


def verify_fields(target_org: str) -> bool:
    """Verify the formula fields exist and return correct values."""

    instance_url, access_token = get_sf_auth(target_org)

    print()
    print("=== Verifying Formula Fields ===")

    # Check fields exist on Opportunity describe
    url = f"{instance_url}/services/data/v66.0/sobjects/Opportunity/describe/"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
        },
    )
    with urllib.request.urlopen(req) as resp:
        describe = json.loads(resp.read().decode())

    field_names = {f["name"] for f in describe["fields"]}
    for fname in ["Renewal_Forecast_ACV__c", "Expand_Forecast_ACV__c"]:
        if fname in field_names:
            print(f"  [OK] {fname} exists")
        else:
            print(f"  [MISSING] {fname} — deploy fields first")
            return False

    # Spot-check: query a few opps with both Renewal and Expand
    print()
    print("  Spot-checking values...")
    query = (
        "SELECT Account.Name, Type, APTS_Forecast_ACV_AVG__c, "
        "Renewal_Forecast_ACV__c, Expand_Forecast_ACV__c "
        "FROM Opportunity "
        "WHERE Type IN ('Renewal','Expand') AND IsClosed = false "
        "ORDER BY Account.Name "
        "LIMIT 20"
    )
    url = f"{instance_url}/services/data/v66.0/query/?q={urllib.parse.quote(query)}"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {access_token}",
        },
    )

    with urllib.request.urlopen(req) as resp:
        result = json.loads(resp.read().decode())

    print(
        f"  {'Account':<30} {'Type':<10} {'Forecast ACV':>15} {'Renewal ACV':>15} {'Expand ACV':>15}"
    )
    print(f"  {'-' * 30} {'-' * 10} {'-' * 15} {'-' * 15} {'-' * 15}")
    errors = 0
    for rec in result.get("records", []):
        acct = (rec.get("Account", {}) or {}).get("Name", "N/A")[:29]
        opp_type = rec.get("Type", "")
        forecast = rec.get("APTS_Forecast_ACV_AVG__c") or 0
        renewal = rec.get("Renewal_Forecast_ACV__c") or 0
        expand = rec.get("Expand_Forecast_ACV__c") or 0

        print(
            f"  {acct:<30} {opp_type:<10} {forecast:>15,.2f} {renewal:>15,.2f} {expand:>15,.2f}"
        )

        # Validate logic
        if opp_type == "Renewal" and renewal != forecast:
            print("    ERROR: Renewal ACV should equal Forecast ACV")
            errors += 1
        if opp_type == "Expand" and expand != forecast:
            print("    ERROR: Expand ACV should equal Forecast ACV")
            errors += 1
        if opp_type == "Renewal" and expand != 0:
            print("    ERROR: Expand ACV should be 0 for Renewal")
            errors += 1
        if opp_type == "Expand" and renewal != 0:
            print("    ERROR: Renewal ACV should be 0 for Expand")
            errors += 1

    if errors:
        print(f"\n  [FAIL] {errors} validation errors found")
        return False
    else:
        print(f"\n  [OK] All {result.get('totalSize', 0)} records validated correctly")
        return True


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Uptick % fields and update report"
    )
    parser.add_argument(
        "--deploy-fields",
        action="store_true",
        help="Deploy formula fields to Salesforce",
    )
    parser.add_argument(
        "--update-report",
        action="store_true",
        help="Update report with new columns + CSF",
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify formula fields return correct values",
    )
    parser.add_argument(
        "--target-org",
        default="apro@simcorp.com",
        help="Salesforce org alias or username",
    )
    args = parser.parse_args()

    if not any([args.deploy_fields, args.update_report, args.verify]):
        parser.print_help()
        print("\nExample workflow:")
        print(
            "  1. sf org login web --alias admin --instance-url https://simcorp.my.salesforce.com"
        )
        print("  2. python3 deploy_uptick_fields.py --deploy-fields --target-org admin")
        print(
            "  3. python3 deploy_uptick_fields.py --update-report --target-org apro@simcorp.com"
        )
        print(
            "  4. python3 deploy_uptick_fields.py --verify --target-org apro@simcorp.com"
        )
        sys.exit(0)

    success = True

    if args.deploy_fields:
        if not deploy_formula_fields(args.target_org):
            print(
                "\nField deployment failed. Ensure you have ModifyMetadata permission."
            )
            print(
                "Try: sf org login web --alias admin --instance-url https://simcorp.my.salesforce.com"
            )
            success = False
            sys.exit(1)

    if args.update_report:
        if not update_report(args.target_org):
            success = False
            sys.exit(1)

    if args.verify:
        if not verify_fields(args.target_org):
            success = False
            sys.exit(1)

    if success:
        print("\n=== All Steps Complete ===")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
