"""
Pull the 18 "Pipeline Forecast Review" Historical Trending reports and
append Q1 / Q2 snapshot sheets to each director's workbook.

Each report holds 4 snapshots with per-deal ARR and stage at each date,
plus Change columns. This is the authoritative SF view of what moved
quarter-to-quarter, higher fidelity than our OpportunityFieldHistory
reconstruction.

Output per director workbook:
  - Sheet "Q1 Snapshot Trend", <N> rows of deals that slipped out of Q1
  - Sheet "Q2 Snapshot Trend", <N> rows of deals currently in Q2

Re-runs replace those two sheets. Other sheets are untouched.
"""

import argparse
import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import requests
from openpyxl import load_workbook
from openpyxl.styles import Alignment, Font, PatternFill


ROOT = Path(__file__).resolve().parents[1]

# director workbook slug -> (Q1 report id, Q2 report id)
REPORTS = {
    "jesper-tyrer": ("00OTb000008g11VMAQ", "00OTb000008gYVJMA2"),  # APAC
    "megan-miceli": ("00OTb000008gYNFMA2", "00OTb000008gYdNMAU"),  # Canada
    "sarah-pittroff": ("00OTb000008gYFBMA2", "00OTb000008gYNGMA2"),  # CE
    "mourad-essofi": ("00OTb000008gYLdMAM", "00OTb000008gYblMAE"),  # MEA
    "patrick-gaughan": ("00OTb000008gYOrMAM", "00OTb000008gYezMAE"),  # NA AM
    "christian-ebbesen": ("00OTb000008gYK1MAM", "00OTb000008gYa9MAE"),  # NE
    "adam-steinhaus": ("00OTb000008gYQTMA2", "00OTb000008gYgbMAE"),  # P&I
    "francois-thaury": ("00OTb000008gYGnMAM", "00OTb000008gYWvMAM"),  # SWE
    "dan-peppett": ("00OTb000008gYIPMA2", "00OTb000008gYYXMA2"),  # UKI
}


HEADER_FILL = PatternFill(start_color="083EA7", end_color="083EA7", fill_type="solid")
HEADER_FONT = Font(bold=True, color="FFFFFF", size=10)
BODY_FONT = Font(size=9)
EUR_FMT = "#,##0"


def _auth():
    data = json.loads(
        subprocess.run(
            ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    )["result"]
    return data["accessToken"], data["instanceUrl"]


def _parse_column(raw):
    """Rewrite historical-trending column tokens to readable headers.

    Incoming:
        'Opportunity__hd.APTS_Forecast_ARR__c_hst.CONVERT.2026-04-12.Change'
        'Opportunity__hd.StageName__hst.CONVERT.2026-04-12'
        'Opportunity.Account.Name'
    Outgoing:
        'ARR Change 2026-04-12'
        'Stage 2026-04-12'
        'Account'
    """
    # Historical snapshot columns
    m = re.match(
        r"Opportunity__hd\.([A-Za-z0-9_]+)_hst(?:\.CONVERT)?\.(\d{4}-\d{2}-\d{2})(\.Change)?",
        raw,
    )
    if m:
        field, date, change = m.groups()
        friendly = {
            "APTS_Forecast_ARR__c": "ARR",
            "APTS_Opportunity_ARR__c": "Opp ARR",
            "StageName": "Stage",
            "CloseDate": "Close",
            "ForecastCategoryName": "ForecastCat",
        }.get(field, field)
        suffix = " Change" if change else ""
        return f"{friendly}{suffix} {date}"
    # Live (non-snapshot) columns
    short = {
        "Opportunity.Account.Name": "Account",
        "Opportunity.Name": "Opportunity",
        "Opportunity.CloseDate": "Close Date (live)",
        "Opportunity.StageName": "Stage (live)",
        "Opportunity.Owner.Name": "Owner",
        "Opportunity.APTS_Forecast_ARR__c": "ARR Wtd (live)",
        "Opportunity.APTS_Opportunity_ARR__c": "ARR Unwtd (live)",
    }.get(raw)
    return short or raw


def _run_report(session, instance, report_id):
    """Run a Historical Trending report and return (labels, dtypes, rows)."""
    r = session.post(
        f"{instance}/services/data/v66.0/analytics/reports/{report_id}"
        "?includeDetails=true",
        headers={"Content-Type": "application/json"},
    ).json()
    md = r.get("reportMetadata", {})
    cols = md.get("detailColumns", [])
    ext = r.get("reportExtendedMetadata", {}).get("detailColumnInfo", {})
    labels = [_parse_column(c) for c in cols]
    dtypes = [ext.get(c, {}).get("dataType", "string") for c in cols]
    rows = []
    for row in r.get("factMap", {}).get("T!T", {}).get("rows", []):
        rows.append([c.get("label", "") for c in row.get("dataCells", [])])
    return labels, dtypes, rows, md.get("historicalSnapshotDates", [])


def _write_sheet(wb, sheet_name, labels, dtypes, rows, snapshots):
    if sheet_name in wb.sheetnames:
        del wb[sheet_name]
    ws = wb.create_sheet(sheet_name)

    # Subtitle row with snapshot dates
    if snapshots:
        ws.cell(
            row=1,
            column=1,
            value=f"Historical Trending, snapshots: {', '.join(snapshots)}",
        ).font = Font(italic=True, size=9, color="595959")

    # Headers on row 2
    for i, label in enumerate(labels, 1):
        cell = ws.cell(row=2, column=i, value=label)
        cell.font = HEADER_FONT
        cell.fill = HEADER_FILL
        cell.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=True
        )
    ws.row_dimensions[2].height = 32

    # Data rows from row 3
    for r_i, row in enumerate(rows, 3):
        for c_i, (val, dt) in enumerate(zip(row, dtypes), 1):
            cell = ws.cell(row=r_i, column=c_i)
            if val is None or val == "":
                cell.value = None
            elif dt == "currency":
                # Values come as strings like 'EUR 1.234.567,89' or '-'
                try:
                    # Strip EUR / spaces / parse European format
                    s = str(val).replace("EUR", "").strip()
                    if s in ("-", ""):
                        cell.value = None
                    else:
                        s = (
                            s.replace(".", "").replace(",", ".")
                            if "," in s
                            else s.replace(",", "")
                        )
                        cell.value = float(s)
                        cell.number_format = EUR_FMT
                except (ValueError, TypeError):
                    cell.value = val
            else:
                cell.value = val
            cell.font = BODY_FONT

    # Freeze header and first 2 columns (Account, Opportunity)
    ws.freeze_panes = "C3"
    # Auto-width-ish
    for col_idx in range(1, len(labels) + 1):
        letter = ws.cell(row=2, column=col_idx).column_letter
        header_len = len(str(labels[col_idx - 1] or ""))
        ws.column_dimensions[letter].width = max(12, min(header_len + 2, 22))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--workbooks-dir",
        type=Path,
        default=Path("output/director_live_workbooks")
        / datetime.now().strftime("%Y-%m-%d"),
    )
    parser.add_argument(
        "--director",
        help="Only process one director slug (e.g. jesper-tyrer)",
    )
    args = parser.parse_args()

    if not args.workbooks_dir.exists():
        print(f"  workbooks dir missing: {args.workbooks_dir}")
        return 1

    token, instance = _auth()
    session = requests.Session()
    session.headers.update({"Authorization": f"Bearer {token}"})

    targets = [args.director] if args.director else list(REPORTS.keys())
    for slug in targets:
        if slug not in REPORTS:
            print(f"  skip {slug}: no report mapping")
            continue
        wb_path = args.workbooks_dir / f"{slug}.xlsx"
        if not wb_path.exists():
            print(f"  skip {slug}: workbook missing")
            continue

        q1_id, q2_id = REPORTS[slug]
        print(f"  {slug}...")
        wb = load_workbook(wb_path)

        for label, rid in [("Q1 Snapshot Trend", q1_id), ("Q2 Snapshot Trend", q2_id)]:
            try:
                labels, dtypes, rows, snapshots = _run_report(session, instance, rid)
                _write_sheet(wb, label, labels, dtypes, rows, snapshots)
                print(f"    {label}: {len(rows)} rows, snapshots {len(snapshots)}")
            except Exception as exc:
                print(f"    {label}: failed ({exc})")

        wb.save(wb_path)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
