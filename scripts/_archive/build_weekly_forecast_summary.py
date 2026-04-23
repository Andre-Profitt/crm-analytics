#!/usr/bin/env python3
"""Build Weekly Forecast Summary datasets from OpportunityFieldHistory.

Reconstructs each opportunity's ForecastCategory and CloseDate at the end of
every ISO week since 2025-01-01 by rolling back field history changes.

Produces two datasets:
  1. Weekly_Forecast_Summary  -- aggregate weekly forecast state
  2. Weekly_Forecast_Opps     -- 1 row per Opportunity x Week with change flags

Both are uploaded to CRM Analytics via the InsightsExternalData API,
with CSV fallback to /Users/test/crm-analytics/data/.
"""

import csv
import io
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta

# Allow importing helpers from the parent directory
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    set_record_links_xmd,
    upload_dataset,
)
from commercial_operating_model import ownership_alignment, primary_motion_persona, role_dimension_row

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

HISTORY_START = "2025-01-01"
HISTORY_START_ISO = "2025-01-01T00:00:00Z"

SUMMARY_DS = "Weekly_Forecast_Summary"
SUMMARY_DS_LABEL = "Weekly Forecast Summary"
OPPS_DS = "Weekly_Forecast_Opps"
OPPS_DS_LABEL = "Weekly Forecast Opps"

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def _date_only(value):
    return str(value or "")[:10]


def _quarter_label(date_str):
    dt = _parse_date(date_str)
    if not dt:
        return ""
    quarter = ((dt.month - 1) // 3) + 1
    return f"Q{quarter}"


def _fy_label(date_str):
    dt = _parse_date(date_str)
    return f"FY{dt.year}" if dt else ""


def _fc_rank(value):
    normalized = str(value or "").strip().lower().replace(" ", "")
    ranks = {
        "omitted": 0,
        "pipeline": 1,
        "bestcase": 2,
        "commit": 3,
        "closed": 4,
        "closedwon": 4,
    }
    return ranks.get(normalized, 0)


def _week_index_map(week_endings):
    return {week_end.isoformat(): index for index, week_end in enumerate(week_endings)}


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


# ═══════════════════════════════════════════════════════════════════════════
#  Step 1: Query OpportunityFieldHistory
# ═══════════════════════════════════════════════════════════════════════════


def query_field_history(inst, tok):
    """Fetch all ForecastCategoryName and CloseDate changes since 2025-01-01."""
    print("\n=== Step 1: Querying OpportunityFieldHistory ===")
    records = _soql(
        inst,
        tok,
        "SELECT OpportunityId, Field, OldValue, NewValue, CreatedDate "
        "FROM OpportunityFieldHistory "
        f"WHERE CreatedDate >= {HISTORY_START_ISO} "
        "AND Field IN ('ForecastCategoryName', 'CloseDate') "
        "ORDER BY OpportunityId, CreatedDate ASC",
    )
    print(f"  Retrieved {len(records):,} field history records")

    # Break down by field
    forecast_count = sum(1 for r in records if r.get("Field") == "ForecastCategoryName")
    close_count = sum(1 for r in records if r.get("Field") == "CloseDate")
    print(f"    ForecastCategoryName: {forecast_count:,}")
    print(f"    CloseDate:            {close_count:,}")

    return records


# ═══════════════════════════════════════════════════════════════════════════
#  Step 2: Query current Opportunity state
# ═══════════════════════════════════════════════════════════════════════════


def query_current_opportunities(inst, tok):
    """Fetch current state of all relevant forecast opportunities.

    We intentionally include opportunities without field-history changes.
    A consultant-grade WoW view must count unchanged pipeline too; otherwise
    weekly totals undercount the real open book.
    """
    print("\n=== Step 2: Querying current Opportunity state ===")
    records = _soql(
        inst,
        tok,
        "SELECT Id, Name, ForecastCategoryName, CloseDate, CreatedDate, "
        "APTS_Opportunity_ARR__c, OwnerId, Owner.Name, Owner.ManagerId, Owner.Manager.Name, "
        "Owner.Title, Owner.Department, Owner.Division, Owner.UserRole.Name, "
        "AccountId, Account.Name, Account_Unit_Group__c, Sales_Region__c, "
        "Type, StageName, IsClosed, IsWon "
        "FROM Opportunity "
        "WHERE FiscalYear IN (2025, 2026, 2027) "
        "OR (IsClosed = false AND CloseDate >= 2025-01-01)",
    )
    print(f"  Retrieved {len(records):,} opportunities")
    return records


# ═══════════════════════════════════════════════════════════════════════════
#  Step 3: Build weekly snapshots via rollback
# ═══════════════════════════════════════════════════════════════════════════


def _parse_date(val):
    """Parse a date string, returning None on failure."""
    if not val:
        return None
    try:
        return datetime.strptime(str(val)[:10], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        return None


def _iso_week_label(dt):
    """Return ISO week label like '2025-W03' for a date."""
    iso = dt.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _week_ending_sunday(dt):
    """Return the Sunday ending the ISO week containing dt."""
    # ISO weekday: Monday=1, Sunday=7
    iso_weekday = dt.isocalendar()[2]
    return dt + timedelta(days=7 - iso_weekday)


def generate_week_endings(start_date_str, end_date=None):
    """Generate all ISO week-ending Sundays from start_date to now."""
    start = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end = end_date or datetime.utcnow().date()
    week_end = _week_ending_sunday(start)
    weeks = []
    while week_end <= end:
        weeks.append(week_end)
        week_end += timedelta(days=7)
    return weeks


def build_history_index(history_records):
    """Index field history changes by OpportunityId.

    Returns a dict:
        opp_id -> {
            'ForecastCategoryName': [(created_date, old_value, new_value), ...],
            'CloseDate': [(created_date, old_value, new_value), ...],
        }

    Each list is sorted by CreatedDate ascending.
    """
    index = defaultdict(lambda: defaultdict(list))
    for r in history_records:
        opp_id = r.get("OpportunityId", "")
        field = r.get("Field", "")
        created = _parse_date(r.get("CreatedDate"))
        if not opp_id or not field or not created:
            continue
        old_val = r.get("OldValue")
        new_val = r.get("NewValue")
        index[opp_id][field].append((created, old_val, new_val))

    # Sort each list by date ascending (should already be, but be safe)
    for opp_id in index:
        for field in index[opp_id]:
            index[opp_id][field].sort(key=lambda x: x[0])

    return index


def reconstruct_at_week_end(current_value, changes, week_end):
    """Reconstruct a field's value at the end of a given week.

    Strategy: start from the current (latest) value and roll back any changes
    that happened AFTER the week_end. Rolling back means replacing the value
    with the OldValue from each change, processed in reverse chronological order.

    Args:
        current_value: The field's current value on the opportunity.
        changes: List of (created_date, old_value, new_value) sorted ascending.
        week_end: The date representing end-of-week (Sunday).

    Returns:
        The reconstructed value at end of that week.
    """
    value = current_value
    # Walk changes in reverse. For any change that happened after week_end,
    # roll it back by using old_value.
    for created_date, old_value, new_value in reversed(changes):
        if created_date > week_end:
            value = old_value
    return value


def compute_push_count(close_date_changes, week_end):
    """Count cumulative CloseDate pushes up to (and including) week_end.

    A push is defined as a CloseDate change where the new date is later than
    the old date.
    """
    count = 0
    for created_date, old_value, new_value in close_date_changes:
        if created_date > week_end:
            break
        old_dt = _parse_date(old_value)
        new_dt = _parse_date(new_value)
        if old_dt and new_dt and new_dt > old_dt:
            count += 1
    return count


def build_weekly_snapshots(opportunities, history_index, week_endings):
    """Build per-opportunity per-week snapshot rows.

    Returns:
        opp_week_rows: list of dicts (1 per opp x week)
        summary_rows:  list of dicts (1 per week x forecast category)
    """
    print("\n=== Step 3: Building weekly snapshots ===")

    week_index = _week_index_map(week_endings)

    # Build a lookup from opp id to current opportunity data
    opp_lookup = {}
    for opp in opportunities:
        opp_id = opp.get("Id", "")
        if not opp_id:
            continue
        owner = opp.get("Owner") or {}
        if isinstance(owner, dict):
            owner_name = owner.get("Name", "")
            owner_role = ((owner.get("UserRole") or {}).get("Name")) or ""
            role_row = role_dimension_row(
                owner_id=opp.get("OwnerId") or "",
                owner_name=owner_name,
                title=owner.get("Title") or "",
                user_role=owner_role,
                department=owner.get("Department") or "",
                division=owner.get("Division") or "",
                manager_id=owner.get("ManagerId") or "",
                manager_name=((owner.get("Manager") or {}).get("Name")) or "",
            )
            manager_name = ((owner.get("Manager") or {}).get("Name")) or ""
        else:
            owner_name = str(owner)
            owner_role = ""
            role_row = role_dimension_row(
                owner_id=opp.get("OwnerId") or "",
                owner_name=owner_name,
                manager_id="",
                manager_name="",
            )
            manager_name = ""
        close_date = _date_only(opp.get("CloseDate"))
        created_date = _date_only(opp.get("CreatedDate"))
        motion_type = opp.get("Type") or ""
        opp_lookup[opp_id] = {
            "Name": opp.get("Name", ""),
            "AccountId": opp.get("AccountId") or "",
            "AccountName": ((opp.get("Account") or {}).get("Name")) or "",
            "ForecastCategoryName": opp.get("ForecastCategoryName", ""),
            "CloseDate": close_date,
            "CreatedDate": created_date,
            "ARR": _safe_float(opp.get("APTS_Opportunity_ARR__c")),
            "OwnerId": opp.get("OwnerId") or "",
            "OwnerName": owner_name,
            "ManagerId": role_row["ManagerId"],
            "ManagerName": role_row["ManagerName"] or manager_name,
            "OwnerRole": owner_role,
            "OwnerTitle": role_row["Title"],
            "OwnerDepartment": role_row["Department"],
            "OwnerDivision": role_row["Division"],
            "OwnerPersona": role_row["Persona"],
            "OwnerRegion": role_row["Region"],
            "UnitGroup": opp.get("Account_Unit_Group__c") or "",
            "SalesRegion": opp.get("Sales_Region__c") or "",
            "MotionType": motion_type,
            "ExpectedMotionPersona": primary_motion_persona(motion_type),
            "OwnershipAlignment": ownership_alignment(role_row["Persona"], motion_type),
            "StageName": opp.get("StageName", ""),
            "IsClosed": opp.get("IsClosed", False),
            "IsWon": opp.get("IsWon", False),
        }

    # Determine which opportunities to process: all relevant current opportunities.
    all_opp_ids = set(opp_lookup.keys())
    print(f"  Opportunities in modeled weekly universe: {len(all_opp_ids):,}")
    print(f"  Weeks to process: {len(week_endings)}")
    total_rows = len(all_opp_ids) * len(week_endings)
    print(f"  Expected detail rows: {total_rows:,}")

    opp_week_rows = []
    # Aggregate: (week, region, manager, forecast_category) -> totals
    agg = defaultdict(lambda: {"TotalARR": 0.0, "OppCount": 0})

    processed = 0
    for opp_id in sorted(all_opp_ids):
        opp = opp_lookup[opp_id]
        fc_changes = history_index.get(opp_id, {}).get("ForecastCategoryName", [])
        cd_changes = history_index.get(opp_id, {}).get("CloseDate", [])

        current_fc = opp["ForecastCategoryName"]
        current_cd = opp["CloseDate"]
        created_date = _parse_date(opp["CreatedDate"])

        for week_end in week_endings:
            if created_date and created_date > week_end:
                continue

            week_label = _iso_week_label(week_end)

            # Reconstruct forecast category at end of this week
            fc_at_week = reconstruct_at_week_end(current_fc, fc_changes, week_end)
            if fc_at_week is None:
                fc_at_week = ""

            # Reconstruct close date at end of this week
            cd_at_week = reconstruct_at_week_end(current_cd, cd_changes, week_end)
            cd_at_week_str = str(cd_at_week or "")[:10]

            # Compute cumulative push count
            push_count = compute_push_count(cd_changes, week_end)

            arr = opp["ARR"]
            try:
                arr = float(arr) if arr else 0.0
            except (ValueError, TypeError):
                arr = 0.0

            row = {
                "Week": week_label,
                "WeekEndDate": week_end.isoformat(),
                "WeekIndex": week_index[week_end.isoformat()],
                "OpportunityId": opp_id,
                "OpportunityName": opp["Name"],
                "AccountId": opp["AccountId"],
                "AccountName": opp["AccountName"],
                "CreatedDate": opp["CreatedDate"],
                "OwnerId": opp["OwnerId"],
                "OwnerName": opp["OwnerName"],
                "ManagerId": opp["ManagerId"],
                "ManagerName": opp["ManagerName"],
                "OwnerRole": opp["OwnerRole"],
                "OwnerTitle": opp["OwnerTitle"],
                "OwnerDepartment": opp["OwnerDepartment"],
                "OwnerDivision": opp["OwnerDivision"],
                "OwnerPersona": opp["OwnerPersona"],
                "OwnerRegion": opp["OwnerRegion"],
                "SalesRegion": opp["SalesRegion"],
                "UnitGroup": opp["UnitGroup"],
                "MotionType": opp["MotionType"],
                "ExpectedMotionPersona": opp["ExpectedMotionPersona"],
                "OwnershipAlignment": opp["OwnershipAlignment"],
                "StageName": opp["StageName"],
                "ForecastCategory": str(fc_at_week),
                "PrevForecastCategory": "",
                "MovementPair": "",
                "WeekChangeStory": "",
                "CloseDate": cd_at_week_str,
                "PrevCloseDate": "",
                "PrevCloseQuarter": "",
                "CloseQuarter": _quarter_label(cd_at_week_str),
                "PrevFYLabel": "",
                "FYLabel": _fy_label(cd_at_week_str),
                "ARR": round(arr, 2),
                "PushCount": push_count,
                "CloseDateDeltaDays": 0,
                "QuarterChangedFlag": 0,
                "PushedOutOfQuarterFlag": 0,
                "PulledIntoQuarterFlag": 0,
                "PushThisWeekFlag": 0,
                "PullInThisWeekFlag": 0,
                "CategoryChangedFlag": 0,
                "PromotionThisWeekFlag": 0,
                "DemotionThisWeekFlag": 0,
                "NewThisWeekFlag": 0,
                "BigBetFlag": int(arr >= 1000000),
                "CurrentWeekFlag": 0,
                "PreviousWeekFlag": 0,
                "ChangedThisWeekFlag": 0,
            }
            opp_week_rows.append(row)

            # Aggregate
            key = (
                week_label,
                opp["SalesRegion"],
                opp["UnitGroup"],
                opp["ManagerName"],
                opp["OwnerName"],
                opp["OwnerPersona"],
                opp["OwnershipAlignment"],
                _quarter_label(cd_at_week_str),
                _fy_label(cd_at_week_str),
                str(fc_at_week),
            )
            agg[key]["TotalARR"] += arr
            agg[key]["OppCount"] += 1

        processed += 1
        if processed % 500 == 0:
            print(f"  Processed {processed:,} / {len(all_opp_ids):,} opportunities...")

    print(
        f"  Processed {processed:,} opportunities, {len(opp_week_rows):,} detail rows"
    )

    # Add previous-week state and change flags.
    current_week_end = week_endings[-1].isoformat() if week_endings else ""
    previous_week_end = week_endings[-2].isoformat() if len(week_endings) >= 2 else ""
    by_opp = defaultdict(list)
    for row in opp_week_rows:
        by_opp[row["OpportunityId"]].append(row)
    for rows in by_opp.values():
        rows.sort(key=lambda row: row["WeekIndex"])
        prior = None
        for row in rows:
            if prior:
                row["PrevForecastCategory"] = prior["ForecastCategory"]
                row["PrevCloseDate"] = prior["CloseDate"]
                row["PrevCloseQuarter"] = prior["CloseQuarter"]
                row["PrevFYLabel"] = prior["FYLabel"]
                row["MovementPair"] = (
                    f"{prior['ForecastCategory']} -> {row['ForecastCategory']}"
                    if prior["ForecastCategory"] != row["ForecastCategory"]
                    else (
                        f"{prior['CloseQuarter']} -> {row['CloseQuarter']}"
                        if prior["CloseQuarter"] != row["CloseQuarter"]
                        else ("Date Shift" if prior["CloseDate"] != row["CloseDate"] else "No Change")
                    )
                )
                category_changed = int(prior["ForecastCategory"] != row["ForecastCategory"])
                push_this_week = int(
                    bool(prior["CloseDate"])
                    and bool(row["CloseDate"])
                    and (_parse_date(row["CloseDate"]) or datetime.min.date())
                    > (_parse_date(prior["CloseDate"]) or datetime.min.date())
                )
                pull_in_this_week = int(
                    bool(prior["CloseDate"])
                    and bool(row["CloseDate"])
                    and (_parse_date(row["CloseDate"]) or datetime.min.date())
                    < (_parse_date(prior["CloseDate"]) or datetime.min.date())
                )
                promoted = int(
                    category_changed
                    and _fc_rank(row["ForecastCategory"]) > _fc_rank(prior["ForecastCategory"])
                )
                demoted = int(
                    category_changed
                    and _fc_rank(row["ForecastCategory"]) < _fc_rank(prior["ForecastCategory"])
                )
                row["CategoryChangedFlag"] = category_changed
                row["PushThisWeekFlag"] = push_this_week
                row["PullInThisWeekFlag"] = pull_in_this_week
                row["PromotionThisWeekFlag"] = promoted
                row["DemotionThisWeekFlag"] = demoted
                close_date_delta = 0
                if prior["CloseDate"] and row["CloseDate"]:
                    close_date_delta = (
                        (_parse_date(row["CloseDate"]) or datetime.min.date())
                        - (_parse_date(prior["CloseDate"]) or datetime.min.date())
                    ).days
                row["CloseDateDeltaDays"] = close_date_delta
                quarter_changed = int(prior["CloseQuarter"] != row["CloseQuarter"])
                row["QuarterChangedFlag"] = quarter_changed
                row["PushedOutOfQuarterFlag"] = int(push_this_week and quarter_changed)
                row["PulledIntoQuarterFlag"] = int(pull_in_this_week and quarter_changed)
                if promoted:
                    row["WeekChangeStory"] = f"Promoted to {row['ForecastCategory']}"
                elif demoted:
                    row["WeekChangeStory"] = f"Moved down to {row['ForecastCategory']}"
                elif row["PushedOutOfQuarterFlag"]:
                    row["WeekChangeStory"] = f"Pushed to {row['CloseQuarter']}"
                elif row["PulledIntoQuarterFlag"]:
                    row["WeekChangeStory"] = f"Pulled into {row['CloseQuarter']}"
                elif push_this_week:
                    row["WeekChangeStory"] = "Close date pushed"
                elif pull_in_this_week:
                    row["WeekChangeStory"] = "Close date pulled in"
                else:
                    row["WeekChangeStory"] = "No material change"
                row["ChangedThisWeekFlag"] = int(
                    category_changed or push_this_week or pull_in_this_week
                )
            else:
                if row["CreatedDate"] and _week_ending_sunday(_parse_date(row["CreatedDate"])) == _parse_date(row["WeekEndDate"]):
                    row["MovementPair"] = "New Opportunity"
                    row["WeekChangeStory"] = "Created this week"
                    row["NewThisWeekFlag"] = 1
                    row["ChangedThisWeekFlag"] = 1
                else:
                    row["MovementPair"] = "Pre-existing"
                    row["WeekChangeStory"] = "Pre-existing opportunity"
            row["CurrentWeekFlag"] = int(row["WeekEndDate"] == current_week_end)
            row["PreviousWeekFlag"] = int(row["WeekEndDate"] == previous_week_end)
            prior = row

    # Build summary rows.
    summary_rows = []
    for (
        week_label,
        sales_region,
        unit_group,
        manager_name,
        owner_name,
        owner_persona,
        ownership_status,
        close_quarter,
        fy_label,
        fc,
    ), vals in sorted(agg.items()):
        total_arr = vals["TotalARR"]
        opp_count = vals["OppCount"]
        avg_arr = round(total_arr / opp_count, 2) if opp_count > 0 else 0.0
        summary_rows.append(
            {
                "Week": week_label,
                "WeekEndDate": "",
                "WeekIndex": 0,
                "SalesRegion": sales_region,
                "UnitGroup": unit_group,
                "ManagerName": manager_name,
                "OwnerName": owner_name,
                "OwnerPersona": owner_persona,
                "OwnershipAlignment": ownership_status,
                "CloseQuarter": close_quarter,
                "FYLabel": fy_label,
                "ForecastCategory": fc,
                "TotalARR": round(total_arr, 2),
                "OppCount": opp_count,
                "AvgARR": avg_arr,
                "PrevTotalARR": 0.0,
                "ARRDeltaWoW": 0.0,
                "PrevOppCount": 0,
                "OppCountDeltaWoW": 0,
                "CurrentWeekFlag": 0,
                "PreviousWeekFlag": 0,
            }
        )

    # Add previous-week deltas to summary rows.
    summary_lookup = defaultdict(list)
    week_end_by_label = {_iso_week_label(week_end): week_end.isoformat() for week_end in week_endings}
    for row in summary_rows:
        row["WeekEndDate"] = week_end_by_label.get(row["Week"], "")
        row["WeekIndex"] = week_index.get(row["WeekEndDate"], 0)
        row["CurrentWeekFlag"] = int(row["WeekEndDate"] == current_week_end)
        row["PreviousWeekFlag"] = int(row["WeekEndDate"] == previous_week_end)
        summary_key = (
            row["SalesRegion"],
            row["UnitGroup"],
            row["ManagerName"],
            row["OwnerName"],
            row["OwnerPersona"],
            row["OwnershipAlignment"],
            row["CloseQuarter"],
            row["FYLabel"],
            row["ForecastCategory"],
        )
        summary_lookup[summary_key].append(row)

    for rows in summary_lookup.values():
        rows.sort(key=lambda row: row["WeekIndex"])
        prior = None
        for row in rows:
            if prior:
                row["PrevTotalARR"] = prior["TotalARR"]
                row["ARRDeltaWoW"] = round(row["TotalARR"] - prior["TotalARR"], 2)
                row["PrevOppCount"] = prior["OppCount"]
                row["OppCountDeltaWoW"] = row["OppCount"] - prior["OppCount"]
            prior = row

    print(f"  Summary rows: {len(summary_rows):,}")

    return opp_week_rows, summary_rows


# ═══════════════════════════════════════════════════════════════════════════
#  Step 4: Write CSVs
# ═══════════════════════════════════════════════════════════════════════════

SUMMARY_FIELDS = [
    "Week",
    "WeekEndDate",
    "WeekIndex",
    "SalesRegion",
    "UnitGroup",
    "ManagerName",
    "OwnerName",
    "OwnerPersona",
    "OwnershipAlignment",
    "CloseQuarter",
    "FYLabel",
    "ForecastCategory",
    "TotalARR",
    "OppCount",
    "AvgARR",
    "PrevTotalARR",
    "ARRDeltaWoW",
    "PrevOppCount",
    "OppCountDeltaWoW",
    "CurrentWeekFlag",
    "PreviousWeekFlag",
]

DETAIL_FIELDS = [
    "Week",
    "WeekEndDate",
    "WeekIndex",
    "OpportunityId",
    "OpportunityName",
    "AccountId",
    "AccountName",
    "CreatedDate",
    "OwnerId",
    "OwnerName",
    "ManagerId",
    "ManagerName",
    "OwnerRole",
    "OwnerTitle",
    "OwnerDepartment",
    "OwnerDivision",
    "OwnerPersona",
    "OwnerRegion",
    "SalesRegion",
    "UnitGroup",
    "MotionType",
    "ExpectedMotionPersona",
    "OwnershipAlignment",
    "StageName",
    "ForecastCategory",
    "PrevForecastCategory",
    "MovementPair",
    "WeekChangeStory",
    "CloseDate",
    "PrevCloseDate",
    "PrevCloseQuarter",
    "CloseQuarter",
    "PrevFYLabel",
    "FYLabel",
    "ARR",
    "PushCount",
    "CloseDateDeltaDays",
    "QuarterChangedFlag",
    "PushedOutOfQuarterFlag",
    "PulledIntoQuarterFlag",
    "PushThisWeekFlag",
    "PullInThisWeekFlag",
    "CategoryChangedFlag",
    "PromotionThisWeekFlag",
    "DemotionThisWeekFlag",
    "NewThisWeekFlag",
    "BigBetFlag",
    "CurrentWeekFlag",
    "PreviousWeekFlag",
    "ChangedThisWeekFlag",
]


def write_csv(rows, fieldnames, filename):
    """Write rows to an in-memory CSV and return bytes. Also save to disk."""
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow(row)
    csv_text = buf.getvalue()
    csv_bytes = csv_text.encode("utf-8")

    # Save local copy
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        f.write(csv_text)
    print(f"  Saved {filepath} ({len(csv_bytes):,} bytes, {len(rows):,} rows)")

    return csv_bytes


# ═══════════════════════════════════════════════════════════════════════════
#  Step 5: Upload to CRM Analytics
# ═══════════════════════════════════════════════════════════════════════════


SUMMARY_META = [
    _dim("Week", "Week"),
    _date("WeekEndDate", "Week End Date"),
    _measure("WeekIndex", "Week Index", scale=0, precision=6),
    _dim("SalesRegion", "Sales Region"),
    _dim("UnitGroup", "Unit Group"),
    _dim("ManagerName", "Manager"),
    _dim("OwnerName", "Owner"),
    _dim("OwnerPersona", "Owner Persona"),
    _dim("OwnershipAlignment", "Ownership Alignment"),
    _dim("CloseQuarter", "Close Quarter"),
    _dim("FYLabel", "Fiscal Year"),
    _dim("ForecastCategory", "Forecast Category"),
    _measure("TotalARR", "Total ARR"),
    _measure("OppCount", "Opp Count", scale=0, precision=8),
    _measure("AvgARR", "Avg ARR"),
    _measure("PrevTotalARR", "Previous Total ARR"),
    _measure("ARRDeltaWoW", "ARR Delta WoW"),
    _measure("PrevOppCount", "Previous Opp Count", scale=0, precision=8),
    _measure("OppCountDeltaWoW", "Opp Count Delta WoW", scale=0, precision=8),
    _measure("CurrentWeekFlag", "Current Week", scale=0, precision=3),
    _measure("PreviousWeekFlag", "Previous Week", scale=0, precision=3),
]

DETAIL_META = [
    _dim("Week", "Week"),
    _date("WeekEndDate", "Week End Date"),
    _measure("WeekIndex", "Week Index", scale=0, precision=6),
    _dim("OpportunityId", "Opportunity ID"),
    _dim("OpportunityName", "Opportunity Name"),
    _dim("AccountId", "Account ID"),
    _dim("AccountName", "Account Name"),
    _date("CreatedDate", "Created Date"),
    _dim("OwnerId", "Owner ID"),
    _dim("OwnerName", "Owner Name"),
    _dim("ManagerId", "Manager ID"),
    _dim("ManagerName", "Manager"),
    _dim("OwnerRole", "Owner Role"),
    _dim("OwnerTitle", "Owner Title"),
    _dim("OwnerDepartment", "Owner Department"),
    _dim("OwnerDivision", "Owner Division"),
    _dim("OwnerPersona", "Owner Persona"),
    _dim("OwnerRegion", "Owner Region"),
    _dim("SalesRegion", "Sales Region"),
    _dim("UnitGroup", "Unit Group"),
    _dim("MotionType", "Motion Type"),
    _dim("ExpectedMotionPersona", "Expected Motion Persona"),
    _dim("OwnershipAlignment", "Ownership Alignment"),
    _dim("StageName", "Stage Name"),
    _dim("ForecastCategory", "Forecast Category"),
    _dim("PrevForecastCategory", "Previous Forecast Category"),
    _dim("MovementPair", "Movement Pair"),
    _dim("WeekChangeStory", "Week Change Story"),
    _date("CloseDate", "Close Date"),
    _date("PrevCloseDate", "Previous Close Date"),
    _dim("PrevCloseQuarter", "Previous Close Quarter"),
    _dim("CloseQuarter", "Close Quarter"),
    _dim("PrevFYLabel", "Previous Fiscal Year"),
    _dim("FYLabel", "Fiscal Year"),
    _measure("ARR", "ARR"),
    _measure("PushCount", "Push Count", scale=0, precision=6),
    _measure("CloseDateDeltaDays", "Close Date Delta Days", scale=0, precision=6),
    _measure("QuarterChangedFlag", "Quarter Changed", scale=0, precision=3),
    _measure("PushedOutOfQuarterFlag", "Pushed Out Of Quarter", scale=0, precision=3),
    _measure("PulledIntoQuarterFlag", "Pulled Into Quarter", scale=0, precision=3),
    _measure("PushThisWeekFlag", "Push This Week", scale=0, precision=3),
    _measure("PullInThisWeekFlag", "Pull In This Week", scale=0, precision=3),
    _measure("CategoryChangedFlag", "Category Changed", scale=0, precision=3),
    _measure("PromotionThisWeekFlag", "Promoted This Week", scale=0, precision=3),
    _measure("DemotionThisWeekFlag", "Demoted This Week", scale=0, precision=3),
    _measure("NewThisWeekFlag", "New This Week", scale=0, precision=3),
    _measure("BigBetFlag", "Big Bet", scale=0, precision=3),
    _measure("CurrentWeekFlag", "Current Week", scale=0, precision=3),
    _measure("PreviousWeekFlag", "Previous Week", scale=0, precision=3),
    _measure("ChangedThisWeekFlag", "Changed This Week", scale=0, precision=3),
]


def upload_datasets(inst, tok, summary_bytes, detail_bytes):
    """Upload both datasets to CRM Analytics."""
    print("\n=== Step 5: Uploading to CRM Analytics ===")

    print(f"\n  Uploading {SUMMARY_DS} ({len(summary_bytes):,} bytes)...")
    ok1 = upload_dataset(
        inst, tok, SUMMARY_DS, SUMMARY_DS_LABEL, SUMMARY_META, summary_bytes
    )

    print(f"\n  Uploading {OPPS_DS} ({len(detail_bytes):,} bytes)...")
    ok2 = upload_dataset(inst, tok, OPPS_DS, OPPS_DS_LABEL, DETAIL_META, detail_bytes)

    if ok2:
        set_record_links_xmd(
            inst,
            tok,
            OPPS_DS,
            [
                {"field": "OpportunityName", "id_field": "OpportunityId", "label": "Opportunity"},
                {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            ],
        )

    return ok1, ok2


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    print("=" * 70)
    print("  Weekly Forecast Summary Builder")
    print(f"  History start: {HISTORY_START}")
    print(f"  Run date:      {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 70)

    # Authenticate
    print("\nAuthenticating via sf CLI...")
    try:
        inst, tok = get_auth()
    except Exception as e:
        print(f"\nERROR: Could not authenticate. {e}")
        print("Ensure 'sf org display' works for your default org.")
        sys.exit(1)
    print(f"  Instance: {inst}")

    # Step 1: Query field history
    history_records = query_field_history(inst, tok)
    if not history_records:
        print(
            "\nWARNING: No field history records found. Check your SOQL filter dates."
        )

    # Step 2: Query current opportunity state
    opportunities = query_current_opportunities(inst, tok)
    if not opportunities:
        print("\nERROR: No opportunities returned. Exiting.")
        sys.exit(1)

    # Step 3: Build weekly snapshots
    history_index = build_history_index(history_records)
    week_endings = generate_week_endings(HISTORY_START)
    opp_week_rows, summary_rows = build_weekly_snapshots(
        opportunities, history_index, week_endings
    )

    if not opp_week_rows:
        print("\nERROR: No snapshot rows generated. Exiting.")
        sys.exit(1)

    # Step 4: Write CSVs
    print("\n=== Step 4: Writing CSVs ===")
    summary_bytes = write_csv(
        summary_rows, SUMMARY_FIELDS, "weekly_forecast_category.csv"
    )
    detail_bytes = write_csv(opp_week_rows, DETAIL_FIELDS, "weekly_forecast_opps.csv")

    # Step 5: Upload to CRM Analytics
    try:
        ok_summary, ok_detail = upload_datasets(inst, tok, summary_bytes, detail_bytes)
    except Exception as e:
        print(f"\nERROR during upload: {e}")
        print("CSVs have been saved locally for manual upload:")
        print(f"  {os.path.join(DATA_DIR, 'weekly_forecast_category.csv')}")
        print(f"  {os.path.join(DATA_DIR, 'weekly_forecast_opps.csv')}")
        sys.exit(1)

    # Summary
    print("\n" + "=" * 70)
    print("  Results")
    print("=" * 70)
    print(f"  Summary dataset ({SUMMARY_DS}): {'OK' if ok_summary else 'FAILED'}")
    print(f"  Detail dataset  ({OPPS_DS}):    {'OK' if ok_detail else 'FAILED'}")
    print(f"  Local CSVs saved to: {os.path.abspath(DATA_DIR)}")
    print(f"  Summary rows: {len(summary_rows):,}")
    print(f"  Detail rows:  {len(opp_week_rows):,}")
    weeks_covered = len(week_endings)
    opps_covered = len(set(r["OpportunityId"] for r in opp_week_rows))
    print(f"  Weeks covered: {weeks_covered}")
    print(f"  Opportunities: {opps_covered:,}")
    print()

    if not (ok_summary and ok_detail):
        sys.exit(1)


if __name__ == "__main__":
    main()
