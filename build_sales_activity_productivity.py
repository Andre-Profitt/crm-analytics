#!/usr/bin/env python3
"""
Sales Activity & Productivity Dashboard
========================================
Closes the single biggest data gap in the analytics suite: zero activity data.
Combines Task + Event objects into a unified Activity dataset and builds a
4-page progressive-disclosure dashboard.

Pages:
  1. Team Overview — KPIs, activity trend, type breakdown, top reps
  2. Rep Productivity — per-rep metrics, cadence, activity-to-outcome correlation
  3. Account & Deal Coverage — account activity gaps, deal engagement depth
  4. Exceptions & Actions — stale accounts, low-activity reps, no-next-step deals
"""

import csv
import io
import sys
from collections import defaultdict
from datetime import datetime

from crm_analytics_helpers import (
    KPI_CARD_STYLE,
    _date,
    _dim,
    _measure,
    _soql,
    af,
    build_dashboard_state,
    compare_table,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    hdr,
    heatmap_chart,
    line_chart,
    listselector,
    nav_link,
    nav_row,
    num,
    pg,
    rich_chart,
    scatter_chart,
    section_label,
    set_record_links_xmd,
    sq,
    upload_dataset,
)
from portfolio_foundation import month_key, safe_float

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DS = "Sales_Activity_Productivity"
DS_LABEL = "Sales Activity & Productivity"
DASHBOARD_LABEL = "Sales Activity & Productivity"

# Consulting-grade faceting: KPIs respond to filter pillboxes
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_owner", "f_type", "f_month", "f_account"],
    },
}

TASK_SOQL = (
    "SELECT Id, OwnerId, Owner.Name, WhoId, WhatId, What.Type, "
    "AccountId, Account.Name, Subject, Type, TaskSubtype, Status, "
    "ActivityDate, CreatedDate, Priority, CallDurationInSeconds "
    "FROM Task "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z"
)

EVENT_SOQL = (
    "SELECT Id, OwnerId, Owner.Name, WhoId, WhatId, What.Type, "
    "AccountId, Account.Name, Subject, Type, "
    "ActivityDate, StartDateTime, EndDateTime, DurationInMinutes, CreatedDate "
    "FROM Event "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z"
)

# For linking activities to opportunities and their pipeline amounts
OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, OwnerId, Owner.Name, "
    "Type, StageName, Amount, ForecastCategoryName, IsClosed, IsWon, "
    "CloseDate, CreatedDate, NextStep "
    "FROM Opportunity "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z OR IsClosed = false"
)

ACTIVITY_TYPES = {
    "Call": "Call",
    "Meeting": "Meeting",
    "Task": "Task",
    "Outbound Email": "Email",
    "Inbound Email": "Email",
    "Email": "Email",
    "None": "Other",
}


# ---------------------------------------------------------------------------
# Dataset builder
# ---------------------------------------------------------------------------
def create_dataset(inst, tok):
    """Build unified activity dataset from Task + Event + Opportunity."""
    print("  Querying Task records...")
    tasks = _soql(inst, tok, TASK_SOQL)
    print(f"  → {len(tasks):,} tasks")

    print("  Querying Event records...")
    events = _soql(inst, tok, EVENT_SOQL)
    print(f"  → {len(events):,} events")

    print("  Querying Opportunity records (for coverage analysis)...")
    opps = _soql(inst, tok, OPP_SOQL)
    print(f"  → {len(opps):,} opportunities")

    # Build opportunity lookup by ID for deal-level analysis
    opp_by_id = {}
    opp_by_account = defaultdict(list)
    for opp in opps:
        opp_id = opp.get("Id", "")
        acct_id = opp.get("AccountId", "")
        opp_by_id[opp_id] = opp
        if acct_id:
            opp_by_account[acct_id].append(opp)

    # Track per-rep, per-account, per-month activity counts
    rep_activities = defaultdict(lambda: defaultdict(int))
    account_activities = defaultdict(lambda: defaultdict(int))
    rep_names = {}
    account_names = {}

    rows = []
    today = datetime.now().strftime("%Y-%m-%d")
    today_dt = datetime.now()

    # ----- Process Tasks -----
    for t in tasks:
        owner_name = (t.get("Owner") or {}).get("Name", "Unknown")
        owner_id = t.get("OwnerId", "")
        acct_name = (t.get("Account") or {}).get("Name", "")
        acct_id = t.get("AccountId", "")
        what_type = (t.get("What") or {}).get("Type", "")
        what_id = t.get("WhatId", "")
        raw_type = t.get("Type") or t.get("TaskSubtype") or "Other"
        act_type = ACTIVITY_TYPES.get(raw_type, "Other")
        # Fallback: use TaskSubtype for better classification
        if act_type == "Other" and t.get("TaskSubtype"):
            subtype = t["TaskSubtype"]
            if subtype == "Email":
                act_type = "Email"
            elif subtype == "Call":
                act_type = "Call"
            elif subtype == "Task":
                act_type = "Task"
        activity_date = t.get("ActivityDate") or t.get("CreatedDate", "")[:10]
        created = t.get("CreatedDate", "")[:10]
        status = t.get("Status", "")
        is_completed = 1 if status == "Completed" else 0
        duration = safe_float(t.get("CallDurationInSeconds"), 0) / 60.0  # to minutes
        has_opp = 1 if what_type == "Opportunity" else 0
        has_account = 1 if acct_id else 0
        mk = month_key(activity_date) if activity_date else month_key(created)

        # Track for summaries
        rep_names[owner_id] = owner_name
        if acct_id:
            account_names[acct_id] = acct_name
        rep_activities[owner_id][mk] += 1
        if acct_id:
            account_activities[acct_id][mk] += 1

        # Compute days since activity
        try:
            act_dt = datetime.strptime(activity_date, "%Y-%m-%d")
            days_ago = (today_dt - act_dt).days
        except (ValueError, TypeError):
            days_ago = 999

        rows.append(
            {
                "RecordType": "activity",
                "ActivityId": t.get("Id", ""),
                "Source": "Task",
                "OwnerId": owner_id,
                "OwnerName": owner_name,
                "AccountId": acct_id,
                "AccountName": acct_name,
                "OpportunityId": what_id if has_opp else "",
                "ActivityType": act_type,
                "RawType": raw_type,
                "Subject": (t.get("Subject") or "")[:100],
                "Status": status,
                "IsCompleted": is_completed,
                "ActivityDate": activity_date,
                "CreatedDate": created,
                "MonthLabel": mk,
                "WeekLabel": _week_label(activity_date),
                "DayOfWeek": _day_of_week(activity_date),
                "DurationMinutes": round(duration, 1),
                "HasOpportunity": has_opp,
                "HasAccount": has_account,
                "DaysAgo": days_ago,
                "ActivityCount": 1,
                "OppAmount": 0,
                "OppStage": "",
                "OppType": "",
            }
        )

    # ----- Process Events -----
    for e in events:
        owner_name = (e.get("Owner") or {}).get("Name", "Unknown")
        owner_id = e.get("OwnerId", "")
        acct_name = (e.get("Account") or {}).get("Name", "")
        acct_id = e.get("AccountId", "")
        what_type = (e.get("What") or {}).get("Type", "")
        what_id = e.get("WhatId", "")
        raw_type = e.get("Type", "Meeting")
        act_type = ACTIVITY_TYPES.get(raw_type, "Meeting")
        activity_date = e.get("ActivityDate") or e.get("CreatedDate", "")[:10]
        created = e.get("CreatedDate", "")[:10]
        duration = safe_float(e.get("DurationInMinutes"), 0)
        has_opp = 1 if what_type == "Opportunity" else 0
        has_account = 1 if acct_id else 0
        mk = month_key(activity_date) if activity_date else month_key(created)

        rep_names[owner_id] = owner_name
        if acct_id:
            account_names[acct_id] = acct_name
        rep_activities[owner_id][mk] += 1
        if acct_id:
            account_activities[acct_id][mk] += 1

        try:
            act_dt = datetime.strptime(activity_date, "%Y-%m-%d")
            days_ago = (today_dt - act_dt).days
        except (ValueError, TypeError):
            days_ago = 999

        rows.append(
            {
                "RecordType": "activity",
                "ActivityId": e.get("Id", ""),
                "Source": "Event",
                "OwnerId": owner_id,
                "OwnerName": owner_name,
                "AccountId": acct_id,
                "AccountName": acct_name,
                "OpportunityId": what_id if has_opp else "",
                "ActivityType": act_type,
                "RawType": raw_type,
                "Subject": (e.get("Subject") or "")[:100],
                "Status": "Completed",
                "IsCompleted": 1,
                "ActivityDate": activity_date,
                "CreatedDate": created,
                "MonthLabel": mk,
                "WeekLabel": _week_label(activity_date),
                "DayOfWeek": _day_of_week(activity_date),
                "DurationMinutes": round(duration, 1),
                "HasOpportunity": has_opp,
                "HasAccount": has_account,
                "DaysAgo": days_ago,
                "ActivityCount": 1,
                "OppAmount": 0,
                "OppStage": "",
                "OppType": "",
            }
        )

    # ----- Build rep-month summary rows -----
    for rep_id, months in rep_activities.items():
        for mk, cnt in months.items():
            rows.append(
                {
                    "RecordType": "rep_month",
                    "ActivityId": "",
                    "Source": "",
                    "OwnerId": rep_id,
                    "OwnerName": rep_names.get(rep_id, "Unknown"),
                    "AccountId": "",
                    "AccountName": "",
                    "OpportunityId": "",
                    "ActivityType": "All",
                    "RawType": "",
                    "Subject": "",
                    "Status": "",
                    "IsCompleted": 0,
                    "ActivityDate": f"{mk}-01",
                    "CreatedDate": f"{mk}-01",
                    "MonthLabel": mk,
                    "WeekLabel": "",
                    "DayOfWeek": "",
                    "DurationMinutes": 0,
                    "HasOpportunity": 0,
                    "HasAccount": 0,
                    "DaysAgo": 0,
                    "ActivityCount": cnt,
                    "OppAmount": 0,
                    "OppStage": "",
                    "OppType": "",
                }
            )

    # ----- Build account coverage rows -----
    for acct_id, months in account_activities.items():
        total = sum(months.values())
        last_activity = max(months.keys()) if months else "2020-01"
        try:
            last_dt = datetime.strptime(f"{last_activity}-01", "%Y-%m-%d")
            days_since = (today_dt - last_dt).days
        except ValueError:
            days_since = 999

        # Check if account has open opportunities
        open_opps = [
            o for o in opp_by_account.get(acct_id, []) if not o.get("IsClosed", True)
        ]
        open_pipeline = sum(safe_float(o.get("Amount"), 0) for o in open_opps)
        has_next_step = any(o.get("NextStep") for o in open_opps)

        rows.append(
            {
                "RecordType": "account_coverage",
                "ActivityId": "",
                "Source": "",
                "OwnerId": "",
                "OwnerName": "",
                "AccountId": acct_id,
                "AccountName": account_names.get(acct_id, "Unknown"),
                "OpportunityId": "",
                "ActivityType": "All",
                "RawType": "",
                "Subject": "",
                "Status": "",
                "IsCompleted": 0,
                "ActivityDate": f"{last_activity}-01",
                "CreatedDate": "",
                "MonthLabel": last_activity,
                "WeekLabel": "",
                "DayOfWeek": "",
                "DurationMinutes": 0,
                "HasOpportunity": len(open_opps),
                "HasAccount": 1,
                "DaysAgo": days_since,
                "ActivityCount": total,
                "OppAmount": round(open_pipeline, 2),
                "OppStage": f"{len(open_opps)} open",
                "OppType": "Has Next Step" if has_next_step else "No Next Step",
            }
        )

    # ----- Build opp coverage rows -----
    for opp in opps:
        if opp.get("IsClosed"):
            continue
        opp_id = opp.get("Id", "")
        # Count activities linked to this opportunity
        opp_activities = sum(
            1
            for r in rows
            if r["RecordType"] == "activity" and r["OpportunityId"] == opp_id
        )
        acct_id = opp.get("AccountId", "")
        rows.append(
            {
                "RecordType": "opp_coverage",
                "ActivityId": "",
                "Source": "",
                "OwnerId": opp.get("OwnerId", ""),
                "OwnerName": (opp.get("Owner") or {}).get("Name", "Unknown"),
                "AccountId": acct_id,
                "AccountName": (opp.get("Account") or {}).get("Name", ""),
                "OpportunityId": opp_id,
                "ActivityType": "All",
                "RawType": "",
                "Subject": opp.get("Name", ""),
                "Status": "",
                "IsCompleted": 0,
                "ActivityDate": opp.get("CloseDate", ""),
                "CreatedDate": (opp.get("CreatedDate") or "")[:10],
                "MonthLabel": month_key((opp.get("CloseDate") or "2026-01-01")),
                "WeekLabel": "",
                "DayOfWeek": "",
                "DurationMinutes": 0,
                "HasOpportunity": 1,
                "HasAccount": 1 if acct_id else 0,
                "DaysAgo": 0,
                "ActivityCount": opp_activities,
                "OppAmount": safe_float(opp.get("Amount"), 0),
                "OppStage": opp.get("StageName", ""),
                "OppType": opp.get("Type", ""),
            }
        )

    print(
        f"  → {len(rows):,} total rows ({sum(1 for r in rows if r['RecordType'] == 'activity'):,} activities)"
    )

    # Build CSV
    field_names = [
        "RecordType",
        "ActivityId",
        "Source",
        "OwnerId",
        "OwnerName",
        "AccountId",
        "AccountName",
        "OpportunityId",
        "ActivityType",
        "RawType",
        "Subject",
        "Status",
        "IsCompleted",
        "ActivityDate",
        "CreatedDate",
        "MonthLabel",
        "WeekLabel",
        "DayOfWeek",
        "DurationMinutes",
        "HasOpportunity",
        "HasAccount",
        "DaysAgo",
        "ActivityCount",
        "OppAmount",
        "OppStage",
        "OppType",
    ]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=field_names, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    csv_bytes = buf.getvalue().encode("utf-8")

    fields_meta = [
        _dim("RecordType", "Record Type"),
        _dim("ActivityId", "Activity ID"),
        _dim("Source", "Source Object"),
        _dim("OwnerId", "Owner ID"),
        _dim("OwnerName", "Owner Name"),
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account Name"),
        _dim("OpportunityId", "Opportunity ID"),
        _dim("ActivityType", "Activity Type"),
        _dim("RawType", "Raw Activity Type"),
        _dim("Subject", "Subject"),
        _dim("Status", "Status"),
        _measure("IsCompleted", "Is Completed", scale=0, precision=2),
        _date("ActivityDate", "Activity Date"),
        _date("CreatedDate", "Created Date"),
        _dim("MonthLabel", "Month"),
        _dim("WeekLabel", "Week"),
        _dim("DayOfWeek", "Day of Week"),
        _measure("DurationMinutes", "Duration (Minutes)", scale=1, precision=8),
        _measure("HasOpportunity", "Has Opportunity", scale=0, precision=5),
        _measure("HasAccount", "Has Account", scale=0, precision=2),
        _measure("DaysAgo", "Days Since Activity", scale=0, precision=5),
        _measure("ActivityCount", "Activity Count", scale=0, precision=8),
        _measure("OppAmount", "Pipeline Amount", scale=2, precision=18),
        _dim("OppStage", "Opportunity Stage"),
        _dim("OppType", "Opportunity Type"),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _week_label(date_str):
    """Return ISO week label like '2026-W10'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return f"{dt.isocalendar()[0]}-W{dt.isocalendar()[1]:02d}"
    except (ValueError, TypeError):
        return ""


def _day_of_week(date_str):
    """Return day name like 'Monday'."""
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%A")
    except (ValueError, TypeError):
        return ""


# ---------------------------------------------------------------------------
# Dashboard state builders
# ---------------------------------------------------------------------------
def build_steps(ds_id):
    """Build all SAQL steps."""
    ds_meta = [{"id": ds_id, "name": DS}]
    detail = f'q = load "{DS}";\nq = filter q by RecordType == "activity";\n'
    rep_month = f'q = load "{DS}";\nq = filter q by RecordType == "rep_month";\n'
    acct_cov = f'q = load "{DS}";\nq = filter q by RecordType == "account_coverage";\n'
    opp_cov = f'q = load "{DS}";\nq = filter q by RecordType == "opp_coverage";\n'

    # Build summary step with facet scope so KPIs respond to filters
    s_summary = sq(
        detail
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "count() as TotalActivities, "
        + "sum(IsCompleted) as CompletedActivities, "
        + "sum(DurationMinutes) as TotalDuration, "
        + "unique(OwnerName) as UniqueReps, "
        + "unique(AccountName) as UniqueAccounts;"
    )
    s_summary.update(KPI_FACET_SCOPE)

    # Build exception KPIs step with facet scope
    s_exception_kpis = sq(
        acct_cov
        + "q = group q by all;\n"
        + "q = foreach q generate "
        + "sum(case when DaysAgo > 30 and HasOpportunity > 0 then 1 else 0 end) as StaleAccounts, "
        + "sum(case when DaysAgo > 30 and HasOpportunity > 0 then OppAmount else 0 end) as AtRiskPipeline, "
        + "count() as TotalAccounts;"
    )
    s_exception_kpis.update(KPI_FACET_SCOPE)

    return {
        # --- Filters ---
        "f_owner": af("OwnerName", ds_meta),
        "f_type": af("ActivityType", ds_meta),
        "f_month": af("MonthLabel", ds_meta),
        "f_account": af("AccountName", ds_meta),
        # --- Page 1: Team Overview ---
        "s_summary": s_summary,
        "s_by_type": sq(
            detail
            + "q = group q by ActivityType;\n"
            + "q = foreach q generate ActivityType, count() as ActivityCount;\n"
            + "q = order q by ActivityCount desc;"
        ),
        "s_monthly_trend": sq(
            detail
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "count() as TotalActivities, "
            + 'sum(case when ActivityType == "Call" then 1 else 0 end) as Calls, '
            + 'sum(case when ActivityType == "Meeting" then 1 else 0 end) as Meetings, '
            + 'sum(case when ActivityType == "Email" then 1 else 0 end) as Emails;\n'
            + "q = order q by MonthLabel asc;"
        ),
        "s_top_reps": sq(
            detail
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "count() as TotalActivities, "
            + 'sum(case when ActivityType == "Call" then 1 else 0 end) as Calls, '
            + 'sum(case when ActivityType == "Meeting" then 1 else 0 end) as Meetings, '
            + 'sum(case when ActivityType == "Email" then 1 else 0 end) as Emails, '
            + "unique(AccountName) as AccountsTouched;\n"
            + "q = order q by TotalActivities desc;\n"
            + "q = limit q 20;"
        ),
        # --- Page 2: Rep Productivity ---
        "s_rep_cadence": sq(
            detail
            + "q = group q by (OwnerName, MonthLabel);\n"
            + "q = foreach q generate OwnerName, MonthLabel, "
            + "count() as ActivityCount, "
            + 'sum(case when ActivityType == "Call" then 1 else 0 end) as Calls, '
            + 'sum(case when ActivityType == "Meeting" then 1 else 0 end) as Meetings;\n'
            + "q = order q by OwnerName asc, MonthLabel asc;"
        ),
        "s_day_heatmap": sq(
            detail
            + 'q = filter q by DayOfWeek != "";\n'
            + "q = group q by (DayOfWeek, ActivityType);\n"
            + "q = foreach q generate DayOfWeek, ActivityType, "
            + "count() as ActivityCount;\n"
            + "q = order q by ActivityCount desc;"
        ),
        "s_rep_compare": sq(
            detail
            + 'q = filter q by MonthLabel >= "2025-01";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "count() as TotalActivities, "
            + 'sum(case when ActivityType == "Call" then 1 else 0 end) as Calls, '
            + 'sum(case when ActivityType == "Meeting" then 1 else 0 end) as Meetings, '
            + 'sum(case when ActivityType == "Email" then 1 else 0 end) as Emails, '
            + "unique(AccountName) as AccountsTouched, "
            + "sum(DurationMinutes) as TotalMinutes;\n"
            + "q = order q by TotalActivities desc;"
        ),
        # --- Page 3: Account & Deal Coverage ---
        "s_account_activity": sq(
            acct_cov
            + "q = group q by (AccountName, DaysAgo, OppType);\n"
            + "q = foreach q generate AccountName, "
            + "sum(ActivityCount) as TotalActivities, "
            + "max(DaysAgo) as DaysSinceLastActivity, "
            + "sum(OppAmount) as OpenPipeline, "
            + "max(HasOpportunity) as OpenOppCount, "
            + "first(OppType) as NextStepStatus;\n"
            + "q = order q by DaysSinceLastActivity desc;"
        ),
        "s_opp_engagement": sq(
            opp_cov
            + "q = group q by (Subject, OppStage, OwnerName, OppType);\n"
            + "q = foreach q generate Subject as OppName, OppStage, OwnerName, OppType, "
            + "sum(ActivityCount) as LinkedActivities, "
            + "sum(OppAmount) as Amount;\n"
            + "q = order q by LinkedActivities asc;\n"
            + "q = limit q 50;"
        ),
        "s_coverage_scatter": sq(
            opp_cov
            + "q = group q by (Subject, OppStage);\n"
            + "q = foreach q generate Subject as OppName, OppStage, "
            + "sum(ActivityCount) as LinkedActivities, "
            + "sum(OppAmount) as Amount;\n"
            + "q = order q by Amount desc;"
        ),
        # --- Page 4: Exceptions & Actions ---
        "s_stale_accounts": sq(
            acct_cov
            + "q = filter q by DaysAgo > 30;\n"
            + "q = filter q by HasOpportunity > 0;\n"
            + "q = group q by (AccountName, AccountId);\n"
            + "q = foreach q generate AccountName, first(AccountId) as AccountId, "
            + "max(DaysAgo) as DaysSinceLastActivity, "
            + "sum(OppAmount) as AtRiskPipeline, "
            + "max(HasOpportunity) as OpenOpps;\n"
            + "q = order q by AtRiskPipeline desc;\n"
            + "q = limit q 30;"
        ),
        "s_low_activity_reps": sq(
            rep_month
            + 'q = filter q by MonthLabel >= "2026-01";\n'
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, "
            + "sum(ActivityCount) as TotalActivities, "
            + "avg(ActivityCount) as AvgMonthlyActivities;\n"
            + "q = order q by TotalActivities asc;\n"
            + "q = limit q 20;"
        ),
        "s_no_next_step": sq(
            opp_cov
            + 'q = filter q by OppType == "No Next Step";\n'
            + "q = group q by (Subject, OppStage, OwnerName, OpportunityId);\n"
            + "q = foreach q generate Subject as OppName, OppStage, OwnerName, "
            + "first(OpportunityId) as OpportunityId, "
            + "sum(OppAmount) as Amount, "
            + "sum(ActivityCount) as Activities;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 30;"
        ),
        "s_exception_kpis": s_exception_kpis,
    }


def build_widgets():
    """Build all widget definitions."""
    w = {}

    # ===== Page 1: Team Overview =====
    w["p1_hdr"] = hdr(
        "Sales Activity & Productivity",
        "Team activity overview — calls, meetings, emails, tasks",
    )
    w["p1_n_total"] = num(
        "s_summary",
        "TotalActivities",
        "Total Activities",
        "#0070D2",
        compact=True,
        tier="primary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_completed"] = num(
        "s_summary",
        "CompletedActivities",
        "Completed",
        "#04844B",
        compact=True,
        tier="primary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_reps"] = num(
        "s_summary",
        "UniqueReps",
        "Active Reps",
        "#9050E9",
        tier="secondary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p1_n_accounts"] = num(
        "s_summary",
        "UniqueAccounts",
        "Accounts Touched",
        "#E87722",
        tier="secondary",
        widget_style=KPI_CARD_STYLE,
    )

    w["p1_section_charts"] = section_label("Activity Breakdown")
    w["p1_ch_type"] = rich_chart(
        "s_by_type",
        "hbar",
        "Activity Breakdown by Type",
        ["ActivityType"],
        ["ActivityCount"],
        subtitle="Distribution of completed activities across call, meeting, email, and task types",
    )
    w["p1_ch_trend"] = line_chart(
        "s_monthly_trend",
        "Monthly Activity Trend",
        show_legend=True,
        axis_title="Activity Count",
        subtitle="Month-over-month trajectory — look for declining trends that signal engagement drops",
        reference_lines=[
            {"value": 50, "label": "Activity Target", "color": "#04844B"},
        ],
    )
    w["p1_section_reps"] = section_label("Top Performers")
    w["p1_ch_reps"] = compare_table(
        "s_top_reps",
        "Top Reps by Activity Volume",
        columns=[
            "OwnerName",
            "TotalActivities",
            "Calls",
            "Meetings",
            "Emails",
            "AccountsTouched",
        ],
        subtitle="Ranked by total activities — compare call:meeting:email ratios across reps",
        format_rules=[
            {
                "type": "threshold",
                "field": "TotalActivities",
                "rules": [
                    {"value": 100, "color": "#04844B", "operator": "gte"},
                    {"value": 30, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )

    # ===== Page 2: Rep Productivity =====
    w["p2_hdr"] = hdr(
        "Rep Productivity & Cadence",
        "Per-rep activity patterns, day-of-week distribution, and comparison",
    )
    w["p2_section_heatmap"] = section_label("Activity Patterns")
    w["p2_ch_heatmap"] = heatmap_chart(
        "s_day_heatmap",
        "Activity by Day of Week × Type",
    )
    w["p2_ch_heatmap"]["parameters"]["title"]["subtitleLabel"] = (
        "Darker = more activities — identifies peak days and gaps in weekly cadence"
    )
    w["p2_ch_cadence"] = rich_chart(
        "s_rep_cadence",
        "stackcolumn",
        "Rep Activity Cadence by Month",
        ["MonthLabel"],
        ["ActivityCount"],
        split=["OwnerName"],
        show_legend=True,
        subtitle="Stacked by rep — watch for reps dropping off or seasonal patterns",
    )
    w["p2_section_compare"] = section_label("Rep Comparison")
    w["p2_ch_compare"] = compare_table(
        "s_rep_compare",
        "Rep Comparison (2025+)",
        columns=[
            "OwnerName",
            "TotalActivities",
            "Calls",
            "Meetings",
            "Emails",
            "AccountsTouched",
            "TotalMinutes",
        ],
        subtitle="Side-by-side rep metrics — TotalMinutes = time invested in activities",
        format_rules=[
            {
                "type": "threshold",
                "field": "TotalActivities",
                "rules": [
                    {"value": 100, "color": "#04844B", "operator": "gte"},
                    {"value": 30, "color": "#FFB75D", "operator": "gte"},
                ],
            },
            {
                "type": "threshold",
                "field": "TotalMinutes",
                "rules": [
                    {"value": 500, "color": "#04844B", "operator": "gte"},
                    {"value": 100, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )

    # ===== Page 3: Account & Deal Coverage =====
    w["p3_hdr"] = hdr(
        "Account & Deal Coverage",
        "Which accounts and deals have enough engagement depth?",
    )
    w["p3_section_acct"] = section_label("Account Coverage")
    w["p3_ch_acct"] = compare_table(
        "s_account_activity",
        "Account Activity Coverage",
        columns=[
            "AccountName",
            "TotalActivities",
            "DaysSinceLastActivity",
            "OpenPipeline",
            "OpenOppCount",
        ],
        subtitle="Sorted by days since last activity — high pipeline + stale = intervention needed",
        format_rules=[
            {
                "type": "threshold",
                "field": "DaysSinceLastActivity",
                "rules": [
                    {"value": 60, "color": "#D4504C", "operator": "gte"},
                    {"value": 30, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )
    w["p3_section_deals"] = section_label("Deal Engagement")
    w["p3_ch_scatter"] = scatter_chart(
        "s_coverage_scatter",
        "Deal Size vs Activity Depth",
    )
    w["p3_ch_scatter"]["parameters"]["title"]["subtitleLabel"] = (
        "Bottom-right = big deal, few activities = risk | Top-left = small deal, over-invested"
    )
    w["p3_ch_opp"] = compare_table(
        "s_opp_engagement",
        "Deals with Lowest Activity",
        columns=[
            "OppName",
            "OppStage",
            "OwnerName",
            "OppType",
            "LinkedActivities",
            "Amount",
        ],
        subtitle="Sorted ascending — top rows are the most neglected open deals",
        format_rules=[
            {
                "type": "threshold",
                "field": "LinkedActivities",
                "rules": [
                    {"value": 1, "color": "#D4504C", "operator": "lte"},
                    {"value": 3, "color": "#FFB75D", "operator": "lte"},
                ],
            },
        ],
    )

    # ===== Page 4: Exceptions & Actions =====
    w["p4_hdr"] = hdr(
        "Exceptions & Actions",
        "Stale accounts, low-activity reps, deals without next steps",
    )
    w["p4_n_stale"] = num(
        "s_exception_kpis",
        "StaleAccounts",
        "Stale Accounts (30+ days)",
        "#E3394D",
        tier="primary",
        widget_style=KPI_CARD_STYLE,
    )
    w["p4_n_risk"] = num(
        "s_exception_kpis",
        "AtRiskPipeline",
        "At-Risk Pipeline",
        "#E3394D",
        compact=True,
        tier="primary",
        prefix="€",
        widget_style=KPI_CARD_STYLE,
    )

    w["p4_section_stale"] = section_label("Stale Accounts")
    w["p4_ch_stale"] = compare_table(
        "s_stale_accounts",
        "Stale Accounts with Open Pipeline",
        columns=[
            "AccountName",
            "DaysSinceLastActivity",
            "AtRiskPipeline",
            "OpenOpps",
        ],
        subtitle="Accounts with >30 days since last activity AND open opportunities — highest risk first",
        format_rules=[
            {
                "type": "threshold",
                "field": "DaysSinceLastActivity",
                "rules": [
                    {"value": 60, "color": "#D4504C", "operator": "gte"},
                    {"value": 30, "color": "#FFB75D", "operator": "gte"},
                ],
            },
            {
                "type": "threshold",
                "field": "AtRiskPipeline",
                "rules": [
                    {"value": 100000, "color": "#D4504C", "operator": "gte"},
                    {"value": 50000, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )
    w["p4_section_coaching"] = section_label("Coaching & Process Gaps")
    w["p4_ch_low_reps"] = compare_table(
        "s_low_activity_reps",
        "Reps Below Activity Threshold (2026 YTD)",
        columns=["OwnerName", "TotalActivities", "AvgMonthlyActivities"],
        subtitle="Lowest-activity reps — may need coaching, enablement, or workload rebalancing",
        format_rules=[
            {
                "type": "threshold",
                "field": "TotalActivities",
                "rules": [
                    {"value": 10, "color": "#D4504C", "operator": "lte"},
                    {"value": 30, "color": "#FFB75D", "operator": "lte"},
                ],
            },
        ],
    )
    w["p4_ch_no_step"] = compare_table(
        "s_no_next_step",
        "Open Deals Without Next Step",
        columns=["OppName", "OppStage", "OwnerName", "Amount", "Activities"],
        subtitle="Deals missing NextStep field — these are likely stalled or poorly maintained",
        format_rules=[
            {
                "type": "threshold",
                "field": "Amount",
                "rules": [
                    {"value": 100000, "color": "#D4504C", "operator": "gte"},
                    {"value": 50000, "color": "#FFB75D", "operator": "gte"},
                ],
            },
        ],
    )

    # Navigation — per-page nav widgets (each page highlights its own tab)
    pages = ["overview", "productivity", "coverage", "exceptions"]
    labels = ["Team Overview", "Rep Productivity", "Account Coverage", "Exceptions"]
    for pg_idx in range(4):
        for nav_idx in range(4):
            name = f"p{pg_idx + 1}_nav{nav_idx + 1}"
            w[name] = nav_link(
                pages[nav_idx], labels[nav_idx], active=(pg_idx == nav_idx)
            )

    # Filters
    w["f_owner_w"] = listselector("f_owner", "Owner")
    w["f_type_w"] = listselector("f_type", "Activity Type")
    w["f_month_w"] = listselector("f_month", "Month")
    w["f_account_w"] = listselector("f_account", "Account")

    return w


def build_layout():
    """Build the dashboard layout."""
    filt = [
        {"name": "f_owner_w", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "f_type_w", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "f_month_w", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "f_account_w", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
    ]

    p1 = (
        nav_row("p1", 4)
        + [{"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {"name": "p1_n_total", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
            {
                "name": "p1_n_completed",
                "row": 5,
                "column": 3,
                "colspan": 3,
                "rowspan": 4,
            },
            {"name": "p1_n_reps", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
            {
                "name": "p1_n_accounts",
                "row": 5,
                "column": 9,
                "colspan": 3,
                "rowspan": 4,
            },
            {
                "name": "p1_section_charts",
                "row": 9,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p1_ch_type", "row": 10, "column": 0, "colspan": 4, "rowspan": 8},
            {"name": "p1_ch_trend", "row": 10, "column": 4, "colspan": 8, "rowspan": 8},
            {
                "name": "p1_section_reps",
                "row": 18,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p1_ch_reps", "row": 19, "column": 0, "colspan": 12, "rowspan": 8},
        ]
    )

    p2 = (
        nav_row("p2", 4)
        + [{"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {
                "name": "p2_section_heatmap",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_heatmap",
                "row": 6,
                "column": 0,
                "colspan": 5,
                "rowspan": 7,
            },
            {
                "name": "p2_ch_cadence",
                "row": 6,
                "column": 5,
                "colspan": 7,
                "rowspan": 7,
            },
            {
                "name": "p2_section_compare",
                "row": 13,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p2_ch_compare",
                "row": 14,
                "column": 0,
                "colspan": 12,
                "rowspan": 10,
            },
        ]
    )

    p3 = (
        nav_row("p3", 4)
        + [{"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {
                "name": "p3_section_acct",
                "row": 5,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p3_ch_acct", "row": 6, "column": 0, "colspan": 12, "rowspan": 8},
            {
                "name": "p3_section_deals",
                "row": 14,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p3_ch_scatter",
                "row": 15,
                "column": 0,
                "colspan": 6,
                "rowspan": 8,
            },
            {"name": "p3_ch_opp", "row": 15, "column": 6, "colspan": 6, "rowspan": 8},
        ]
    )

    p4 = (
        nav_row("p4", 4)
        + [{"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2}]
        + filt
        + [
            {"name": "p4_n_stale", "row": 5, "column": 0, "colspan": 6, "rowspan": 3},
            {"name": "p4_n_risk", "row": 5, "column": 6, "colspan": 6, "rowspan": 3},
            {
                "name": "p4_section_stale",
                "row": 8,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {"name": "p4_ch_stale", "row": 9, "column": 0, "colspan": 12, "rowspan": 7},
            {
                "name": "p4_section_coaching",
                "row": 16,
                "column": 0,
                "colspan": 12,
                "rowspan": 1,
            },
            {
                "name": "p4_ch_low_reps",
                "row": 17,
                "column": 0,
                "colspan": 6,
                "rowspan": 7,
            },
            {
                "name": "p4_ch_no_step",
                "row": 17,
                "column": 6,
                "colspan": 6,
                "rowspan": 7,
            },
        ]
    )

    return {
        "name": "sales_activity_productivity",
        "numColumns": 12,
        "pages": [
            pg("overview", "Team Overview", p1),
            pg("productivity", "Rep Productivity", p2),
            pg("coverage", "Account Coverage", p3),
            pg("exceptions", "Exceptions & Actions", p4),
        ],
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    print("=" * 60)
    print("Building: Sales Activity & Productivity Dashboard")
    print("=" * 60)

    inst, tok = get_auth()

    print("\n[1/4] Creating dataset...")
    ok = create_dataset(inst, tok)
    if not ok:
        print("FAILED: Dataset upload failed.")
        sys.exit(1)

    print("\n[2/4] Resolving dataset ID...")
    ds_id = get_dataset_id(inst, tok, DS)
    if not ds_id:
        print("FAILED: Could not find dataset.")
        sys.exit(1)
    print(f"  Dataset ID: {ds_id}")

    print("\n[3/4] Building dashboard state...")
    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()
    state = build_dashboard_state(
        steps,
        widgets,
        layout,
        bg_color="#F4F6F9",
        cell_spacing=8,
        row_height="normal",
    )

    print("\n[4/4] Deploying dashboard...")
    dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    deploy_dashboard(inst, tok, dash_id, state)
    print(f"  Dashboard ID: {dash_id}")

    # Set record links for account and opportunity navigation
    print("\n  Setting record links...")
    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "AccountName", "id_field": "AccountId", "label": "Account"},
            {
                "field": "OpportunityId",
                "id_field": "OpportunityId",
                "label": "Opportunity",
            },
        ],
    )

    print("\n✓ Sales Activity & Productivity dashboard deployed!")
    print(f"  Open: https://simcorp.lightning.force.com/analytics/dashboard/{dash_id}")


if __name__ == "__main__":
    main()
