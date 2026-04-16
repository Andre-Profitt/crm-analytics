#!/usr/bin/env python3
"""Build the Lead Management KPI dashboard (AP 1.1).

Features:
  - 4 pages: MQL Funnel, Channel Mix, Conversion Tracking, Activity & Engagement
  - UNION SAQL funnel: Total Leads -> MQL -> Converted -> Won
  - MQL rate gauge, activity rate gauge
  - Channel mix donuts, stacked area, combo charts
  - Lead aging bands, velocity, waterfall
  - High-score no-activity drill-down table
  - broadcastFacet on chart steps for cross-filtering
  - Global 4-filter bar (Source, Status, Month, Owner) on all pages
  - Phase 3: KPI trend indicators (YoY via CreatedDate substr)
"""

import csv
import io
import sys
from datetime import datetime

from crm_analytics_helpers import (
    _date_diff,
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    get_dataset_id,
    create_dashboard_if_needed,
    sq,
    num,
    trend_step,
    rich_chart,
    gauge,
    waterfall_chart,
    hdr,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    af,
    pillbox,
    coalesce_filter,
    section_label,
    sankey_chart,
    heatmap_chart,
    area_chart,
    bullet_chart,
    treemap_chart,
    bubble_chart,
    create_dataflow,
    run_dataflow,
    set_record_links_xmd,  # noqa: F401
    compare_table,
    funnel_chart,
    KPI_CARD_STYLE,
)

DS = "Lead_Management"
DS_LABEL = "Lead Management"
DASHBOARD_LABEL = "Lead Management KPIs"

TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# Filter binding expressions
SF = coalesce_filter("f_source", "LeadSource")
STF = coalesce_filter("f_status", "Status")
MF = coalesce_filter("f_month", "CreatedMonth")
OF = coalesce_filter("f_owner", "OwnerName")

# ── YoY trend filters (use CreatedDate_Year dimension, not substr in filter) ──
TREND_CURRENT = 'q = filter q by CreatedDate_Year == "2026";\n'
TREND_PRIOR = 'q = filter q by CreatedDate_Year == "2025";\n'
TREND_BASE = SF + STF + MF + OF

# Consulting-grade facet scope: KPI tiles listen to all filter pillboxes
KPI_FACET_SCOPE = {
    "receiveFacetSource": {
        "mode": "include",
        "steps": ["f_source", "f_status", "f_month", "f_owner"],
    },
}


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset creation
# ═══════════════════════════════════════════════════════════════════════════


def create_dataset(inst, tok):
    """Query Lead object, compute derived fields, upload CSV dataset."""
    print("\n=== Building Lead Management dataset ===")

    leads = _soql(
        inst,
        tok,
        "SELECT Id, Name, Owner.Name, Status, LeadSource, CreatedDate, "
        "LastActivityDate, ConvertedDate, IsConverted, ConvertedOpportunityId, "
        "Company, pi__score__c, pi__campaign__c, pi__utm_campaign__c, "
        "Disqualified_Reason__c "
        "FROM Lead WHERE CreatedDate >= 2024-01-01T00:00:00Z",
    )
    print(f"  Queried {len(leads)} leads")

    # ── Build CSV ──
    fields = [
        "Id",
        "Name",
        "OwnerName",
        "Status",
        "LeadSource",
        "CreatedDate",
        "LastActivityDate",
        "ConvertedDate",
        "Company",
        "CreatedMonth",
        "LeadAgeDays",
        "IsDisqualified",
        "IsMQL",
        "HasActivity",
        "DaysToConvert",
        "DaysToFirstActivity",
        "LeadScore",
        "Campaign",
        "DisqualifiedReason",
        "ConvertedFlag",
        "ConvertedCount",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    for lead in leads:
        owner = lead.get("Owner") or {}
        created = (lead.get("CreatedDate") or "")[:10]
        last_activity = (lead.get("LastActivityDate") or "")[:10]
        converted_date = (lead.get("ConvertedDate") or "")[:10]
        is_converted = lead.get("IsConverted", False)
        status = lead.get("Status") or ""

        # Computed fields
        created_month = created[:7] if created else ""
        lead_age_days = _date_diff(created, TODAY) if created else 0
        is_disqualified = "true" if "Disqualified" in status else "false"
        is_mql = "false" if status in ("New", "Disqualified", "") else "true"
        has_activity = "true" if last_activity else "false"
        days_to_convert = (
            _date_diff(created, converted_date)
            if is_converted and converted_date
            else 0
        )
        days_to_first_activity = (
            _date_diff(created, last_activity) if last_activity else 0
        )
        lead_score = lead.get("pi__score__c") or 0
        campaign = lead.get("pi__campaign__c") or lead.get("pi__utm_campaign__c") or ""
        dq_reason = lead.get("Disqualified_Reason__c") or ""
        converted_flag = str(is_converted).lower()

        writer.writerow(
            {
                "Id": lead.get("Id", ""),
                "Name": (lead.get("Name") or "")[:255],
                "OwnerName": (owner.get("Name") or "")[:255],
                "Status": status,
                "LeadSource": lead.get("LeadSource") or "",
                "CreatedDate": created,
                "LastActivityDate": last_activity,
                "ConvertedDate": converted_date,
                "Company": (lead.get("Company") or "")[:255],
                "CreatedMonth": created_month,
                "LeadAgeDays": lead_age_days,
                "IsDisqualified": is_disqualified,
                "IsMQL": is_mql,
                "HasActivity": has_activity,
                "DaysToConvert": days_to_convert,
                "DaysToFirstActivity": days_to_first_activity,
                "LeadScore": lead_score,
                "Campaign": campaign[:255] if campaign else "",
                "DisqualifiedReason": dq_reason[:255] if dq_reason else "",
                "ConvertedFlag": converted_flag,
                "ConvertedCount": 1 if is_converted else 0,
            }
        )

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes, {len(leads)} rows")

    # ── Metadata ──
    fields_meta = [
        _dim("Id", "Lead ID"),
        _dim("Name", "Lead Name"),
        _dim("OwnerName", "Owner"),
        _dim("Status", "Status"),
        _dim("LeadSource", "Lead Source"),
        _date("CreatedDate", "Created Date"),
        _date("LastActivityDate", "Last Activity Date"),
        _date("ConvertedDate", "Converted Date"),
        _dim("Company", "Company"),
        _dim("CreatedMonth", "Created Month"),
        _measure("LeadAgeDays", "Lead Age (Days)", scale=0, precision=6),
        _dim("IsDisqualified", "Is Disqualified"),
        _dim("IsMQL", "Is MQL"),
        _dim("HasActivity", "Has Activity"),
        _measure("DaysToConvert", "Days to Convert", scale=0, precision=6),
        _measure(
            "DaysToFirstActivity",
            "Days to First Activity",
            scale=0,
            precision=6,
        ),
        _measure("LeadScore", "Lead Score", scale=0, precision=6),
        _dim("Campaign", "Campaign"),
        _dim("DisqualifiedReason", "Disqualified Reason"),
        _dim("ConvertedFlag", "Converted"),
        _measure("ConvertedCount", "Converted Count", scale=0, precision=6),
    ]

    return upload_dataset(inst, tok, DS, DS_LABEL, fields_meta, csv_bytes)


# ═══════════════════════════════════════════════════════════════════════════
#  Step builders
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_id):
    L = f'q = load "{DS}";\n'
    DS_META = [{"id": ds_id, "name": DS}]

    steps = {
        # ── Filter steps ──
        "f_source": af("LeadSource", DS_META),
        "f_status": af("Status", DS_META),
        "f_month": af("CreatedMonth", DS_META),
        "f_owner": af("OwnerName", DS_META),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 1 — MQL Funnel
        # ═══════════════════════════════════════════════════════════════
        # Funnel: UNION step — Total > MQL > Converted > Won (by count desc)
        # NOTE: UNION steps skip filter bindings (summary funnels show totals)
        "s_funnel": sq(
            f'q1 = load "{DS}";\n'
            + "q1 = group q1 by all;\n"
            + 'q1 = foreach q1 generate "1 - Total Leads" as Stage, '
            + "count() as cnt;\n"
            #
            + f'q2 = load "{DS}";\n'
            + 'q2 = filter q2 by IsMQL == "true";\n'
            + "q2 = group q2 by all;\n"
            + 'q2 = foreach q2 generate "2 - MQL" as Stage, '
            + "count() as cnt;\n"
            #
            + f'q3 = load "{DS}";\n'
            + 'q3 = filter q3 by ConvertedFlag == "true";\n'
            + "q3 = group q3 by all;\n"
            + 'q3 = foreach q3 generate "3 - Converted" as Stage, '
            + "count() as cnt;\n"
            #
            + f'q4 = load "{DS}";\n'
            + 'q4 = filter q4 by ConvertedFlag == "true";\n'
            + "q4 = group q4 by all;\n"
            + 'q4 = foreach q4 generate "4 - Won" as Stage, '
            + "count() as cnt;\n"
            #
            + "q = union q1, q2, q3, q4;\n"
            + "q = order q by cnt desc;"
        ),
        # KPI: Total lead count
        "s_total": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = group q by all;\n"
            + "q = foreach q generate count() as total_leads;"
        ),
        # KPI: Conversion rate %
        "s_conv_rate": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(ConvertedCount) / count()) * 100 as conv_rate;"
        ),
        # KPI: Avg days to convert
        "s_avg_convert": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = filter q by ConvertedFlag == "true";\n'
            + "q = filter q by DaysToConvert > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(DaysToConvert) as avg_days;"
        ),
        # Gauge: MQL rate %
        "s_mql_rate": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = foreach q generate (case when IsMQL == "true" '
            + "then 1 else 0 end) as is_mql;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(is_mql) / count()) * 100 as mql_rate;"
        ),
        # ── YoY trend steps (Phase 3) ──
        # Trend: Total lead count
        "s_trend_total": trend_step(
            DS,
            TREND_BASE,
            TREND_CURRENT,
            TREND_PRIOR,
            "all",
            "count()",
            "cnt",
        ),
        # Trend: Conversion rate %
        "s_trend_conv": trend_step(
            DS,
            TREND_BASE,
            TREND_CURRENT,
            TREND_PRIOR,
            "all",
            "(sum(ConvertedCount) / count()) * 100",
            "conv_rate",
        ),
        # Trend: Avg days to convert (base includes converted-only filter)
        "s_trend_avg_days": trend_step(
            DS,
            TREND_BASE
            + 'q = filter q by ConvertedFlag == "true";\n'
            + "q = filter q by DaysToConvert > 0;\n",
            TREND_CURRENT,
            TREND_PRIOR,
            "all",
            "avg(DaysToConvert)",
            "avg_days",
        ),
        # Trend: SLA breach count
        "s_trend_sla": trend_step(
            DS,
            TREND_BASE,
            TREND_CURRENT,
            TREND_PRIOR,
            "all",
            'sum(case when HasActivity == "false" then 1 '
            "when DaysToFirstActivity > 2 then 1 "
            "else 0 end)",
            "breach_count",
        ),
        # Hbar: Status distribution (skip STF — groups by Status)
        "s_status": sq(
            L
            + SF
            + MF
            + OF
            + "q = group q by Status;\n"
            + "q = foreach q generate Status, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Donut: DQ reasons
        "s_dq_reasons": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = filter q by IsDisqualified == "true";\n'
            + 'q = filter q by DisqualifiedReason != "";\n'
            + "q = group q by DisqualifiedReason;\n"
            + "q = foreach q generate DisqualifiedReason, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Area: MQL trend (skip MF — groups by CreatedMonth)
        "s_mql_trend": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by IsMQL == "true";\n'
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 2 — Channel Mix
        # ═══════════════════════════════════════════════════════════════
        # Donut: Lead source distribution (skip SF — groups by LeadSource)
        "s_source": sq(
            L
            + STF
            + MF
            + OF
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Stack area: Source over time (skip SF + MF — groups by CreatedMonth, LeadSource)
        "s_source_time": sq(
            L
            + STF
            + OF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by (CreatedMonth, LeadSource);\n"
            + "q = foreach q generate CreatedMonth, LeadSource, "
            + "count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Comparison table: Top campaigns with count + conversion rate
        "s_campaigns": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = filter q by Campaign != "";\n'
            + "q = foreach q generate Campaign, "
            + '(case when ConvertedFlag == "true" then 1 else 0 end) '
            + "as is_conv;\n"
            + "q = group q by Campaign;\n"
            + "q = foreach q generate Campaign, count() as cnt, "
            + "(sum(is_conv) / count()) * 100 as conv_rate;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 25;"
        ),
        # Combo: Conversion rate by source (skip SF — groups by LeadSource)
        "s_source_conv": sq(
            L
            + STF
            + MF
            + OF
            + "q = foreach q generate LeadSource, "
            + '(case when ConvertedFlag == "true" then 1 else 0 end) '
            + "as is_conv;\n"
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as cnt, "
            + "(sum(is_conv) / count()) * 100 as conv_rate;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 3 — Conversion Tracking
        # ═══════════════════════════════════════════════════════════════
        # Column: Monthly conversions (skip MF — groups by CreatedMonth)
        "s_monthly_conv": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by ConvertedFlag == "true";\n'
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Line: Avg days to convert by month (skip MF — groups by CreatedMonth)
        "s_monthly_days": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by ConvertedFlag == "true";\n'
            + "q = filter q by DaysToConvert > 0;\n"
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, "
            + "avg(DaysToConvert) as avg_days;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Stackhbar: Lead aging bands
        "s_aging": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = foreach q generate "
            + '(case when LeadAgeDays <= 7 then "0-7d" '
            + 'when LeadAgeDays <= 30 then "8-30d" '
            + 'when LeadAgeDays <= 90 then "31-90d" '
            + 'else "90d+" end) as AgeBand;\n'
            + "q = group q by AgeBand;\n"
            + "q = foreach q generate AgeBand, count() as cnt;\n"
            + "q = order q by AgeBand asc;"
        ),
        # Area: Lead velocity (skip MF — groups by CreatedMonth)
        "s_velocity": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Waterfall: Monthly net leads (skip MF — groups by CreatedMonth)
        "s_waterfall": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = foreach q generate CreatedMonth, "
            + '(case when ConvertedFlag == "true" then -1 else 0 end) '
            + "as conv_out, "
            + '(case when IsDisqualified == "true" then -1 else 0 end) '
            + "as dq_out;\n"
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, "
            + "count() + sum(conv_out) + sum(dq_out) as net_leads;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Source effectiveness: conversion rate + avg days by source
        "s_source_eff_conv": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as total, "
            + 'sum(case when ConvertedFlag == "true" then 1 else 0 end) as conv_cnt, '
            + "avg(DaysToConvert) as avg_days;\n"
            + "q = foreach q generate LeadSource, total, conv_cnt, "
            + "(conv_cnt * 100 / total) as conv_rate, avg_days;\n"
            + "q = order q by conv_rate desc;"
        ),
        # Source volume: lead count by source over time (skip SF + MF)
        "s_source_volume": sq(
            L
            + STF
            + OF
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by (CreatedMonth, LeadSource);\n"
            + "q = foreach q generate CreatedMonth, LeadSource, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 4 — Activity & Engagement
        # ═══════════════════════════════════════════════════════════════
        # Gauge: Activity rate %
        "s_activity_rate": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = foreach q generate (case when HasActivity == "true" '
            + "then 1 else 0 end) as has_act;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(has_act) / count()) * 100 as activity_rate;"
        ),
        "s_bullet_activity": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = foreach q generate (case when HasActivity == "true" '
            + "then 1 else 0 end) as has_act;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "(sum(has_act) / count()) * 100 as activity_rate, 80 as target;"
        ),
        # Number: SLA breach count (>2d no activity)
        "s_sla_breach": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = foreach q generate "
            + "(case "
            + 'when HasActivity == "false" then 1 '
            + "when DaysToFirstActivity > 2 then 1 "
            + "else 0 end) as breached;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate sum(breached) as breach_count;"
        ),
        # Line: Activity trend (skip MF — groups by CreatedMonth)
        "s_activity_trend": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by HasActivity == "true";\n'
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Comparison table: High-score leads with no activity (top 25)
        "s_no_activity": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = filter q by HasActivity == "false";\n'
            + "q = filter q by LeadScore > 0;\n"
            + "q = foreach q generate Id, Name, Company, OwnerName, Status, "
            + "LeadScore, LeadAgeDays;\n"
            + "q = order q by LeadScore desc;\n"
            + "q = limit q 25;"
        ),
        # ═══ SOURCE ATTRIBUTION & HOT LEADS (AP 1.1 gaps) ═══
        # Source attribution gauge: % leads with non-null LeadSource
        "s_source_attrib": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = foreach q generate (case when LeadSource != "" then 1 else 0 end) as has_source;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_source) / count()) * 100 as attrib_rate, count() as total;"
        ),
        "s_bullet_source_attrib": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = foreach q generate (case when LeadSource != "" then 1 else 0 end) as has_source;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(has_source) / count()) * 100 as attrib_rate, 85 as target;"
        ),
        # Hot leads: high-score leads with response metrics
        "s_hot_leads": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = filter q by LeadScore > 50;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt, avg(DaysToFirstActivity) as avg_response;"
        ),
        # Hot lead response by owner (top responders)
        "s_hot_by_owner": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + "q = filter q by LeadScore > 50;\n"
            + "q = group q by OwnerName;\n"
            + "q = foreach q generate OwnerName, count() as cnt, "
            + "avg(DaysToFirstActivity) as avg_response;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 15;"
        ),
        # ═══ V2: Advanced Visualizations ═══
        # Sankey: Lead Source → Status flow
        "s_sankey_source": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + 'q = filter q by LeadSource != "" && Status != "";\n'
            + "q = group q by (LeadSource, Status);\n"
            + "q = foreach q generate LeadSource as source, Status as target, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Heatmap: Source × Month lead volume
        "s_heatmap_source_month": sq(
            L
            + SF
            + STF
            + OF
            + 'q = filter q by LeadSource != "" && CreatedMonth != "";\n'
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by (LeadSource, CreatedMonth);\n"
            + "q = foreach q generate LeadSource, CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # Stacked area: Leads by source over time
        "s_area_source": sq(
            L
            + STF
            + OF
            + 'q = filter q by LeadSource != "" && CreatedMonth != "";\n'
            + 'q = filter q by CreatedMonth >= "2025-01";\n'
            + "q = group q by (CreatedMonth, LeadSource);\n"
            + "q = foreach q generate CreatedMonth, LeadSource, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ═══ V2 Phase 6: Bullet Charts ═══
        "s_bullet_mql": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + 'q = foreach q generate (case when IsMQL == "true" then 1 else 0 end) as is_mql;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_mql) / count()) * 100 as mql_rate, 20 as target;"
        ),
        "s_bullet_conv": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + 'q = foreach q generate (case when ConvertedFlag == "true" then 1 else 0 end) as is_conv;\n'
            + "q = group q by all;\n"
            + "q = foreach q generate (sum(is_conv) / count()) * 100 as conv_rate, 15 as target;"
        ),
        # ═══ V2 Phase 8: Statistical Analysis ═══
        "s_stat_source_conv": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + 'q = filter q by LeadSource != "";\n'
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as total, "
            + 'sum(case when ConvertedFlag == "true" then 1 else 0 end) as converted, '
            + '(sum(case when ConvertedFlag == "true" then 1 else 0 end) / count()) * 100 as conv_pct;\n'
            + "q = order q by conv_pct desc;"
        ),
        "s_stat_lead_age_dist": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + "q = foreach q generate "
            + '(case when LeadAgeDays <= 7 then "0-7d" '
            + 'when LeadAgeDays <= 30 then "8-30d" '
            + 'when LeadAgeDays <= 90 then "31-90d" '
            + 'else "90d+" end) as AgeBand;\n'
            + "q = group q by AgeBand;\n"
            + "q = foreach q generate AgeBand, count() as cnt;\n"
            + "q = order q by AgeBand asc;"
        ),
        # ═══ V2 Phase 10: Treemap ═══
        "s_treemap_source": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ═══ V2 Phase 10: Bubble ═══
        "s_bubble_source": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, "
            + "count() as total, "
            + 'sum(case when ConvertedFlag == "true" then 1 else 0 end) as converted, '
            + "avg(LeadAgeDays) as avg_age;\n"
            + "q = order q by total desc;"
        ),
        # ═══ V2 Phase 10: Stats ═══
        "s_stat_score_percentiles": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as cnt, "
            + "avg(LeadScore) as mean_score, "
            + "stddev(LeadScore) as std_dev, "
            + "percentile_disc(0.25) within group (order by LeadScore) as p25, "
            + "percentile_disc(0.50) within group (order by LeadScore) as median_score, "
            + "percentile_disc(0.75) within group (order by LeadScore) as p75;"
        ),
        "s_stat_convert_percentiles": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + 'q = filter q by ConvertedFlag == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "count() as cnt, "
            + "avg(DaysToConvert) as mean_days, "
            + "stddev(DaysToConvert) as std_dev, "
            + "percentile_disc(0.25) within group (order by DaysToConvert) as p25, "
            + "percentile_disc(0.50) within group (order by DaysToConvert) as median_days, "
            + "percentile_disc(0.75) within group (order by DaysToConvert) as p75;"
        ),
        "s_stat_running_leads": sq(
            L
            + SF
            + STF
            + MF
            + OF
            + TREND_CURRENT
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as monthly_cnt;\n"
            + "q = order q by CreatedMonth asc;\n"
            + "q = foreach q generate CreatedMonth, monthly_cnt, "
            + "sum(monthly_cnt) over (order by CreatedMonth rows unbounded preceding) as cumulative_cnt;"
        ),
    }

    # Apply consulting-grade facet scope to KPI summary steps
    for key in (
        "s_total",
        "s_conv_rate",
        "s_avg_convert",
        "s_mql_rate",
        "s_sla_breach",
        "s_activity_rate",
        "s_source_attrib",
        "s_hot_leads",
    ):
        steps[key].update(KPI_FACET_SCOPE)

    return steps


# ═══════════════════════════════════════════════════════════════════════════
#  Widget builders
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    w = {
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 1 — MQL Funnel
        # ═══════════════════════════════════════════════════════════════
        "p1_nav1": nav_link("funnel", "MQL Funnel", active=True),
        "p1_nav2": nav_link("channel", "Channel Mix"),
        "p1_nav3": nav_link("conversion", "Conversion"),
        "p1_nav4": nav_link("activity", "Activity"),
        "p1_hdr": hdr(
            "Lead Management — MQL Funnel",
            "Since Jan 2024 | Lead lifecycle from creation to conversion",
        ),
        # Filter bar
        "p1_f_source": pillbox("f_source", "Lead Source"),
        "p1_f_status": pillbox("f_status", "Status"),
        "p1_f_month": pillbox("f_month", "Created Month"),
        "p1_f_owner": pillbox("f_owner", "Owner"),
        # Funnel chart
        "p1_funnel": funnel_chart("s_funnel", "Lead Funnel", "Stage", "cnt"),
        # Section: KPI Summary
        "p1_sec_kpi": section_label("KPI Summary"),
        # KPI tiles (with YoY trend indicators — Phase 3)
        "p1_total": num(
            "s_trend_total",
            "cnt",
            "Total Leads",
            "#0070D2",
            compact=True,
            tier="primary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_conv_rate": num(
            "s_trend_conv",
            "conv_rate",
            "Conversion Rate %",
            "#04844B",
            compact=False,
            tier="primary",
            suffix="%",
            widget_style=KPI_CARD_STYLE,
        ),
        "p1_avg_days": num(
            "s_trend_avg_days",
            "avg_days",
            "Avg Days to Convert",
            "#FF6600",
            compact=False,
            tier="primary",
            suffix=" days",
            widget_style=KPI_CARD_STYLE,
        ),
        # Gauge: MQL Rate
        "p1_mql_gauge": gauge(
            "s_mql_rate",
            "mql_rate",
            "MQL Rate %",
            min_val=0,
            max_val=100,
            bands=[
                {"start": 0, "stop": 70, "color": "#D4504C"},
                {"start": 70, "stop": 90, "color": "#FFB75D"},
                {"start": 90, "stop": 100, "color": "#04844B"},
            ],
        ),
        # Section: Status Breakdown
        "p1_sec_status": section_label("Status Breakdown"),
        # Hbar: Status distribution
        "p1_status": rich_chart(
            "s_status",
            "hbar",
            "Status Distribution",
            ["Status"],
            ["cnt"],
            axis_title="Count",
            show_values=True,
        ),
        # Donut: DQ reasons
        "p1_dq": rich_chart(
            "s_dq_reasons",
            "donut",
            "Disqualification Reasons",
            ["DisqualifiedReason"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Section: MQL Trend
        "p1_sec_mql": section_label("MQL Trend"),
        # Area: MQL trend
        "p1_mql_trend": rich_chart(
            "s_mql_trend",
            "area",
            "MQL Leads by Month",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 2 — Channel Mix
        # ═══════════════════════════════════════════════════════════════
        "p2_nav1": nav_link("funnel", "MQL Funnel"),
        "p2_nav2": nav_link("channel", "Channel Mix", active=True),
        "p2_nav3": nav_link("conversion", "Conversion"),
        "p2_nav4": nav_link("activity", "Activity"),
        "p2_hdr": hdr(
            "Lead Management — Channel Mix",
            "Lead sources, campaigns, and channel effectiveness",
        ),
        # Filter bar
        "p2_f_source": pillbox("f_source", "Lead Source"),
        "p2_f_status": pillbox("f_status", "Status"),
        "p2_f_month": pillbox("f_month", "Created Month"),
        "p2_f_owner": pillbox("f_owner", "Owner"),
        # Section: Source Overview
        "p2_sec_source": section_label("Source Overview"),
        # Donut: Source distribution
        "p2_source": rich_chart(
            "s_source",
            "donut",
            "Lead Source Distribution",
            ["LeadSource"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Stacked area: Source over time
        "p2_source_time": rich_chart(
            "s_source_time",
            "stackarea",
            "Lead Source over Time",
            ["CreatedMonth"],
            ["cnt"],
            split=["LeadSource"],
            show_legend=True,
            axis_title="Count",
        ),
        # Section: Campaign Performance
        "p2_sec_campaigns": section_label("Campaign Performance"),
        # Comparison table: Top campaigns
        "p2_campaigns": compare_table(
            "s_campaigns",
            "Top Campaigns -- Count & Conversion Rate",
            columns=["Campaign", "cnt", "conv_rate"],
            subtitle="Ranked by volume | conv_rate = converted / total x 100",
            format_rules=[
                {
                    "measure": "conv_rate",
                    "ranges": [
                        {"min": 0, "max": 10, "color": "#D4504C"},
                        {"min": 10, "max": 20, "color": "#FFB75D"},
                        {"min": 20, "color": "#04844B"},
                    ],
                },
            ],
        ),
        # Section: Channel Effectiveness
        "p2_sec_conv": section_label("Channel Effectiveness"),
        # Combo: Conversion rate by source
        "p2_source_conv": rich_chart(
            "s_source_conv",
            "combo",
            "Source: Volume vs Conversion Rate",
            ["LeadSource"],
            ["cnt", "conv_rate"],
            show_legend=True,
            axis_title="Count / Conv %",
            combo_config={
                "plotConfiguration": [
                    {"series": "cnt", "chartType": "column"},
                    {"series": "conv_rate", "chartType": "line"},
                ]
            },
            reference_lines=[
                {"value": 15, "label": "Conv Target 15%", "color": "#04844B"},
            ],
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 3 — Conversion Tracking
        # ═══════════════════════════════════════════════════════════════
        "p3_nav1": nav_link("funnel", "MQL Funnel"),
        "p3_nav2": nav_link("channel", "Channel Mix"),
        "p3_nav3": nav_link("conversion", "Conversion", active=True),
        "p3_nav4": nav_link("activity", "Activity"),
        "p3_hdr": hdr(
            "Lead Management — Conversion Tracking",
            "Monthly conversions, aging analysis, and pipeline velocity",
        ),
        # Filter bar
        "p3_f_source": pillbox("f_source", "Lead Source"),
        "p3_f_status": pillbox("f_status", "Status"),
        "p3_f_month": pillbox("f_month", "Created Month"),
        "p3_f_owner": pillbox("f_owner", "Owner"),
        # Column: Monthly conversions
        "p3_monthly_conv": rich_chart(
            "s_monthly_conv",
            "column",
            "Monthly Conversions",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Converted Leads",
            show_values=True,
        ),
        # Line: Avg days to convert
        "p3_monthly_days": rich_chart(
            "s_monthly_days",
            "line",
            "Avg Days to Convert (Monthly)",
            ["CreatedMonth"],
            ["avg_days"],
            axis_title="Days",
            reference_lines=[
                {"value": 30, "label": "SLA Target 30d", "color": "#D4504C"},
            ],
        ),
        # Stackhbar: Lead aging bands
        "p3_aging": rich_chart(
            "s_aging",
            "stackhbar",
            "Lead Aging Bands",
            ["AgeBand"],
            ["cnt"],
            axis_title="Count",
            show_values=True,
        ),
        # Area: Lead velocity
        "p3_velocity": rich_chart(
            "s_velocity",
            "area",
            "Lead Velocity (Monthly Creation Rate)",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Leads Created",
        ),
        # Waterfall: Monthly net leads
        "p3_waterfall": waterfall_chart(
            "s_waterfall",
            "Monthly Net Leads (Created - Converted - DQ)",
            "CreatedMonth",
            "net_leads",
            axis_label="Net Leads",
        ),
        # Section: Source Effectiveness
        "p3_sec_source_eff": section_label("Source Effectiveness Analysis"),
        # Comparison table: Source conversion performance
        "p3_ch_source_conv": compare_table(
            "s_source_eff_conv",
            "Source Conversion Performance",
            columns=["LeadSource", "total", "conv_cnt", "conv_rate", "avg_days"],
            subtitle="Volume, conversions, rate, and avg days by lead source",
            format_rules=[
                {
                    "measure": "conv_rate",
                    "ranges": [
                        {"min": 0, "max": 10, "color": "#D4504C"},
                        {"min": 10, "max": 20, "color": "#FFB75D"},
                        {"min": 20, "color": "#04844B"},
                    ],
                },
                {
                    "measure": "avg_days",
                    "ranges": [
                        {"min": 0, "max": 15, "color": "#04844B"},
                        {"min": 15, "max": 30, "color": "#FFB75D"},
                        {"min": 30, "color": "#D4504C"},
                    ],
                },
            ],
        ),
        # Area: Lead volume by source over time
        "p3_ch_source_vol": rich_chart(
            "s_source_volume",
            "area",
            "Lead Volume by Source Over Time",
            ["CreatedMonth"],
            ["cnt"],
            split=["LeadSource"],
            show_legend=True,
            axis_title="Leads",
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 4 — Activity & Engagement
        # ═══════════════════════════════════════════════════════════════
        "p4_nav1": nav_link("funnel", "MQL Funnel"),
        "p4_nav2": nav_link("channel", "Channel Mix"),
        "p4_nav3": nav_link("conversion", "Conversion"),
        "p4_nav4": nav_link("activity", "Activity", active=True),
        "p4_hdr": hdr(
            "Lead Management — Activity & Engagement",
            "Activity rates, SLA compliance, and engagement gaps",
        ),
        # Filter bar
        "p4_f_source": pillbox("f_source", "Lead Source"),
        "p4_f_status": pillbox("f_status", "Status"),
        "p4_f_month": pillbox("f_month", "Created Month"),
        "p4_f_owner": pillbox("f_owner", "Owner"),
        # Gauge: Activity rate
        "p4_bullet_activity": bullet_chart(
            "s_bullet_activity",
            "Activity Rate vs Target",
            axis_title="%",
        ),
        # Number: SLA breach count (with YoY trend — Phase 3)
        "p4_sla": num(
            "s_trend_sla",
            "breach_count",
            "SLA Breach (>2d No Activity)",
            "#D4504C",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        # Line: Activity trend
        "p4_act_trend": rich_chart(
            "s_activity_trend",
            "line",
            "Monthly Activity Trend",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Leads with Activity",
            reference_lines=[
                {"value": 50, "label": "Min Target", "color": "#FFB75D"},
            ],
        ),
        # Comparison table: High-score leads, no activity
        "p4_no_activity": compare_table(
            "s_no_activity",
            "High-Score Leads with No Activity (Top 25)",
            columns=[
                "Name",
                "Company",
                "OwnerName",
                "Status",
                "LeadScore",
                "LeadAgeDays",
            ],
            subtitle="High-score leads with zero activity -- prioritize outreach",
            format_rules=[
                {
                    "measure": "LeadScore",
                    "ranges": [
                        {"min": 0, "max": 30, "color": "#FFB75D"},
                        {"min": 30, "color": "#D4504C"},
                    ],
                },
                {
                    "measure": "LeadAgeDays",
                    "ranges": [
                        {"min": 0, "max": 30, "color": "#04844B"},
                        {"min": 30, "max": 90, "color": "#FFB75D"},
                        {"min": 90, "color": "#D4504C"},
                    ],
                },
            ],
        ),
        # ── Source Attribution & Hot Leads (AP 1.1 gaps) ──
        "p4_sec_hot": section_label("Source Attribution & Hot Leads"),
        "p4_bullet_source_attrib": bullet_chart(
            "s_bullet_source_attrib",
            "Source Attribution vs Target",
            axis_title="%",
        ),
        "p4_n_hot": num(
            "s_hot_leads",
            "cnt",
            "Hot Leads (Score > 50)",
            "#D4504C",
            compact=True,
            tier="secondary",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_n_hot_response": num(
            "s_hot_leads",
            "avg_response",
            "Hot Lead Avg Response (Days)",
            "#FF6600",
            compact=False,
            tier="tertiary",
            suffix=" days",
            widget_style=KPI_CARD_STYLE,
        ),
        "p4_ch_hot_owner": compare_table(
            "s_hot_by_owner",
            "Hot Lead Response by Owner",
            columns=["OwnerName", "cnt", "avg_response"],
            subtitle="Avg response time in days for leads with score > 50",
            format_rules=[
                {
                    "measure": "avg_response",
                    "ranges": [
                        {"min": 0, "max": 1, "color": "#04844B"},
                        {"min": 1, "max": 3, "color": "#FFB75D"},
                        {"min": 3, "color": "#D4504C"},
                    ],
                },
            ],
        ),
    }

    # ── Phase 6: Reference lines ──────────────────────────────────────────
    from crm_analytics_helpers import add_reference_line

    add_reference_line(w["p3_monthly_conv"], 10, "Target 10%", "#D4504C", "dashed")

    # ═══ V2 PAGE 5: Advanced Analytics ═══
    w["p5_nav1"] = nav_link("funnel", "MQL Funnel")
    w["p5_nav2"] = nav_link("channel", "Channel Mix")
    w["p5_nav3"] = nav_link("conversion", "Conversion")
    w["p5_nav4"] = nav_link("activity", "Activity")
    w["p5_nav5"] = nav_link("advanalytics", "Advanced", active=True)
    w["p5_hdr"] = hdr(
        "Advanced Analytics",
        "Lead Flow | Source Heatmap | Channel Trends",
    )
    w["p5_f_source"] = pillbox("f_source", "Lead Source")
    w["p5_f_status"] = pillbox("f_status", "Status")
    w["p5_f_month"] = pillbox("f_month", "Month")
    w["p5_f_owner"] = pillbox("f_owner", "Owner")
    # Sankey: Source → Status flow
    w["p5_sec_sankey"] = section_label("Lead Flow: Source → Status")
    w["p5_ch_sankey"] = sankey_chart("s_sankey_source", "Lead Source → Outcome")
    # Heatmap: Source × Month
    w["p5_sec_heatmap"] = section_label("Lead Volume Matrix")
    w["p5_ch_heatmap"] = heatmap_chart(
        "s_heatmap_source_month", "Leads by Source × Month"
    )
    # Stacked area: Source trend
    w["p5_sec_area"] = section_label("Lead Source Trends")
    w["p5_ch_area"] = area_chart(
        "s_area_source", "Monthly Leads by Source", stacked=True, show_legend=True
    )
    # Treemap: Lead volume by source
    w["p5_sec_treemap"] = section_label("Lead Volume by Source")
    w["p5_ch_treemap"] = treemap_chart(
        "s_treemap_source",
        "Lead Count by Source",
        ["LeadSource"],
        "cnt",
    )
    # Bubble: Source effectiveness
    w["p5_sec_bubble"] = section_label("Source Effectiveness (Volume vs Age)")
    w["p5_ch_bubble"] = bubble_chart(
        "s_bubble_source",
        "Lead Volume vs Avg Age by Source",
    )

    # ═══ V2 PAGE 6: Bullet Charts & Statistical Analysis ═══
    w["p6_nav1"] = nav_link("funnel", "MQL Funnel")
    w["p6_nav2"] = nav_link("channel", "Channel Mix")
    w["p6_nav3"] = nav_link("conversion", "Conversion")
    w["p6_nav4"] = nav_link("activity", "Activity")
    w["p6_nav5"] = nav_link("advanalytics", "Advanced")
    w["p6_nav6"] = nav_link("leadstats", "Statistics", active=True)
    w["p6_hdr"] = hdr(
        "Lead Statistical Analysis",
        "MQL/Conversion Targets | Source Conversion | Age Distribution",
    )
    w["p6_f_source"] = pillbox("f_source", "Source")
    w["p6_f_status"] = pillbox("f_status", "Status")
    w["p6_f_month"] = pillbox("f_month", "Month")
    w["p6_f_owner"] = pillbox("f_owner", "Owner")
    # Bullet charts
    w["p6_sec_bullet"] = section_label("Target vs Actual KPIs")
    w["p6_bullet_mql"] = bullet_chart(
        "s_bullet_mql", "MQL Rate (Target: 20%)", axis_title="%"
    )
    w["p6_bullet_conv"] = bullet_chart(
        "s_bullet_conv", "Conversion Rate (Target: 15%)", axis_title="%"
    )
    # Stats: Source conversion analysis
    w["p6_sec_source_conv"] = section_label("Conversion Rate by Lead Source")
    w["p6_stat_source_conv"] = rich_chart(
        "s_stat_source_conv",
        "hbar",
        "Conversion % by Source",
        ["LeadSource"],
        ["conv_pct"],
        axis_title="Conversion %",
        show_values=True,
    )
    # Stats: Lead age distribution
    w["p6_sec_age_dist"] = section_label("Lead Age Distribution")
    w["p6_stat_age_dist"] = rich_chart(
        "s_stat_lead_age_dist",
        "column",
        "Lead Count by Age Band",
        ["AgeBand"],
        ["cnt"],
        axis_title="Count",
        show_values=True,
    )
    # Stats: Lead Score Percentiles
    w["p6_sec_score_pct"] = section_label("Lead Score Distribution (Percentiles)")
    w["p6_tbl_score_pct"] = compare_table(
        "s_stat_score_percentiles",
        "Lead Score Percentiles",
        columns=["cnt", "mean_score", "std_dev", "p25", "median_score", "p75"],
        subtitle="Distribution of lead scores across the portfolio",
    )
    # Stats: Days-to-Convert Percentiles
    w["p6_sec_convert_pct"] = section_label("Conversion Time Distribution")
    w["p6_tbl_convert_pct"] = compare_table(
        "s_stat_convert_percentiles",
        "Days-to-Convert Percentiles",
        columns=["cnt", "mean_days", "std_dev", "p25", "median_days", "p75"],
        subtitle="Distribution of conversion velocity for converted leads",
    )
    # Stats: Cumulative Lead Volume (Running Total)
    w["p6_sec_running"] = section_label("Cumulative Lead Volume Over Time")
    w["p6_ch_running"] = area_chart(
        "s_stat_running_leads",
        "Cumulative Leads (Running Total)",
        ["CreatedMonth"],
        ["cumulative_cnt"],
        axis_title="Cumulative Count",
    )

    # ── Phase 7: Embedded table actions ──────────────────────────────────
    from crm_analytics_helpers import add_table_action

    add_table_action(w["p4_no_activity"], "salesforceActions", "Lead", "Id")

    # Keep the manager surface focused on operational funnel, conversion,
    # and action work. Advanced/statistical analysis belongs in a separate lab.
    return {
        name: widget
        for name, widget in w.items()
        if not name.startswith(("p5_", "p6_")) and "_nav" not in name
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    # ── Page 1: MQL Funnel ──
    p1 = [
        # Header
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p1_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Section: KPI Summary
        {"name": "p1_sec_kpi", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        # KPI row (3 numbers + 1 gauge)
        {"name": "p1_total", "row": 6, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_conv_rate", "row": 6, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_avg_days", "row": 6, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_mql_gauge", "row": 6, "column": 9, "colspan": 3, "rowspan": 4},
        # Funnel
        {"name": "p1_funnel", "row": 10, "column": 0, "colspan": 12, "rowspan": 8},
        # Section: Status Breakdown
        {"name": "p1_sec_status", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        # Status + DQ reasons
        {"name": "p1_status", "row": 19, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_dq", "row": 19, "column": 6, "colspan": 6, "rowspan": 8},
        # Section: MQL Trend
        {"name": "p1_sec_mql", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        # MQL trend
        {"name": "p1_mql_trend", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 2: Channel Mix ──
    p2 = [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p2_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p2_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Section: Source Overview
        {"name": "p2_sec_source", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        # Source donut + stacked area
        {"name": "p2_source", "row": 6, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_source_time", "row": 6, "column": 6, "colspan": 6, "rowspan": 8},
        # Section: Channel Effectiveness
        {"name": "p2_sec_conv", "row": 14, "column": 0, "colspan": 12, "rowspan": 1},
        # Combo: source conversion
        {"name": "p2_source_conv", "row": 15, "column": 0, "colspan": 12, "rowspan": 8},
        # Section: Campaign Performance
        {
            "name": "p2_sec_campaigns",
            "row": 23,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        # Campaigns table
        {"name": "p2_campaigns", "row": 24, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    # ── Page 3: Conversion Tracking ──
    p3 = [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p3_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p3_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Monthly conversions + avg days
        {"name": "p3_monthly_conv", "row": 5, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_monthly_days", "row": 5, "column": 6, "colspan": 6, "rowspan": 8},
        # Aging bands + velocity
        {"name": "p3_aging", "row": 13, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_velocity", "row": 13, "column": 6, "colspan": 6, "rowspan": 8},
        # Waterfall
        {"name": "p3_waterfall", "row": 21, "column": 0, "colspan": 12, "rowspan": 8},
        # Source Effectiveness section
        {
            "name": "p3_sec_source_eff",
            "row": 29,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p3_ch_source_conv",
            "row": 30,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        {
            "name": "p3_ch_source_vol",
            "row": 40,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
    ]

    # ── Page 4: Activity & Engagement ──
    p4 = [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p4_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p4_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Gauge + SLA breach
        {
            "name": "p4_bullet_activity",
            "row": 5,
            "column": 0,
            "colspan": 6,
            "rowspan": 4,
        },
        {"name": "p4_sla", "row": 5, "column": 6, "colspan": 6, "rowspan": 4},
        # Activity trend
        {"name": "p4_act_trend", "row": 9, "column": 0, "colspan": 12, "rowspan": 8},
        # No-activity table
        {
            "name": "p4_no_activity",
            "row": 17,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
        # Source Attribution & Hot Leads
        {"name": "p4_sec_hot", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {
            "name": "p4_bullet_source_attrib",
            "row": 28,
            "column": 0,
            "colspan": 4,
            "rowspan": 4,
        },
        {"name": "p4_n_hot", "row": 28, "column": 4, "colspan": 4, "rowspan": 4},
        {
            "name": "p4_n_hot_response",
            "row": 28,
            "column": 8,
            "colspan": 4,
            "rowspan": 4,
        },
        {
            "name": "p4_ch_hot_owner",
            "row": 32,
            "column": 0,
            "colspan": 12,
            "rowspan": 10,
        },
    ]

    p5 = nav_row("p5", 6) + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p5_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p5_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Sankey
        {"name": "p5_sec_sankey", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_sankey", "row": 6, "column": 0, "colspan": 12, "rowspan": 10},
        # Heatmap
        {"name": "p5_sec_heatmap", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_heatmap", "row": 17, "column": 0, "colspan": 12, "rowspan": 10},
        # Area
        {"name": "p5_sec_area", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_area", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
        # Treemap
        {"name": "p5_sec_treemap", "row": 36, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_treemap", "row": 37, "column": 0, "colspan": 12, "rowspan": 10},
        # Bubble
        {"name": "p5_sec_bubble", "row": 47, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_bubble", "row": 48, "column": 0, "colspan": 12, "rowspan": 10},
    ]

    p6 = nav_row("p6", 6) + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p6_f_source", "row": 3, "column": 0, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_status", "row": 3, "column": 3, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_month", "row": 3, "column": 6, "colspan": 3, "rowspan": 2},
        {"name": "p6_f_owner", "row": 3, "column": 9, "colspan": 3, "rowspan": 2},
        # Bullet charts
        {"name": "p6_sec_bullet", "row": 5, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_bullet_mql", "row": 6, "column": 0, "colspan": 6, "rowspan": 5},
        {"name": "p6_bullet_conv", "row": 6, "column": 6, "colspan": 6, "rowspan": 5},
        # Source conversion
        {
            "name": "p6_sec_source_conv",
            "row": 11,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_stat_source_conv",
            "row": 12,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Age distribution
        {
            "name": "p6_sec_age_dist",
            "row": 20,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_stat_age_dist",
            "row": 21,
            "column": 0,
            "colspan": 12,
            "rowspan": 8,
        },
        # Lead Score Percentiles
        {
            "name": "p6_sec_score_pct",
            "row": 29,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_tbl_score_pct",
            "row": 30,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
        # Days-to-Convert Percentiles
        {
            "name": "p6_sec_convert_pct",
            "row": 36,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {
            "name": "p6_tbl_convert_pct",
            "row": 37,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
        # Cumulative Lead Volume
        {"name": "p6_sec_running", "row": 43, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_running", "row": 44, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("funnel", "MQL Funnel", p1),
            pg("channel", "Channel Mix", p2),
            pg("conversion", "Conversion", p3),
            pg("activity", "Activity", p4),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def create_dataflow_definition():
    """Return a CRM Analytics dataflow definition for Lead_Management."""
    return {
        "Extract_Leads": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Lead",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                    {"name": "OwnerId"},
                    {"name": "Status"},
                    {"name": "LeadSource"},
                    {"name": "CreatedDate"},
                    {"name": "IsConverted"},
                    {"name": "ConvertedDate"},
                    {"name": "Email"},
                    {"name": "Company"},
                    {"name": "Rating"},
                ],
            },
        },
        "Extract_Users": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "User",
                "fields": [{"name": "Id"}, {"name": "Name"}],
            },
        },
        "Augment_Owner": {
            "action": "augment",
            "parameters": {
                "left": "Extract_Leads",
                "left_key": ["OwnerId"],
                "relationship": "Owner",
                "right": "Extract_Users",
                "right_key": ["Id"],
                "right_select": ["Name"],
            },
        },
        "Register_Dataset": {
            "action": "sfdcRegister",
            "parameters": {
                "source": "Augment_Owner",
                "name": "Lead_Management",
                "alias": "Lead_Management",
                "label": "Lead Management",
            },
        },
    }


def main():
    instance_url, token = get_auth()

    if "--create-dataflow" in sys.argv:
        print("\n=== Creating/updating dataflow ===")
        df_def = create_dataflow_definition()
        df_id = create_dataflow(instance_url, token, "DF_Lead_Management", df_def)
        if df_id and "--run-dataflow" in sys.argv:
            run_dataflow(instance_url, token, df_id)
        return

    # 1. Build and upload dataset
    ds_ok = create_dataset(instance_url, token)
    if not ds_ok:
        print("ERROR: Dataset upload failed — aborting")
        return

    # Set record navigation links via XMD
    set_record_links_xmd(
        instance_url,
        token,
        DS,
        [
            {"field": "Name", "sobject": "Lead", "id_field": "Id"},
        ],
    )

    # 2. Look up dataset ID
    ds_id = get_dataset_id(instance_url, token, DS)
    if not ds_id:
        print("ERROR: Could not find dataset ID — aborting")
        return
    print(f"  Dataset ID: {ds_id}")

    # 3. Create or find dashboard
    dashboard_id = create_dashboard_if_needed(instance_url, token, DASHBOARD_LABEL)
    print(f"  Dashboard ID: {dashboard_id}")

    # 4. Build and deploy
    steps = build_steps(ds_id)
    widgets = build_widgets()
    layout = build_layout()
    used_steps = {name for name in steps if name.startswith("f_")}
    for widget in widgets.values():
        step_name = widget.get("parameters", {}).get("step")
        if step_name:
            used_steps.add(step_name)
    steps = {name: step for name, step in steps.items() if name in used_steps}

    state = build_dashboard_state(
        steps, widgets, layout, bg_color="#F4F6F9", cell_spacing=8, row_height="fine"
    )
    deploy_dashboard(instance_url, token, dashboard_id, state)


if __name__ == "__main__":
    main()
