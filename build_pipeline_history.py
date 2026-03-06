#!/usr/bin/env python3
"""Build pipeline history datasets and add Pipeline History page to Opp Mgmt dashboard.

Phase 4 of the interactivity upgrade:
  4A: Opp_History dataset from OpportunityHistory (stage changes, amount deltas)
  4B: Pipeline snapshot dataflow (append-mode daily snapshots)
  4C: Pipeline History widgets (line, comparison table, waterfall, hbar, area)
  4D: Opp_Field_History dataset from OpportunityFieldHistory (CloseDate/Stage/Amount changes)

Datasets:
  - Opp_History (from OpportunityHistory SOQL)
  - Opp_Field_History (from OpportunityFieldHistory SOQL)
  - Pipeline_Snapshots (from daily append dataflow)
"""

import csv
import io
from datetime import datetime

from crm_analytics_helpers import (
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    create_dataflow,
    sq,
    num,
    rich_chart,
    hdr,
    section_label,
    nav_link,
    pg,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    coalesce_filter,
    pillbox,
    set_security_predicate,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

HISTORY_DS = "Opp_History"
HISTORY_DS_LABEL = "Opportunity History"
FIELD_HISTORY_DS = "Opp_Field_History"
FIELD_HISTORY_DS_LABEL = "Opportunity Field History"
SNAPSHOT_DS = "Pipeline_Snapshots"
DASHBOARD_LABEL = "Pipeline History"

# ── Coalesce filter bindings ──────────────────────────────────────────────
SMF = coalesce_filter("f_stage", "StageName")  # Stage filter
MF = coalesce_filter("f_month", "CreatedMonth")  # Month filter

# Stage ordering for velocity calculations
STAGE_ORDER = {
    "Stage 1 - Prospect Qualification": 1,
    "Stage 1.5 - Technical Qualification": 2,
    "Stage 2 - Approval": 3,
    "Stage 3 - Discovery": 4,
    "Stage 4 - Solution Validation": 5,
    "Stage 5 - Negotiation": 6,
    "Stage 6 - Verbal Agreement": 7,
    "Stage 7 - Closed Won": 8,
    "Closed Lost": 0,
}

TODAY = datetime.utcnow().strftime("%Y-%m-%d")


# ═══════════════════════════════════════════════════════════════════════════
#  4A: OpportunityHistory Dataset
# ═══════════════════════════════════════════════════════════════════════════


def create_history_dataset(inst, tok):
    """Query OpportunityHistory, compute stage transitions, upload dataset."""
    print("\n=== Building Opp_History dataset ===")

    records = _soql(
        inst,
        tok,
        "SELECT OpportunityId, StageName, Amount, CloseDate, CreatedDate "
        "FROM OpportunityHistory "
        "WHERE CreatedDate >= 2025-01-01T00:00:00Z "
        "ORDER BY OpportunityId, CreatedDate ASC",
    )
    print(f"  Queried {len(records)} history records")

    fields = [
        "OpportunityId",
        "StageName",
        "StageNumber",
        "Amount",
        "CloseDate",
        "CreatedDate",
        "CreatedMonth",
        "DaysInStage",
        "IsBackward",
        "AmountDelta",
        "PrevStage",
        "PrevAmount",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    # Group by OpportunityId to compute deltas
    prev_by_opp = {}
    for r in records:
        opp_id = r.get("OpportunityId", "")
        stage = r.get("StageName") or ""
        amount = r.get("Amount") or 0
        close_date = (r.get("CloseDate") or "")[:10]
        created = (r.get("CreatedDate") or "")[:19]
        created_date = created[:10]
        created_month = created[:7]

        stage_num = STAGE_ORDER.get(stage, 0)
        prev = prev_by_opp.get(opp_id, {})
        prev_stage = prev.get("stage", "")
        prev_stage_num = prev.get("stage_num", 0)
        prev_amount = prev.get("amount", 0)
        prev_created = prev.get("created", "")

        # Compute days in previous stage
        days_in_stage = 0
        if prev_created and created_date:
            try:
                d1 = datetime.strptime(prev_created[:10], "%Y-%m-%d")
                d2 = datetime.strptime(created_date, "%Y-%m-%d")
                days_in_stage = max(0, (d2 - d1).days)
            except ValueError:
                pass

        is_backward = (
            "true" if stage_num < prev_stage_num and prev_stage_num > 0 else "false"
        )
        amount_delta = round(amount - prev_amount, 2)

        writer.writerow(
            {
                "OpportunityId": opp_id,
                "StageName": stage,
                "StageNumber": stage_num,
                "Amount": amount,
                "CloseDate": close_date,
                "CreatedDate": created_date,
                "CreatedMonth": created_month,
                "DaysInStage": days_in_stage,
                "IsBackward": is_backward,
                "AmountDelta": amount_delta,
                "PrevStage": prev_stage,
                "PrevAmount": prev_amount,
            }
        )

        prev_by_opp[opp_id] = {
            "stage": stage,
            "stage_num": stage_num,
            "amount": amount,
            "created": created,
        }

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes")

    fields_meta = [
        _dim("OpportunityId", "Opportunity ID"),
        _dim("StageName", "Stage Name"),
        _measure("StageNumber", "Stage Number", scale=0, precision=3),
        _measure("Amount", "Amount"),
        _date("CloseDate", "Close Date"),
        _date("CreatedDate", "Created Date"),
        _dim("CreatedMonth", "Created Month"),
        _measure("DaysInStage", "Days in Stage", scale=0, precision=6),
        _dim("IsBackward", "Is Backward"),
        _measure("AmountDelta", "Amount Delta"),
        _dim("PrevStage", "Previous Stage"),
        _measure("PrevAmount", "Previous Amount"),
    ]

    return upload_dataset(
        inst, tok, HISTORY_DS, HISTORY_DS_LABEL, fields_meta, csv_bytes
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4D: OpportunityFieldHistory Dataset
# ═══════════════════════════════════════════════════════════════════════════


def create_field_history_dataset(inst, tok):
    """Query OpportunityFieldHistory for CloseDate/Stage/Amount/ForecastCategory changes."""
    print("\n=== Building Opp_Field_History dataset ===")

    records = _soql(
        inst,
        tok,
        "SELECT OpportunityId, Field, OldValue, NewValue, CreatedDate "
        "FROM OpportunityFieldHistory "
        "WHERE CreatedDate >= 2025-06-01T00:00:00Z "
        "AND Field IN ('StageName', 'CloseDate', 'Amount', 'ForecastCategoryName')",
    )
    print(f"  Queried {len(records)} field history records")

    fields = [
        "OpportunityId",
        "Field",
        "OldValue",
        "NewValue",
        "CreatedDate",
        "CreatedMonth",
        "IsCloseDatePush",
        "CloseDateDeltaDays",
    ]

    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fields, lineterminator="\n")
    writer.writeheader()

    for r in records:
        field = r.get("Field") or ""
        old_val = str(r.get("OldValue") or "")
        new_val = str(r.get("NewValue") or "")
        created = (r.get("CreatedDate") or "")[:10]
        created_month = created[:7]

        # Detect CloseDate pushes
        is_push = "false"
        delta_days = 0
        if field == "CloseDate" and old_val and new_val:
            try:
                old_dt = datetime.strptime(old_val[:10], "%Y-%m-%d")
                new_dt = datetime.strptime(new_val[:10], "%Y-%m-%d")
                delta_days = (new_dt - old_dt).days
                is_push = "true" if delta_days > 0 else "false"
            except ValueError:
                pass

        writer.writerow(
            {
                "OpportunityId": r.get("OpportunityId", ""),
                "Field": field,
                "OldValue": old_val[:255],
                "NewValue": new_val[:255],
                "CreatedDate": created,
                "CreatedMonth": created_month,
                "IsCloseDatePush": is_push,
                "CloseDateDeltaDays": delta_days,
            }
        )

    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"  CSV: {len(csv_bytes):,} bytes")

    fields_meta = [
        _dim("OpportunityId", "Opportunity ID"),
        _dim("Field", "Field Changed"),
        _dim("OldValue", "Old Value"),
        _dim("NewValue", "New Value"),
        _date("CreatedDate", "Created Date"),
        _dim("CreatedMonth", "Created Month"),
        _dim("IsCloseDatePush", "Is Close Date Push"),
        _measure("CloseDateDeltaDays", "Close Date Delta (Days)", scale=0, precision=6),
    ]

    return upload_dataset(
        inst, tok, FIELD_HISTORY_DS, FIELD_HISTORY_DS_LABEL, fields_meta, csv_bytes
    )


# ═══════════════════════════════════════════════════════════════════════════
#  4B: Pipeline Snapshot Dataflow (append mode)
# ═══════════════════════════════════════════════════════════════════════════


def create_snapshot_dataflow(inst, tok):
    """Create DF_Pipeline_Snapshot dataflow for daily append-mode snapshots."""
    print("\n=== Creating Pipeline Snapshot dataflow ===")

    definition = {
        "Extract_Opportunities": {
            "action": "sfdcDigest",
            "parameters": {
                "object": "Opportunity",
                "fields": [
                    {"name": "Id"},
                    {"name": "Name"},
                    {"name": "StageName"},
                    {"name": "APTS_Forecast_ARR__c"},
                    {"name": "ForecastCategoryName"},
                    {"name": "Owner.Name"},
                    {"name": "Account_Unit_Group__c"},
                    {"name": "IsClosed"},
                    {"name": "CloseDate"},
                ],
                "filterConditions": [
                    {
                        "field": "IsClosed",
                        "operator": "EqualTo",
                        "value": "false",
                    },
                    {
                        "field": "FiscalYear",
                        "operator": "GreaterThanOrEqualTo",
                        "value": "2025",
                    },
                ],
            },
        },
        "Add_Snapshot_Date": {
            "action": "computeExpression",
            "parameters": {
                "source": "Extract_Opportunities",
                "mergeWithSource": True,
                "computedFields": [
                    {
                        "name": "SnapshotDate",
                        "type": "Text",
                        "label": "Snapshot Date",
                        "saqlExpression": "now()",
                    },
                    {
                        "name": "SnapshotMonth",
                        "type": "Text",
                        "label": "Snapshot Month",
                        "saqlExpression": "substr(string(now()), 1, 7)",
                    },
                ],
            },
        },
        "Register_Snapshots": {
            "action": "sfdcRegister",
            "parameters": {
                "source": "Add_Snapshot_Date",
                "alias": SNAPSHOT_DS,
                "name": SNAPSHOT_DS,
                "label": "Pipeline Snapshots",
                "operation": "Append",
            },
        },
    }

    df_id = create_dataflow(inst, tok, "DF_Pipeline_Snapshot", definition)
    return df_id


# ═══════════════════════════════════════════════════════════════════════════
#  4C: Steps & Widgets for Pipeline History page
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_meta=None):
    """Build SAQL steps for the Pipeline History dashboard/page."""
    from crm_analytics_helpers import af

    HL = f'q = load "{HISTORY_DS}";\n'
    FL = f'q = load "{FIELD_HISTORY_DS}";\n'

    # Default ds_meta if not provided (filters will be basic)
    if ds_meta is None:
        ds_meta = [{"name": HISTORY_DS}]

    return {
        # ── Filter steps (aggregateflex) ──────────────────────────────────
        "f_stage": af("StageName", ds_meta),
        "f_month": af("CreatedMonth", ds_meta),
        # ── Stage velocity: avg days per stage (groups by StageName → skip SMF) ──
        "s_stage_velocity": sq(
            HL
            + MF
            + "q = filter q by DaysInStage > 0;\n"
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, "
            + "avg(DaysInStage) as avg_days, count() as transitions;\n"
            + "q = order q by avg_days desc;"
        ),
        # ── Backward stage movements by month (groups by CreatedMonth → skip MF) ──
        "s_backward_trend": sq(
            HL
            + SMF
            + 'q = filter q by IsBackward == "true";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, count() as cnt;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ── Amount change distribution ──
        "s_amount_changes": sq(
            HL
            + SMF
            + MF
            + "q = filter q by AmountDelta != 0;\n"
            + "q = foreach q generate "
            + '(case when AmountDelta > 0 then "Increase" '
            + 'when AmountDelta < 0 then "Decrease" '
            + 'else "No Change" end) as Direction, '
            + "abs(AmountDelta) as AbsDelta;\n"
            + "q = group q by Direction;\n"
            + "q = foreach q generate Direction, count() as cnt, "
            + "sum(AbsDelta) as total_delta;\n"
            + "q = order q by cnt desc;"
        ),
        # ── Close date push analysis (FL dataset, groups by CreatedMonth → skip MF; no StageName → skip SMF) ──
        "s_push_trend": sq(
            FL
            + 'q = filter q by Field == "CloseDate";\n'
            + 'q = filter q by IsCloseDatePush == "true";\n'
            + "q = group q by CreatedMonth;\n"
            + "q = foreach q generate CreatedMonth, "
            + "count() as push_count, "
            + "avg(CloseDateDeltaDays) as avg_push_days;\n"
            + "q = order q by CreatedMonth asc;"
        ),
        # ── Field change frequency by type (FL dataset, no StageName → skip SMF) ──
        "s_field_changes": sq(
            FL
            + MF
            + "q = group q by Field;\n"
            + "q = foreach q generate Field, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # ── Stage transition matrix (groups by StageName → skip SMF) ──
        "s_stage_transitions": sq(
            HL
            + MF
            + 'q = filter q by PrevStage != "";\n'
            + "q = group q by (PrevStage, StageName);\n"
            + "q = foreach q generate PrevStage, StageName, count() as cnt;\n"
            + "q = order q by cnt desc;\n"
            + "q = limit q 25;"
        ),
        # ── KPI: Total backward moves ──
        "s_backward_total": sq(
            HL
            + SMF
            + MF
            + 'q = filter q by IsBackward == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # ── KPI: Avg days in stage (all transitions) ──
        "s_avg_days_stage": sq(
            HL
            + SMF
            + MF
            + "q = filter q by DaysInStage > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(DaysInStage) as avg_days;"
        ),
        # ── KPI: Total close date pushes (FL dataset, no StageName → skip SMF) ──
        "s_push_total": sq(
            FL
            + MF
            + 'q = filter q by IsCloseDatePush == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt, "
            + "avg(CloseDateDeltaDays) as avg_push;"
        ),
        # ── KPI: Total field changes (FL dataset, no StageName → skip SMF) ──
        "s_changes_total": sq(
            FL + MF + "q = group q by all;\n" + "q = foreach q generate count() as cnt;"
        ),
    }


def build_widgets():
    """Build widgets for the Pipeline History dashboard/page."""
    return {
        # Nav
        "p1_nav1": nav_link("history", "Pipeline History", active=True),
        # Header
        "p1_hdr": hdr(
            "Pipeline History & Velocity",
            "Stage progression, close date changes, and pipeline movement analysis",
        ),
        # Filters
        "p1_f_stage": pillbox("f_stage", "Stage"),
        "p1_f_month": pillbox("f_month", "Month"),
        # Hero KPIs
        "p1_avg_days": num(
            "s_avg_days_stage", "avg_days", "Avg Days in Stage", "#0070D2"
        ),
        "p1_backward": num("s_backward_total", "cnt", "Backward Moves", "#D4504C"),
        "p1_pushes": num("s_push_total", "cnt", "Close Date Pushes", "#FFB75D"),
        "p1_changes": num("s_changes_total", "cnt", "Total Field Changes", "#54698D"),
        # Section: Stage Velocity
        "p1_sec_velocity": section_label("Stage Velocity"),
        "p1_ch_velocity": rich_chart(
            "s_stage_velocity",
            "hbar",
            "Average Days per Stage",
            ["StageName"],
            ["avg_days"],
            axis_title="Days",
        ),
        "p1_ch_backward": rich_chart(
            "s_backward_trend",
            "column",
            "Backward Stage Moves by Month",
            ["CreatedMonth"],
            ["cnt"],
            axis_title="Count",
        ),
        # Section: Close Date Analysis
        "p1_sec_push": section_label("Close Date Push Analysis"),
        "p1_ch_push": rich_chart(
            "s_push_trend",
            "column",
            "Close Date Pushes by Month",
            ["CreatedMonth"],
            ["push_count"],
            axis_title="Pushes",
        ),
        "p1_ch_fields": rich_chart(
            "s_field_changes",
            "donut",
            "Field Change Distribution",
            ["Field"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        # Section: Transitions
        "p1_sec_trans": section_label("Stage Transition Patterns"),
        "p1_ch_amount": rich_chart(
            "s_amount_changes",
            "donut",
            "Amount Change Direction",
            ["Direction"],
            ["cnt"],
            show_legend=True,
            show_pct=True,
        ),
        "p1_tbl_trans": rich_chart(
            "s_stage_transitions",
            "comparisontable",
            "Top Stage Transitions (From → To)",
            ["PrevStage", "StageName"],
            ["cnt"],
        ),
    }


def build_layout():
    """Layout for the Pipeline History page."""
    p1 = [
        {"name": "p1_nav1", "row": 0, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # Filter bar
        {"name": "p1_f_stage", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p1_f_month", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # Hero KPIs (+2 rows)
        {"name": "p1_avg_days", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p1_backward", "row": 5, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p1_pushes", "row": 5, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p1_changes", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Stage Velocity
        {"name": "p1_sec_velocity", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_velocity", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_backward", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
        # Close Date Analysis
        {"name": "p1_sec_push", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_push", "row": 19, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_fields", "row": 19, "column": 6, "colspan": 6, "rowspan": 8},
        # Transitions
        {"name": "p1_sec_trans", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_amount", "row": 28, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_tbl_trans", "row": 28, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("history", "Pipeline History", p1),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    inst, tok = get_auth()

    # 4A: Build OpportunityHistory dataset
    history_ok = create_history_dataset(inst, tok)
    if not history_ok:
        print("WARNING: Opp_History dataset failed")

    if history_ok:
        set_security_predicate(inst, tok, HISTORY_DS)

    # 4D: Build OpportunityFieldHistory dataset
    field_ok = create_field_history_dataset(inst, tok)
    if not field_ok:
        print("WARNING: Opp_Field_History dataset failed")

    # 4B: Create snapshot dataflow (won't run it — needs scheduling)
    try:
        df_id = create_snapshot_dataflow(inst, tok)
        print(f"  Snapshot dataflow ready: {df_id}")
        print("  NOTE: Schedule this dataflow to run daily via CRM Analytics UI")
    except Exception as e:
        print(f"WARNING: Snapshot dataflow creation failed: {e}")

    # 4C: Deploy Pipeline History dashboard
    if history_ok or field_ok:
        from crm_analytics_helpers import get_dataset_id

        dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
        ds_id = get_dataset_id(inst, tok, HISTORY_DS)
        ds_meta = (
            [{"id": ds_id, "name": HISTORY_DS}] if ds_id else [{"name": HISTORY_DS}]
        )
        state = build_dashboard_state(
            build_steps(ds_meta), build_widgets(), build_layout()
        )
        deploy_dashboard(inst, tok, dash_id, state)
    else:
        print("ERROR: No datasets available — skipping dashboard deployment")


if __name__ == "__main__":
    main()
