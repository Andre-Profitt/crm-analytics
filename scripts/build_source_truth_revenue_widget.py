#!/usr/bin/env python3
"""Build one source-aligned CRM Analytics widget for FY closed vs forecast call ladder.

This intentionally bypasses the stale Executive_Revenue_Forecast dataset and
builds from live Opportunity source data only.
"""

from __future__ import annotations

import csv
import io
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from crm_analytics_helpers import (  # noqa: E402
    _date,
    _measure,
    _soql,
    build_dashboard_state,
    create_dashboard_if_needed,
    deploy_dashboard,
    get_auth,
    get_dataset_id,
    pg,
    rich_chart,
    sq,
    upload_dataset,
)

DS = "FY_Closed_Forecast_Call_Ladder_Widget"
DS_LABEL = "FY Closed v Forecast Call Ladder Widget"
DASHBOARD_LABEL = "FY Closed v Forecast Call Ladder"
TODAY = date.today()
CURRENT_FY = TODAY.year
PRIOR_FY = CURRENT_FY - 1

SOQL = (
    "SELECT Id, Name, FiscalYear, CloseDate, IsWon, IsClosed, ForecastCategoryName, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedForecastARR "
    "FROM Opportunity "
    f"WHERE FiscalYear IN ({PRIOR_FY}, {CURRENT_FY}) "
    "AND CloseDate != null "
    "AND (IsWon = true OR IsClosed = false)"
)


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def create_dataset(inst: str, tok: str) -> None:
    opps = _soql(inst, tok, SOQL)
    actual_by_month = {m: 0.0 for m in range(1, 13)}
    commit_open_by_month = {m: 0.0 for m in range(1, 13)}
    best_case_open_by_month = {m: 0.0 for m in range(1, 13)}
    pipeline_open_by_month = {m: 0.0 for m in range(1, 13)}
    prior_by_month = {m: 0.0 for m in range(1, 13)}

    for opp in opps:
        close_date = opp.get("CloseDate") or ""
        if len(close_date) < 7:
            continue
        month_num = int(close_date[5:7])
        amount = _safe_float(opp.get("ConvertedForecastARR"))
        fiscal_year = int(_safe_float(opp.get("FiscalYear")))
        forecast_category = (opp.get("ForecastCategoryName") or "").strip()
        is_won = bool(opp.get("IsWon"))
        is_closed = bool(opp.get("IsClosed"))

        if fiscal_year == PRIOR_FY and is_won:
            prior_by_month[month_num] += amount
            continue

        if fiscal_year != CURRENT_FY:
            continue

        if is_won:
            actual_by_month[month_num] += amount
            continue

        if not is_closed:
            if forecast_category == "Commit":
                commit_open_by_month[month_num] += amount
            elif forecast_category == "Best Case":
                best_case_open_by_month[month_num] += amount
            elif forecast_category == "Pipeline":
                pipeline_open_by_month[month_num] += amount

    rows = []
    actual_cume = 0.0
    commit_open_cume = 0.0
    best_case_open_cume = 0.0
    pipeline_open_cume = 0.0
    prior_cume = 0.0
    for month_num in range(1, 13):
        actual_cume += actual_by_month[month_num]
        commit_open_cume += commit_open_by_month[month_num]
        best_case_open_cume += best_case_open_by_month[month_num]
        pipeline_open_cume += pipeline_open_by_month[month_num]
        prior_cume += prior_by_month[month_num]
        commit_call_cume = actual_cume + commit_open_cume
        best_case_call_cume = commit_call_cume + best_case_open_cume
        pipeline_call_cume = best_case_call_cume + pipeline_open_cume
        rows.append(
            {
                "MonthDate": f"{CURRENT_FY:04d}-{month_num:02d}-01",
                "MonthLabel": f"{CURRENT_FY:04d}-{month_num:02d}",
                "MonthNumber": month_num,
                "ActualClosedWonARR": round(actual_by_month[month_num], 2),
                "CommitOpenARR": round(commit_open_by_month[month_num], 2),
                "BestCaseOpenARR": round(best_case_open_by_month[month_num], 2),
                "PipelineOpenARR": round(pipeline_open_by_month[month_num], 2),
                "YoY10TargetARR": round(prior_by_month[month_num] * 1.10, 2),
                "ActualClosedWonCumeARR": round(actual_cume, 2),
                "CommitCallCumeARR": round(commit_call_cume, 2),
                "BestCaseCallCumeARR": round(best_case_call_cume, 2),
                "PipelineCallCumeARR": round(pipeline_call_cume, 2),
                "YoY10TargetCumeARR": round(prior_cume * 1.10, 2),
            }
        )

    buffer = io.StringIO()
    writer = csv.DictWriter(
        buffer,
        fieldnames=[
            "MonthDate",
            "MonthLabel",
            "MonthNumber",
            "ActualClosedWonARR",
            "CommitOpenARR",
            "BestCaseOpenARR",
            "PipelineOpenARR",
            "YoY10TargetARR",
            "ActualClosedWonCumeARR",
            "CommitCallCumeARR",
            "BestCaseCallCumeARR",
            "PipelineCallCumeARR",
            "YoY10TargetCumeARR",
        ],
        lineterminator="\n",
    )
    writer.writeheader()
    writer.writerows(rows)

    fields_meta = [
        _date("MonthDate", "Month"),
        {
            "fullyQualifiedName": "MonthLabel",
            "name": "MonthLabel",
            "type": "Text",
            "label": "Month Label",
        },
        _measure("MonthNumber", "Month Number", scale=0, precision=2),
        _measure("ActualClosedWonARR", f"{CURRENT_FY} Actual Closed Won ARR"),
        _measure("CommitOpenARR", f"{CURRENT_FY} Open Commit ARR"),
        _measure("BestCaseOpenARR", f"{CURRENT_FY} Open Best Case ARR"),
        _measure("PipelineOpenARR", f"{CURRENT_FY} Open Pipeline ARR"),
        _measure("YoY10TargetARR", "10% YoY Monthly Target"),
        _measure(
            "ActualClosedWonCumeARR",
            f"{CURRENT_FY} Cumulative Actual Closed Won ARR",
        ),
        _measure(
            "CommitCallCumeARR",
            f"{CURRENT_FY} Cumulative Commit Call ARR",
        ),
        _measure(
            "BestCaseCallCumeARR",
            f"{CURRENT_FY} Cumulative Best Case Call ARR",
        ),
        _measure(
            "PipelineCallCumeARR",
            f"{CURRENT_FY} Cumulative Pipeline Call ARR",
        ),
        _measure("YoY10TargetCumeARR", "10% YoY Cumulative Target"),
    ]

    if not upload_dataset(
        inst,
        tok,
        DS,
        DS_LABEL,
        fields_meta,
        buffer.getvalue().encode("utf-8"),
    ):
        raise RuntimeError("Dataset upload failed")


def build_steps() -> dict[str, dict]:
    truth_query = f"""
q = load "{DS}";
q = group q by (MonthDate, MonthLabel);
q = foreach q generate
    MonthDate,
    MonthLabel,
    sum(ActualClosedWonCumeARR) as ActualClosedWonCumeARR,
    sum(CommitCallCumeARR) as CommitCallCumeARR,
    sum(BestCaseCallCumeARR) as BestCaseCallCumeARR,
    sum(PipelineCallCumeARR) as PipelineCallCumeARR,
    sum(YoY10TargetCumeARR) as YoY10TargetCumeARR;
q = order q by MonthDate asc;
""".strip()
    return {"s_truth": sq(truth_query)}


def build_widgets() -> dict[str, dict]:
    return {
        "truth_chart": rich_chart(
            "s_truth",
            "line",
            f"FY{CURRENT_FY} Closed Won and Forecast Call Ladder",
            ["MonthLabel"],
            [
                "ActualClosedWonCumeARR",
                "CommitCallCumeARR",
                "BestCaseCallCumeARR",
                "PipelineCallCumeARR",
                "YoY10TargetCumeARR",
            ],
            show_legend=True,
            axis_title="ARR (EUR)",
            subtitle=(
                "Built directly from live Opportunity source using convertCurrency(APTS_Forecast_ARR__c). "
                "Commit/Best Case/Pipeline lines are cumulative call ladders layered on top of actual closed won by expected close month. "
                "Target line equals prior-year actuals plus 10%."
            ),
            number_format="$#,##0",
        )
    }


def build_layout() -> dict:
    return {
        "name": "Default",
        "pages": [
            pg(
                "truth",
                "Truth",
                [
                    {
                        "name": "truth_chart",
                        "row": 0,
                        "column": 0,
                        "colspan": 12,
                        "rowspan": 14,
                    }
                ],
            )
        ],
    }


def main() -> None:
    inst, tok = get_auth()
    create_dataset(inst, tok)
    if not get_dataset_id(inst, tok, DS):
        raise RuntimeError("Dataset ID lookup failed after upload")
    dashboard_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)
    state = build_dashboard_state(build_steps(), build_widgets(), build_layout())
    deploy_dashboard(inst, tok, dashboard_id, state)
    print(f"{inst}/analytics/dashboard/{dashboard_id}")


if __name__ == "__main__":
    main()
