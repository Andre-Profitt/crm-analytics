#!/usr/bin/env python3
"""Phase 2 Workbook Builder — one Excel workbook per Sales Director.

Reads JSON cache files produced by extract_director_data.py and builds a
12-tab Excel workbook per director.

Usage:
    python3 scripts/build_director_workbooks.py --director "Dan Peppett" --snapshot-date 2026-04-10
    python3 scripts/build_director_workbooks.py --all --snapshot-date 2026-04-10
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime
from pathlib import Path

# Allow importing shared helpers from scripts/
_SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(_SCRIPTS))

import director_data_helpers as h
from md1_presets import load_md1_preset_config, find_md1_preset

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
OUTPUT_BASE = REPO_ROOT / "output" / "director_data_dumps"

# Colours
TEAL_DARK = "003E52"
TEAL_TEXT = "003E52"
WHITE = "FFFFFF"
RED_FILL = "FDE8E8"
AMBER_FILL = "FFF3E0"
GREEN_FILL = "E8F5E9"

TODAY = date.today()


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------


def safe_num(val, default=0.0):
    if val is None:
        return default
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_str(val, max_len=100):
    if val is None:
        return ""
    s = str(val)
    return s[:max_len] if len(s) > max_len else s


def nested_get(d, *keys, default=None):
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k, default)
        else:
            return default
    return d


def parse_date(val) -> date | None:
    if not val:
        return None
    s = str(val)[:10]
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        return None


def fmt_eur(val) -> str:
    n = safe_num(val)
    if n == 0:
        return "€0"
    if abs(n) >= 1_000_000:
        return f"€{n / 1_000_000:,.1f}M"
    if abs(n) >= 1_000:
        return f"€{n / 1_000:,.0f}K"
    return f"€{n:,.0f}"


def fmt_pct(val) -> str:
    return f"{safe_num(val):.1f}%"


# ---------------------------------------------------------------------------
# Style helpers
# ---------------------------------------------------------------------------


def _make_fill(hex_color: str) -> PatternFill:
    return PatternFill("solid", fgColor=hex_color)


def _header_font(bold=True, size=11, color=WHITE) -> Font:
    return Font(name="Calibri", bold=bold, size=size, color=color)


def _data_font(bold=False, size=10, color="000000") -> Font:
    return Font(name="Calibri", bold=bold, size=size, color=color)


def _title_font() -> Font:
    return Font(name="Calibri", bold=True, size=14, color=TEAL_TEXT)


def _section_font() -> Font:
    return Font(name="Calibri", bold=True, size=12, color=TEAL_TEXT)


def _write_header_row(ws, row: int, headers: list[str]) -> None:
    """Write a styled header row."""
    fill = _make_fill(TEAL_DARK)
    font = _header_font()
    for col_idx, header in enumerate(headers, start=1):
        cell = ws.cell(row=row, column=col_idx, value=header)
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(
            horizontal="center", vertical="center", wrap_text=True
        )


def _auto_width(ws, min_w=10, max_w=40) -> None:
    """Set column widths based on content."""
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                cell_len = len(str(cell.value)) if cell.value is not None else 0
                max_len = max(max_len, cell_len)
            except Exception:
                pass
        width = min(max(max_len + 2, min_w), max_w)
        ws.column_dimensions[col_letter].width = width


def _apply_row_fill(ws, row: int, n_cols: int, hex_color: str) -> None:
    fill = _make_fill(hex_color)
    for col_idx in range(1, n_cols + 1):
        ws.cell(row=row, column=col_idx).fill = fill


# ---------------------------------------------------------------------------
# Cache loader
# ---------------------------------------------------------------------------


class DirectorCache:
    def __init__(self, cache_dir: Path):
        self._dir = cache_dir

    def load(self, filename: str, default=None):
        p = self._dir / filename
        if not p.exists():
            return default if default is not None else []
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception as exc:
            print(f"  WARN: could not load {filename}: {exc}", file=sys.stderr)
            return default if default is not None else []

    @property
    def open_pipeline(self) -> list[dict]:
        return self.load("soql_open_pipeline.json")

    @property
    def won_this_quarter(self) -> list[dict]:
        return self.load("soql_won_this_quarter.json")

    @property
    def lost_this_quarter(self) -> list[dict]:
        return self.load("soql_lost_this_quarter.json")

    @property
    def won_q1(self) -> list[dict]:
        return self.load("soql_won_q1.json")

    @property
    def lost_q1(self) -> list[dict]:
        return self.load("soql_lost_q1.json")

    @property
    def pushed_deals(self) -> list[dict]:
        return self.load("soql_pushed_deals.json")

    @property
    def new_pipeline(self) -> list[dict]:
        return self.load("soql_new_pipeline.json")

    @property
    def forecast_categories(self) -> list[dict]:
        return self.load("soql_forecast_categories.json")

    @property
    def sources(self) -> list[dict]:
        return self.load("_sources.json")

    def forecast_items(self, period: str, type_: str) -> list[dict]:
        return self.load(f"forecast_item_{period}_{type_}.json")

    def field_history(self, field: str) -> list[dict]:
        return self.load(f"field_history_{field}.json")


# ---------------------------------------------------------------------------
# Tab 1: Scorecard
# ---------------------------------------------------------------------------


def build_scorecard(ws, cache: DirectorCache, director_name: str, territory: str):
    ws.title = "Scorecard"

    # Title
    ws["A1"].value = f"Sales Director Scorecard — {director_name} ({territory})"
    ws["A1"].font = _title_font()
    ws["A1"].alignment = Alignment(horizontal="left", vertical="center")
    ws.row_dimensions[1].height = 24
    ws.merge_cells("A1:B1")

    ws["A2"].value = f"Generated: {TODAY.isoformat()}"
    ws["A2"].font = _data_font(size=9, color="666666")

    current_row = 4

    def section(label: str):
        nonlocal current_row
        ws.cell(row=current_row, column=1, value=label).font = _section_font()
        ws.cell(row=current_row, column=1).fill = _make_fill("E8F4F8")
        ws.merge_cells(f"A{current_row}:B{current_row}")
        current_row += 1

    def kpi(name: str, value):
        nonlocal current_row
        ws.cell(row=current_row, column=1, value=name).font = _data_font(bold=True)
        ws.cell(row=current_row, column=2, value=value).font = _data_font()
        ws.cell(row=current_row, column=2).alignment = Alignment(horizontal="right")
        current_row += 1

    pipeline = cache.open_pipeline
    won_q = cache.won_this_quarter
    lost_q = cache.lost_this_quarter
    won_q1 = cache.won_q1
    lost_q1 = cache.lost_q1
    new_pipe = cache.new_pipeline

    # — PIPELINE HEALTH —
    section("PIPELINE HEALTH")
    total_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in pipeline)
    deal_count = len(pipeline)
    avg_deal = (total_arr / deal_count) if deal_count else 0.0
    weighted = sum(
        safe_num(r.get("APTS_Opportunity_ARR__c"))
        * safe_num(r.get("Probability"))
        / 100
        for r in pipeline
    )
    new_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in new_pipe)

    kpi("Total Open Pipeline ARR", fmt_eur(total_arr))
    kpi("Deal Count", deal_count)
    kpi("Avg Deal Size", fmt_eur(avg_deal))
    kpi("Weighted Pipeline (probability-adj)", fmt_eur(weighted))
    kpi("Coverage Ratio", "—")  # placeholder: needs quota target
    kpi("New Pipeline This Quarter", fmt_eur(new_arr))

    current_row += 1

    # — EXECUTION —
    section("EXECUTION")
    won_count = len(won_q)
    won_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in won_q)
    lost_count = len(lost_q)
    lost_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in lost_q)
    total_closed_count = won_count + lost_count
    wr_count = (won_count / total_closed_count * 100) if total_closed_count else 0.0
    total_closed_arr = won_arr + lost_arr
    wr_arr = (won_arr / total_closed_arr * 100) if total_closed_arr else 0.0

    kpi("Won This Quarter (count)", won_count)
    kpi("Won This Quarter (ARR)", fmt_eur(won_arr))
    kpi("Lost This Quarter (count)", lost_count)
    kpi("Lost This Quarter (ARR)", fmt_eur(lost_arr))
    kpi("Win Rate by Count", fmt_pct(wr_count))
    kpi("Win Rate by ARR", fmt_pct(wr_arr))

    current_row += 1

    # — RISK —
    section("RISK")
    stale_30 = [r for r in pipeline if safe_num(r.get("LastActivityInDays")) > 30]
    stale_30_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in stale_30)
    high_stale = [
        r
        for r in pipeline
        if safe_num(r.get("LastActivityInDays")) > 60
        and safe_num(r.get("APTS_Forecast_ARR__c")) >= 1_000_000
    ]
    high_stale_arr = sum(safe_num(r.get("APTS_Forecast_ARR__c")) for r in high_stale)
    pushed_5 = [r for r in pipeline if safe_num(r.get("PushCount")) >= 5]
    pushed_5_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in pushed_5)
    overdue = [
        r
        for r in pipeline
        if parse_date(r.get("CloseDate")) and parse_date(r.get("CloseDate")) < TODAY
    ]
    overdue_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in overdue)
    aging_365 = [r for r in pipeline if safe_num(r.get("AgeInDays")) > 365]
    aging_365_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in aging_365)

    kpi("Stale 30d+ (count)", len(stale_30))
    kpi("Stale 30d+ (ARR)", fmt_eur(stale_30_arr))
    kpi("High-Value Stale 60d / €1M+ (count)", len(high_stale))
    kpi("High-Value Stale 60d / €1M+ (ARR)", fmt_eur(high_stale_arr))
    kpi("Pushed 5+ (count)", len(pushed_5))
    kpi("Pushed 5+ (ARR)", fmt_eur(pushed_5_arr))
    kpi("Overdue Close (count)", len(overdue))
    kpi("Overdue Close (ARR)", fmt_eur(overdue_arr))
    kpi("Aging 365+ (count)", len(aging_365))
    kpi("Aging 365+ (ARR)", fmt_eur(aging_365_arr))

    current_row += 1

    # — PROCESS COMPLIANCE —
    section("PROCESS COMPLIANCE")

    # Stage >= "3" means stage number prefix >= 3
    def stage_ge3(r):
        stage = safe_str(r.get("StageName"))
        try:
            prefix = stage.split(" ")[0].replace("-", "").strip()
            return float(prefix) >= 3
        except (ValueError, IndexError):
            return False

    at_stage3_plus = [r for r in pipeline if stage_ge3(r)]
    approved = [r for r in at_stage3_plus if r.get("Stage_20_Approval__c") is True]
    approval_rate = (
        (len(approved) / len(at_stage3_plus) * 100) if at_stage3_plus else 0.0
    )

    missing_approval = [
        r
        for r in pipeline
        if r.get("Type") == "Land"
        and stage_ge3(r)
        and not r.get("Stage_20_Approval__c")
    ]

    kpi("Approval Rate (stage 3+)", fmt_pct(approval_rate))
    kpi("Missing Approval (Land, stage 3+)", len(missing_approval))

    # Column widths
    ws.column_dimensions["A"].width = 42
    ws.column_dimensions["B"].width = 22


# ---------------------------------------------------------------------------
# Tab 2: Pipeline Detail
# ---------------------------------------------------------------------------

PIPELINE_HEADERS = [
    "Account",
    "Opportunity",
    "Owner",
    "Stage",
    "Close Date",
    "ARR (€)",
    "ACV (€)",
    "Forecast ARR (€)",
    "Forecast Category",
    "Probability (%)",
    "Type",
    "Sub-Type",
    "Push Count",
    "Age (Days)",
    "Days In Stage",
    "Last Activity",
    "Activity Days Ago",
    "Risk Level",
    "Approval",
    "Approval Status",
    "Next Step",
    "Director Book",
    "Region",
    "Industry",
]


def build_pipeline_detail(ws, cache: DirectorCache):
    ws.title = "Pipeline Detail"
    pipeline = cache.open_pipeline

    _write_header_row(ws, 1, PIPELINE_HEADERS)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(PIPELINE_HEADERS))}1"

    n_cols = len(PIPELINE_HEADERS)
    for row_idx, r in enumerate(pipeline, start=2):
        arr = safe_num(r.get("APTS_Opportunity_ARR__c"))
        activity_days = safe_num(r.get("LastActivityInDays"))
        close_date = parse_date(r.get("CloseDate"))
        push_count = safe_num(r.get("PushCount"))

        row_data = [
            nested_get(r, "Account", "Name", default=""),
            safe_str(r.get("Name")),
            nested_get(r, "Owner", "Name", default=""),
            safe_str(r.get("StageName")),
            safe_str(r.get("CloseDate", ""))[:10],
            arr,
            safe_num(r.get("Opportunity_Average_ACV__c")),
            safe_num(r.get("APTS_Forecast_ARR__c")),
            safe_str(r.get("ForecastCategoryName")),
            safe_num(r.get("Probability")),
            safe_str(r.get("Type")),
            safe_str(r.get("APTS_Opportunity_Sub_Type__c")),
            int(push_count),
            int(safe_num(r.get("AgeInDays"))),
            int(safe_num(r.get("LastStageChangeInDays"))),
            safe_str(r.get("LastActivityDate", ""))[:10],
            int(activity_days),
            safe_str(r.get("Risk_Assessment_Level__c")),
            "Yes" if r.get("Stage_20_Approval__c") else "No",
            safe_str(r.get("Approval_Status__c")),
            safe_str(r.get("NextStep"), max_len=200),
            safe_str(r.get("Sales_Director_Book__c")),
            safe_str(r.get("Sales_Region__c")),
            nested_get(r, "Account", "Industry", default=""),
        ]

        for col_idx, val in enumerate(row_data, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.font = _data_font()

        # Conditional formatting
        is_red = (
            push_count >= 5
            or (activity_days > 60 and arr >= 1_000_000)
            or (close_date is not None and close_date < TODAY)
        )
        is_amber = activity_days > 30

        if is_red:
            _apply_row_fill(ws, row_idx, n_cols, RED_FILL)
        elif is_amber:
            _apply_row_fill(ws, row_idx, n_cols, AMBER_FILL)

    _auto_width(ws)


# ---------------------------------------------------------------------------
# Tab 3: Q1 Review
# ---------------------------------------------------------------------------


def build_q1_review(ws, cache: DirectorCache, territory: str):
    ws.title = "Q1 Review"

    row = 1

    def title(text):
        nonlocal row
        ws.cell(row=row, column=1, value=text).font = _title_font()
        row += 1

    def subheader(text, cols=6):
        nonlocal row
        ws.cell(row=row, column=1, value=text).font = _section_font()
        ws.cell(row=row, column=1).fill = _make_fill("E8F4F8")
        row += 1

    def blank():
        nonlocal row
        row += 1

    title(f"Q1 2026 Review — {territory}")
    blank()

    # — Section A: Forecast vs Actual —
    subheader("A  Forecast vs Actual (Q1 2026 — ARR)")
    headers_a = ["Category", "Forecast Amount (€)", "Adj Amount (€)"]
    _write_header_row(ws, row, headers_a)
    row += 1

    fi = cache.forecast_items("Q1_2026", "ARR")
    CATEGORIES = ["Commit", "BestCase", "Pipeline", "Closed"]
    totals = {cat: {"forecast": 0.0, "adj": 0.0} for cat in CATEGORIES}
    for item in fi:
        cat = item.get("ForecastCategoryName") or item.get(
            "ForecastingItemCategory", ""
        )
        for c in CATEGORIES:
            if c.lower() in cat.lower():
                totals[c]["forecast"] += safe_num(item.get("ForecastAmount"))
                totals[c]["adj"] += safe_num(item.get("AmountWithoutAdjustments"))
                break

    for cat in CATEGORIES:
        ws.cell(row=row, column=1, value=cat).font = _data_font()
        ws.cell(
            row=row, column=2, value=round(totals[cat]["forecast"], 2)
        ).font = _data_font()
        ws.cell(
            row=row, column=3, value=round(totals[cat]["adj"], 2)
        ).font = _data_font()
        row += 1

    # Won / Lost actuals
    won_q1 = cache.won_q1
    lost_q1 = cache.lost_q1
    won_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in won_q1)
    lost_arr = sum(safe_num(r.get("APTS_Opportunity_ARR__c")) for r in lost_q1)
    blank()
    ws.cell(row=row, column=1, value="Won Q1 (count)").font = _data_font(bold=True)
    ws.cell(row=row, column=2, value=len(won_q1)).font = _data_font()
    row += 1
    ws.cell(row=row, column=1, value="Won Q1 (ARR €)").font = _data_font(bold=True)
    ws.cell(row=row, column=2, value=round(won_arr, 2)).font = _data_font()
    row += 1
    ws.cell(row=row, column=1, value="Lost Q1 (count)").font = _data_font(bold=True)
    ws.cell(row=row, column=2, value=len(lost_q1)).font = _data_font()
    row += 1
    ws.cell(row=row, column=1, value="Lost Q1 (ARR €)").font = _data_font(bold=True)
    ws.cell(row=row, column=2, value=round(lost_arr, 2)).font = _data_font()
    row += 1
    blank()

    # — Section B: Deals Pushed Out of Q1 —
    subheader("B  Deals Pushed Out of Q1 (CloseDate moved from Q1 → Q2+)")
    headers_b = [
        "Account",
        "Opportunity",
        "Owner",
        "ARR (€)",
        "Old Close",
        "New Close",
        "Stage",
    ]
    _write_header_row(ws, row, headers_b)
    row += 1

    fh_cd = cache.field_history("CloseDate")
    q1_start = date(2026, 1, 1)
    q1_end = date(2026, 3, 31)
    q2_start = date(2026, 4, 1)

    pushed_out = []
    seen_ids = set()
    for fh in fh_cd:
        old_date = parse_date(fh.get("OldValue"))
        new_date = parse_date(fh.get("NewValue"))
        if not old_date or not new_date:
            continue
        if not (q1_start <= old_date <= q1_end):
            continue
        if new_date < q2_start:
            continue
        opp_id = fh.get("OpportunityId", "")
        if opp_id in seen_ids:
            continue
        seen_ids.add(opp_id)
        pushed_out.append(fh)

    for fh in pushed_out:
        opp = fh.get("Opportunity") or {}
        arr = safe_num(opp.get("APTS_Opportunity_ARR__c"))
        ws.cell(
            row=row, column=1, value=nested_get(opp, "Account", "Name", default="")
        ).font = _data_font()
        ws.cell(row=row, column=2, value=safe_str(opp.get("Name"))).font = _data_font()
        ws.cell(
            row=row, column=3, value=nested_get(opp, "Owner", "Name", default="")
        ).font = _data_font()
        ws.cell(row=row, column=4, value=round(arr, 2)).font = _data_font()
        ws.cell(
            row=row, column=5, value=safe_str(fh.get("OldValue", ""))[:10]
        ).font = _data_font()
        ws.cell(
            row=row, column=6, value=safe_str(fh.get("NewValue", ""))[:10]
        ).font = _data_font()
        ws.cell(
            row=row, column=7, value=safe_str(opp.get("StageName"))
        ).font = _data_font()
        row += 1

    if not pushed_out:
        ws.cell(
            row=row, column=1, value="No deals pushed out of Q1 found."
        ).font = _data_font(color="888888")
        row += 1

    blank()

    # — Section C: Forecast Category Movement —
    subheader("C  Forecast Category Movement")
    headers_c = [
        "Opportunity",
        "Owner",
        "ARR (€)",
        "Old Category",
        "New Category",
        "Date",
    ]
    _write_header_row(ws, row, headers_c)
    row += 1

    fh_fcn = cache.field_history("ForecastCategoryName")
    for fh in fh_fcn:
        opp = fh.get("Opportunity") or {}
        arr = safe_num(opp.get("APTS_Opportunity_ARR__c"))
        ws.cell(row=row, column=1, value=safe_str(opp.get("Name"))).font = _data_font()
        ws.cell(
            row=row, column=2, value=nested_get(opp, "Owner", "Name", default="")
        ).font = _data_font()
        ws.cell(row=row, column=3, value=round(arr, 2)).font = _data_font()
        ws.cell(
            row=row, column=4, value=safe_str(fh.get("OldValue"))
        ).font = _data_font()
        ws.cell(
            row=row, column=5, value=safe_str(fh.get("NewValue"))
        ).font = _data_font()
        ws.cell(
            row=row, column=6, value=safe_str(fh.get("CreatedDate", ""))[:10]
        ).font = _data_font()
        row += 1

    if not fh_fcn:
        ws.cell(
            row=row, column=1, value="No forecast category movements found."
        ).font = _data_font(color="888888")

    _auto_width(ws)


# ---------------------------------------------------------------------------
# Tab 4: Rep Performance
# ---------------------------------------------------------------------------

REP_HEADERS = [
    "Rep",
    "Open Pipeline ARR (€)",
    "Deal Count",
    "Avg Deal Size (€)",
    "Won ARR Q (€)",
    "Lost ARR Q (€)",
    "Win Rate %",
    "Stale Deals",
    "Pushed Deals",
    "Missing Approvals",
]


def build_rep_performance(ws, cache: DirectorCache):
    ws.title = "Rep Performance"

    pipeline = cache.open_pipeline
    won_q = cache.won_this_quarter
    lost_q = cache.lost_this_quarter

    reps: dict[str, dict] = {}

    def get_rep(name: str) -> dict:
        if name not in reps:
            reps[name] = {
                "pipeline_arr": 0.0,
                "deal_count": 0,
                "won_arr": 0.0,
                "lost_arr": 0.0,
                "stale": 0,
                "pushed": 0,
                "missing_approval": 0,
            }
        return reps[name]

    def stage_ge3(r):
        stage = safe_str(r.get("StageName"))
        try:
            prefix = stage.split(" ")[0].replace("-", "").strip()
            return float(prefix) >= 3
        except (ValueError, IndexError):
            return False

    for r in pipeline:
        rep = nested_get(r, "Owner", "Name", default="Unknown")
        d = get_rep(rep)
        arr = safe_num(r.get("APTS_Opportunity_ARR__c"))
        d["pipeline_arr"] += arr
        d["deal_count"] += 1
        if safe_num(r.get("LastActivityInDays")) > 30:
            d["stale"] += 1
        if safe_num(r.get("PushCount")) >= 5:
            d["pushed"] += 1
        if (
            r.get("Type") == "Land"
            and stage_ge3(r)
            and not r.get("Stage_20_Approval__c")
        ):
            d["missing_approval"] += 1

    for r in won_q:
        rep = nested_get(r, "Owner", "Name", default="Unknown")
        get_rep(rep)["won_arr"] += safe_num(r.get("APTS_Opportunity_ARR__c"))

    for r in lost_q:
        rep = nested_get(r, "Owner", "Name", default="Unknown")
        get_rep(rep)["lost_arr"] += safe_num(r.get("APTS_Opportunity_ARR__c"))

    sorted_reps = sorted(reps.items(), key=lambda x: x[1]["pipeline_arr"], reverse=True)

    _write_header_row(ws, 1, REP_HEADERS)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(REP_HEADERS))}1"

    for row_idx, (rep_name, d) in enumerate(sorted_reps, start=2):
        total_closed = d["won_arr"] + d["lost_arr"]
        wr = (d["won_arr"] / total_closed * 100) if total_closed else 0.0
        avg = (d["pipeline_arr"] / d["deal_count"]) if d["deal_count"] else 0.0

        row_data = [
            rep_name,
            round(d["pipeline_arr"], 2),
            d["deal_count"],
            round(avg, 2),
            round(d["won_arr"], 2),
            round(d["lost_arr"], 2),
            round(wr, 1),
            d["stale"],
            d["pushed"],
            d["missing_approval"],
        ]
        for col_idx, val in enumerate(row_data, start=1):
            ws.cell(row=row_idx, column=col_idx, value=val).font = _data_font()

    _auto_width(ws)


# ---------------------------------------------------------------------------
# Tab 5: Won-Lost
# ---------------------------------------------------------------------------

WON_LOST_HEADERS = [
    "Status",
    "Account",
    "Opportunity",
    "Owner",
    "Type",
    "Stage",
    "ARR (€)",
    "ACV (€)",
    "Close Date",
    "Created Date",
    "Sales Cycle (Days)",
    "Reason Won/Lost",
    "Sub-Reason",
    "Competitor",
    "Lost Comments",
]


def build_won_lost(ws, cache: DirectorCache):
    ws.title = "Won-Lost"

    all_won = {r["Id"]: r for r in cache.won_this_quarter}
    all_won.update({r["Id"]: r for r in cache.won_q1})
    all_lost = {r["Id"]: r for r in cache.lost_this_quarter}
    all_lost.update({r["Id"]: r for r in cache.lost_q1})

    _write_header_row(ws, 1, WON_LOST_HEADERS)
    ws.auto_filter.ref = f"A1:{get_column_letter(len(WON_LOST_HEADERS))}1"

    def _write_opp(row_idx: int, r: dict, status: str):
        close_date = parse_date(r.get("CloseDate"))
        created_date = parse_date(
            r.get("CreatedDate", "")[:10] if r.get("CreatedDate") else None
        )
        cycle_days = (
            (close_date - created_date).days if close_date and created_date else None
        )

        row_data = [
            status,
            nested_get(r, "Account", "Name", default=""),
            safe_str(r.get("Name")),
            nested_get(r, "Owner", "Name", default=""),
            safe_str(r.get("Type")),
            safe_str(r.get("StageName")),
            safe_num(r.get("APTS_Opportunity_ARR__c")),
            safe_num(r.get("Opportunity_Average_ACV__c")),
            safe_str(r.get("CloseDate", ""))[:10],
            safe_str(r.get("CreatedDate", ""))[:10],
            cycle_days,
            safe_str(r.get("Reason_Won_Lost__c")),
            safe_str(r.get("Sub_Reason__c")),
            safe_str(r.get("Lost_to_Competitor__c")),
            safe_str(r.get("Lost_Comments__c"), max_len=200),
        ]
        n_cols = len(WON_LOST_HEADERS)
        for col_idx, val in enumerate(row_data, start=1):
            ws.cell(row=row_idx, column=col_idx, value=val).font = _data_font()

        fill_color = GREEN_FILL if status == "Won" else RED_FILL
        _apply_row_fill(ws, row_idx, n_cols, fill_color)

    current_row = 2
    for r in all_won.values():
        _write_opp(current_row, r, "Won")
        current_row += 1

    for r in all_lost.values():
        _write_opp(current_row, r, "Lost")
        current_row += 1

    _auto_width(ws)


# ---------------------------------------------------------------------------
# Tab 6: Sources & Lineage
# ---------------------------------------------------------------------------

SOURCES_HEADERS = [
    "Source ID",
    "Source Type",
    "Name",
    "Query / Endpoint",
    "Record Count",
    "Extracted At",
]


def build_sources(ws, cache: DirectorCache):
    ws.title = "Sources & Lineage"

    sources = cache.sources
    _write_header_row(ws, 1, SOURCES_HEADERS)

    for row_idx, s in enumerate(sources, start=2):
        row_data = [
            safe_str(s.get("source_id")),
            safe_str(s.get("source_type")),
            safe_str(s.get("name")),
            safe_str(s.get("query_or_endpoint"), max_len=500),
            s.get("record_count", 0),
            safe_str(s.get("extracted_at")),
        ]
        for col_idx, val in enumerate(row_data, start=1):
            ws.cell(row=row_idx, column=col_idx, value=val).font = _data_font()

    # Wide query column
    ws.column_dimensions["D"].width = 60
    _auto_width(ws)
    ws.column_dimensions["D"].width = 60  # re-apply after auto


# ---------------------------------------------------------------------------
# Placeholder tabs
# ---------------------------------------------------------------------------

PLACEHOLDERS = [
    ("Q2 Outlook", "Q2 forecast detail — to be populated"),
    ("Commercial Approval", "Approval state + candidates from D1 — to be populated"),
    ("Renewals & Retention", "GRR/NRR from CRMA + renewals — to be populated"),
    ("Risk Register", "Composite risk score — to be populated"),
    ("Data Quality", "D2 hygiene metrics by rep — to be populated"),
    (
        "Quota & Targets",
        "Placeholder — populated when Finance delivers regional targets",
    ),
]


def build_placeholder(ws, tab_name: str, message: str):
    ws.title = tab_name
    ws["A1"].value = tab_name
    ws["A1"].font = _title_font()
    ws["A3"].value = message
    ws["A3"].font = _data_font(color="888888")


# ---------------------------------------------------------------------------
# Main workbook assembler
# ---------------------------------------------------------------------------


def build_workbook(director_name: str, territory: str, cache_dir: Path, out_path: Path):
    cache = DirectorCache(cache_dir)

    wb = Workbook()
    # Remove default sheet
    default_ws = wb.active
    wb.remove(default_ws)

    print(f"  Building: {director_name} ({territory})")

    print("    Tab 1: Scorecard")
    ws1 = wb.create_sheet("Scorecard")
    build_scorecard(ws1, cache, director_name, territory)

    print("    Tab 2: Pipeline Detail")
    ws2 = wb.create_sheet("Pipeline Detail")
    build_pipeline_detail(ws2, cache)

    print("    Tab 3: Q1 Review")
    ws3 = wb.create_sheet("Q1 Review")
    build_q1_review(ws3, cache, territory)

    print("    Tab 4: Rep Performance")
    ws4 = wb.create_sheet("Rep Performance")
    build_rep_performance(ws4, cache)

    print("    Tab 5: Won-Lost")
    ws5 = wb.create_sheet("Won-Lost")
    build_won_lost(ws5, cache)

    print("    Tab 6: Sources & Lineage")
    ws6 = wb.create_sheet("Sources & Lineage")
    build_sources(ws6, cache)

    print("    Tabs 7-12: Placeholders")
    for tab_name, message in PLACEHOLDERS:
        ws_ph = wb.create_sheet(tab_name)
        build_placeholder(ws_ph, tab_name, message)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(str(out_path))
    print(f"  Saved: {out_path}")
    return out_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Build Sales Director Excel workbooks from cache."
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--director", metavar="NAME", help='Director name e.g. "Dan Peppett"'
    )
    group.add_argument(
        "--all", action="store_true", help="Build for all directors in preset config"
    )
    parser.add_argument(
        "--snapshot-date",
        metavar="YYYY-MM-DD",
        default=TODAY.isoformat(),
        help="Snapshot date (default: today)",
    )
    args = parser.parse_args()

    snapshot_date = args.snapshot_date
    dump_dir = OUTPUT_BASE / snapshot_date
    cache_base = dump_dir / ".cache"

    config = load_md1_preset_config(REPO_ROOT / h.CONFIG_PATH)

    if args.all:
        presets = list(config.presets)
    else:
        preset = find_md1_preset(config, args.director)
        if preset is None:
            print(
                f"ERROR: Director '{args.director}' not found in preset config.",
                file=sys.stderr,
            )
            print(
                "Available directors:",
                [p.name for p in config.presets],
                file=sys.stderr,
            )
            sys.exit(1)
        presets = [preset]

    built = []
    for preset in presets:
        slug = h.slugify(preset.name)
        cache_dir = cache_base / slug
        if not cache_dir.exists():
            print(f"  SKIP: cache dir not found: {cache_dir}", file=sys.stderr)
            continue

        safe_name = preset.name.replace("/", "-").replace("\\", "-")
        out_filename = f"Sales Director Data - {safe_name} ({preset.territory}).xlsx"
        out_path = dump_dir / out_filename

        build_workbook(preset.name, preset.territory, cache_dir, out_path)
        built.append(out_path)

    print(f"\nDone. Built {len(built)} workbook(s).")
    for p in built:
        print(f"  {p}")


if __name__ == "__main__":
    main()
