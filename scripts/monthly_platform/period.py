from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any


@dataclass(frozen=True)
class QuarterWindow:
    label: str
    title: str
    year: int
    quarter: int
    start_date: str
    end_date: str
    month_start: str
    month_end: str
    range_label: str


@dataclass(frozen=True)
class PeriodContext:
    as_of_date: str
    reporting_month: str
    snapshot_date: str
    deck_date: str
    month_title: str
    fiscal_year: str
    current_quarter: QuarterWindow
    prior_quarter: QuarterWindow
    forward_quarter: QuarterWindow
    reporting_window_start: str
    reporting_window_end: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_MONTH_LABELS = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}

_MONTH_TITLES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def _coerce_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _last_day_of_previous_month(as_of: date) -> date:
    first_of_month = as_of.replace(day=1)
    return first_of_month - timedelta(days=1)


def _quarter_window(target: date) -> QuarterWindow:
    quarter = ((target.month - 1) // 3) + 1
    start_month = ((quarter - 1) * 3) + 1
    end_month = start_month + 2
    start = date(target.year, start_month, 1)
    end = date(target.year, end_month, monthrange(target.year, end_month)[1])
    return QuarterWindow(
        label=f"Q{quarter}",
        title=f"Q{quarter} {target.year}",
        year=target.year,
        quarter=quarter,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        month_start=f"{target.year}-{start_month:02d}",
        month_end=f"{target.year}-{end_month:02d}",
        range_label=f"{_MONTH_LABELS[start_month]}-{_MONTH_LABELS[end_month]}",
    )


def _shift_quarter(window: QuarterWindow, delta: int) -> QuarterWindow:
    quarter = window.quarter + delta
    year = window.year
    while quarter < 1:
        quarter += 4
        year -= 1
    while quarter > 4:
        quarter -= 4
        year += 1
    start_month = ((quarter - 1) * 3) + 1
    return _quarter_window(date(year, start_month, 1))


def resolve_period_context(
    *,
    as_of_date: str | date | datetime | None = None,
    snapshot_date: str | date | datetime | None = None,
    deck_date: str | date | datetime | None = None,
) -> PeriodContext:
    as_of = _coerce_date(as_of_date)
    snapshot = (
        _coerce_date(snapshot_date)
        if snapshot_date is not None
        else _last_day_of_previous_month(as_of)
    )
    deck = _coerce_date(deck_date) if deck_date is not None else snapshot

    current = _quarter_window(snapshot)
    prior = _shift_quarter(current, -1)
    forward = _shift_quarter(current, 1)

    return PeriodContext(
        as_of_date=as_of.isoformat(),
        reporting_month=snapshot.strftime("%Y-%m"),
        snapshot_date=snapshot.isoformat(),
        deck_date=deck.isoformat(),
        month_title=f"{_MONTH_TITLES[snapshot.month]} {snapshot.year}",
        fiscal_year=f"FY{snapshot.year % 100:02d}",
        current_quarter=current,
        prior_quarter=prior,
        forward_quarter=forward,
        reporting_window_start=date(snapshot.year, 1, 1).isoformat(),
        reporting_window_end=forward.end_date,
    )


def sheet_names(fy: str = "") -> dict[str, str]:
    """Canonical workbook sheet name contract.

    Returns a dict mapping semantic keys to the actual Excel sheet names.
    All consumers should use these keys instead of hardcoding sheet names.
    If fy is not provided, resolves from current date.
    """
    if not fy:
        fy = f"FY{datetime.now().year % 100:02d}"
    return {
        "pipeline_open": f"Pipeline Open {fy}",
        "won_lost": f"Won Lost {fy}",
        "renewals": f"Renewals {fy}",
        "commercial_approval": "Commercial Approval",
        "pipeline_inspection": "Pipeline Inspection",
        "q1_movement": "Q1 Movement",
        "q2_movement": "Q2 Movement",
        "activity_volume": "Activity Volume",
        "commit_items": "Commit Items",
        "stage_history": "Stage History",
        "forecast_cat_history": "Forecast Category History",
        "close_date_history": "Close Date History",
    }
