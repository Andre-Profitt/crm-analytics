from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _date_token(value: Any) -> str:
    return str(value or "")[:10]


@dataclass(frozen=True)
class ReportingScope:
    start_date: str
    end_date: str

    def contains(self, value: Any) -> bool:
        token = _date_token(value)
        return bool(token) and self.start_date <= token <= self.end_date


def make_reporting_scope(start_date: Any, end_date: Any) -> ReportingScope:
    return ReportingScope(
        start_date=_date_token(start_date),
        end_date=_date_token(end_date),
    )


def is_land_type(value: Any, *, type_key: str = "Type") -> bool:
    if isinstance(value, dict):
        token = value.get(type_key)
    else:
        token = value
    return str(token or "").strip().lower() == "land"


def is_real_loss(stage: Any) -> bool:
    """True for actual competitive losses, false for disqualifications."""
    s = str(stage or "").strip()
    return "Lost" in s and "No Opportunity" not in s


def is_active_forecast_category(category: Any) -> bool:
    token = str(category or "").strip()
    return token not in ("", "Omitted")


def is_approved_2026_status(status: Any) -> bool:
    token = str(status or "").strip()
    return token == "Approved 2026"


def is_pending_approval_status(status: Any) -> bool:
    token = str(status or "").strip()
    return "Conditionally" in token or "Pending" in token


def is_missing_stage3_status(status: Any) -> bool:
    token = str(status or "").strip()
    return "Missing" in token


def filter_rows_in_reporting_scope(
    rows: list[dict[str, Any]],
    scope: ReportingScope,
    *,
    date_key: str = "Close Date",
) -> list[dict[str, Any]]:
    return [row for row in rows if scope.contains(row.get(date_key))]


def filter_active_pipeline_rows(
    rows: list[dict[str, Any]],
    scope: ReportingScope,
    *,
    type_key: str = "Type",
    date_key: str = "Close Date",
    forecast_category_key: str = "Forecast Category",
) -> list[dict[str, Any]]:
    return [
        row
        for row in rows
        if is_land_type(row, type_key=type_key)
        and scope.contains(row.get(date_key))
        and is_active_forecast_category(row.get(forecast_category_key))
    ]


def summarize_approval_rows(
    rows: list[dict[str, Any]],
    *,
    status_key: str = "Status",
) -> dict[str, int]:
    approved_2026 = 0
    conditionally_approved = 0
    missing_stage3 = 0

    for row in rows:
        status = row.get(status_key)
        if is_approved_2026_status(status):
            approved_2026 += 1
        if is_pending_approval_status(status):
            conditionally_approved += 1
        if is_missing_stage3_status(status):
            missing_stage3 += 1

    return {
        "approved_2026": approved_2026,
        "conditionally_approved": conditionally_approved,
        "missing_stage3": missing_stage3,
    }
