from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from typing import Any

try:
    from monthly_platform.period import resolve_period_context
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.period import resolve_period_context


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def as_number(value: Any) -> float:
    if value in (None, ""):
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    token = (
        str(value)
        .replace(",", "")
        .replace("€", "")
        .replace("EUR", "")
        .strip()
    )
    multiplier = 1.0
    if token.endswith("%"):
        token = token[:-1]
    if token.endswith("B"):
        multiplier = 1_000_000_000
        token = token[:-1]
    elif token.endswith("M"):
        multiplier = 1_000_000
        token = token[:-1]
    elif token.endswith("K"):
        multiplier = 1_000
        token = token[:-1]
    try:
        return float(token) * multiplier
    except ValueError:
        return 0.0


def is_in_scope_forecast_category(value: Any) -> bool:
    token = as_text(value).lower()
    return bool(token) and token != "omitted"


def is_within_window(date_text: Any, start: str, end: str) -> bool:
    token = as_text(date_text)[:10]
    return bool(token) and start <= token <= end


def _window_records(
    records: list[dict[str, Any]], start: str, end: str
) -> list[dict[str, Any]]:
    return [
        record
        for record in records or []
        if is_within_window(record.get("Close Date"), start, end)
    ]


def top_active_opportunities(
    records: list[dict[str, Any]], start: str, end: str, limit: int = 10
) -> list[dict[str, Any]]:
    filtered = [
        record
        for record in _window_records(records, start, end)
        if is_in_scope_forecast_category(record.get("Forecast Category"))
    ]
    filtered.sort(
        key=lambda record: (
            -as_number(record.get("ARR (€ converted)")),
            as_text(record.get("Close Date")),
            as_text(record.get("Opportunity")),
        )
    )
    return filtered[:limit]


def _normalize_breakdown_rows(
    rows: list[dict[str, Any]] | None,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    normalized: list[dict[str, Any]] = []
    for row in rows or []:
        category = as_text(row.get("Forecast Category"))
        if not category:
            continue
        normalized_row = dict(row)
        normalized_row["Forecast Category"] = category
        normalized_row["Deal Count"] = int(round(as_number(row.get("Deal Count"))))
        normalized_row["ARR (€ converted)"] = round(
            as_number(row.get("ARR (€ converted)")), 2
        )
        normalized.append(normalized_row)
    normalized.sort(
        key=lambda row: (
            -as_number(row.get("ARR (€ converted)")),
            as_text(row.get("Forecast Category")),
        )
    )
    return normalized, {
        as_text(row.get("Forecast Category")): row for row in normalized
    }


def _rollup_breakdown_rows(
    records: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    rollup: dict[str, dict[str, Any]] = defaultdict(
        lambda: {"Forecast Category": "", "Deal Count": 0, "ARR (€ converted)": 0.0}
    )
    for record in records:
        category = as_text(record.get("Forecast Category")) or "Unspecified"
        bucket = rollup[category]
        bucket["Forecast Category"] = category
        bucket["Deal Count"] += 1
        bucket["ARR (€ converted)"] += as_number(record.get("ARR (€ converted)"))
    breakdown = list(rollup.values())
    breakdown.sort(
        key=lambda row: (
            -as_number(row.get("ARR (€ converted)")),
            as_text(row.get("Forecast Category")),
        )
    )
    for row in breakdown:
        row["ARR (€ converted)"] = round(as_number(row.get("ARR (€ converted)")), 2)
    return breakdown, {
        as_text(row.get("Forecast Category")): row for row in breakdown
    }


def _active_arr_from_categories(by_category: dict[str, dict[str, Any]]) -> float:
    return round(
        sum(
            as_number(row.get("ARR (€ converted)"))
            for category, row in (by_category or {}).items()
            if is_in_scope_forecast_category(category)
        ),
        2,
    )


def build_quarter_summary(
    *,
    label: str,
    title: str,
    start: str,
    end: str,
    records: list[dict[str, Any]],
    note: str = "",
    breakdown_rows: list[dict[str, Any]] | None = None,
    active_arr_override: Any = None,
) -> dict[str, Any]:
    window_rows = _window_records(records, start, end)
    if breakdown_rows:
        breakdown, by_category = _normalize_breakdown_rows(breakdown_rows)
    else:
        breakdown, by_category = _rollup_breakdown_rows(window_rows)
    active_rows = [
        row
        for row in window_rows
        if is_in_scope_forecast_category(row.get("Forecast Category"))
    ]
    active_deal_count = len(active_rows)
    if active_deal_count == 0 and by_category:
        active_deal_count = int(
            round(
                sum(
                    as_number(row.get("Deal Count"))
                    for category, row in by_category.items()
                    if is_in_scope_forecast_category(category)
                )
            )
        )
    return {
        "label": label,
        "title": title,
        "start_date": start,
        "end_date": end,
        "note": note,
        "breakdown": breakdown,
        "by_category": by_category,
        "top_active_opportunities": top_active_opportunities(records, start, end),
        "active_deal_count": active_deal_count,
        "active_arr": round(
            as_number(active_arr_override)
            if active_arr_override not in (None, "")
            else _active_arr_from_categories(by_category),
            2,
        ),
    }


def quarterly_pipeline_display_from_snapshot(
    snapshot: dict[str, Any],
) -> dict[str, Any]:
    snapshot_date = as_text(snapshot.get("snapshot_date"))
    if not snapshot_date:
        return {
            "current_quarter": {},
            "forward_quarter": {},
            "display_quarter": {},
        }

    period = resolve_period_context(
        as_of_date=snapshot_date,
        snapshot_date=snapshot_date,
        deck_date=snapshot_date,
    )
    records = ((snapshot.get("pipeline_detail") or {}).get("records") or [])
    pipeline_metrics = (
        (((snapshot.get("scorecard") or {}).get("sections") or {}).get("pipeline-health"))
        or {}
    ).get("metrics") or {}
    current_metric_key = (
        f"Pipeline ARR — {period.current_quarter.label} {period.current_quarter.year} "
        "Close Dates Only (excl. Omitted)"
    )
    current_outlook = snapshot.get("q2_outlook") or {}

    current_quarter = build_quarter_summary(
        label=period.current_quarter.label,
        title=period.current_quarter.title,
        start=period.current_quarter.start_date,
        end=period.current_quarter.end_date,
        records=records,
        note=as_text(current_outlook.get("note")),
        breakdown_rows=current_outlook.get("breakdown") or [],
        active_arr_override=pipeline_metrics.get(current_metric_key),
    )
    forward_quarter = build_quarter_summary(
        label=period.forward_quarter.label,
        title=period.forward_quarter.title,
        start=period.forward_quarter.start_date,
        end=period.forward_quarter.end_date,
        records=records,
    )

    display_quarter = deepcopy(current_quarter)
    if current_quarter.get("active_deal_count", 0) > 0:
        reason = "current_quarter"
        footnote = ""
    elif forward_quarter.get("active_deal_count", 0) > 0:
        display_quarter = deepcopy(forward_quarter)
        reason = "forward_quarter_fallback"
        footnote = (
            f"No {current_quarter['title']} in-scope pipeline; "
            f"showing {forward_quarter['title']} forward-quarter outlook."
        )
    else:
        reason = "empty_current_and_forward"
        footnote = (
            f"No {current_quarter['title']} in-scope pipeline and no "
            f"{forward_quarter['title']} forward-quarter pipeline."
        )
    display_quarter["reason"] = reason
    display_quarter["footnote"] = footnote

    return {
        "current_quarter": current_quarter,
        "forward_quarter": forward_quarter,
        "display_quarter": display_quarter,
    }
