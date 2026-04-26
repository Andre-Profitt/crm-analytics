#!/usr/bin/env python3
"""Shared report-surface intelligence helpers."""

from __future__ import annotations

import re
from typing import Any


def _normalize_report_signal(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _has_report_signal(values: list[Any], *tokens: str) -> bool:
    normalized_values = [_normalize_report_signal(item) for item in values]
    return any(token in value for value in normalized_values for token in tokens if value)


def cap_follow_up_fit_verdict(report_format: str | None, verdict: str) -> tuple[str, str | None]:
    normalized_format = str(report_format or "").upper()
    if normalized_format == "SUMMARY" and verdict == "strong_follow_up_fit":
        return "moderate_follow_up_fit", "summary_caps_follow_up_fit"
    if normalized_format == "MATRIX" and verdict != "weak_follow_up_fit":
        return "weak_follow_up_fit", "matrix_caps_follow_up_fit"
    return verdict, None


def assess_report_action_surface_contract(surface_contract: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(surface_contract, dict):
        return {
            "verdict": "weak_follow_up_fit",
            "overall_score": 0,
            "reasons": [],
            "warnings": [
                "surface_contract is missing, so report action-surface assessment could not run."
            ],
            "queue_ready_format": False,
        }

    report_format = str(surface_contract.get("report_format") or "").upper()
    columns = [item for item in surface_contract.get("columns", []) if isinstance(item, str)]
    filters = [item for item in surface_contract.get("filters", []) if isinstance(item, str)]
    group_by = [item for item in surface_contract.get("group_by", []) if isinstance(item, str)]
    sort_by = [item for item in surface_contract.get("sort_by", []) if isinstance(item, str)]
    handoff_target = surface_contract.get("handoff_target")
    handoff_surface = surface_contract.get("handoff_surface")

    score = 0
    reasons: list[str] = []
    warnings: list[str] = []

    if report_format == "TABULAR":
        score += 4
        reasons.append("TABULAR format keeps the report queue-first for row-level follow-up.")
    elif report_format == "SUMMARY":
        score += 3
        reasons.append(
            "SUMMARY format can support a compact follow-up path when row accountability is still visible."
        )
    elif report_format == "MATRIX":
        score += 1
        warnings.append("MATRIX format is diagnostic-heavy and usually weak as the primary follow-up queue.")
    else:
        warnings.append("Unknown report format weakens follow-up-fit confidence.")

    owner_visibility = _has_report_signal(columns + filters + group_by, "owner", "manager")
    if owner_visibility:
        score += 4
        reasons.append("Owner or manager cues are present in the packaged columns, filters, or grouping.")
    else:
        warnings.append("Report package lacks owner accountability cues in columns, filters, or grouping.")

    date_coverage = _has_report_signal(
        columns + filters,
        "date",
        "renewal",
        "close",
        "period",
        "quarter",
        "month",
    )
    if date_coverage:
        score += 3
        reasons.append("Date or renewal cues are present, so the queue can be scoped to a real operating window.")
    else:
        warnings.append("Report package lacks explicit date or renewal cues for period-scoped follow-up.")

    value_visibility = _has_report_signal(
        columns + filters,
        "amount",
        "value",
        "arr",
        "mrr",
        "forecast",
        "pipeline",
    )
    if value_visibility:
        score += 2
        reasons.append("Value or forecast cues are present for prioritization.")
    else:
        warnings.append("Report package lacks explicit value or forecast cues for prioritization.")

    risk_visibility = _has_report_signal(columns + filters, "risk", "stage", "health", "variance", "hygiene")
    if risk_visibility:
        score += 2
        reasons.append("Risk or stage cues are present, which strengthens queue triage.")
    else:
        warnings.append("Report package lacks explicit risk or stage cues for queue triage.")

    account_context = _has_report_signal(columns, "account", "opportunity", "product", "family")
    if account_context:
        score += 1
        reasons.append("Detail columns include account, opportunity, or product context for follow-up.")

    if sort_by:
        score += 2
        reasons.append("Explicit sort order is packaged, which helps push the action queue to the top.")
        if _has_report_signal(sort_by, "risk", "date", "renewal", "amount", "forecast", "owner", "manager"):
            score += 1
            reasons.append("Sort intent aligns with urgency, timing, or ownership cues.")
    else:
        warnings.append(
            "Report package lacks explicit sort order, so the action queue may not surface the right rows first."
        )

    column_count = len(columns)
    if 4 <= column_count <= 8:
        score += 1
        reasons.append("Column count stays in a scan-fast range for follow-up review.")
    elif column_count > 10:
        warnings.append("Report package exceeds 10 visible columns, which may degrade queue readability.")

    if isinstance(handoff_target, dict) and handoff_target.get("destination_type") == "dashboard":
        score += 1
        reasons.append(
            "Dashboard handoff target keeps the report in a follow-up support role instead of replacing the story surface."
        )
    elif isinstance(handoff_surface, str) and "dashboard" in handoff_surface:
        score += 1
        reasons.append("Dashboard handoff surface keeps the report in a follow-up support role.")

    raw_verdict = "weak_follow_up_fit"
    if score >= 14:
        raw_verdict = "strong_follow_up_fit"
    elif score >= 9:
        raw_verdict = "moderate_follow_up_fit"

    verdict, verdict_cap = cap_follow_up_fit_verdict(report_format, raw_verdict)
    if verdict_cap == "summary_caps_follow_up_fit":
        warnings.append("SUMMARY format can support follow-up, but it should not outrank a queue-first TABULAR report.")
    elif verdict_cap == "matrix_caps_follow_up_fit":
        warnings.append("MATRIX format is diagnostic-heavy enough that it should not be treated as a strong follow-up queue.")

    primary_surface_fit = "weak_primary_fit"
    if report_format == "TABULAR":
        primary_surface_fit = "strong_primary_fit" if verdict == "strong_follow_up_fit" else "moderate_primary_fit"
    elif report_format == "SUMMARY":
        primary_surface_fit = "limited_primary_fit"

    return {
        "report_format": report_format or None,
        "overall_score": score,
        "raw_verdict": raw_verdict,
        "verdict": verdict,
        "verdict_cap": verdict_cap,
        "owner_visibility": owner_visibility,
        "date_coverage": date_coverage,
        "value_visibility": value_visibility,
        "risk_visibility": risk_visibility,
        "account_context": account_context,
        "explicit_sort": bool(sort_by),
        "column_count": column_count,
        "queue_ready_format": report_format == "TABULAR",
        "primary_surface_fit": primary_surface_fit,
        "reasons": reasons,
        "warnings": warnings,
    }
