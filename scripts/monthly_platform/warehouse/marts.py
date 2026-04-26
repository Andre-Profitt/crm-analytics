"""Track H — pure functions that materialize each warehouse table.

Each ``build_*`` function takes the raw JSON evidence the upstream pipeline
already produces and returns a list of dicts (rows) plus the column order.
None of these functions touches the filesystem — :mod:`writer` does that.
Keeping the build pure makes parity assertion trivial: the row count of
each function's output equals the row count the writer persists, and both
equal the row count visible in the JSON evidence.

Issue-prefix routing (Track B vs Track D)
-----------------------------------------
The audit's ``findings`` list mixes Track B/C/D output. We split into two
staged tables by issue prefix so analysts can query each track without
re-deriving the split:

* ``source_distribution_*``  -> ``staged_distribution_findings``
* everything else            -> ``staged_source_quality_findings``

Both tables carry an explicit ``track`` column so the split is self-describing.
"""

from __future__ import annotations

from collections import Counter
from typing import Any


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _track_for_issue(issue: str) -> str:
    if not issue:
        return "unknown"
    if issue.startswith("source_distribution_"):
        return "D"
    if issue.startswith("source_quality_baseline_"):
        return "C"
    if issue.startswith("source_extract_") or issue.startswith("source_required_"):
        return "B"
    if issue.startswith("source_row_count_") or issue.startswith(
        "source_required_field_"
    ):
        return "B"
    return "B"


def _row_count_policy_summary(policy: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(policy, dict):
        return {"allow_zero": None, "min_rows": None, "max_rows": None}
    return {
        "allow_zero": bool(policy.get("allow_zero", True)),
        "min_rows": int(policy.get("min_rows", 0))
        if policy.get("min_rows") is not None
        else None,
        "max_rows": int(policy["max_rows"])
        if policy.get("max_rows") is not None
        else None,
    }


# ---------------------------------------------------------------------------
# Raw — direct evidence mirrors
# ---------------------------------------------------------------------------


RAW_EXTRACT_PLAN_COLUMNS = [
    "snapshot_date",
    "run_id",
    "requirement_id",
    "source_system",
    "source_type",
    "salesforce_object",
    "dataset",
    "output_grain",
    "scope",
    "territory",
    "director",
    "region",
    "period_role",
    "quarter_label",
    "source_id",
    "source_label",
    "status",
]


def build_raw_salesforce_extract_plan(
    *,
    plan: dict[str, Any],
    run_id: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    snapshot_date = str(plan.get("snapshot_date") or "")
    for item in plan.get("items", []) or []:
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "run_id": run_id,
                "requirement_id": str(item.get("requirement_id") or ""),
                "source_system": str(item.get("source_system") or ""),
                "source_type": str(item.get("source_type") or ""),
                "salesforce_object": str(item.get("salesforce_object") or ""),
                "dataset": str(item.get("dataset") or ""),
                "output_grain": str(item.get("output_grain") or ""),
                "scope": str(item.get("scope") or ""),
                "territory": item.get("territory"),
                "director": item.get("director"),
                "region": item.get("region"),
                "period_role": str(item.get("period_role") or ""),
                "quarter_label": str(item.get("quarter_label") or ""),
                "source_id": str(item.get("source_id") or ""),
                "source_label": str(item.get("source_label") or ""),
                "status": str(item.get("status") or ""),
            }
        )
    rows.sort(
        key=lambda r: (
            r["requirement_id"],
            r["territory"] or "",
            r["period_role"],
            r["source_id"],
        )
    )
    return rows


RAW_SOURCE_QUALITY_AUDIT_COLUMNS = [
    "snapshot_date",
    "run_id",
    "source_key",
    "status",
    "requirement_id",
    "dataset",
    "source_type",
    "salesforce_id",
    "label",
    "territory",
    "director",
    "period_role",
    "quarter_label",
    "row_count",
    "row_count_status",
    "required_field_count",
    "required_fields_present_count",
    "missing_required_fields_count",
    "finding_count",
    "high_finding_count",
    "medium_finding_count",
    "quality_hash",
]


def build_raw_source_quality_audit(audit: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    snapshot_date = str(audit.get("snapshot_date") or "")
    run_id = str(audit.get("run_id") or "")
    for src in audit.get("sources", []) or []:
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "run_id": run_id,
                "source_key": str(src.get("source_key") or ""),
                "status": str(src.get("status") or ""),
                "requirement_id": str(src.get("requirement_id") or ""),
                "dataset": str(src.get("dataset") or ""),
                "source_type": str(src.get("source_type") or ""),
                "salesforce_id": str(src.get("salesforce_id") or ""),
                "label": str(src.get("label") or ""),
                "territory": src.get("territory"),
                "director": src.get("director"),
                "period_role": str(src.get("period_role") or ""),
                "quarter_label": str(src.get("quarter_label") or ""),
                "row_count": int(src.get("row_count") or 0),
                "row_count_status": str(src.get("row_count_status") or ""),
                "required_field_count": int(src.get("required_field_count") or 0),
                "required_fields_present_count": len(
                    src.get("required_fields_present") or []
                ),
                "missing_required_fields_count": len(
                    src.get("missing_required_fields") or []
                ),
                "finding_count": int(src.get("finding_count") or 0),
                "high_finding_count": int(src.get("high_finding_count") or 0),
                "medium_finding_count": int(src.get("medium_finding_count") or 0),
                "quality_hash": str(src.get("quality_hash") or ""),
            }
        )
    rows.sort(key=lambda r: r["source_key"])
    return rows


# ---------------------------------------------------------------------------
# Staged — flattened, typed projections
# ---------------------------------------------------------------------------


STAGED_SOURCE_REQUIREMENTS_COLUMNS = [
    "requirement_id",
    "enabled",
    "owner",
    "source_system",
    "source_type",
    "dataset",
    "output_grain",
    "scope",
    "allow_zero",
    "min_rows",
    "max_rows",
    "has_distribution_policy",
    "distribution_dimension_count",
    "slice_sentinel_count",
    "fallback_policy_present",
    "tag_count",
]


def build_staged_source_requirements(registry: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for req in registry.get("requirements", []) or []:
        rcp = _row_count_policy_summary(req.get("row_count_policy"))
        dist = req.get("distribution_policy")
        rows.append(
            {
                "requirement_id": str(req.get("requirement_id") or ""),
                "enabled": bool(req.get("enabled", True)),
                "owner": str(req.get("owner") or ""),
                "source_system": str(req.get("source_system") or ""),
                "source_type": str(req.get("source_type") or ""),
                "dataset": str(req.get("dataset") or ""),
                "output_grain": str(req.get("output_grain") or ""),
                "scope": str(req.get("scope") or ""),
                "allow_zero": rcp["allow_zero"],
                "min_rows": rcp["min_rows"],
                "max_rows": rcp["max_rows"],
                "has_distribution_policy": dist is not None,
                "distribution_dimension_count": (
                    len(dist.get("dimensions") or []) if isinstance(dist, dict) else 0
                ),
                "slice_sentinel_count": (
                    len(dist.get("slice_sentinels") or [])
                    if isinstance(dist, dict)
                    else 0
                ),
                "fallback_policy_present": req.get("fallback_policy") is not None,
                "tag_count": len(req.get("tags") or []),
            }
        )
    rows.sort(key=lambda r: r["requirement_id"])
    return rows


STAGED_FINDING_COLUMNS = [
    "snapshot_date",
    "run_id",
    "track",
    "severity",
    "issue",
    "evidence",
    "owner",
]


def _findings_rows(audit: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot_date = str(audit.get("snapshot_date") or "")
    run_id = str(audit.get("run_id") or "")
    rows: list[dict[str, Any]] = []
    for finding in audit.get("findings", []) or []:
        issue = str(finding.get("issue") or "")
        rows.append(
            {
                "snapshot_date": snapshot_date,
                "run_id": run_id,
                "track": _track_for_issue(issue),
                "severity": str(finding.get("severity") or ""),
                "issue": issue,
                "evidence": str(finding.get("evidence") or ""),
                "owner": finding.get("owner"),
            }
        )
    return rows


def build_staged_source_quality_findings(
    audit: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = [r for r in _findings_rows(audit) if r["track"] != "D"]
    rows.sort(key=lambda r: (r["track"], r["severity"], r["issue"], r["evidence"]))
    return rows


def build_staged_distribution_findings(audit: dict[str, Any]) -> list[dict[str, Any]]:
    rows = [r for r in _findings_rows(audit) if r["track"] == "D"]
    rows.sort(key=lambda r: (r["severity"], r["issue"], r["evidence"]))
    return rows


# ---------------------------------------------------------------------------
# Marts — analyst-friendly aggregates
# ---------------------------------------------------------------------------


MART_DIRECTOR_SOURCE_HEALTH_COLUMNS = [
    "snapshot_date",
    "run_id",
    "director",
    "source_count",
    "ok_source_count",
    "warning_source_count",
    "blocked_source_count",
    "total_row_count",
    "total_finding_count",
]


def build_mart_director_source_health(audit: dict[str, Any]) -> list[dict[str, Any]]:
    snapshot_date = str(audit.get("snapshot_date") or "")
    run_id = str(audit.get("run_id") or "")
    by_director: dict[str | None, dict[str, Any]] = {}
    for src in audit.get("sources", []) or []:
        director = src.get("director")
        bucket = by_director.setdefault(
            director,
            {
                "snapshot_date": snapshot_date,
                "run_id": run_id,
                "director": director or "",
                "source_count": 0,
                "ok_source_count": 0,
                "warning_source_count": 0,
                "blocked_source_count": 0,
                "total_row_count": 0,
                "total_finding_count": 0,
            },
        )
        bucket["source_count"] += 1
        status = src.get("status")
        if status == "ok":
            bucket["ok_source_count"] += 1
        elif status == "warning":
            bucket["warning_source_count"] += 1
        elif status == "blocked":
            bucket["blocked_source_count"] += 1
        bucket["total_row_count"] += int(src.get("row_count") or 0)
        bucket["total_finding_count"] += int(src.get("finding_count") or 0)
    rows = list(by_director.values())
    rows.sort(key=lambda r: r["director"])
    return rows


MART_SOURCE_RUN_SUMMARY_COLUMNS = [
    "snapshot_date",
    "run_id",
    "generated_at",
    "status",
    "selected_source_count",
    "source_count",
    "ok_source_count",
    "warning_source_count",
    "blocked_source_count",
    "finding_count",
    "high_finding_count",
    "medium_finding_count",
    "baseline_drift_finding_count",
    "baseline_high_finding_count",
    "baseline_matched_source_count",
    "baseline_missing_source_count",
    "distribution_finding_count",
    "distribution_high_finding_count",
    "distribution_matched_source_count",
    "distribution_missing_seed_source_count",
    "distribution_missing_seed_dimension_count",
]


def build_mart_source_run_summary(audit: dict[str, Any]) -> list[dict[str, Any]]:
    summary = audit.get("summary") or {}
    row = {
        "snapshot_date": str(audit.get("snapshot_date") or ""),
        "run_id": str(audit.get("run_id") or ""),
        "generated_at": str(audit.get("generated_at") or ""),
        "status": str(audit.get("status") or ""),
        "selected_source_count": int(summary.get("selected_source_count") or 0),
        "source_count": int(summary.get("source_count") or 0),
        "ok_source_count": int(summary.get("ok_source_count") or 0),
        "warning_source_count": int(summary.get("warning_source_count") or 0),
        "blocked_source_count": int(summary.get("blocked_source_count") or 0),
        "finding_count": int(summary.get("finding_count") or 0),
        "high_finding_count": int(summary.get("high_finding_count") or 0),
        "medium_finding_count": int(summary.get("medium_finding_count") or 0),
        "baseline_drift_finding_count": int(
            summary.get("baseline_drift_finding_count") or 0
        ),
        "baseline_high_finding_count": int(
            summary.get("baseline_high_finding_count") or 0
        ),
        "baseline_matched_source_count": int(
            summary.get("baseline_matched_source_count") or 0
        ),
        "baseline_missing_source_count": int(
            summary.get("baseline_missing_source_count") or 0
        ),
        "distribution_finding_count": int(
            summary.get("distribution_finding_count") or 0
        ),
        "distribution_high_finding_count": int(
            summary.get("distribution_high_finding_count") or 0
        ),
        "distribution_matched_source_count": int(
            summary.get("distribution_matched_source_count") or 0
        ),
        "distribution_missing_seed_source_count": int(
            summary.get("distribution_missing_seed_source_count") or 0
        ),
        "distribution_missing_seed_dimension_count": int(
            summary.get("distribution_missing_seed_dimension_count") or 0
        ),
    }
    return [row]


# ---------------------------------------------------------------------------
# Lookup — used by writer.py and parity.py
# ---------------------------------------------------------------------------


TABLE_BUILDERS = {
    "raw_salesforce_extract_plan": (
        build_raw_salesforce_extract_plan,
        RAW_EXTRACT_PLAN_COLUMNS,
    ),
    "raw_source_quality_audit": (
        build_raw_source_quality_audit,
        RAW_SOURCE_QUALITY_AUDIT_COLUMNS,
    ),
    "staged_source_requirements": (
        build_staged_source_requirements,
        STAGED_SOURCE_REQUIREMENTS_COLUMNS,
    ),
    "staged_source_quality_findings": (
        build_staged_source_quality_findings,
        STAGED_FINDING_COLUMNS,
    ),
    "staged_distribution_findings": (
        build_staged_distribution_findings,
        STAGED_FINDING_COLUMNS,
    ),
    "mart_director_source_health": (
        build_mart_director_source_health,
        MART_DIRECTOR_SOURCE_HEALTH_COLUMNS,
    ),
    "mart_source_run_summary": (
        build_mart_source_run_summary,
        MART_SOURCE_RUN_SUMMARY_COLUMNS,
    ),
}


def track_finding_distribution(audit: dict[str, Any]) -> Counter[str]:
    """Count findings by track for parity-report use."""
    return Counter(
        _track_for_issue(str(f.get("issue") or ""))
        for f in audit.get("findings", []) or []
    )
