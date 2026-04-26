"""Pandera schema for ``mart_source_run_summary`` (Track I, slice 7 / final).

One-row-per-run summary mart. Carries the full count vector for a
(snapshot_date, run_id) tuple — selected/source/ok/warning/blocked
counts plus the Track C (baseline) and Track D (distribution) finding
totals the audit ``summary`` block aggregates.

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``observed_schemas.json``) — exactly 1 row per run.

Wide-row contracts
------------------
- ``ok + warning + blocked ≤ source_count`` (sources can also be in
  ``failed`` / planned states, so ≤ rather than ==).
- ``high_finding_count + medium_finding_count ≤ finding_count``
  (info / low findings exist alongside the high/medium pair).
- ``baseline_high_finding_count ≤ baseline_drift_finding_count``
  (high baselines are a subset of total drift findings).
- ``distribution_high_finding_count ≤ distribution_finding_count``
  (high distribution findings are a subset of total).
- ``distribution_missing_seed_dimension_count`` is a per-dimension
  counter, not bounded by source count.

Status enum mirrors what ``extract_salesforce_sources.build_quality_audit``
can emit: ok / warning / blocked / failed / planned.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import (
    run_id_column,
    snapshot_date_column,
)


TABLE_ID = "mart_source_run_summary"


def _bucketed_counts_within_source_count(df) -> bool:
    bucketed = (
        df["ok_source_count"] + df["warning_source_count"] + df["blocked_source_count"]
    )
    return bool((bucketed <= df["source_count"]).all())


def _high_medium_within_total_findings(df) -> bool:
    return bool(
        (
            df["high_finding_count"] + df["medium_finding_count"] <= df["finding_count"]
        ).all()
    )


def _baseline_high_within_total(df) -> bool:
    return bool(
        (df["baseline_high_finding_count"] <= df["baseline_drift_finding_count"]).all()
    )


def _distribution_high_within_total(df) -> bool:
    return bool(
        (
            df["distribution_high_finding_count"] <= df["distribution_finding_count"]
        ).all()
    )


def _non_negative_count(c: str) -> Column:
    return Column(int, checks=Check.greater_than_or_equal_to(0))


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns={
        "snapshot_date": snapshot_date_column(),
        "run_id": run_id_column(),
        "generated_at": Column(
            str,
            checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}T"),
            description=(
                "ISO-8601 timestamp prefix; full timezone suffix varies "
                "(``+00:00`` vs ``Z``) so the regex anchors only the "
                "``YYYY-MM-DDTHH:`` lead."
            ),
        ),
        "status": Column(
            str,
            checks=Check.isin(["ok", "warning", "blocked", "failed", "planned"]),
        ),
        "selected_source_count": _non_negative_count("selected_source_count"),
        "source_count": _non_negative_count("source_count"),
        "ok_source_count": _non_negative_count("ok_source_count"),
        "warning_source_count": _non_negative_count("warning_source_count"),
        "blocked_source_count": _non_negative_count("blocked_source_count"),
        "finding_count": _non_negative_count("finding_count"),
        "high_finding_count": _non_negative_count("high_finding_count"),
        "medium_finding_count": _non_negative_count("medium_finding_count"),
        "baseline_drift_finding_count": _non_negative_count(
            "baseline_drift_finding_count"
        ),
        "baseline_high_finding_count": _non_negative_count(
            "baseline_high_finding_count"
        ),
        "baseline_matched_source_count": _non_negative_count(
            "baseline_matched_source_count"
        ),
        "baseline_missing_source_count": _non_negative_count(
            "baseline_missing_source_count"
        ),
        "distribution_finding_count": _non_negative_count("distribution_finding_count"),
        "distribution_high_finding_count": _non_negative_count(
            "distribution_high_finding_count"
        ),
        "distribution_matched_source_count": _non_negative_count(
            "distribution_matched_source_count"
        ),
        "distribution_missing_seed_source_count": _non_negative_count(
            "distribution_missing_seed_source_count"
        ),
        "distribution_missing_seed_dimension_count": _non_negative_count(
            "distribution_missing_seed_dimension_count"
        ),
    },
    checks=[
        Check(
            _bucketed_counts_within_source_count,
            error="ok+warning+blocked exceeds source_count",
            name="bucketed_counts_within_source_count",
        ),
        Check(
            _high_medium_within_total_findings,
            error="high_finding_count + medium_finding_count exceeds finding_count",
            name="high_medium_within_total_findings",
        ),
        Check(
            _baseline_high_within_total,
            error="baseline_high_finding_count exceeds baseline_drift_finding_count",
            name="baseline_high_within_total",
        ),
        Check(
            _distribution_high_within_total,
            error="distribution_high_finding_count exceeds distribution_finding_count",
            name="distribution_high_within_total",
        ),
    ],
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
    unique=["snapshot_date", "run_id"],
)


__all__ = ["SCHEMA", "TABLE_ID"]
