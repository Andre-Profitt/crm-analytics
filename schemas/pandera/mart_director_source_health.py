"""Pandera schema for ``mart_director_source_health`` (Track I, slice 6).

Per-director aggregate of source-quality health: how many sources, how
many ok / warning / blocked, total rows, total findings. One row per
distinct ``director`` value in the audit; the global-scope source
shows up as ``director = ""`` rather than ``None`` (the marts builder
substitutes the empty string when it groups).

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``observed_schemas.json``) — 10 director rows in the v20c snapshot.

Constraints
-----------
- ``director`` is unique within a (snapshot_date, run_id) tuple — the
  mart aggregates by director, so two rows for the same director
  inside one run is a routing bug.
- All ``*_count`` columns are BIGINT >= 0.
- ``ok + warning + blocked`` ≤ ``source_count`` (cannot exceed; some
  sources may be in other states like ``failed``). Pandera lacks a
  pure cross-column inequality check without a custom check function;
  declared as a wide-row check below.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import (
    run_id_column,
    snapshot_date_column,
)


TABLE_ID = "mart_director_source_health"


def _ok_warning_blocked_within_source_count(df) -> bool:
    """Wide-row sanity: per-row ok+warning+blocked must not exceed source_count."""
    bucketed = (
        df["ok_source_count"] + df["warning_source_count"] + df["blocked_source_count"]
    )
    return bool((bucketed <= df["source_count"]).all())


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns={
        "snapshot_date": snapshot_date_column(),
        "run_id": run_id_column(),
        "director": Column(
            str,
            description=(
                "Director display name. Empty string for global-scope "
                'sources (the marts builder substitutes ``""`` when it '
                "groups). Uniqueness within a single run is asserted via "
                "the wide-frame check below."
            ),
        ),
        "source_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "ok_source_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "warning_source_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "blocked_source_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "total_row_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "total_finding_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
    },
    checks=[
        Check(
            _ok_warning_blocked_within_source_count,
            error="ok+warning+blocked exceeds source_count",
            name="bucketed_counts_within_source_count",
        ),
    ],
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
    unique=["snapshot_date", "run_id", "director"],
)


__all__ = ["SCHEMA", "TABLE_ID"]
