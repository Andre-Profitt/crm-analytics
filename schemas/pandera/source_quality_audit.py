"""Pandera schema for ``raw_source_quality_audit`` (Track I, first slice).

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/observed_schemas.json``).

Column types come from the typed Parquet schema the warehouse writer
emits (post-Codex P2 fix on PR #6 — ``BIGINT``/``BOOLEAN``/``VARCHAR``
preserved through both empty and non-empty paths).

Pandera type choices
--------------------
- DuckDB ``BIGINT`` -> Pandera ``int`` (pandas reads as ``int64``).
- DuckDB ``VARCHAR`` -> Pandera ``str`` (pandas reads as ``object``
  with string values; pandera v2's string-coerce path is fine).
- DuckDB ``BOOLEAN`` -> Pandera ``bool`` (no booleans in this table).

Nullability
-----------
``territory`` and ``director`` are nullable in the warehouse (a global
source has neither). Every other column is required. The mart-level
``mart_director_source_health`` schema (next slice) handles the rolled-up
"director can be empty string" case separately.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Column, Check


TABLE_ID = "raw_source_quality_audit"


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns={
        "snapshot_date": Column(
            str,
            checks=Check.str_matches(r"^\d{4}-\d{2}-\d{2}$"),
            description="ISO-8601 date of the monthly snapshot (YYYY-MM-DD).",
        ),
        "run_id": Column(
            str,
            checks=Check.str_length(min_value=1),
            description="Stable identifier for the extract run.",
        ),
        "source_key": Column(
            str,
            checks=Check.str_length(min_value=1),
            description=(
                "Composite source identifier: "
                "<requirement_id>.<territory>.<period_role>.<quarter_label>.<salesforce_id>"
            ),
            unique=True,
        ),
        "status": Column(
            str,
            checks=Check.isin(["ok", "warning", "blocked"]),
        ),
        "requirement_id": Column(str, checks=Check.str_length(min_value=1)),
        "dataset": Column(str, checks=Check.str_length(min_value=1)),
        "source_type": Column(
            str,
            checks=Check.isin(
                ["salesforce_report", "salesforce_list_view", "salesforce_soql_probe"]
            ),
        ),
        "salesforce_id": Column(str, checks=Check.str_length(min_value=1)),
        "label": Column(str),
        # Track D's territory normalization preserves the original display
        # name (e.g. "APAC", "Middle East & Africa"); the slug lives on the
        # baseline_key in source_quality_findings, not here.
        "territory": Column(str, nullable=True),
        "director": Column(str, nullable=True),
        "period_role": Column(
            str,
            checks=Check.isin(["prior_quarter", "current_quarter", "forward_quarter"]),
        ),
        "quarter_label": Column(
            str,
            checks=Check.str_matches(r"^Q[1-4]$"),
        ),
        "row_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "row_count_status": Column(
            str,
            checks=Check.isin(["ok", "warning", "blocked"]),
        ),
        "required_field_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "required_fields_present_count": Column(
            int, checks=Check.greater_than_or_equal_to(0)
        ),
        "missing_required_fields_count": Column(
            int, checks=Check.greater_than_or_equal_to(0)
        ),
        "finding_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "high_finding_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "medium_finding_count": Column(int, checks=Check.greater_than_or_equal_to(0)),
        "quality_hash": Column(
            str,
            checks=Check.str_matches(r"^[a-f0-9]{64}$"),
            description="SHA-256 of the per-source quality payload.",
        ),
    },
    strict=True,  # reject any column not declared above
    coerce=False,  # types must already match — the writer handles that
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
