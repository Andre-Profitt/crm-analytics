"""Pandera schema for ``staged_source_requirements`` (Track I, slice 5).

Flattened, typed projection of ``config/monthly_source_requirements.json``
``requirements[]``. Captures the contract metadata each requirement
declares (lane membership, source type, row-count policy bounds, Track D
opt-in) without copying the full nested policy objects — the warehouse
keeps this table small and queryable; full policy structures live in
the JSON file the contract authors edit.

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``observed_schemas.json``).

Type notes
----------
- ``min_rows`` / ``max_rows`` are nullable BIGINT — the marts builder
  emits ``None`` when ``RowCountPolicy.max_rows`` is left open.
- ``enabled`` / ``allow_zero`` / ``has_distribution_policy`` /
  ``fallback_policy_present`` are BOOLEAN.
- All ``*_count`` columns are BIGINT ≥ 0.

Constraints
-----------
- ``requirement_id`` unique (one row per declared requirement).
- ``source_system`` enum: today only ``"salesforce"``.
- ``source_type`` enum: matches the runtime ``SourceType`` literal.
- ``scope`` enum: ``territory`` / ``global``.
- ``min_rows`` / ``max_rows`` non-negative when present; ``max_rows >= 1``
  when present (RowCountPolicy validator constraint).
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column


TABLE_ID = "staged_source_requirements"


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns={
        "requirement_id": Column(
            str,
            checks=Check.str_length(min_value=1),
            unique=True,
            description="Unique identifier from the contract registry.",
        ),
        "enabled": Column(bool),
        "owner": Column(str, checks=Check.str_length(min_value=1)),
        "source_system": Column(str, checks=Check.isin(["salesforce"])),
        "source_type": Column(
            str,
            checks=Check.isin(
                [
                    "salesforce_report",
                    "salesforce_list_view",
                    "salesforce_soql_probe",
                ]
            ),
        ),
        "dataset": Column(str, checks=Check.str_length(min_value=1)),
        "output_grain": Column(str, checks=Check.str_length(min_value=1)),
        "scope": Column(str, checks=Check.isin(["territory", "global"])),
        "allow_zero": Column(bool),
        "min_rows": Column(
            int,
            nullable=True,
            checks=Check.greater_than_or_equal_to(0),
            description=(
                "RowCountPolicy.min_rows; nullable when the policy leaves "
                "the lower bound open."
            ),
        ),
        "max_rows": Column(
            int,
            nullable=True,
            checks=Check.greater_than_or_equal_to(1),
            description=(
                "RowCountPolicy.max_rows; nullable when the policy leaves "
                "the upper bound open. Pydantic validator requires >= 1 "
                "when present."
            ),
        ),
        "has_distribution_policy": Column(bool),
        "distribution_dimension_count": Column(
            int,
            checks=Check.greater_than_or_equal_to(0),
        ),
        "slice_sentinel_count": Column(
            int,
            checks=Check.greater_than_or_equal_to(0),
        ),
        "fallback_policy_present": Column(bool),
        "tag_count": Column(
            int,
            checks=Check.greater_than_or_equal_to(0),
        ),
    },
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
