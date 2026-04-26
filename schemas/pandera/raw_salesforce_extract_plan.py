"""Pandera schema for ``raw_salesforce_extract_plan`` (Track I, slice 4).

Direct Parquet mirror of ``source_requirement_plan.json items[]``. One
row per (requirement_id, territory, period_role, quarter_label) tuple;
``status`` records whether the source was successfully resolved to a
Salesforce report/list-view id.

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/observed_schemas.json``).

Nullability split
-----------------
``territory``, ``director``, ``region`` are legitimately ``None`` for
``scope == "global"`` requirements (no per-territory binding). Every
other column is required.

``source_id`` and ``source_label`` may be empty strings when
``status == "missing_source_id"`` — the row is still recorded so the
extract step can emit a finding. We don't enforce a length constraint
on those columns; the enum check on ``status`` is what gates them
semantically.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import (
    run_id_column,
    snapshot_date_column,
)


TABLE_ID = "raw_salesforce_extract_plan"


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns={
        "snapshot_date": snapshot_date_column(),
        "run_id": run_id_column(),
        "requirement_id": Column(str, checks=Check.str_length(min_value=1)),
        "source_system": Column(
            str,
            checks=Check.isin(["salesforce"]),
            description="Today only Salesforce is supported.",
        ),
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
        "salesforce_object": Column(str, checks=Check.str_length(min_value=1)),
        "dataset": Column(str, checks=Check.str_length(min_value=1)),
        "output_grain": Column(str, checks=Check.str_length(min_value=1)),
        "scope": Column(str, checks=Check.isin(["territory", "global"])),
        "territory": Column(str, nullable=True),
        "director": Column(str, nullable=True),
        "region": Column(str, nullable=True),
        "period_role": Column(
            str,
            checks=Check.isin(["prior_quarter", "current_quarter", "forward_quarter"]),
        ),
        "quarter_label": Column(
            str,
            checks=Check.str_matches(r"^Q[1-4]$"),
        ),
        # ``source_id`` / ``source_label`` may legitimately be empty when
        # ``status == "missing_source_id"``. Type-check only.
        "source_id": Column(str),
        "source_label": Column(str),
        "status": Column(
            str,
            checks=Check.isin(["configured", "missing_source_id", "disabled"]),
        ),
    },
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
