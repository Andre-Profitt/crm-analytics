"""Pandera schema for ``raw_salesforce_extract_plan`` (Track I, slice 4).

Direct Parquet mirror of ``source_requirement_plan.json items[]``. One
row per (requirement_id, territory, period_role, quarter_label) tuple;
``status`` records whether the source was successfully resolved to a
Salesforce report/list-view id.

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/observed_schemas.json``).

Nullability split + cross-field contracts
-----------------------------------------
The column-level rules are deliberately permissive (``territory`` /
``director`` / ``region`` nullable; ``source_id`` / ``source_label``
unconstrained on length) because each is legitimate in some shapes and
illegitimate in others. The two cross-field checks below close the
contract:

1. ``scope == "territory"`` rows MUST have non-empty ``territory`` and
   ``director``. Nulls / empties for those two are only valid for
   ``scope == "global"``. ``region`` is intentionally NOT required —
   the v20c live evidence has it null on every territory row because
   region is denormalized metadata the warehouse layer joins in
   downstream, not a field carried on the source plan items.
2. ``status == "configured"`` rows MUST have non-empty ``source_id``.
   An empty ``source_id`` on a "configured" row would silently fail at
   extract time; catch it at the contract layer instead.

``source_label`` is intentionally NOT required when configured — the
v20c live evidence has at least one configured row whose label fell
back to an empty string, and tightening would create a false-fail
without clear operational value.
"""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import (
    run_id_column,
    snapshot_date_column,
)


TABLE_ID = "raw_salesforce_extract_plan"


def _territory_scope_metadata_complete(df: pd.DataFrame) -> bool:
    """When scope == "territory", territory and director must be non-empty.

    ``region`` is intentionally not checked: the v20c live evidence has
    it null on every territory row because region is denormalized
    metadata the warehouse joins downstream, not a field carried on the
    source plan items themselves.
    """
    territory_rows = df[df["scope"] == "territory"]
    if territory_rows.empty:
        return True
    for col in ("territory", "director"):
        # Treat NaN/None and empty string as missing.
        missing = territory_rows[col].isna() | (territory_rows[col] == "")
        if missing.any():
            return False
    return True


def _configured_rows_have_source_id(df: pd.DataFrame) -> bool:
    """When status == "configured", source_id must be non-empty."""
    configured = df[df["status"] == "configured"]
    if configured.empty:
        return True
    missing = configured["source_id"].isna() | (configured["source_id"] == "")
    return not bool(missing.any())


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
        # ``status == "missing_source_id"``. Cross-field check below
        # forbids the empty case when status is "configured".
        "source_id": Column(str, nullable=True),
        "source_label": Column(str, nullable=True),
        "status": Column(
            str,
            checks=Check.isin(["configured", "missing_source_id", "disabled"]),
        ),
    },
    checks=[
        Check(
            _territory_scope_metadata_complete,
            error=("scope='territory' rows must have non-empty territory and director"),
            name="territory_scope_metadata_complete",
        ),
        Check(
            _configured_rows_have_source_id,
            error=("status='configured' rows must have non-empty source_id"),
            name="configured_rows_have_source_id",
        ),
    ],
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
