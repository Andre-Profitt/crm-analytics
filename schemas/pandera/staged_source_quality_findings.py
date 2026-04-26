"""Pandera schema for ``staged_source_quality_findings`` (Track I, slice 2).

Anchored against the 2026-04-26 v20d ETL spine checkpoint
(``docs/checkpoints/v20d-etl-spine-checkpoint-2026-04-26/observed_schemas.json``).

This is the **non-distribution** findings table. Track D distribution
findings live in their own ``staged_distribution_findings`` table —
the two are mutually exclusive on the ``track`` column. The split is
authoritative because each finding's track is computed from its
``issue`` prefix in :mod:`scripts.monthly_platform.warehouse.marts`
(``_track_for_issue``):

* ``source_distribution_*``  -> Track D     (NOT in this table)
* ``source_quality_baseline_*`` -> Track C
* every other ``source_*`` -> Track B (or "unknown" defensively)

The schema enforces that split: ``track`` must be in ``{B, C, unknown}``,
and the matching ``staged_distribution_findings`` schema (next slice)
will require ``track == "D"``. Together they guarantee the warehouse
can never silently route a finding into the wrong table.

Severities follow ``contracts.FindingSeverity``:
``high`` / ``medium`` / ``low`` / ``info``.

Issue codes are loosely constrained by the regex ``^source_[a-z_]+$``
rather than an exact enum — the project ships ~30 distinct codes and
adding a new one shouldn't require a schema migration. Typos and
non-source-namespaced issues are still rejected.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import finding_columns_with


TABLE_ID = "staged_source_quality_findings"


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns=finding_columns_with(
        track_column=Column(
            str,
            checks=Check.isin(["B", "C", "unknown"]),
            description=(
                "Track ownership of the finding, derived from the issue "
                "prefix. ``D`` is forbidden here — distribution findings "
                "live in ``staged_distribution_findings``."
            ),
        ),
        issue_column=Column(
            str,
            checks=Check.str_matches(r"^source_[a-z_]+$"),
            description=(
                "Issue code emitted by the extract / baseline pipeline. "
                "Must be in the ``source_*`` namespace and snake_case."
            ),
        ),
    ),
    strict=True,  # reject any column not declared above
    coerce=False,  # types must already match — the writer handles casting
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
