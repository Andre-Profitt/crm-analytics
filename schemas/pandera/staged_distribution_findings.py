"""Pandera schema for ``staged_distribution_findings`` (Track I, slice 3).

The Track D companion to ``staged_source_quality_findings``: same column
shape, opposite routing rule. The warehouse builder splits findings by
issue prefix in :mod:`scripts.monthly_platform.warehouse.marts`
(``_track_for_issue``) — ``source_distribution_*`` lands here, every
other ``source_*`` lands in the quality-findings table. The two schemas
together enforce the cross-table contract on ``track``:

* ``staged_source_quality_findings`` — ``track ∈ {B, C, unknown}``
* ``staged_distribution_findings``   — ``track == "D"``

If both schemas pass for the same row, the warehouse routed it to the
wrong table; tests cover that explicitly via the
``track_b_in_distribution_table`` / ``track_c_in_distribution_table``
negative controls and the symmetrical ``track="D"`` rejection in the
quality-findings table.

The issue regex allows digits (``^source_distribution_[a-z0-9_]+$``)
because slice / sentinel concepts can naturally carry numeric tokens
(stage_5_presence, q3_close, etc.) even when the current set of emitted
codes doesn't. Tightening to no-digits would force a schema migration
the next time someone adds a numeric-bearing distribution issue.
"""

from __future__ import annotations

import pandera.pandas as pa
from pandera.pandas import Check, Column

from schemas.pandera._finding_common import finding_columns_with


TABLE_ID = "staged_distribution_findings"


SCHEMA: pa.DataFrameSchema = pa.DataFrameSchema(
    columns=finding_columns_with(
        track_column=Column(
            str,
            checks=Check.equal_to("D"),
            description=(
                "Track D distribution findings only. Track B/C/unknown "
                "are forbidden here — they live in "
                "``staged_source_quality_findings``."
            ),
        ),
        issue_column=Column(
            str,
            checks=Check.str_matches(r"^source_distribution_[a-z0-9_]+$"),
            description=(
                "Issue code in the ``source_distribution_*`` namespace. "
                "Digits allowed for sentinel / slice identifiers."
            ),
        ),
    ),
    strict=True,
    coerce=False,
    name=f"warehouse.{TABLE_ID}",
)


__all__ = ["SCHEMA", "TABLE_ID"]
