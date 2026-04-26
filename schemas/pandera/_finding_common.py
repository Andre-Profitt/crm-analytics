"""Shared Pandera column definitions for finding-shaped tables.

Both ``staged_source_quality_findings`` and ``staged_distribution_findings``
share an identical (snapshot_date, run_id, severity, owner) prefix and
the same evidence semantics. Centralizing the column factories here:

* keeps the two tables genuinely consistent (a checks edit lands once,
  not in two near-identical files), and
* leaves the per-table differences — ``track`` enum and ``issue``
  regex — visible in each schema module.

This is **not** a generic schema toolkit. It's a small, narrowly-scoped
deduplication for the two findings tables. Don't grow it without a
third caller. Other table families (raw extract plan, mart summaries)
have different column shapes and shouldn't reach into this module.
"""

from __future__ import annotations

from pandera.pandas import Check, Column


SEVERITY_VALUES = ["high", "medium", "low", "info"]
SNAPSHOT_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"


def snapshot_date_column() -> Column:
    return Column(
        str,
        checks=Check.str_matches(SNAPSHOT_DATE_PATTERN),
        description="ISO-8601 date of the monthly snapshot (YYYY-MM-DD).",
    )


def run_id_column() -> Column:
    return Column(
        str,
        checks=Check.str_length(min_value=1),
        description="Stable identifier for the extract run.",
    )


def severity_column() -> Column:
    return Column(
        str,
        checks=Check.isin(SEVERITY_VALUES),
        description="From contracts.FindingSeverity.",
    )


def evidence_column() -> Column:
    """``evidence`` may be empty for some findings, but the column is required.

    The Pandera ``str`` dtype already rejects ``None``; allowing the empty
    string is intentional — not every finding carries free-form evidence.
    """
    return Column(str, nullable=False)


def owner_column() -> Column:
    return Column(
        str,
        nullable=True,
        description=(
            "Optional owner attribution from contracts.Finding.owner; "
            "``None`` for findings without a designated owner."
        ),
    )


def finding_columns_with(
    *,
    track_column: Column,
    issue_column: Column,
) -> dict[str, Column]:
    """Build the canonical column dict for a finding table.

    Callers supply the two table-specific columns (``track`` and
    ``issue``) and get back a dict in the warehouse writer's column
    order, ready to drop into ``pa.DataFrameSchema(columns=...)``.
    """
    return {
        "snapshot_date": snapshot_date_column(),
        "run_id": run_id_column(),
        "track": track_column,
        "severity": severity_column(),
        "issue": issue_column,
        "evidence": evidence_column(),
        "owner": owner_column(),
    }


__all__ = [
    "SEVERITY_VALUES",
    "SNAPSHOT_DATE_PATTERN",
    "evidence_column",
    "finding_columns_with",
    "owner_column",
    "run_id_column",
    "severity_column",
    "snapshot_date_column",
]
