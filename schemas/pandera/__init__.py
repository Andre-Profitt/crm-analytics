"""Pandera dataframe schemas — one per warehouse table_id.

Module layout::

    schemas/pandera/
        __init__.py                 ← exports SCHEMAS registry
        source_quality_audit.py     ← raw_source_quality_audit (this PR)
        # source_requirements.py    ← staged_source_requirements (next slice)
        # extract_plan.py           ← raw_salesforce_extract_plan (next slice)
        # source_quality_findings.py
        # distribution_findings.py
        # director_source_health.py
        # source_run_summary.py

Each module declares a single ``SCHEMA: pa.DataFrameSchema`` and a
matching ``TABLE_ID: str`` so the validator in
``scripts/monthly_platform/dataframe_contracts.py`` can route by
table_id without import-time circular dependencies.

The ``SCHEMAS`` dict here is the canonical registry. Add a new table by
importing it once and registering ``TABLE_ID`` -> ``SCHEMA``.
"""

from __future__ import annotations

from schemas.pandera import (
    mart_director_source_health,
    mart_source_run_summary,
    raw_salesforce_extract_plan,
    source_quality_audit,
    staged_distribution_findings,
    staged_source_quality_findings,
    staged_source_requirements,
)


SCHEMAS = {
    raw_salesforce_extract_plan.TABLE_ID: raw_salesforce_extract_plan.SCHEMA,
    source_quality_audit.TABLE_ID: source_quality_audit.SCHEMA,
    staged_source_quality_findings.TABLE_ID: staged_source_quality_findings.SCHEMA,
    staged_distribution_findings.TABLE_ID: staged_distribution_findings.SCHEMA,
    staged_source_requirements.TABLE_ID: staged_source_requirements.SCHEMA,
    mart_director_source_health.TABLE_ID: mart_director_source_health.SCHEMA,
    mart_source_run_summary.TABLE_ID: mart_source_run_summary.SCHEMA,
}


__all__ = ["SCHEMAS"]
