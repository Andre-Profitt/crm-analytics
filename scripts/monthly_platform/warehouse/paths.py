"""Track H — warehouse filesystem layout."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


WAREHOUSE_OUTPUT_ROOT_NAME = "source_backed_warehouse"

# Parquet table relative paths under ``<warehouse_root>/<snapshot>/<run_id>/``.
TABLE_RELATIVE_PATHS: dict[str, str] = {
    # raw — direct mirrors of pipeline JSON evidence.
    "raw_salesforce_extract_plan": "raw/salesforce_extract_plan.parquet",
    "raw_source_quality_audit": "raw/source_quality_audit.parquet",
    # staged — flattened / typed views of the contract + findings.
    "staged_source_requirements": "staged/source_requirements.parquet",
    "staged_source_quality_findings": "staged/source_quality_findings.parquet",
    "staged_distribution_findings": "staged/distribution_findings.parquet",
    # marts — analyst-friendly aggregates.
    "mart_director_source_health": "marts/director_source_health.parquet",
    "mart_source_run_summary": "marts/source_run_summary.parquet",
}

MANIFEST_FILENAME = "warehouse_manifest.json"
PARITY_FILENAME = "parity_report.json"


@dataclass(frozen=True)
class WarehousePaths:
    """All paths the warehouse build touches for one (snapshot, run_id)."""

    repo_root: Path
    snapshot_date: str
    run_id: str
    warehouse_root: Path | None = None

    @property
    def root(self) -> Path:
        base = self.warehouse_root or (
            self.repo_root / "output" / WAREHOUSE_OUTPUT_ROOT_NAME
        )
        return base / self.snapshot_date / self.run_id

    @property
    def manifest_path(self) -> Path:
        return self.root / MANIFEST_FILENAME

    @property
    def parity_report_path(self) -> Path:
        return self.root / PARITY_FILENAME

    def table_path(self, table_id: str) -> Path:
        if table_id not in TABLE_RELATIVE_PATHS:
            raise KeyError(
                f"unknown warehouse table_id={table_id}; "
                f"valid={sorted(TABLE_RELATIVE_PATHS)}"
            )
        return self.root / TABLE_RELATIVE_PATHS[table_id]

    @property
    def all_table_paths(self) -> dict[str, Path]:
        return {tid: self.table_path(tid) for tid in TABLE_RELATIVE_PATHS}

    @property
    def source_run_dir(self) -> Path:
        """Where the upstream extract stage parked its evidence for this run."""
        return (
            self.repo_root
            / "output"
            / "monthly_salesforce_sources"
            / self.snapshot_date
            / self.run_id
        )

    @property
    def source_plan_path(self) -> Path:
        return self.source_run_dir / "plans" / "source_requirement_plan.json"

    @property
    def source_quality_audit_path(self) -> Path:
        return self.source_run_dir / "audits" / "source_extract_quality_audit.json"

    @property
    def requirements_registry_path(self) -> Path:
        return self.repo_root / "config" / "monthly_source_requirements.json"
