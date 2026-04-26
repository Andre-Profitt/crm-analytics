"""Track H — input-vs-warehouse parity report.

Asserts that the warehouse's row counts match the JSON evidence's row
counts. Parity does NOT prove semantic correctness — it only proves the
warehouse is a complete, faithful materialization of what the upstream
pipeline already wrote. Track I will add semantic validation via Pandera +
Frictionless schemas; Track H deliberately stops at "the row counts agree".

Output shape matches the rest of the audit family — one ``schema_version``
plus a ``checks`` list with explicit pass/fail rows so a reviewer can see
*which* table broke parity if it ever does.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from scripts.monthly_platform.contracts import utc_now_iso
from scripts.monthly_platform.warehouse.marts import (
    track_finding_distribution,
)
from scripts.monthly_platform.warehouse.paths import (
    PARITY_FILENAME,
    WarehousePaths,
)


SCHEMA_VERSION = "monthly_platform.source_backed_warehouse_parity_report.v1"


@dataclass
class ParityCheck:
    name: str
    expected: int
    observed: int
    status: str  # "pass" | "fail"
    notes: str = ""


@dataclass
class ParityReport:
    schema_version: str = SCHEMA_VERSION
    generated_at: str = field(default_factory=utc_now_iso)
    snapshot_date: str = ""
    run_id: str = ""
    status: str = "pass"
    checks: list[ParityCheck] = field(default_factory=list)

    def model_dump(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "snapshot_date": self.snapshot_date,
            "run_id": self.run_id,
            "status": self.status,
            "checks": [
                {
                    "name": c.name,
                    "expected": c.expected,
                    "observed": c.observed,
                    "status": c.status,
                    "notes": c.notes,
                }
                for c in self.checks
            ],
        }


def _table_row_counts(manifest: dict[str, Any]) -> dict[str, int]:
    return {t["table_id"]: int(t["row_count"]) for t in manifest.get("tables", [])}


def compute_parity(
    *,
    paths: WarehousePaths,
    plan: dict[str, Any],
    audit: dict[str, Any],
    registry: dict[str, Any],
    manifest: dict[str, Any],
) -> ParityReport:
    """Compare JSON evidence row counts to warehouse table row counts.

    Each ``ParityCheck`` is a (expected, observed) pair plus a ``status``.
    Run-level ``status`` is ``"fail"`` if any check fails, otherwise ``"pass"``.
    """
    counts = _table_row_counts(manifest)
    findings = audit.get("findings") or []
    track_counts = track_finding_distribution(audit)
    checks: list[ParityCheck] = []

    def add(name: str, expected: int, observed: int, notes: str = "") -> None:
        checks.append(
            ParityCheck(
                name=name,
                expected=expected,
                observed=observed,
                status="pass" if expected == observed else "fail",
                notes=notes,
            )
        )

    # raw — direct mirrors.
    add(
        "raw_salesforce_extract_plan vs source_requirement_plan.items",
        expected=len(plan.get("items") or []),
        observed=counts.get("raw_salesforce_extract_plan", -1),
    )
    add(
        "raw_source_quality_audit vs audit.sources",
        expected=len(audit.get("sources") or []),
        observed=counts.get("raw_source_quality_audit", -1),
    )

    # staged — flattened views.
    add(
        "staged_source_requirements vs registry.requirements",
        expected=len(registry.get("requirements") or []),
        observed=counts.get("staged_source_requirements", -1),
    )
    add(
        "staged_source_quality_findings vs non-distribution findings",
        expected=track_counts["B"] + track_counts["C"] + track_counts.get("unknown", 0),
        observed=counts.get("staged_source_quality_findings", -1),
        notes=(
            "Track B + Track C findings + any 'unknown' track = audit.findings minus "
            "Track D distribution_*"
        ),
    )
    add(
        "staged_distribution_findings vs distribution_* findings",
        expected=track_counts["D"],
        observed=counts.get("staged_distribution_findings", -1),
    )
    add(
        "all-track findings vs audit.findings",
        expected=len(findings),
        observed=counts.get("staged_source_quality_findings", -1)
        + counts.get("staged_distribution_findings", -1),
        notes="Sum of staged finding tables must equal audit.findings length",
    )

    # marts — derived; check structural invariants only.
    add(
        "mart_director_source_health rows == distinct directors in audit",
        expected=len({src.get("director") for src in audit.get("sources") or []}),
        observed=counts.get("mart_director_source_health", -1),
    )
    add(
        "mart_source_run_summary rows == 1",
        expected=1,
        observed=counts.get("mart_source_run_summary", -1),
        notes="One row per (snapshot, run_id)",
    )

    status = "pass" if all(c.status == "pass" for c in checks) else "fail"
    return ParityReport(
        schema_version=SCHEMA_VERSION,
        snapshot_date=paths.snapshot_date,
        run_id=paths.run_id,
        status=status,
        checks=checks,
    )


def write_parity_report(paths: WarehousePaths, report: ParityReport) -> Path:
    paths.parity_report_path.parent.mkdir(parents=True, exist_ok=True)
    paths.parity_report_path.write_text(
        json.dumps(report.model_dump(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return paths.parity_report_path


def load_parity_report(paths: WarehousePaths) -> dict[str, Any]:
    return json.loads(paths.parity_report_path.read_text(encoding="utf-8"))


__all__ = [
    "PARITY_FILENAME",
    "ParityCheck",
    "ParityReport",
    "SCHEMA_VERSION",
    "compute_parity",
    "load_parity_report",
    "write_parity_report",
]
