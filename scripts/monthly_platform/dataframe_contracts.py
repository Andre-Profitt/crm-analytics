"""Track I — runtime dataframe contracts.

Two-tier validation for warehouse Parquet tables:

* :func:`validate_pandera` — runtime check against the Pandera schema
  registered for ``table_id`` in :mod:`schemas.pandera`. Used inside
  the warehouse builder (or ad-hoc by tests / operators) to fail fast
  on type drift, missing columns, range / pattern / enum violations.
* :func:`validate_frictionless` — portable check against the
  Frictionless Table Schema at
  ``schemas/table_schema/<table_id>.schema.json``. Same semantics,
  expressed in a tool-agnostic format so non-Python consumers can
  validate the same Parquet artifact without depending on pandera.

Both functions return a structured :class:`ContractFinding` list rather
than raising, so callers can choose to gate (raise) or report
(append-to-findings). The pattern mirrors Track B/C/D — severity is the
caller's policy choice, not baked into the validator.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


SchemaTier = Literal["pandera", "frictionless"]


@dataclass
class ContractFinding:
    table_id: str
    tier: SchemaTier
    severity: Literal["error", "warning"]
    message: str
    evidence: str = ""


@dataclass
class ContractReport:
    table_id: str
    tier: SchemaTier
    status: Literal["pass", "fail"]
    findings: list[ContractFinding] = field(default_factory=list)

    def model_dump(self) -> dict[str, Any]:
        return {
            "table_id": self.table_id,
            "tier": self.tier,
            "status": self.status,
            "findings": [
                {
                    "table_id": f.table_id,
                    "tier": f.tier,
                    "severity": f.severity,
                    "message": f.message,
                    "evidence": f.evidence,
                }
                for f in self.findings
            ],
        }


# ---------------------------------------------------------------------------
# Pandera tier
# ---------------------------------------------------------------------------


def validate_pandera(
    *,
    table_id: str,
    df: Any,
) -> ContractReport:
    """Validate ``df`` against the registered Pandera schema for ``table_id``.

    Returns a :class:`ContractReport` with all errors collected (lazy
    validation), so callers see every problem in one pass instead of
    bailing at the first.
    """
    from schemas.pandera import SCHEMAS

    schema = SCHEMAS.get(table_id)
    if schema is None:
        return ContractReport(
            table_id=table_id,
            tier="pandera",
            status="fail",
            findings=[
                ContractFinding(
                    table_id=table_id,
                    tier="pandera",
                    severity="error",
                    message=f"no Pandera schema registered for table_id={table_id!r}",
                )
            ],
        )

    import pandera.pandas as pa  # noqa: F401  - registered as pandera ext

    try:
        schema.validate(df, lazy=True)
        return ContractReport(table_id=table_id, tier="pandera", status="pass")
    except Exception as exc:  # pandera lazy validation raises a SchemaErrors
        findings: list[ContractFinding] = []
        failure_cases = getattr(exc, "failure_cases", None)
        if failure_cases is not None:
            # SchemaErrors path — iterate the per-row failure rows.
            for record in failure_cases.to_dict(orient="records"):
                findings.append(
                    ContractFinding(
                        table_id=table_id,
                        tier="pandera",
                        severity="error",
                        message=str(record.get("check") or "schema check failed"),
                        evidence=(
                            f"column={record.get('column')!r}; "
                            f"failure={record.get('failure_case')!r}; "
                            f"index={record.get('index')!r}"
                        ),
                    )
                )
        if not findings:
            findings.append(
                ContractFinding(
                    table_id=table_id,
                    tier="pandera",
                    severity="error",
                    message=type(exc).__name__,
                    evidence=str(exc),
                )
            )
        return ContractReport(
            table_id=table_id,
            tier="pandera",
            status="fail",
            findings=findings,
        )


# ---------------------------------------------------------------------------
# Frictionless tier
# ---------------------------------------------------------------------------


def _table_schema_path(repo_root: Path, table_id: str) -> Path:
    return repo_root / "schemas" / "table_schema" / f"{table_id}.schema.json"


def validate_frictionless(
    *,
    table_id: str,
    parquet_path: Path,
    repo_root: Path | None = None,
) -> ContractReport:
    """Validate ``parquet_path`` against the Frictionless Table Schema.

    Reads the JSON schema from
    ``<repo_root>/schemas/table_schema/<table_id>.schema.json`` and uses
    Frictionless to validate the Parquet file. Errors land in
    :class:`ContractFinding` rows the same shape as the Pandera path.
    """
    repo_root = repo_root or Path(__file__).resolve().parents[2]
    schema_path = _table_schema_path(repo_root, table_id)
    if not schema_path.exists():
        return ContractReport(
            table_id=table_id,
            tier="frictionless",
            status="fail",
            findings=[
                ContractFinding(
                    table_id=table_id,
                    tier="frictionless",
                    severity="error",
                    message=f"missing Frictionless schema at {schema_path}",
                )
            ],
        )

    from frictionless import Resource, Schema

    schema = Schema.from_descriptor(json.loads(schema_path.read_text(encoding="utf-8")))
    # Frictionless rejects absolute paths in ``Resource(path=...)`` as a
    # security default. Split into ``basepath`` + relative filename so
    # the safety check sees only a name and trusts the explicit base —
    # warehouses live at absolute paths in production and tmp paths in
    # tests; both are operator-supplied, not attacker-supplied.
    resource = Resource(
        path=parquet_path.name,
        schema=schema,
        basepath=str(parquet_path.parent),
    )
    report = resource.validate()
    findings: list[ContractFinding] = []
    if not report.valid:
        for task in report.tasks:
            for err in task.errors:
                findings.append(
                    ContractFinding(
                        table_id=table_id,
                        tier="frictionless",
                        severity="error",
                        message=err.title or err.code,
                        evidence=err.message or "",
                    )
                )
    return ContractReport(
        table_id=table_id,
        tier="frictionless",
        status="pass" if not findings else "fail",
        findings=findings,
    )


__all__ = [
    "ContractFinding",
    "ContractReport",
    "SchemaTier",
    "validate_frictionless",
    "validate_pandera",
]
