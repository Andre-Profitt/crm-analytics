"""Track E — director workbook contract loader + structural validator.

Loads ``config/director_workbook_contract.yaml`` and validates it
against ``schemas/director_workbook_contract.schema.json``. Provides
helpers used by the deck contract validator (E1), the binding resolver
(E3), and the workbook validator (E2):

  - sheets_by_name()        sheet name -> declared sheet entry
  - snapshot_roles()        role name  -> declared role entry
  - resolve_pattern_role()  role + actual workbook -> physical column

This module is read-only; nothing here touches a real .xlsx file.
The workbook validator (E2) handles real-workbook checks.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
import jsonschema


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKBOOK_CONTRACT_PATH = REPO_ROOT / "config" / "director_workbook_contract.yaml"
WORKBOOK_SCHEMA_PATH = REPO_ROOT / "schemas" / "director_workbook_contract.schema.json"
SCHEMA_VERSION = "monthly_platform.director_workbook_contract.v1"


@dataclass(frozen=True)
class ResolvedSnapshotRole:
    role: str
    sheet: str | None
    physical_column: str | None
    resolved_date: str | None
    source: str  # "pattern" | "runtime"
    status: str  # "pass" | "missing" | "ambiguous"
    detail: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "role": self.role,
            "sheet": self.sheet,
            "physical_column": self.physical_column,
            "resolved_date": self.resolved_date,
            "source": self.source,
            "status": self.status,
            "detail": self.detail,
        }


@dataclass
class DirectorWorkbookContract:
    raw: dict[str, Any]
    path: Path

    @property
    def schema_version(self) -> str:
        return str(self.raw["schema_version"])

    def sheets_by_name(self) -> dict[str, dict[str, Any]]:
        return {s["name"]: s for s in self.raw.get("sheets", [])}

    def snapshot_roles(self) -> dict[str, dict[str, Any]]:
        return dict(self.raw.get("snapshot_roles", {}) or {})

    def resolve_pattern_role(
        self,
        role_name: str,
        actual_headers_by_sheet: dict[str, list[str]],
    ) -> ResolvedSnapshotRole:
        """Resolve a single pattern-based role against real workbook headers.

        ``actual_headers_by_sheet`` maps sheet name -> list of header
        cell values. ``selection`` decides which match wins when more
        than one column matches.
        """
        roles = self.snapshot_roles()
        if role_name not in roles:
            return ResolvedSnapshotRole(
                role=role_name,
                sheet=None,
                physical_column=None,
                resolved_date=None,
                source="pattern",
                status="missing",
                detail=f"role {role_name!r} not declared in workbook contract",
            )
        role = roles[role_name]
        if role.get("source") == "runtime":
            return ResolvedSnapshotRole(
                role=role_name,
                sheet=None,
                physical_column=None,
                resolved_date=None,
                source="runtime",
                status="pass",
                detail=f"runtime-bound role -> {role['value']}",
            )

        sheet = role["sheet"]
        pattern = re.compile(role["column_pattern"])
        selection = role["selection"]
        headers = actual_headers_by_sheet.get(sheet, [])
        matches: list[tuple[str, str]] = []
        for h in headers:
            if h is None:
                continue
            m = pattern.match(str(h))
            if m and "date" in m.groupdict():
                matches.append((m.group("date"), str(h)))
        if not matches:
            return ResolvedSnapshotRole(
                role=role_name,
                sheet=sheet,
                physical_column=None,
                resolved_date=None,
                source="pattern",
                status="missing",
                detail=f"no headers in sheet {sheet!r} match {role['column_pattern']!r}",
            )
        matches.sort()
        chosen = matches[0] if selection == "earliest" else matches[-1]
        return ResolvedSnapshotRole(
            role=role_name,
            sheet=sheet,
            physical_column=chosen[1],
            resolved_date=chosen[0],
            source="pattern",
            status="pass",
        )


def load(path: Path | str | None = None) -> DirectorWorkbookContract:
    p = Path(path) if path else WORKBOOK_CONTRACT_PATH
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{p}: contract must be a YAML mapping")
    return DirectorWorkbookContract(raw=raw, path=p)


def _load_schema() -> dict[str, Any]:
    return json.loads(WORKBOOK_SCHEMA_PATH.read_text(encoding="utf-8"))


def validate(
    contract: DirectorWorkbookContract | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    contract = contract or load()
    findings: list[dict[str, Any]] = []

    try:
        jsonschema.validate(contract.raw, _load_schema())
    except jsonschema.ValidationError as e:
        findings.append(
            {
                "severity": "blocker",
                "code": "schema_violation",
                "path": ".".join(str(p) for p in e.absolute_path),
                "message": str(e.message),
            }
        )

    if contract.schema_version != SCHEMA_VERSION:
        findings.append(
            {
                "severity": "blocker",
                "code": "schema_version_mismatch",
                "path": "schema_version",
                "message": f"expected {SCHEMA_VERSION}, got {contract.schema_version!r}",
            }
        )

    # snapshot_role sheet references must resolve to a declared sheet.
    sheet_names = set(contract.sheets_by_name().keys())
    for role_name, role in contract.snapshot_roles().items():
        if role.get("source") == "runtime":
            continue
        if role.get("sheet") not in sheet_names:
            findings.append(
                {
                    "severity": "blocker",
                    "code": "snapshot_role_unknown_sheet",
                    "path": f"snapshot_roles.{role_name}.sheet",
                    "message": f"role {role_name!r} targets sheet {role.get('sheet')!r} which is not declared",
                }
            )
        try:
            pattern = re.compile(role["column_pattern"])
            if "date" not in pattern.groupindex:
                findings.append(
                    {
                        "severity": "blocker",
                        "code": "snapshot_role_missing_date_group",
                        "path": f"snapshot_roles.{role_name}.column_pattern",
                        "message": f"role {role_name!r} pattern must contain a named (?P<date>...) group",
                    }
                )
        except re.error as e:
            findings.append(
                {
                    "severity": "blocker",
                    "code": "snapshot_role_invalid_regex",
                    "path": f"snapshot_roles.{role_name}.column_pattern",
                    "message": str(e),
                }
            )

    blockers = [f for f in findings if f["severity"] == "blocker"]
    warnings = [f for f in findings if f["severity"] == "warning"]
    report = {
        "schema_version": "monthly_platform.director_workbook_contract_report.v1",
        "contract_path": str(contract.path),
        "workbook_contract_schema_version": contract.schema_version,
        "status": "pass" if not blockers else "fail",
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "sheet_count": len(contract.sheets_by_name()),
        "snapshot_role_count": len(contract.snapshot_roles()),
        "findings": findings,
    }
    return findings, report


__all__ = [
    "DirectorWorkbookContract",
    "ResolvedSnapshotRole",
    "SCHEMA_VERSION",
    "WORKBOOK_CONTRACT_PATH",
    "WORKBOOK_SCHEMA_PATH",
    "load",
    "validate",
]
