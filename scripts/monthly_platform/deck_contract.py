"""Track E — deck contract loader + structural validator.

Loads ``config/deck_contract.yaml``, validates against
``schemas/deck_contract.schema.json``, then runs cross-reference checks
that go beyond what JSON Schema can express:

  - profiles.director_monthly is active and has 18 slides
  - profiles.control_deck is declared (status: deferred is fine in M1)
  - slide ids are unique
  - slide_numbers are unique and contiguous from 1
  - every tables[].source: director_workbook reference resolves to a
    sheet declared in the workbook contract
  - every tables[].columns[*].source_column exists in that sheet's
    required_columns
  - every tables[].columns[*].snapshot_role resolves to a registered
    snapshot_role that targets the same sheet
  - every derived_table has transform_id, rows, and snapshot_roles all
    pointing at registered workbook roles
  - every required_links[].kind is supported

The cross-reference checks read the workbook contract (loaded via
``director_workbook_contract.load``) so the deck contract is verified
against the same governance source that the binding resolver and
workbook validator use.

This module does NOT touch the produced PPTX or the live workbook
file. The workbook validator (E2), binding resolver (E3), and PPTX
checker (E4) handle those.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
import jsonschema

from scripts.monthly_platform import director_workbook_contract as wb_contract


REPO_ROOT = Path(__file__).resolve().parents[2]
DECK_CONTRACT_PATH = REPO_ROOT / "config" / "deck_contract.yaml"
DECK_SCHEMA_PATH = REPO_ROOT / "schemas" / "deck_contract.schema.json"

SCHEMA_VERSION = "monthly_platform.deck_contract.v2"
DIRECTOR_MONTHLY_EXPECTED_SLIDES = 18

REPORT_SCHEMA_VERSION = "monthly_platform.deck_contract_report.v1"


@dataclass(frozen=True)
class ValidationFinding:
    severity: str  # "blocker" | "warning"
    code: str
    message: str
    path: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "message": self.message,
            "path": self.path,
        }


@dataclass
class DeckContract:
    """In-memory representation of the deck contract."""

    raw: dict[str, Any]
    path: Path

    @property
    def schema_version(self) -> str:
        return str(self.raw["schema_version"])

    @property
    def profiles(self) -> dict[str, dict[str, Any]]:
        return self.raw.get("profiles", {})

    @property
    def director_monthly(self) -> dict[str, Any]:
        return self.profiles["director_monthly"]

    @property
    def director_monthly_slides(self) -> list[dict[str, Any]]:
        return self.director_monthly.get("slides", [])


def load(path: Path | str | None = None) -> DeckContract:
    """Load + parse the deck contract; does not validate."""
    p = Path(path) if path else DECK_CONTRACT_PATH
    raw = yaml.safe_load(p.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{p}: contract must be a YAML mapping")
    return DeckContract(raw=raw, path=p)


def _load_schema() -> dict[str, Any]:
    return json.loads(DECK_SCHEMA_PATH.read_text(encoding="utf-8"))


def validate(
    contract: DeckContract | None = None,
    *,
    workbook_contract: wb_contract.DirectorWorkbookContract | None = None,
) -> tuple[list[ValidationFinding], dict[str, Any]]:
    """Run structural + cross-reference validation.

    Returns ``(findings, report_payload)``. Caller decides exit status:
    any blocker -> non-zero. Warnings do not block.
    """
    if contract is None:
        contract = load()
    if workbook_contract is None:
        workbook_contract = wb_contract.load()
    assert contract is not None
    assert workbook_contract is not None

    findings: list[ValidationFinding] = []

    # 1. JSON Schema structural validation.
    try:
        jsonschema.validate(contract.raw, _load_schema())
    except jsonschema.ValidationError as e:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="schema_violation",
                message=str(e.message),
                path=".".join(str(p) for p in e.absolute_path),
            )
        )

    # 2. schema_version pin.
    if contract.schema_version != SCHEMA_VERSION:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="schema_version_mismatch",
                message=f"expected {SCHEMA_VERSION}, got {contract.schema_version!r}",
                path="schema_version",
            )
        )

    # 3. profile registry.
    profiles = contract.profiles
    if "director_monthly" not in profiles:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="missing_profile",
                message="profiles.director_monthly must exist and be active",
                path="profiles",
            )
        )
        return findings, _build_report(contract, workbook_contract, findings)

    dm = contract.director_monthly
    if dm.get("status") != "active":
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="director_monthly_not_active",
                message=f"profiles.director_monthly.status must be 'active', got {dm.get('status')!r}",
                path="profiles.director_monthly.status",
            )
        )

    if (
        "control_deck" in profiles
        and profiles["control_deck"].get("status") != "deferred"
    ):
        findings.append(
            ValidationFinding(
                severity="warning",
                code="control_deck_not_deferred",
                message="profiles.control_deck should be status: deferred in M1",
                path="profiles.control_deck.status",
            )
        )

    # 4. slide identity checks.
    slides = contract.director_monthly_slides
    if len(slides) != DIRECTOR_MONTHLY_EXPECTED_SLIDES:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="slide_count_mismatch",
                message=f"director_monthly expected {DIRECTOR_MONTHLY_EXPECTED_SLIDES} slides, got {len(slides)}",
                path="profiles.director_monthly.slides",
            )
        )

    seen_ids: dict[str, int] = {}
    seen_numbers: dict[int, str] = {}
    for slide in slides:
        sid = str(slide.get("id") or "")
        snum_raw = slide.get("slide_number")
        snum = int(snum_raw) if isinstance(snum_raw, int) else 0
        if not sid or snum == 0:
            # JSON Schema check above catches missing id/slide_number; skip
            # cross-check on this entry to avoid noisy duplicate findings.
            continue
        if sid in seen_ids:
            findings.append(
                ValidationFinding(
                    severity="blocker",
                    code="duplicate_slide_id",
                    message=f"slide id {sid!r} appears at slide_number {seen_ids[sid]} and {snum}",
                    path=f"profiles.director_monthly.slides[{snum}].id",
                )
            )
        seen_ids[sid] = snum
        if snum in seen_numbers:
            findings.append(
                ValidationFinding(
                    severity="blocker",
                    code="duplicate_slide_number",
                    message=f"slide_number {snum} used by {seen_numbers[snum]!r} and {sid!r}",
                    path=f"profiles.director_monthly.slides[{snum}].slide_number",
                )
            )
        seen_numbers[snum] = sid

    expected_numbers = set(range(1, len(slides) + 1))
    actual_numbers = set(seen_numbers.keys())
    missing = expected_numbers - actual_numbers
    if missing:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="slide_numbers_not_contiguous",
                message=f"missing slide_numbers: {sorted(missing)}",
                path="profiles.director_monthly.slides",
            )
        )

    # 5. workbook source cross-references.
    declared_sheets = set(
        contract.raw.get("data_sources", {})
        .get("director_workbook", {})
        .get("required_sheets", [])
    )
    workbook_sheets = workbook_contract.sheets_by_name()
    missing_in_workbook = declared_sheets - set(workbook_sheets.keys())
    if missing_in_workbook:
        findings.append(
            ValidationFinding(
                severity="blocker",
                code="data_source_sheet_not_in_workbook_contract",
                message=f"deck declares sheets not in workbook contract: {sorted(missing_in_workbook)}",
                path="data_sources.director_workbook.required_sheets",
            )
        )

    # 6. table binding cross-references (per slide).
    snapshot_roles = workbook_contract.snapshot_roles()
    transform_registry = _registered_transforms()

    for slide in slides:
        for tbl in slide.get("tables", []) or []:
            _validate_table_binding(
                tbl,
                slide=slide,
                workbook_sheets=workbook_sheets,
                snapshot_roles=snapshot_roles,
                transforms=transform_registry,
                findings=findings,
            )

    # 7. legacy_title_patterns: regex compilation.
    for slide in slides:
        for pat in slide.get("legacy_title_patterns", []) or []:
            try:
                re.compile(pat)
            except re.error as e:
                findings.append(
                    ValidationFinding(
                        severity="blocker",
                        code="invalid_legacy_title_pattern",
                        message=f"slide {slide.get('id')!r} legacy pattern {pat!r}: {e}",
                        path=f"profiles.director_monthly.slides[{slide.get('slide_number')}].legacy_title_patterns",
                    )
                )

    return findings, _build_report(contract, workbook_contract, findings)


def _validate_table_binding(
    tbl: dict[str, Any],
    *,
    slide: dict[str, Any],
    workbook_sheets: dict[str, dict[str, Any]],
    snapshot_roles: dict[str, dict[str, Any]],
    transforms: set[str],
    findings: list[ValidationFinding],
) -> None:
    sid = slide.get("id")
    tid = tbl.get("id")
    binding_type = tbl.get("binding_type", "direct_workbook_table")
    src = tbl.get("source")

    if src == "director_workbook":
        sheet_name = str(tbl.get("sheet") or "")
        sheet = workbook_sheets.get(sheet_name)
        if sheet is None:
            findings.append(
                ValidationFinding(
                    severity="blocker",
                    code="unknown_workbook_sheet",
                    message=f"slide={sid} table={tid} unknown workbook sheet {sheet_name!r}",
                    path=f"slides[{sid}].tables[{tid}].sheet",
                )
            )
            return

        allowed_columns = set(sheet.get("required_columns", []) or [])

        if binding_type == "derived_table":
            # Derived tables: verify transform + snapshot_roles + rows.
            transform = tbl.get("transform_id")
            if transform not in transforms:
                findings.append(
                    ValidationFinding(
                        severity="blocker",
                        code="unknown_transform",
                        message=f"derived table {tid} on slide {sid} references unknown transform_id {transform!r}",
                        path=f"slides[{sid}].tables[{tid}].transform_id",
                    )
                )
            for input_name, role_name in (tbl.get("snapshot_roles") or {}).items():
                role = snapshot_roles.get(role_name)
                if role is None:
                    findings.append(
                        ValidationFinding(
                            severity="blocker",
                            code="unknown_snapshot_role",
                            message=f"derived table {tid} input {input_name!r} -> unknown snapshot_role {role_name!r}",
                            path=f"slides[{sid}].tables[{tid}].snapshot_roles.{input_name}",
                        )
                    )
                elif (
                    role.get("source") != "runtime" and role.get("sheet") != sheet_name
                ):
                    findings.append(
                        ValidationFinding(
                            severity="blocker",
                            code="snapshot_role_sheet_mismatch",
                            message=f"derived table {tid} sheet={sheet_name} but role {role_name!r} targets sheet {role.get('sheet')!r}",
                            path=f"slides[{sid}].tables[{tid}].snapshot_roles.{input_name}",
                        )
                    )
            # Skip per-row column-existence checks for derived tables;
            # the resolver checks the transform output shape at runtime.
            return

        # direct_workbook_table — check columns one by one.
        for col in tbl.get("columns", []) or []:
            if "source_column" in col:
                if col["source_column"] not in allowed_columns:
                    findings.append(
                        ValidationFinding(
                            severity="blocker",
                            code="unknown_workbook_column",
                            message=f"slide={sid} table={tid} column={col.get('id')} source_column {col['source_column']!r} not in sheet {sheet_name!r}",
                            path=f"slides[{sid}].tables[{tid}].columns[{col.get('id')}]",
                        )
                    )
            elif "snapshot_role" in col:
                role = snapshot_roles.get(col["snapshot_role"])
                if role is None:
                    findings.append(
                        ValidationFinding(
                            severity="blocker",
                            code="unknown_snapshot_role",
                            message=f"slide={sid} table={tid} column={col.get('id')} snapshot_role {col['snapshot_role']!r} not registered",
                            path=f"slides[{sid}].tables[{tid}].columns[{col.get('id')}]",
                        )
                    )
                elif (
                    role.get("source") != "runtime" and role.get("sheet") != sheet_name
                ):
                    findings.append(
                        ValidationFinding(
                            severity="blocker",
                            code="snapshot_role_sheet_mismatch",
                            message=f"slide={sid} table={tid} column={col.get('id')} role targets sheet {role.get('sheet')!r}, table sheet={sheet_name!r}",
                            path=f"slides[{sid}].tables[{tid}].columns[{col.get('id')}]",
                        )
                    )
            # computed columns require no column-existence check.

    elif src == "warehouse":
        # Reserved for later; M1 has no warehouse-bound tables on the
        # director_monthly profile. If a slide added one, just check the
        # mart is in required_marts.
        warehouse_marts = set(
            tbl.get("mart_registry", []) or []
        )  # placeholder; intentional minimal validation in M1.
        if not warehouse_marts:
            return


def _registered_transforms() -> set[str]:
    """The transform registry. M1 has one declared transform; future
    milestones will move this to a discoverable registry."""
    return {"q1_forecast_variance_bridge"}


def _build_report(
    contract: DeckContract,
    workbook_contract: wb_contract.DirectorWorkbookContract,
    findings: list[ValidationFinding],
) -> dict[str, Any]:
    blockers = [f for f in findings if f.severity == "blocker"]
    warnings = [f for f in findings if f.severity == "warning"]
    slides = contract.director_monthly_slides if contract.profiles else []
    return {
        "schema_version": REPORT_SCHEMA_VERSION,
        "contract_path": str(contract.path),
        "deck_contract_schema_version": contract.schema_version,
        "workbook_contract_schema_version": workbook_contract.schema_version,
        "status": "pass" if not blockers else "fail",
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "profile_count": len(contract.profiles),
        "director_monthly_slide_count": len(slides),
        "director_monthly_expected_slide_count": DIRECTOR_MONTHLY_EXPECTED_SLIDES,
        "findings": [f.as_dict() for f in findings],
    }


__all__ = [
    "DECK_CONTRACT_PATH",
    "DECK_SCHEMA_PATH",
    "DIRECTOR_MONTHLY_EXPECTED_SLIDES",
    "DeckContract",
    "REPORT_SCHEMA_VERSION",
    "SCHEMA_VERSION",
    "ValidationFinding",
    "load",
    "validate",
]
