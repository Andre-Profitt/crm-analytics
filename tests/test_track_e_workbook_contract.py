"""Track E — E5 negative-control tests for the workbook contract validator.

The workbook contract validator (E1) is structural-only — it verifies
the YAML against the JSON Schema and checks snapshot_role sheet
references + regex compilation. The real .xlsx-validation logic lives
in E2 (scripts/validate_track_e_workbook.py).

Fixtures here exercise the structural surface only:
  - good                                  -> pass, 0 blockers
  - snapshot_role_unknown_sheet           -> blocker
  - snapshot_role_missing_date_group      -> blocker
  - snapshot_role_invalid_regex           -> blocker
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import director_workbook_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL = REPO_ROOT / "config" / "director_workbook_contract.yaml"


@pytest.fixture(scope="module")
def canonical() -> dict:
    return yaml.safe_load(CANONICAL.read_text(encoding="utf-8"))


def _validate(raw: dict):
    contract = director_workbook_contract.DirectorWorkbookContract(
        raw=raw, path=CANONICAL
    )
    return director_workbook_contract.validate(contract)


def _has(findings, code: str) -> bool:
    return any(f["code"] == code for f in findings)


def test_canonical_workbook_contract_passes(canonical):
    findings, report = _validate(copy.deepcopy(canonical))
    assert report["status"] == "pass", findings
    assert report["sheet_count"] == 13
    assert report["snapshot_role_count"] == 9


def test_snapshot_role_unknown_sheet_is_blocked(canonical):
    raw = copy.deepcopy(canonical)
    raw["snapshot_roles"]["q1_opening"]["sheet"] = "Phantom Sheet"
    findings, report = _validate(raw)
    assert report["status"] == "fail"
    assert _has(findings, "snapshot_role_unknown_sheet")


def test_snapshot_role_missing_date_group_is_blocked(canonical):
    raw = copy.deepcopy(canonical)
    # Pattern lacks the required (?P<date>...) named group.
    raw["snapshot_roles"]["q1_opening"]["column_pattern"] = r"^ARR \d{4}-\d{2}-\d{2}$"
    findings, report = _validate(raw)
    assert report["status"] == "fail"
    assert _has(findings, "snapshot_role_missing_date_group")


def test_snapshot_role_invalid_regex_is_blocked(canonical):
    raw = copy.deepcopy(canonical)
    raw["snapshot_roles"]["q1_opening"]["column_pattern"] = (
        "^ARR (?P<date>"  # unbalanced
    )
    findings, report = _validate(raw)
    assert report["status"] == "fail"
    assert _has(findings, "snapshot_role_invalid_regex") or _has(
        findings, "schema_violation"
    )
