"""Track E — E5 negative-control tests for the deck contract validator.

Strategy: load the canonical config/deck_contract.yaml as a dict,
mutate one thing per test to introduce a single defect, run
deck_contract.validate(), and assert the expected finding code is
present (and that the canonical contract still passes cleanly).

Fixture matrix (per GPT review):
  - good                                  -> pass, 0 blockers
  - missing_slide_id                      -> blocker
  - duplicate_slide_number                -> blocker (duplicate_slide_number)
  - unknown_workbook_sheet                -> blocker (unknown_workbook_sheet)
  - unknown_workbook_column               -> blocker (unknown_workbook_column)
  - missing_required_takeaway_metrics     -> blocker (schema_violation)
  - invalid_table_binding                 -> blocker (schema_violation)
  - missing_director_monthly_profile      -> blocker (missing_profile)
  - control_deck_status_active            -> warning (control_deck_not_deferred)
  - missing_snapshot_role_for_derived     -> blocker (unknown_snapshot_role)
  - unknown_transform_for_derived         -> blocker (unknown_transform)
  - invalid_legacy_title_pattern          -> blocker (invalid_legacy_title_pattern)
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import deck_contract
from scripts.monthly_platform import director_workbook_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
CANONICAL_DECK = REPO_ROOT / "config" / "deck_contract.yaml"
CANONICAL_WORKBOOK = REPO_ROOT / "config" / "director_workbook_contract.yaml"


@pytest.fixture(scope="module")
def canonical_deck() -> dict:
    return yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def canonical_workbook_contract() -> (
    director_workbook_contract.DirectorWorkbookContract
):
    return director_workbook_contract.load(CANONICAL_WORKBOOK)


def _validate(raw: dict, workbook):
    contract = deck_contract.DeckContract(raw=raw, path=CANONICAL_DECK)
    return deck_contract.validate(contract, workbook_contract=workbook)


def _has(findings, code: str) -> bool:
    return any(f.code == code for f in findings)


# ---------------------------------------------------------------------
# Positive control.
# ---------------------------------------------------------------------


def test_canonical_contract_passes(canonical_deck, canonical_workbook_contract):
    findings, report = _validate(
        copy.deepcopy(canonical_deck), canonical_workbook_contract
    )
    assert report["status"] == "pass", [
        f.as_dict() for f in findings if f.severity == "blocker"
    ]
    assert report["director_monthly_slide_count"] == 18
    assert report["blocker_count"] == 0


# ---------------------------------------------------------------------
# Negative controls.
# ---------------------------------------------------------------------


def test_missing_slide_id_is_blocked(canonical_deck, canonical_workbook_contract):
    raw = copy.deepcopy(canonical_deck)
    del raw["profiles"]["director_monthly"]["slides"][2]["id"]
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    # JSON Schema catches it as schema_violation; cross-check still runs.
    assert _has(findings, "schema_violation")


def test_duplicate_slide_number_is_blocked(canonical_deck, canonical_workbook_contract):
    raw = copy.deepcopy(canonical_deck)
    # Force two slides to share slide_number=5.
    raw["profiles"]["director_monthly"]["slides"][2]["slide_number"] = 5
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "duplicate_slide_number")


def test_unknown_workbook_sheet_is_blocked(canonical_deck, canonical_workbook_contract):
    raw = copy.deepcopy(canonical_deck)
    # Repoint the since_last_review table to a sheet that doesn't exist.
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "since_last_review"
    )
    slide["tables"][0]["sheet"] = "Imaginary Sheet"
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "unknown_workbook_sheet")


def test_unknown_workbook_column_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "top_open_opportunities"
    )
    # Replace a real source_column with a fictitious one.
    slide["tables"][0]["columns"][0]["source_column"] = "NotARealColumn"
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "unknown_workbook_column")


def test_takeaway_required_without_template_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "executive_summary"
    )
    # Remove the template field even though required_takeaway.required is True.
    slide["required_takeaway"].pop("template", None)
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "schema_violation")


def test_invalid_table_binding_is_blocked(canonical_deck, canonical_workbook_contract):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "renewals"
    )
    # Drop columns from the renewals table.
    del slide["tables"][0]["columns"]
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "schema_violation")


def test_missing_director_monthly_profile_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    del raw["profiles"]["director_monthly"]
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "missing_profile") or _has(findings, "schema_violation")


def test_control_deck_active_is_warning(canonical_deck, canonical_workbook_contract):
    raw = copy.deepcopy(canonical_deck)
    raw["profiles"]["control_deck"]["status"] = "active"
    findings, report = _validate(raw, canonical_workbook_contract)
    # warning, not blocker.
    assert _has(findings, "control_deck_not_deferred")


def test_unknown_snapshot_role_in_derived_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "q1_forecast_variance"
    )
    derived = next(
        t for t in slide["tables"] if t.get("binding_type") == "derived_table"
    )
    derived["snapshot_roles"]["opening_arr"] = "not_a_role"
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "unknown_snapshot_role")


def test_unknown_transform_for_derived_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "q1_forecast_variance"
    )
    derived = next(
        t for t in slide["tables"] if t.get("binding_type") == "derived_table"
    )
    derived["transform_id"] = "no_such_transform"
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "unknown_transform")


def test_invalid_legacy_title_pattern_is_blocked(
    canonical_deck, canonical_workbook_contract
):
    raw = copy.deepcopy(canonical_deck)
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "owner_coaching"
    )
    # Unbalanced parenthesis -> re.compile raises.
    slide["legacy_title_patterns"] = ["^(unbalanced"]
    findings, report = _validate(raw, canonical_workbook_contract)
    assert report["status"] == "fail"
    assert _has(findings, "invalid_legacy_title_pattern")
