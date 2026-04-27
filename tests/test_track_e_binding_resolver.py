"""Track E — E5 tests for the deck binding resolver (E3).

Positive control: every binding on the active director_monthly profile
resolves cleanly against the live APAC workbook anchor at
``~/Downloads/jesper-tyrer-2026-04-20.xlsx``.

Negative controls confirm:
  - derived_table with an unresolvable snapshot_role surfaces fail
    status on the binding row + a blocker
  - direct_workbook_table with an unknown source_column produces fail
    status on the column row
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import deck_contract
from scripts.monthly_platform import deck_binding_resolver
from scripts.monthly_platform import director_workbook_contract


APAC_WORKBOOK = Path("/Users/test/Downloads/jesper-tyrer-2026-04-20.xlsx")
CANONICAL_DECK = Path(__file__).resolve().parents[1] / "config" / "deck_contract.yaml"


@pytest.mark.skipif(
    not APAC_WORKBOOK.exists(),
    reason="Live APAC workbook not present.",
)
def test_apac_anchor_resolves_all_bindings():
    report = deck_binding_resolver.resolve(workbook_path=APAC_WORKBOOK)
    assert report["status"] == "pass", report["blockers"]
    assert report["fail_count"] == 0
    # Sanity: at least one of each major binding type.
    types = {b["binding_type"] for b in report["bindings"]}
    assert "derived_table" in types
    assert "direct_workbook_table" in types
    assert "generated_takeaway" in types
    assert "external_link" in types
    assert "legal_text" in types
    assert "source_note" in types


@pytest.mark.skipif(
    not APAC_WORKBOOK.exists(),
    reason="Live APAC workbook not present.",
)
def test_derived_table_with_unresolvable_role_fails():
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "q1_forecast_variance"
    )
    derived = next(
        t for t in slide["tables"] if t.get("binding_type") == "derived_table"
    )
    # Force an unresolvable role mapping. The resolver looks up the
    # role in the workbook contract; pointing at a real role name that
    # the resolver can find, but at a sheet with no matching pattern.
    # Easier: invent a workbook contract where the role doesn't exist.
    # Simplest path: rename the role to one not declared.
    derived["snapshot_roles"]["opening_arr"] = "totally_unknown_role"
    deck = deck_contract.DeckContract(raw=raw, path=CANONICAL_DECK)
    workbook = director_workbook_contract.load()
    report = deck_binding_resolver.resolve(
        workbook_path=APAC_WORKBOOK, deck=deck, workbook=workbook
    )
    assert report["status"] == "fail"
    assert any(
        b["binding_type"] == "derived_table" and b["status"] == "fail"
        for b in report["bindings"]
    )


@pytest.mark.skipif(
    not APAC_WORKBOOK.exists(),
    reason="Live APAC workbook not present.",
)
def test_direct_table_with_unknown_column_fails():
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "top_open_opportunities"
    )
    slide["tables"][0]["columns"][0]["source_column"] = "Phantom Column"
    deck = deck_contract.DeckContract(raw=raw, path=CANONICAL_DECK)
    workbook = director_workbook_contract.load()
    report = deck_binding_resolver.resolve(
        workbook_path=APAC_WORKBOOK, deck=deck, workbook=workbook
    )
    assert report["status"] == "fail"
    # The failing table should report the bad column.
    failing = [
        b
        for b in report["bindings"]
        if b["kind"] == "table" and b.get("status") == "fail"
    ]
    assert failing
    assert any(
        any(c.get("status") == "fail" for c in (b.get("columns") or []))
        for b in failing
    )
