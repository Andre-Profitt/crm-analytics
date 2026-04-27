"""Track E — E5 tests for the produced-PPTX checker (E4).

Positive control runs against the live APAC anchor
``~/Downloads/jesper-tyrer-LAND.pptx``. Skipped if missing.

Negative controls modify the contract in-memory to exercise the
specific blocker paths:
  - title_neither_stable_nor_legacy
  - table_count_mismatch
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import deck_contract
from scripts.validate_director_monthly_pptx import validate_pptx


APAC_DECK = Path("/Users/test/Downloads/jesper-tyrer-LAND.pptx")
CANONICAL_DECK = Path(__file__).resolve().parents[1] / "config" / "deck_contract.yaml"


def _make_contract(raw: dict) -> deck_contract.DeckContract:
    return deck_contract.DeckContract(raw=raw, path=CANONICAL_DECK)


@pytest.mark.skipif(
    not APAC_DECK.exists(),
    reason="Live APAC pptx not present; run with the deck in ~/Downloads/",
)
def test_apac_anchor_passes_in_legacy_mode():
    report = validate_pptx(APAC_DECK)
    # 16 verbose-title slides + 1 missing-link warning expected;
    # 0 blockers because of the M1 transition policy.
    assert report["status"] == "pass", report["findings"]
    assert report["actual_slide_count"] == 18
    assert report["legacy_verbose_title_count"] == 16
    assert any(
        f["code"] == "missing_required_link_transition" for f in report["findings"]
    )


@pytest.mark.skipif(
    not APAC_DECK.exists(),
    reason="Live APAC pptx not present.",
)
def test_title_neither_stable_nor_legacy_blocks():
    # Drop legacy_title_patterns so the verbose title can't match.
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "owner_coaching"
    )
    slide.pop("legacy_title_patterns", None)
    contract = _make_contract(raw)
    report = validate_pptx(APAC_DECK, contract=contract)
    assert report["status"] == "fail"
    assert any(
        f["code"] == "title_neither_stable_nor_legacy" for f in report["findings"]
    )


@pytest.mark.skipif(
    not APAC_DECK.exists(),
    reason="Live APAC pptx not present.",
)
def test_table_count_mismatch_blocks():
    # Force renewals (slide 17) to declare 4 tables when the deck has 1.
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "renewals"
    )
    extra_tables = [copy.deepcopy(slide["tables"][0]) for _ in range(3)]
    for i, t in enumerate(extra_tables):
        t["id"] = f"tbl_renewals_extra_{i}"
    slide["tables"].extend(extra_tables)
    contract = _make_contract(raw)
    report = validate_pptx(APAC_DECK, contract=contract)
    assert report["status"] == "fail"
    assert any(f["code"] == "table_count_mismatch" for f in report["findings"])
