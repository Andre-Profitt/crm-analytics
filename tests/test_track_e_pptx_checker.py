"""Track E — E5 tests for the produced-PPTX checker (E4).

Positive control runs against the live APAC anchor
``~/Downloads/jesper-tyrer-LAND.pptx``. Skipped if missing.

Negative controls modify the contract in-memory to exercise the
specific blocker paths:
  - title_neither_stable_nor_legacy
  - table_count_mismatch
  - table_header_mismatch (Cond 1)
  - required_link_target_mismatch (Cond 2)

Cond 1 (table-header validation) test list:
  - canonical contract passes against live deck (legacy_header_sets
    cover the legacy production headers)
  - contract header mutated -> blocker (table_header_mismatch)
  - evidence_only table is excluded from the per-slide check
  - dual-table slide (slide 6 q1_loss_drivers) checks both tables in order

Cond 2 (Salesforce link target):
  - missing link entirely -> warning (M1 transition)
  - hyperlink present but wrong target -> blocker
    (required_link_target_mismatch)
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
    # Expected (post Cond 1+2 patch):
    #   - 16 legacy verbose titles (warnings)
    #   - 14 legacy header drifts on tables (warnings, Cond 1)
    #   - 1 missing required link (warning, M1 transition)
    #   - 0 blockers
    assert report["status"] == "pass", report["findings"]
    assert report["actual_slide_count"] == 18
    assert report["legacy_verbose_title_count"] == 16
    assert any(
        f["code"] == "missing_required_link_transition" for f in report["findings"]
    )
    # Cond 1: header validator runs and emits legacy_header_drift warnings
    # on the slides whose production headers differ from the stable contract.
    legacy_header_findings = [
        f for f in report["findings"] if f["code"] == "legacy_header_drift"
    ]
    assert len(legacy_header_findings) == 14, (
        f"expected 14 legacy_header_drift warnings, got {len(legacy_header_findings)}"
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


# ----------------------------------------------------------------------
# Cond 1 — table-header validation
# ----------------------------------------------------------------------


@pytest.mark.skipif(not APAC_DECK.exists(), reason="Live APAC pptx not present.")
def test_table_header_mismatch_blocks_when_neither_stable_nor_legacy():
    """If a table's headers match neither the stable contract headers
    nor any legacy_header_sets entry, the checker emits a blocker."""
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "owner_coaching"
    )
    tbl = slide["tables"][0]
    # Mutate stable headers + drop legacy patterns -> production headers
    # match neither -> blocker.
    tbl["columns"][0]["header"] = "TotallyDifferentHeader"
    tbl.pop("legacy_header_sets", None)
    contract = _make_contract(raw)
    report = validate_pptx(APAC_DECK, contract=contract)
    assert report["status"] == "fail"
    assert any(f["code"] == "table_header_mismatch" for f in report["findings"])


@pytest.mark.skipif(not APAC_DECK.exists(), reason="Live APAC pptx not present.")
def test_evidence_only_table_excluded_from_header_check():
    """The Q1 Forecast Variance evidence_only table must not be checked
    against produced PPTX headers — slide 5 only has 1 displayed table
    (the bridge), so the evidence_only opportunity-grain table should
    not generate a missing-table or header-mismatch finding."""
    report = validate_pptx(APAC_DECK)
    # slide 5 is q1_forecast_variance. Find its slide_result.
    slide_5 = next(s for s in report["slides"] if s["slide_number"] == 5)
    assert slide_5["expected_tables"] == 1, slide_5
    assert slide_5["actual_tables"] == 1, slide_5
    # No table_missing_in_pptx finding should target the evidence table.
    for f in report["findings"]:
        assert "tbl_q1_forecast_variance_evidence" not in f.get("path", ""), f


@pytest.mark.skipif(not APAC_DECK.exists(), reason="Live APAC pptx not present.")
def test_dual_table_slide_checks_both_tables_in_order():
    """Slide 6 (q1_loss_drivers) has two tables. The header validator
    must run against BOTH in order and surface a finding when the
    SECOND table's headers don't match anything."""
    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slide = next(
        s
        for s in raw["profiles"]["director_monthly"]["slides"]
        if s["id"] == "q1_loss_drivers"
    )
    # Mutate the SECOND table's stable headers + drop legacy.
    second = slide["tables"][1]
    second["columns"][0]["header"] = "WrongHeaderForStageReached"
    second.pop("legacy_header_sets", None)
    contract = _make_contract(raw)
    report = validate_pptx(APAC_DECK, contract=contract)
    # The blocker should target the second table specifically.
    matching = [
        f
        for f in report["findings"]
        if f["code"] == "table_header_mismatch"
        and "tbl_q1_loss_stage_reached" in f.get("path", "")
    ]
    assert matching, [f for f in report["findings"] if f["severity"] == "blocker"]


# ----------------------------------------------------------------------
# Cond 2 — Salesforce link target validation
# ----------------------------------------------------------------------


def _build_pptx_with_link(tmp_path, link_url: str | None) -> Path:
    """Build a minimal 18-slide deck with a hyperlink on slide 12."""
    from pptx import Presentation as _P
    from pptx.util import Inches

    raw = yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))
    slides = raw["profiles"]["director_monthly"]["slides"]
    prs = _P()
    blank_layout = prs.slide_layouts[6]
    for s in slides:
        slide = prs.slides.add_slide(blank_layout)
        # Stable title.
        tx = slide.shapes.add_textbox(Inches(1), Inches(0.5), Inches(8), Inches(0.6))
        tf = tx.text_frame
        tf.text = s["title"]
        if s["id"] == "pushed_deals" and link_url:
            box = slide.shapes.add_textbox(
                Inches(1), Inches(1.5), Inches(8), Inches(0.6)
            )
            p = box.text_frame.paragraphs[0]
            run = p.add_run()
            run.text = "Open in Salesforce"
            run.hyperlink.address = link_url
    out = tmp_path / "synth_deck.pptx"
    prs.save(out)
    return out


def test_salesforce_link_target_correct_passes(tmp_path):
    pptx = _build_pptx_with_link(
        tmp_path,
        "https://simcorp.lightning.force.com/lightning/o/Opportunity/list?filterName=Recent",
    )
    report = validate_pptx(pptx)
    # The cover slide doesn't render properly without the master, so we
    # only assert the link finding is *not* present.
    assert not any(
        f["code"] == "required_link_target_mismatch" for f in report["findings"]
    )
    assert not any(
        f["code"] == "missing_required_link_transition" for f in report["findings"]
    )


def test_salesforce_link_target_wrong_blocks(tmp_path):
    pptx = _build_pptx_with_link(tmp_path, "https://example.com/some-other-page")
    report = validate_pptx(pptx)
    assert report["status"] == "fail"
    assert any(f["code"] == "required_link_target_mismatch" for f in report["findings"])


def test_salesforce_link_missing_warns_only(tmp_path):
    pptx = _build_pptx_with_link(tmp_path, None)
    report = validate_pptx(pptx)
    # No hyperlinks at all -> M1 transition warning, not blocker.
    assert any(
        f["code"] == "missing_required_link_transition" for f in report["findings"]
    )
    assert not any(
        f["code"] == "required_link_target_mismatch" for f in report["findings"]
    )
