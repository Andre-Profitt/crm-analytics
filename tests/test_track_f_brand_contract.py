"""Track F / F4 — brand fingerprint validator tests.

Positive control runs against the real ``assets/SimCorp_PPT_Template.pptx``;
skipped if missing. Negative controls mutate the contract dict in-memory
to exercise each blocker code path without committing fake fixture .pptx
files into the repo.
"""

from __future__ import annotations

import copy
from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import brand_contract
from scripts.monthly_platform import deck_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPLATE_PATH = REPO_ROOT / "assets" / "SimCorp_PPT_Template.pptx"
CANONICAL_DECK = REPO_ROOT / "config" / "deck_contract.yaml"


@pytest.fixture(scope="module")
def canonical_deck() -> dict:
    return yaml.safe_load(CANONICAL_DECK.read_text(encoding="utf-8"))


def _make_contract(raw: dict) -> deck_contract.DeckContract:
    return deck_contract.DeckContract(raw=raw, path=CANONICAL_DECK)


@pytest.mark.skipif(
    not TEMPLATE_PATH.exists(),
    reason="SimCorp template not present at assets/",
)
def test_canonical_brand_passes(canonical_deck):
    contract = _make_contract(copy.deepcopy(canonical_deck))
    report = brand_contract.validate_brand(contract)
    assert report.status == "pass", report.findings
    assert report.blocker_count == 0
    assert report.warning_count == 0
    assert report.template_sha256 == report.expected_sha256
    assert report.slide_master_count == 1
    # All 5 builder-used layouts present
    assert report.layouts_missing == []


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_sha_mismatch_blocks(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["expected_template_sha256"] = "0" * 64
    report = brand_contract.validate_brand(_make_contract(raw))
    assert report.status == "fail"
    assert any(f.code == "template_sha256_mismatch" for f in report.findings)


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_template_missing_blocks(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["template"] = "assets/does_not_exist.pptx"
    report = brand_contract.validate_brand(_make_contract(raw))
    assert report.status == "fail"
    assert any(f.code == "template_missing" for f in report.findings)


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_required_layout_missing_blocks(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["required_layouts"] = list(
        raw["brand"].get("required_layouts") or []
    ) + ["Phantom Layout"]
    report = brand_contract.validate_brand(_make_contract(raw))
    assert report.status == "fail"
    assert any(f.code == "required_layout_missing" for f in report.findings)
    assert "Phantom Layout" in report.layouts_missing


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_slide_master_count_mismatch_blocks(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["expected_slide_master_count"] = 99
    report = brand_contract.validate_brand(_make_contract(raw))
    assert report.status == "fail"
    assert any(f.code == "slide_master_count_mismatch" for f in report.findings)


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_invalid_theme_color_warns(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["theme"]["colors"]["bad"] = "not-a-hex"
    report = brand_contract.validate_brand(_make_contract(raw))
    # Warning, not blocker — bad hex is informational.
    assert "bad" in report.theme_color_invalid
    assert any(
        f.code == "theme_color_invalid_hex" and f.severity == "warning"
        for f in report.findings
    )
    assert report.status == "pass"  # blockers still 0


@pytest.mark.skipif(not TEMPLATE_PATH.exists(), reason="SimCorp template not present.")
def test_size_mismatch_warns_only(canonical_deck):
    raw = copy.deepcopy(canonical_deck)
    raw["brand"]["expected_template_size_bytes"] = 1
    report = brand_contract.validate_brand(_make_contract(raw))
    # Warning level — SHA is the actual blocker for content drift.
    assert any(
        f.code == "template_size_mismatch" and f.severity == "warning"
        for f in report.findings
    )
    assert report.status == "pass"
