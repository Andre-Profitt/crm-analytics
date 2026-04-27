"""Track G-Lite — release packet orchestrator tests.

Positive control runs the orchestrator end-to-end against the live
anchors and asserts publish_decision == publish_ready (matches the
state of the branch as of this commit: all 8 validators clean).

Negative control mutates the deck contract to force a blocker and
asserts publish_decision == blocked.
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from scripts.build_release_packet import build_release_packet


REPO_ROOT = Path(__file__).resolve().parents[1]
APAC_WORKBOOK = Path("/Users/test/Downloads/jesper-tyrer-2026-04-20.xlsx")
APAC_DECK = Path("/Users/test/Downloads/jesper-tyrer-LAND.pptx")
SOFFICE_BIN = Path("/opt/homebrew/bin/soffice")


def _have_soffice() -> bool:
    return SOFFICE_BIN.exists() or bool(shutil.which("soffice"))


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
@pytest.mark.skipif(not _have_soffice(), reason="soffice (LibreOffice) not installed.")
def test_release_packet_publish_ready():
    report = build_release_packet(workbook=APAC_WORKBOOK, pptx=APAC_DECK)
    assert report["publish_decision"] == "publish_ready", [
        s for s in report["summaries"] if s["status"] != "pass"
    ]
    assert report["validator_count"] == 8
    assert report["validators_pass"] == 8
    assert report["validators_fail"] == 0
    assert report["blocker_total"] == 0
    assert report["warning_total"] == 0


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
@pytest.mark.skipif(not _have_soffice(), reason="soffice (LibreOffice) not installed.")
def test_release_packet_blocked_when_contract_mutated(tmp_path):
    """Mutate the deck contract to force a blocker (slide_count_mismatch)
    and assert the orchestrator surfaces publish_decision=blocked."""
    canonical = REPO_ROOT / "config" / "deck_contract.yaml"
    raw = yaml.safe_load(canonical.read_text(encoding="utf-8"))
    # Drop a slide so the contract claims 15 but PPTX has 16 -> blocker.
    raw["profiles"]["director_monthly"]["slides"].pop()
    raw["profiles"]["director_monthly"]["expected_slide_count"] = 15
    mutated_path = tmp_path / "mutated_deck_contract.yaml"
    mutated_path.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")
    report = build_release_packet(
        workbook=APAC_WORKBOOK,
        pptx=APAC_DECK,
        deck_contract_path=mutated_path,
        skip_visual=True,  # baseline anchored to canonical contract; skip
    )
    assert report["publish_decision"] == "blocked"
    assert report["blocker_total"] > 0


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
@pytest.mark.skipif(not _have_soffice(), reason="soffice (LibreOffice) not installed.")
def test_release_packet_skip_visual_marks_skipped():
    """--skip-visual records the visual regression as skipped, not failed."""
    report = build_release_packet(
        workbook=APAC_WORKBOOK, pptx=APAC_DECK, skip_visual=True
    )
    visual_summary = next(
        s for s in report["summaries"] if s["validator"] == "deck_visual_regression"
    )
    assert visual_summary["status"] == "skipped"
    assert report["validators_skipped"] == 1
    # The other 7 still pass -> publish_ready.
    assert report["publish_decision"] == "publish_ready"


def test_release_packet_artifact_digests_include_template():
    """Sanity check: artifact_digests block always includes the template
    SHA-256 since brand_fingerprint depends on it."""
    if not (APAC_WORKBOOK.exists() and APAC_DECK.exists()):
        pytest.skip("Live anchors not present")
    if not _have_soffice():
        pytest.skip("soffice not installed")
    report = build_release_packet(
        workbook=APAC_WORKBOOK, pptx=APAC_DECK, skip_visual=True
    )
    digests = report["artifact_digests"]
    assert "template_pptx" in digests
    assert digests["template_pptx"]["sha256"] is not None
    assert len(digests["template_pptx"]["sha256"]) == 64
