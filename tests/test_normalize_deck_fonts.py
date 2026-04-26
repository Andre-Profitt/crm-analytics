import json
import sys
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from scripts import normalize_deck_fonts as normalize_script


def _write_pptx(path: Path, slide_xml: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with ZipFile(path, "w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", "<Types/>")
        zf.writestr("ppt/slides/slide1.xml", slide_xml)


def test_main_rewrites_calibri_typeface(tmp_path: Path, monkeypatch) -> None:
    decks_dir = tmp_path / "decks"
    pptx_path = decks_dir / "sample.pptx"
    _write_pptx(pptx_path, '<a:rPr><a:latin typeface="Calibri"/></a:rPr>')

    monkeypatch.setattr(
        normalize_script,
        "OUTPUT_ROOT",
        tmp_path / "output" / "deck_font_normalization",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "normalize_deck_fonts.py",
            "--date",
            "2026-04-22",
            "--decks-dir",
            str(decks_dir),
        ],
    )

    assert normalize_script.main() == 0
    with ZipFile(pptx_path) as zf:
        slide_xml = zf.read("ppt/slides/slide1.xml").decode("utf-8")
    assert 'typeface="Arial"' in slide_xml
    assert 'typeface="Calibri"' not in slide_xml

    audit_path = (
        tmp_path
        / "output"
        / "deck_font_normalization"
        / "2026-04-22"
        / "deck_font_normalization.json"
    )
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "ok"
    assert payload["modified_deck_count"] == 1
    assert payload["replacement_count"] == 1


def test_main_leaves_clean_deck_unchanged(tmp_path: Path, monkeypatch) -> None:
    decks_dir = tmp_path / "decks"
    pptx_path = decks_dir / "sample.pptx"
    _write_pptx(pptx_path, '<a:rPr><a:latin typeface="Arial"/></a:rPr>')

    monkeypatch.setattr(
        normalize_script,
        "OUTPUT_ROOT",
        tmp_path / "output" / "deck_font_normalization",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "normalize_deck_fonts.py",
            "--date",
            "2026-04-22",
            "--decks-dir",
            str(decks_dir),
        ],
    )

    assert normalize_script.main() == 0
    with ZipFile(pptx_path) as zf:
        slide_xml = zf.read("ppt/slides/slide1.xml").decode("utf-8")
    assert 'typeface="Arial"' in slide_xml

    audit_path = (
        tmp_path
        / "output"
        / "deck_font_normalization"
        / "2026-04-22"
        / "deck_font_normalization.json"
    )
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["modified_deck_count"] == 0
