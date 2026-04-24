from __future__ import annotations

import json
from pathlib import Path
import sys
import zipfile


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_source_backed_release_bundle import build_release_bundle  # noqa: E402


def test_build_release_bundle_copies_hashes_and_zips_artifacts(tmp_path: Path) -> None:
    deck = tmp_path / "deck.pptx"
    workbook = tmp_path / "workbook.xlsx"
    audit = tmp_path / "audit.json"
    deck.write_bytes(b"deck")
    workbook.write_bytes(b"workbook")
    audit.write_text('{"status":"ok"}\n', encoding="utf-8")

    payload = build_release_bundle(
        snapshot_date="2026-04-30",
        source_run_id="run-a",
        output_dir=tmp_path / "bundle",
        artifacts=[
            {"name": "deck", "category": "deliverables", "path": str(deck), "required": True},
            {
                "name": "workbook",
                "category": "deliverables",
                "path": str(workbook),
                "required": True,
            },
            {"name": "audit", "category": "audits", "path": str(audit), "required": True},
        ],
    )

    assert payload["status"] == "ok"
    assert payload["summary"]["artifact_count"] == 3
    assert payload["summary"]["missing_required_artifact_count"] == 0
    assert payload["sharepoint_handoff"]["upload_ready"] is True
    assert all(artifact["sha256"] for artifact in payload["artifacts"])
    assert Path(payload["output_path"]).exists()
    assert Path(payload["zip_path"]).exists()
    with zipfile.ZipFile(payload["zip_path"]) as archive:
        names = set(archive.namelist())
    assert "deliverables/deck.pptx" in names
    assert "deliverables/workbook.xlsx" in names
    assert "audits/audit.json" in names
    assert "source_backed_release_bundle_manifest.json" in names
    assert "summary.md" in names


def test_build_release_bundle_blocks_missing_required_artifact(tmp_path: Path) -> None:
    payload = build_release_bundle(
        snapshot_date="2026-04-30",
        source_run_id="run-a",
        output_dir=tmp_path / "bundle",
        artifacts=[
            {
                "name": "missing_deck",
                "category": "deliverables",
                "path": str(tmp_path / "missing.pptx"),
                "required": True,
            }
        ],
    )

    assert payload["status"] == "blocked"
    assert payload["summary"]["missing_required_artifact_count"] == 1
    assert payload["sharepoint_handoff"]["upload_ready"] is False
    assert json.loads(Path(payload["output_path"]).read_text(encoding="utf-8"))["status"] == "blocked"
