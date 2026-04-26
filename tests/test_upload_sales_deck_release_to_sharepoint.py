from __future__ import annotations

import json
import sys
from pathlib import Path

from scripts import upload_sales_deck_release_to_sharepoint as upload


def _write(path: Path, content: str = "x") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return path


def _source_backed_packet(tmp_path: Path, *, upload_ready: bool = True) -> Path:
    bundle_manifest = tmp_path / "bundle_manifest.json"
    bundle_zip = _write(tmp_path / "source_backed_release_bundle.zip")
    deck = _write(tmp_path / "source_backed_monthly_review.pptx")
    analyst = _write(tmp_path / "source_backed_analyst_workbook.xlsx")
    thinkcell = _write(tmp_path / "thinkcell_source.xlsx")
    ppttc = _write(tmp_path / "thinkcell_data.ppttc")
    bundle_manifest.write_text(
        json.dumps(
            {
                "status": "ok",
                "sharepoint_handoff": {
                    "folder_name": (
                        "Sales Director Monthly Review - 2026-04-30 - dry-run"
                    ),
                    "upload_ready": upload_ready,
                },
            }
        ),
        encoding="utf-8",
    )
    packet = {
        "schema_version": "monthly_platform.source_backed_release_packet.v1",
        "status": "ok",
        "publish_recommendation": "publish",
        "snapshot_date": "2026-04-30",
        "run_id": "dry-run",
        "summary": {"release_bundle_upload_ready": upload_ready},
        "release_checks": [{"name": "all_good", "status": "pass"}],
        "artifacts": {
            "release_bundle_manifest": str(bundle_manifest),
            "release_bundle_zip": str(bundle_zip),
            "source_backed_deck": str(deck),
            "analyst_workbook": str(analyst),
            "thinkcell_workbook": str(thinkcell),
            "thinkcell_ppttc": str(ppttc),
        },
    }
    packet_path = tmp_path / "source_backed_release_packet.json"
    packet_path.write_text(json.dumps(packet), encoding="utf-8")
    return packet_path


def test_source_backed_dry_run_plans_bundle_and_deliverables(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    packet_path = _source_backed_packet(tmp_path)
    output_path = tmp_path / "sharepoint_upload_plan.json"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "upload_sales_deck_release_to_sharepoint.py",
            "--release-packet-json",
            str(packet_path),
            "--folder-prefix",
            "General/Monthly",
            "--dry-run",
            "--output-path",
            str(output_path),
        ],
    )

    exit_code = upload.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "planned"
    assert payload["source_backed"] is True
    assert payload["publish_ready"] is True
    assert payload["planned_count"] == 5
    assert payload["missing_count"] == 0
    assert payload["folder"].endswith(
        "Q2 2026/Sales Director Monthly Review - 2026-04-30 - dry-run"
    )
    assert payload["planned"][0]["publish_name"] == (
        "Sales Director Monthly Source-Backed Release Bundle - April 2026.zip"
    )
    assert json.loads(output_path.read_text(encoding="utf-8"))["status"] == "planned"


def test_source_backed_dry_run_blocks_when_release_packet_not_publish_ready(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    packet_path = _source_backed_packet(tmp_path, upload_ready=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "upload_sales_deck_release_to_sharepoint.py",
            "--release-packet-json",
            str(packet_path),
            "--dry-run",
        ],
    )

    exit_code = upload.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason"] == "release_packet_not_publish_ready"


def test_source_backed_dry_run_blocks_missing_planned_asset(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    packet_path = _source_backed_packet(tmp_path)
    packet = json.loads(packet_path.read_text(encoding="utf-8"))
    Path(packet["artifacts"]["source_backed_deck"]).unlink()
    packet_path.write_text(json.dumps(packet), encoding="utf-8")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "upload_sales_deck_release_to_sharepoint.py",
            "--release-packet-json",
            str(packet_path),
            "--dry-run",
        ],
    )

    exit_code = upload.main()
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 2
    assert payload["status"] == "blocked"
    assert payload["reason"] == "missing_publish_asset"
    assert payload["missing_count"] == 1
