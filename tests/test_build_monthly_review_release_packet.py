import json
import sys
from pathlib import Path

from scripts import build_monthly_review_release_packet as packet_script


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_build_release_packet_surfaces_publish_blockers(tmp_path: Path) -> None:
    repo_root = tmp_path
    manifest_path = repo_root / "output" / "pipeline_logs" / "2026-04-22" / "manifest.json"
    manifest = {
        "run_date": "2026-04-22",
        "started_at": "2026-04-22T10:00:00",
        "finished_at": "2026-04-22T10:30:00",
        "steps": [
            {"name": "0_source_contract_preflight", "status": "ok"},
            {"name": "3b_validate_deck_delivery_contract", "status": "ok"},
            {"name": "4_validate_tie_out", "status": "ok"},
        ],
        "outputs": {
            "extracts": [{"file": "a.xlsx"}],
            "decks": [{"file": "deck.pptx"}],
            "reports": [{"file": "report.xlsx"}],
        },
    }
    _write_json(manifest_path, manifest)
    _write_json(
        repo_root / "output" / "source_contract_audit" / "2026-04-22" / "source_contract_audit.json",
        {"run_date": "2026-04-22", "active_lane": {"status": "ok"}, "candidate_forward_quarter": {"status": "ok"}},
    )
    _write_json(
        repo_root / "output" / "deck_delivery_contract" / "2026-04-22" / "deck_delivery_contract_audit.json",
        {
            "run_date": "2026-04-22",
            "status": "ok",
            "validated_director_count": 9,
            "expected_director_count": 9,
            "failures": [],
            "warnings": [],
        },
    )
    _write_json(
        repo_root / "output" / "tie_out" / "2026-04-22" / "tie_out_audit.json",
        {
            "run_date": "2026-04-22",
            "status": "ok",
            "checks": 90,
            "mismatches": 0,
            "directors_audited": 9,
        },
    )
    _write_json(
        repo_root / "output" / "deck_font_audit" / "2026-04-22" / "deck_font_audit.json",
        {
            "run_date": "2026-04-22",
            "status": "warning",
            "deck_count": 10,
            "decks_with_issues": 9,
            "failures": [],
        },
    )

    packet = packet_script.build_monthly_review_release_packet(
        manifest=manifest,
        manifest_path=manifest_path,
        repo_root=repo_root,
    )

    assert packet["status"] == "blocked"
    assert packet["publish_ready"] is False
    assert packet["pipeline_ok"] is True
    assert packet["output_counts"] == {"extracts": 1, "decks": 1, "reports": 1}
    assert "Deck font audit is `warning` with `9` deck(s) showing issues." in packet["publish_blockers"]
    assert packet["deck_font_audit"]["decks_with_issues"] == 9


def test_main_writes_bundle_and_latest_files(tmp_path: Path, monkeypatch) -> None:
    logs_root = tmp_path / "output" / "pipeline_logs"
    output_root = tmp_path / "output" / "monthly_review_release_packets"
    manifest_path = logs_root / "2026-04-22" / "manifest.json"
    _write_json(
        manifest_path,
        {
            "run_date": "2026-04-22",
            "started_at": "2026-04-22T10:00:00",
            "finished_at": "2026-04-22T10:05:00",
            "steps": [
                {"name": "0_source_contract_preflight", "status": "failed"},
            ],
            "outputs": {"extracts": [], "decks": [], "reports": []},
        },
    )
    _write_json(
        tmp_path / "output" / "source_contract_audit" / "2026-04-22" / "source_contract_audit.json",
        {"run_date": "2026-04-22", "active_lane": {"status": "failed"}},
    )

    monkeypatch.setattr(packet_script, "ROOT", tmp_path)
    monkeypatch.setattr(packet_script, "DEFAULT_LOGS_ROOT", logs_root)
    monkeypatch.setattr(packet_script, "DEFAULT_OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_monthly_review_release_packet.py",
            "--date",
            "2026-04-22",
            "--output-root",
            str(output_root),
        ],
    )

    assert packet_script.main() == 0
    latest_json = json.loads((output_root / "latest.json").read_text(encoding="utf-8"))
    latest_md = (output_root / "latest.md").read_text(encoding="utf-8")

    assert latest_json["status"] == "blocked"
    assert latest_json["publish_ready"] is False
    assert latest_json["packet_dir"] == str(output_root / "2026-04-22")
    assert "Active source contract status is `failed`." in latest_md
