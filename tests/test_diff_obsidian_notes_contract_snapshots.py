import sys
from pathlib import Path

from scripts import diff_obsidian_notes_contract_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "obsidian_notes_contract"
    for run_date in ["2026-04-20", "2026-04-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "obsidian_notes_contract_audit.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-04-22"
    assert diff_script._resolve_baseline_date("2026-04-20") is None


def test_build_snapshot_diff_surfaces_validated_and_issue_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-20",
        "status": "ok",
        "validated": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_history_present": True,
            }
        ],
        "failures": [],
        "warnings": [],
    }
    current_payload = {
        "run_date": "2026-04-22",
        "status": "failed",
        "validated": [],
        "failures": [
            {
                "director": "Jesper Tyrer",
                "issue": "missing_auto_note",
                "message": "missing jesper-tyrer.auto.md",
            }
        ],
        "warnings": [],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["obsidian_notes"]["status_before"] == "ok"
    assert payload["obsidian_notes"]["status_after"] == "failed"
    assert payload["obsidian_notes"]["validated_count_before"] == 1
    assert payload["obsidian_notes"]["validated_count_after"] == 0
    assert payload["obsidian_notes"]["failure_count_before"] == 0
    assert payload["obsidian_notes"]["failure_count_after"] == 1
    assert payload["obsidian_notes"]["validated_changes"] == [
        {
            "change": "removed",
            "director": "Jesper Tyrer",
            "before": {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_history_present": True,
            },
        }
    ]
    assert payload["obsidian_notes"]["failure_changes"]["added"] == [
        {
            "director": "Jesper Tyrer",
            "issue": "missing_auto_note",
            "message": "missing jesper-tyrer.auto.md",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "obsidian_notes_contract"
    output_root = tmp_path / "output" / "obsidian_notes_contract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "obsidian_notes_contract_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_obsidian_notes_contract_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = (
        output_root
        / "2026-04-22"
        / "obsidian_notes_contract_snapshot_diff.json"
    )

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
