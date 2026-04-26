import sys
from pathlib import Path

from scripts import diff_deck_font_audit_snapshots as diff_script


def test_build_snapshot_diff_surfaces_font_issue_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-20",
        "status": "ok",
        "deck_count": 2,
        "decks_with_issues": 0,
        "decks": [
            {
                "deck": "jesper-tyrer-LAND",
                "font_missing_overall": [],
                "font_substituted_overall": [],
                "font_missing_count": 0,
                "font_substituted_count": 0,
            }
        ],
        "failures": [],
    }
    current_payload = {
        "run_date": "2026-04-22",
        "status": "warning",
        "deck_count": 2,
        "decks_with_issues": 1,
        "decks": [
            {
                "deck": "jesper-tyrer-LAND",
                "font_missing_overall": ["calibri"],
                "font_substituted_overall": [],
                "font_missing_count": 1,
                "font_substituted_count": 0,
            }
        ],
        "failures": [],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["font_audit"]["status_before"] == "ok"
    assert payload["font_audit"]["status_after"] == "warning"
    assert payload["font_audit"]["decks_with_issues_before"] == 0
    assert payload["font_audit"]["decks_with_issues_after"] == 1
    assert payload["font_audit"]["deck_changes"] == [
        {
            "change": "modified",
            "deck": "jesper-tyrer-LAND",
            "changes": {
                "font_missing_count": {
                    "before": 0,
                    "after": 1,
                    "delta": 1,
                },
                "font_missing_overall": {
                    "added": ["calibri"],
                    "removed": [],
                },
            },
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "deck_font_audit"
    output_root = tmp_path / "output" / "deck_font_audit_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "deck_font_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"warning"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_deck_font_audit_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "deck_font_audit_snapshot_diff.json"
    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
