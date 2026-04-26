import json
import sys
from pathlib import Path

from scripts import diff_tie_out_snapshots as diff_script
from scripts import validate_tie_out as tie_out_script


def test_write_tieout_artifacts_writes_machine_readable_audit(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(tie_out_script, "ROOT", tmp_path)
    monkeypatch.setattr(tie_out_script, "VAULT", tmp_path / "obsidian")
    monkeypatch.setattr(tie_out_script, "OUTPUT_ROOT", tmp_path / "output" / "tie_out")
    (tmp_path / "obsidian" / "Monthly" / "2026-04").mkdir(parents=True, exist_ok=True)
    (tmp_path / "obsidian" / "Monthly" / "2026-04" / "tie-out.md").write_text(
        "# tie-out\n",
        encoding="utf-8",
    )

    all_results = [
        {
            "director": "Jesper Tyrer",
            "territory": "APAC",
            "slug": "jesper-tyrer",
            "results": [
                ("Open Land pipeline deals", 8, 8, 8, 8, "match"),
                (
                    "Q1 Land losses, count",
                    14,
                    14,
                    12,
                    14,
                    "Extract vs Regional mismatch",
                ),
            ],
        }
    ]

    output_dir, mismatches = tie_out_script.write_tieout_artifacts(
        "2026-04-22", all_results
    )

    assert mismatches == 1
    audit_path = output_dir / "tie_out_audit.json"
    payload = json.loads(audit_path.read_text(encoding="utf-8"))
    assert payload["status"] == "failed"
    assert payload["checks"] == 2
    assert payload["mismatches"] == 1
    assert payload["directors_with_mismatches"] == 1
    assert payload["directors"][0]["slug"] == "jesper-tyrer"
    assert payload["directors"][0]["mismatch_count"] == 1


def test_diff_tie_out_snapshot_surfaces_metric_status_changes() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "checks": 90,
        "mismatches": 0,
        "directors_audited": 9,
        "directors_with_mismatches": 0,
        "failures": [],
        "directors": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "slug": "jesper-tyrer",
                "mismatch_count": 0,
                "metrics": [
                    {
                        "metric": "Q1 Land losses, count",
                        "salesforce": 14,
                        "extract": 14,
                        "regional": 14,
                        "deck": 14,
                        "status": "match",
                    }
                ],
            }
        ],
    }
    current_payload = {
        "run_date": "2026-05-22",
        "status": "failed",
        "checks": 90,
        "mismatches": 1,
        "directors_audited": 9,
        "directors_with_mismatches": 1,
        "failures": [],
        "directors": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "slug": "jesper-tyrer",
                "mismatch_count": 1,
                "metrics": [
                    {
                        "metric": "Q1 Land losses, count",
                        "salesforce": 14,
                        "extract": 14,
                        "regional": 12,
                        "deck": 14,
                        "status": "Extract vs Regional mismatch",
                    }
                ],
            }
        ],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["tie_out"]["status_before"] == "ok"
    assert payload["tie_out"]["status_after"] == "failed"
    assert payload["tie_out"]["mismatches_before"] == 0
    assert payload["tie_out"]["mismatches_after"] == 1
    assert payload["tie_out"]["director_changes"] == [
        {
            "change": "modified",
            "slug": "jesper-tyrer",
            "changes": {
                "metadata": {
                    "mismatch_count": {
                        "before": 0,
                        "after": 1,
                        "delta": 1.0,
                    }
                },
                "metric_changes": [
                    {
                        "change": "modified",
                        "metric": "Q1 Land losses, count",
                        "changes": {
                            "regional": {
                                "before": 14,
                                "after": 12,
                                "delta": -2.0,
                            },
                            "status": {
                                "before": "match",
                                "after": "Extract vs Regional mismatch",
                            },
                        },
                    }
                ],
            },
        }
    ]


def test_main_writes_skipped_tie_out_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "tie_out"
    output_root = tmp_path / "output" / "tie_out_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "tie_out_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_tie_out_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "tie_out_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
