import sys
from pathlib import Path

from scripts import diff_sharepoint_analysis_contract_snapshots as diff_script


def test_build_snapshot_diff_surfaces_workbook_and_issue_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "validated": [
            {
                "workbook_id": "master",
                "workbook_type": "master",
                "territory": None,
                "workbook_path": "output/sharepoint/FY26 Pipeline Review, All Territories.xlsx",
                "sheet_count": 42,
                "file_size_bytes": 100,
                "key_sheet_row_counts": {"Summary": 10, "Methodology": 5},
            }
        ],
        "failures": [],
        "warnings": [],
    }
    current_payload = {
        "run_date": "2026-05-22",
        "status": "failed",
        "validated": [
            {
                "workbook_id": "master",
                "workbook_type": "master",
                "territory": None,
                "workbook_path": "output/sharepoint/FY26 Pipeline Review, All Territories.xlsx",
                "sheet_count": 41,
                "file_size_bytes": 120,
                "key_sheet_row_counts": {"Summary": 12, "Methodology": 5},
            }
        ],
        "failures": [
            {
                "workbook_id": "dashboard_q1",
                "issue": "missing_required_sheets",
                "message": "PI Summary",
            }
        ],
        "warnings": [
            {
                "workbook_id": "FY26 Pipeline Review, Unexpected.xlsx",
                "issue": "unexpected_regional_workbook",
                "message": "FY26 Pipeline Review, Unexpected.xlsx",
            }
        ],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["sharepoint_analysis_contract"]["status_before"] == "ok"
    assert payload["sharepoint_analysis_contract"]["status_after"] == "failed"
    assert payload["sharepoint_analysis_contract"]["failure_count_before"] == 0
    assert payload["sharepoint_analysis_contract"]["failure_count_after"] == 1
    assert payload["sharepoint_analysis_contract"]["warning_count_before"] == 0
    assert payload["sharepoint_analysis_contract"]["warning_count_after"] == 1
    assert payload["sharepoint_analysis_contract"]["validated_changes"] == [
        {
            "change": "modified",
            "workbook_id": "master",
            "changes": {
                "metadata": {
                    "file_size_bytes": {
                        "before": 100,
                        "after": 120,
                        "delta": 20.0,
                    },
                    "sheet_count": {
                        "before": 42,
                        "after": 41,
                        "delta": -1.0,
                    },
                },
                "key_sheet_row_counts": {
                    "Summary": {
                        "before": 10,
                        "after": 12,
                        "delta": 2.0,
                    }
                },
            },
        }
    ]
    assert payload["sharepoint_analysis_contract"]["failure_changes"]["added"] == [
        {
            "workbook_id": "dashboard_q1",
            "issue": "missing_required_sheets",
            "message": "PI Summary",
        }
    ]
    assert payload["sharepoint_analysis_contract"]["warning_changes"]["added"] == [
        {
            "workbook_id": "FY26 Pipeline Review, Unexpected.xlsx",
            "issue": "unexpected_regional_workbook",
            "message": "FY26 Pipeline Review, Unexpected.xlsx",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "sharepoint_analysis_contract"
    output_root = tmp_path / "output" / "sharepoint_analysis_contract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "sharepoint_analysis_contract_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_sharepoint_analysis_contract_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = (
        output_root
        / "2026-04-22"
        / "sharepoint_analysis_contract_snapshot_diff.json"
    )

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
