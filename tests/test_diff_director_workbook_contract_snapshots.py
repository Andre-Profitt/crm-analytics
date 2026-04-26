import sys
from pathlib import Path

from scripts import diff_director_workbook_contract_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "director_workbook_contract"
    for run_date in ["2026-04-22", "2026-05-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "director_workbook_contract_audit.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-05-22"
    assert diff_script._resolve_baseline_date("2026-04-22") is None


def test_build_snapshot_diff_surfaces_validated_and_issue_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "scope": "all",
        "validated": [
            {
                "slug": "jesper-tyrer",
                "workbook_path": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
                "sheet_count": 15,
                "historical_sheets": ["Q1 Snapshot Trend", "Q2 Snapshot Trend"],
            }
        ],
        "failures": [],
        "warnings": [],
    }
    current_payload = {
        "run_date": "2026-08-10",
        "status": "failed",
        "scope": "all",
        "validated": [
            {
                "slug": "jesper-tyrer",
                "workbook_path": "output/director_live_workbooks/2026-08-10/jesper-tyrer.xlsx",
                "sheet_count": 14,
                "historical_sheets": ["Q2 Snapshot Trend"],
            }
        ],
        "failures": [
            {
                "slug": "jesper-tyrer",
                "issue": "missing_sheet",
                "message": "Q3 Snapshot Trend",
            }
        ],
        "warnings": [
            {
                "slug": "jesper-tyrer",
                "issue": "unexpected_optional_sheet",
                "message": "Pipeline Inspection Forward",
            }
        ],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["workbook_contract"]["status_before"] == "ok"
    assert payload["workbook_contract"]["status_after"] == "failed"
    assert payload["workbook_contract"]["failure_count_before"] == 0
    assert payload["workbook_contract"]["failure_count_after"] == 1
    assert payload["workbook_contract"]["warning_count_before"] == 0
    assert payload["workbook_contract"]["warning_count_after"] == 1
    assert payload["workbook_contract"]["validated_changes"] == [
        {
            "change": "modified",
            "slug": "jesper-tyrer",
            "changes": {
                "historical_sheets": {
                    "before": ["Q1 Snapshot Trend", "Q2 Snapshot Trend"],
                    "after": ["Q2 Snapshot Trend"],
                },
                "sheet_count": {
                    "before": 15,
                    "after": 14,
                    "delta": -1.0,
                },
                "workbook_path": {
                    "before": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
                    "after": "output/director_live_workbooks/2026-08-10/jesper-tyrer.xlsx",
                },
            },
        }
    ]
    assert payload["workbook_contract"]["failure_changes"]["added"] == [
        {
            "slug": "jesper-tyrer",
            "issue": "missing_sheet",
            "message": "Q3 Snapshot Trend",
        }
    ]
    assert payload["workbook_contract"]["warning_changes"]["added"] == [
        {
            "slug": "jesper-tyrer",
            "issue": "unexpected_optional_sheet",
            "message": "Pipeline Inspection Forward",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "director_workbook_contract"
    output_root = tmp_path / "output" / "director_workbook_contract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "director_workbook_contract_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_director_workbook_contract_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = (
        output_root
        / "2026-04-22"
        / "director_workbook_contract_snapshot_diff.json"
    )

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
