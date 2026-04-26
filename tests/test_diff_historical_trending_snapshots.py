import sys
from pathlib import Path

from scripts import diff_historical_trending_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "historical_trending_extract"
    for run_date in ["2026-04-22", "2026-05-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "historical_trending_extract_audit.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-05-22"
    assert diff_script._resolve_baseline_date("2026-04-22") is None


def test_build_snapshot_diff_surfaces_sheet_and_failure_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "scope": "all",
        "retrospective_quarter_title": "Q1 2026",
        "current_quarter_title": "Q2 2026",
        "processed": [
            {
                "slug": "jesper-tyrer",
                "workbook_path": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
                "sheets": [
                    {
                        "sheet_name": "Q1 Snapshot Trend",
                        "report_id": "00OT-q1",
                        "row_count": 15,
                        "snapshot_dates": ["2026-01-01", "2026-04-15"],
                        "latest_snapshot_date": "2026-04-15",
                    },
                    {
                        "sheet_name": "Q2 Snapshot Trend",
                        "report_id": "00OT-q2",
                        "row_count": 0,
                        "snapshot_dates": ["2026-04-15"],
                        "latest_snapshot_date": "2026-04-15",
                    },
                ],
            }
        ],
        "failures": [],
    }
    current_payload = {
        "run_date": "2026-08-10",
        "status": "failed",
        "scope": "all",
        "retrospective_quarter_title": "Q2 2026",
        "current_quarter_title": "Q3 2026",
        "processed": [
            {
                "slug": "jesper-tyrer",
                "workbook_path": "output/director_live_workbooks/2026-08-10/jesper-tyrer.xlsx",
                "sheets": [
                    {
                        "sheet_name": "Q2 Snapshot Trend",
                        "report_id": "00OT-q2",
                        "row_count": 0,
                        "snapshot_dates": ["2026-04-15"],
                        "latest_snapshot_date": "2026-04-15",
                    }
                ],
            }
        ],
        "failures": [
            {
                "slug": "jesper-tyrer",
                "sheet_name": "Q2 Snapshot Trend",
                "report_id": "00OT-q2",
                "issues": ["snapshot_review_month_mismatch"],
                "latest_snapshot_date": "2026-04-15",
                "run_date": "2026-08-10",
            }
        ],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["historical_trending"]["status_before"] == "ok"
    assert payload["historical_trending"]["status_after"] == "failed"
    assert payload["historical_trending"]["retrospective_quarter_before"] == "Q1 2026"
    assert payload["historical_trending"]["current_quarter_after"] == "Q3 2026"
    assert payload["historical_trending"]["failure_count_before"] == 0
    assert payload["historical_trending"]["failure_count_after"] == 1
    assert payload["historical_trending"]["processed_changes"] == [
        {
            "change": "modified",
            "slug": "jesper-tyrer",
            "changes": {
                "metadata": {
                    "workbook_path": {
                        "before": "output/director_live_workbooks/2026-04-22/jesper-tyrer.xlsx",
                        "after": "output/director_live_workbooks/2026-08-10/jesper-tyrer.xlsx",
                    }
                },
                "sheet_changes": [
                    {
                        "change": "removed",
                        "sheet_name": "Q1 Snapshot Trend",
                        "before": {
                            "sheet_name": "Q1 Snapshot Trend",
                            "report_id": "00OT-q1",
                            "row_count": 15,
                            "snapshot_dates": ["2026-01-01", "2026-04-15"],
                            "latest_snapshot_date": "2026-04-15",
                        },
                    }
                ],
            },
        }
    ]
    assert payload["historical_trending"]["failure_changes"]["added"] == [
        {
            "slug": "jesper-tyrer",
            "sheet_name": "Q2 Snapshot Trend",
            "report_id": "00OT-q2",
            "issues": ["snapshot_review_month_mismatch"],
            "latest_snapshot_date": "2026-04-15",
            "run_date": "2026-08-10",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "historical_trending_extract"
    output_root = tmp_path / "output" / "historical_trending_snapshot_diff"
    current_dir = audit_root / "2026-08-10"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "historical_trending_extract_audit.json").write_text(
        '{"run_date":"2026-08-10","status":"failed"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["diff_historical_trending_snapshots.py", "--current-date", "2026-08-10"],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-08-10" / "summary.md"
    payload = output_root / "2026-08-10" / "historical_trending_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
