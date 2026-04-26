import sys
from pathlib import Path

from scripts import diff_source_contract_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "source_contract_audit"
    for run_date in ["2026-04-22", "2026-05-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "source_contract_audit.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-05-22"
    assert diff_script._resolve_baseline_date("2026-04-22") is None


def test_build_snapshot_diff_surfaces_status_and_issue_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "active_lane": {
            "status": "ok",
            "dashboards": [
                {
                    "dashboard_id": "01Z",
                    "status": "ok",
                    "component_count": 13,
                    "component_report_ids": ["00O1", "00O2"],
                    "missing_required_report_ids": [],
                }
            ],
            "pi_list_views": [
                {
                    "territory": "APAC",
                    "status": "ok",
                    "status_code": 200,
                    "list_view_id": "00B1",
                    "row_probe_count": 1,
                    "sample_fields": ["Name"],
                }
            ],
            "historical_reports": [
                {
                    "director_slug": "jesper-tyrer",
                    "quarter_label": "Q1",
                    "sheet_name": "Q1 Snapshot Trend",
                    "status": "ok",
                    "report_id": "00OT-Q1",
                    "expected_start": "2026-01-01",
                    "expected_end": "2026-03-31",
                    "actual_start": "2026-01-01",
                    "actual_end": "2026-03-31",
                    "latest_snapshot_date": "2026-04-12",
                    "snapshot_dates": ["2026-04-12"],
                    "issues": [],
                }
            ],
        },
        "candidate_forward_quarter": {
            "status": "ok",
            "quarter_title": "Q3 2026",
            "pi_list_views": [
                {
                    "territory": "APAC",
                    "status": "ok",
                    "status_code": 200,
                    "list_view_id": "00B-Q3",
                    "row_probe_count": 1,
                    "sample_fields": ["Name"],
                }
            ],
            "historical_reports": [
                {
                    "director_slug": "APAC",
                    "quarter_label": "Q3",
                    "status": "ok",
                    "report_id": "00OT-Q3",
                    "expected_start": "2026-07-01",
                    "expected_end": "2026-09-30",
                    "actual_start": "2026-07-01",
                    "actual_end": "2026-09-30",
                    "latest_snapshot_date": "2026-04-15",
                    "snapshot_dates": ["2026-04-15"],
                    "issues": [],
                }
            ],
            "missing_config": [],
        },
    }
    current_payload = {
        "run_date": "2026-08-10",
        "active_lane": {
            "status": "failed",
            "dashboards": [
                {
                    "dashboard_id": "01Z",
                    "status": "ok",
                    "component_count": 15,
                    "component_report_ids": ["00O1", "00O2", "00O3"],
                    "missing_required_report_ids": [],
                }
            ],
            "pi_list_views": [
                {
                    "territory": "APAC",
                    "status": "ok",
                    "status_code": 200,
                    "list_view_id": "00B1",
                    "row_probe_count": 1,
                    "sample_fields": ["Name"],
                }
            ],
            "historical_reports": [
                {
                    "director_slug": "jesper-tyrer",
                    "quarter_label": "Q2",
                    "sheet_name": "Q2 Snapshot Trend",
                    "status": "failed",
                    "report_id": "00OT-Q2",
                    "expected_start": "2026-04-01",
                    "expected_end": "2026-06-30",
                    "actual_start": "2026-04-01",
                    "actual_end": "2026-06-30",
                    "latest_snapshot_date": "2026-04-15",
                    "snapshot_dates": ["2026-04-15"],
                    "issues": ["snapshot_review_month_mismatch"],
                }
            ],
        },
        "candidate_forward_quarter": {
            "status": "warning",
            "quarter_title": "Q4 2026",
            "pi_list_views": [],
            "historical_reports": [],
            "missing_config": [
                {
                    "territory": "APAC",
                    "source": "forward_quarter_pi_list_views",
                    "quarter_label": "Q4",
                },
                {
                    "territory": "APAC",
                    "source": "forward_quarter_historical_trending_report_ids",
                    "quarter_label": "Q4",
                },
            ],
        },
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["active_lane"]["status_before"] == "ok"
    assert payload["active_lane"]["status_after"] == "failed"
    assert payload["active_lane"]["issue_delta"]["new"] == {
        "snapshot_review_month_mismatch": 1
    }
    assert payload["active_lane"]["quarter_labels_before"] == ["Q1"]
    assert payload["active_lane"]["quarter_labels_after"] == ["Q2"]
    assert len(payload["active_lane"]["dashboard_changes"]) == 1
    assert payload["candidate_lane"]["quarter_title_before"] == "Q3 2026"
    assert payload["candidate_lane"]["quarter_title_after"] == "Q4 2026"
    assert payload["candidate_lane"]["missing_config_count_before"] == 0
    assert payload["candidate_lane"]["missing_config_count_after"] == 2
    assert len(payload["candidate_lane"]["missing_config_changes"]["added"]) == 2


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "source_contract_audit"
    output_root = tmp_path / "output" / "source_contract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "source_contract_audit.json").write_text(
        '{"run_date":"2026-04-22","active_lane":{"status":"ok"},"candidate_forward_quarter":{"status":"ok","quarter_title":"Q3 2026"}}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["diff_source_contract_snapshots.py", "--current-date", "2026-04-22"],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "source_contract_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
