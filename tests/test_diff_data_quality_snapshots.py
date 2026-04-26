import sys
from pathlib import Path

from scripts import diff_data_quality_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "data_quality"
    for run_date in ["2026-04-20", "2026-04-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "flags.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-04-22"
    assert diff_script._resolve_baseline_date("2026-04-20") is None


def test_build_snapshot_diff_surfaces_backlog_and_error_drift() -> None:
    baseline_payload = {
        "run_date": "2026-04-20",
        "results": [
            {
                "key": "total_open",
                "label": "Total open FY26 pipeline",
                "severity": "baseline",
                "count": 1260,
            },
            {
                "key": "closed_won_quota_retirement_blank",
                "label": "Closed Won · Quota Retirement blank",
                "severity": "Critical",
                "count": 140,
            },
            {
                "key": "stage3_plus_no_next_step",
                "label": "Stage 3+ with no NextStep",
                "severity": "Important",
                "count": 325,
            },
            {
                "key": "ghost_installation",
                "label": "Active Installation · ExtendedToDate in past",
                "severity": "Critical",
                "error": "timeout",
            },
        ],
    }
    current_payload = {
        "run_date": "2026-04-22",
        "results": [
            {
                "key": "total_open",
                "label": "Total open FY26 pipeline",
                "severity": "baseline",
                "count": 1272,
            },
            {
                "key": "closed_won_quota_retirement_blank",
                "label": "Closed Won · Quota Retirement blank",
                "severity": "Critical",
                "count": 147,
            },
            {
                "key": "stage3_plus_no_next_step",
                "label": "Stage 3+ with no NextStep",
                "severity": "Important",
                "count": 322,
            },
            {
                "key": "ghost_installation",
                "label": "Active Installation · ExtendedToDate in past",
                "severity": "Critical",
                "count": 0,
            },
        ],
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["baseline_run_date"] == "2026-04-20"
    assert payload["current_run_date"] == "2026-04-22"
    assert payload["data_quality"]["check_count_before"] == 4
    assert payload["data_quality"]["check_count_after"] == 4
    assert payload["data_quality"]["error_count_before"] == 1
    assert payload["data_quality"]["error_count_after"] == 0
    assert payload["data_quality"]["severity_totals_before"] == {
        "Critical": 140,
        "Important": 325,
        "Domain": 0,
    }
    assert payload["data_quality"]["severity_totals_after"] == {
        "Critical": 147,
        "Important": 322,
        "Domain": 0,
    }
    assert payload["data_quality"]["baseline_changes"] == [
        {
            "key": "total_open",
            "label": "Total open FY26 pipeline",
            "severity": "baseline",
            "before": 1260,
            "after": 1272,
            "before_error": None,
            "after_error": None,
            "delta": 12,
        }
    ]
    assert payload["data_quality"]["gap_changes"][0] == {
        "key": "closed_won_quota_retirement_blank",
        "label": "Closed Won · Quota Retirement blank",
        "severity": "Critical",
        "before": 140,
        "after": 147,
        "before_error": None,
        "after_error": None,
        "delta": 7,
        "direction": "worse",
    }
    assert payload["data_quality"]["resolved_errors"] == [
        {
            "key": "ghost_installation",
            "label": "Active Installation · ExtendedToDate in past",
            "severity": "Critical",
            "error": "timeout",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "data_quality"
    output_root = tmp_path / "output" / "data_quality_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "flags.json").write_text(
        '{"run_date":"2026-04-22","results":[]}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_data_quality_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "data_quality_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
