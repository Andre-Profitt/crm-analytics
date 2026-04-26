import sys
from pathlib import Path

from scripts import diff_director_live_extract_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_audit(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "director_live_extract"
    for run_date in ["2026-04-22", "2026-05-22", "2026-08-10"]:
        path = audit_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "director_live_extract_audit.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-05-22"
    assert diff_script._resolve_baseline_date("2026-04-22") is None


def test_build_snapshot_diff_surfaces_metric_and_failure_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "scope": "all",
        "territories_requested": ["APAC", "Canada"],
        "processed": [
            {
                "territory": "APAC",
                "director": "Jesper Tyrer",
                "analysis_year": 2026,
                "fy_label": "FY26",
                "counts": {
                    "pipeline_open": 100,
                    "pipeline_inspection": 12,
                    "pipeline_inspection_forward": 3,
                },
                "arr": {
                    "pipeline_open_eur": 1000.0,
                },
                "pi_source": {
                    "list_view_id": "00B-active",
                    "scope": "FY26",
                    "deal_count": 12,
                },
                "forward_quarter_pi": {
                    "status": "configured",
                    "quarter_label": "Q3",
                    "quarter_title": "Q3 2026",
                    "list_view_id": "00B-q3",
                    "deal_count": 3,
                },
            }
        ],
        "failures": [],
        "query_telemetry_totals": {
            "queries": 8,
            "rows": 1000,
            "duration_ms": 4000,
        },
    }
    current_payload = {
        "run_date": "2026-05-22",
        "status": "failed",
        "scope": "all",
        "territories_requested": ["APAC", "Canada"],
        "processed": [
            {
                "territory": "APAC",
                "director": "Jesper Tyrer",
                "analysis_year": 2026,
                "fy_label": "FY26",
                "counts": {
                    "pipeline_open": 110,
                    "pipeline_inspection": 10,
                    "pipeline_inspection_forward": 0,
                },
                "arr": {
                    "pipeline_open_eur": 1200.0,
                },
                "pi_source": {
                    "list_view_id": "00B-active",
                    "scope": "FY26",
                    "deal_count": 10,
                },
                "forward_quarter_pi": {
                    "status": "unavailable",
                    "quarter_label": "Q3",
                    "quarter_title": "Q3 2026",
                    "list_view_id": "",
                    "deal_count": 0,
                },
            }
        ],
        "failures": [
            {
                "territory": "Canada",
                "error_type": "RuntimeError",
                "message": "source missing",
            }
        ],
        "query_telemetry_totals": {
            "queries": 9,
            "rows": 1200,
            "duration_ms": 5000,
        },
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["extract"]["status_before"] == "ok"
    assert payload["extract"]["status_after"] == "failed"
    assert payload["extract"]["failure_count_before"] == 0
    assert payload["extract"]["failure_count_after"] == 1
    assert payload["extract"]["query_telemetry_totals"]["queries"] == {
        "before": 8,
        "after": 9,
        "delta": 1.0,
    }
    assert payload["extract"]["territory_changes"] == [
        {
            "change": "modified",
            "territory": "APAC",
            "changes": {
                "counts": {
                    "pipeline_inspection": {
                        "before": 12,
                        "after": 10,
                        "delta": -2.0,
                    },
                    "pipeline_inspection_forward": {
                        "before": 3,
                        "after": 0,
                        "delta": -3.0,
                    },
                    "pipeline_open": {
                        "before": 100,
                        "after": 110,
                        "delta": 10.0,
                    },
                },
                "arr": {
                    "pipeline_open_eur": {
                        "before": 1000.0,
                        "after": 1200.0,
                        "delta": 200.0,
                    }
                },
                "pi_source": {
                    "deal_count": {
                        "before": 12,
                        "after": 10,
                        "delta": -2.0,
                    }
                },
                "forward_quarter_pi": {
                    "deal_count": {
                        "before": 3,
                        "after": 0,
                        "delta": -3.0,
                    },
                    "list_view_id": {
                        "before": "00B-q3",
                        "after": "",
                    },
                    "status": {
                        "before": "configured",
                        "after": "unavailable",
                    },
                },
            },
        }
    ]
    assert payload["extract"]["failure_changes"]["added"] == [
        {
            "territory": "Canada",
            "error_type": "RuntimeError",
            "message": "source missing",
        }
    ]


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "director_live_extract"
    output_root = tmp_path / "output" / "director_live_extract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "director_live_extract_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["diff_director_live_extract_snapshots.py", "--current-date", "2026-04-22"],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "director_live_extract_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
