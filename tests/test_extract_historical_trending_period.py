import json
from pathlib import Path

import pytest

import scripts.extract_historical_trending as extract_hist
from scripts.extract_historical_trending import (
    DIRECTOR_SLUG_TO_TERRITORY,
    QUARTER_REPORTS,
    REPORTS,
    _load_historical_report_audit_fallback,
    _resolve_report_plan,
    _resolve_runtime_context,
    _validate_snapshot_freshness,
    _write_run_audit,
)


def test_runtime_context_infers_report_date_from_workbooks_dir() -> None:
    context = _resolve_runtime_context(
        workbooks_dir=Path("/tmp/director_live_workbooks/2026-04-22")
    )

    assert context["report_date"] == "2026-04-22"
    assert context["analysis_year"] == 2026
    assert context["retrospective_quarter_label"] == "Q1"
    assert context["retrospective_quarter_title"] == "Q1 2026"
    assert context["current_quarter_label"] == "Q2"
    assert context["current_quarter_title"] == "Q2 2026"


def test_snapshot_date_overrides_workbooks_dir_report_date() -> None:
    context = _resolve_runtime_context(
        snapshot_date="2026-04-10",
        workbooks_dir=Path("/tmp/director_live_workbooks/2026-04-22"),
    )

    assert context["report_date"] == "2026-04-10"
    assert context["current_quarter_title"] == "Q2 2026"


def test_report_plan_supports_legacy_q2_contract() -> None:
    plan = _resolve_report_plan(snapshot_date="2026-04-22")

    assert plan["jesper-tyrer"] == [
        ("Q1 Snapshot Trend", REPORTS["jesper-tyrer"][0]),
        ("Q2 Snapshot Trend", REPORTS["jesper-tyrer"][1]),
    ]


def test_report_plan_rolls_to_q2_q3_contract_when_runtime_month_advances() -> None:
    plan = _resolve_report_plan(snapshot_date="2026-08-10")

    assert plan["jesper-tyrer"] == [
        ("Q2 Snapshot Trend", REPORTS["jesper-tyrer"][1]),
        ("Q3 Snapshot Trend", QUARTER_REPORTS["jesper-tyrer"]["Q3"]),
    ]


def test_report_registry_is_loaded_from_territory_config() -> None:
    assert REPORTS["jesper-tyrer"] == (
        "00OTb000008g11VMAQ",
        "00OTb000008gYVJMA2",
    )
    assert REPORTS["adam-steinhaus"] == (
        "00OTb000008gYQTMA2",
        "00OTb000008gYgbMAE",
    )
    assert QUARTER_REPORTS["jesper-tyrer"]["Q3"] == "00OTb000008jXo1MAE"


def test_report_plan_rejects_unsupported_quarters() -> None:
    with pytest.raises(ValueError, match="Historical-trending report registry is incomplete"):
        _resolve_report_plan(snapshot_date="2026-11-10")


def test_load_historical_report_audit_fallback_uses_latest_prior_matching_quarter(
    tmp_path: Path, monkeypatch
) -> None:
    old_dir = tmp_path / "output" / "source_contract_audit" / "2026-07-15"
    old_dir.mkdir(parents=True, exist_ok=True)
    (old_dir / "source_contract_audit.json").write_text(
        json.dumps(
            {
                "candidate_forward_quarter": {
                    "quarter_label": "Q4",
                    "quarter_title": "Q4 2026",
                    "historical_reports": [
                        {
                            "director_slug": "APAC",
                            "report_id": "00OT-old-apac-q4",
                            "status": "ok",
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    new_dir = tmp_path / "output" / "source_contract_audit" / "2026-08-10"
    new_dir.mkdir(parents=True, exist_ok=True)
    (new_dir / "source_contract_audit.json").write_text(
        json.dumps(
            {
                "candidate_forward_quarter": {
                    "quarter_label": "Q4",
                    "quarter_title": "Q4 2026",
                    "historical_reports": [
                        {
                            "director_slug": "APAC",
                            "report_id": "00OT-new-apac-q4",
                            "status": "ok",
                        }
                    ],
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        extract_hist,
        "SOURCE_CONTRACT_AUDIT_ROOT",
        tmp_path / "output" / "source_contract_audit",
    )

    assert _load_historical_report_audit_fallback(
        target_quarter_label="Q4",
        target_quarter_title="Q4 2026",
        run_date="2026-11-10",
    ) == {"jesper-tyrer": "00OT-new-apac-q4"}


def test_report_plan_can_use_prior_audit_fallback_for_missing_current_quarter(
    tmp_path: Path, monkeypatch
) -> None:
    audit_dir = tmp_path / "output" / "source_contract_audit" / "2026-08-10"
    audit_dir.mkdir(parents=True, exist_ok=True)
    historical_reports = []
    for slug, territory in DIRECTOR_SLUG_TO_TERRITORY.items():
        historical_reports.append(
            {
                "director_slug": territory,
                "report_id": f"fallback-{slug}-q4",
                "status": "ok",
            }
        )
    (audit_dir / "source_contract_audit.json").write_text(
        json.dumps(
            {
                "candidate_forward_quarter": {
                    "quarter_label": "Q4",
                    "quarter_title": "Q4 2026",
                    "historical_reports": historical_reports,
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        extract_hist,
        "SOURCE_CONTRACT_AUDIT_ROOT",
        tmp_path / "output" / "source_contract_audit",
    )

    plan = _resolve_report_plan(snapshot_date="2026-11-10")

    assert plan["jesper-tyrer"] == [
        ("Q3 Snapshot Trend", QUARTER_REPORTS["jesper-tyrer"]["Q3"]),
        ("Q4 Snapshot Trend", "fallback-jesper-tyrer-q4"),
    ]


def test_validate_snapshot_freshness_allows_same_month_snapshots() -> None:
    result = _validate_snapshot_freshness(
        ["2026-04-01", "2026-04-07", "2026-04-15"],
        report_date="2026-04-22",
    )

    assert result["ok"] is True
    assert result["issues"] == []
    assert result["latest_snapshot_date"] == "2026-04-15"


def test_validate_snapshot_freshness_blocks_stale_month() -> None:
    result = _validate_snapshot_freshness(
        ["2026-04-01", "2026-04-07", "2026-04-15"],
        report_date="2026-08-10",
    )

    assert result["ok"] is False
    assert result["issues"] == ["snapshot_review_month_mismatch"]


def test_write_run_audit_emits_json_and_summary(tmp_path: Path) -> None:
    _write_run_audit(
        tmp_path,
        {
            "run_date": "2026-08-10",
            "workbooks_dir": "output/director_live_workbooks/2026-08-10",
            "scope": "jesper-tyrer",
            "retrospective_quarter_title": "Q2 2026",
            "current_quarter_title": "Q3 2026",
            "status": "failed",
            "processed": [],
            "failures": [
                {
                    "slug": "jesper-tyrer",
                    "sheet_name": "Q2 Snapshot Trend",
                    "issues": ["snapshot_review_month_mismatch"],
                }
            ],
        },
    )

    audit_json = tmp_path / "historical_trending_extract_audit.json"
    summary_md = tmp_path / "summary.md"

    assert audit_json.exists()
    assert summary_md.exists()
    assert "snapshot_review_month_mismatch" in audit_json.read_text(encoding="utf-8")
    assert "Q2 Snapshot Trend" in summary_md.read_text(encoding="utf-8")
