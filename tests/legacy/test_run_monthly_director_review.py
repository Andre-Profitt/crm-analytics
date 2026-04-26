from __future__ import annotations

import json
import os
import sys
import types
from pathlib import Path

from scripts import run_monthly_director_review as runner


def _seed_data_quality_artifacts(output_root: Path, run_date: str) -> None:
    audit_dir = output_root / "data_quality" / run_date
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / "flags.json").write_text("{}", encoding="utf-8")
    (audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    diff_dir = output_root / "data_quality_snapshot_diff" / run_date
    diff_dir.mkdir(parents=True, exist_ok=True)
    (diff_dir / "data_quality_snapshot_diff.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")


def _seed_obsidian_notes_artifacts(root: Path, output_root: Path, run_date: str) -> None:
    month_dir = root / "obsidian" / "Monthly" / run_date[:7]
    month_dir.mkdir(parents=True, exist_ok=True)
    (month_dir / "README.md").write_text("# readme\n", encoding="utf-8")
    snapshot_history = root / "obsidian" / "snapshot_history.json"
    snapshot_history.parent.mkdir(parents=True, exist_ok=True)
    snapshot_history.write_text("{}", encoding="utf-8")
    audit_dir = output_root / "obsidian_notes_contract" / run_date
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / "obsidian_notes_contract_audit.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    diff_dir = output_root / "obsidian_notes_contract_snapshot_diff" / run_date
    diff_dir.mkdir(parents=True, exist_ok=True)
    (diff_dir / "obsidian_notes_contract_snapshot_diff.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")


def _seed_deck_font_normalization_artifacts(output_root: Path, run_date: str) -> None:
    audit_dir = output_root / "deck_font_normalization" / run_date
    audit_dir.mkdir(parents=True, exist_ok=True)
    (audit_dir / "deck_font_normalization.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")


def _step_by_name(manifest: dict, name: str) -> dict:
    for step in manifest["steps"]:
        if step["name"] == name:
            return step
    raise AssertionError(f"missing step {name}")


def test_main_aborts_after_historical_trending_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    source_audit_dir = runner.OUTPUT_ROOT / "source_contract_audit" / "2026-08-10"
    source_audit_dir.mkdir(parents=True, exist_ok=True)
    (source_audit_dir / "source_contract_audit.json").write_text("{}", encoding="utf-8")
    (source_audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    source_diff_dir = (
        runner.OUTPUT_ROOT / "source_contract_snapshot_diff" / "2026-08-10"
    )
    source_diff_dir.mkdir(parents=True, exist_ok=True)
    (source_diff_dir / "source_contract_snapshot_diff.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (source_diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    registry_refresh_dir = (
        runner.OUTPUT_ROOT / "source_contract_registry_refresh" / "2026-08-10"
    )
    registry_refresh_dir.mkdir(parents=True, exist_ok=True)
    (registry_refresh_dir / "registry_refresh.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (registry_refresh_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    (registry_refresh_dir / "proposed_sd_monthly_territories.json").write_text(
        "{}",
        encoding="utf-8",
    )

    live_extract_audit_dir = runner.OUTPUT_ROOT / "director_live_extract" / "2026-08-10"
    live_extract_audit_dir.mkdir(parents=True, exist_ok=True)
    (live_extract_audit_dir / "director_live_extract_audit.json").write_text(
        "{}", encoding="utf-8"
    )
    (live_extract_audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    live_extract_diff_dir = (
        runner.OUTPUT_ROOT / "director_live_extract_snapshot_diff" / "2026-08-10"
    )
    live_extract_diff_dir.mkdir(parents=True, exist_ok=True)
    (live_extract_diff_dir / "director_live_extract_snapshot_diff.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (live_extract_diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    historical_audit_dir = (
        runner.OUTPUT_ROOT / "historical_trending_extract" / "2026-08-10"
    )
    historical_audit_dir.mkdir(parents=True, exist_ok=True)
    (historical_audit_dir / "historical_trending_extract_audit.json").write_text(
        "{}", encoding="utf-8"
    )
    (historical_audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    historical_diff_dir = (
        runner.OUTPUT_ROOT / "historical_trending_snapshot_diff" / "2026-08-10"
    )
    historical_diff_dir.mkdir(parents=True, exist_ok=True)
    (historical_diff_dir / "historical_trending_snapshot_diff.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (historical_diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    workbook_contract_dir = (
        runner.OUTPUT_ROOT / "director_workbook_contract" / "2026-08-10"
    )
    workbook_contract_dir.mkdir(parents=True, exist_ok=True)
    (workbook_contract_dir / "director_workbook_contract_audit.json").write_text(
        "{}",
        encoding="utf-8",
    )
    (workbook_contract_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "1b_extract_historical_trending" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
    ]
    assert manifests
    assert [step["name"] for step in manifests[-1]["steps"]] == calls
    assert manifests[-1]["steps"][0]["artifacts"] == [
        {
            "type": "source_contract_audit",
            "path": "output/source_contract_audit/2026-08-10/source_contract_audit.json",
        },
        {
            "type": "source_contract_summary",
            "path": "output/source_contract_audit/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][1]["artifacts"] == [
        {
            "type": "source_contract_snapshot_diff",
            "path": "output/source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        },
        {
            "type": "source_contract_snapshot_diff_summary",
            "path": "output/source_contract_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][2]["artifacts"] == [
        {
            "type": "forward_quarter_registry_refresh",
            "path": "output/source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        },
        {
            "type": "forward_quarter_registry_refresh_summary",
            "path": "output/source_contract_registry_refresh/2026-08-10/summary.md",
        },
        {
            "type": "forward_quarter_registry_proposed_config",
            "path": "output/source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        },
    ]
    assert manifests[-1]["steps"][3]["artifacts"] == [
        {
            "type": "director_live_extract_audit",
            "path": "output/director_live_extract/2026-08-10/director_live_extract_audit.json",
        },
        {
            "type": "director_live_extract_summary",
            "path": "output/director_live_extract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][4]["artifacts"] == [
        {
            "type": "director_live_extract_snapshot_diff",
            "path": "output/director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        },
        {
            "type": "director_live_extract_snapshot_diff_summary",
            "path": "output/director_live_extract_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][5]["artifacts"] == [
        {
            "type": "historical_trending_extract_audit",
            "path": "output/historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        },
        {
            "type": "historical_trending_extract_summary",
            "path": "output/historical_trending_extract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][6]["artifacts"] == [
        {
            "type": "historical_trending_snapshot_diff",
            "path": "output/historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        },
        {
            "type": "historical_trending_snapshot_diff_summary",
            "path": "output/historical_trending_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-2]["status"] == "failed"


def test_main_aborts_after_workbook_contract_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "1b3_validate_director_workbook_contract" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
        "1b3_validate_director_workbook_contract",
        "1b4_director_workbook_contract_snapshot_diff",
    ]
    assert manifests[-1]["steps"][-2]["name"] == "1b3_validate_director_workbook_contract"
    assert manifests[-1]["steps"][-2]["status"] == "failed"
    assert manifests[-1]["steps"][-2]["artifacts"] == [
        {
            "type": "director_workbook_contract_audit",
            "path": "output/director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        },
        {
            "type": "director_workbook_contract_summary",
            "path": "output/director_workbook_contract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-1]["name"] == "1b4_director_workbook_contract_snapshot_diff"
    assert manifests[-1]["steps"][-1]["status"] == "ok"
    assert manifests[-1]["steps"][-1]["artifacts"] == [
        {
            "type": "director_workbook_contract_snapshot_diff",
            "path": "output/director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        },
        {
            "type": "director_workbook_contract_snapshot_diff_summary",
            "path": "output/director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        },
    ]


def test_acquire_single_flight_lock_reclaims_stale_holder(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    stale_path = runner._single_flight_lock_path()
    stale_path.parent.mkdir(parents=True, exist_ok=True)
    stale_path.write_text(
        json.dumps(
            {
                "pid": 999999,
                "run_date": "2026-08-10",
                "started_at": "2026-08-10T00:00:00",
                "hostname": "host",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runner, "_pid_is_running", lambda pid: False)

    lock = runner._acquire_single_flight_lock("2026-08-11")

    assert lock["acquired"] is True
    assert runner._read_lock_payload(stale_path)["run_date"] == "2026-08-11"
    runner._release_single_flight_lock(lock)
    assert not stale_path.exists()


def test_acquire_single_flight_lock_blocks_live_holder(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    lock_path = runner._single_flight_lock_path()
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text(
        json.dumps(
            {
                "pid": os.getpid(),
                "run_date": "2026-08-10",
                "started_at": "2026-08-10T00:00:00",
                "hostname": "host",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(runner, "_pid_is_running", lambda pid: True)

    lock = runner._acquire_single_flight_lock("2026-08-11")

    assert lock["acquired"] is False
    assert lock["reason"] == "already_running"
    assert lock["holder"]["run_date"] == "2026-08-10"


def test_main_exits_before_steps_when_single_flight_lock_is_held(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        runner,
        "_acquire_single_flight_lock",
        lambda run_date: {
            "acquired": False,
            "reason": "already_running",
            "path": tmp_path / "pipeline_logs" / ".run_monthly_director_review.lock",
            "holder": {
                "pid": 1234,
                "run_date": "2026-08-10",
                "started_at": "2026-08-10T00:00:00",
                "hostname": "host",
            },
        },
    )
    calls: list[str] = []
    monkeypatch.setattr(
        runner,
        "run_step",
        lambda name, cmd, log_path: calls.append(name),
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == []
    conflict_files = list(
        (tmp_path / "pipeline_logs" / "2026-08-10").glob("single_flight_lock_conflict-*.json")
    )
    assert len(conflict_files) == 1
    manifest = json.loads(
        (tmp_path / "pipeline_logs" / "2026-08-10" / "manifest.json").read_text(
            encoding="utf-8"
        )
    )
    assert manifest["single_flight_lock"]["status"] == "blocked"
    assert manifest["single_flight_lock"]["reason"] == "already_running"
    assert manifest["single_flight_lock"]["conflict_artifact"].startswith(
        "pipeline_logs/2026-08-10/single_flight_lock_conflict-"
    )
    assert manifest["steps"] == [
        {
            "name": "0_single_flight_lock",
            "command": "run_monthly_director_review.py --date 2026-08-10",
            "exit_code": 1,
            "duration_seconds": 0.0,
            "log_path": "",
            "status": "blocked",
            "artifacts": [
                {
                    "type": "single_flight_lock_conflict",
                    "path": f"pipeline_logs/2026-08-10/{conflict_files[0].name}",
                }
            ],
        }
    ]


def test_main_releases_single_flight_lock_on_early_abort(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    source_audit_dir = runner.OUTPUT_ROOT / "source_contract_audit" / "2026-08-10"
    source_audit_dir.mkdir(parents=True, exist_ok=True)
    (source_audit_dir / "source_contract_audit.json").write_text("{}", encoding="utf-8")
    (source_audit_dir / "summary.md").write_text("# summary\n", encoding="utf-8")
    source_diff_dir = runner.OUTPUT_ROOT / "source_contract_snapshot_diff" / "2026-08-10"
    source_diff_dir.mkdir(parents=True, exist_ok=True)
    (source_diff_dir / "source_contract_snapshot_diff.json").write_text("{}", encoding="utf-8")
    (source_diff_dir / "summary.md").write_text("# summary\n", encoding="utf-8")

    def fake_run_step(name, cmd, log_path):
        exit_code = 1 if name == "0_source_contract_preflight" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert not runner._single_flight_lock_path().exists()


def test_main_aborts_after_sharepoint_analysis_contract_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    fake_sharepoint = types.ModuleType("build_sharepoint_analysis")
    fake_sharepoint.DIRECTORS = [("Jesper Tyrer", "APAC", "jesper-tyrer")]
    monkeypatch.setitem(sys.modules, "build_sharepoint_analysis", fake_sharepoint)

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        "sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract/2026-08-10/summary.md",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    _seed_data_quality_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "2b2_validate_sharepoint_analysis_contract" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
        "1b3_validate_director_workbook_contract",
        "1b4_director_workbook_contract_snapshot_diff",
        "1c_data_quality_audit",
        "1c2_data_quality_snapshot_diff",
        "2a_analyze_consolidated_review",
        "2a2_regional_APAC",
        "2b_analyze_dashboard_q1",
        "2b2_validate_sharepoint_analysis_contract",
        "2b3_sharepoint_analysis_contract_snapshot_diff",
    ]
    assert manifests[-1]["steps"][9]["name"] == "1c_data_quality_audit"
    assert manifests[-1]["steps"][9]["artifacts"] == [
        {
            "type": "data_quality_flags",
            "path": "output/data_quality/2026-08-10/flags.json",
        },
        {
            "type": "data_quality_summary",
            "path": "output/data_quality/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][10]["name"] == "1c2_data_quality_snapshot_diff"
    assert manifests[-1]["steps"][10]["artifacts"] == [
        {
            "type": "data_quality_snapshot_diff",
            "path": "output/data_quality_snapshot_diff/2026-08-10/data_quality_snapshot_diff.json",
        },
        {
            "type": "data_quality_snapshot_diff_summary",
            "path": "output/data_quality_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-2]["name"] == "2b2_validate_sharepoint_analysis_contract"
    assert manifests[-1]["steps"][-2]["status"] == "failed"
    assert manifests[-1]["steps"][-2]["artifacts"] == [
        {
            "type": "sharepoint_analysis_contract_audit",
            "path": "output/sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        },
        {
            "type": "sharepoint_analysis_contract_summary",
            "path": "output/sharepoint_analysis_contract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-1]["name"] == "2b3_sharepoint_analysis_contract_snapshot_diff"
    assert manifests[-1]["steps"][-1]["status"] == "ok"
    assert manifests[-1]["steps"][-1]["artifacts"] == [
        {
            "type": "sharepoint_analysis_contract_snapshot_diff",
            "path": "output/sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        },
        {
            "type": "sharepoint_analysis_contract_snapshot_diff_summary",
            "path": "output/sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
        },
    ]


def test_main_aborts_after_deck_scope_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    fake_sharepoint = types.ModuleType("build_sharepoint_analysis")
    fake_sharepoint.DIRECTORS = [("Jesper Tyrer", "APAC", "jesper-tyrer")]
    monkeypatch.setitem(sys.modules, "build_sharepoint_analysis", fake_sharepoint)
    workbooks_dir = runner.WORKBOOKS_ROOT / "2026-08-10"
    workbooks_dir.mkdir(parents=True, exist_ok=True)
    (workbooks_dir / "jesper-tyrer.xlsx").write_text("", encoding="utf-8")
    (runner.DECKS_ROOT / "2026-08-10" / "land-only").mkdir(parents=True, exist_ok=True)

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        "sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract/2026-08-10/summary.md",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_delivery_contract/2026-08-10/deck_delivery_contract_audit.json",
        "deck_delivery_contract/2026-08-10/summary.md",
        "deck_delivery_contract_snapshot_diff/2026-08-10/deck_delivery_contract_snapshot_diff.json",
        "deck_delivery_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        "deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        "deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        "deck_visual_snapshot_diff/2026-08-10/summary.md",
        "golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        "golden_deck_regression_pack/2026-08-10/summary.md",
        "deck_font_audit/2026-08-10/deck_font_audit.json",
        "deck_font_audit/2026-08-10/summary.md",
        "deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        "deck_font_audit_snapshot_diff/2026-08-10/summary.md",
        "tie_out/2026-08-10/tie_out_audit.json",
        "tie_out/2026-08-10/summary.md",
        "tie_out_snapshot_diff/2026-08-10/tie_out_snapshot_diff.json",
        "tie_out_snapshot_diff/2026-08-10/summary.md",
        "deck_scope_audit/2026-08-10/deck_scope_audit.json",
        "deck_scope_audit/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    _seed_data_quality_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")

    tie_out_note = tmp_path / "obsidian" / "Monthly" / "2026-08" / "tie-out.md"
    tie_out_note.parent.mkdir(parents=True, exist_ok=True)
    tie_out_note.write_text("# tie-out\n", encoding="utf-8")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "4b_audit_deck_scope" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
        "1b3_validate_director_workbook_contract",
        "1b4_director_workbook_contract_snapshot_diff",
        "1c_data_quality_audit",
        "1c2_data_quality_snapshot_diff",
        "2a_analyze_consolidated_review",
        "2a2_regional_APAC",
        "2b_analyze_dashboard_q1",
        "2b2_validate_sharepoint_analysis_contract",
        "2b3_sharepoint_analysis_contract_snapshot_diff",
        "3_ship_deck_jesper-tyrer",
        "3_ship_exec_rollup",
        "3a_normalize_deck_fonts",
        "3b_validate_deck_delivery_contract",
        "3b2_deck_delivery_contract_snapshot_diff",
        "3b3_deck_fill_payload_snapshot_diff",
        "3b4_deck_visual_snapshot_diff",
        "3b5_assemble_golden_deck_regression_pack",
        "3b6_audit_deck_fonts",
        "3b7_deck_font_audit_snapshot_diff",
        "4_validate_tie_out",
        "4a2_tie_out_snapshot_diff",
        "4b_audit_deck_scope",
    ]
    assert "5_update_obsidian_notes" not in calls
    normalize_step = _step_by_name(manifests[-1], "3a_normalize_deck_fonts")
    assert normalize_step["status"] == "ok"
    assert normalize_step["artifacts"] == [
        {
            "type": "deck_font_normalization",
            "path": "output/deck_font_normalization/2026-08-10/deck_font_normalization.json",
        },
        {
            "type": "deck_font_normalization_summary",
            "path": "output/deck_font_normalization/2026-08-10/summary.md",
        },
    ]
    fill_payload_step = _step_by_name(
        manifests[-1], "3b3_deck_fill_payload_snapshot_diff"
    )
    assert fill_payload_step["status"] == "ok"
    assert fill_payload_step["artifacts"] == [
        {
            "type": "deck_fill_payload_snapshot_diff",
            "path": "output/deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        },
        {
            "type": "deck_fill_payload_snapshot_diff_summary",
            "path": "output/deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    visual_diff_step = _step_by_name(manifests[-1], "3b4_deck_visual_snapshot_diff")
    assert visual_diff_step["status"] == "ok"
    assert visual_diff_step["artifacts"] == [
        {
            "type": "deck_visual_snapshot_diff",
            "path": "output/deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        },
        {
            "type": "deck_visual_snapshot_diff_summary",
            "path": "output/deck_visual_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    golden_pack_step = _step_by_name(
        manifests[-1], "3b5_assemble_golden_deck_regression_pack"
    )
    assert golden_pack_step["status"] == "ok"
    assert golden_pack_step["artifacts"] == [
        {
            "type": "golden_deck_regression_pack",
            "path": "output/golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        },
        {
            "type": "golden_deck_regression_pack_summary",
            "path": "output/golden_deck_regression_pack/2026-08-10/summary.md",
        },
    ]
    font_audit_step = _step_by_name(manifests[-1], "3b6_audit_deck_fonts")
    assert font_audit_step["status"] == "ok"
    assert font_audit_step["artifacts"] == [
        {
            "type": "deck_font_audit",
            "path": "output/deck_font_audit/2026-08-10/deck_font_audit.json",
        },
        {
            "type": "deck_font_audit_summary",
            "path": "output/deck_font_audit/2026-08-10/summary.md",
        },
    ]
    font_audit_diff_step = _step_by_name(
        manifests[-1], "3b7_deck_font_audit_snapshot_diff"
    )
    assert font_audit_diff_step["status"] == "ok"
    assert font_audit_diff_step["artifacts"] == [
        {
            "type": "deck_font_audit_snapshot_diff",
            "path": "output/deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        },
        {
            "type": "deck_font_audit_snapshot_diff_summary",
            "path": "output/deck_font_audit_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    tie_out_step = _step_by_name(manifests[-1], "4_validate_tie_out")
    assert tie_out_step["status"] == "ok"
    assert tie_out_step["artifacts"] == [
        {
            "type": "tie_out_note",
            "path": "obsidian/Monthly/2026-08/tie-out.md",
        },
        {
            "type": "tie_out_audit",
            "path": "output/tie_out/2026-08-10/tie_out_audit.json",
        },
        {
            "type": "tie_out_summary",
            "path": "output/tie_out/2026-08-10/summary.md",
        },
    ]
    tie_out_diff_step = _step_by_name(manifests[-1], "4a2_tie_out_snapshot_diff")
    assert tie_out_diff_step["status"] == "ok"
    assert tie_out_diff_step["artifacts"] == [
        {
            "type": "tie_out_snapshot_diff",
            "path": "output/tie_out_snapshot_diff/2026-08-10/tie_out_snapshot_diff.json",
        },
        {
            "type": "tie_out_snapshot_diff_summary",
            "path": "output/tie_out_snapshot_diff/2026-08-10/summary.md",
        }
    ]
    deck_scope_step = _step_by_name(manifests[-1], "4b_audit_deck_scope")
    assert deck_scope_step["status"] == "failed"
    assert deck_scope_step["artifacts"] == [
        {
            "type": "deck_scope_audit",
            "path": "output/deck_scope_audit/2026-08-10/deck_scope_audit.json",
        },
        {
            "type": "deck_scope_summary",
            "path": "output/deck_scope_audit/2026-08-10/summary.md",
        },
    ]


def test_main_aborts_after_tie_out_failure_and_still_runs_scope_audit(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    fake_sharepoint = types.ModuleType("build_sharepoint_analysis")
    fake_sharepoint.DIRECTORS = [("Jesper Tyrer", "APAC", "jesper-tyrer")]
    monkeypatch.setitem(sys.modules, "build_sharepoint_analysis", fake_sharepoint)
    workbooks_dir = runner.WORKBOOKS_ROOT / "2026-08-10"
    workbooks_dir.mkdir(parents=True, exist_ok=True)
    (workbooks_dir / "jesper-tyrer.xlsx").write_text("", encoding="utf-8")
    (runner.DECKS_ROOT / "2026-08-10" / "land-only").mkdir(parents=True, exist_ok=True)

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        "sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract/2026-08-10/summary.md",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_delivery_contract/2026-08-10/deck_delivery_contract_audit.json",
        "deck_delivery_contract/2026-08-10/summary.md",
        "deck_delivery_contract_snapshot_diff/2026-08-10/deck_delivery_contract_snapshot_diff.json",
        "deck_delivery_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        "deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        "deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        "deck_visual_snapshot_diff/2026-08-10/summary.md",
        "golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        "golden_deck_regression_pack/2026-08-10/summary.md",
        "deck_font_audit/2026-08-10/deck_font_audit.json",
        "deck_font_audit/2026-08-10/summary.md",
        "deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        "deck_font_audit_snapshot_diff/2026-08-10/summary.md",
        "tie_out/2026-08-10/tie_out_audit.json",
        "tie_out/2026-08-10/summary.md",
        "tie_out_snapshot_diff/2026-08-10/tie_out_snapshot_diff.json",
        "tie_out_snapshot_diff/2026-08-10/summary.md",
        "deck_scope_audit/2026-08-10/deck_scope_audit.json",
        "deck_scope_audit/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    _seed_data_quality_artifacts(runner.OUTPUT_ROOT, "2026-08-10")

    tie_out_note = tmp_path / "obsidian" / "Monthly" / "2026-08" / "tie-out.md"
    tie_out_note.parent.mkdir(parents=True, exist_ok=True)
    tie_out_note.write_text("# tie-out\n", encoding="utf-8")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "4_validate_tie_out" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
        "1b3_validate_director_workbook_contract",
        "1b4_director_workbook_contract_snapshot_diff",
        "1c_data_quality_audit",
        "1c2_data_quality_snapshot_diff",
        "2a_analyze_consolidated_review",
        "2a2_regional_APAC",
        "2b_analyze_dashboard_q1",
        "2b2_validate_sharepoint_analysis_contract",
        "2b3_sharepoint_analysis_contract_snapshot_diff",
        "3_ship_deck_jesper-tyrer",
        "3_ship_exec_rollup",
        "3a_normalize_deck_fonts",
        "3b_validate_deck_delivery_contract",
        "3b2_deck_delivery_contract_snapshot_diff",
        "3b3_deck_fill_payload_snapshot_diff",
        "3b4_deck_visual_snapshot_diff",
        "3b5_assemble_golden_deck_regression_pack",
        "3b6_audit_deck_fonts",
        "3b7_deck_font_audit_snapshot_diff",
        "4_validate_tie_out",
        "4a2_tie_out_snapshot_diff",
        "4b_audit_deck_scope",
    ]
    assert "5_update_obsidian_notes" not in calls
    assert _step_by_name(manifests[-1], "3a_normalize_deck_fonts")["status"] == "ok"
    assert (
        _step_by_name(manifests[-1], "3b3_deck_fill_payload_snapshot_diff")["status"]
        == "ok"
    )
    assert _step_by_name(manifests[-1], "3b4_deck_visual_snapshot_diff")["status"] == "ok"
    assert (
        _step_by_name(manifests[-1], "3b5_assemble_golden_deck_regression_pack")[
            "status"
        ]
        == "ok"
    )
    assert _step_by_name(manifests[-1], "3b6_audit_deck_fonts")["status"] == "ok"
    assert (
        _step_by_name(manifests[-1], "3b7_deck_font_audit_snapshot_diff")["status"]
        == "ok"
    )
    assert _step_by_name(manifests[-1], "4_validate_tie_out")["status"] == "failed"
    assert _step_by_name(manifests[-1], "4a2_tie_out_snapshot_diff")["status"] == "ok"
    assert _step_by_name(manifests[-1], "4b_audit_deck_scope")["status"] == "ok"


def test_main_aborts_after_deck_delivery_contract_failure(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    fake_sharepoint = types.ModuleType("build_sharepoint_analysis")
    fake_sharepoint.DIRECTORS = [("Jesper Tyrer", "APAC", "jesper-tyrer")]
    monkeypatch.setitem(sys.modules, "build_sharepoint_analysis", fake_sharepoint)
    workbooks_dir = runner.WORKBOOKS_ROOT / "2026-08-10"
    workbooks_dir.mkdir(parents=True, exist_ok=True)
    (workbooks_dir / "jesper-tyrer.xlsx").write_text("", encoding="utf-8")
    (runner.DECKS_ROOT / "2026-08-10" / "land-only").mkdir(parents=True, exist_ok=True)

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        "sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract/2026-08-10/summary.md",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_delivery_contract/2026-08-10/deck_delivery_contract_audit.json",
        "deck_delivery_contract/2026-08-10/summary.md",
        "deck_delivery_contract_snapshot_diff/2026-08-10/deck_delivery_contract_snapshot_diff.json",
        "deck_delivery_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        "deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        "deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        "deck_visual_snapshot_diff/2026-08-10/summary.md",
        "golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        "golden_deck_regression_pack/2026-08-10/summary.md",
        "deck_font_audit/2026-08-10/deck_font_audit.json",
        "deck_font_audit/2026-08-10/summary.md",
        "deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        "deck_font_audit_snapshot_diff/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")
    _seed_data_quality_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        exit_code = 1 if name == "3b_validate_deck_delivery_contract" else 0
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": exit_code,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "failed" if exit_code else "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 1
    assert calls == [
        "0_source_contract_preflight",
        "0b_source_contract_snapshot_diff",
        "0c_forward_quarter_registry_refresh",
        "1a_extract_salesforce",
        "1a2_director_live_extract_snapshot_diff",
        "1b_extract_historical_trending",
        "1b2_historical_trending_snapshot_diff",
        "1b3_validate_director_workbook_contract",
        "1b4_director_workbook_contract_snapshot_diff",
        "1c_data_quality_audit",
        "1c2_data_quality_snapshot_diff",
        "2a_analyze_consolidated_review",
        "2a2_regional_APAC",
        "2b_analyze_dashboard_q1",
        "2b2_validate_sharepoint_analysis_contract",
        "2b3_sharepoint_analysis_contract_snapshot_diff",
        "3_ship_deck_jesper-tyrer",
        "3_ship_exec_rollup",
        "3a_normalize_deck_fonts",
        "3b_validate_deck_delivery_contract",
        "3b2_deck_delivery_contract_snapshot_diff",
        "3b3_deck_fill_payload_snapshot_diff",
        "3b4_deck_visual_snapshot_diff",
        "3b5_assemble_golden_deck_regression_pack",
        "3b6_audit_deck_fonts",
        "3b7_deck_font_audit_snapshot_diff",
    ]
    assert "4_validate_tie_out" not in calls
    normalize_step = _step_by_name(manifests[-1], "3a_normalize_deck_fonts")
    assert normalize_step["status"] == "ok"
    assert normalize_step["artifacts"] == [
        {
            "type": "deck_font_normalization",
            "path": "output/deck_font_normalization/2026-08-10/deck_font_normalization.json",
        },
        {
            "type": "deck_font_normalization_summary",
            "path": "output/deck_font_normalization/2026-08-10/summary.md",
        },
    ]
    deck_delivery_step = _step_by_name(
        manifests[-1], "3b_validate_deck_delivery_contract"
    )
    assert deck_delivery_step["status"] == "failed"
    assert deck_delivery_step["artifacts"] == [
        {
            "type": "deck_delivery_contract_audit",
            "path": "output/deck_delivery_contract/2026-08-10/deck_delivery_contract_audit.json",
        },
        {
            "type": "deck_delivery_contract_summary",
            "path": "output/deck_delivery_contract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-6]["name"] == "3b2_deck_delivery_contract_snapshot_diff"
    assert manifests[-1]["steps"][-6]["status"] == "ok"
    assert manifests[-1]["steps"][-6]["artifacts"] == [
        {
            "type": "deck_delivery_contract_snapshot_diff",
            "path": "output/deck_delivery_contract_snapshot_diff/2026-08-10/deck_delivery_contract_snapshot_diff.json",
        },
        {
            "type": "deck_delivery_contract_snapshot_diff_summary",
            "path": "output/deck_delivery_contract_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-5]["name"] == "3b3_deck_fill_payload_snapshot_diff"
    assert manifests[-1]["steps"][-5]["status"] == "ok"
    assert manifests[-1]["steps"][-5]["artifacts"] == [
        {
            "type": "deck_fill_payload_snapshot_diff",
            "path": "output/deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        },
        {
            "type": "deck_fill_payload_snapshot_diff_summary",
            "path": "output/deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-4]["name"] == "3b4_deck_visual_snapshot_diff"
    assert manifests[-1]["steps"][-4]["status"] == "ok"
    assert manifests[-1]["steps"][-4]["artifacts"] == [
        {
            "type": "deck_visual_snapshot_diff",
            "path": "output/deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        },
        {
            "type": "deck_visual_snapshot_diff_summary",
            "path": "output/deck_visual_snapshot_diff/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-3]["name"] == "3b5_assemble_golden_deck_regression_pack"
    assert manifests[-1]["steps"][-3]["status"] == "ok"
    assert manifests[-1]["steps"][-3]["artifacts"] == [
        {
            "type": "golden_deck_regression_pack",
            "path": "output/golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        },
        {
            "type": "golden_deck_regression_pack_summary",
            "path": "output/golden_deck_regression_pack/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-2]["name"] == "3b6_audit_deck_fonts"
    assert manifests[-1]["steps"][-2]["status"] == "ok"
    assert manifests[-1]["steps"][-2]["artifacts"] == [
        {
            "type": "deck_font_audit",
            "path": "output/deck_font_audit/2026-08-10/deck_font_audit.json",
        },
        {
            "type": "deck_font_audit_summary",
            "path": "output/deck_font_audit/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-1]["name"] == "3b7_deck_font_audit_snapshot_diff"
    assert manifests[-1]["steps"][-1]["status"] == "ok"
    assert manifests[-1]["steps"][-1]["artifacts"] == [
        {
            "type": "deck_font_audit_snapshot_diff",
            "path": "output/deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        },
        {
            "type": "deck_font_audit_snapshot_diff_summary",
            "path": "output/deck_font_audit_snapshot_diff/2026-08-10/summary.md",
        },
    ]


def test_main_runs_obsidian_notes_contract_validation_and_diff(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "pipeline_logs")
    monkeypatch.setattr(runner, "WORKBOOKS_ROOT", tmp_path / "director_live_workbooks")
    monkeypatch.setattr(runner, "DECKS_ROOT", tmp_path / "simcorp_director_decks")
    monkeypatch.setattr(runner, "SHAREPOINT_ROOT", tmp_path / "sharepoint")

    fake_sharepoint = types.ModuleType("build_sharepoint_analysis")
    fake_sharepoint.DIRECTORS = [("Jesper Tyrer", "APAC", "jesper-tyrer")]
    monkeypatch.setitem(sys.modules, "build_sharepoint_analysis", fake_sharepoint)
    workbooks_dir = runner.WORKBOOKS_ROOT / "2026-08-10"
    workbooks_dir.mkdir(parents=True, exist_ok=True)
    (workbooks_dir / "jesper-tyrer.xlsx").write_text("", encoding="utf-8")
    (runner.DECKS_ROOT / "2026-08-10" / "land-only").mkdir(parents=True, exist_ok=True)

    for rel_path in [
        "source_contract_audit/2026-08-10/source_contract_audit.json",
        "source_contract_audit/2026-08-10/summary.md",
        "source_contract_snapshot_diff/2026-08-10/source_contract_snapshot_diff.json",
        "source_contract_snapshot_diff/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/registry_refresh.json",
        "source_contract_registry_refresh/2026-08-10/summary.md",
        "source_contract_registry_refresh/2026-08-10/proposed_sd_monthly_territories.json",
        "director_live_extract/2026-08-10/director_live_extract_audit.json",
        "director_live_extract/2026-08-10/summary.md",
        "director_live_extract_snapshot_diff/2026-08-10/director_live_extract_snapshot_diff.json",
        "director_live_extract_snapshot_diff/2026-08-10/summary.md",
        "historical_trending_extract/2026-08-10/historical_trending_extract_audit.json",
        "historical_trending_extract/2026-08-10/summary.md",
        "historical_trending_snapshot_diff/2026-08-10/historical_trending_snapshot_diff.json",
        "historical_trending_snapshot_diff/2026-08-10/summary.md",
        "director_workbook_contract/2026-08-10/director_workbook_contract_audit.json",
        "director_workbook_contract/2026-08-10/summary.md",
        "director_workbook_contract_snapshot_diff/2026-08-10/director_workbook_contract_snapshot_diff.json",
        "director_workbook_contract_snapshot_diff/2026-08-10/summary.md",
        "sharepoint_analysis_contract/2026-08-10/sharepoint_analysis_contract_audit.json",
        "sharepoint_analysis_contract/2026-08-10/summary.md",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/sharepoint_analysis_contract_snapshot_diff.json",
        "sharepoint_analysis_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_delivery_contract/2026-08-10/deck_delivery_contract_audit.json",
        "deck_delivery_contract/2026-08-10/summary.md",
        "deck_delivery_contract_snapshot_diff/2026-08-10/deck_delivery_contract_snapshot_diff.json",
        "deck_delivery_contract_snapshot_diff/2026-08-10/summary.md",
        "deck_fill_payload_snapshot_diff/2026-08-10/deck_fill_payload_snapshot_diff.json",
        "deck_fill_payload_snapshot_diff/2026-08-10/summary.md",
        "deck_visual_snapshot_diff/2026-08-10/deck_visual_snapshot_diff.json",
        "deck_visual_snapshot_diff/2026-08-10/summary.md",
        "golden_deck_regression_pack/2026-08-10/golden_deck_regression_pack.json",
        "golden_deck_regression_pack/2026-08-10/summary.md",
        "deck_font_audit/2026-08-10/deck_font_audit.json",
        "deck_font_audit/2026-08-10/summary.md",
        "deck_font_audit_snapshot_diff/2026-08-10/deck_font_audit_snapshot_diff.json",
        "deck_font_audit_snapshot_diff/2026-08-10/summary.md",
        "tie_out/2026-08-10/tie_out_audit.json",
        "tie_out/2026-08-10/summary.md",
        "tie_out_snapshot_diff/2026-08-10/tie_out_snapshot_diff.json",
        "tie_out_snapshot_diff/2026-08-10/summary.md",
        "deck_scope_audit/2026-08-10/deck_scope_audit.json",
        "deck_scope_audit/2026-08-10/summary.md",
    ]:
        path = runner.OUTPUT_ROOT / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}", encoding="utf-8")

    _seed_data_quality_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_deck_font_normalization_artifacts(runner.OUTPUT_ROOT, "2026-08-10")
    _seed_obsidian_notes_artifacts(tmp_path, runner.OUTPUT_ROOT, "2026-08-10")

    calls: list[str] = []
    manifests: list[dict] = []

    def fake_run_step(name, cmd, log_path):
        calls.append(name)
        return {
            "name": name,
            "command": " ".join(str(part) for part in cmd),
            "exit_code": 0,
            "duration_seconds": 0.1,
            "log_path": str(log_path),
            "status": "ok",
        }

    def fake_write_manifest(manifest, log_dir):
        manifests.append(
            {
                "run_date": manifest["run_date"],
                "steps": [dict(step) for step in manifest["steps"]],
                "log_dir": str(log_dir),
            }
        )

    monkeypatch.setattr(runner, "run_step", fake_run_step)
    monkeypatch.setattr(runner, "_write_manifest", fake_write_manifest)
    monkeypatch.setattr(runner, "_print_summary", lambda manifest: None)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_monthly_director_review.py", "--date", "2026-08-10"],
    )

    assert runner.main() == 0
    assert calls[-3:] == [
        "5_update_obsidian_notes",
        "5a_validate_obsidian_notes_contract",
        "5b_obsidian_notes_contract_snapshot_diff",
    ]
    assert manifests[-1]["steps"][-3]["artifacts"] == [
        {
            "type": "obsidian_monthly_readme",
            "path": "obsidian/Monthly/2026-08/README.md",
        },
        {
            "type": "obsidian_snapshot_history",
            "path": "obsidian/snapshot_history.json",
        },
    ]
    assert manifests[-1]["steps"][-2]["artifacts"] == [
        {
            "type": "obsidian_notes_contract_audit",
            "path": "output/obsidian_notes_contract/2026-08-10/obsidian_notes_contract_audit.json",
        },
        {
            "type": "obsidian_notes_contract_summary",
            "path": "output/obsidian_notes_contract/2026-08-10/summary.md",
        },
    ]
    assert manifests[-1]["steps"][-1]["artifacts"] == [
        {
            "type": "obsidian_notes_contract_snapshot_diff",
            "path": "output/obsidian_notes_contract_snapshot_diff/2026-08-10/obsidian_notes_contract_snapshot_diff.json",
        },
        {
            "type": "obsidian_notes_contract_snapshot_diff_summary",
            "path": "output/obsidian_notes_contract_snapshot_diff/2026-08-10/summary.md",
        },
    ]


def test_write_manifest_with_release_packet_writes_packet_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(runner, "ROOT", tmp_path)
    monkeypatch.setattr(runner, "OUTPUT_ROOT", tmp_path / "output")
    monkeypatch.setattr(runner, "LOGS_ROOT", tmp_path / "output" / "pipeline_logs")

    source_dir = runner.OUTPUT_ROOT / "source_contract_audit" / "2026-04-22"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "source_contract_audit.json").write_text(
        json.dumps(
            {"run_date": "2026-04-22", "active_lane": {"status": "ok"}},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    font_dir = runner.OUTPUT_ROOT / "deck_font_audit" / "2026-04-22"
    font_dir.mkdir(parents=True, exist_ok=True)
    (font_dir / "deck_font_audit.json").write_text(
        json.dumps(
            {
                "run_date": "2026-04-22",
                "status": "warning",
                "deck_count": 10,
                "decks_with_issues": 9,
                "failures": [],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    log_dir = runner.LOGS_ROOT / "2026-04-22"
    log_dir.mkdir(parents=True, exist_ok=True)
    manifest = {
        "run_date": "2026-04-22",
        "started_at": "2026-04-22T10:00:00",
        "steps": [{"name": "0_source_contract_preflight", "status": "ok"}],
        "outputs": {"extracts": [], "decks": [], "reports": []},
    }

    runner._write_manifest_with_release_packet(manifest, log_dir)

    manifest_path = log_dir / "manifest.json"
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert payload["release_packet"]["status"] == "blocked"
    assert (
        tmp_path
        / payload["release_packet"]["json_path"]
    ).exists()
    assert (
        tmp_path
        / payload["release_packet"]["summary_path"]
    ).exists()
    assert payload["release_packet"]["snapshot_diff_status"] == "skipped"
    assert (
        tmp_path
        / payload["release_packet"]["snapshot_diff_json_path"]
    ).exists()
    assert (
        tmp_path
        / payload["release_packet"]["snapshot_diff_summary_path"]
    ).exists()
    assert payload["release_packet"]["history_generated_at"] is not None
    assert payload["release_packet"]["history_run_count"] == 1
    assert payload["release_packet"]["history_green_run_count"] == 0
    assert payload["release_packet"]["history_blocked_run_count"] == 1
    assert payload["release_packet"]["history_current_green_streak"] == 0
    assert (
        payload["release_packet"]["history_latest_core_state_transition_baseline_run_date"]
        is None
    )
    assert (
        payload["release_packet"]["history_latest_core_state_transition_run_date"]
        is None
    )
    assert payload["release_packet"]["history_latest_core_state_transition_changes"] == []
    assert (
        payload["release_packet"][
            "history_latest_core_state_transition_publish_blockers_added"
        ]
        == []
    )
    assert (
        payload["release_packet"][
            "history_latest_core_state_transition_publish_blockers_resolved"
        ]
        == []
    )
    assert (
        payload["release_packet"][
            "history_latest_core_state_transition_pipeline_blockers_added"
        ]
        == []
    )
    assert (
        payload["release_packet"][
            "history_latest_core_state_transition_pipeline_blockers_resolved"
        ]
        == []
    )
    assert payload["release_packet"]["history_latest_blocked_run_date"] == "2026-04-22"
    assert payload["release_packet"]["history_latest_blocked_publish_blockers"] == [
        "Deck font audit is `warning` with `9` deck(s) showing issues."
    ]
    assert payload["release_packet"]["history_latest_blocked_pipeline_blockers"] == []
    assert (
        payload["release_packet"]["history_latest_drift_baseline_run_date"] is None
    )
    assert payload["release_packet"]["history_latest_drift_run_date"] is None
    assert payload["release_packet"]["history_latest_drift_changed_gates"] == []
    assert (
        tmp_path
        / payload["release_packet"]["history_json_path"]
    ).exists()
    assert (
        tmp_path
        / payload["release_packet"]["history_summary_path"]
    ).exists()
