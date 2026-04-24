from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import run_source_backed_monthly_pipeline as pipeline  # noqa: E402


SNAPSHOT_DATE = "2026-04-30"
RUN_ID = "live-all-sources-pipeline-open-v5"


EXPECTED_STAGE_SCRIPTS = [
    "compile_monthly_source_contract_config.py",
    "audit_pi_list_view_filters.py",
    "build_monthly_source_contract.py",
    "lint_monthly_source_contract.py",
    "extract_salesforce_sources.py",
    "build_source_bundles_from_extracts.py",
    "build_director_bundles_from_sources.py",
    "build_monthly_source_contract.py",
    "audit_director_dataset_readiness.py",
    "build_source_backed_analyst_workbook.py",
    "build_thinkcell_source_from_bundles.py",
    "validate_monthly_source_backed_run.py",
    "build_director_gold_analytics.py",
    "build_deck_truth_packet.py",
    "build_source_backed_deck.py",
    "polish_source_backed_deck_language.py",
    "validate_source_backed_deck_visuals.py",
    "validate_source_backed_deck_table_contract.py",
    "validate_source_backed_deck_semantics.py",
    "validate_source_backed_deck_render.py",
    "build_source_backed_release_bundle.py",
    "upload_sales_deck_release_to_sharepoint.py",
]


def _parse_json_output(text: str) -> dict[str, Any]:
    stripped = text.strip()
    assert stripped, "expected JSON output"
    decoder = json.JSONDecoder()
    for index, char in enumerate(stripped):
        if char != "{":
            continue
        try:
            payload, end = decoder.raw_decode(stripped[index:])
        except json.JSONDecodeError:
            continue
        if index + end == len(stripped) and isinstance(payload, dict):
            return payload
    pytest.fail(f"could not parse JSON output: {text}")


def _run_main(
    argv: list[str],
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, dict[str, Any]]:
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_source_backed_monthly_pipeline.py", *argv],
    )
    result = pipeline.main()
    captured = capsys.readouterr()
    if isinstance(result, dict):
        return int(result.get("returncode", 0)), result
    return int(result or 0), _parse_json_output(captured.out)


def _command_text(command: Any) -> str:
    if isinstance(command, (list, tuple)):
        return " ".join(str(part) for part in command)
    return str(command)


def _stage_commands(payload: dict[str, Any]) -> list[str]:
    stages = (
        payload.get("stages")
        or payload.get("stage_plan")
        or payload.get("planned_stages")
        or []
    )
    assert isinstance(stages, list)
    commands: list[str] = []
    for stage in stages:
        assert isinstance(stage, dict)
        command = stage.get("command") or stage.get("cmd") or stage.get("argv")
        assert command, f"stage lacks command: {stage}"
        commands.append(_command_text(command))
    return commands


def _command_for(commands: list[str], script_name: str) -> str:
    matches = [command for command in commands if script_name in command]
    assert len(matches) == 1, f"expected exactly one command for {script_name}"
    return matches[0]


def _commands_for(commands: list[str], script_name: str) -> list[str]:
    matches = [command for command in commands if script_name in command]
    assert matches, f"expected at least one command for {script_name}"
    return matches


def _script_order(commands: list[str]) -> list[str]:
    ordered: list[str] = []
    for command in commands:
        for script_name in EXPECTED_STAGE_SCRIPTS:
            if script_name in command:
                ordered.append(script_name)
                break
    return ordered


def test_plan_only_assembles_all_source_backed_stage_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)

    def fail_if_called(*args: Any, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        raise AssertionError("plan-only mode must not run subprocess commands")

    monkeypatch.setattr(pipeline.subprocess, "run", fail_if_called)

    returncode, payload = _run_main(
        [
            "--snapshot-date",
            SNAPSHOT_DATE,
            "--run-id",
            RUN_ID,
            "--plan-only",
            "--output-path",
            str(tmp_path / "pipeline_run_manifest.json"),
        ],
        monkeypatch,
        capsys,
    )

    assert returncode == 0
    commands = _stage_commands(payload)
    assert _script_order(commands) == EXPECTED_STAGE_SCRIPTS
    assert all(SNAPSHOT_DATE in command for command in commands)
    assert RUN_ID in " ".join(commands)
    assert "sf " not in " ".join(commands)

    assert "--check" in _command_for(
        commands,
        "compile_monthly_source_contract_config.py",
    )
    assert "--output-path" in _command_for(commands, "audit_pi_list_view_filters.py")
    source_contract_commands = _commands_for(commands, "build_monthly_source_contract.py")
    assert len(source_contract_commands) == 2
    assert all("--json" in command for command in source_contract_commands)
    assert all(RUN_ID in command for command in source_contract_commands)
    assert "--require-bundles" not in source_contract_commands[0]
    assert "--require-bundles" in source_contract_commands[1]
    assert "--bundle-dir" in source_contract_commands[1]
    assert "--json" in _command_for(commands, "lint_monthly_source_contract.py")
    assert "--json" in _command_for(commands, "extract_salesforce_sources.py")
    assert "--require-complete" in _command_for(
        commands,
        "build_source_bundles_from_extracts.py",
    )
    assert "--require-valid" in _command_for(
        commands,
        "build_director_bundles_from_sources.py",
    )
    assert "--dataset pipeline_open" in _command_for(
        commands,
        "audit_director_dataset_readiness.py",
    )
    assert "--source-backed-publish-gate" in _command_for(
        commands,
        "build_deck_truth_packet.py",
    )
    upload_plan_command = _command_for(
        commands,
        "upload_sales_deck_release_to_sharepoint.py",
    )
    assert "--source-backed-bundle-manifest-json" in upload_plan_command
    assert "--dry-run" in upload_plan_command
    assert RUN_ID in _command_for(commands, "build_director_gold_analytics.py")
    assert RUN_ID in _command_for(commands, "build_deck_truth_packet.py")


def test_required_stage_failure_stops_before_downstream_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    calls: list[list[str]] = []

    def fake_run(
        command: list[str],
        cwd: Path | str | None = None,
        capture_output: bool = False,
        text: bool = False,
        **kwargs: Any,
    ) -> subprocess.CompletedProcess[str]:
        calls.append([str(part) for part in command])
        script_name = Path(command[1]).name
        if script_name == "extract_salesforce_sources.py":
            return subprocess.CompletedProcess(
                command,
                2,
                stdout=json.dumps({"status": "blocked", "stage": script_name}),
                stderr="required source extraction failed",
            )
        _write_fake_output_path(command)
        return subprocess.CompletedProcess(
            command,
            0,
            stdout=json.dumps(
                {"status": "ok", **_fake_stage_payload(script_name, tmp_path)}
            ),
            stderr="",
        )

    monkeypatch.setattr(pipeline.subprocess, "run", fake_run)

    returncode, payload = _run_main(
        [
            "--snapshot-date",
            SNAPSHOT_DATE,
            "--run-id",
            RUN_ID,
            "--output-path",
            str(tmp_path / "pipeline_run_manifest.json"),
        ],
        monkeypatch,
        capsys,
    )

    called_scripts = [Path(call[1]).name for call in calls]
    assert returncode != 0
    assert called_scripts == [
        "compile_monthly_source_contract_config.py",
        "audit_pi_list_view_filters.py",
        "build_monthly_source_contract.py",
        "lint_monthly_source_contract.py",
        "extract_salesforce_sources.py",
    ]
    assert "build_source_bundles_from_extracts.py" not in called_scripts
    assert payload["status"] in {"failed", "error", "blocked"}
    assert payload.get("failed_stage") in {
        "extract_salesforce_sources",
        "extract_salesforce_sources.py",
        "salesforce_extraction",
    }


def test_source_backed_publish_gate_command_uses_list_view_audit_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    monkeypatch.setattr(
        pipeline.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("plan-only mode must not execute stages"),
    )

    _, payload = _run_main(
        [
            "--snapshot-date",
            SNAPSHOT_DATE,
            "--run-id",
            RUN_ID,
            "--plan-only",
            "--output-path",
            str(tmp_path / "pipeline_run_manifest.json"),
        ],
        monkeypatch,
        capsys,
    )

    command = _command_for(
        _stage_commands(payload),
        "validate_monthly_source_backed_run.py",
    )
    parts = command.split()
    assert "--list-view-audit" in parts
    audit_path = Path(parts[parts.index("--list-view-audit") + 1])
    assert audit_path.name == "pi_list_view_filter_audit.json"
    assert SNAPSHOT_DATE in str(audit_path)
    assert RUN_ID in str(audit_path)


def test_sharepoint_upload_flag_adds_actual_upload_stage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    stages = pipeline.build_stage_plan(
        snapshot_date=SNAPSHOT_DATE,
        run_id=RUN_ID,
        sharepoint_upload=True,
    )

    assert stages[-2].name == "plan_source_backed_sharepoint_upload"
    assert stages[-2].accepted_statuses == frozenset({"planned"})
    assert stages[-1].name == "upload_source_backed_sharepoint_assets"
    assert "scripts/upload_sales_deck_release_to_sharepoint.py" in stages[-1].command
    assert "--dry-run" not in stages[-1].command


def test_existing_run_lock_blocks_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    lock_path = pipeline.build_paths(SNAPSHOT_DATE, RUN_ID).run_lock
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.write_text('{"pid":123}\n', encoding="utf-8")
    monkeypatch.setattr(
        pipeline.subprocess,
        "run",
        lambda *args, **kwargs: pytest.fail("locked run must not execute stages"),
    )

    returncode, payload = _run_main(
        [
            "--snapshot-date",
            SNAPSHOT_DATE,
            "--run-id",
            RUN_ID,
            "--output-path",
            str(tmp_path / "pipeline_run_manifest.json"),
        ],
        monkeypatch,
        capsys,
    )

    assert returncode == 2
    assert payload["status"] == "blocked"
    assert payload["failed_stage"] == "run_lock"
    assert "Run lock already exists" in payload["blocking_reason"]
    assert lock_path.exists()


def test_summarize_manifest_surfaces_operator_metrics() -> None:
    manifest = {
        "status": "ok",
        "stages": [
            {
                "name": "source_contract_authoring_config_check",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "target_count": 3,
                    "drift_count": 0,
                    "missing_count": 0,
                    "finding_count": 0,
                    "high_finding_count": 0,
                },
            },
            {
                "name": "pi_list_view_filter_audit",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "view_count": 27,
                    "finding_count": 0,
                    "high_finding_count": 0,
                    "dead_or_invalid_filter_field_count": 4,
                },
            },
            {
                "name": "extract_salesforce_sources",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "selected_source_count": 55,
                    "executed_source_count": 55,
                    "source_extract_count": 55,
                    "failed_source_count": 0,
                    "finding_count": 0,
                },
            },
            {
                "name": "build_source_bundles",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "bundle_count": 9,
                    "territory_count": 9,
                    "forward_fallback_count": 4,
                },
            },
            {
                "name": "source_contract_final",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "quarter_policy": {
                        "name": "calendar_quarter",
                        "fiscal_year_start_month": 1,
                    },
                    "period": {
                        "current_quarter": {"title": "Q2 2026"},
                        "forward_quarter": {"title": "Q3 2026"},
                        "reporting_window_start": "2026-01-01",
                        "reporting_window_end": "2026-09-30",
                    },
                },
            },
            {
                "name": "validate_source_backed_deck_visuals",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "summary": {"finding_count": 0, "high_finding_count": 0},
                    "checks": {"slide_count": 6, "table_count": 5, "chart_count": 1},
                },
            },
            {
                "name": "polish_source_backed_deck_language",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "summary": {
                        "finding_count": 0,
                        "high_finding_count": 0,
                        "replacements_applied_count": 12,
                    },
                    "checks": {"polished_text_checked": True},
                },
            },
            {
                "name": "validate_source_backed_deck_semantics",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "summary": {
                        "finding_count": 0,
                        "high_finding_count": 0,
                        "medium_finding_count": 0,
                        "human_style_score": 100,
                    },
                    "checks": {"business_readiness_checked": True},
                },
            },
            {
                "name": "validate_source_backed_deck_table_contract",
                "status": "ok",
                "payload": {
                    "status": "ok",
                    "summary": {
                        "finding_count": 0,
                        "high_finding_count": 0,
                        "medium_finding_count": 0,
                        "table_count": 5,
                    },
                    "checks": {
                        "expected_table_count": 5,
                        "table_contract_checked": True,
                    },
                },
            },
            {
                "name": "plan_source_backed_sharepoint_upload",
                "status": "ok",
                "payload": {
                    "status": "planned",
                    "planned_count": 5,
                    "missing_count": 0,
                    "publish_ready": True,
                    "source_backed": True,
                    "folder": "General/Monthly/Q2 2026/run",
                },
            },
        ],
    }

    summary = pipeline.summarize_manifest(manifest)

    assert summary["stage_count"] == 10
    assert summary["ok_stage_count"] == 10
    assert summary["source_contract_authoring_target_count"] == 3
    assert summary["source_contract_authoring_drift_count"] == 0
    assert summary["list_view_audit_view_count"] == 27
    assert summary["source_extract_count"] == 55
    assert summary["forward_fallback_count"] == 4
    assert summary["visual_slide_count"] == 6
    assert summary["visual_finding_count"] == 0
    assert summary["polish_replacements_applied_count"] == 12
    assert summary["table_contract_table_count"] == 5
    assert summary["semantic_human_style_score"] == 100
    assert summary["quarter_policy_name"] == "calendar_quarter"
    assert summary["current_quarter_title"] == "Q2 2026"
    assert summary["forward_quarter_title"] == "Q3 2026"
    assert summary["sharepoint_upload_plan_status"] == "planned"
    assert summary["sharepoint_upload_planned_count"] == 5


def test_summarize_manifest_reads_source_contract_artifact_for_quarter_policy(
    tmp_path: Path,
) -> None:
    contract_path = tmp_path / "monthly_source_contract.json"
    contract_path.write_text(
        json.dumps(
            {
                "status": "ok",
                "quarter_policy": {
                    "name": "calendar_quarter",
                    "fiscal_year_start_month": 1,
                },
                "period": {
                    "current_quarter": {"title": "Q2 2026"},
                    "forward_quarter": {"title": "Q3 2026"},
                    "reporting_window_start": "2026-01-01",
                    "reporting_window_end": "2026-09-30",
                },
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "status": "ok",
        "stages": [
            {
                "name": "source_contract_final",
                "status": "ok",
                "output_path": str(contract_path),
                "payload": {"status": "ok", "territory_count": 9},
            },
        ],
    }

    summary = pipeline.summarize_manifest(manifest)

    assert summary["quarter_policy_name"] == "calendar_quarter"
    assert summary["quarter_policy_fiscal_year_start_month"] == 1
    assert summary["current_quarter_title"] == "Q2 2026"
    assert summary["forward_quarter_title"] == "Q3 2026"
    assert summary["reporting_window_start"] == "2026-01-01"
    assert summary["reporting_window_end"] == "2026-09-30"


def test_release_packet_recommends_publish_for_clean_manifest(tmp_path: Path) -> None:
    manifest = {
        "status": "ok",
        "snapshot_date": SNAPSHOT_DATE,
        "run_id": RUN_ID,
        "manifest_path": str(tmp_path / "pipeline_run_manifest.json"),
        "summary": {
            "source_contract_authoring_target_count": 3,
            "source_contract_authoring_drift_count": 0,
            "source_contract_authoring_missing_count": 0,
            "source_contract_authoring_high_finding_count": 0,
            "source_contract_preflight_high_finding_count": 0,
            "source_contract_preflight_missing_report_id_count": 0,
            "source_contract_lint_finding_count": 0,
            "source_contract_lint_high_finding_count": 0,
            "source_contract_final_high_finding_count": 0,
            "source_contract_final_warning_finding_count": 0,
            "source_contract_final_missing_report_id_count": 0,
            "source_contract_final_missing_bundle_count": 0,
            "source_contract_final_territory_count": 9,
            "quarter_policy_name": "calendar_quarter",
            "quarter_policy_fiscal_year_start_month": 1,
            "current_quarter_title": "Q2 2026",
            "forward_quarter_title": "Q3 2026",
            "reporting_window_start": "2026-01-01",
            "reporting_window_end": "2026-09-30",
            "list_view_audit_view_count": 27,
            "list_view_audit_finding_count": 0,
            "list_view_audit_high_finding_count": 0,
            "selected_source_count": 55,
            "executed_source_count": 55,
            "source_extract_count": 55,
            "failed_source_count": 0,
            "bundle_count": 9,
            "territory_count": 9,
            "missing_selected_source_count": 0,
            "director_bundle_count": 9,
            "publish_gate_director_bundle_count": 9,
            "publish_gate_finding_count": 0,
            "publish_gate_high_finding_count": 0,
            "high_blocker_count": 0,
            "visual_high_finding_count": 0,
            "visual_finding_count": 0,
            "visual_slide_count": 6,
            "visual_table_count": 5,
            "visual_chart_count": 1,
            "polish_finding_count": 0,
            "polish_high_finding_count": 0,
            "polish_replacements_applied_count": 12,
            "polished_text_checked": True,
            "table_contract_finding_count": 0,
            "table_contract_high_finding_count": 0,
            "table_contract_medium_finding_count": 0,
            "table_contract_table_count": 5,
            "table_contract_expected_table_count": 5,
            "table_contract_checked": True,
            "semantic_finding_count": 0,
            "semantic_high_finding_count": 0,
            "semantic_medium_finding_count": 0,
            "semantic_human_style_score": 100,
            "business_readiness_checked": True,
            "render_finding_count": 0,
            "render_high_finding_count": 0,
            "render_deck_slide_count": 6,
            "rendered_slide_count": 6,
            "rendered_png_checked": True,
            "release_bundle_artifact_count": 21,
            "release_bundle_copied_artifact_count": 21,
            "release_bundle_required_artifact_count": 21,
            "release_bundle_missing_required_artifact_count": 0,
            "release_bundle_zip_size_bytes": 1000,
            "release_bundle_upload_ready": True,
            "sharepoint_upload_plan_status": "planned",
            "sharepoint_upload_planned_count": 5,
            "sharepoint_upload_missing_count": 0,
            "sharepoint_upload_publish_ready": True,
            "sharepoint_upload_source_backed": True,
            "claim_count": 60,
            "tieout_mismatch_count": 0,
        },
        "paths": {
            "analyst_workbook": str(tmp_path / "analyst.xlsx"),
            "thinkcell_dir": str(tmp_path / "thinkcell"),
            "publish_gate": str(tmp_path / "publish_gate.json"),
            "deck_truth_packet": str(tmp_path / "truth.json"),
            "source_backed_deck": str(tmp_path / "deck.pptx"),
            "source_backed_deck_polish_audit": str(tmp_path / "polish.json"),
            "source_backed_deck_visual_audit": str(tmp_path / "visual.json"),
            "source_backed_deck_table_contract_audit": str(tmp_path / "table.json"),
            "source_backed_deck_semantic_audit": str(tmp_path / "semantic.json"),
            "source_backed_deck_render_audit": str(tmp_path / "render.json"),
            "release_bundle_manifest": str(tmp_path / "bundle.json"),
            "release_bundle_zip": str(tmp_path / "bundle.zip"),
            "sharepoint_upload_plan": str(tmp_path / "sharepoint_upload_plan.json"),
            "list_view_audit": str(tmp_path / "list_view.json"),
        },
        "stages": [{"name": "gate", "status": "ok", "payload_status": "ok"}],
    }

    packet = pipeline.release_packet_from_manifest(manifest)

    assert packet["status"] == "ok"
    assert packet["publish_recommendation"] == "publish"
    assert packet["runner_manifest_path"] == str(tmp_path / "pipeline_run_manifest.json")
    assert packet["artifacts"]["source_backed_deck"] == str(tmp_path / "deck.pptx")
    assert packet["artifacts"]["polish_audit"] == str(tmp_path / "polish.json")
    assert packet["artifacts"]["table_contract_audit"] == str(tmp_path / "table.json")
    assert packet["artifacts"]["semantic_audit"] == str(tmp_path / "semantic.json")
    assert packet["artifacts"]["release_bundle_manifest"] == str(tmp_path / "bundle.json")
    assert packet["artifacts"]["release_bundle_zip"] == str(tmp_path / "bundle.zip")
    assert packet["artifacts"]["sharepoint_upload_plan"] == str(
        tmp_path / "sharepoint_upload_plan.json"
    )
    assert all(check["status"] == "pass" for check in packet["release_checks"])


def test_release_packet_blocks_weak_source_or_visual_evidence(tmp_path: Path) -> None:
    manifest = {
        "status": "ok",
        "snapshot_date": SNAPSHOT_DATE,
        "run_id": RUN_ID,
        "manifest_path": str(tmp_path / "pipeline_run_manifest.json"),
        "summary": {
            "source_contract_authoring_target_count": 3,
            "source_contract_authoring_drift_count": 0,
            "source_contract_authoring_missing_count": 0,
            "source_contract_authoring_high_finding_count": 0,
            "source_contract_preflight_high_finding_count": 0,
            "source_contract_preflight_missing_report_id_count": 0,
            "source_contract_lint_finding_count": 0,
            "source_contract_lint_high_finding_count": 0,
            "source_contract_final_high_finding_count": 0,
            "source_contract_final_warning_finding_count": 0,
            "source_contract_final_missing_report_id_count": 0,
            "source_contract_final_missing_bundle_count": 0,
            "source_contract_final_territory_count": 9,
            "quarter_policy_name": "calendar_quarter",
            "quarter_policy_fiscal_year_start_month": 1,
            "current_quarter_title": "Q2 2026",
            "forward_quarter_title": "Q3 2026",
            "reporting_window_start": "2026-01-01",
            "reporting_window_end": "2026-09-30",
            "list_view_audit_view_count": 27,
            "list_view_audit_finding_count": 1,
            "list_view_audit_high_finding_count": 0,
            "selected_source_count": 55,
            "executed_source_count": 54,
            "source_extract_count": 54,
            "failed_source_count": 0,
            "bundle_count": 9,
            "territory_count": 9,
            "missing_selected_source_count": 0,
            "director_bundle_count": 9,
            "publish_gate_director_bundle_count": 9,
            "publish_gate_finding_count": 0,
            "publish_gate_high_finding_count": 0,
            "high_blocker_count": 0,
            "tieout_mismatch_count": 0,
            "claim_count": 60,
            "visual_finding_count": 1,
            "visual_high_finding_count": 0,
            "visual_slide_count": 5,
            "visual_table_count": 5,
            "visual_chart_count": 1,
            "polish_finding_count": 1,
            "polish_high_finding_count": 0,
            "polished_text_checked": True,
            "table_contract_finding_count": 1,
            "table_contract_high_finding_count": 1,
            "table_contract_medium_finding_count": 0,
            "table_contract_table_count": 4,
            "table_contract_expected_table_count": 5,
            "table_contract_checked": True,
            "semantic_finding_count": 1,
            "semantic_high_finding_count": 1,
            "semantic_human_style_score": 60,
            "business_readiness_checked": True,
            "render_finding_count": 1,
            "render_high_finding_count": 0,
            "render_deck_slide_count": 6,
            "rendered_slide_count": 5,
            "rendered_png_checked": True,
            "release_bundle_artifact_count": 21,
            "release_bundle_copied_artifact_count": 20,
            "release_bundle_required_artifact_count": 21,
            "release_bundle_missing_required_artifact_count": 1,
            "release_bundle_zip_size_bytes": 0,
            "release_bundle_upload_ready": False,
            "sharepoint_upload_plan_status": "blocked",
            "sharepoint_upload_planned_count": 5,
            "sharepoint_upload_missing_count": 1,
            "sharepoint_upload_publish_ready": False,
            "sharepoint_upload_source_backed": True,
        },
        "paths": {},
        "stages": [],
    }

    packet = pipeline.release_packet_from_manifest(manifest)
    failed_checks = {
        check["name"]
        for check in packet["release_checks"]
        if check["status"] == "fail"
    }

    assert packet["status"] == "blocked"
    assert packet["publish_recommendation"] == "do_not_publish"
    assert "list_view_audit_clean" in failed_checks
    assert "salesforce_extracts_complete" in failed_checks
    assert "deck_visuals_clean" in failed_checks
    assert "deck_polish_clean" in failed_checks
    assert "deck_table_contract_clean" in failed_checks
    assert "deck_semantics_clean" in failed_checks
    assert "deck_render_clean" in failed_checks
    assert "release_bundle_complete" in failed_checks
    assert "sharepoint_upload_plan_clean" in failed_checks


def test_write_manifest_and_release_packet_updates_latest_aliases(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(pipeline, "ROOT", tmp_path)
    output_path = pipeline.default_manifest_path(SNAPSHOT_DATE, RUN_ID)
    manifest = {
        "status": "ok",
        "snapshot_date": SNAPSHOT_DATE,
        "run_id": RUN_ID,
        "target_org": "apro@simcorp.com",
        "summary": {
            "stage_count": 20,
            "ok_stage_count": 20,
            "source_contract_authoring_target_count": 3,
            "source_contract_authoring_drift_count": 0,
            "source_contract_authoring_missing_count": 0,
            "source_contract_authoring_high_finding_count": 0,
            "source_contract_preflight_high_finding_count": 0,
            "source_contract_preflight_missing_report_id_count": 0,
            "source_contract_lint_finding_count": 0,
            "source_contract_lint_high_finding_count": 0,
            "source_contract_final_high_finding_count": 0,
            "source_contract_final_warning_finding_count": 0,
            "source_contract_final_missing_report_id_count": 0,
            "source_contract_final_missing_bundle_count": 0,
            "source_contract_final_territory_count": 9,
            "quarter_policy_name": "calendar_quarter",
            "quarter_policy_fiscal_year_start_month": 1,
            "current_quarter_title": "Q2 2026",
            "forward_quarter_title": "Q3 2026",
            "reporting_window_start": "2026-01-01",
            "reporting_window_end": "2026-09-30",
            "list_view_audit_view_count": 27,
            "list_view_audit_finding_count": 0,
            "list_view_audit_high_finding_count": 0,
            "selected_source_count": 55,
            "executed_source_count": 55,
            "source_extract_count": 55,
            "failed_source_count": 0,
            "bundle_count": 9,
            "territory_count": 9,
            "missing_selected_source_count": 0,
            "director_bundle_count": 9,
            "publish_gate_director_bundle_count": 9,
            "publish_gate_finding_count": 0,
            "publish_gate_high_finding_count": 0,
            "high_blocker_count": 0,
            "tieout_mismatch_count": 0,
            "claim_count": 60,
            "visual_finding_count": 0,
            "visual_high_finding_count": 0,
            "visual_slide_count": 6,
            "visual_table_count": 5,
            "visual_chart_count": 1,
            "polish_finding_count": 0,
            "polish_high_finding_count": 0,
            "polish_replacements_applied_count": 12,
            "polished_text_checked": True,
            "table_contract_finding_count": 0,
            "table_contract_high_finding_count": 0,
            "table_contract_medium_finding_count": 0,
            "table_contract_table_count": 5,
            "table_contract_expected_table_count": 5,
            "table_contract_checked": True,
            "semantic_finding_count": 0,
            "semantic_high_finding_count": 0,
            "semantic_medium_finding_count": 0,
            "semantic_human_style_score": 100,
            "business_readiness_checked": True,
            "render_finding_count": 0,
            "render_high_finding_count": 0,
            "render_deck_slide_count": 6,
            "rendered_slide_count": 6,
            "rendered_png_checked": True,
            "release_bundle_artifact_count": 21,
            "release_bundle_copied_artifact_count": 21,
            "release_bundle_required_artifact_count": 21,
            "release_bundle_missing_required_artifact_count": 0,
            "release_bundle_zip_size_bytes": 1000,
            "release_bundle_upload_ready": True,
            "sharepoint_upload_plan_status": "planned",
            "sharepoint_upload_planned_count": 5,
            "sharepoint_upload_missing_count": 0,
            "sharepoint_upload_publish_ready": True,
            "sharepoint_upload_source_backed": True,
        },
        "paths": {
            "analyst_workbook": str(tmp_path / "analyst.xlsx"),
            "thinkcell_dir": str(tmp_path / "thinkcell"),
            "publish_gate": str(tmp_path / "publish_gate.json"),
            "deck_truth_packet": str(tmp_path / "truth.json"),
            "source_backed_deck": str(tmp_path / "deck.pptx"),
            "source_backed_deck_polish_audit": str(tmp_path / "polish.json"),
            "source_backed_deck_visual_audit": str(tmp_path / "visual.json"),
            "source_backed_deck_table_contract_audit": str(tmp_path / "table.json"),
            "source_backed_deck_semantic_audit": str(tmp_path / "semantic.json"),
            "source_backed_deck_render_audit": str(tmp_path / "render.json"),
            "release_bundle_manifest": str(tmp_path / "bundle.json"),
            "release_bundle_zip": str(tmp_path / "bundle.zip"),
            "sharepoint_upload_plan": str(tmp_path / "sharepoint_upload_plan.json"),
            "list_view_audit": str(tmp_path / "list_view.json"),
        },
        "stages": [{"name": "gate", "status": "ok", "payload_status": "ok"}],
    }

    pipeline.write_manifest_and_release_packet(
        manifest=manifest,
        output_path=output_path,
        plan_only=False,
    )

    root_latest = tmp_path / "output" / "source_backed_monthly_pipeline_runs" / "latest.json"
    snapshot_latest = (
        tmp_path
        / "output"
        / "source_backed_monthly_pipeline_runs"
        / SNAPSHOT_DATE
        / "latest.json"
    )
    latest = json.loads(root_latest.read_text(encoding="utf-8"))
    assert latest["status"] == "ok"
    assert latest["publish_recommendation"] == "publish"
    assert latest["run_id"] == RUN_ID
    assert latest["summary"]["rendered_slide_count"] == 6
    assert latest["summary"]["table_contract_table_count"] == 5
    assert latest["summary"]["semantic_human_style_score"] == 100
    assert latest["summary"]["quarter_policy_name"] == "calendar_quarter"
    assert latest["summary"]["current_quarter_title"] == "Q2 2026"
    assert latest["summary"]["release_bundle_artifact_count"] == 21
    assert latest["summary"]["release_bundle_upload_ready"] is True
    assert latest["summary"]["sharepoint_upload_plan_status"] == "planned"
    assert latest["summary"]["sharepoint_upload_planned_count"] == 5
    assert json.loads(snapshot_latest.read_text(encoding="utf-8"))["run_id"] == RUN_ID
    assert (
        tmp_path / "output" / "source_backed_monthly_pipeline_runs" / "latest.md"
    ).exists()


def _fake_stage_payload(script_name: str, output_root: Path) -> dict[str, Any]:
    run_dir = output_root / SNAPSHOT_DATE / RUN_ID
    payloads: dict[str, dict[str, Any]] = {
        "compile_monthly_source_contract_config.py": {
            "target_count": 3,
            "drift_count": 0,
            "missing_count": 0,
            "finding_count": 0,
            "high_finding_count": 0,
        },
        "audit_pi_list_view_filters.py": {
            "output_path": str(run_dir / "pi_list_view_filter_audit.json"),
        },
        "build_monthly_source_contract.py": {
            "manifest_path": str(run_dir / "monthly_source_contract.json"),
        },
        "extract_salesforce_sources.py": {
            "output_dir": str(run_dir / "salesforce_sources"),
        },
        "build_source_bundles_from_extracts.py": {
            "output_dir": str(run_dir / "source_bundles"),
            "manifest_path": str(
                run_dir / "source_bundles/source_bundle_manifest.json"
            ),
        },
        "build_director_bundles_from_sources.py": {
            "output_dir": str(run_dir / "director_bundles"),
            "manifest_path": str(
                run_dir / "director_bundles/director_bundle_manifest.json"
            ),
        },
        "audit_director_dataset_readiness.py": {
            "output_path": str(run_dir / "readiness/pipeline_open_readiness.json"),
        },
        "build_source_backed_analyst_workbook.py": {
            "workbook_path": str(run_dir / "analyst_workbook/source_backed.xlsx"),
        },
        "build_thinkcell_source_from_bundles.py": {
            "workbook_path": str(run_dir / "thinkcell/thinkcell_source.xlsx"),
            "ppttc_path": str(run_dir / "thinkcell/thinkcell_data.ppttc"),
        },
        "validate_monthly_source_backed_run.py": {
            "output_path": str(run_dir / "source_backed_publish_gate.json"),
        },
        "build_director_gold_analytics.py": {
            "manifest_paths": [str(run_dir / "gold_analytics/manifest.json")],
        },
        "build_deck_truth_packet.py": {
            "manifest_path": str(run_dir / "deck_truth_packet/deck_truth_packet.json"),
        },
        "build_source_backed_deck.py": {
            "deck_path": str(run_dir / "deck/source_backed_deck.pptx"),
            "manifest_path": str(run_dir / "deck/source_backed_deck_manifest.json"),
        },
        "polish_source_backed_deck_language.py": {
            "output_path": str(run_dir / "polish_audit/source_backed_deck_polish.json"),
        },
        "validate_source_backed_deck_visuals.py": {
            "output_path": str(run_dir / "visual_audit/source_backed_deck_visuals.json"),
        },
        "validate_source_backed_deck_table_contract.py": {
            "output_path": str(run_dir / "table_audit/source_backed_deck_table_contract.json"),
        },
        "validate_source_backed_deck_semantics.py": {
            "output_path": str(run_dir / "semantic_audit/source_backed_deck_semantic.json"),
        },
        "validate_source_backed_deck_render.py": {
            "output_path": str(run_dir / "render_audit/source_backed_deck_render.json"),
        },
        "build_source_backed_release_bundle.py": {
            "output_path": str(run_dir / "release_bundle/source_backed_release_bundle.json"),
        },
        "upload_sales_deck_release_to_sharepoint.py": {
            "status": "planned",
            "planned_count": 5,
            "missing_count": 0,
            "publish_ready": True,
            "source_backed": True,
            "folder": "General/Monthly/Q2 2026/run",
        },
    }
    return payloads.get(script_name, {})


def _write_fake_output_path(command: list[str]) -> None:
    command_parts = [str(part) for part in command]
    if "scripts/build_monthly_source_contract.py" in command_parts:
        output_root = Path(command_parts[command_parts.index("--output-root") + 1])
        snapshot_date = command_parts[command_parts.index("--snapshot-date") + 1]
        output_path = output_root / snapshot_date / "monthly_source_contract.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text('{"status":"ok"}\n', encoding="utf-8")
        return
    if "--output-path" not in command_parts:
        return
    output_path = Path(command_parts[command_parts.index("--output-path") + 1])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text('{"status":"ok"}\n', encoding="utf-8")
