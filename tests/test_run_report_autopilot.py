from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "run_report_autopilot.py"


def load_module():
    spec = importlib.util.spec_from_file_location("run_report_autopilot_test", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _workflow_payload(
    *,
    payload_status: str,
    workflow_status: str,
    apply_ready: bool,
    external_fill_requirement_count: int,
    manual_filter_intent_count: int,
    manual_detail_intent_count: int,
    omitted_sort_intent_count: int,
    review_artifact: str,
    workflow_output_dir: str,
) -> dict[str, object]:
    return {
        "status": payload_status,
        "workflow": {
            "output_dir": workflow_output_dir,
            "summary": {
                "workflow_status": workflow_status,
                "effective_surface_type": "salesforce_report",
                "candidate_surface_id": "commercial_rhythm_control_tower",
                "evaluation_verdict": "pass",
                "report_apply_ready": apply_ready,
                "report_apply_strategy": "create_new",
                "report_native_authoring_ready": True,
                "report_external_fill_requirement_count": external_fill_requirement_count,
                "resolved_report_filter_override_count": 2,
                "manual_report_filter_intent_count": manual_filter_intent_count,
                "report_manual_detail_intent_count": manual_detail_intent_count,
                "report_omitted_sort_intent_count": omitted_sort_intent_count,
                "planned_report_filter_overrides": [
                    {"source_label": "renewal_period", "value": "This Week"},
                    {"source_label": "risk_band", "value": "High"},
                ],
                "review_artifact": review_artifact,
                "executed_steps": [
                    {
                        "name": "preview_report_rest",
                        "messages": ["filter_override_applied", "rest_preview_ready"],
                    },
                    {
                        "name": "apply_report_rest_dry_run",
                        "messages": ["apply_preview_ready"] if apply_ready else ["apply_blocked"],
                    },
                ],
            },
        },
    }


def _write_workflow_artifacts(
    *,
    output_dir: Path,
    manual_filter_intent_count: int,
    manual_detail_intent_count: int,
    omitted_sort_intent_count: int,
) -> None:
    (output_dir / "report_handoff").mkdir(parents=True, exist_ok=True)
    (output_dir / "evaluation").mkdir(parents=True, exist_ok=True)
    (output_dir / "report_apply_preview").mkdir(parents=True, exist_ok=True)
    (output_dir / "README.md").write_text("# Workflow Review\n", encoding="utf-8")
    (output_dir / "report_handoff" / "build_package.json").write_text("{}", encoding="utf-8")
    (output_dir / "evaluation" / "evaluation.json").write_text(
        json.dumps({"verdict": "pass"}, indent=2),
        encoding="utf-8",
    )

    manual_filter_intents = [
        {
            "source_label": "owner",
            "guidance": "Apply the owner filter manually.",
        }
        for _ in range(manual_filter_intent_count)
    ]
    manual_detail_intents = [
        {
            "source_label": "Actual Ownership Alignment",
            "guidance": "Replace with a real field or formula before live use.",
        }
        for _ in range(manual_detail_intent_count)
    ]
    omitted_sort_intents = [
        {
            "source_label": "Action Queue: Handoff Quality",
            "guidance": "Apply this sort manually if still required.",
        }
        for _ in range(omitted_sort_intent_count)
    ]
    (output_dir / "report_apply_preview" / "salesforce_report_apply_preview.json").write_text(
        json.dumps(
            {
                "target_org": "apro@simcorp.com",
                "applied_filter_overrides": [
                    {"source_label": "renewal_period", "value": "This Week", "operator": "equals"},
                    {"source_label": "risk_band", "value": "High", "operator": "equals"},
                ],
                "manual_filter_intents": manual_filter_intents,
                "manual_detail_intents": manual_detail_intents,
                "omitted_sort_intents": omitted_sort_intents,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_report_autopilot_writes_manifest_and_summary(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    queue_path = tmp_path / "report_queue.json"
    queue_path.write_text(
        json.dumps(
            {
                "items": [
                    {
                        "key": "ready_report",
                        "priority": 10,
                        "query": "Manager owner list report for high risk renewals needing follow-up this week",
                    },
                    {
                        "key": "manual_followup_report",
                        "priority": 15,
                        "query": "Manager owner list report for renewals needing follow-up this week",
                    },
                    {
                        "key": "blocked_report",
                        "priority": 20,
                        "query": 'Manager owner list report for owner "Taylor Smith" in product family "Axioma"',
                    },
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    calls: list[tuple[str, Path]] = []

    def fake_run_report_workflow(*, query: str, output_dir: Path):
        calls.append((query, output_dir))
        if "Taylor Smith" in query:
            _write_workflow_artifacts(
                output_dir=output_dir,
                manual_filter_intent_count=0,
                manual_detail_intent_count=0,
                omitted_sort_intent_count=0,
            )
            payload = _workflow_payload(
                payload_status="warn",
                workflow_status="warn",
                apply_ready=False,
                external_fill_requirement_count=3,
                manual_filter_intent_count=0,
                manual_detail_intent_count=0,
                omitted_sort_intent_count=0,
                review_artifact=str(output_dir / "README.md"),
                workflow_output_dir=str(output_dir),
            )
            return module.WorkflowCommandResult(
                command=["workflow"],
                returncode=1,
                stdout=json.dumps(payload),
                stderr="",
                payload=payload,
            )
        if "renewals needing follow-up this week" in query and "high risk" not in query:
            _write_workflow_artifacts(
                output_dir=output_dir,
                manual_filter_intent_count=2,
                manual_detail_intent_count=2,
                omitted_sort_intent_count=2,
            )
            payload = _workflow_payload(
                payload_status="warn",
                workflow_status="warn",
                apply_ready=True,
                external_fill_requirement_count=0,
                manual_filter_intent_count=2,
                manual_detail_intent_count=2,
                omitted_sort_intent_count=2,
                review_artifact=str(output_dir / "README.md"),
                workflow_output_dir=str(output_dir),
            )
            return module.WorkflowCommandResult(
                command=["workflow"],
                returncode=1,
                stdout=json.dumps(payload),
                stderr="",
                payload=payload,
            )
        _write_workflow_artifacts(
            output_dir=output_dir,
            manual_filter_intent_count=0,
            manual_detail_intent_count=0,
            omitted_sort_intent_count=0,
        )
        payload = _workflow_payload(
            payload_status="warn",
            workflow_status="warn",
            apply_ready=True,
            external_fill_requirement_count=0,
            manual_filter_intent_count=0,
            manual_detail_intent_count=0,
            omitted_sort_intent_count=0,
            review_artifact=str(output_dir / "README.md"),
            workflow_output_dir=str(output_dir),
        )
        return module.WorkflowCommandResult(
            command=["workflow"],
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    monkeypatch.setattr(module, "run_report_workflow", fake_run_report_workflow)
    output_dir = tmp_path / "report_autopilot_run"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--queue",
            str(queue_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    exit_code = module.main()

    assert exit_code == 0
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    items = {item["key"]: item for item in manifest["items"]}
    assert items["ready_report"]["status"] == "ready"
    assert items["ready_report"]["report_apply_ready"] is True
    assert items["ready_report"]["promotion_runnable"] is True
    assert Path(items["ready_report"]["complete_command_artifact"]).exists()
    ready_plan = json.loads(Path(items["ready_report"]["promotion_plan_artifact"]).read_text(encoding="utf-8"))
    assert ready_plan["promotion_runnable"] is True
    assert ready_plan["clone_from_report_id"] == "00OTb000008TZaTMAW"
    assert "--clone-from-report-id" in ready_plan["complete_command"]
    ready_command = Path(items["ready_report"]["complete_command_artifact"]).read_text(encoding="utf-8")
    assert "salesforce_report_executor.py complete" in ready_command
    assert "00OTb000008TZaTMAW" in ready_command
    assert "filter_overrides.json" in ready_command
    assert items["manual_followup_report"]["status"] == "ready_manual_followup"
    assert items["manual_followup_report"]["promotion_runnable"] is False
    assert items["manual_followup_report"]["manual_followup_reasons"] == [
        "manual_filter_intents:2",
        "manual_detail_intents:2",
        "omitted_sort_intents:2",
    ]
    assert Path(items["manual_followup_report"]["manual_followup_artifact"]).exists()
    manual_followup_text = Path(items["manual_followup_report"]["manual_followup_artifact"]).read_text(
        encoding="utf-8"
    )
    assert "Manual Detail Columns" in manual_followup_text
    assert items["blocked_report"]["status"] == "blocked"
    assert items["blocked_report"]["blocked_reasons"] == [
        "report_apply_not_ready",
        "external_fill_requirements:3",
    ]
    summary_text = (output_dir / "RUN_SUMMARY.md").read_text(encoding="utf-8")
    assert "- Ready items: `1`" in summary_text
    assert "- Ready with manual follow-up items: `1`" in summary_text
    assert "- Blocked items: `1`" in summary_text
    blocked_readme = (output_dir / "blocked_report" / "README.md").read_text(encoding="utf-8")
    assert "external_fill_requirements:3" in blocked_readme
    assert calls == [
        (
            "Manager owner list report for high risk renewals needing follow-up this week",
            output_dir / "ready_report" / "workflow",
        ),
        (
            "Manager owner list report for renewals needing follow-up this week",
            output_dir / "manual_followup_report" / "workflow",
        ),
        (
            'Manager owner list report for owner "Taylor Smith" in product family "Axioma"',
            output_dir / "blocked_report" / "workflow",
        ),
    ]


def test_report_autopilot_resume_skips_completed_items(tmp_path: Path, monkeypatch) -> None:
    module = load_module()
    queue_path = tmp_path / "report_queue.json"
    queue_path.write_text(
        json.dumps(
            {
                "items": [
                    {"key": "ready_report", "priority": 10, "query": "ready"},
                    {"key": "new_report", "priority": 20, "query": "new"},
                ]
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    run_dir = tmp_path / "report_autopilot_run"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "manifest.json").write_text(
        json.dumps(
            {
                "queue": str(queue_path),
                "run_dir": str(run_dir),
                "started_at": "2026-03-31T09-00-00",
                "items": [
                    {
                        "key": "ready_report",
                        "status": "ready",
                        "report_apply_ready": True,
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    calls: list[str] = []

    def fake_run_report_workflow(*, query: str, output_dir: Path):
        calls.append(query)
        _write_workflow_artifacts(
            output_dir=output_dir,
            manual_filter_intent_count=0,
            manual_detail_intent_count=0,
            omitted_sort_intent_count=0,
        )
        payload = _workflow_payload(
            payload_status="warn",
            workflow_status="warn",
            apply_ready=True,
            external_fill_requirement_count=0,
            manual_filter_intent_count=0,
            manual_detail_intent_count=0,
            omitted_sort_intent_count=0,
            review_artifact=str(output_dir / "README.md"),
            workflow_output_dir=str(output_dir),
        )
        return module.WorkflowCommandResult(
            command=["workflow"],
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
            payload=payload,
        )

    monkeypatch.setattr(module, "run_report_workflow", fake_run_report_workflow)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT),
            "--queue",
            str(queue_path),
            "--resume-run",
            str(run_dir),
        ],
    )

    exit_code = module.main()

    assert exit_code == 0
    assert calls == ["new"]
    manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
    items = {item["key"]: item for item in manifest["items"]}
    assert items["ready_report"]["status"] == "ready"
    assert items["new_report"]["status"] == "ready"
