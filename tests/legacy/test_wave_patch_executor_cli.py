from __future__ import annotations

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading


ROOT = Path(__file__).resolve().parents[1]
BUILDER_BRAIN = ROOT / "scripts" / "builder_brain.py"
EXECUTOR = ROOT / "scripts" / "wave_patch_executor.py"


def run_builder_brain(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(BUILDER_BRAIN), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def run_executor(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    effective_env = os.environ.copy()
    if env:
        effective_env.update(env)
    effective_env.setdefault("CRM_AI_MEMORY_ROOT", tempfile.mkdtemp(prefix="crm_ai_wave_memory_"))
    return subprocess.run(
        [sys.executable, str(EXECUTOR), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=effective_env,
    )


def build_payload(tmp_path: Path, *, evaluation_path: Path | None = None) -> Path:
    command = [
        "handoff",
        "--query",
        "Executive commercial rhythm view for ownership alignment, handoff quality, and renewal semantic confidence",
        "--surface",
        "salesforce_report",
        "--output-dir",
        str(tmp_path),
    ]
    if evaluation_path is not None:
        command.extend(["--evaluation", str(evaluation_path)])
    command.append("--json")
    result = run_builder_brain(*command)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    return Path(payload["executor_handoff"]["wave_patch_payload_artifact"])


def write_evaluation_artifact(tmp_path: Path, *, verdict: str = "pass") -> Path:
    evaluation_path = tmp_path / f"evaluation_{verdict}.json"
    evaluation_path.write_text(
        json.dumps(
            {
                "run_id": "run_20260329_001",
                "verdict": verdict,
                "mutation_ready": verdict == "pass",
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return evaluation_path


def build_fake_sf_failure_bin(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import sys",
                "print('sf org display failed for test', file=sys.stderr)",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    return fake_bin


def build_fake_sf_success_bin(tmp_path: Path, *, instance_url: str, access_token: str = "test-token") -> Path:
    fake_bin = tmp_path / "bin_success"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "print(json.dumps({",
                f"  'result': {{'accessToken': {access_token!r}, 'instanceUrl': {instance_url!r}}}",
                "}))",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    return fake_bin


class _PatchCaptureHandler(BaseHTTPRequestHandler):
    def do_PATCH(self) -> None:  # noqa: N802
        content_length = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_length)
        self.server.requests.append(  # type: ignore[attr-defined]
            {
                "method": "PATCH",
                "path": self.path,
                "headers": dict(self.headers.items()),
                "body": json.loads(raw_body.decode("utf-8")),
            }
        )
        payload = json.dumps(self.server.response_payload).encode("utf-8")  # type: ignore[attr-defined]
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: object) -> None:
        return


class _PatchCaptureServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], response_payload: dict[str, object]) -> None:
        super().__init__(server_address, _PatchCaptureHandler)
        self.requests: list[dict[str, object]] = []
        self.response_payload = response_payload


def start_patch_capture_server(*, response_payload: dict[str, object]) -> tuple[_PatchCaptureServer, threading.Thread]:
    server = _PatchCaptureServer(("127.0.0.1", 0), response_payload)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def test_validate_wave_patch_payload(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    result = run_executor("validate", "--payload", str(payload_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["page_count"] == 3
    assert payload["summary"]["widget_count"] >= 4
    assert payload["summary"]["explicit_full_widgets"] >= 1


def test_compile_wave_patch_worklist(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    output_path = tmp_path / "wave_patch_worklist.json"
    result = run_executor(
        "worklist",
        "--payload",
        str(payload_path),
        "--output",
        str(output_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert output_path.exists()
    worklist = payload["worklist"]
    assert worklist["worklist_type"] == "wave_patch_worklist"
    assert worklist["summary"]["page_steps"] == 3
    assert worklist["summary"]["navigation_steps"] == 3
    assert worklist["summary"]["has_handoff_link"] is False
    assert worklist["summary"]["has_external_handoff"] is True
    assert any(step["operation"] == "upsert_widget" for step in worklist["steps"])
    assert any(
        step["operation"] == "record_external_handoff" and step["target_surface"] == "salesforce_report"
        for step in worklist["steps"]
    )


def test_compile_wave_patch_bundle_against_baseline(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    baseline_path = tmp_path / "baseline_dashboard.json"
    baseline_path.write_text(
        json.dumps(
                {
                    "state": {
                        "gridLayouts": [
                            {
                                "pages": [
                                {"name": "summary", "widgets": []},
                                {"name": "legacy_page", "widgets": []},
                            ]
                        }
                        ],
                        "widgets": {
                            "legacy_widget": {"type": "chart", "parameters": {"visualizationType": "line"}}
                        },
                        "steps": {
                            "f_fy": {
                                "type": "aggregateflex",
                                "broadcastFacet": True,
                                "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                                "query": {"query": '{"measures":[["count","*"]],"groups":["FYLabel"]}', "version": -1.0},
                            },
                            "f_region": {
                                "type": "aggregateflex",
                                "broadcastFacet": True,
                                "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                                "query": {"query": '{"measures":[["count","*"]],"groups":["SalesRegion"]}', "version": -1.0},
                            },
                            "f_motion": {
                                "type": "aggregateflex",
                                "broadcastFacet": True,
                                "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                                "query": {"query": '{"measures":[["count","*"]],"groups":["MotionType"]}', "version": -1.0},
                            },
                            "f_persona": {
                                "type": "aggregateflex",
                                "broadcastFacet": True,
                                "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                                "query": {"query": '{"measures":[["count","*"]],"groups":["OppOwnerPersona"]}', "version": -1.0},
                            },
                            "f_manager": {
                                "type": "aggregateflex",
                                "broadcastFacet": True,
                                "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                                "query": {"query": '{"measures":[["count","*"]],"groups":["OppManagerName"]}', "version": -1.0},
                            },
                        },
                    }
                }
            ),
            encoding="utf-8",
        )
    output_dir = tmp_path / "bundle"
    result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert (output_dir / "normalized_baseline_state.json").exists()
    assert (output_dir / "wave_patch_worklist.json").exists()
    assert (output_dir / "wave_patch_bundle.json").exists()
    assert (output_dir / "wave_patch_set.json").exists()
    assert (output_dir / "dashboard_state.patch.json").exists()
    assert (output_dir / "wave_patch_autofill_summary.json").exists()
    assert (output_dir / "wave_patch_fill_requirements.json").exists()
    assert (output_dir / "wave_patch_query_review_checklist.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    assert Path(payload["browser_health_index_artifact"]).exists()
    assert Path(payload["browser_health_landing_artifact"]).exists()
    assert isinstance(payload["browser_health_summary"], dict)
    assert isinstance(payload["browser_health_summary"].get("run_recency_counts"), dict)
    health_index_path = Path(payload["browser_health_index_artifact"])
    health_landing_path = Path(payload["browser_health_landing_artifact"])
    assert health_index_path.exists()
    assert health_landing_path.exists()
    review_text = Path(payload["review_artifact"]).read_text(encoding="utf-8")
    collection_overview = Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    browser_health = json.loads(health_index_path.read_text(encoding="utf-8"))
    assert "# Wave PATCH Run" in review_text
    assert "# Wave PATCH Runs" in collection_overview
    assert "# AI OS Collections" in browser_overview
    assert "## Health Snapshot" in browser_overview
    assert str(health_landing_path) in browser_overview
    assert json.loads(Path(payload["browser_index_artifact"]).read_text(encoding="utf-8"))["health_summary"]["collection_count"] >= 1
    assert browser_health["collection_count"] >= 1
    assert payload["browser_health_summary"]["collection_count"] == browser_health["collection_count"]
    assert any(item["code"] == "wave_patch_review_ready" for item in payload["messages"])
    assert any(item["code"] == "wave_patch_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_health_ready" for item in payload["messages"])
    patch_bundle = payload["patch_bundle"]
    assert patch_bundle["bundle_type"] == "wave_patch_bundle"
    assert patch_bundle["baseline"]["widget_count"] == 1
    assert any(item["page_name"] == "summary" and item["baseline_page_present"] is True for item in patch_bundle["fragments"]["page_scaffolds"])
    assert any(item["page_name"] == "ownership_handoffs" and item["baseline_page_present"] is False for item in patch_bundle["fragments"]["page_scaffolds"])
    assert any(item["column_map_patch"]["mode"] == "explicit_full" for item in patch_bundle["fragments"]["widget_upserts"])
    patch_set = patch_bundle["patch_set"]
    assert patch_set["patch_set_type"] == "wave_dashboard_patch_set"
    assert any(fragment["payload"]["name"] == "summary" for fragment in patch_set["page_fragments"])
    assert any(fragment["payload"]["parameters"]["visualizationType"] == "hbar" for fragment in patch_set["widget_fragments"] if fragment["payload"]["type"] == "chart")
    assert all(fragment["payload"]["type"] == "saql" for fragment in patch_set["step_fragments"])
    assert all(fragment["payload"]["query"].startswith('q = load "Commercial_Rhythm_Control_Tower";') for fragment in patch_set["step_fragments"])
    assert all(fragment["payload"]["broadcastFacet"] is True for fragment in patch_set["step_fragments"])
    assert any(
        fragment["payload"]["parameters"]["columnMap"]["dimensionAxis"] == ["ManagerName"]
        for fragment in patch_set["widget_fragments"]
        if isinstance(fragment["payload"].get("parameters", {}).get("columnMap"), dict)
    )
    assert any(
        fragment["payload"]["parameters"]["columnMap"]["plots"] == ["ForecastHygiene"]
        for fragment in patch_set["widget_fragments"]
        if isinstance(fragment["payload"].get("parameters", {}).get("columnMap"), dict)
    )
    assert any(fragment_id.startswith("widget_") for fragment_id in patch_set["apply_order"])
    step_index = next(index for index, value in enumerate(patch_set["apply_order"]) if value.startswith("step_"))
    widget_index = next(index for index, value in enumerate(patch_set["apply_order"]) if value.startswith("widget_"))
    assert step_index < widget_index
    candidate_summary = payload["candidate_state_summary"]
    assert candidate_summary["page_count"] == 3
    assert candidate_summary["contract_violation_count"] == 0
    assert candidate_summary["pages"] == ["summary", "ownership_handoffs", "process_quality"]
    assert candidate_summary["widget_count"] == 5
    assert payload["candidate_contract_violations"] == []
    autofill_summary = payload["autofill_summary"]
    assert autofill_summary["total_autofills"] == 14
    assert autofill_summary["by_category"]["step_definition"] == 10
    assert autofill_summary["by_category"]["widget_binding"] == 4
    assert "handoff_binding" not in autofill_summary["by_category"]
    assert autofill_summary["review_required_count"] == 0
    assert autofill_summary["review_required_by_category"] == {}
    fill_requirements = payload["fill_requirements"]
    assert fill_requirements["artifact_type"] == "wave_patch_fill_requirements"
    assert fill_requirements["blocking_for_live_patch"] is False
    assert fill_requirements["summary"]["total_requirements"] == 0
    assert fill_requirements["summary"]["by_category"] == {}
    assert all(item["code"] != "heuristic_query_review_required" for item in payload["messages"])
    query_review_checklist = payload["query_review_checklist"]
    assert query_review_checklist["artifact_type"] == "wave_patch_query_review_checklist"
    assert query_review_checklist["review_required_for_live_patch"] is False
    assert query_review_checklist["summary"]["total_items"] == 0

    step_fragments = {
        fragment["fragment_id"]: fragment["payload"]["query"] for fragment in patch_set["step_fragments"]
    }
    assert 'sum(CoveredRenewalOppCount) as CoveredRenewalOppCount' in step_fragments["step_summary_actual_ownership_alignment_1"]
    assert 'case when RenewalOppCount > 0 then (CoveredRenewalOppCount / RenewalOppCount) * 100 else 100 end as ActualOwnershipAlignment' in step_fragments["step_summary_actual_ownership_alignment_1"]
    assert 'q = foreach q generate OppManagerName as ManagerName, sum(ReviewCandidateCount) as ForecastHygiene, sum(OpenValue) as OpenValue;' in step_fragments["step_summary_variance_driver_forecast_hygiene_2"]
    assert 'q = foreach q generate OppOwnerName as OwnerName, OppOwnerPersona as Persona, sum(ReviewCandidateCount) as ForecastHygiene, sum(OpenValue) as OpenValue;' in step_fragments["step_ownership_handoffs_variance_driver_forecast_hygiene_1"]
    assert 'case when RenewalOppCount > 0 then (CoveredRenewalOppCount / RenewalOppCount) * 100 else 0 end as RenewalSemanticConfidence' in step_fragments["step_ownership_handoffs_risk_renewal_semantic_confidence_2"]
    assert 'max(ReviewCandidateCount) as HandoffQuality' in step_fragments["step_process_quality_action_queue_handoff_quality_1"]
    assert 'q = order q by HandoffQuality desc;' in step_fragments["step_process_quality_action_queue_handoff_quality_1"]

    candidate_state = json.loads((output_dir / "dashboard_state.patch.json").read_text())
    assert list(candidate_state["steps"].keys())
    assert candidate_state["widgets"]["summary_headline_story_1"]["type"] == "number"
    assert candidate_state["widgets"]["summary_headline_story_1"]["parameters"]["measureField"] == "ActualOwnershipAlignment"
    assert "text" not in candidate_state["widgets"]["summary_headline_story_1"]["parameters"]
    assert "handoff_link_salesforce_report" not in candidate_state["widgets"]
    assert all(
        item["name"] != "handoff_link_salesforce_report"
        for item in candidate_state["gridLayouts"][0]["pages"][-1]["widgets"]
    )
    assert patch_set["handoff_fragment"] is None
    assert patch_set["external_handoff"]["destination_type"] == "report"
    assert patch_set["external_handoff"]["target_surface_id"] == "00OTb000008TZaTMAW"

    autofill_summary_file = json.loads((output_dir / "wave_patch_autofill_summary.json").read_text())
    assert autofill_summary_file["total_autofills"] == 14
    assert autofill_summary_file["by_category"]["step_definition"] == 10
    assert autofill_summary_file["by_category"]["widget_binding"] == 4
    assert "handoff_binding" not in autofill_summary_file["by_category"]
    assert autofill_summary_file["review_required_count"] == 0

    fill_requirements_file = json.loads((output_dir / "wave_patch_fill_requirements.json").read_text())
    assert fill_requirements_file["summary"]["total_requirements"] == 0
    assert fill_requirements_file["requirements"] == []

    query_review_file = json.loads((output_dir / "wave_patch_query_review_checklist.json").read_text())
    assert query_review_file["summary"]["total_items"] == 0
    assert query_review_file["items"] == []


def test_bundle_resolves_known_crma_handoff_destination(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "payload_type": "wave_patch_payload",
                "target_surface": {
                    "surface_type": "crma_dashboard",
                    "candidate_surface_id": "bdr_manager",
                    "candidate_surface_labels": ["BDR Manager"],
                },
                "baseline_requirements": {
                    "requires_live_export": True,
                    "normalization_required": True,
                    "guardrails": ["Run normalized contract lint before any PATCH attempt."],
                },
                "navigation_contract": {
                    "mode": "single_page",
                    "pages": [
                        {
                            "page": "Summary",
                            "page_name": "summary",
                            "destination_name": "summary",
                        }
                    ],
                },
                "page_mutations": [
                    {
                        "page": "Summary",
                        "page_name": "summary",
                        "purpose": "Show the current queue headline.",
                        "section_mutations": [
                            {
                                "section": "headline_story",
                                "section_order": 1,
                                "layout_band": "hero_row",
                                "intent": "Name the current operating issue.",
                                "widget_mutations": [
                                    {
                                        "component_key": "summary_headline_story_1",
                                        "role": "headline_metric",
                                        "metric": "Actual Outreach",
                                        "visualization_type": "number",
                                        "layout_band": "hero_row",
                                        "recommended_step_alias": "summary_actual_outreach_1",
                                        "column_map_strategy": "auto_detect",
                                        "contract_checks": [
                                            "Do not force a columnMap unless the live asset proves it is necessary."
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "handoff_link": {
                    "target_surface": "crma_dashboard",
                    "target_surface_label": "BDR Manager",
                    "mode": "named_surface_link",
                },
                "validation_contract": {
                    "review_gates": ["semantic_truth"],
                    "design_constraints": ["Keep the action path intact."],
                    "required_checks": ["normalized_contract_lint", "screenshot_review"],
                },
            }
        ),
        encoding="utf-8",
    )
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "state": {
                    "gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}],
                    "widgets": {},
                    "steps": {
                        "f_manager": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "BDR_Operating_System"}],
                            "query": {
                                "query": '{"measures":[["count","*"]],"groups":["ManagerName"]}',
                                "version": -1.0,
                            },
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["fill_requirements"]["summary"]["total_requirements"] == 0
    handoff_fragment = payload["patch_bundle"]["patch_set"]["handoff_fragment"]
    assert handoff_fragment["payload"]["parameters"]["destination"] == "0FKTb0000000IzROAU"
    assert handoff_fragment["payload"]["parameters"]["destinationLink"]["name"] == "0FKTb0000000IzROAU"
    assert payload["query_review_checklist"]["summary"]["total_items"] == 1


def test_bundle_resolves_known_report_handoff_destination(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    payload_path.write_text(
        json.dumps(
            {
                "payload_type": "wave_patch_payload",
                "target_surface": {
                    "surface_type": "crma_dashboard",
                    "candidate_surface_id": "commercial_rhythm_control_tower",
                    "candidate_surface_labels": ["Commercial Rhythm Control Tower"],
                },
                "baseline_requirements": {
                    "requires_live_export": True,
                    "normalization_required": True,
                    "guardrails": ["Run normalized contract lint before any PATCH attempt."],
                },
                "navigation_contract": {
                    "mode": "single_page",
                    "pages": [
                        {
                            "page": "Summary",
                            "page_name": "summary",
                            "destination_name": "summary",
                        }
                    ],
                },
                "page_mutations": [
                    {
                        "page": "Summary",
                        "page_name": "summary",
                        "purpose": "Show the current operating issue.",
                        "section_mutations": [
                            {
                                "section": "headline_story",
                                "section_order": 1,
                                "layout_band": "hero_row",
                                "intent": "Name the current operating issue.",
                                "widget_mutations": [
                                    {
                                        "component_key": "summary_headline_story_1",
                                        "role": "headline_metric",
                                        "metric": "Actual Ownership Alignment",
                                        "visualization_type": "number",
                                        "layout_band": "hero_row",
                                        "recommended_step_alias": "summary_actual_ownership_alignment_1",
                                        "column_map_strategy": "auto_detect",
                                        "contract_checks": [
                                            "Do not force a columnMap unless the live asset proves it is necessary."
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
                "handoff_link": {
                    "target_surface": "salesforce_report",
                    "target_surface_id": "00OTb000008TZaTMAW",
                    "target_surface_label": "Forecast & Closed Won",
                    "target_destination_name": "00OTb000008TZaTMAW",
                    "destination_type": "report",
                    "mode": "named_surface_link",
                },
                "validation_contract": {
                    "review_gates": ["semantic_truth"],
                    "design_constraints": ["Keep the action path intact."],
                    "required_checks": ["normalized_contract_lint", "screenshot_review"],
                },
            }
        ),
        encoding="utf-8",
    )
    baseline_path = tmp_path / "baseline.json"
    baseline_path.write_text(
        json.dumps(
            {
                "state": {
                    "gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}],
                    "widgets": {},
                    "steps": {
                        "f_manager": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {
                                "query": '{"measures":[["count","*"]],"groups":["ManagerName"]}',
                                "version": -1.0,
                            },
                        }
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["fill_requirements"]["summary"]["total_requirements"] == 0
    assert payload["patch_bundle"]["patch_set"]["handoff_fragment"] is None
    external_handoff = payload["patch_bundle"]["patch_set"]["external_handoff"]
    assert external_handoff["target_surface_id"] == "00OTb000008TZaTMAW"
    assert external_handoff["destination_type"] == "report"
    assert external_handoff["implementation"] == "package_only"
    assert payload["query_review_checklist"]["summary"]["total_items"] == 0


def test_deploy_preview_infers_dashboard_target_from_baseline_summary(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps(
            {
                "state": {
                    "gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}],
                    "widgets": {},
                    "steps": {
                        "f_fy": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {"query": '{"measures":[["count","*"]],"groups":["FYLabel"]}', "version": -1.0},
                        },
                        "f_region": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {"query": '{"measures":[["count","*"]],"groups":["SalesRegion"]}', "version": -1.0},
                        },
                        "f_motion": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {"query": '{"measures":[["count","*"]],"groups":["MotionType"]}', "version": -1.0},
                        },
                        "f_persona": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {"query": '{"measures":[["count","*"]],"groups":["OppOwnerPersona"]}', "version": -1.0},
                        },
                        "f_manager": {
                            "type": "aggregateflex",
                            "broadcastFacet": True,
                            "datasets": [{"id": "0FbTest", "name": "Commercial_Rhythm_Control_Tower"}],
                            "query": {"query": '{"measures":[["count","*"]],"groups":["OppManagerName"]}', "version": -1.0},
                        },
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps(
            {
                "id": "0FKTb0000000JPFOA2",
                "label": "Commercial Rhythm Control Tower",
                "name": "Commercial_Rhythm_Control_Tower",
            }
        ),
        encoding="utf-8",
    )
    bundle_dir = tmp_path / "bundle"
    bundle_result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(bundle_dir),
        "--json",
    )
    assert bundle_result.returncode == 0, bundle_result.stderr or bundle_result.stdout

    deploy_dir = tmp_path / "deploy"
    deploy_result = run_executor(
        "deploy",
        "--state",
        str(bundle_dir / "dashboard_state.patch.json"),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(deploy_dir),
        "--json",
    )
    assert deploy_result.returncode == 0, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "read_only"
    assert payload["deploy_target"]["dashboard_id"] == "0FKTb0000000JPFOA2"
    assert payload["deploy_target"]["dashboard_label"] == "Commercial Rhythm Control Tower"
    assert payload["deploy_target"]["source"] == "summary_json"
    assert payload["deploy_summary"]["mode"] == "dry_run"
    assert payload["deploy_summary"]["contract_violation_count"] == 0
    assert payload["request_preview"]["request_path"] == "/services/data/v66.0/wave/dashboards/0FKTb0000000JPFOA2"
    assert (deploy_dir / "wave_patch_request.json").exists()
    assert Path(payload["review_artifact"]).exists()
    assert Path(payload["collection_index_artifact"]).exists()
    assert Path(payload["collection_landing_artifact"]).exists()
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    assert "# Wave PATCH Run" in Path(payload["review_artifact"]).read_text(encoding="utf-8")
    assert "# Wave PATCH Runs" in Path(payload["collection_landing_artifact"]).read_text(encoding="utf-8")
    assert "# AI OS Collections" in Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    assert any(item["code"] == "wave_patch_review_ready" for item in payload["messages"])
    assert any(item["code"] == "wave_patch_collection_index_ready" for item in payload["messages"])
    assert any(item["code"] == "ai_os_browser_ready" for item in payload["messages"])


def test_deploy_apply_requires_pass_evaluation(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps({"state": {"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}}),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps({"id": "0FKTb0000000JPFOA2", "label": "Commercial Rhythm Control Tower"}),
        encoding="utf-8",
    )
    bundle_dir = tmp_path / "bundle"
    bundle_result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(bundle_dir),
        "--json",
    )
    assert bundle_result.returncode == 0, bundle_result.stderr or bundle_result.stdout

    deploy_result = run_executor(
        "deploy",
        "--state",
        str(bundle_dir / "dashboard_state.patch.json"),
        "--baseline",
        str(baseline_path),
        "--apply",
        "--json",
    )
    assert deploy_result.returncode == 1, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_required" for item in payload["messages"])


def test_deploy_preview_blocks_missing_selector_step_references(tmp_path: Path) -> None:
    state_path = tmp_path / "dashboard_state.patch.json"
    state_path.write_text(
        json.dumps(
            {
                "filters": [],
                "gridLayouts": [
                    {
                        "pages": [
                            {
                                "name": "summary",
                                "widgets": [
                                    {
                                        "name": "headline_kpi",
                                        "row": 0,
                                        "column": 0,
                                        "rowspan": 2,
                                        "colspan": 2,
                                    }
                                ],
                            }
                        ]
                    }
                ],
                "widgets": {
                    "headline_kpi": {
                        "type": "number",
                        "parameters": {
                            "step": "summary_kpi",
                            "measureField": "Value",
                        },
                    }
                },
                "steps": {
                    "summary_kpi": {
                        "type": "saql",
                        "query": (
                            'q = load "Dataset";\n'
                            'q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), '
                            'column(f_region.result, ["SalesRegion"])).asEquality(\'SalesRegion\')}};\n'
                            "q = group q by all;\n"
                            "q = foreach q generate count() as Value;"
                        ),
                    }
                },
                "widgetStyle": {},
            }
        ),
        encoding="utf-8",
    )
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps({"state": {"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}}),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps({"id": "0FKTb0000000JPFOA2", "label": "Commercial Rhythm Control Tower"}),
        encoding="utf-8",
    )

    deploy_result = run_executor(
        "deploy",
        "--state",
        str(state_path),
        "--baseline",
        str(baseline_path),
        "--json",
    )
    assert deploy_result.returncode == 1, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["status"] == "error"
    assert payload["deploy_summary"]["contract_violation_count"] == 1
    assert any(item["code"] == "candidate_contract_violation" for item in payload["messages"])
    assert any("step_reference_missing" in item["text"] for item in payload["messages"])


def test_deploy_apply_writes_bypass_audit_artifact(tmp_path: Path) -> None:
    payload_path = build_payload(tmp_path)
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps({"state": {"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}}),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps({"id": "0FKTb0000000JPFOA2", "label": "Commercial Rhythm Control Tower"}),
        encoding="utf-8",
    )
    bundle_dir = tmp_path / "bundle"
    bundle_result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(bundle_dir),
        "--json",
    )
    assert bundle_result.returncode == 0, bundle_result.stderr or bundle_result.stdout
    assert (bundle_dir / "wave_patch_memory_context.json").exists()

    fake_bin = build_fake_sf_failure_bin(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    deploy_dir = tmp_path / "deploy"
    deploy_result = run_executor(
        "deploy",
        "--state",
        str(bundle_dir / "dashboard_state.patch.json"),
        "--baseline",
        str(baseline_path),
        "--apply",
        "--allow-missing-evaluation",
        "--output-dir",
        str(deploy_dir),
        "--json",
        env=env,
    )
    assert deploy_result.returncode == 1, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["status"] == "error"
    assert any(item["code"] == "evaluation_bypass_used" for item in payload["messages"])
    assert payload["policy_exceptions"] == ["evaluation_bypass"]
    assert payload["memory_record"]["run_id"] == "bundle"
    bypass_path = deploy_dir / "evaluation_bypass_audit.json"
    assert bypass_path.exists()
    bypass_payload = json.loads(bypass_path.read_text(encoding="utf-8"))
    assert bypass_payload["policy_exceptions"] == ["evaluation_bypass"]
    assert bypass_payload["evaluation_gate"]["bypassed"] is True
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "bundle.json").read_text(encoding="utf-8"))
    assert memory_record["goal"] == "Wave PATCH for Commercial Rhythm Control Tower"
    assert memory_record["policy_exceptions"] == ["evaluation_bypass"]
    assert memory_record["outcome"] == "wave_patch_executor_deploy_error"


def test_deploy_apply_derives_memory_context_from_baseline_summary(tmp_path: Path) -> None:
    state_path = tmp_path / "dashboard_state.patch.json"
    state_path.write_text(
        json.dumps({"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}),
        encoding="utf-8",
    )
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps({"state": {"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}}),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps({"id": "0FKTb0000000JPFOA2", "label": "Commercial Rhythm Control Tower"}),
        encoding="utf-8",
    )

    fake_bin = build_fake_sf_failure_bin(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
    deploy_dir = tmp_path / "deploy_manual"
    deploy_result = run_executor(
        "deploy",
        "--state",
        str(state_path),
        "--baseline",
        str(baseline_path),
        "--apply",
        "--allow-missing-evaluation",
        "--output-dir",
        str(deploy_dir),
        "--json",
        env=env,
    )
    assert deploy_result.returncode == 1, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["memory_record"]["run_id"] == "deploy_manual"
    memory_record = json.loads((tmp_path / "agent_memory" / "runs" / "deploy_manual.json").read_text(encoding="utf-8"))
    assert memory_record["goal"] == "Wave PATCH for Commercial Rhythm Control Tower"
    assert "0FKTb0000000JPFOA2" in memory_record["tags"]
    assert memory_record["policy_exceptions"] == ["evaluation_bypass"]


def test_deploy_apply_returns_success_when_patch_succeeds(tmp_path: Path) -> None:
    evaluation_path = write_evaluation_artifact(tmp_path, verdict="pass")
    payload_path = build_payload(tmp_path, evaluation_path=evaluation_path)
    baseline_dir = tmp_path / "baseline_export"
    baseline_dir.mkdir(parents=True)
    baseline_path = baseline_dir / "dashboard.json"
    baseline_path.write_text(
        json.dumps({"state": {"gridLayouts": [{"pages": [{"name": "summary", "widgets": []}]}], "widgets": {}, "steps": {}}}),
        encoding="utf-8",
    )
    (baseline_dir / "summary.json").write_text(
        json.dumps({"id": "0FKTb0000000JPFOA2", "label": "Commercial Rhythm Control Tower"}),
        encoding="utf-8",
    )
    bundle_dir = tmp_path / "bundle"
    bundle_result = run_executor(
        "bundle",
        "--payload",
        str(payload_path),
        "--baseline",
        str(baseline_path),
        "--output-dir",
        str(bundle_dir),
        "--json",
    )
    assert bundle_result.returncode == 0, bundle_result.stderr or bundle_result.stdout

    response_payload = {
        "id": "0FKTb0000000JPFOA2",
        "name": "sales_ops_quarterly_dashboard",
        "label": "Sales Ops Quarterly Dashboard",
    }
    server, thread = start_patch_capture_server(response_payload=response_payload)
    try:
        fake_bin = build_fake_sf_success_bin(
            tmp_path,
            instance_url=f"http://127.0.0.1:{server.server_address[1]}",
        )
        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["CRM_AI_MEMORY_ROOT"] = str(tmp_path / "agent_memory")
        deploy_dir = tmp_path / "deploy_success"
        deploy_result = run_executor(
            "deploy",
            "--state",
            str(bundle_dir / "dashboard_state.patch.json"),
            "--baseline",
            str(baseline_path),
            "--apply",
            "--evaluation",
            str(evaluation_path),
            "--output-dir",
            str(deploy_dir),
            "--json",
            env=env,
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert deploy_result.returncode == 0, deploy_result.stderr or deploy_result.stdout
    payload = json.loads(deploy_result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["deploy_summary"]["mode"] == "apply"
    assert payload["applied_dashboard"]["id"] == "0FKTb0000000JPFOA2"
    assert payload["applied_dashboard"]["name"] == "sales_ops_quarterly_dashboard"
    assert payload["applied_dashboard"]["label"] == "Sales Ops Quarterly Dashboard"
    assert any(item["code"] == "deploy_applied" for item in payload["messages"])
    assert len(server.requests) == 1
    request = server.requests[0]
    assert request["path"] == "/services/data/v66.0/wave/dashboards/0FKTb0000000JPFOA2"
    assert request["headers"]["Authorization"] == "Bearer test-token"
    assert request["headers"]["Content-Type"] == "application/json"
    assert isinstance(request["body"], dict)
    assert "state" in request["body"]
