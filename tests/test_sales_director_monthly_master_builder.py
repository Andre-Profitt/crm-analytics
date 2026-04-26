from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_validated_director_brief import (
    build_structured_fill_payload,
    build_validation_artifacts,
    validate_excel_brief,
)
import scripts.claude_office_etl as office_etl
from scripts.claude_office_etl import _permission_group_from_dump, _status_from_dump, _status_snapshot
from scripts.monthly_platform.period import resolve_period_context
from scripts.run_sales_director_monthly_master_builder import (
    DirectorTarget,
    build_latest_status_markdown,
    build_latest_status_packet,
    build_preflight_failure_payload,
    build_run_summary_payload,
    build_targets,
    director_record_base,
    execute_target,
    plan_review_deck,
    prepare_review_deck,
    run_excel_brief,
    run_powerpoint_lane,
    write_latest_aliases,
)


def test_build_targets_reuses_snapshot_and_matches_existing_deck(tmp_path: Path) -> None:
    workbook_root = tmp_path / "workbooks"
    snapshot_root = tmp_path / "snapshots"
    deck_root = tmp_path / "decks"
    snapshot_date = "2026-04-10"

    workbook_dir = workbook_root / snapshot_date
    workbook_dir.mkdir(parents=True)
    workbook_path = workbook_dir / "Sales Director Data - Jane Doe (APAC).xlsx"
    workbook_path.write_bytes(b"placeholder")

    snapshot_dir = snapshot_root / snapshot_date
    snapshot_dir.mkdir(parents=True)
    snapshot_path = snapshot_dir / "jane-doe.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "director_name": "Jane Doe",
                "territory": "APAC",
                "snapshot_date": snapshot_date,
            }
        ),
        encoding="utf-8",
    )

    deck_dir = deck_root / snapshot_date
    deck_dir.mkdir(parents=True)
    deck_path = deck_dir / "Sales Director Monthly - Jane Doe (APAC).pptx"
    deck_path.write_bytes(b"pptx")

    targets = build_targets(
        snapshot_date=snapshot_date,
        director="Jane Doe",
        workbook_root=workbook_root,
        snapshot_root=snapshot_root,
        deck_root=deck_root,
        deck_date=snapshot_date,
        refresh_snapshots=False,
    )

    assert len(targets) == 1
    target = targets[0]
    assert target.director_name == "Jane Doe"
    assert target.snapshot_path == snapshot_path
    assert target.existing_deck_path == deck_path


def test_build_targets_supports_live_workbook_slug_filenames(tmp_path: Path) -> None:
    workbook_root = tmp_path / "director_live_workbooks"
    snapshot_root = tmp_path / "snapshots"
    deck_root = tmp_path / "decks"
    snapshot_date = "2026-04-23"

    workbook_dir = workbook_root / snapshot_date
    workbook_dir.mkdir(parents=True)
    workbook_path = workbook_dir / "jesper-tyrer.xlsx"
    workbook_path.write_bytes(b"placeholder")

    snapshot_dir = snapshot_root / snapshot_date
    snapshot_dir.mkdir(parents=True)
    snapshot_path = snapshot_dir / "jesper-tyrer.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "director_name": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_date": snapshot_date,
            }
        ),
        encoding="utf-8",
    )

    targets = build_targets(
        snapshot_date=snapshot_date,
        director="Jesper Tyrer",
        workbook_root=workbook_root,
        snapshot_root=snapshot_root,
        deck_root=deck_root,
        deck_date=snapshot_date,
        refresh_snapshots=False,
    )

    assert len(targets) == 1
    target = targets[0]
    assert target.director_name == "Jesper Tyrer"
    assert target.workbook_path == workbook_path
    assert target.snapshot_path == snapshot_path


def test_build_preflight_failure_payload_lists_available_snapshots(tmp_path: Path) -> None:
    workbook_root = tmp_path / "workbooks"
    (workbook_root / "2026-03-31").mkdir(parents=True)
    (workbook_root / "2026-04-10").mkdir()
    (workbook_root / "notes").mkdir()

    payload = build_preflight_failure_payload(
        snapshot_date="2026-04-30",
        director="Jane Doe",
        deck_source="canonical-shell",
        fallback_workbook_deck=False,
        workbook_root=workbook_root,
        period=resolve_period_context(as_of_date="2026-05-01"),
        exc=FileNotFoundError("No workbook files found for snapshot date 2026-04-30."),
    )

    assert payload["status"] == "error"
    assert payload["phase"] == "preflight"
    assert payload["preflight"]["requested_snapshot_date"] == "2026-04-30"
    assert payload["preflight"]["available_snapshot_dates"] == [
        "2026-03-31",
        "2026-04-10",
    ]
    assert payload["preflight"]["latest_available_snapshot_date"] == "2026-04-10"


def test_prepare_review_deck_skips_when_existing_deck_is_missing() -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=Path("/tmp/jane.xlsx"),
        snapshot_path=Path("/tmp/jane.json"),
        existing_deck_path=None,
    )

    review_path, stage = prepare_review_deck(
        target,
        snapshot_date="2026-04-10",
        deck_source="existing",
        template_deck_path=Path("/tmp/template.pptx"),
        shell_root=Path("/tmp/shells"),
        canonical_shell_root=Path("/tmp/canonical"),
        allow_generated_shell_fallback=False,
        fallback_workbook_deck=False,
        workbook_deck_root=Path("/tmp/workbook-decks"),
        render_workbook_deck=False,
    )

    assert review_path is None
    assert stage["status"] == "skipped"
    assert "No existing review deck" in stage["reason"]


def test_prepare_review_deck_uses_template_source(tmp_path: Path) -> None:
    template = tmp_path / "SimCorp_PPT_Template.pptx"
    template.write_bytes(b"pptx")
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "jane.xlsx",
        snapshot_path=tmp_path / "jane.json",
        existing_deck_path=None,
    )

    review_path, stage = prepare_review_deck(
        target,
        snapshot_date="2026-04-10",
        deck_source="template",
        template_deck_path=template,
        shell_root=tmp_path / "shells",
        canonical_shell_root=tmp_path / "canonical",
        allow_generated_shell_fallback=False,
        fallback_workbook_deck=False,
        workbook_deck_root=tmp_path / "workbook-decks",
        render_workbook_deck=False,
    )

    assert review_path == template
    assert stage == {
        "status": "ok",
        "source": "template",
        "deck_path": str(template),
    }


def test_prepare_review_deck_uses_generated_shell_source(monkeypatch, tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "jane.xlsx",
        snapshot_path=tmp_path / "jane.json",
        existing_deck_path=None,
    )
    captured: dict[str, object] = {}

    def fake_build_shell_deck(**kwargs):  # type: ignore[no-untyped-def]
        captured.update(kwargs)
        output_path = kwargs["output_path"]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_bytes(b"pptx")
        return {"deck_path": str(output_path), "slide_count": 11}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_shell_deck",
        fake_build_shell_deck,
    )

    review_path, stage = prepare_review_deck(
        target,
        snapshot_date="2026-04-10",
        deck_source="shell",
        template_deck_path=tmp_path / "SimCorp_PPT_Template.pptx",
        shell_root=tmp_path / "shells",
        canonical_shell_root=tmp_path / "canonical",
        allow_generated_shell_fallback=False,
        fallback_workbook_deck=False,
        workbook_deck_root=tmp_path / "workbook-decks",
        render_workbook_deck=False,
    )

    assert review_path is not None
    assert review_path.name == "Sales Director Monthly Shell - Jane Doe (APAC).pptx"
    assert captured["director_name"] == "Jane Doe"
    assert captured["territory"] == "APAC"
    assert stage["status"] == "ok"
    assert stage["source"] == "generated-shell"
    assert stage["publish_safe"] is False


def test_prepare_review_deck_uses_canonical_shell_when_present(tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "jane.xlsx",
        snapshot_path=tmp_path / "jane.json",
        existing_deck_path=None,
    )
    canonical = tmp_path / "canonical" / "2026-04-10" / "Sales Director Monthly Shell - Jane Doe (APAC).pptx"
    canonical.parent.mkdir(parents=True)
    canonical.write_bytes(b"pptx")

    review_path, stage = prepare_review_deck(
        target,
        snapshot_date="2026-04-10",
        deck_source="canonical-shell",
        template_deck_path=tmp_path / "SimCorp_PPT_Template.pptx",
        shell_root=tmp_path / "shells",
        canonical_shell_root=tmp_path / "canonical",
        allow_generated_shell_fallback=False,
        fallback_workbook_deck=False,
        workbook_deck_root=tmp_path / "workbook-decks",
        render_workbook_deck=False,
    )

    assert review_path == canonical
    assert stage["source"] == "canonical-shell"


def test_prepare_review_deck_requires_canonical_shell_by_default(tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "jane.xlsx",
        snapshot_path=tmp_path / "jane.json",
        existing_deck_path=None,
    )

    try:
        prepare_review_deck(
            target,
            snapshot_date="2026-04-10",
            deck_source="canonical-shell",
            template_deck_path=tmp_path / "SimCorp_PPT_Template.pptx",
            shell_root=tmp_path / "shells",
            canonical_shell_root=tmp_path / "canonical",
            allow_generated_shell_fallback=False,
            fallback_workbook_deck=False,
            workbook_deck_root=tmp_path / "workbook-decks",
            render_workbook_deck=False,
        )
    except FileNotFoundError as exc:
        assert "Create and promote the canonical shell first" in str(exc)
    else:  # pragma: no cover
        raise AssertionError("Expected FileNotFoundError when canonical director shell is missing.")


def test_plan_review_deck_reports_missing_canonical_shell(tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "jane.xlsx",
        snapshot_path=tmp_path / "jane.json",
        existing_deck_path=None,
    )

    review_path, stage = plan_review_deck(
        target,
        snapshot_date="2026-04-10",
        deck_source="canonical-shell",
        template_deck_path=tmp_path / "SimCorp_PPT_Template.pptx",
        shell_root=tmp_path / "shells",
        canonical_shell_root=tmp_path / "canonical",
        allow_generated_shell_fallback=False,
        fallback_workbook_deck=False,
        workbook_deck_root=tmp_path / "workbook-decks",
    )

    assert review_path is None
    assert stage["status"] == "missing"
    assert stage["source"] == "canonical-shell"


def test_validate_excel_brief_accepts_blank_input() -> None:
    report = validate_excel_brief({"q1_review": {"actuals": {}}}, "")
    assert report["issues"] == [
        {
            "severity": "info",
            "message": "No Excel Claude brief supplied; generated the fact pack directly from the validated snapshot.",
        }
    ]


def test_build_structured_fill_payload_falls_forward_when_current_quarter_is_empty() -> None:
    snapshot = {
        "director_name": "Jane Doe",
        "territory": "APAC",
        "snapshot_date": "2026-04-30",
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": "€0",
                        "Weighted Pipeline (probability-adj)": "€0.8M",
                    }
                }
            }
        },
        "q2_outlook": {
            "breakdown": [
                {"Forecast Category": "Commit", "Deal Count": 0, "ARR (€ converted)": 0},
                {"Forecast Category": "Best Case", "Deal Count": 0, "ARR (€ converted)": 0},
                {"Forecast Category": "Omitted", "Deal Count": 1, "ARR (€ converted)": 200000},
            ]
        },
        "pipeline_detail": {
            "records": [
                {
                    "Opportunity": "Q3 Deal A",
                    "ARR (€ converted)": 700000,
                    "Close Date": "2026-07-15",
                    "Forecast Category": "Commit",
                    "Owner": "Rep A",
                },
                {
                    "Opportunity": "Q3 Deal B",
                    "ARR (€ converted)": 300000,
                    "Close Date": "2026-08-10",
                    "Forecast Category": "Best Case",
                    "Owner": "Rep B",
                },
            ],
            "top_opportunities": [],
        },
    }

    payload = build_structured_fill_payload(snapshot)

    quarterly = next(
        slide for slide in payload["slides"] if slide["id"] == "quarterly-pipeline"
    )
    coverage = next(
        slide for slide in payload["slides"] if slide["id"] == "pipeline-coverage-intel"
    )

    assert quarterly["slots"]["headline_pipeline_arr_q2"] == "€1.0M"
    assert quarterly["slots"]["q2_commit_arr"] == "€700K"
    assert quarterly["slots"]["q2_best_case_arr"] == "€300K"
    assert quarterly["slots"]["quarterly_pipeline_label"] == "Q3"
    assert quarterly["slots"]["quarterly_pipeline_title"] == "Q3 2026"
    assert (
        quarterly["slots"]["quarterly_pipeline_display_reason"]
        == "forward_quarter_fallback"
    )
    assert (
        quarterly["slots"]["quarterly_pipeline_footnote"]
        == "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook."
    )
    assert coverage["slots"]["top_opportunities"][0]["opportunity"] == "Q3 Deal A"
    assert "Q3 2026 active ARR" in coverage["slots"]["pipeline_coverage_statement"]


def test_build_validation_artifacts_include_powerpoint_build_prompt() -> None:
    snapshot = {
        "director_name": "Jane Doe",
        "territory": "APAC",
        "snapshot_date": "2026-04-10",
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — All Open (any close date)": "€10.0M",
                        "Deal Count": 12,
                        "Pipeline ARR — FY26 Close Dates Only (excl. Omitted)": "€6.0M",
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": "€2.0M",
                        "Weighted Pipeline (probability-adj)": "€1.0M",
                        "New Pipeline This Quarter (excl. Omitted)": "€0.5M",
                    }
                },
                "process-compliance": {
                    "metrics": {
                        "Approval Rate (stage 3+)": "25.0%",
                        "Missing Approval (Land, stage 3+)": 2,
                    }
                },
                "risk": {
                    "metrics": {
                        "Stale 30d+ (ARR)": "€1.5M",
                        "Aging 365+ (ARR)": "€0.8M",
                    }
                },
            }
        },
        "q2_outlook": {
            "by_category": {
                "Commit": {"ARR (€ converted)": 500000},
                "Best Case": {"ARR (€ converted)": 700000},
                "Pipeline": {"ARR (€ converted)": 800000},
                "Omitted": {"ARR (€ converted)": 100000},
            }
        },
        "renewals": {
            "open_renewals": [{"Renewal ACV (€ converted)": 250000}],
            "risk_levels": [{"Risk Level": "Medium", "Deal Count": 1, "ACV (€ converted)": 250000}],
        },
        "q1_review": {"actuals": {"won_count": 2, "lost_count": 1, "won_arr": 300000, "lost_arr": 100000, "slipped_count": 1, "slipped_arr": 50000}},
        "data_quality": {"total": {"Rep": "TOTAL", "Missing Amount": 1, "Missing Next Step": 2}},
        "commercial_approval": {
            "summary": [
                {"Category": "Approved", "Deal Count": 1, "ARR (€ converted)": 400000},
                {"Category": "Pending / Missing Approval", "Deal Count": 2, "ARR (€ converted)": 500000},
                {"Category": "No Approval Needed", "Deal Count": 1, "ARR (€ converted)": 100000},
            ],
            "missing_candidates": [{"Opportunity": "Big Deal", "ARR (€ converted)": 300000}],
        },
        "pipeline_detail": {
            "records": [
                {"Opportunity": "Overdue A", "ARR (€ converted)": 900000, "Close Date": "2026-03-31", "Owner": "Rep A"},
                {"Opportunity": "Current B", "ARR (€ converted)": 400000, "Close Date": "2026-06-30", "Owner": "Rep B"},
            ],
            "top_opportunities": [{"Opportunity": "Deal A", "ARR (€ converted)": 900000}],
        },
        "won_lost": {
            "records": [
                {"Opportunity": "Lost A", "ARR (€ converted)": 250000, "Stage": "0 - Lost", "Reason Won/Lost": ""},
                {"Opportunity": "No Opp B", "ARR (€ converted)": 100000, "Stage": "0 - No Opportunity", "Reason Won/Lost": ""},
            ],
            "lost": [],
        },
        "rep_performance": {
            "top_reps": [
                {"Rep": "Rep A", "Open Pipeline ARR (€ converted)": 1200000, "Deal Count": 4, "Stale Deals": 2, "Missing Approvals": 1}
            ]
        },
        "risk_register": {
            "top_arr": [
                {"Opportunity": "Risky A", "ARR (€ converted)": 800000, "Activity Days Ago": 45, "Push Count": 2}
            ]
        },
    }

    artifacts = build_validation_artifacts(snapshot, "")
    assert "Salesforce Hygiene and Activity Controls" in artifacts["validated_brief"]
    assert "Missing win/loss reason control shows 1 materially ranked rows missing reason hygiene." in artifacts["validated_brief"]
    payload = artifacts["structured_fill_payload"]
    executive_summary = next(slide for slide in payload["slides"] if slide["id"] == "executive-summary")
    assert executive_summary["management_question"].startswith("What is this director's operating position")
    assert executive_summary["visual_family"] == "four-card-kpi-strip"
    assert executive_summary["slots"]["headline_pipeline_arr_q2"] == "€2.0M"
    hygiene_slide = next(slide for slide in payload["slides"] if slide["id"] == "salesforce-hygiene-activity")
    assert hygiene_slide["slots"]["overdue_close_count"] == "1"
    assert hygiene_slide["slots"]["total_data_quality_issues"] == "3"
    missing_reason_slide = next(slide for slide in payload["slides"] if slide["id"] == "missing-win-loss-reason")
    assert missing_reason_slide["slots"]["missing_win_loss_reason_count"] == "1"
    assert missing_reason_slide["slots"]["missing_win_loss_reason_rows"][0]["opportunity"] == "Lost A"
    assert "Use the SD Deck Audit skill if it is available." in artifacts["powerpoint_prompt"]
    assert "Use the SD PowerPoint Builder skill if it is available." in artifacts["powerpoint_build_prompt"]
    assert "rewrite or update the deck in place" in artifacts["powerpoint_build_prompt"].lower()
    assert "Structured fill payload (JSON)" in artifacts["powerpoint_build_prompt"]
    assert "Management question:" in artifacts["powerpoint_build_prompt"]
    assert "Visual family:" in artifacts["powerpoint_build_prompt"]
    assert "Density limit:" in artifacts["powerpoint_build_prompt"]
    assert "use message titles in the populated deck" in artifacts["powerpoint_build_prompt"].lower()
    assert "`executive-summary`" in artifacts["powerpoint_build_prompt"]
    assert "`salesforce-hygiene-activity`" in artifacts["powerpoint_build_prompt"]
    assert "`missing-win-loss-reason`" in artifacts["powerpoint_build_prompt"]
    assert "`overdue-close-open-opps`" in artifacts["powerpoint_build_prompt"]
    assert "`churn-finance`" in artifacts["powerpoint_build_prompt"]


def test_permission_prompt_helpers_detect_edit_gate() -> None:
    dump = """
    button Stop
    UI element Permission required of group 24 of UI element 1 of scroll area 1
    button Allow once ⏎
    button Always allow ⌘⏎
    button Scroll to bottom
    """
    status = _status_from_dump(dump)
    assert status["running"] is True
    assert status["active_run"] is False
    assert status["permission_required"] is True
    assert status["accept_all_edits"] is False
    assert status["scroll_to_bottom"] is True
    assert _permission_group_from_dump(dump) == 24


def test_status_snapshot_extracts_step_progress() -> None:
    dump = """
    static text Responding
    static text Step 6 of 10
    button Stop
    group Claude of splitter group 1
    """
    status = _status_snapshot(office_etl.TARGETS["powerpoint"], dump)
    assert status["step_label"] == "Step 6 of 10"
    assert status["step_current"] == 6
    assert status["step_total"] == 10


def test_run_powerpoint_lane_uses_build_permissions(monkeypatch, tmp_path: Path) -> None:
    review_deck = tmp_path / "source.pptx"
    review_deck.write_bytes(b"pptx")
    captured: dict[str, object] = {}

    def fake_run_skill(target, **kwargs):  # type: ignore[no-untyped-def]
        captured["target"] = target.key
        captured.update(kwargs)
        return {"message_copied": False}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.run_skill",
        fake_run_skill,
    )

    result = run_powerpoint_lane(
        review_deck,
        "prompt",
        mode="build",
        skill_name=None,
        timeout=120,
        run_dir=tmp_path / "run",
    )

    assert captured["target"] == "powerpoint"
    assert captured["edit_permission_mode"] == "always-allow"
    assert captured["save_document_on_finish"] is True
    assert captured["wait_finish_seconds"] == 900
    assert "[build " in str(captured["source_file"])
    assert result["status"] == "ok"
    assert result["wait_finish_seconds"] == 900


def test_run_excel_brief_retries_after_excel_pane_boot_failure(monkeypatch, tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "workbook.xlsx",
        snapshot_path=tmp_path / "snapshot.json",
        existing_deck_path=None,
    )
    target.workbook_path.write_bytes(b"xlsx")
    calls: list[Path] = []
    quits: list[tuple[str, bool]] = []

    def fake_run_skill(run_target, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(kwargs["run_dir"])
        if len(calls) == 1:
            raise office_etl.AutomationError("Could not find or open the Claude pane in Microsoft Excel.")
        return {"message_path": str(kwargs["run_dir"] / "excel-message.txt")}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.run_skill",
        fake_run_skill,
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder._quit_office_app",
        lambda app_name, save: quits.append((app_name, save)),
    )

    result = run_excel_brief(
        target,
        prompt_template="Brief this workbook.",
        timeout=120,
        run_dir=tmp_path / "excel_brief",
    )

    assert result["status"] == "ok"
    assert result["attempt_count"] == 2
    assert quits == [("Microsoft Excel", False)]
    assert calls == [tmp_path / "excel_brief" / "attempt-1", tmp_path / "excel_brief" / "attempt-2"]
    assert result["attempts"] == [
        {
            "attempt": 1,
            "error_type": "AutomationError",
            "error_message": "Could not find or open the Claude pane in Microsoft Excel.",
        }
    ]


def test_execute_target_emits_progress_updates(monkeypatch, tmp_path: Path) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "workbook.xlsx",
        snapshot_path=tmp_path / "snapshot.json",
        existing_deck_path=tmp_path / "existing.pptx",
    )
    target.workbook_path.write_bytes(b"xlsx")
    target.snapshot_path.write_text("{}", encoding="utf-8")
    target.existing_deck_path.write_bytes(b"pptx")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.prepare_review_deck",
        lambda *args, **kwargs: (
            target.existing_deck_path,
            {"status": "ok", "source": "existing", "deck_path": str(target.existing_deck_path)},
        ),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.run_excel_brief",
        lambda *args, **kwargs: {"status": "ok", "message_path": str(tmp_path / "brief.txt")},
    )
    (tmp_path / "brief.txt").write_text("brief", encoding="utf-8")
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_validated_bridge",
        lambda *args, **kwargs: {
            "status": "ok",
            "powerpoint_fill_payload": str(tmp_path / "fill-payload.json"),
            "powerpoint_prompt": str(tmp_path / "ppt-prompt.txt"),
            "powerpoint_build_prompt": str(tmp_path / "ppt-build-prompt.txt"),
        },
    )
    (tmp_path / "fill-payload.json").write_text("{}", encoding="utf-8")
    (tmp_path / "ppt-prompt.txt").write_text("audit", encoding="utf-8")
    (tmp_path / "ppt-build-prompt.txt").write_text("build", encoding="utf-8")
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview",
        lambda *args, **kwargs: {"status": "ok", "deck_path": str(tmp_path / "deterministic.pptx")},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.render_deterministic_preview",
        lambda *args, **kwargs: {"status": "ok", "montage_path": str(tmp_path / "montage.png")},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview_audit",
        lambda *args, **kwargs: {"status": "ok", "ok": True, "finding_count": 0},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview_layout_audit",
        lambda *args, **kwargs: {"status": "ok", "ok": True, "report_path": str(tmp_path / "layout.json")},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.run_powerpoint_lane",
        lambda *args, **kwargs: {"status": "ok", "mode": kwargs["mode"]},
    )

    args = type(
        "Args",
        (),
        {
            "snapshot_date": "2026-04-10",
            "deck_source": "existing",
            "template_deck_path": tmp_path / "template.pptx",
            "shell_root": tmp_path / "shells",
            "canonical_shell_root": tmp_path / "canonical",
            "allow_generated_shell_fallback": False,
            "fallback_workbook_deck": False,
            "workbook_deck_root": tmp_path / "fallback",
            "render_workbook_deck": False,
            "skip_excel_brief": False,
            "excel_brief_prompt": "Brief this workbook.",
            "excel_timeout": 120,
            "skip_powerpoint_review": False,
            "powerpoint_mode": "build",
            "powerpoint_timeout": 120,
        },
    )()

    record = director_record_base(target)
    snapshots: list[tuple[str, dict[str, str]]] = []

    def progress_callback() -> None:
        snapshots.append(
            (
                record["status"],
                {
                    name: stage["status"]
                    for name, stage in record["stages"].items()
                    if isinstance(stage, dict) and "status" in stage
                },
            )
        )

    result = execute_target(
        target,
        args=args,
        run_root=tmp_path / "runs",
        record=record,
        progress_callback=progress_callback,
    )

    assert result["status"] == "ok"
    assert snapshots
    assert any(stage_statuses.get("excel_brief") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("validated_bridge") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("deterministic_preview") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("deterministic_preview_render") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("deterministic_preview_audit") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("deterministic_preview_layout_audit") == "running" for _, stage_statuses in snapshots)
    assert any(stage_statuses.get("powerpoint_review") == "running" for _, stage_statuses in snapshots)


def test_execute_target_skips_review_deck_when_powerpoint_review_is_disabled(
    monkeypatch,
    tmp_path: Path,
) -> None:
    target = DirectorTarget(
        director_name="Jane Doe",
        territory="APAC",
        workbook_path=tmp_path / "workbook.xlsx",
        snapshot_path=tmp_path / "snapshot.json",
        existing_deck_path=tmp_path / "existing.pptx",
    )
    target.workbook_path.write_bytes(b"xlsx")
    target.snapshot_path.write_text("{}", encoding="utf-8")
    target.existing_deck_path.write_bytes(b"pptx")

    called: dict[str, bool] = {}

    def fail_prepare_review_deck(*args, **kwargs):  # type: ignore[no-untyped-def]
        called["prepare_review_deck"] = True
        raise AssertionError("prepare_review_deck should not run")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.prepare_review_deck",
        fail_prepare_review_deck,
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_validated_bridge",
        lambda *args, **kwargs: {
            "status": "ok",
            "powerpoint_fill_payload": str(tmp_path / "fill-payload.json"),
            "powerpoint_prompt": str(tmp_path / "ppt-prompt.txt"),
            "powerpoint_build_prompt": str(tmp_path / "ppt-build-prompt.txt"),
        },
    )
    (tmp_path / "fill-payload.json").write_text("{}", encoding="utf-8")
    (tmp_path / "ppt-prompt.txt").write_text("audit", encoding="utf-8")
    (tmp_path / "ppt-build-prompt.txt").write_text("build", encoding="utf-8")
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview",
        lambda *args, **kwargs: {"status": "ok", "deck_path": str(tmp_path / "deterministic.pptx")},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.render_deterministic_preview",
        lambda *args, **kwargs: {"status": "ok", "montage_path": str(tmp_path / "montage.png")},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview_audit",
        lambda *args, **kwargs: {"status": "ok", "ok": True, "finding_count": 0},
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_master_builder.build_deterministic_preview_layout_audit",
        lambda *args, **kwargs: {"status": "ok", "ok": True, "report_path": str(tmp_path / "layout.json")},
    )

    args = type(
        "Args",
        (),
        {
            "snapshot_date": "2026-04-10",
            "deck_source": "canonical-shell",
            "template_deck_path": tmp_path / "template.pptx",
            "shell_root": tmp_path / "shells",
            "canonical_shell_root": tmp_path / "canonical",
            "allow_generated_shell_fallback": False,
            "fallback_workbook_deck": False,
            "workbook_deck_root": tmp_path / "fallback",
            "render_workbook_deck": False,
            "skip_excel_brief": True,
            "excel_brief_prompt": "Brief this workbook.",
            "excel_timeout": 120,
            "skip_powerpoint_review": True,
            "powerpoint_mode": "audit",
            "powerpoint_timeout": 120,
        },
    )()

    record = director_record_base(target)
    result = execute_target(
        target,
        args=args,
        run_root=tmp_path / "runs",
        record=record,
        progress_callback=None,
    )

    assert result["status"] == "ok"
    assert "prepare_review_deck" not in called
    assert result["stages"]["review_deck"]["status"] == "skipped"
    assert result["stages"]["powerpoint_review"]["status"] == "skipped"


def test_build_run_summary_payload_extracts_headline_metrics(tmp_path: Path) -> None:
    payload_path = tmp_path / "fill.json"
    payload_path.write_text(
        json.dumps(
            {
                "slides": [
                    {
                        "id": "executive-summary",
                        "slots": {
                            "headline_pipeline_arr_q2": "€2.0M",
                            "headline_renewal_acv": "€500K",
                        },
                    },
                    {
                        "id": "commercial-approval-overview",
                        "slots": {
                            "missing_approval_candidate_count": "4",
                        },
                    },
                    {
                        "id": "quarterly-pipeline",
                        "slots": {
                            "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                            "quarterly_pipeline_title": "Q3 2026",
                            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                        },
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "snapshot_date": "2026-04-10",
        "run_dir": str(tmp_path / "run"),
        "status": "ok",
        "targets": [
            {
                "director_name": "Jane Doe",
                "territory": "APAC",
                "status": "ok",
                "stages": {
                    "validated_bridge": {"powerpoint_fill_payload": str(payload_path)},
                    "deterministic_preview_render": {
                        "montage_path": str(tmp_path / "montage.png"),
                        "font_report": {
                            "font_missing_overall": [],
                            "font_substituted_overall": [],
                        },
                    },
                    "deterministic_preview_audit": {
                        "ok": True,
                        "finding_count": 0,
                        "report_path": str(tmp_path / "audit.json"),
                    },
                    "deterministic_preview_layout_audit": {
                        "ok": True,
                        "report_path": str(tmp_path / "layout.json"),
                    },
                    "deterministic_preview": {
                        "deck_path": str(tmp_path / "deck.pptx"),
                    },
                },
            }
        ],
    }

    summary = build_run_summary_payload(manifest)

    assert summary["target_count"] == 1
    row = summary["targets"][0]
    assert row["q2_active_arr"] == "€2.0M"
    assert row["open_renewal_acv"] == "€500K"
    assert row["approval_backlog"] == "4"
    assert row["audit_ok"] is True
    assert row["layout_ok"] is True
    assert row["font_missing_count"] == 0
    assert row["quarterly_pipeline_display_reason"] == "forward_quarter_fallback"
    assert row["quarterly_pipeline_title"] == "Q3 2026"


def test_build_latest_status_packet_and_aliases_surface_quarter_disclosure(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "run" / "jane" / "validated_bridge" / "fill.json"
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(
        json.dumps(
            {
                "slides": [
                    {
                        "id": "executive-summary",
                        "slots": {
                            "headline_pipeline_arr_q2": "€1.0M",
                            "headline_renewal_acv": "€250K",
                        },
                    },
                    {
                        "id": "quarterly-pipeline",
                        "slots": {
                            "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                            "quarterly_pipeline_title": "Q3 2026",
                            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                        },
                    },
                ]
            }
        ),
        encoding="utf-8",
    )
    manifest = {
        "snapshot_date": "2026-04-10",
        "run_dir": str(tmp_path / "run"),
        "status": "ok",
        "targets": [
            {
                "director_name": "Jane Doe",
                "territory": "APAC",
                "status": "ok",
                "stages": {
                    "validated_bridge": {"powerpoint_fill_payload": str(payload_path)},
                    "deterministic_preview_render": {
                        "montage_path": str(tmp_path / "run" / "jane" / "montage.png"),
                        "font_report": {
                            "font_missing_overall": [],
                            "font_substituted_overall": [],
                        },
                    },
                    "deterministic_preview_audit": {
                        "ok": True,
                        "finding_count": 0,
                        "report_path": str(tmp_path / "run" / "jane" / "audit.json"),
                    },
                    "deterministic_preview_layout_audit": {
                        "ok": True,
                        "report_path": str(tmp_path / "run" / "jane" / "layout.json"),
                    },
                    "deterministic_preview": {
                        "deck_path": str(tmp_path / "run" / "jane" / "deck.pptx"),
                    },
                },
            }
        ],
    }

    packet = build_latest_status_packet(manifest)
    markdown = build_latest_status_markdown(packet)
    write_latest_aliases(
        output_root=tmp_path / "output",
        snapshot_date="2026-04-10",
        packet=packet,
        markdown=markdown,
    )

    assert packet["target_status_counts"] == {"ok": 1}
    assert packet["quarterly_pipeline_disclosures"]["forward_quarter_fallbacks"] == [
        {
            "director_name": "Jane Doe",
            "territory": "APAC",
            "display_reason": "forward_quarter_fallback",
            "quarterly_pipeline_title": "Q3 2026",
            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        }
    ]
    assert "Forward-quarter fallback: Jane Doe (APAC) showing Q3 2026." in markdown
    assert "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook." in markdown

    snapshot_latest_json = tmp_path / "output" / "2026-04-10" / "latest.json"
    root_latest_json = tmp_path / "output" / "latest.json"
    snapshot_latest_md = tmp_path / "output" / "2026-04-10" / "latest.md"
    root_latest_md = tmp_path / "output" / "latest.md"

    assert json.loads(snapshot_latest_json.read_text(encoding="utf-8"))["run_dir"] == str(
        tmp_path / "run"
    )
    assert json.loads(root_latest_json.read_text(encoding="utf-8"))["status"] == "ok"
    assert "Q3 2026" in snapshot_latest_md.read_text(encoding="utf-8")
    assert "Q3 2026" in root_latest_md.read_text(encoding="utf-8")


def test_wait_for_run_finish_exits_after_scroll_when_copy_ready(monkeypatch) -> None:
    dumps = iter(
        [
            """
            static text Responding
            button Stop
            button Scroll to bottom
            group Claude of splitter group 1
            """,
            """
            button Copy message of group 5 of UI element 1 of scroll area 1
            button Scroll to bottom
            group Claude of splitter group 1
            """,
        ]
    )

    monkeypatch.setattr(office_etl, "dump_claude_pane", lambda target: next(dumps))
    monkeypatch.setattr(office_etl, "_scroll_to_bottom_if_present", lambda target: True)
    monkeypatch.setattr(office_etl, "_accept_all_edits_if_present", lambda target, dump: False)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.wait_for_run_finish(
        office_etl.TARGETS["excel"],
        timeout_seconds=1,
        edit_permission_mode="ask",
        trace=None,
    )


def test_wait_for_run_finish_does_not_toggle_accept_all_edits(monkeypatch) -> None:
    dumps = iter(
        [
            """
            static text Responding
            button Stop
            button Accept all edits
            button Scroll to bottom
            group Claude of splitter group 1
            """,
            """
            button Copy message of group 5 of UI element 1 of scroll area 1
            button Accept all edits
            button Scroll to bottom
            group Claude of splitter group 1
            """,
        ]
    )
    accept_calls: list[tuple[object, object]] = []

    monkeypatch.setattr(office_etl, "dump_claude_pane", lambda target: next(dumps))
    monkeypatch.setattr(office_etl, "_scroll_to_bottom_if_present", lambda target: True)

    def fake_accept(target, dump):  # type: ignore[no-untyped-def]
        accept_calls.append((target, dump))
        return True

    monkeypatch.setattr(office_etl, "_accept_all_edits_if_present", fake_accept)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.wait_for_run_finish(
        office_etl.TARGETS["powerpoint"],
        timeout_seconds=1,
        edit_permission_mode="always-allow",
        trace=None,
    )

    assert accept_calls == []


def test_wait_for_run_finish_does_not_scroll_while_run_is_active(monkeypatch) -> None:
    dumps = iter(
        [
            """
            static text Responding
            button Stop
            button Scroll to bottom
            group Claude of splitter group 1
            """,
            """
            button Copy message of group 5 of UI element 1 of scroll area 1
            group Claude of splitter group 1
            """,
        ]
    )
    scroll_calls: list[object] = []

    monkeypatch.setattr(office_etl, "dump_claude_pane", lambda target: next(dumps))

    def fake_scroll(target):  # type: ignore[no-untyped-def]
        scroll_calls.append(target)
        return True

    monkeypatch.setattr(office_etl, "_scroll_to_bottom_if_present", fake_scroll)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.wait_for_run_finish(
        office_etl.TARGETS["powerpoint"],
        timeout_seconds=1,
        edit_permission_mode="always-allow",
        trace=None,
    )

    assert scroll_calls == []


def test_wait_for_run_finish_recovers_window_loss_after_terminal_ready(monkeypatch) -> None:
    dumps = iter(
        [
            """
            static text Responding
            button Stop
            button Send message
            button Copy message of group 5 of UI element 1 of scroll area 1
            group Claude of splitter group 1
            """,
            """
            static text Responding
            button Stop
            button Send message
            button Copy message of group 5 of UI element 1 of scroll area 1
            group Claude of splitter group 1
            """,
            """
            static text Responding
            button Stop
            button Send message
            button Copy message of group 5 of UI element 1 of scroll area 1
            group Claude of splitter group 1
            """,
            office_etl.AutomationError("No window became available in Microsoft PowerPoint."),
        ]
    )

    def fake_dump(target):  # type: ignore[no-untyped-def]
        item = next(dumps)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(office_etl, "dump_claude_pane", fake_dump)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.wait_for_run_finish(
        office_etl.TARGETS["powerpoint"],
        timeout_seconds=1,
        edit_permission_mode="always-allow",
        trace=None,
    )


def test_wait_for_run_finish_raises_window_loss_before_terminal_ready(monkeypatch) -> None:
    dumps = iter(
        [
            """
            static text Responding
            button Stop
            group Claude of splitter group 1
            """,
            office_etl.AutomationError("No window became available in Microsoft PowerPoint."),
        ]
    )

    def fake_dump(target):  # type: ignore[no-untyped-def]
        item = next(dumps)
        if isinstance(item, Exception):
            raise item
        return item

    monkeypatch.setattr(office_etl, "dump_claude_pane", fake_dump)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    try:
        office_etl.wait_for_run_finish(
            office_etl.TARGETS["powerpoint"],
            timeout_seconds=1,
            edit_permission_mode="always-allow",
            trace=None,
        )
    except office_etl.AutomationError as exc:
        assert str(exc) == "No window became available in Microsoft PowerPoint."
    else:
        raise AssertionError("Expected late window loss without terminal ready state to raise.")


def test_wait_for_run_finish_rebinds_pane_after_called_process_error(monkeypatch) -> None:
    called = {"dump": 0, "rebind": 0}
    error = subprocess.CalledProcessError(
        1,
        ["osascript", "-e", 'return entire contents of group "Claude" of splitter group 1 of front window'],
        stderr='Can’t get group "Claude" of splitter group 1 of front window',
    )

    def fake_dump(target):  # type: ignore[no-untyped-def]
        called["dump"] += 1
        if called["dump"] == 1:
            return """
            static text Responding
            button Stop
            group Claude of splitter group 1
            """
        if called["dump"] == 2:
            raise error
        return """
        button Copy message of group 5 of UI element 1 of scroll area 1
        group Claude of splitter group 1
        """

    def fake_ensure(target, trace=None):  # type: ignore[no-untyped-def]
        called["rebind"] += 1

    monkeypatch.setattr(office_etl, "dump_claude_pane", fake_dump)
    monkeypatch.setattr(office_etl, "ensure_claude_pane", fake_ensure)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.wait_for_run_finish(
        office_etl.TARGETS["powerpoint"],
        timeout_seconds=1,
        edit_permission_mode="always-allow",
        trace=None,
    )

    assert called["rebind"] == 1


def test_send_message_uses_keyboard_fallback_when_button_is_missing(monkeypatch) -> None:
    dumps = iter(
        [
            """
            group Claude of splitter group 1
            button New chat
            text area 1
            """,
            """
            static text Responding
            button Stop
            group Claude of splitter group 1
            """,
        ]
    )
    keyboard_calls: list[bool] = []

    times = iter([0, 0.1, 0.2, 0.3, 0.4])

    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "dump_claude_pane", lambda target: next(dumps))
    monkeypatch.setattr(office_etl, "_click_if_exists", lambda app_name, expression: False)
    monkeypatch.setattr(
        office_etl,
        "time",
        type(
            "T",
            (),
            {
                "time": staticmethod(lambda: next(times)),
                "sleep": staticmethod(lambda seconds: None),
            },
        ),
    )

    def fake_keyboard_send(target, *, command_modifier=False):  # type: ignore[no-untyped-def]
        keyboard_calls.append(command_modifier)

    monkeypatch.setattr(office_etl, "_keyboard_send_message", fake_keyboard_send)

    office_etl.send_message(office_etl.TARGETS["excel"], timeout_seconds=1, trace=None)

    assert keyboard_calls == [False]


def test_paste_prompt_uses_quartz_focus_for_excel(monkeypatch) -> None:
    captured_scripts: list[str] = []

    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "_pbcopy", lambda text: None)
    monkeypatch.setattr(office_etl, "_click_compose_box_with_quartz", lambda target: True)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(
        office_etl,
        "_osascript",
        lambda script, timeout=30: captured_scripts.append(script) or "",
    )

    office_etl.paste_prompt(office_etl.TARGETS["excel"], "hello", trace=None)

    assert captured_scripts
    assert 'set focused of text area 1' not in captured_scripts[-1]
    assert 'keystroke "v" using {command down}' in captured_scripts[-1]


def test_ensure_claude_pane_prefers_excel_workbook_ribbon_before_addins(monkeypatch) -> None:
    monkeypatch.setattr(office_etl, "_activate", lambda app_name: None)
    monkeypatch.setattr(office_etl, "_wait_for_window", lambda target: None)
    monkeypatch.setattr(office_etl, "_dismiss_excel_recovery_if_needed", lambda: None)
    monkeypatch.setattr(office_etl, "_pane_window_name", lambda target: None)
    monkeypatch.setattr(
        office_etl,
        "_preferred_excel_window_name",
        lambda: "Sales Director Data - Sarah Pittroff (Central Europe)",
    )
    monkeypatch.setattr(office_etl, "_open_excel_claude_on_window", lambda window_name: True)
    monkeypatch.setattr(
        office_etl,
        "_launch_excel_claude_via_addins_dialog",
        lambda: (_ for _ in ()).throw(AssertionError("add-ins dialog should not be used first")),
    )

    office_etl.ensure_claude_pane(office_etl.TARGETS["excel"], trace=None)


def test_pane_window_name_prefers_window_hint(monkeypatch) -> None:
    monkeypatch.setattr(office_etl, "_window_names", lambda target: ["Old Deck", "New Deck"])
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], "New Deck")

    def fake_osascript(script: str, *, timeout: int = 30) -> str:
        if 'window "New Deck"' in script:
            return "yes"
        if 'window "Old Deck"' in script:
            return "yes"
        return "no"

    monkeypatch.setattr(office_etl, "_osascript", fake_osascript)

    assert office_etl._pane_window_name(office_etl.TARGETS["powerpoint"]) == "New Deck"
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], None)


def test_dump_claude_pane_prefers_bound_window_over_front_window(monkeypatch) -> None:
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], "Bound Deck")
    monkeypatch.setattr(office_etl, "_preferred_window_name", lambda target: "Bound Deck")
    monkeypatch.setattr(office_etl, "_pane_window_name", lambda target: "Old Deck")

    def fake_osascript(script: str, *, timeout: int = 30) -> str:
        if 'window "Bound Deck"' in script:
            return "bound contents"
        raise AssertionError("front window fallback should not be used")

    monkeypatch.setattr(office_etl, "_osascript", fake_osascript)

    assert office_etl.dump_claude_pane(office_etl.TARGETS["powerpoint"], ensure_pane=False) == "bound contents"
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], None)


def test_ensure_claude_pane_uses_preferred_powerpoint_window(monkeypatch) -> None:
    monkeypatch.setattr(office_etl, "_activate", lambda app_name: None)
    monkeypatch.setattr(office_etl, "_wait_for_window", lambda target: None)
    monkeypatch.setattr(office_etl, "_window_names", lambda target: ["New Deck", "Old Deck"])
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], "New Deck")
    activations: list[str] = []
    ribbon_windows: list[str] = []
    pane_windows = iter([None, "New Deck", "New Deck"])

    monkeypatch.setattr(office_etl, "_activate_window", lambda target, window_name: activations.append(window_name))
    monkeypatch.setattr(office_etl, "_pane_window_name", lambda target: next(pane_windows))
    monkeypatch.setattr(
        office_etl,
        "_window_ribbon_button_expression",
        lambda target, button_name, window_name: ribbon_windows.append(window_name) or "expr",
    )
    monkeypatch.setattr(office_etl, "_ribbon_button_expression", lambda target, button_name: None)
    monkeypatch.setattr(office_etl, "_click_if_exists", lambda app_name, expression: True)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.ensure_claude_pane(office_etl.TARGETS["powerpoint"], trace=None)

    assert ribbon_windows == ["New Deck"]
    assert "New Deck" in activations
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], None)


def test_close_claude_pane_on_window_uses_direct_close_button(monkeypatch) -> None:
    states = iter([True, False])
    monkeypatch.setattr(office_etl, "_activate_window", lambda target, window_name: None)
    monkeypatch.setattr(office_etl, "_click_if_exists", lambda app_name, expression: "Close Claude" in expression)
    monkeypatch.setattr(office_etl, "_window_ribbon_button_expression", lambda target, button_name, window_name: None)
    monkeypatch.setattr(office_etl, "_pane_present_on_window", lambda target, window_name: next(states))
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    assert office_etl._close_claude_pane_on_window(office_etl.TARGETS["powerpoint"], "Old Deck") is True


def test_ensure_claude_pane_closes_mismatched_powerpoint_pane_before_rebinding(monkeypatch) -> None:
    monkeypatch.setattr(office_etl, "_activate", lambda app_name: None)
    monkeypatch.setattr(office_etl, "_wait_for_window", lambda target: None)
    monkeypatch.setattr(office_etl, "_window_names", lambda target: ["New Deck", "Old Deck"])
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], "New Deck")
    activations: list[str] = []
    ribbon_windows: list[str] = []
    closed_windows: list[str] = []
    pane_windows = iter(["Old Deck", None, "New Deck", "New Deck"])

    monkeypatch.setattr(office_etl, "_activate_window", lambda target, window_name: activations.append(window_name))
    monkeypatch.setattr(office_etl, "_pane_window_name", lambda target: next(pane_windows))
    monkeypatch.setattr(
        office_etl,
        "_close_claude_pane_on_window",
        lambda target, window_name: closed_windows.append(window_name) or True,
    )
    monkeypatch.setattr(
        office_etl,
        "_window_ribbon_button_expression",
        lambda target, button_name, window_name: ribbon_windows.append(window_name) or "expr",
    )
    monkeypatch.setattr(office_etl, "_ribbon_button_expression", lambda target, button_name: None)
    monkeypatch.setattr(office_etl, "_click_if_exists", lambda app_name, expression: True)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.ensure_claude_pane(office_etl.TARGETS["powerpoint"], trace=None)

    assert closed_windows == ["Old Deck"]
    assert ribbon_windows == ["New Deck"]
    assert "New Deck" in activations
    office_etl._set_window_hint(office_etl.TARGETS["powerpoint"], None)


def test_close_conflicting_powerpoint_presentations_filters_same_family(monkeypatch) -> None:
    monkeypatch.setattr(
        office_etl,
        "_powerpoint_presentation_names",
        lambda: [
            "Sales Region Monthly Shell - EMEA.pptx",
            "Sales Region Monthly Shell - EMEA [build 20260411-132517].pptx",
            "Sales Region Monthly Shell - EMEA [build 20260411-132517]  -  Repaired",
            "Sales Region Monthly Shell - APAC.pptx",
        ],
    )
    closed: list[str] = []
    monkeypatch.setattr(
        office_etl,
        "_close_powerpoint_presentation",
        lambda name, save=False: closed.append(name) or True,
    )

    result = office_etl._close_conflicting_powerpoint_presentations(
        Path("Sales Region Monthly Shell - EMEA [build 20260411-132736].pptx")
    )

    assert result == [
        "Sales Region Monthly Shell - EMEA.pptx",
        "Sales Region Monthly Shell - EMEA [build 20260411-132517].pptx",
        "Sales Region Monthly Shell - EMEA [build 20260411-132517]  -  Repaired",
    ]
    assert closed == result


def test_new_chat_accepts_existing_compose_box(monkeypatch) -> None:
    dump = """
    group Claude of splitter group 1
    text area 1 of UI element 1 of scroll area 1
    static text Reply
    """

    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target: None)
    monkeypatch.setattr(office_etl, "_dismiss_pane_banner_if_needed", lambda target: None)
    monkeypatch.setattr(office_etl, "dump_claude_pane", lambda target: dump)
    monkeypatch.setattr(office_etl, "_click_if_exists", lambda app_name, expression: False)
    monkeypatch.setattr(office_etl.time, "sleep", lambda seconds: None)

    office_etl.new_chat(office_etl.TARGETS["powerpoint"])


def test_click_if_exists_returns_false_on_stale_ax_path(monkeypatch) -> None:
    def fake_osascript(script: str, *, timeout: int = 30) -> str:
        raise subprocess.CalledProcessError(
            1,
            ["osascript", "-e", script],
            stderr="System Events got an error: Invalid index.",
        )

    monkeypatch.setattr(office_etl, "_osascript", fake_osascript)

    assert office_etl._click_if_exists("Microsoft PowerPoint", 'button "Accept all edits" of group 23') is False


def test_run_skill_recovers_late_powerpoint_window_loss_after_finish(monkeypatch, tmp_path: Path) -> None:
    deck_path = tmp_path / "editable.pptx"
    deck_path.write_bytes(b"before")

    monkeypatch.setattr(office_etl, "_close_conflicting_powerpoint_presentations", lambda source_file: [])
    monkeypatch.setattr(office_etl, "_open_file", lambda target, file_path: None)
    monkeypatch.setattr(office_etl, "_wait_for_named_window", lambda target, window_name, timeout_seconds=20: True)
    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "new_chat", lambda target: None)
    monkeypatch.setattr(office_etl, "paste_prompt", lambda target, prompt, trace=None: None)
    monkeypatch.setattr(office_etl, "send_message", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "wait_for_run_start_with_trace", lambda target, timeout_seconds, trace=None: None)
    monkeypatch.setattr(
        office_etl,
        "wait_for_run_finish",
        lambda target, timeout_seconds, edit_permission_mode="ask", trace=None: None,
    )

    def fake_save_open_document(target):  # type: ignore[no-untyped-def]
        deck_path.write_bytes(b"after")

    monkeypatch.setattr(office_etl, "save_open_document", fake_save_open_document)
    monkeypatch.setattr(
        office_etl,
        "copy_latest_message_with_trace",
        lambda target, trace=None: (_ for _ in ()).throw(
            office_etl.AutomationError("No window became available in Microsoft PowerPoint.")
        ),
    )
    monkeypatch.setattr(
        office_etl,
        "save_transcript",
        lambda target, output_path: (_ for _ in ()).throw(
            office_etl.AutomationError("No window became available in Microsoft PowerPoint.")
        ),
    )

    result = office_etl.run_skill(
        office_etl.TARGETS["powerpoint"],
        source_file=deck_path,
        skill_name=None,
        prompt="Update deck.",
        wait_finish_seconds=60,
        run_dir=tmp_path / "run",
        edit_permission_mode="always-allow",
        save_document_on_finish=True,
    )

    assert result["message_copied"] is False
    assert result["window_loss_recovered"] is True
    assert result["transcript_path"]
    transcript_path = Path(result["transcript_path"])
    assert transcript_path.exists()
    assert "[transcript unavailable]" in transcript_path.read_text(encoding="utf-8")


def test_run_skill_restarts_powerpoint_when_named_window_never_appears(monkeypatch, tmp_path: Path) -> None:
    deck_path = tmp_path / "editable.pptx"
    deck_path.write_bytes(b"pptx")
    open_calls: list[Path] = []
    wait_results = iter([False, True])
    restarted: list[bool] = []

    monkeypatch.setattr(office_etl, "_close_conflicting_powerpoint_presentations", lambda source_file: [])
    monkeypatch.setattr(office_etl, "_open_file", lambda target, file_path: open_calls.append(file_path))
    monkeypatch.setattr(
        office_etl,
        "_wait_for_named_window",
        lambda target, window_name, timeout_seconds=20: next(wait_results),
    )
    monkeypatch.setattr(office_etl, "_restart_powerpoint_process", lambda: restarted.append(True))
    monkeypatch.setattr(office_etl, "_window_names", lambda target: [deck_path.stem])
    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "new_chat", lambda target: None)
    monkeypatch.setattr(office_etl, "paste_prompt", lambda target, prompt, trace=None: None)
    monkeypatch.setattr(office_etl, "send_message", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "wait_for_run_start_with_trace", lambda target, timeout_seconds, trace=None: None)
    monkeypatch.setattr(
        office_etl,
        "wait_for_run_finish",
        lambda target, timeout_seconds, edit_permission_mode="ask", trace=None: None,
    )
    monkeypatch.setattr(office_etl, "copy_latest_message_with_trace", lambda target, trace=None: "")
    monkeypatch.setattr(office_etl, "save_transcript", lambda target, output_path: output_path.write_text("trace\n", encoding="utf-8"))

    result = office_etl.run_skill(
        office_etl.TARGETS["powerpoint"],
        source_file=deck_path,
        skill_name=None,
        prompt="Update deck.",
        wait_finish_seconds=60,
        run_dir=tmp_path / "run",
        edit_permission_mode="always-allow",
        save_document_on_finish=False,
    )

    assert open_calls == [deck_path, deck_path]
    assert restarted == [True]
    assert result["app"] == "powerpoint"
    assert result["message_copied"] is False


def test_run_skill_avoids_reensuring_pane_after_initial_bind(monkeypatch, tmp_path: Path) -> None:
    deck_path = tmp_path / "editable.pptx"
    deck_path.write_bytes(b"pptx")
    ensure_calls: list[str] = []

    monkeypatch.setattr(office_etl, "_close_conflicting_powerpoint_presentations", lambda source_file: [])
    monkeypatch.setattr(office_etl, "_open_file", lambda target, file_path: None)
    monkeypatch.setattr(office_etl, "_wait_for_named_window", lambda target, window_name, timeout_seconds=20: True)
    monkeypatch.setattr(
        office_etl,
        "ensure_claude_pane",
        lambda target, trace=None: ensure_calls.append(target.key),
    )
    monkeypatch.setattr(office_etl, "new_chat", lambda target, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("new_chat should not re-ensure")))
    monkeypatch.setattr(office_etl, "paste_prompt", lambda target, prompt, trace=None, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("paste_prompt should not re-ensure")))
    monkeypatch.setattr(office_etl, "send_message", lambda target, timeout_seconds=10, trace=None, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("send_message should not re-ensure")))
    monkeypatch.setattr(office_etl, "wait_for_run_start_with_trace", lambda target, timeout_seconds=30, trace=None, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("wait_start should not re-ensure")))
    monkeypatch.setattr(
        office_etl,
        "wait_for_run_finish",
        lambda target, timeout_seconds=300, edit_permission_mode="ask", trace=None, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("wait_finish should not re-ensure")),
    )
    monkeypatch.setattr(office_etl, "copy_latest_message_with_trace", lambda target, trace=None, ensure_pane=True: None if not ensure_pane else (_ for _ in ()).throw(AssertionError("copy should not re-ensure")))
    monkeypatch.setattr(office_etl, "save_transcript", lambda target, output_path, ensure_pane=True: output_path.write_text("trace\n", encoding="utf-8"))

    result = office_etl.run_skill(
        office_etl.TARGETS["powerpoint"],
        source_file=deck_path,
        skill_name=None,
        prompt="Update deck.",
        wait_finish_seconds=60,
        run_dir=tmp_path / "run",
        edit_permission_mode="always-allow",
        save_document_on_finish=False,
    )

    assert ensure_calls == ["powerpoint"]
    assert result["message_copied"] is False


def test_run_skill_preserves_primary_error_when_transcript_save_fails(monkeypatch, tmp_path: Path) -> None:
    workbook_path = tmp_path / "workbook.xlsx"
    workbook_path.write_bytes(b"xlsx")

    monkeypatch.setattr(office_etl, "_open_file", lambda target, file_path: None)
    monkeypatch.setattr(office_etl, "_wait_for_named_window", lambda target, window_name, timeout_seconds=20: True)
    monkeypatch.setattr(office_etl, "ensure_claude_pane", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "new_chat", lambda target: None)
    monkeypatch.setattr(office_etl, "paste_prompt", lambda target, prompt, trace=None: None)
    monkeypatch.setattr(office_etl, "send_message", lambda target, trace=None: None)
    monkeypatch.setattr(office_etl, "wait_for_run_start_with_trace", lambda target, timeout_seconds, trace=None: None)

    def fake_wait_for_run_finish(target, timeout_seconds, edit_permission_mode="ask", trace=None):  # type: ignore[no-untyped-def]
        raise office_etl.AutomationError("Claude did not return to an idle state in Microsoft Excel.")

    monkeypatch.setattr(office_etl, "wait_for_run_finish", fake_wait_for_run_finish)
    monkeypatch.setattr(
        office_etl,
        "save_transcript",
        lambda target, output_path: (_ for _ in ()).throw(
            office_etl.AutomationError("No window became available in Microsoft Excel.")
        ),
    )

    try:
        office_etl.run_skill(
            office_etl.TARGETS["excel"],
            source_file=workbook_path,
            skill_name=None,
            prompt="Brief workbook.",
            wait_finish_seconds=60,
            run_dir=tmp_path / "run",
        )
    except office_etl.AutomationError as exc:
        assert str(exc) == "Claude did not return to an idle state in Microsoft Excel."
    else:
        raise AssertionError("Expected the primary run failure to be raised.")


def test_pane_window_name_checks_non_front_windows(monkeypatch) -> None:
    monkeypatch.setattr(
        office_etl,
        "_window_names",
        lambda target: ["Office Add-ins", "Sales Director Data - Sarah Pittroff (Central Europe)"],
    )

    def fake_osascript(script: str, *, timeout: int = 30) -> str:
        if 'window "Sales Director Data - Sarah Pittroff (Central Europe)"' in script:
            return "yes"
        return "no"

    monkeypatch.setattr(office_etl, "_osascript", fake_osascript)

    assert (
        office_etl._pane_window_name(office_etl.TARGETS["excel"])
        == "Sales Director Data - Sarah Pittroff (Central Europe)"
    )


def test_preferred_excel_window_name_skips_blank_and_modal_windows(monkeypatch) -> None:
    monkeypatch.setattr(
        office_etl,
        "_window_names",
        lambda target: [
            "Office Add-ins",
            "Book1",
            "Open new and recent files",
            "Sales Director Data - Sarah Pittroff (Central Europe)",
        ],
    )

    assert office_etl._preferred_excel_window_name() == "Sales Director Data - Sarah Pittroff (Central Europe)"
