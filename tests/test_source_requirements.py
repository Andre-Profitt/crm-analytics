import json
from pathlib import Path

from scripts.monthly_platform.period import resolve_period_context
from scripts.monthly_platform.source_requirements import (
    SourceRequirementsRegistry,
    build_source_requirement_plan,
    load_source_requirements,
    requirement_summary,
)


def _territories() -> dict:
    return {
        "APAC": {
            "director": "Jesper Tyrer",
            "pipeline_open_list_view_id": "00B-apac-pipeline-open",
            "pipeline_open_list_view_label": "Pipeline Open APAC",
            "pi_list_view_id": "00B-apac-q2",
            "pi_list_view_label": "PI APAC Q2",
            "forward_quarter_pi_list_views": {
                "Q3": {
                    "list_view_id": "00B-apac-q3",
                    "list_view_label": "PI APAC Q3",
                }
            },
            "historical_trending_report_ids": {
                "Q1": "00O-apac-q1",
                "Q2": "00O-apac-q2",
            },
            "forward_quarter_historical_trending_report_ids": {
                "Q3": "00O-apac-q3"
            },
        },
        "Central Europe": {
            "director": "Sarah Pittroff",
            "pipeline_open_list_view_id": "00B-ce-pipeline-open",
            "pipeline_open_list_view_label": "Pipeline Open CE",
            "pi_list_view_id": "00B-ce-q2",
            "pi_list_view_label": "PI CE Q2",
            "forward_quarter_pi_list_views": {
                "Q3": {
                    "list_view_id": "00B-ce-q3",
                    "list_view_label": "PI CE Q3",
                }
            },
            "historical_trending_report_ids": {
                "Q1": "00O-ce-q1",
                "Q2": "00O-ce-q2",
            },
            "forward_quarter_historical_trending_report_ids": {"Q3": "00O-ce-q3"},
        },
    }


def test_default_source_requirements_resolve_current_registry() -> None:
    registry = load_source_requirements(Path("config/monthly_source_requirements.json"))
    period = resolve_period_context(snapshot_date="2026-04-30")

    plan = build_source_requirement_plan(
        registry=registry,
        territories=_territories(),
        period=period,
    )

    summary = requirement_summary(plan)
    assert plan.status == "ok"
    assert summary["configured_count"] == 13
    assert summary["missing_source_id_count"] == 0
    assert summary["by_requirement"] == {
        "sd_pipeline_open": 2,
        "sd_pipeline_open_reference": 1,
        "sd_historical_trending": 6,
        "sd_pipeline_inspection": 4,
    }
    assert any(
        item.requirement_id == "sd_pipeline_open"
        and item.territory == "Central Europe"
        and item.source_id == "00B-ce-pipeline-open"
        for item in plan.items
    )
    apac_forward_pi = next(
        item
        for item in plan.items
        if item.requirement_id == "sd_pipeline_inspection"
        and item.territory == "APAC"
        and item.period_role == "forward_quarter"
    )
    assert apac_forward_pi.source_id == "00B-apac-q3"
    assert apac_forward_pi.source_label == "PI APAC Q3"
    assert apac_forward_pi.fallback_policy is not None


def test_missing_source_id_blocks_plan() -> None:
    registry = load_source_requirements(Path("config/monthly_source_requirements.json"))
    period = resolve_period_context(snapshot_date="2026-04-30")
    territories = _territories()
    del territories["APAC"]["forward_quarter_historical_trending_report_ids"]

    plan = build_source_requirement_plan(
        registry=registry,
        territories=territories,
        period=period,
    )

    assert plan.status == "blocked"
    assert any(
        finding.issue == "source_requirement_missing_source_id"
        and "sd_historical_trending APAC forward_quarter Q3" in finding.evidence
        for finding in plan.findings
    )


def test_disabled_requirement_is_omitted(tmp_path: Path) -> None:
    payload = json.loads(Path("config/monthly_source_requirements.json").read_text())
    for requirement in payload["requirements"]:
        if requirement["requirement_id"] == "sd_pipeline_inspection":
            requirement["enabled"] = False
    registry = SourceRequirementsRegistry.model_validate(payload)

    plan = build_source_requirement_plan(
        registry=registry,
        territories=_territories(),
        period=resolve_period_context(snapshot_date="2026-04-30"),
    )

    assert {item.requirement_id for item in plan.items} == {
        "sd_historical_trending",
        "sd_pipeline_open",
        "sd_pipeline_open_reference",
    }
    assert requirement_summary(plan)["configured_count"] == 9
