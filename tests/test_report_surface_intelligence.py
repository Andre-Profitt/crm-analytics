from __future__ import annotations

from scripts import report_surface_intelligence


def test_assess_report_action_surface_contract_keeps_tabular_queue_first() -> None:
    assessment = report_surface_intelligence.assess_report_action_surface_contract(
        {
            "surface_type": "salesforce_report",
            "report_format": "tabular",
            "columns": [
                "Owner",
                "Account",
                "Opportunity",
                "Close Date",
                "Amount",
                "Forecast Category",
            ],
            "filters": ["renewal_period", "owner", "risk_band"],
            "group_by": ["Manager", "Owner"],
            "sort_by": ["Amount", "Owner"],
            "handoff_surface": "crma_dashboard",
            "handoff_target": {"destination_type": "dashboard"},
        }
    )

    assert assessment["verdict"] == "strong_follow_up_fit"
    assert assessment["queue_ready_format"] is True
    assert assessment["verdict_cap"] is None
    assert assessment["primary_surface_fit"] == "strong_primary_fit"


def test_assess_report_action_surface_contract_caps_summary_follow_up_fit() -> None:
    assessment = report_surface_intelligence.assess_report_action_surface_contract(
        {
            "surface_type": "salesforce_report",
            "report_format": "summary",
            "columns": [
                "Owner",
                "Account",
                "Opportunity",
                "Close Date",
                "Amount",
                "Forecast Category",
            ],
            "filters": ["renewal_period", "owner", "risk_band"],
            "group_by": ["Manager", "Owner"],
            "sort_by": ["Amount", "Owner"],
            "handoff_surface": "crma_dashboard",
            "handoff_target": {"destination_type": "dashboard"},
        }
    )

    assert assessment["raw_verdict"] == "strong_follow_up_fit"
    assert assessment["verdict"] == "moderate_follow_up_fit"
    assert assessment["verdict_cap"] == "summary_caps_follow_up_fit"
    assert assessment["queue_ready_format"] is False
    assert assessment["primary_surface_fit"] == "limited_primary_fit"


def test_assess_report_action_surface_contract_caps_matrix_follow_up_fit() -> None:
    assessment = report_surface_intelligence.assess_report_action_surface_contract(
        {
            "surface_type": "salesforce_report",
            "report_format": "matrix",
            "columns": [
                "Owner",
                "Account",
                "Opportunity",
                "Close Date",
                "Amount",
                "Forecast Category",
            ],
            "filters": ["renewal_period", "owner", "risk_band"],
            "group_by": ["Manager", "Owner"],
            "sort_by": ["Amount", "Owner"],
            "handoff_surface": "crma_dashboard",
            "handoff_target": {"destination_type": "dashboard"},
        }
    )

    assert assessment["raw_verdict"] == "strong_follow_up_fit"
    assert assessment["verdict"] == "weak_follow_up_fit"
    assert assessment["verdict_cap"] == "matrix_caps_follow_up_fit"
    assert assessment["queue_ready_format"] is False
    assert assessment["primary_surface_fit"] == "weak_primary_fit"
