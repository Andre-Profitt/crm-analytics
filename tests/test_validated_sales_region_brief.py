from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_validated_sales_region_brief import build_validation_artifacts


def test_build_validation_artifacts_include_regional_shell_ids() -> None:
    snapshot = {
        "region_name": "EMEA",
        "snapshot_date": "2026-04-10",
        "component_books": [
            {"director_name": "Sarah Pittroff", "territory": "Central Europe"},
            {"director_name": "Mourad Essofi", "territory": "Middle East & Africa"},
        ],
        "forecast_hierarchy_note": "Middle East & Africa is included under EMEA in the forecast hierarchy.",
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — All Open (any close date)": "€20.0M",
                        "Pipeline ARR — FY26 Close Dates Only (excl. Omitted)": "€16.0M",
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": "€4.0M",
                        "Deal Count": 20,
                        "Weighted Pipeline (probability-adj)": "€2.0M",
                        "New Pipeline This Quarter (excl. Omitted)": "€1.0M",
                    }
                },
                "process-compliance": {"metrics": {"Approval Rate (stage 3+)": "40.0%", "Missing Approval (Land, stage 3+)": 3}},
                "risk": {"metrics": {"Stale 30d+ (ARR)": "€2.0M", "Aging 365+ (ARR)": "€3.0M"}},
            }
        },
        "pipeline_detail": {"top_opportunities": [{"Opportunity": "Deal A", "ARR (€ converted)": 1000000}]},
        "q2_outlook": {
            "by_category": {
                "Commit": {"ARR (€ converted)": 1000000},
                "Best Case": {"ARR (€ converted)": 2000000},
                "Pipeline": {"ARR (€ converted)": 500000},
                "Omitted": {"ARR (€ converted)": 100000},
            }
        },
        "commercial_approval": {"missing_candidates": [{"Opportunity": "Big Missing", "ARR (€ converted)": 900000}]},
        "renewals": {
            "open_renewals": [{"Opportunity": "Renewal A", "Renewal ACV (€ converted)": 300000}],
            "risk_levels": [{"Risk Level": "Medium", "Deal Count": 1, "ACV (€ converted)": 300000}],
            "summary_metrics": {"open_acv": 300000, "open_deal_count": 2},
        },
        "q1_review": {"actuals": {"won_count": 2, "won_arr": 400000, "lost_count": 1, "lost_arr": 200000, "slipped_count": 3, "slipped_arr": 500000}},
        "data_quality": {"total": {"Total Issues": 10}},
        "won_lost": {"lost": []},
    }
    artifacts = build_validation_artifacts(snapshot)
    assert "Validated Regional Fact Pack: EMEA" in artifacts["validated_brief"]
    assert "Open renewals in this region: 2 deals totaling €300K ACV." in artifacts["validated_brief"]
    payload = artifacts["structured_fill_payload"]
    executive_summary = next(slide for slide in payload["slides"] if slide["id"] == "executive-summary")
    assert executive_summary["support_level"] == "strong"
    assert executive_summary["slots"]["headline_pipeline_arr_q2"] == "€4.0M"
    churn_slide = next(slide for slide in payload["slides"] if slide["id"] == "churn-finance")
    assert churn_slide["support_level"] == "placeholder"
    assert "Finance churn input is not integrated" in churn_slide["slots"]["finance_churn_inputs_status"]
    assert "`regional-book-breakdown`" in artifacts["powerpoint_build_prompt"]
    assert "`churn-finance`" in artifacts["powerpoint_build_prompt"]
    assert "Support level: placeholder" in artifacts["powerpoint_build_prompt"]
    assert "Structured fill payload (JSON)" in artifacts["powerpoint_build_prompt"]
    assert "do not move ME&A out of EMEA" in artifacts["powerpoint_build_prompt"]
    assert "where a slide is marked placeholder, keep it as a controlled placeholder" in artifacts["powerpoint_build_prompt"]
    assert "rewrite visible slide titles into message titles" in artifacts["powerpoint_build_prompt"]


def test_build_validation_artifacts_fall_forward_for_empty_current_quarter() -> None:
    snapshot = {
        "region_name": "APAC",
        "snapshot_date": "2026-04-30",
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — All Open (any close date)": "€6.0M",
                        "Pipeline ARR — FY26 Close Dates Only (excl. Omitted)": "€3.0M",
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": "€0",
                        "Deal Count": 6,
                        "Weighted Pipeline (probability-adj)": "€0.8M",
                        "New Pipeline This Quarter (excl. Omitted)": "€0.2M",
                    }
                },
                "process-compliance": {"metrics": {"Approval Rate (stage 3+)": "40.0%", "Missing Approval (Land, stage 3+)": 1}},
                "risk": {"metrics": {"Stale 30d+ (ARR)": "€1.0M", "Aging 365+ (ARR)": "€0.5M"}},
            }
        },
        "pipeline_detail": {
            "records": [
                {
                    "Opportunity": "APAC Q3 Deal",
                    "ARR (€ converted)": 900000,
                    "Owner": "Rep A",
                    "Close Date": "2026-07-20",
                    "Forecast Category": "Commit",
                }
            ],
            "top_opportunities": [
                {"Opportunity": "APAC Q3 Deal", "ARR (€ converted)": 900000}
            ],
        },
        "q2_outlook": {
            "by_category": {
                "Omitted": {"ARR (€ converted)": 200000},
            },
            "breakdown": [
                {"Forecast Category": "Omitted", "Deal Count": 1, "ARR (€ converted)": 200000}
            ],
        },
        "commercial_approval": {"missing_candidates": [{"Opportunity": "Big Missing", "ARR (€ converted)": 300000}]},
        "renewals": {
            "open_renewals": [{"Opportunity": "Renewal A", "Renewal ACV (€ converted)": 300000}],
            "risk_levels": [{"Risk Level": "Medium", "Deal Count": 1, "ACV (€ converted)": 300000}],
            "summary_metrics": {"open_acv": 300000, "open_deal_count": 1},
        },
        "q1_review": {"actuals": {"won_count": 1, "won_arr": 100000, "lost_count": 1, "lost_arr": 50000, "slipped_count": 1, "slipped_arr": 150000}},
        "data_quality": {"total": {"Total Issues": 5}},
        "won_lost": {"lost": []},
    }

    artifacts = build_validation_artifacts(snapshot)
    quarterly = next(
        slide
        for slide in artifacts["structured_fill_payload"]["slides"]
        if slide["id"] == "quarterly-pipeline"
    )

    assert quarterly["slots"]["headline_pipeline_arr_q2"] == "€900K"
    assert quarterly["slots"]["q2_commit_arr"] == "€900K"
    assert quarterly["slots"]["quarterly_pipeline_label"] == "Q3"
    assert (
        quarterly["slots"]["quarterly_pipeline_footnote"]
        == "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook."
    )
    assert "Q3 2026 active pipeline is €900K ARR." in artifacts["validated_brief"]
    assert "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook." in artifacts["validated_brief"]
