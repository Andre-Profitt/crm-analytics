from __future__ import annotations

from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_validated_sales_global_summary_brief import build_validation_artifacts


def test_build_validation_artifacts_include_global_shell_ids() -> None:
    snapshot = {
        "snapshot_date": "2026-04-10",
        "regions": [
            {
                "region_name": "APAC",
                "headline_pipeline_arr_q2": "€4.0M",
                "q2_commit_arr": "€1.0M",
                "q2_best_case_arr": "€2.0M",
                "q2_omitted_arr": "€100K",
                "approval_rate_stage3_plus": "20.0%",
                "renewal_open_acv": "€300K",
                "top_risk": "APAC risk.",
                "top_action": "APAC action.",
            },
            {
                "region_name": "EMEA",
                "headline_pipeline_arr_q2": "€10.0M",
                "q2_commit_arr": "€2.0M",
                "q2_best_case_arr": "€4.0M",
                "q2_omitted_arr": "€200K",
                "approval_rate_stage3_plus": "30.0%",
                "renewal_open_acv": "€500K",
                "top_risk": "EMEA risk.",
                "top_action": "EMEA action.",
            },
            {
                "region_name": "North America",
                "headline_pipeline_arr_q2": "€3.0M",
                "q2_commit_arr": "€1.0M",
                "q2_best_case_arr": "€1.5M",
                "q2_omitted_arr": "€50K",
                "approval_rate_stage3_plus": "25.0%",
                "renewal_open_acv": "€200K",
                "top_risk": "NA risk.",
                "top_action": "NA action.",
            },
        ],
        "global_summary": {
            "global_pipeline_arr_q2": "€17.0M",
            "global_renewal_acv_q2": "€1.0M",
            "global_missing_approval_count": 8,
            "global_top_risk": "Global risk.",
            "global_top_action": "Global action.",
        },
        "commercial_approval": {
            "approved_2026_by_region": [{"region_name": "APAC", "deal_count": 2, "arr_eur": "€1.0M"}],
            "missing_approval_by_region": [{"region_name": "EMEA", "candidate_count": 3, "arr_eur": "€2.0M"}],
            "largest_global_missing_candidates": [{"region_name": "EMEA", "opportunity": "Big Deal", "arr_eur": "€1.5M"}],
        },
        "metric_definition_notes": ["Pipeline = ARR", "Renewals = ACV"],
        "region_rollup_notes": ["MEA remains under EMEA."],
        "known_gaps": ["Global summary does not replace director decks."],
    }

    artifacts = build_validation_artifacts(snapshot)
    assert "Validated Global Summary Fact Pack" in artifacts["validated_brief"]
    payload = artifacts["structured_fill_payload"]
    exec_slide = next(slide for slide in payload["slides"] if slide["id"] == "global-executive-summary")
    assert exec_slide["slots"]["global_pipeline_arr_q2"] == "€17.0M"
    emea_slide = next(slide for slide in payload["slides"] if slide["id"] == "emea-region-summary")
    assert emea_slide["slots"]["top_action"] == "EMEA action."
    appendix = next(slide for slide in payload["slides"] if slide["id"] == "global-appendix")
    assert appendix["slots"]["region_rollup_notes"] == ["MEA remains under EMEA."]
    assert "Structured fill payload (JSON)" in artifacts["powerpoint_build_prompt"]
    assert "`global-commercial-approval-overview`" in artifacts["powerpoint_build_prompt"]
    assert "`north-america-region-summary`" in artifacts["powerpoint_build_prompt"]

