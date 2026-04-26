from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
MATERIALIZER = ROOT / "scripts" / "materialize_wave_patch_contract.py"


def run_materializer(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(MATERIALIZER), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_materialize_wave_patch_contract(tmp_path: Path) -> None:
    source_dashboard = tmp_path / "source_dashboard.json"
    source_dashboard.write_text(
        json.dumps(
            {
                "state": {
                    "steps": {
                        "s_duns_rate": {
                            "type": "saql",
                            "broadcastFacet": True,
                            "query": "q = load &quot;Account_Intelligence&quot;;\nq = group q by all;\nq = foreach q generate 91 as fill_rate;",
                        }
                    },
                    "widgets": {
                        "p1_g_duns": {
                            "type": "chart",
                            "parameters": {
                                "step": "s_duns_rate",
                                "visualizationType": "gauge",
                                "title": {"label": "DUNS Fill Rate %"},
                                "gauge": {"min": 0, "max": 100},
                            },
                        },
                        "p1_tbl_poor": {
                            "type": "chart",
                            "parameters": {
                                "step": "s_dq_poor_list",
                                "visualizationType": "comparisontable",
                                "columns": ["Name", "DataQualityScore"],
                                "formatRules": [
                                    {
                                        "field": "DataQualityScore",
                                        "type": "threshold",
                                        "rules": [{"operator": "lte", "value": 1, "color": "#D4504C"}],
                                    }
                                ],
                                "title": {"label": "Source Table"},
                            },
                        },
                    },
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    patch_set = tmp_path / "wave_patch_set.json"
    patch_set.write_text(
        json.dumps(
            {
                "step_fragments": [
                    {
                        "fragment_id": "step_s_duns_rate",
                        "target_path": "steps.s_duns_rate",
                        "payload": {"type": "aggregateflex", "query": "__bad__"},
                        "todo_fields": ["type", "query"],
                    },
                    {
                        "fragment_id": "step_sales_ops_closed_reason_missing_list",
                        "target_path": "steps.sales_ops_closed_reason_missing_list",
                        "payload": {"type": "saql", "query": "__bad__"},
                        "todo_fields": ["query"],
                    },
                ],
                "widget_fragments": [
                    {
                        "fragment_id": "widget_crm_data_quality_headline_story_1",
                        "target_path": "widgets.crm_data_quality_headline_story_1",
                        "payload": {"type": "chart", "parameters": {"step": "s_duns_rate", "visualizationType": "gauge"}},
                        "todo_fields": ["parameters.step"],
                    },
                    {
                        "fragment_id": "widget_crm_data_quality_action_layer_3",
                        "target_path": "widgets.crm_data_quality_action_layer_3",
                        "payload": {
                            "type": "chart",
                            "parameters": {
                                "step": "sales_ops_closed_reason_missing_list",
                                "visualizationType": "comparisontable",
                            },
                        },
                        "todo_fields": ["parameters.step"],
                    },
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    candidate_state = tmp_path / "dashboard_state.patch.json"
    candidate_state.write_text(
        json.dumps(
            {
                "steps": {
                    "s_duns_rate": {"type": "aggregateflex", "query": "__bad__"},
                    "sales_ops_closed_reason_missing_list": {"type": "saql", "query": "__bad__"},
                },
                "widgets": {
                    "crm_data_quality_headline_story_1": {
                        "type": "chart",
                        "parameters": {"step": "s_duns_rate", "visualizationType": "gauge"},
                    },
                    "crm_data_quality_action_layer_3": {
                        "type": "chart",
                        "parameters": {
                            "step": "sales_ops_closed_reason_missing_list",
                            "visualizationType": "comparisontable",
                        },
                    },
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    step_contract = tmp_path / "step_contract.json"
    step_contract.write_text(
        json.dumps(
            {
                "module_id": "test_module",
                "target_page": {"page_name": "crm_data_quality"},
                "baseline_exports": {
                    "account_intelligence_dashboard": str(source_dashboard)
                },
                "reuse_steps": [
                    {
                        "target_component_key": "crm_data_quality_headline_story_1",
                        "target_metric": "DUNS Coverage %",
                        "target_step_alias": "s_duns_rate",
                        "reuse_mode": "copy_live_step_and_widget_contract",
                        "source_widget_key": "p1_g_duns",
                        "source_step_alias": "s_duns_rate",
                        "visualization_type": "gauge",
                        "saql": "q = load \"Account_Intelligence\";\nq = group q by all;\nq = foreach q generate 91 as fill_rate;",
                    }
                ],
                "new_steps": [
                    {
                        "target_component_key": "crm_data_quality_action_layer_3",
                        "target_metric": "Action Queue: Closed Opportunities Missing Won/Lost Reason",
                        "target_step_alias": "sales_ops_closed_reason_missing_list",
                        "reuse_mode": "new_step_required",
                        "visualization_type": "comparisontable",
                        "widget_contract": {
                            "widget_type": "chart",
                            "template_export_key": "account_intelligence_dashboard",
                            "template_widget_key": "p1_tbl_poor",
                            "title_label": "Closed Opportunities Missing Won/Lost Reason",
                            "columns": ["OpportunityName", "AccountName", "CloseDate"],
                        },
                        "saql": "q = load \"Forecast_Revenue_Motions\";\nq = foreach q generate OpportunityName, AccountName, CloseDate;",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "materialized"
    result = run_materializer(
        "--patch-set",
        str(patch_set),
        "--candidate-state",
        str(candidate_state),
        "--step-contract",
        str(step_contract),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["step_updates"] == 2
    assert payload["summary"]["widget_updates"] == 2

    materialized_patch = json.loads((output_dir / "wave_patch_set.materialized.json").read_text(encoding="utf-8"))
    materialized_state = json.loads(
        (output_dir / "dashboard_state.patch.materialized.json").read_text(encoding="utf-8")
    )

    step_fragments = {
        fragment["target_path"].split("steps.", 1)[1]: fragment
        for fragment in materialized_patch["step_fragments"]
    }
    assert step_fragments["s_duns_rate"]["payload"]["type"] == "saql"
    assert step_fragments["s_duns_rate"]["payload"]["query"] == (
        'q = load "Account_Intelligence";\nq = group q by all;\nq = foreach q generate 91 as fill_rate;'
    )
    assert "todo_fields" not in step_fragments["s_duns_rate"]

    widget_fragments = {
        fragment["target_path"].split("widgets.", 1)[1]: fragment
        for fragment in materialized_patch["widget_fragments"]
    }
    assert widget_fragments["crm_data_quality_headline_story_1"]["payload"]["parameters"]["title"]["label"] == (
        "DUNS Fill Rate %"
    )
    table_widget = widget_fragments["crm_data_quality_action_layer_3"]["payload"]
    assert table_widget["parameters"]["step"] == "sales_ops_closed_reason_missing_list"
    assert table_widget["parameters"]["columns"] == ["OpportunityName", "AccountName", "CloseDate"]
    assert table_widget["parameters"]["title"]["label"] == "Closed Opportunities Missing Won/Lost Reason"
    assert "formatRules" not in table_widget["parameters"]

    assert materialized_state["steps"]["sales_ops_closed_reason_missing_list"]["query"].startswith(
        'q = load "Forecast_Revenue_Motions";'
    )
    assert materialized_state["widgets"]["crm_data_quality_action_layer_3"]["parameters"]["step"] == (
        "sales_ops_closed_reason_missing_list"
    )


def test_materialize_wave_patch_contract_applies_parameter_overrides(tmp_path: Path) -> None:
    source_dashboard = tmp_path / "source_dashboard.json"
    source_dashboard.write_text(
        json.dumps(
            {
                "state": {
                    "steps": {
                        "s_source_chart": {
                            "type": "saql",
                            "broadcastFacet": True,
                            "query": "q = load &quot;Pipeline_Opportunity_Operations&quot;;\nq = group q by OwnerName;\nq = foreach q generate OwnerName, sum(PastDueCount) as pastdue_cnt;",
                        }
                    },
                    "widgets": {
                        "p11_pastdue_owner": {
                            "type": "chart",
                            "parameters": {
                                "step": "s_source_chart",
                                "visualizationType": "hbar",
                                "columnMap": {
                                    "dimensionAxis": ["OwnerName"],
                                    "plots": ["pastdue_cnt"],
                                    "split": [],
                                    "trellis": [],
                                },
                                "measureAxis1": {"title": "Past Due Count"},
                                "title": {"label": "Past Due by Owner (Top 15)"},
                            },
                        }
                    },
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    patch_set = tmp_path / "wave_patch_set.json"
    patch_set.write_text(
        json.dumps(
            {
                "step_fragments": [
                    {
                        "fragment_id": "step_sales_ops_stage_aging",
                        "target_path": "steps.sales_ops_stage_aging",
                        "payload": {"type": "saql", "query": "__bad__"},
                        "todo_fields": ["query"],
                    }
                ],
                "widget_fragments": [
                    {
                        "fragment_id": "widget_pipeline_hygiene_diagnostic_breakdown_2",
                        "target_path": "widgets.pipeline_hygiene_diagnostic_breakdown_2",
                        "payload": {
                            "type": "chart",
                            "parameters": {
                                "step": "sales_ops_stage_aging",
                                "visualizationType": "hbar",
                            },
                        },
                        "todo_fields": ["parameters.step"],
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    candidate_state = tmp_path / "dashboard_state.patch.json"
    candidate_state.write_text(
        json.dumps(
            {
                "steps": {"sales_ops_stage_aging": {"type": "saql", "query": "__bad__"}},
                "widgets": {
                    "pipeline_hygiene_diagnostic_breakdown_2": {
                        "type": "chart",
                        "parameters": {
                            "step": "sales_ops_stage_aging",
                            "visualizationType": "hbar",
                        },
                    }
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    step_contract = tmp_path / "step_contract.json"
    step_contract.write_text(
        json.dumps(
            {
                "module_id": "page5_module",
                "target_page": {"page_name": "pipeline_hygiene"},
                "baseline_exports": {"sales_ops_shell_dashboard": str(source_dashboard)},
                "reuse_steps": [],
                "new_steps": [
                    {
                        "target_component_key": "pipeline_hygiene_diagnostic_breakdown_2",
                        "target_metric": "Avg Days In Stage by Stage",
                        "target_step_alias": "sales_ops_stage_aging",
                        "reuse_mode": "new_step_required",
                        "visualization_type": "hbar",
                        "widget_contract": {
                            "widget_type": "chart",
                            "template_export_key": "sales_ops_shell_dashboard",
                            "template_widget_key": "p11_pastdue_owner",
                            "title_label": "Avg Days In Stage by Stage",
                            "parameter_overrides": {
                                "columnMap": {
                                    "dimensionAxis": ["StageName"],
                                    "plots": ["avg_days_in_stage"],
                                    "split": [],
                                    "trellis": [],
                                },
                                "measureAxis1": {"title": "Avg Days In Stage"},
                            },
                        },
                        "saql": "q = load \"Pipeline_Opportunity_Operations\";\nq = group q by StageName;\nq = foreach q generate StageName, avg(DaysInStage) as avg_days_in_stage;",
                    }
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    output_dir = tmp_path / "materialized"
    result = run_materializer(
        "--patch-set",
        str(patch_set),
        "--candidate-state",
        str(candidate_state),
        "--step-contract",
        str(step_contract),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout

    materialized_patch = json.loads((output_dir / "wave_patch_set.materialized.json").read_text(encoding="utf-8"))
    widget_fragments = {
        fragment["target_path"].split("widgets.", 1)[1]: fragment
        for fragment in materialized_patch["widget_fragments"]
    }
    chart_widget = widget_fragments["pipeline_hygiene_diagnostic_breakdown_2"]["payload"]
    assert chart_widget["parameters"]["step"] == "sales_ops_stage_aging"
    assert chart_widget["parameters"]["title"]["label"] == "Avg Days In Stage by Stage"
    assert chart_widget["parameters"]["columnMap"] == {
        "dimensionAxis": ["StageName"],
        "plots": ["avg_days_in_stage"],
        "split": [],
        "trellis": [],
    }
    assert chart_widget["parameters"]["measureAxis1"]["title"] == "Avg Days In Stage"
