from __future__ import annotations

from crm_analytics_helpers import find_dashboard_patch_contract_violations


def test_find_dashboard_patch_contract_violations_flags_missing_widget_and_step_refs() -> None:
    state = {
        "filters": [],
        "gridLayouts": [
            {
                "pages": [
                    {
                        "name": "summary",
                        "widgets": [
                            {"name": "headline_kpi", "row": 0, "column": 0, "rowspan": 2, "colspan": 2},
                            {"name": "missing_widget", "row": 2, "column": 0, "rowspan": 2, "colspan": 2},
                        ],
                    }
                ]
            }
        ],
        "widgets": {
            "headline_kpi": {
                "type": "number",
                "parameters": {
                    "step": "missing_step",
                    "measureField": "Value",
                },
            }
        },
        "steps": {
            "query_step": {
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

    violations = find_dashboard_patch_contract_violations(state)
    codes = {item["code"] for item in violations}

    assert "page_widget_missing" in codes
    assert "widget_step_missing" in codes
    assert "step_reference_missing" in codes


def test_find_dashboard_patch_contract_violations_allows_defined_selector_step_refs() -> None:
    state = {
        "filters": [],
        "gridLayouts": [
            {
                "pages": [
                    {
                        "name": "summary",
                        "widgets": [
                            {"name": "region_selector", "row": 0, "column": 0, "rowspan": 2, "colspan": 2},
                            {"name": "headline_kpi", "row": 2, "column": 0, "rowspan": 2, "colspan": 2},
                        ],
                    }
                ]
            }
        ],
        "widgets": {
            "region_selector": {
                "type": "listselector",
                "parameters": {
                    "step": "f_region",
                },
            },
            "headline_kpi": {
                "type": "number",
                "parameters": {
                    "step": "query_step",
                    "measureField": "Value",
                },
            },
        },
        "steps": {
            "f_region": {
                "type": "aggregateflex",
                "datasets": [{"name": "Dataset"}],
                "query": {"query": '{"measures":[["count","*"]],"groups":["SalesRegion"]}'},
            },
            "query_step": {
                "type": "saql",
                "query": (
                    'q = load "Dataset";\n'
                    'q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), '
                    'column(f_region.result, ["SalesRegion"])).asEquality(\'SalesRegion\')}};\n'
                    "q = group q by all;\n"
                    "q = foreach q generate count() as Value;"
                ),
            },
        },
        "widgetStyle": {},
    }

    violations = find_dashboard_patch_contract_violations(state)
    codes = {item["code"] for item in violations}

    assert "page_widget_missing" not in codes
    assert "widget_step_missing" not in codes
    assert "step_reference_missing" not in codes
