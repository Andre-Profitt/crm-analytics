#!/usr/bin/env python3
"""Add YoY/QoQ variance indicator widgets to all Gen 2 dashboards.

Adds a small variance number widget row beneath existing KPI number widgets
on each dashboard's summary page. Each variance widget shows a YoY or QoQ
percentage change.

Pattern: Create a cogroup step comparing current vs prior period, then add
small number widgets (size=16, rowspan=2) positioned below the main KPIs.
"""

import json
from pathlib import Path
import subprocess
import sys
import urllib.request

# ── Shared utilities ──────────────────────────────────────────────────────

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def get_token():
    r = subprocess.run(
        ["sf", "org", "display", "--json"], capture_output=True, text=True
    )
    d = json.loads(r.stdout)
    return d["result"]["accessToken"], d["result"]["instanceUrl"]


def _normalize_state(state):
    from crm_analytics_helpers import normalize_dashboard_state_for_patch

    return normalize_dashboard_state_for_patch(state)


def get_dashboard(instance, token, dashboard_id):
    url = f"{instance}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)
    data = json.loads(resp.read())
    return data


def patch_dashboard(instance, token, dashboard_id, state):
    url = f"{instance}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    payload = json.dumps({"state": state}).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=payload,
        method="PATCH",
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    resp = urllib.request.urlopen(req)
    return resp.status


# ── Variance step builders ────────────────────────────────────────────────


def build_cogroup_variance_step(
    dataset,
    record_type,
    measure_expr,
    measure_alias,
    current_filter,
    prior_filter,
    filter_bindings=None,
    extra_filters=None,
):
    """Build a cogroup SAQL step comparing current vs prior period.

    Returns a dict with type=saql and the query string.
    """
    bindings = ""
    if filter_bindings:
        for fb in filter_bindings:
            binding_line = f'q_X = filter q_X by {{{{coalesce(column({fb["step"]}.selection, ["{fb["field"]}"]), column({fb["step"]}.result, ["{fb["field"]}"])).asEquality(\'{fb["field"]}\')}}}};\n'
            bindings += binding_line

    extra = ""
    if extra_filters:
        for ef in extra_filters:
            extra += f"q_X = filter q_X by {ef};\n"

    cur_bindings = bindings.replace("q_X", "q_cur")
    pri_bindings = bindings.replace("q_X", "q_pri")
    cur_extra = extra.replace("q_X", "q_cur")
    pri_extra = extra.replace("q_X", "q_pri")

    q = f"""q_cur = load "{dataset}";
q_cur = filter q_cur by RecordType == "{record_type}";
{cur_bindings}{cur_extra}{current_filter.replace("q_X", "q_cur")}q_cur = group q_cur by all;
q_cur = foreach q_cur generate {measure_expr} as {measure_alias};
q_pri = load "{dataset}";
q_pri = filter q_pri by RecordType == "{record_type}";
{pri_bindings}{pri_extra}{prior_filter.replace("q_X", "q_pri")}q_pri = group q_pri by all;
q_pri = foreach q_pri generate {measure_expr} as {measure_alias};
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate coalesce(sum(q_cur.{measure_alias}), 0) as current_val, coalesce(sum(q_pri.{measure_alias}), 0) as prior_val, (coalesce(sum(q_cur.{measure_alias}), 0) - coalesce(sum(q_pri.{measure_alias}), 0)) as delta, (case when coalesce(sum(q_pri.{measure_alias}), 0) != 0 then ((coalesce(sum(q_cur.{measure_alias}), 0) - coalesce(sum(q_pri.{measure_alias}), 0)) / abs(coalesce(sum(q_pri.{measure_alias}), 0))) * 100 else 0 end) as pct_change;"""

    return {"type": "saql", "query": q, "broadcastFacet": True}


def build_simple_yoy_step(
    dataset, record_type_current, year_field, current_year, prior_year, measures_dict
):
    """Build a simple YoY step for datasets that have yearly pre-computed rows.

    measures_dict: {alias: field_name, ...} e.g. {"nrr": "NRR", "grr": "GRR"}
    """
    cur_fields = ", ".join(f"{v} as cur_{k}" for k, v in measures_dict.items())
    pri_fields = ", ".join(f"{v} as pri_{k}" for k, v in measures_dict.items())

    cur_selects = ", ".join(
        f"coalesce(sum(q_cur.cur_{k}), 0) as cur_{k}" for k in measures_dict
    )
    pri_selects = ", ".join(
        f"coalesce(sum(q_pri.pri_{k}), 0) as pri_{k}" for k in measures_dict
    )

    delta_selects = []
    for k in measures_dict:
        delta_selects.append(
            f"(coalesce(sum(q_cur.cur_{k}), 0) - coalesce(sum(q_pri.pri_{k}), 0)) as delta_{k}"
        )
        delta_selects.append(
            f"(case when coalesce(sum(q_pri.pri_{k}), 0) != 0 then ((coalesce(sum(q_cur.cur_{k}), 0) - coalesce(sum(q_pri.pri_{k}), 0)) / abs(coalesce(sum(q_pri.pri_{k}), 0))) * 100 else 0 end) as pct_{k}"
        )

    q = f"""q_cur = load "{dataset}";
q_cur = filter q_cur by RecordType == "{record_type_current}";
q_cur = filter q_cur by {year_field} == "{current_year}";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate {cur_fields};
q_pri = load "{dataset}";
q_pri = filter q_pri by RecordType == "{record_type_current}";
q_pri = filter q_pri by {year_field} == "{prior_year}";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate {pri_fields};
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate {cur_selects}, {pri_selects}, {", ".join(delta_selects)};"""

    return {"type": "saql", "query": q, "broadcastFacet": True}


def variance_number_widget(step_name, field, title, color="#54698D"):
    """Small variance number widget."""
    return {
        "type": "number",
        "parameters": {
            "compact": False,
            "exploreLink": True,
            "interactions": [],
            "measureField": field,
            "numberColor": color,
            "numberSize": 16,
            "step": step_name,
            "textAlignment": "center",
            "title": title,
            "titleColor": "#54698D",
            "titleSize": 10,
        },
    }


# ── Dashboard-specific configurations ─────────────────────────────────────

DASHBOARD_CONFIGS = [
    # 1. Executive Pipeline Risk & Process
    {
        "id": "0FKTb0000000I09OAE",
        "label": "Executive Pipeline Risk & Process",
        "page_index": 0,
        "kpi_widgets": ["p1_n_projected", "p1_n_gap", "p1_n_atrisk"],
        "variance_steps": {
            "s_pipeline_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Pipeline_Opportunity_Operations";
q_cur = filter q_cur by RecordType == "detail";
q_cur = filter q_cur by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_cur = filter q_cur by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = filter q_cur by IsClosed == "false";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(WeightedOpenARR) as pipe_val;
q_pri = load "Pipeline_Opportunity_Operations";
q_pri = filter q_pri by RecordType == "detail";
q_pri = filter q_pri by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_pri = filter q_pri by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = filter q_pri by IsClosed == "false";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(WeightedOpenARR) as pipe_val;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pipe_val), 0) != 0 then ((coalesce(sum(q_cur.pipe_val), 0) - coalesce(sum(q_pri.pipe_val), 0)) / abs(coalesce(sum(q_pri.pipe_val), 0))) * 100 else 0 end) as pipe_pct, coalesce(sum(q_cur.pipe_val), 0) - coalesce(sum(q_pri.pipe_val), 0) as pipe_delta;""",
            }
        },
        "variance_widgets": {
            "p1_v_projected": variance_number_widget(
                "s_pipeline_variance", "pipe_pct", "YoY Pipeline %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_projected",
                "row": 11,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
        ],
    },
    # 2. Executive Customer Risk & Growth
    {
        "id": "0FKTb0000000I1lOAE",
        "label": "Executive Customer Risk & Growth",
        "page_index": 0,
        "kpi_widgets": ["p1_n_arr", "p1_n_expand", "p1_n_renewal"],
        "variance_steps": {
            "s_customer_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Customer_Account_Health";
q_cur = filter q_cur by RecordType == "revenue_trend";
q_cur = filter q_cur by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_cur = filter q_cur by {{coalesce(column(f_segment.selection, ["Segment"]), column(f_segment.result, ["Segment"])).asEquality('Segment')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(WonARR) as cur_arr;
q_pri = load "Customer_Account_Health";
q_pri = filter q_pri by RecordType == "revenue_trend";
q_pri = filter q_pri by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_pri = filter q_pri by {{coalesce(column(f_segment.selection, ["Segment"]), column(f_segment.result, ["Segment"])).asEquality('Segment')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(WonARR) as pri_arr;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate coalesce(sum(q_cur.cur_arr), 0) as cur_arr, coalesce(sum(q_pri.pri_arr), 0) as pri_arr, (case when coalesce(sum(q_pri.pri_arr), 0) != 0 then ((coalesce(sum(q_cur.cur_arr), 0) - coalesce(sum(q_pri.pri_arr), 0)) / abs(coalesce(sum(q_pri.pri_arr), 0))) * 100 else 0 end) as arr_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_arr": variance_number_widget(
                "s_customer_variance", "arr_pct", "YoY Customer ARR %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_arr", "row": 11, "column": 0, "colspan": 12, "rowspan": 2},
        ],
    },
    # 3. Executive Product Mix & Industry
    {
        "id": "0FKTb0000000IBROA2",
        "label": "Executive Product Mix & Industry",
        "page_index": 0,
        "kpi_widgets": [
            "p1_n_installed",
            "p1_n_whitespace",
            "p1_n_expansion",
            "p1_n_saas",
            "p1_n_accounts",
        ],
        "variance_steps": {
            "s_product_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Product_Portfolio_Whitespace";
q_cur = filter q_cur by RecordType == "portfolio_trend";
q_cur = filter q_cur by {{coalesce(column(f_industry.selection, ["IndustryVertical"]), column(f_industry.result, ["IndustryVertical"])).asEquality('IndustryVertical')}};
q_cur = filter q_cur by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(InstalledARR) as cur_installed;
q_pri = load "Product_Portfolio_Whitespace";
q_pri = filter q_pri by RecordType == "portfolio_trend";
q_pri = filter q_pri by {{coalesce(column(f_industry.selection, ["IndustryVertical"]), column(f_industry.result, ["IndustryVertical"])).asEquality('IndustryVertical')}};
q_pri = filter q_pri by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(InstalledARR) as pri_installed;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_installed), 0) != 0 then ((coalesce(sum(q_cur.cur_installed), 0) - coalesce(sum(q_pri.pri_installed), 0)) / abs(coalesce(sum(q_pri.pri_installed), 0))) * 100 else 0 end) as installed_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_installed": variance_number_widget(
                "s_product_variance", "installed_pct", "YoY Installed ARR %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_installed",
                "row": 11,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
        ],
    },
    # 4. Forecast & Revenue Motions
    {
        "id": "0FKTb0000000HthOAE",
        "label": "Forecast & Revenue Motions",
        "page_index": 0,
        "kpi_widgets": ["p1_n_actual", "p1_n_projected", "p1_n_gap", "p1_n_conf"],
        "variance_steps": {
            "s_forecast_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Forecast_Revenue_Motions";
q_cur = filter q_cur by RecordType == "detail";
q_cur = filter q_cur by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_cur = filter q_cur by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = filter q_cur by IsWon == "true";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(ARR) as cur_closed;
q_pri = load "Forecast_Revenue_Motions";
q_pri = filter q_pri by RecordType == "detail";
q_pri = filter q_pri by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_pri = filter q_pri by {{coalesce(column(f_region.selection, ["SalesRegion"]), column(f_region.result, ["SalesRegion"])).asEquality('SalesRegion')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = filter q_pri by IsWon == "true";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(ARR) as pri_closed;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate coalesce(sum(q_cur.cur_closed), 0) as cur_closed, coalesce(sum(q_pri.pri_closed), 0) as pri_closed, (coalesce(sum(q_cur.cur_closed), 0) - coalesce(sum(q_pri.pri_closed), 0)) as closed_delta, (case when coalesce(sum(q_pri.pri_closed), 0) != 0 then ((coalesce(sum(q_cur.cur_closed), 0) - coalesce(sum(q_pri.pri_closed), 0)) / abs(coalesce(sum(q_pri.pri_closed), 0))) * 100 else 0 end) as closed_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_actual": variance_number_widget(
                "s_forecast_variance", "closed_pct", "YoY Closed Won %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_actual",
                "row": 11,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
        ],
    },
    # 5. Lead Funnel
    {
        "id": "0FKTb0000000HwvOAE",
        "label": "Lead Funnel",
        "page_index": 0,
        "kpi_widgets": ["p1_n_open", "p1_n_projected", "p1_n_sla"],
        "variance_steps": {
            "s_lead_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Lead_Funnel";
q_cur = filter q_cur by RecordType == "trend";
q_cur = filter q_cur by {{coalesce(column(f_region.selection, ["Region"]), column(f_region.result, ["Region"])).asEquality('Region')}};
q_cur = filter q_cur by {{coalesce(column(f_source.selection, ["SourceGroup"]), column(f_source.result, ["SourceGroup"])).asEquality('SourceGroup')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(ConvertedCount) as cur_converted, sum(CreatedCount) as cur_created;
q_pri = load "Lead_Funnel";
q_pri = filter q_pri by RecordType == "trend";
q_pri = filter q_pri by {{coalesce(column(f_region.selection, ["Region"]), column(f_region.result, ["Region"])).asEquality('Region')}};
q_pri = filter q_pri by {{coalesce(column(f_source.selection, ["SourceGroup"]), column(f_source.result, ["SourceGroup"])).asEquality('SourceGroup')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(ConvertedCount) as pri_converted, sum(CreatedCount) as pri_created;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_converted), 0) != 0 then ((coalesce(sum(q_cur.cur_converted), 0) - coalesce(sum(q_pri.pri_converted), 0)) / abs(coalesce(sum(q_pri.pri_converted), 0))) * 100 else 0 end) as conversion_pct, (case when coalesce(sum(q_pri.pri_created), 0) != 0 then ((coalesce(sum(q_cur.cur_created), 0) - coalesce(sum(q_pri.pri_created), 0)) / abs(coalesce(sum(q_pri.pri_created), 0))) * 100 else 0 end) as volume_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_volume": variance_number_widget(
                "s_lead_variance", "volume_pct", "YoY Lead Volume %"
            ),
            "p1_v_conv": variance_number_widget(
                "s_lead_variance", "conversion_pct", "YoY Conversions %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_volume", "row": 11, "column": 0, "colspan": 6, "rowspan": 2},
            {"name": "p1_v_conv", "row": 11, "column": 6, "colspan": 6, "rowspan": 2},
        ],
    },
    # 6. Contract Operations & Renewals
    {
        "id": "0FKTb0000000HyXOAU",
        "label": "Contract Operations & Renewals",
        "page_index": 0,
        "kpi_widgets": ["p1_n_active", "p1_n_expiring", "p1_n_backlog"],
        "variance_steps": {
            "s_contract_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Contract_Operations_Renewals";
q_cur = filter q_cur by RecordType == "trend";
q_cur = filter q_cur by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(ActivatedCount) as cur_activated, sum(ExpiredCount) as cur_expired;
q_pri = load "Contract_Operations_Renewals";
q_pri = filter q_pri by RecordType == "trend";
q_pri = filter q_pri by {{coalesce(column(f_unit.selection, ["UnitGroup"]), column(f_unit.result, ["UnitGroup"])).asEquality('UnitGroup')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(ActivatedCount) as pri_activated, sum(ExpiredCount) as pri_expired;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_activated), 0) != 0 then ((coalesce(sum(q_cur.cur_activated), 0) - coalesce(sum(q_pri.pri_activated), 0)) / abs(coalesce(sum(q_pri.pri_activated), 0))) * 100 else 0 end) as activated_pct, (case when coalesce(sum(q_pri.pri_expired), 0) != 0 then ((coalesce(sum(q_cur.cur_expired), 0) - coalesce(sum(q_pri.pri_expired), 0)) / abs(coalesce(sum(q_pri.pri_expired), 0))) * 100 else 0 end) as expired_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_activated": variance_number_widget(
                "s_contract_variance", "activated_pct", "YoY Activated %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_activated",
                "row": 11,
                "column": 0,
                "colspan": 12,
                "rowspan": 2,
            },
        ],
    },
    # 7. Revenue Retention & Health
    {
        "id": "0FKTb0000000ITBOA2",
        "label": "Revenue Retention & Health",
        "page_index": 0,
        "kpi_widgets": ["p1_n_nrr", "p1_n_grr", "p1_n_churn", "p1_n_ending"],
        "variance_steps": {
            "s_retention_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Revenue_Retention_Health";
q_cur = filter q_cur by RecordType == "yearly_metric";
q_cur = filter q_cur by Year == "2025";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate max(NRR) as cur_nrr, max(GRR) as cur_grr, max(ChurnRate) as cur_churn, max(EndingARR) as cur_arr;
q_pri = load "Revenue_Retention_Health";
q_pri = filter q_pri by RecordType == "yearly_metric";
q_pri = filter q_pri by Year == "2024";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate max(NRR) as pri_nrr, max(GRR) as pri_grr, max(ChurnRate) as pri_churn, max(EndingARR) as pri_arr;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (coalesce(sum(q_cur.cur_nrr), 0) - coalesce(sum(q_pri.pri_nrr), 0)) as nrr_delta, (coalesce(sum(q_cur.cur_grr), 0) - coalesce(sum(q_pri.pri_grr), 0)) as grr_delta, (coalesce(sum(q_cur.cur_churn), 0) - coalesce(sum(q_pri.pri_churn), 0)) as churn_delta, (case when coalesce(sum(q_pri.pri_arr), 0) != 0 then ((coalesce(sum(q_cur.cur_arr), 0) - coalesce(sum(q_pri.pri_arr), 0)) / abs(coalesce(sum(q_pri.pri_arr), 0))) * 100 else 0 end) as arr_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_nrr": variance_number_widget(
                "s_retention_variance", "nrr_delta", "NRR YoY Δ (pp)"
            ),
            "p1_v_grr": variance_number_widget(
                "s_retention_variance", "grr_delta", "GRR YoY Δ (pp)"
            ),
            "p1_v_churn": variance_number_widget(
                "s_retention_variance", "churn_delta", "Churn YoY Δ (pp)"
            ),
            "p1_v_arr": variance_number_widget(
                "s_retention_variance", "arr_pct", "ARR YoY %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_nrr", "row": 11, "column": 0, "colspan": 3, "rowspan": 2},
            {"name": "p1_v_grr", "row": 11, "column": 3, "colspan": 3, "rowspan": 2},
            {"name": "p1_v_churn", "row": 11, "column": 6, "colspan": 3, "rowspan": 2},
            {"name": "p1_v_arr", "row": 11, "column": 9, "colspan": 3, "rowspan": 2},
        ],
    },
    # 8. Sales Activity & Productivity
    {
        "id": "0FKTb0000000IRZOA2",
        "label": "Sales Activity & Productivity",
        "page_index": 0,
        "kpi_widgets": ["p1_n_total", "p1_n_completed", "p1_n_accounts", "p1_n_reps"],
        "variance_steps": {
            "s_activity_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Sales_Activity_Productivity";
q_cur = filter q_cur by RecordType == "activity";
q_cur = filter q_cur by MonthLabel >= "2025-01" && MonthLabel <= "2025-12";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate count() as cur_total, sum(IsCompleted) as cur_completed, unique(AccountName) as cur_accounts;
q_pri = load "Sales_Activity_Productivity";
q_pri = filter q_pri by RecordType == "activity";
q_pri = filter q_pri by MonthLabel >= "2024-01" && MonthLabel <= "2024-12";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate count() as pri_total, sum(IsCompleted) as pri_completed, unique(AccountName) as pri_accounts;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_total), 0) != 0 then ((coalesce(sum(q_cur.cur_total), 0) - coalesce(sum(q_pri.pri_total), 0)) / abs(coalesce(sum(q_pri.pri_total), 0))) * 100 else 0 end) as total_pct, (case when coalesce(sum(q_pri.pri_completed), 0) != 0 then ((coalesce(sum(q_cur.cur_completed), 0) - coalesce(sum(q_pri.pri_completed), 0)) / abs(coalesce(sum(q_pri.pri_completed), 0))) * 100 else 0 end) as completed_pct, (case when coalesce(sum(q_pri.pri_accounts), 0) != 0 then ((coalesce(sum(q_cur.cur_accounts), 0) - coalesce(sum(q_pri.pri_accounts), 0)) / abs(coalesce(sum(q_pri.pri_accounts), 0))) * 100 else 0 end) as accounts_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_total": variance_number_widget(
                "s_activity_variance", "total_pct", "YoY Activities %"
            ),
            "p1_v_completed": variance_number_widget(
                "s_activity_variance", "completed_pct", "YoY Completed %"
            ),
            "p1_v_accounts": variance_number_widget(
                "s_activity_variance", "accounts_pct", "YoY Accounts %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_total", "row": 11, "column": 0, "colspan": 4, "rowspan": 2},
            {
                "name": "p1_v_completed",
                "row": 11,
                "column": 4,
                "colspan": 4,
                "rowspan": 2,
            },
            {
                "name": "p1_v_accounts",
                "row": 11,
                "column": 8,
                "colspan": 4,
                "rowspan": 2,
            },
        ],
    },
    # 9. SaaS Transition & Delivery Model
    {
        "id": "0FKTb0000000IUnOAM",
        "label": "SaaS Transition & Delivery Model",
        "page_index": 0,
        "kpi_widgets": [
            "p1_n_saas_pct",
            "p1_n_saas_rev",
            "p1_n_onprem_rev",
            "p1_n_total",
        ],
        "variance_steps": {
            "s_saas_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "SaaS_Transition_Delivery";
q_cur = filter q_cur by RecordType == "yearly_summary";
q_cur = filter q_cur by Year == "2025";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate max(SaaSPct) as cur_saas_pct, max(SaaSRevenue) as cur_saas_rev, max(TotalRevenue) as cur_total;
q_pri = load "SaaS_Transition_Delivery";
q_pri = filter q_pri by RecordType == "yearly_summary";
q_pri = filter q_pri by Year == "2024";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate max(SaaSPct) as pri_saas_pct, max(SaaSRevenue) as pri_saas_rev, max(TotalRevenue) as pri_total;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (coalesce(sum(q_cur.cur_saas_pct), 0) - coalesce(sum(q_pri.pri_saas_pct), 0)) as saas_pct_delta, (case when coalesce(sum(q_pri.pri_saas_rev), 0) != 0 then ((coalesce(sum(q_cur.cur_saas_rev), 0) - coalesce(sum(q_pri.pri_saas_rev), 0)) / abs(coalesce(sum(q_pri.pri_saas_rev), 0))) * 100 else 0 end) as saas_rev_pct, (case when coalesce(sum(q_pri.pri_total), 0) != 0 then ((coalesce(sum(q_cur.cur_total), 0) - coalesce(sum(q_pri.pri_total), 0)) / abs(coalesce(sum(q_pri.pri_total), 0))) * 100 else 0 end) as total_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_saas_pct": variance_number_widget(
                "s_saas_variance", "saas_pct_delta", "Cloud Share Δ (pp)"
            ),
            "p1_v_saas_rev": variance_number_widget(
                "s_saas_variance", "saas_rev_pct", "YoY Cloud Rev %"
            ),
            "p1_v_total": variance_number_widget(
                "s_saas_variance", "total_pct", "YoY Total Rev %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_saas_pct",
                "row": 11,
                "column": 0,
                "colspan": 4,
                "rowspan": 2,
            },
            {
                "name": "p1_v_saas_rev",
                "row": 11,
                "column": 4,
                "colspan": 4,
                "rowspan": 2,
            },
            {"name": "p1_v_total", "row": 11, "column": 8, "colspan": 4, "rowspan": 2},
        ],
    },
    # 10. AE Performance Dashboard
    {
        "id": "0FKTb0000000IGHOA2",
        "label": "AE Performance Dashboard",
        "page_index": 0,
        "kpi_widgets": ["w_kpi_closed", "w_kpi_attain", "w_kpi_pipe", "w_kpi_opps"],
        "variance_steps": {
            "s_ae_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Forecast_Revenue_Motions";
q_cur = filter q_cur by RecordType == "detail";
q_cur = filter q_cur by IsWon == "true";
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(ARR) as cur_closed;
q_pri = load "Forecast_Revenue_Motions";
q_pri = filter q_pri by RecordType == "detail";
q_pri = filter q_pri by IsWon == "true";
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(ARR) as pri_closed;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_closed), 0) != 0 then ((coalesce(sum(q_cur.cur_closed), 0) - coalesce(sum(q_pri.pri_closed), 0)) / abs(coalesce(sum(q_pri.pri_closed), 0))) * 100 else 0 end) as closed_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_closed": variance_number_widget(
                "s_ae_variance", "closed_pct", "YoY Closed Won %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_closed", "row": 7, "column": 0, "colspan": 12, "rowspan": 2},
        ],
    },
    # 11. Manager Coaching Dashboard
    {
        "id": "0FKTb0000000IJVOA2",
        "label": "Manager Coaching Dashboard",
        "page_index": 0,
        "kpi_widgets": ["w_k1", "w_k2", "w_k3", "w_k4"],
        "variance_steps": {
            "s_coaching_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "Forecast_Revenue_Motions";
q_cur = filter q_cur by RecordType == "detail";
q_cur = filter q_cur by IsWon == "true";
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(ARR) as cur_closed, count() as cur_deals;
q_pri = load "Forecast_Revenue_Motions";
q_pri = filter q_pri by RecordType == "detail";
q_pri = filter q_pri by IsWon == "true";
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(ARR) as pri_closed, count() as pri_deals;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_closed), 0) != 0 then ((coalesce(sum(q_cur.cur_closed), 0) - coalesce(sum(q_pri.pri_closed), 0)) / abs(coalesce(sum(q_pri.pri_closed), 0))) * 100 else 0 end) as closed_pct, (case when coalesce(sum(q_pri.pri_deals), 0) != 0 then ((coalesce(sum(q_cur.cur_deals), 0) - coalesce(sum(q_pri.pri_deals), 0)) / abs(coalesce(sum(q_pri.pri_deals), 0))) * 100 else 0 end) as deals_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_closed": variance_number_widget(
                "s_coaching_variance", "closed_pct", "YoY Closed Won %"
            ),
            "p1_v_deals": variance_number_widget(
                "s_coaching_variance", "deals_pct", "YoY Won Deals %"
            ),
        },
        "variance_layout": [
            {"name": "p1_v_closed", "row": 7, "column": 0, "colspan": 6, "rowspan": 2},
            {"name": "p1_v_deals", "row": 7, "column": 6, "colspan": 6, "rowspan": 2},
        ],
    },
    # 12. BDR Manager
    {
        "id": "0FKTb0000000I8DOAU",
        "label": "BDR Manager",
        "page_index": 0,
        "kpi_widgets": [
            "p1_n_open",
            "p1_n_qualified",
            "p1_n_meeting",
            "p1_n_sla",
            "p1_n_pipeline",
        ],
        "variance_steps": {
            "s_bdr_variance": {
                "type": "saql",
                "broadcastFacet": True,
                "query": """q_cur = load "BDR_Operating_Rhythm";
q_cur = filter q_cur by RecordType == "lead_detail";
q_cur = filter q_cur by {{coalesce(column(f_team.selection, ["BDRTeam"]), column(f_team.result, ["BDRTeam"])).asEquality('BDRTeam')}};
q_cur = filter q_cur by FYLabel == "FY2026";
q_cur = group q_cur by all;
q_cur = foreach q_cur generate sum(QualifiedCount) as cur_qualified, sum(SourcedARR) as cur_arr;
q_pri = load "BDR_Operating_Rhythm";
q_pri = filter q_pri by RecordType == "lead_detail";
q_pri = filter q_pri by {{coalesce(column(f_team.selection, ["BDRTeam"]), column(f_team.result, ["BDRTeam"])).asEquality('BDRTeam')}};
q_pri = filter q_pri by FYLabel == "FY2025";
q_pri = group q_pri by all;
q_pri = foreach q_pri generate sum(QualifiedCount) as pri_qualified, sum(SourcedARR) as pri_arr;
q = cogroup q_cur by all, q_pri by all;
q = foreach q generate (case when coalesce(sum(q_pri.pri_qualified), 0) != 0 then ((coalesce(sum(q_cur.cur_qualified), 0) - coalesce(sum(q_pri.pri_qualified), 0)) / abs(coalesce(sum(q_pri.pri_qualified), 0))) * 100 else 0 end) as qualified_pct, (case when coalesce(sum(q_pri.pri_arr), 0) != 0 then ((coalesce(sum(q_cur.cur_arr), 0) - coalesce(sum(q_pri.pri_arr), 0)) / abs(coalesce(sum(q_pri.pri_arr), 0))) * 100 else 0 end) as arr_pct;""",
            }
        },
        "variance_widgets": {
            "p1_v_qualified": variance_number_widget(
                "s_bdr_variance", "qualified_pct", "YoY Qualified %"
            ),
            "p1_v_arr": variance_number_widget(
                "s_bdr_variance", "arr_pct", "YoY Sourced ARR %"
            ),
        },
        "variance_layout": [
            {
                "name": "p1_v_qualified",
                "row": 15,
                "column": 0,
                "colspan": 6,
                "rowspan": 2,
            },
            {"name": "p1_v_arr", "row": 15, "column": 6, "colspan": 6, "rowspan": 2},
        ],
    },
]


# ── Main execution ────────────────────────────────────────────────────────


def detect_layout_format(state):
    """Detect whether dashboard uses rowspan (lowercase) or rowSpan (camelCase)."""
    gl = state.get("gridLayouts", state.get("gridLayout"))
    if isinstance(gl, list) and gl:
        pages = gl[0].get("pages", [])
    elif isinstance(gl, dict):
        pages = gl.get("pages", [])
    else:
        return "rowspan", "colspan"  # default lowercase

    for page in pages:
        for w in page.get("widgets", []):
            if "rowSpan" in w:
                return "rowSpan", "colSpan"
            if "rowspan" in w:
                return "rowspan", "colspan"
    return "rowspan", "colspan"


def get_grid_layouts(state):
    """Get the grid layouts list, handling both gridLayouts (list) and gridLayout (dict)."""
    if "gridLayouts" in state:
        return state["gridLayouts"]
    elif "gridLayout" in state:
        # Wrap single dict in a list for uniform access
        return [state["gridLayout"]]
    return []


def process_dashboard(config, token, instance):
    """Add variance indicators to a single dashboard."""
    did = config["id"]
    label = config["label"]
    page_idx = config["page_index"]

    print(f"\n{'=' * 60}")
    print(f"Processing: {label} ({did})")

    # 1. GET dashboard
    data = get_dashboard(instance, token, did)
    state = _normalize_state(data["state"])

    # 2. Detect layout format
    rs_key, cs_key = detect_layout_format(state)
    print(f"  Layout format: {rs_key}/{cs_key}")

    # 3. Check if variance steps already exist
    existing_variance = [
        s for s in state["steps"] if s.startswith("s_") and "variance" in s.lower()
    ]
    if existing_variance:
        print(f"  ⚠ Variance steps already exist: {existing_variance}")
        print("  Skipping to avoid duplication.")
        return True

    # 4. Add variance steps
    for step_name, step_def in config["variance_steps"].items():
        state["steps"][step_name] = step_def
        print(f"  + Added step: {step_name}")

    # 5. Add variance widgets
    for widget_name, widget_def in config["variance_widgets"].items():
        state["widgets"][widget_name] = widget_def
        print(f"  + Added widget: {widget_name}")

    # 6. Add to layout
    layouts = get_grid_layouts(state)
    page = layouts[0]["pages"][page_idx]

    # Find where KPI widgets are and insert variance row right after them
    kpi_row = 0
    kpi_rowspan = 0
    for w in page["widgets"]:
        w_rs = w.get(rs_key, w.get("rowspan", w.get("rowSpan", 0)))
        w_row = w.get("row", 0)
        if w["name"] in config["kpi_widgets"]:
            if w_row + w_rs > kpi_row + kpi_rowspan:
                kpi_row = w_row
                kpi_rowspan = w_rs

    # The variance row goes right after the KPI row
    variance_row = kpi_row + kpi_rowspan

    # Shift all widgets below the KPI row down by 2 to make room
    for w in page["widgets"]:
        if w.get("row", 0) >= variance_row:
            w["row"] = w.get("row", 0) + 2

    # Add variance widgets at the new row, using the correct key format
    for layout_item in config["variance_layout"]:
        item = {
            "name": layout_item["name"],
            "row": variance_row,
            "column": layout_item["column"],
            rs_key: layout_item["rowspan"],
            cs_key: layout_item["colspan"],
        }
        page["widgets"].append(item)
        print(f"  + Layout: {item['name']} at row {variance_row}")

    # 7. PATCH dashboard
    try:
        status = patch_dashboard(instance, token, did, state)
        print(f"  ✓ Deployed ({status})")
        return True
    except Exception as e:
        print(f"  ✗ Deploy failed: {e}")
        # Try to read error body
        if hasattr(e, "read"):
            err_body = e.read().decode("utf-8", errors="replace")
            print(f"    Error body: {err_body[:500]}")
        return False


def main():
    token, instance = get_token()

    success = 0
    failed = 0
    skipped = 0

    for config in DASHBOARD_CONFIGS:
        try:
            result = process_dashboard(config, token, instance)
            if result:
                success += 1
        except Exception as e:
            print(f"  ✗ Error: {e}")
            import traceback

            traceback.print_exc()
            failed += 1

    print(f"\n{'=' * 60}")
    print(f"SUMMARY: {success} deployed, {failed} failed, {skipped} skipped")
    print(f"Total dashboards processed: {len(DASHBOARD_CONFIGS)}")


if __name__ == "__main__":
    main()
