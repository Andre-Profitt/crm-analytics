#!/usr/bin/env python3
"""Add descriptive subtitles to chart widgets across all Gen 2 dashboards.

Generates subtitles by parsing SAQL step content:
  - Extracts dataset, group-by dimensions, measures, key filters
  - Combines with visualization type to produce natural-language subtitles
  - Only adds subtitles where missing (preserves existing)

Also supports a KNOWN_STEPS override dictionary for hand-tuned subtitles.
"""

import json
import html
from pathlib import Path
import re
import subprocess
import sys
import urllib.request

# ── Shared utilities ──────────────────────────────────────────────────

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


def fully_unescape(s):
    if not isinstance(s, str):
        return s
    prev = s
    while True:
        s = html.unescape(s)
        if s == prev:
            return s
        prev = s


def get_dashboard(instance, token, dashboard_id):
    url = f"{instance}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    resp = urllib.request.urlopen(req)
    return json.loads(resp.read())


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


# ── All active Gen 2 dashboards ──────────────────────────────────────

DASHBOARDS = {
    "Executive Revenue & Forecast": "0FKTb0000000HqTOAU",
    "Executive Pipeline Risk & Process": "0FKTb0000000I09OAE",
    "Executive Customer Risk & Growth": "0FKTb0000000I1lOAE",
    "Executive Product Mix & Industry": "0FKTb0000000IBROA2",
    "Executive Business Health": "0FKTb0000000IbFOAU",
    "Executive Summary": "0FKTb0000000Io9OAE",
    "Pipeline & Opportunity Operations": "0FKTb0000000Hs5OAE",
    "Forecast & Revenue Motions": "0FKTb0000000HthOAE",
    "Customer & Account Health": "0FKTb0000000HvJOAU",
    "Lead Funnel": "0FKTb0000000HwvOAE",
    "Contract Operations & Renewals": "0FKTb0000000HyXOAU",
    "BDR Manager": "0FKTb0000000I8DOAU",
    "BDR Rep Queue": "0FKTb0000000I9pOAE",
    "AE Performance Dashboard": "0FKTb0000000IGHOA2",
    "Sales Operations Command Center": "0FKTb0000000IHtOAM",
    "Manager Coaching Dashboard": "0FKTb0000000IJVOA2",
    "Finance Revenue Operations": "0FKTb0000000IOLOA2",
    "Revenue Retention & Health": "0FKTb0000000ITBOA2",
    "SaaS Transition & Delivery Model": "0FKTb0000000IUnOAM",
    "Sales Activity & Productivity": "0FKTb0000000IRZOA2",
    "KPI Scorecard": "0FKTb0000000IZdOAM",
    "Analytics Command Center": "0FKTb0000000IEfOAM",
    "Product Portfolio & Whitespace Analysis": "0FKTb0000000IWPOA2",
    "Product ML & Recommendations": "0FKTb0000000ID3OAM",
    "Anomaly Detection & Forecasting Lab": "0FKTb0000000IPxOAM",
    "Forecast Intelligence": "0FKTb0000000IcrOAE",
    "Revenue/Pipeline Analyst Lab": "0FKTb0000000I3NOAU",
    "Customer/Revenue Analyst Lab": "0FKTb0000000I4zOAE",
    "Advanced Pipeline Analytics": "0FKTb0000000HnFOAU",
    "Pipeline History": "0FKTb0000000ImXOAU",
    "Opp Management": "0FKTb0000000IjJOAU",
    "Lead Management KPIs": "0FKTb0000000Ig5OAE",
    "Sales Process Compliance KPIs": "0FKTb0000000IeTOAU",
    "Contract Operations KPIs": "0FKTb0000000IhhOAE",
    "Account Intelligence KPIs": "0FKTb0000000IkvOAE",
    "Marketing Pipeline Attribution": "0FKTb0000000IMjOAM",
}


# ── SAQL parsing ──────────────────────────────────────────────────────


def parse_saql(saql):
    """Extract key components from a SAQL query string."""
    saql = fully_unescape(saql) or ""

    # Datasets
    datasets = re.findall(r'load\s+"([^"]+)"', saql)

    # Group-by fields (single-quoted or bare)
    groups_quoted = re.findall(r"group\s+\w+\s+by\s+\(([^)]+)\)", saql)
    groups_simple = re.findall(r"group\s+\w+\s+by\s+'([^']+)'", saql)
    # Parse multi-field groups
    group_fields = []
    for g in groups_quoted:
        fields = re.findall(r"'([^']+)'", g)
        if not fields:
            fields = [f.strip() for f in g.split(",")]
        group_fields.extend(fields)
    group_fields.extend(groups_simple)
    # Deduplicate
    seen = set()
    unique_groups = []
    for f in group_fields:
        if f not in seen:
            seen.add(f)
            unique_groups.append(f)

    # Aliases (as FieldName)
    aliases = re.findall(r"\bas\s+(\w+)", saql)

    # Aggregate functions
    agg_funcs = re.findall(r"(sum|avg|count|max|min)\s*\(", saql, re.IGNORECASE)

    # Key filter contexts
    filters = []
    if re.search(r"IsWon\s*==\s*[\"']true", saql, re.IGNORECASE):
        filters.append("won")
    if re.search(r"IsClosed\s*==\s*[\"']true", saql, re.IGNORECASE):
        filters.append("closed")
    if re.search(r"IsClosed\s*==\s*[\"']false", saql, re.IGNORECASE):
        filters.append("open")
    if re.search(r"RiskBand\s*==\s*[\"'](Critical|High)", saql, re.IGNORECASE):
        filters.append("at-risk")
    if re.search(r"IsConverted\s*==\s*[\"']true", saql, re.IGNORECASE):
        filters.append("converted")
    if re.search(r"Status\s*==\s*[\"']Active", saql, re.IGNORECASE):
        filters.append("active")

    # Time patterns
    has_timeseries = bool(re.search(r"timeseries|fill\s*\(", saql, re.IGNORECASE))
    has_date_group = any(
        re.search(r"(month|quarter|year|week|date|period|fiscal)", f, re.IGNORECASE)
        for f in unique_groups
    )

    return {
        "datasets": datasets,
        "groups": unique_groups,
        "aliases": aliases,
        "agg_funcs": agg_funcs,
        "filters": filters,
        "has_timeseries": has_timeseries,
        "has_date_group": has_date_group,
    }


# ── Field → human label mapping ──────────────────────────────────────

FIELD_LABELS = {
    "StageName": "stage",
    "Stage": "stage",
    "OwnerName": "rep",
    "Owner": "rep",
    "OwnerId": "rep",
    "ForecastCategory": "forecast category",
    "Type": "opportunity type",
    "OpportunityType": "opportunity type",
    "LeadSource": "lead source",
    "Source": "source",
    "Industry": "industry",
    "Region": "region",
    "Unit_Group__c": "unit group",
    "UnitGroup": "unit group",
    "Product_Family__c": "product family",
    "ProductFamily": "product family",
    "Cluster": "product cluster",
    "ClusterName": "product cluster",
    "RiskBand": "risk band",
    "HealthBand": "health band",
    "HealthScore": "health score",
    "Month": "month",
    "Quarter": "quarter",
    "FiscalQuarter": "fiscal quarter",
    "Year": "year",
    "FiscalYear": "fiscal year",
    "Close_Month": "close month",
    "CloseMonth": "close month",
    "Contract_Status__c": "contract status",
    "Status": "status",
    "Priority": "priority",
    "AccountName": "account",
    "Name": "name",
    "Country": "country",
    "BillingCountry": "country",
    "Campaign": "campaign",
    "Channel": "channel",
    "ActivityType": "activity type",
    "TaskSubtype": "activity type",
    "Delivery_Model__c": "delivery model",
    "DeliveryModel": "delivery model",
}


def humanize_field(field):
    """Convert a field name to a human-readable label."""
    if field in FIELD_LABELS:
        return FIELD_LABELS[field]
    # CamelCase → words
    words = re.sub(r"([a-z])([A-Z])", r"\1 \2", field)
    words = words.replace("_", " ").replace("__c", "").strip().lower()
    return words


# ── Alias → measure description ──────────────────────────────────────

ALIAS_HINTS = {
    "ARR": "ARR",
    "ClosedARR": "closed ARR",
    "PipelineARR": "pipeline ARR",
    "OpenARR": "open pipeline ARR",
    "WeightedARR": "weighted pipeline ARR",
    "Quota": "quota",
    "Revenue": "revenue",
    "Amount": "deal amount",
    "Pipeline": "pipeline value",
    "WinRatePct": "win rate",
    "WinRate": "win rate",
    "ConversionRate": "conversion rate",
    "ConvRate": "conversion rate",
    "RiskScore": "risk score",
    "HealthScore": "health score",
    "WinScore": "win probability score",
    "SlipRisk": "slip risk score",
    "DaysInStage": "days in stage",
    "AvgDays": "average days",
    "CycleTime": "cycle time",
    "Opps": "opportunities",
    "Leads": "leads",
    "Accounts": "accounts",
    "Contacts": "contacts",
    "Meetings": "meetings",
    "Tasks": "activities",
    "Pct": "percentage",
    "Count": "count",
    "Total": "total",
    "Pacing": "pacing",
    "Attainment": "attainment",
    "Coverage": "pipeline coverage",
    "Confidence": "confidence",
    "NRR": "net revenue retention",
    "GRR": "gross revenue retention",
    "ChurnARR": "churned ARR",
    "ExpansionARR": "expansion ARR",
    "MQL": "MQLs",
    "SQL": "SQLs",
}


def primary_measure(aliases):
    """Identify the primary measure from aliases."""
    for alias in aliases:
        if alias in ALIAS_HINTS:
            return ALIAS_HINTS[alias]
    # Check partial matches
    for alias in aliases:
        al = alias.lower()
        if "arr" in al or "revenue" in al or "amount" in al:
            return "ARR"
        if "rate" in al or "pct" in al:
            return "rate"
        if "count" in al or "opps" in al or "leads" in al:
            return "count"
        if "score" in al:
            return "score"
        if "days" in al or "time" in al or "cycle" in al:
            return "time"
    return None


# ── Dataset → entity context ─────────────────────────────────────────

DATASET_ENTITIES = {
    "Pipeline_Opportunity_Operations": "opportunities",
    "Forecast_Revenue_Motions": "opportunities",
    "Customer_Account_Health": "accounts",
    "Lead_Funnel": "leads",
    "Contract_Operations_Renewals": "contracts",
    "Advanced_Pipeline_Analytics": "pipeline deals",
    "Advanced_Analytics_Scores": "ML-scored opportunities",
    "Lead_Conversion_Scores": "ML-scored leads",
    "Product_Portfolio_Whitespace": "product portfolio",
    "Whitespace_Propensity_Scores": "whitespace opportunities",
    "Next_Family_Recommendations": "product recommendations",
    "KPI_Scorecard": "KPI metrics",
    "Renewal_Risk_Scores": "renewal risk scores",
    "BDR_Lead_Attribution": "leads",
    "BDR_Operating_Rhythm": "leads",
    "Opp_Mgmt_KPIs": "opportunities",
    "Account_Intelligence": "accounts",
    "Contact_Coverage": "contacts",
    "Opp_History": "opportunities",
    "Opp_Field_History": "opportunities",
    "Revenue_Retention_Health": "revenue",
    "Pipeline_Trendlines": "pipeline",
    "Pipeline_Monte_Carlo": "pipeline",
    "Pipeline_Analytics": "pipeline",
    "Pipeline_Survival": "pipeline",
    "Pipeline_Transitions": "pipeline",
    "Opp_Geo_Map": "opportunities",
    "Product_ML_Recommendations": "product recommendations",
}


def dataset_entity(datasets):
    """Get the entity context from dataset names."""
    for ds in datasets:
        if ds in DATASET_ENTITIES:
            return DATASET_ENTITIES[ds]
    if datasets:
        # Clean up dataset name — try to derive a meaningful entity
        name = datasets[0].replace("_", " ").lower()
        # Common patterns
        if "opp" in name or "pipeline" in name:
            return "opportunities"
        if "lead" in name or "bdr" in name:
            return "leads"
        if "account" in name or "customer" in name:
            return "accounts"
        if "contract" in name or "renewal" in name:
            return "contracts"
        if "product" in name:
            return "products"
        if "revenue" in name or "forecast" in name:
            return "revenue"
        return name
    return ""


# ── Viz type → subtitle template ─────────────────────────────────────


def generate_subtitle(viz, parsed, step_name="", chart_title=""):
    """Generate a descriptive subtitle from parsed SAQL and viz type."""
    groups = parsed["groups"]
    aliases = parsed["aliases"]
    filters = parsed["filters"]
    datasets = parsed["datasets"]
    has_time = parsed["has_timeseries"] or parsed["has_date_group"]

    dim = humanize_field(groups[0]) if groups else None
    dim2 = humanize_field(groups[1]) if len(groups) > 1 else None
    measure = primary_measure(aliases)
    entity = dataset_entity(datasets)

    # Build scope suffix — only include meaningful filter context
    if filters:
        scope = (
            f" — {', '.join(filters)} {entity}"
            if entity
            else f" — {', '.join(filters)}"
        )
    else:
        scope = ""  # Don't append entity name as noise

    # Step name based hints for common patterns
    sn = step_name.lower()

    # ── Known step patterns → curated subtitles ─────────────────
    if "bullet" in sn:
        if "attain" in sn or "revenue" in sn:
            return "Actual vs quota — current period attainment"
        if "win" in sn:
            return "Win rate vs target — trailing period"
        if "coverage" in sn or "pipe" in sn:
            return "Pipeline coverage ratio vs target"
        if "conv" in sn or "rate" in sn:
            return "Conversion rate vs target benchmark"
        if "health" in sn:
            return "Health score vs target threshold"
        if "motion" in sn:
            return "Motion win rate vs historical benchmark"
        if "renewal" in sn:
            return "Renewal rate vs target — current period"
        if "cycle" in sn:
            return "Cycle time vs target benchmark"
        if "approval" in sn:
            return "Approval cycle time vs SLA target"
        if "mql" in sn:
            return "MQL conversion rate vs target"
        if "commit" in sn:
            return "Commit pipeline vs quota target"
        if "weighted" in sn:
            return "Weighted pipeline vs quota"
        return "Actual vs target — current period"

    if "summary" in sn or "kpi" in sn:
        return None  # Skip — summary/KPI steps are typically number widgets, not charts

    if "forecast" in sn:
        if "quality" in sn or "accuracy" in sn or "acc" in sn:
            return "Forecast accuracy — predicted vs actual close rates"
        if "compare" in sn:
            return "Forecast category comparison — current vs prior period"
        if "waterfall" in sn:
            return "Forecast bridge — commit to best case to pipeline"
        if has_time:
            return "Revenue forecast trajectory over time"
        return "Revenue forecast by category"

    if "funnel" in sn:
        return f"Pipeline progression through stages{scope}"
    if "sankey" in sn or "flow" in sn:
        if "stage" in sn:
            return "Stage transition flow — where deals move between stages"
        if "motion" in sn:
            return "Revenue motion flow — new business, expansion, renewal"
        if "health" in sn or "segment" in sn:
            return "Account health band transitions over time"
        if "lifecycle" in sn:
            return "Contract lifecycle stage flow"
        return f"Flow diagram{scope}"

    if "heatmap" in sn:
        if dim and dim2:
            return f"{dim.title()} × {dim2} performance matrix{scope}"
        if "owner" in sn or "rep" in sn:
            return "Rep performance heatmap across periods"
        if "stage" in sn:
            return "Stage analysis heatmap"
        if "source" in sn:
            return "Source performance heatmap across periods"
        if "product" in sn:
            return "Product family performance heatmap"
        return "Cross-dimensional performance heatmap"

    if "scatter" in sn:
        if "slip" in sn:
            return "Deal risk scatter — days to close vs slip probability"
        if "cycle" in sn:
            return "Cycle time scatter — deal size vs days in pipeline"
        if "health" in sn:
            return "Account health scatter — ARR vs health score"
        if "priority" in sn:
            return "Lead priority scatter — engagement vs conversion likelihood"
        if "coverage" in sn:
            return "Activity coverage scatter — deal value vs touch frequency"
        if "rep" in sn or "coach" in sn:
            return "Rep scatter — pipeline value vs win rate"
        if "bubble" in sn or "model" in sn:
            return "Model performance scatter — predicted vs actual"
        return f"Scatter analysis{scope}"

    if "treemap" in sn:
        if "portfolio" in sn or "product" in sn:
            return "Product portfolio treemap — size represents ARR share"
        if "industry" in sn:
            return "Industry revenue treemap — size represents ARR contribution"
        if "lifecycle" in sn:
            return "Contract lifecycle treemap"
        if "region" in sn or "motion" in sn:
            return "Revenue treemap — proportional to ARR"
        return f"Proportional treemap — size represents value{scope}"

    if "waterfall" in sn or "bridge" in sn:
        if "plan" in sn or "quota" in sn:
            return "Plan-to-actual bridge — gap decomposition by category"
        if "arr" in sn or "revenue" in sn:
            return "ARR bridge — beginning balance through movements to ending"
        if "pipeline" in sn:
            return "Pipeline changes waterfall — additions, removals, movements"
        return f"Bridge/waterfall breakdown{scope}"

    # ── Viz-type based generation ───────────────────────────────

    if viz in ("line", "area", "timeline"):
        if measure:
            if dim and not has_time:
                return f"{measure.capitalize()} by {dim}{scope}"
            return f"{measure.capitalize()} trend over time{scope}"
        if has_time or any(
            "month" in g.lower() or "quarter" in g.lower() for g in groups
        ):
            return f"Trend over time{scope}"
        return f"Trend analysis{scope}"

    if viz in ("hbar", "column"):
        if "attain" in sn:
            return f"Attainment ranking by {dim or 'rep'}{scope}"
        if "leader" in sn or "rank" in sn or "top" in sn:
            return f"Ranked by {measure or 'performance'}{scope}"
        if "win" in sn:
            return f"Win rate comparison by {dim or 'segment'}{scope}"
        if "push" in sn:
            return f"Close date push analysis{scope}"
        if "gap" in sn:
            return f"Gap analysis by {dim or 'segment'}{scope}"
        if "mix" in sn or "share" in sn:
            return f"Composition breakdown by {dim or 'category'}{scope}"
        if "variance" in sn:
            return f"Variance analysis by {dim or 'category'}{scope}"
        if "velocity" in sn or "vel" in sn:
            return f"Velocity by {dim or 'stage'}{scope}"
        if "aging" in sn:
            return f"Aging distribution by {dim or 'band'}{scope}"
        if "stale" in sn:
            return f"Stale {entity or 'items'} by {dim or 'owner'}{scope}"
        if "source" in sn:
            return f"Source distribution by {dim or 'source'}{scope}"
        if "loss" in sn:
            return f"Loss analysis by {dim or 'reason'}{scope}"
        if "product" in sn or "family" in sn:
            return f"Product breakdown by {dim or 'family'}{scope}"
        if "region" in sn or "country" in sn:
            return f"Regional breakdown by {dim or 'region'}{scope}"
        if "owner" in sn or "rep" in sn:
            return f"Rep comparison by {measure or 'performance'}{scope}"
        if "stage" in sn:
            return f"Stage breakdown by {measure or 'count'}{scope}"
        if dim and measure:
            return f"{measure.capitalize()} by {dim}{scope}"
        if dim:
            return f"Distribution by {dim}{scope}"
        if measure:
            return f"Ranked by {measure}{scope}"
        return f"Ranked comparison{scope}"

    if viz in ("stackhbar", "stackcolumn", "stackarea"):
        if dim and dim2:
            return f"{dim.title()} breakdown by {dim2}{scope}"
        if dim and measure:
            return f"Stacked {measure} by {dim}{scope}"
        if "risk" in sn:
            return f"Risk exposure — stacked by risk band{scope}"
        if "product" in sn or "family" in sn:
            return f"Product mix breakdown{scope}"
        if "stage" in sn:
            return f"Stage composition breakdown{scope}"
        if dim:
            return f"Composition by {dim}{scope}"
        return f"Stacked breakdown{scope}"

    if viz in ("donut", "pie"):
        if dim:
            return f"Distribution by {dim}{scope}"
        return f"Proportional distribution{scope}"

    if viz == "funnel":
        if "stage" in sn or "pipe" in sn:
            return "Pipeline stage funnel — volume by stage"
        if "conv" in sn:
            return "Conversion funnel — progression through stages"
        return f"Funnel progression{scope}"

    if viz == "waterfall":
        if "plan" in sn or "quota" in sn:
            return "Plan-to-actual bridge — gap decomposition"
        return f"Bridge decomposition — component contributions{scope}"

    if viz in (
        "comparisontable",
        "comparisonTable",
        "comparetable",
        "compareTable",
        "valuesTable",
        "valuestable",
    ):
        if "risk" in sn or "at_risk" in sn:
            return "At-risk items detail — sortable and filterable"
        if "action" in sn or "exception" in sn or "queue" in sn:
            return "Action queue — prioritized exception list"
        if "leader" in sn or "rank" in sn or "scorecard" in sn:
            return "Performance scorecard — ranked comparison"
        if "backlog" in sn or "expir" in sn:
            return "Upcoming items — sortable detail view"
        if "priority" in sn or "upcoming" in sn:
            return "Priority queue — upcoming actions"
        if "stalled" in sn or "stuck" in sn:
            return "Stalled items requiring attention"
        return "Detail table — click column headers to sort"

    if viz == "gauge":
        return "Current value vs target range"

    if viz == "matrix":
        return f"Cross-tabulation matrix{scope}"

    if viz == "combo":
        return f"Dual-axis comparison — trend vs volume{scope}"

    # Fallback
    if measure and dim:
        return f"{measure.capitalize()} by {dim}{scope}"
    if dim:
        return f"Analysis by {dim}{scope}"
    return None  # Don't add a generic subtitle


# ── Main processing ──────────────────────────────────────────────────


def process_dashboard(instance, token, label, dashboard_id, dry_run=False):
    """Add subtitles to charts missing them."""
    data = get_dashboard(instance, token, dashboard_id)
    state = _normalize_state(data.get("state", {}))

    widgets = state.get("widgets", {})
    steps = state.get("steps", {})
    charts_modified = 0
    changes = []

    for wname, widget in sorted(widgets.items()):
        if widget.get("type") != "chart":
            continue

        params = widget.get("parameters", {})
        viz = params.get("visualizationType", "?")

        # Check if subtitle already exists
        title_obj = params.get("title", {})
        if not isinstance(title_obj, dict):
            title_obj = {}
        existing_subtitle = title_obj.get("subtitleLabel", "")
        if existing_subtitle:
            continue  # Already has subtitle

        chart_title = title_obj.get("label", "")

        # Get SAQL from step
        step_name = params.get("step", "")
        step = steps.get(step_name, {})
        saql = step.get("query", "")
        if isinstance(saql, dict):
            saql = saql.get("query", "")

        # Parse SAQL
        parsed = parse_saql(saql)

        # Generate subtitle
        subtitle = generate_subtitle(viz, parsed, step_name, chart_title)

        if not subtitle:
            continue

        # Truncate if too long
        if len(subtitle) > 120:
            subtitle = subtitle[:117] + "..."

        # Apply
        if "title" not in params:
            params["title"] = {}
        if not isinstance(params["title"], dict):
            params["title"] = {}
        params["title"]["subtitleLabel"] = subtitle

        charts_modified += 1
        changes.append(f'  {wname} ({viz}): "{subtitle}"')

    if charts_modified == 0:
        print(f"  {label}: no charts need subtitles")
        return 0

    if dry_run:
        print(f"  {label}: would add {charts_modified} subtitles")
        for c in changes:
            print(c)
        return charts_modified

    try:
        status = patch_dashboard(instance, token, dashboard_id, state)
        print(f"✓ {label}: {status} — {charts_modified} subtitles added")
        for c in changes[:5]:
            print(c)
        if len(changes) > 5:
            print(f"  ... and {len(changes) - 5} more")
        return charts_modified
    except Exception as e:
        print(f"✗ {label}: FAILED — {e}")
        if hasattr(e, "read"):
            err = e.read().decode("utf-8", errors="replace")[:500]
            print(f"  {err}")
        return 0


def main():
    import sys

    dry_run = "--dry-run" in sys.argv

    token, instance = get_token()
    print(f"Auth OK: {instance}")
    if dry_run:
        print("DRY RUN — no changes will be deployed\n")
    print(f"Processing {len(DASHBOARDS)} dashboards...\n")

    total = 0
    for label, did in sorted(DASHBOARDS.items()):
        try:
            count = process_dashboard(instance, token, label, did, dry_run=dry_run)
            total += count
        except Exception as e:
            print(f"✗ {label}: ERROR — {e}")

    print(f"\n{'=' * 60}")
    print(f"Total charts with subtitles added: {total}")


if __name__ == "__main__":
    main()
