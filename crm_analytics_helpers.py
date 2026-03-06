#!/usr/bin/env python3
"""Shared helpers for CRM Analytics dashboard builders.

Provides reusable functions for:
- Salesforce auth & API calls
- Dataset upload (InsightsExternalData)
- SAQL step builders
- Widget builders (number, chart, gauge, funnel, waterfall, choropleth,
  sankey, treemap, bubble, heatmap, bullet, timeline, combo, scatter, line)
- Widget interaction framework (selection, results, facet interactions)
- Dynamic KPI coloring with threshold-based conditional formatting
- Layout helpers (header, section label, nav link, page)
- Dashboard deployment
- Dataflow management
"""

import base64
import json
import subprocess
import time
import urllib.request
import urllib.error
from datetime import datetime

TARGET_ORG = "apro@simcorp.com"
APP_NAME = "B2B_MA"

# ═══════════════════════════════════════════════════════════════════════════
#  Utilities
# ═══════════════════════════════════════════════════════════════════════════


def _date_diff(d1, d2):
    """Days between two date strings (yyyy-MM-dd). Returns 0 if either is empty."""
    if not d1 or not d2:
        return 0
    try:
        return abs(
            (
                datetime.strptime(d2[:10], "%Y-%m-%d")
                - datetime.strptime(d1[:10], "%Y-%m-%d")
            ).days
        )
    except ValueError:
        return 0


# ═══════════════════════════════════════════════════════════════════════════
#  Auth & API
# ═══════════════════════════════════════════════════════════════════════════


def get_auth():
    """Get Salesforce instance URL and access token via sf CLI."""
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
    )
    clean = "".join(c for c in r.stdout if ord(c) >= 32 or c in "\n\r\t")
    d = json.loads(clean)["result"]
    return d["instanceUrl"], d["accessToken"]


def _sf_api(inst, tok, method, path, body=None):
    """Make a Salesforce REST API call."""
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(
        f"{inst}{path}",
        data=data,
        method=method,
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode()) if resp.status != 204 else {}
    except urllib.error.HTTPError as e:
        err_body = e.read().decode()
        raise RuntimeError(
            f"API {method} {path} → HTTP {e.code}: {err_body[:500]}"
        ) from e


def _soql(inst, tok, query):
    """Run a SOQL query and return all records (handles pagination)."""
    encoded = urllib.request.quote(query)
    url = f"{inst}/services/data/v66.0/query/?q={encoded}"
    records = []
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read().decode())
        records.extend(data.get("records", []))
        next_url = data.get("nextRecordsUrl")
        url = f"{inst}{next_url}" if next_url else None
    return records


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset metadata helpers
# ═══════════════════════════════════════════════════════════════════════════


def _dim(name, label=None):
    """Build a Text (dimension) field metadata entry."""
    return {
        "fullyQualifiedName": name,
        "name": name,
        "type": "Text",
        "label": label or name,
    }


def _measure(name, label=None, scale=2, precision=18):
    """Build a Numeric (measure) field metadata entry."""
    return {
        "fullyQualifiedName": name,
        "name": name,
        "type": "Numeric",
        "label": label or name,
        "precision": precision,
        "scale": scale,
        "defaultValue": "0",
    }


def _date(name, label=None):
    """Build a Date field metadata entry."""
    return {
        "fullyQualifiedName": name,
        "name": name,
        "type": "Date",
        "label": label or name,
        "format": "yyyy-MM-dd",
        "fiscalMonthOffset": 0,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset upload
# ═══════════════════════════════════════════════════════════════════════════


def upload_dataset(
    inst,
    tok,
    ds_name,
    ds_label,
    fields_meta,
    csv_bytes,
    app_name=APP_NAME,
    poll_attempts=30,
    poll_interval=3,
):
    """Upload a CSV dataset to CRM Analytics via InsightsExternalData API.

    Returns True if successful, False otherwise.
    """
    meta = {
        "fileFormat": {
            "charsetName": "UTF-8",
            "fieldsDelimitedBy": ",",
            "linesTerminatedBy": "\n",
        },
        "objects": [
            {
                "connector": "CSV",
                "fullyQualifiedName": ds_name,
                "label": ds_label,
                "name": ds_name,
                "fields": fields_meta,
            }
        ],
    }
    meta_b64 = base64.b64encode(json.dumps(meta).encode()).decode()

    header = _sf_api(
        inst,
        tok,
        "POST",
        "/services/data/v66.0/sobjects/InsightsExternalData",
        {
            "EdgemartAlias": ds_name,
            "EdgemartLabel": ds_label,
            "EdgemartContainer": app_name,
            "FileName": ds_name,
            "Format": "Csv",
            "Operation": "Overwrite",
            "Action": "None",
            "MetadataJson": meta_b64,
        },
    )
    header_id = header["id"]
    print(f"  Upload header: {header_id}")

    chunk_size = 10 * 1024 * 1024
    part = 1
    for i in range(0, len(csv_bytes), chunk_size):
        chunk = csv_bytes[i : i + chunk_size]
        _sf_api(
            inst,
            tok,
            "POST",
            "/services/data/v66.0/sobjects/InsightsExternalDataPart",
            {
                "InsightsExternalDataId": header_id,
                "PartNumber": part,
                "DataFile": base64.b64encode(chunk).decode(),
            },
        )
        part += 1
    print(f"  Uploaded {len(csv_bytes):,} bytes in {part - 1} part(s)")

    _sf_api(
        inst,
        tok,
        "PATCH",
        f"/services/data/v66.0/sobjects/InsightsExternalData/{header_id}",
        {"Action": "Process"},
    )
    print("  Processing started...")

    for _i in range(poll_attempts):
        time.sleep(poll_interval)
        status = _soql(
            inst,
            tok,
            f"SELECT Status, StatusMessage FROM InsightsExternalData "
            f"WHERE Id = '{header_id}'",
        )
        if status:
            s = status[0].get("Status", "")
            msg = status[0].get("StatusMessage", "")
            if s in ("Completed", "CompletedWithWarnings"):
                print(f"  Dataset ready! ({s})")
                return True
            if s == "Failed":
                print(f"  FAILED: {msg}")
                return False
            print(f"  ... {s}")
    print("  Timed out waiting for dataset processing")
    return False


def get_dataset_id(inst, tok, ds_name):
    """Look up a dataset ID by API name."""
    result = _sf_api(
        inst,
        tok,
        "GET",
        f"/services/data/v66.0/wave/datasets?q={ds_name}",
    )
    for d in result.get("datasets", []):
        if d.get("name") == ds_name:
            return d["id"]
    return None


# ═══════════════════════════════════════════════════════════════════════════
#  Step builders
# ═══════════════════════════════════════════════════════════════════════════


def sq(query, broadcast=True):
    """Build a SAQL step dict."""
    return {"type": "saql", "query": query, "broadcastFacet": broadcast}


def af(field, ds_meta, select_mode="multi"):
    """Build an aggregateflex step for filter selectors.

    Args:
        field: Field name to aggregate on
        ds_meta: Dataset metadata list, e.g. [{"id": "...", "name": "DS_Name"}]
        select_mode: "multi" or "single"
    """
    return {
        "type": "aggregateflex",
        "broadcastFacet": True,
        "selectMode": select_mode,
        "datasets": ds_meta,
        "query": {
            "query": json.dumps({"measures": [["count", "*"]], "groups": [field]}),
            "version": -1.0,
        },
        "receiveFacetSource": {"mode": "all", "steps": []},
        "start": "[]",
        "useGlobal": True,
        "useExternalFilters": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widget builders
# ═══════════════════════════════════════════════════════════════════════════


def num(step, field, title, color, compact=False, size=24):
    """Build a number (KPI tile) widget."""
    return {
        "type": "number",
        "parameters": {
            "step": step,
            "measureField": field,
            "compact": compact,
            "title": title,
            "titleColor": "#54698D",
            "titleSize": 12,
            "numberColor": color,
            "numberSize": size,
            "textAlignment": "center",
            "exploreLink": True,
            "interactions": [],
        },
    }


def num_dynamic_color(step, field, title, thresholds, compact=False, size=24):
    """Build a KPI tile with dynamic color based on threshold step values.

    Uses CRM Analytics results interactions to set numberColor dynamically
    based on the value of a threshold field in the step results.

    Args:
        step: step name producing the value
        field: measure field name
        title: widget title
        thresholds: list of (max_value, color) tuples, e.g.
            [(50, "#D4504C"), (80, "#FFB75D"), (100, "#04844B")]
            Values below first max get first color, etc.
        compact: whether to use compact number format
        size: font size for the number
    """
    # Default to green; interactions will override based on value
    default_color = thresholds[-1][1] if thresholds else "#04844B"
    widget = {
        "type": "number",
        "parameters": {
            "step": step,
            "measureField": field,
            "compact": compact,
            "title": title,
            "titleColor": "#54698D",
            "titleSize": 12,
            "numberColor": default_color,
            "numberSize": size,
            "textAlignment": "center",
            "exploreLink": True,
            "interactions": [],
        },
    }
    # Build results interaction rules for dynamic coloring
    rules = []
    for max_val, color in thresholds:
        rules.append({
            "condition": {"operator": "<=", "value": max_val},
            "action": {"type": "setProperty", "property": "numberColor", "value": color},
        })
    if rules:
        widget["parameters"]["interactions"].append({
            "type": "resultsInteraction",
            "group": "color",
            "enabled": True,
            "source": {"step": step, "field": field},
            "rules": rules,
        })
    return widget


def rich_chart(
    step,
    viz,
    title,
    dim_fields,
    measure_fields,
    trellis=None,
    split=None,
    show_legend=False,
    legend_pos="right-top",
    show_pct=False,
    axis_title="",
    combo_config=None,
    normalize=False,
    show_values=False,
    reference_lines=None,
):
    """Build a chart widget with full CRM Analytics configuration.

    normalize: True for 100% stacked charts (all bars sum to 100%).
    show_values: True to show data labels on bars/slices.
    reference_lines: list of {value, label, color} dicts for threshold lines.
    """
    COLUMNMAP_TYPES = {
        "hbar",
        "column",
        "donut",
        "stackhbar",
        "stackcolumn",
        "pie",
        "vbar",
        "stackvbar",
    }
    params = {
        "step": step,
        "visualizationType": viz,
        "theme": "wave",
        "exploreLink": True,
        "showActionMenu": True,
        "autoFitMode": "fit",
        "title": {
            "label": title,
            "fontSize": 14,
            "subtitleFontSize": 11,
            "align": "center",
            "subtitleLabel": "",
        },
        "dimensionAxis": {
            "showTitle": True,
            "showAxis": True,
            "title": "",
            "customSize": "auto",
            "icons": {
                "useIcons": False,
                "iconProps": {"fit": "cover", "column": "", "type": "round"},
            },
        },
        "measureAxis1": {
            "showTitle": bool(axis_title),
            "showAxis": True,
            "title": axis_title,
            "sqrtScale": False,
            "customDomain": {"showDomain": False},
        },
        "legend": {
            "show": show_legend,
            "showHeader": True,
            "position": legend_pos,
            "inside": False,
            "customSize": "auto",
        },
        "applyConditionalFormatting": True,
        "interactions": [],
    }
    if viz in COLUMNMAP_TYPES:
        params["columnMap"] = {
            "dimensionAxis": dim_fields,
            "plots": measure_fields,
            "trellis": trellis or [],
            "split": split or [],
        }
    if combo_config:
        params["combo"] = combo_config
    if normalize:
        params["normalize"] = True
    if show_values:
        params["showValues"] = True
    if reference_lines:
        params["referenceLines"] = reference_lines
    return {"type": "chart", "parameters": params}


def gauge(step, field, title, min_val=0, max_val=100, bands=None):
    """Build a gauge chart widget with colored bands."""
    if bands is None:
        bands = [
            {"start": 0, "stop": 20, "color": "#D4504C"},
            {"start": 20, "stop": 40, "color": "#FFB75D"},
            {"start": 40, "stop": 100, "color": "#04844B"},
        ]
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "gauge",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "gauge": {
                "min": min_val,
                "max": max_val,
                "bands": bands,
            },
            "columnMap": {
                "trellis": [],
                "plots": [field],
            },
            "interactions": [],
        },
    }


def funnel_chart(step, title, dim_field, measure_field):
    """Build a funnel chart widget. Data must be ordered largest→smallest."""
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "funnel",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "columnMap": None,
            "legend": {
                "show": True,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def waterfall_chart(
    step, title, dim_field, measure_field, axis_label="ARR (EUR)", reference_lines=None
):
    """Build a waterfall chart widget.

    reference_lines: optional list of dicts with {value, label, color} for threshold lines.
    """
    params = {
        "step": step,
        "visualizationType": "waterfall",
        "title": {
            "label": title,
            "fontSize": 14,
            "subtitleFontSize": 11,
            "align": "center",
            "subtitleLabel": "",
        },
        "theme": "wave",
        "exploreLink": True,
        "showActionMenu": True,
        "autoFitMode": "fit",
        "columnMap": None,
        "measureAxis1": {
            "showTitle": True,
            "showAxis": True,
            "title": axis_label,
            "sqrtScale": False,
            "customDomain": {"showDomain": False},
        },
        "interactions": [],
    }
    if reference_lines:
        params["referenceLines"] = reference_lines
    return {"type": "chart", "parameters": params}


def choropleth_chart(step, title, geo_field, measure_field, map_type="World"):
    """Build a choropleth (map) chart widget.

    Args:
        step: step name
        title: chart title
        geo_field: dimension field with geographic values
        measure_field: measure field for color intensity
        map_type: map region - "World", "USA", "Europe", "Asia" etc.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "choropleth",
            "map": map_type,
            "binValues": False,
            "lowColor": "#C6DBEF",
            "highColor": "#08519C",
            "columnMap": {
                "locations": [geo_field],
                "color": [measure_field],
                "trellis": [],
                "dimensionAxis": [geo_field],
                "plots": [measure_field],
            },
            "title": {"label": title, "fontSize": 14},
            "theme": "wave",
            "legend": {"show": True, "position": "right-bottom"},
            "interactions": [],
        },
    }


def sankey_chart(
    step, title, source_field="source", target_field="target", measure_field="cnt"
):
    """Build a sankey diagram widget. Data must have source, target, and measure columns.

    columnMap: auto-detect (no explicit columnMap needed).
    SAQL must produce: source_field, target_field, measure_field.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "sankey",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "legend": {
                "show": True,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def treemap_chart(step, title, dim_fields, measure_field, show_legend=False):
    """Build a treemap chart widget.

    columnMap: null (auto-detect). CRM Analytics treemap needs:
    1 field for Segment Size (measure), 1-2 fields for Segments (dimensions).
    SAQL must produce the dimension(s) and measure in the correct order.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "treemap",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "columnMap": None,
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def bubble_chart(step, title, show_legend=True):
    """Build a bubble chart widget. Auto-detect columnMap.

    SAQL must produce: x_field, y_field, size_field (and optionally color_field).
    Uses visualizationType "bubble" for proper 3D rendering with size dimension.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "bubble",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "measureAxis1": {
                "showTitle": True,
                "showAxis": True,
                "title": "",
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def area_chart(step, title, stacked=False, show_legend=True, axis_title=""):
    """Build an area or stacked area chart widget. Auto-detect columnMap.

    SAQL must produce: dimension, measure (and optionally group field for stacking).
    """
    viz = "stackarea" if stacked else "area"
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": viz,
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "measureAxis1": {
                "showTitle": bool(axis_title),
                "showAxis": True,
                "title": axis_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "applyConditionalFormatting": True,
            "interactions": [],
        },
    }


def timeline_chart(step, title, show_legend=True, axis_title=""):
    """Build a timeline chart widget with native forecast band support.

    CRM Analytics timeline charts auto-detect timeseries _high_95/_low_95 suffixes
    and render them as shaded prediction interval bands. Auto-detect columnMap.
    SAQL must produce: date dimension, measure(s), and optionally forecast band columns.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "timeline",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "measureAxis1": {
                "showTitle": bool(axis_title),
                "showAxis": True,
                "title": axis_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def combo_chart(
    step,
    title,
    dim_fields,
    bar_measures,
    line_measures,
    show_legend=True,
    axis_title="",
    axis2_title="",
):
    """Build a combo (bar + line) chart widget.

    Uses columnMap + plotConfiguration array (production-verified format).
    Each bar measure renders as column, each line measure renders as line.
    """
    plot_config = [{"series": m, "chartType": "column"} for m in bar_measures] + [
        {"series": m, "chartType": "line"} for m in line_measures
    ]
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "combo",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "columnMap": {
                "dimensionAxis": dim_fields,
                "plots": bar_measures + line_measures,
                "trellis": [],
                "split": [],
            },
            "combo": {"plotConfiguration": plot_config},
            "dimensionAxis": {
                "showTitle": True,
                "showAxis": True,
                "title": "",
                "customSize": "auto",
                "icons": {
                    "useIcons": False,
                    "iconProps": {"fit": "cover", "column": "", "type": "round"},
                },
            },
            "measureAxis1": {
                "showTitle": bool(axis_title),
                "showAxis": True,
                "title": axis_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "measureAxis2": {
                "showTitle": bool(axis2_title),
                "showAxis": True,
                "title": axis2_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "applyConditionalFormatting": True,
            "interactions": [],
        },
    }


def scatter_chart(step, title, x_title="", y_title="", show_legend=True):
    """Build a scatter chart widget. Auto-detect columnMap.

    SAQL must produce: x_measure, y_measure, and optionally a dimension for color grouping.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "scatter",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "measureAxis1": {
                "showTitle": bool(x_title),
                "showAxis": True,
                "title": x_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "measureAxis2": {
                "showTitle": bool(y_title),
                "showAxis": True,
                "title": y_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "applyConditionalFormatting": True,
            "interactions": [],
        },
    }


def line_chart(step, title, show_legend=True, axis_title=""):
    """Build a line chart widget. Auto-detect columnMap.

    SAQL must produce: dimension, one or more measures.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "line",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "measureAxis1": {
                "showTitle": bool(axis_title),
                "showAxis": True,
                "title": axis_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
            },
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "applyConditionalFormatting": True,
            "interactions": [],
        },
    }


def heatmap_chart(step, title, show_legend=True):
    """Build a heatmap chart widget. Auto-detect columnMap.

    SAQL must produce: row_field, column_field, measure_field.
    CRM Analytics auto-maps first dim to rows, second to columns, measure to color.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "heatmap",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "legend": {
                "show": show_legend,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "interactions": [],
        },
    }


def bullet_chart(step, title, axis_title=""):
    """Build a bullet chart widget for target-vs-actual KPIs.

    SAQL must produce: measure (actual), target, and optionally poor/good range markers.
    CRM Analytics auto-detects the measure/comparative mapping.
    chartType = "bullet", auto-detect columnMap.
    """
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "bullet",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "exploreLink": True,
            "showActionMenu": True,
            "autoFitMode": "fit",
            "legend": {
                "show": True,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
            },
            "axisMode": "sync",
            "measureAxis1": {
                "showTitle": bool(axis_title),
                "customDomain": {"showMin": False, "showMax": False},
                "title": axis_title,
                "showAxis": True,
            },
            "interactions": [],
        },
    }


def listselector(step, title, compact=True, measure="sum_acv"):
    """Build a listselector filter widget."""
    return {
        "type": "listselector",
        "parameters": {
            "step": step,
            "title": title,
            "compact": compact,
            "expanded": False,
            "instant": True,
            "exploreLink": False,
            "showActionMenu": False,
            "measureField": measure,
            "filterStyle": {},
            "interactions": [],
        },
    }


def pillbox(step, title, measure=""):
    """Build a compact pill-style filter widget. Empty measure = count."""
    return {
        "type": "listselector",
        "parameters": {
            "step": step,
            "title": title,
            "compact": True,
            "expanded": True,
            "instant": True,
            "exploreLink": False,
            "showActionMenu": False,
            "measureField": measure,
            "filterStyle": {},
            "interactions": [],
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Layout helpers
# ═══════════════════════════════════════════════════════════════════════════


def hdr(line1, line2=""):
    """Build a header text widget."""
    items = [
        {
            "attributes": {"size": "24px", "color": "#091A3E", "bold": True},
            "insert": line1,
        },
        {"attributes": {"align": "left"}, "insert": "\n"},
    ]
    if line2:
        items += [
            {"attributes": {"size": "14px", "color": "#54698D"}, "insert": line2},
            {"attributes": {"align": "left"}, "insert": "\n"},
        ]
    return {
        "type": "text",
        "parameters": {
            "content": {"richTextContent": items},
            "interactions": [],
        },
    }


def section_label(text):
    """Build a section label text widget."""
    return {
        "type": "text",
        "parameters": {
            "content": {
                "richTextContent": [
                    {
                        "attributes": {
                            "size": "16px",
                            "color": "#091A3E",
                            "bold": True,
                        },
                        "insert": text,
                    },
                    {"attributes": {"align": "left"}, "insert": "\n"},
                ]
            },
            "interactions": [],
        },
    }


def nav_link(page, text, active=False):
    """Build a link widget for page navigation."""
    return {
        "type": "link",
        "parameters": {
            "destinationType": "page",
            "destinationLink": {"name": page},
            "fontSize": 14,
            "includeState": False,
            "text": text,
            "textAlignment": "center",
            "textColor": "#091A3E" if active else "#0070D2",
        },
    }


def set_record_links_xmd(inst, tok, dataset_name, link_configs):
    """Set XMD recordIdField on dataset fields for Salesforce record navigation.

    Uses the native CRM Analytics action menu (recordIdField + salesforceActionsEnabled)
    so clicking a dimension value in a table shows "Open Record" etc.

    Args:
        inst: Salesforce instance URL
        tok: Access token
        dataset_name: API name of the dataset (e.g. 'Opp_Mgmt_KPIs')
        link_configs: list of dicts with keys:
            - field: dataset field name to make clickable (e.g. 'Name')
            - id_field: dataset field containing the Salesforce record ID (e.g. 'Id')
    """
    # 1. Find dataset + current version
    ds_list = _sf_api(
        inst,
        tok,
        "GET",
        f"/services/data/v66.0/wave/datasets?q={dataset_name}",
    )
    ds = None
    for d in ds_list.get("datasets", []):
        if d.get("name") == dataset_name:
            ds = d
            break
    if not ds:
        print(f"  XMD: dataset '{dataset_name}' not found — skipping record links")
        return
    ds_id = ds["id"]
    vid = ds.get("currentVersionId", "")
    if not vid:
        print(f"  XMD: no current version for '{dataset_name}' — skipping")
        return

    # 2. Read existing XMD to preserve measures/other dimensions
    xmd_read = f"/services/data/v66.0/wave/datasets/{ds_id}/versions/{vid}/xmds/main"
    xmd_write = f"/services/data/v66.0/wave/datasets/{ds_id}/versions/{vid}/xmds/user"
    xmd = _sf_api(inst, tok, "GET", xmd_read)

    # Preserve existing measures (only writable fields)
    clean_measures = []
    for m in xmd.get("measures", []):
        cm = {"field": m["field"], "label": m.get("label", m["field"])}
        if m.get("format"):
            cm["format"] = m["format"]
        clean_measures.append(cm)

    # Build action-enabled dimensions from config
    link_fields = {c["field"] for c in link_configs}
    dimensions = []
    for cfg in link_configs:
        dimensions.append(
            {
                "field": cfg["field"],
                "label": cfg.get("label", cfg["field"]),
                "linkTemplateEnabled": True,
                "recordIdField": cfg["id_field"],
                "salesforceActionsEnabled": True,
                "customActionsEnabled": True,
                "showInExplorer": True,
            }
        )

    # Preserve existing dimensions that we're not overwriting
    for d in xmd.get("dimensions", []):
        if d["field"] not in link_fields:
            dim = {"field": d["field"], "label": d.get("label", d["field"])}
            if d.get("recordIdField"):
                dim["recordIdField"] = d["recordIdField"]
                dim["linkTemplateEnabled"] = d.get("linkTemplateEnabled", False)
                dim["salesforceActionsEnabled"] = d.get(
                    "salesforceActionsEnabled", False
                )
            dimensions.append(dim)

    body = {
        "dimensions": dimensions,
        "measures": clean_measures,
        "derivedDimensions": [],
        "derivedMeasures": [],
        "dates": [],
        "organizations": [],
        "showDetailsDefaultFields": [],
    }

    # 3. PUT updated XMD (non-fatal)
    try:
        _sf_api(inst, tok, "PUT", xmd_write, body)
        fields_str = ", ".join(c["field"] for c in link_configs)
        print(f"  XMD: record actions applied on {dataset_name} [{fields_str}]")
    except RuntimeError as e:
        print(f"  XMD WARNING ({dataset_name}): {e}")


def set_security_predicate(inst, tok, dataset_name,
                           predicate="'OwnerId' == \"$User.Id\""):
    """Set a row-level security predicate on a CRM Analytics dataset.

    Args:
        inst: Salesforce instance URL
        tok: Access token
        dataset_name: API name of the dataset
        predicate: SAQL security predicate expression
    """
    ds_list = _sf_api(
        inst, tok, "GET",
        f"/services/data/v66.0/wave/datasets?q={dataset_name}",
    )
    ds = None
    for d in ds_list.get("datasets", []):
        if d.get("name") == dataset_name:
            ds = d
            break
    if not ds:
        print(f"  RLS: dataset '{dataset_name}' not found — skipping predicate")
        return
    ds_id = ds["id"]
    try:
        _sf_api(inst, tok, "PATCH",
                f"/services/data/v66.0/wave/datasets/{ds_id}",
                {"securityPredicate": predicate})
        print(f"  RLS: security predicate set on {dataset_name}")
    except RuntimeError as e:
        print(f"  RLS WARNING ({dataset_name}): {e}")


def pg(name, lbl, widgets):
    """Build a page dict for the grid layout."""
    return {"name": name, "label": lbl, "widgets": widgets}


def nav_row(prefix, count):
    """Generate nav bar layout positions across row 0 for `count` pages."""
    if count == 9:
        widths = [2, 1, 1, 1, 1, 1, 1, 2, 2]
    elif count == 8:
        widths = [2, 2, 1, 1, 2, 1, 1, 2]
    elif count == 7:
        widths = [2, 2, 2, 2, 1, 1, 2]
    elif count == 6:
        widths = [2, 2, 2, 2, 2, 2]
    elif count == 5:
        widths = [3, 2, 2, 3, 2]
    elif count == 4:
        widths = [3, 3, 3, 3]
    elif count == 3:
        widths = [4, 4, 4]
    else:
        w = 12 // count
        widths = [w] * count
        widths[-1] = 12 - w * (count - 1)

    result = []
    col = 0
    for i, w in enumerate(widths):
        result.append(
            {
                "name": f"{prefix}_nav{i + 1}",
                "row": 0,
                "column": col,
                "colspan": w,
                "rowspan": 1,
            }
        )
        col += w
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  Dashboard deployment
# ═══════════════════════════════════════════════════════════════════════════


def build_dashboard_state(steps, widgets, layout):
    """Build the complete dashboard state dict."""
    return {
        "steps": steps,
        "widgets": widgets,
        "gridLayouts": [layout],
        "widgetStyle": {
            "backgroundColor": "#FFFFFF",
            "borderColor": "#E6ECF2",
            "borderEdges": [],
            "borderRadius": 2,
            "borderWidth": 1,
        },
    }


def deploy_dashboard(inst, tok, dashboard_id, state):
    """Deploy dashboard state via PATCH to an existing dashboard."""
    body = json.dumps({"state": state})
    steps = state["steps"]
    widgets = state["widgets"]
    pages = state["gridLayouts"][0]["pages"]
    print(f"Payload: {len(body):,} bytes")
    print(f"  {len(steps)} steps | {len(widgets)} widgets | {len(pages)} pages")

    url = f"{inst}/services/data/v66.0/wave/dashboards/{dashboard_id}"
    req = urllib.request.Request(
        url,
        data=body.encode(),
        method="PATCH",
        headers={
            "Authorization": f"Bearer {tok}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req) as resp:
            r = json.loads(resp.read().decode())
            print(f"\nOK — {r.get('name')} updated")
            st = r.get("state", {})
            print(f"  Steps: {len(st.get('steps', {}))}")
            print(f"  Widgets: {len(st.get('widgets', {}))}")
            gl = st.get("gridLayouts", [{}])[0]
            for p in gl.get("pages", []):
                print(f"  Page '{p.get('label')}': {len(p.get('widgets', []))} widgets")
    except urllib.error.HTTPError as e:
        err = e.read().decode()
        print(f"FAIL (HTTP {e.code}): {err[:2000]}")


# ═══════════════════════════════════════════════════════════════════════════
#  Win Probability Scoring (Rule-Based)
# ═══════════════════════════════════════════════════════════════════════════

# Stage name → score (0-25 points): higher stages = more points
STAGE_SCORE = {
    "Stage 1 - Prospect Qualification": 3,
    "Stage 1.5 - Technical Qualification": 6,
    "Stage 2 - Approval": 9,
    "Stage 3 - Discovery": 13,
    "Stage 4 - Solution Validation": 16,
    "Stage 5 - Negotiation": 19,
    "Stage 6 - Verbal Agreement": 22,
    "Stage 7 - Closed Won": 25,
    "Closed Lost": 0,
    "Closed Won": 25,
}

# Forecast category → score (0-15 points)
FCAT_SCORE = {
    "Commit": 15,
    "BestCase": 10,
    "Best Case": 10,
    "Pipeline": 5,
    "Omitted": 0,
}


def precompute_scoring_stats(opps):
    """Pre-compute per-type win rates and average deal size from opp list.

    Returns (type_win_rates: dict, avg_deal_size: float).
    """
    type_won = {}
    type_closed = {}
    total_arr = 0
    arr_count = 0

    for o in opps:
        opp_type = o.get("Type") or ""
        is_closed = str(o.get("IsClosed", False)).lower() == "true"
        is_won = str(o.get("IsWon", False)).lower() == "true"
        arr = o.get("ConvertedARR") or o.get("APTS_Forecast_ARR__c") or 0

        if arr and arr > 0:
            total_arr += arr
            arr_count += 1

        if is_closed and opp_type:
            type_closed[opp_type] = type_closed.get(opp_type, 0) + 1
            if is_won:
                type_won[opp_type] = type_won.get(opp_type, 0) + 1

    type_win_rates = {}
    for t, closed in type_closed.items():
        won = type_won.get(t, 0)
        type_win_rates[t] = (won / closed * 100) if closed > 0 else 0

    avg_deal = (total_arr / arr_count) if arr_count > 0 else 0
    return type_win_rates, avg_deal


def compute_win_score(opp, type_win_rates, avg_deal_size):
    """Compute a deterministic win probability score (0-100) for an opportunity.

    Scoring factors:
      - Stage position: 0-25 pts
      - Days in stage: 0-15 pts (<30d=15, 30-60d=10, 60-90d=5, >90d=0)
      - Deal age: 0-10 pts (<90d=10, 90-180d=7, 180-365d=3, >365d=0)
      - Commercial approval: 0-10 pts (approved=10)
      - Forecast category: 0-15 pts (Commit=15, BestCase=10, Pipeline=5)
      - Historical type win rate: 0-10 pts (normalized)
      - Deal size vs avg: 0-10 pts (within 2x=10, 2-5x=5, >5x=2)
      - Close date in future: 0-5 pts (future=5, past=0)

    Returns (score: int, band: str).
    """
    score = 0

    # 1. Stage position (0-25)
    stage = opp.get("StageName") or ""
    score += STAGE_SCORE.get(stage, 0)

    # 2. Days in stage (0-15)
    days_in_stage = opp.get("LastStageChangeInDays") or opp.get("DaysInStage") or 0
    try:
        days_in_stage = int(float(days_in_stage))
    except (ValueError, TypeError):
        days_in_stage = 0
    if days_in_stage < 30:
        score += 15
    elif days_in_stage < 60:
        score += 10
    elif days_in_stage < 90:
        score += 5

    # 3. Deal age (0-10)
    age = opp.get("AgeInDays") or 0
    try:
        age = int(float(age))
    except (ValueError, TypeError):
        age = 0
    if age < 90:
        score += 10
    elif age < 180:
        score += 7
    elif age < 365:
        score += 3

    # 4. Commercial approval (0-10)
    approved = str(opp.get("Stage_20_Approval__c", False)).lower() == "true"
    if approved:
        score += 10

    # 5. Forecast category (0-15)
    fcat = opp.get("ForecastCategoryName") or ""
    score += FCAT_SCORE.get(fcat, 0)

    # 6. Historical type win rate (0-10)
    opp_type = opp.get("Type") or ""
    type_wr = type_win_rates.get(opp_type, 0)
    score += min(10, round(type_wr / 10))  # 100% → 10, 50% → 5, etc.

    # 7. Deal size vs average (0-10)
    arr = opp.get("ConvertedARR") or opp.get("APTS_Forecast_ARR__c") or 0
    try:
        arr = float(arr)
    except (ValueError, TypeError):
        arr = 0
    if avg_deal_size > 0 and arr > 0:
        ratio = arr / avg_deal_size
        if ratio <= 2:
            score += 10
        elif ratio <= 5:
            score += 5
        else:
            score += 2
    elif arr > 0:
        score += 5  # no avg available, neutral

    # 8. Close date in future (0-5)
    close_date = opp.get("CloseDate") or ""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if close_date and close_date >= today:
        score += 5

    # Clamp to 0-100
    score = max(0, min(100, score))

    # Band assignment
    if score >= 70:
        band = "High"
    elif score >= 40:
        band = "Medium"
    else:
        band = "Low"

    return score, band


# ═══════════════════════════════════════════════════════════════════════════
#  Filter & Trend helpers
# ═══════════════════════════════════════════════════════════════════════════


def coalesce_filter(step_name, field):
    """Return the SAQL binding string that wires a filter step to a query.

    When nothing is selected, coalesce falls through to result (passthrough).
    """
    return (
        f"q = filter q by {{{{coalesce("
        f'column({step_name}.selection, ["{field}"]), '
        f'column({step_name}.result, ["{field}"])'
        f").asEquality('{field}')}}}};\n"
    )


def filter_bar(page_prefix, filters_config, ds_meta, existing_steps=None):
    """Build filter steps, widgets, and layout entries for a standard 4-filter row.

    Args:
        page_prefix: e.g. "p1" — used to namespace widget names
        filters_config: list of (step_name, field, label) tuples, up to 4
        ds_meta: dataset metadata for af() steps
        existing_steps: dict of steps already defined (won't re-create)

    Returns:
        (steps_dict, widgets_dict, layout_list)
    """
    existing_steps = existing_steps or {}
    steps = {}
    widgets = {}
    layout = []
    col_width = 12 // max(len(filters_config), 1)

    for i, (step_name, field, label) in enumerate(filters_config):
        # Only add the step if not already defined
        if step_name not in existing_steps:
            steps[step_name] = af(field, ds_meta)
        widget_name = f"{page_prefix}_f_{field.lower()}"
        widgets[widget_name] = pillbox(step_name, label)
        layout.append(
            {
                "name": widget_name,
                "row": 3,
                "column": i * col_width,
                "colspan": col_width,
                "rowspan": 2,
            }
        )

    return steps, widgets, layout


def num_with_trend(
    step, value_field, title, color, trend_field="pct_change", compact=False, size=24
):
    """Number widget backed by a cogroup trend step.

    The step should produce `value_field` (current period) and `trend_field`
    (pct_change). The widget shows the current-period value. The pct_change
    field is available in the step for explorer drill-down.
    """
    return num(step, value_field, title, color, compact, size)


def trend_step(
    ds_name,
    base_filters,
    current_filter,
    prior_filter,
    group_field,
    measure_expr,
    measure_alias="current",
):
    """Build a cogroup SAQL step comparing current vs prior period.

    Args:
        ds_name: dataset name
        base_filters: common SAQL filter lines (string)
        current_filter: e.g. "q1 = filter q1 by FiscalYear == 2026;\\n"
        prior_filter: e.g. "q2 = filter q2 by FiscalYear == 2025;\\n"
        group_field: field to group by, or "all" for scalar KPI
        measure_expr: e.g. "sum(ARR)"
    """
    group_clause = f"by {group_field}" if group_field != "all" else "by all"
    dim_select = f"q1.{group_field} as {group_field}, " if group_field != "all" else ""

    # After cogroup, stream-prefixed fields must be wrapped in aggregate functions.
    # Use sum() since each stream produces exactly one row per group key.
    cur = f"sum(q1.{measure_alias})"
    pri = "sum(q2.prior)"
    q = (
        f'q1 = load "{ds_name}";\n'
        f"{base_filters.replace('q =', 'q1 =').replace('q by', 'q1 by').replace('q generate', 'q1 generate')}"
        f"{current_filter.replace('q =', 'q1 =').replace('q by', 'q1 by')}"
        f"q1 = group q1 {group_clause};\n"
        f"q1 = foreach q1 generate {dim_select}{measure_expr} as {measure_alias};\n"
        f'q2 = load "{ds_name}";\n'
        f"{base_filters.replace('q =', 'q2 =').replace('q by', 'q2 by').replace('q generate', 'q2 generate')}"
        f"{prior_filter.replace('q =', 'q2 =').replace('q by', 'q2 by')}"
        f"q2 = group q2 {group_clause};\n"
        f"q2 = foreach q2 generate {dim_select}{measure_expr} as prior;\n"
        f"q = cogroup q1 {group_clause}, q2 {group_clause};\n"
        f"q = foreach q generate {dim_select}"
        f"coalesce({cur}, 0) as {measure_alias}, "
        f"coalesce({pri}, 0) as prior, "
        f"(case when coalesce({pri}, 0) > 0 then "
        f"((coalesce({cur}, 0) - coalesce({pri}, 0)) / coalesce({pri}, 0)) * 100 "
        f"else 0 end) as pct_change;"
    )
    return sq(q)


def add_reference_line(widget, value, label, color="#D4504C", style="dashed"):
    """Add a reference line to an existing chart widget.

    Modifies the widget dict in-place and returns it.
    """
    # NOTE: referenceLines not yet supported in Wave dashboard JSON API.
    # Kept as no-op; reference lines can be added via CRM Analytics UI.
    return widget


def add_table_action(
    widget, action_type="salesforceActions", object_name="Opportunity", id_field="Id"
):
    """Add View Record / Create Task actions to a comparisontable widget.

    Modifies the widget dict in-place and returns it.
    """
    params = widget.get("parameters", widget)
    if "interactions" not in params:
        params["interactions"] = []

    params["interactions"].append(
        {
            "type": action_type,
            "group": "main",
            "columns": [],
            "enabled": True,
            "options": {
                "cell": {
                    "enabled": True,
                    "actions": [
                        {
                            "enabled": True,
                            "type": "recordAction",
                            "label": "View Record",
                            "actionName": "record",
                            "objectApiName": object_name,
                            "recordIdColumn": id_field,
                        },
                        {
                            "enabled": True,
                            "type": "salesforceAction",
                            "label": "Create Task",
                            "actionName": "NewTask",
                            "objectApiName": "Task",
                            "recordIdColumn": id_field,
                        },
                    ],
                },
            },
        }
    )
    return widget


def add_selection_interaction(widget, source_step, source_field, target_steps):
    """Add a selection interaction to a widget for cross-step/cross-dataset binding.

    When a user clicks/selects on this widget, the selection value is broadcast
    to the target steps, enabling cross-dataset filtering and dynamic queries.

    Args:
        widget: widget dict to modify in-place
        source_step: step name that provides the selection
        source_field: field name to broadcast
        target_steps: list of step names to receive the selection

    Returns the modified widget.
    """
    params = widget.get("parameters", widget)
    if "interactions" not in params:
        params["interactions"] = []

    params["interactions"].append({
        "type": "selectionInteraction",
        "group": "selection",
        "enabled": True,
        "source": {"step": source_step, "field": source_field},
        "targets": [{"step": t, "field": source_field} for t in target_steps],
    })
    return widget


def add_results_interaction(widget, source_step, source_field, target_property, rules):
    """Add a results interaction for dynamic widget property updates.

    Enables data-driven dynamic formatting: e.g. change numberColor, title,
    or other widget properties based on query result values.

    Args:
        widget: widget dict to modify in-place
        source_step: step name providing the data
        source_field: field name to evaluate
        target_property: widget property to set (e.g. "numberColor", "titleColor")
        rules: list of dicts with {condition: {operator, value}, action: {value}}

    Returns the modified widget.
    """
    params = widget.get("parameters", widget)
    if "interactions" not in params:
        params["interactions"] = []

    formatted_rules = []
    for rule in rules:
        formatted_rules.append({
            "condition": rule["condition"],
            "action": {
                "type": "setProperty",
                "property": target_property,
                "value": rule["action"]["value"],
            },
        })

    params["interactions"].append({
        "type": "resultsInteraction",
        "group": f"dynamic_{target_property}",
        "enabled": True,
        "source": {"step": source_step, "field": source_field},
        "rules": formatted_rules,
    })
    return widget


def add_initial_selection(widget, step, field, values):
    """Set a default/initial selection on a filter or chart widget.

    Useful for setting dashboard defaults (e.g., current quarter, logged-in
    user's unit group) so the dashboard loads with context.

    Args:
        widget: widget dict to modify in-place
        step: step name to set selection on
        field: field name for the selection
        values: list of default values to select

    Returns the modified widget.
    """
    params = widget.get("parameters", widget)
    if "interactions" not in params:
        params["interactions"] = []

    params["interactions"].append({
        "type": "initialSelection",
        "group": "init",
        "enabled": True,
        "step": step,
        "selections": [{"field": field, "values": values}],
    })
    return widget


def cohort_heatmap_step(ds_name, cohort_field, age_field, measure_expr,
                        measure_alias="conversion_rate", base_filters=""):
    """Build a SAQL step for cohort/retention heatmap visualization.

    Creates a grid: cohort_field (rows) x age_field (columns) with color = measure.
    Commonly used for lead conversion cohorts, renewal retention, etc.

    Args:
        ds_name: dataset name
        cohort_field: field for cohort grouping (e.g. CreatedMonth)
        age_field: field for age/time bucket (e.g. WeeksSinceCreated)
        measure_expr: SAQL measure expression (e.g. "avg(ConversionRate) * 100")
        measure_alias: output field name for the measure
        base_filters: additional SAQL filter lines

    Returns a SAQL step dict.
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by ('{cohort_field}', '{age_field}');\n"
        f"q = foreach q generate "
        f"'{cohort_field}' as '{cohort_field}', "
        f"'{age_field}' as '{age_field}', "
        f"{measure_expr} as '{measure_alias}';\n"
        f"q = order q by ('{cohort_field}' asc, '{age_field}' asc);"
    )
    return sq(q)


def nrr_bridge_step(ds_name, base_filters="", group_field="all"):
    """Build a SAQL step for Net Revenue Retention (NRR) bridge waterfall.

    Computes: Starting ARR → Renewed → Expanded → Churned → Ending ARR
    as waterfall components. Suitable for waterfall_chart().

    Args:
        ds_name: dataset name with motion/renewal fields
        base_filters: SAQL filter lines
        group_field: field for segmentation (or "all" for total)

    Returns a SAQL step dict.
    """
    group_clause = f"by '{group_field}'" if group_field != "all" else "by all"
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q {group_clause};\n"
        f"q = foreach q generate "
        f"sum(case when IsRenewal == \"true\" && IsWon == \"true\" then ARR else 0 end) as renewed_arr, "
        f"sum(case when IsExpand == \"true\" && IsWon == \"true\" then ARR else 0 end) as expanded_arr, "
        f"sum(case when IsClosed == \"true\" && IsWon == \"false\" && IsRenewal == \"true\" then ARR else 0 end) as churned_arr;\n"
        f'r = load "{ds_name}";\n'
        f"{base_filters.replace('q', 'r')}"
        f"r = group r {group_clause.replace('q', 'r')};\n"
        f"r = foreach r generate "
        f"sum(case when IsRenewal == \"true\" then ARR else 0 end) as starting_arr;\n"
        f'q = union q, r;\n'
    )
    return sq(q)


def stage_transition_step(ds_name, base_filters=""):
    """Build a SAQL step for stage-to-stage transition Sankey.

    Creates source→target→count rows from stage hit flags (HitStage1..6).
    Suitable for sankey_chart().

    Args:
        ds_name: dataset name with HitStage1-6 and StageName fields
        base_filters: SAQL filter lines

    Returns a SAQL step dict.
    """
    # Build UNION of stage transitions using HitStage flags
    transitions = []
    stage_names = [
        ("HitStage1", "HitStage2", "Stage 1", "Stage 2"),
        ("HitStage2", "HitStage3", "Stage 2", "Stage 3"),
        ("HitStage3", "HitStage4", "Stage 3", "Stage 4"),
        ("HitStage4", "HitStage5", "Stage 4", "Stage 5"),
        ("HitStage5", "HitStage6", "Stage 5", "Stage 6"),
    ]
    parts = []
    for i, (from_flag, to_flag, from_label, to_label) in enumerate(stage_names):
        alias = f"q{i}"
        part = (
            f'{alias} = load "{ds_name}";\n'
            f"{base_filters.replace('q =', f'{alias} =').replace('q by', f'{alias} by')}"
            f'{alias} = filter {alias} by {from_flag} == "true" && {to_flag} == "true";\n'
            f"{alias} = group {alias} by all;\n"
            f'{alias} = foreach {alias} generate '
            f'"{from_label}" as source, '
            f'"{to_label}" as target, '
            f"count() as cnt;\n"
        )
        parts.append(part)

    q = "".join(parts)
    # Union all transition segments
    q += "q = union q0, q1, q2, q3, q4;\n"
    q += "q = order q by source asc;"
    return sq(q)


def compliance_scorecard_step(ds_name, base_filters=""):
    """Build a SAQL step producing compliance KPI metrics for scorecard.

    Computes: stuck_rate, pastdue_pct, close_date_change_rate, avg_days_in_stage.
    Suitable for num_dynamic_color() tiles.

    Args:
        ds_name: dataset name with DaysInStage, close date fields
        base_filters: SAQL filter lines

    Returns a SAQL step dict.
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f'q = filter q by IsClosed == "false";\n'
        f"q = group q by all;\n"
        f"q = foreach q generate "
        f"(sum(case when DaysInStage > 30 then 1 else 0 end) / count() * 100) as stuck_rate, "
        f"avg(DaysInStage) as avg_days_in_stage, "
        f"count() as total_open;"
    )
    return sq(q)


def renewal_timeline_step(ds_name, date_field="EndDate_Month", base_filters=""):
    """Build a SAQL step for renewal timeline visualization.

    Groups contracts by expiry month showing ARR at risk vs covered.
    Suitable for timeline_chart() or combo_chart().

    Args:
        ds_name: dataset name with contract end date and ARR fields
        date_field: month-level date field for grouping
        base_filters: SAQL filter lines

    Returns a SAQL step dict.
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by '{date_field}';\n"
        f"q = foreach q generate "
        f"'{date_field}' as '{date_field}', "
        f"sum(ARR) as total_arr, "
        f"sum(case when RenewalWindow == \"0 Days\" || RenewalWindow == \"1-30 Days\" then ARR else 0 end) as urgent_arr, "
        f"sum(case when DaysToExpiry > 90 then ARR else 0 end) as covered_arr, "
        f"count() as contract_count;\n"
        f"q = order q by '{date_field}' asc;\n"
        f"q = limit q 24;"
    )
    return sq(q)


def health_transition_step(ds_name, base_filters=""):
    """Build a SAQL step for health band transition Sankey.

    Creates prior_band→current_band→count for visualizing account health movement.
    Requires dataset with PriorHealthBand and HealthBand fields.
    Suitable for sankey_chart().

    Args:
        ds_name: dataset name with health band fields
        base_filters: SAQL filter lines

    Returns a SAQL step dict.
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by (PriorHealthBand, HealthBand);\n"
        f"q = foreach q generate "
        f"PriorHealthBand as source, "
        f"HealthBand as target, "
        f"count() as cnt;\n"
        f"q = order q by source asc;"
    )
    return sq(q)


def motion_flow_step(ds_name, base_filters=""):
    """Build a SAQL step for revenue motion flow Sankey.

    Creates motion→stage/outcome flow visualization.
    Suitable for sankey_chart().

    Args:
        ds_name: dataset name with motion flags (IsLand, IsExpand, IsRenewal)
        base_filters: SAQL filter lines

    Returns a SAQL step dict.
    """
    parts = []
    for i, (flag, label) in enumerate([
        ("IsLand", "Land"), ("IsExpand", "Expand"), ("IsRenewal", "Renewal"),
    ]):
        alias = f"q{i}"
        part = (
            f'{alias} = load "{ds_name}";\n'
            f"{base_filters.replace('q =', f'{alias} =').replace('q by', f'{alias} by')}"
            f'{alias} = filter {alias} by {flag} == "true";\n'
            f"{alias} = group {alias} by StageName;\n"
            f'{alias} = foreach {alias} generate '
            f'"{label}" as source, '
            f"StageName as target, "
            f"sum(ARR) as arr;\n"
        )
        parts.append(part)
    q = "".join(parts)
    q += "q = union q0, q1, q2;\n"
    q += "q = order q by source asc, arr desc;"
    return sq(q)


def shift_layout_rows(layout_entries, row_offset=2, skip_rows=None):
    """Shift all layout entries down by row_offset rows.

    Skips entries at rows in skip_rows (e.g. {0} for nav bar, {1} for header).
    Returns a new list.
    """
    skip_rows = skip_rows or {0}
    result = []
    for entry in layout_entries:
        e = dict(entry)
        if e["row"] not in skip_rows:
            e["row"] = e["row"] + row_offset
        result.append(e)
    return result


# ═══════════════════════════════════════════════════════════════════════════
#  Dataflow helpers
# ═══════════════════════════════════════════════════════════════════════════


def create_dataflow(inst, tok, name, definition, app_name=APP_NAME):
    """Create or update a CRM Analytics dataflow definition.

    Args:
        inst: instance URL
        tok: access token
        name: dataflow API name
        definition: dict with the dataflow node definitions

    Returns:
        dataflow ID
    """
    # Check if dataflow exists
    result = _sf_api(inst, tok, "GET", f"/services/data/v66.0/wave/dataflows?q={name}")
    for df in result.get("dataflows", []):
        if df.get("name") == name:
            df_id = df["id"]
            _sf_api(
                inst,
                tok,
                "PATCH",
                f"/services/data/v66.0/wave/dataflows/{df_id}",
                {"definition": definition},
            )
            print(f"  Updated dataflow: {df_id} ({name})")
            return df_id

    # Create new
    body = {
        "name": name,
        "label": name.replace("_", " "),
        "description": f"Auto-generated dataflow: {name}",
        "definition": definition,
    }

    result = _sf_api(inst, tok, "POST", "/services/data/v66.0/wave/dataflows", body)
    df_id = result["id"]
    print(f"  Created dataflow: {df_id} ({name})")
    return df_id


def run_dataflow(inst, tok, dataflow_id, poll_attempts=60, poll_interval=10):
    """Trigger a dataflow run and poll until completion.

    Returns True if successful, False otherwise.
    """
    result = _sf_api(
        inst,
        tok,
        "POST",
        "/services/data/v66.0/wave/dataflowjobs",
        {"dataflowId": dataflow_id, "command": "Start"},
    )
    job_id = result.get("id")
    if not job_id:
        print("  Failed to start dataflow job")
        return False
    print(f"  Dataflow job started: {job_id}")

    for _ in range(poll_attempts):
        time.sleep(poll_interval)
        job = _sf_api(
            inst, tok, "GET", f"/services/data/v66.0/wave/dataflowjobs/{job_id}"
        )
        status = job.get("status", "")
        if status == "Success":
            print("  Dataflow job completed successfully")
            return True
        if status in ("Failure", "Error"):
            msg = job.get("message", "Unknown error")
            print(f"  Dataflow job FAILED: {msg}")
            return False
        print(f"  ... {status}")

    print("  Timed out waiting for dataflow job")
    return False


def create_dashboard_if_needed(inst, tok, label, app_name=APP_NAME):
    """Create a new CRM Analytics dashboard, or find existing by label.

    Returns the dashboard ID.
    """
    encoded_label = urllib.request.quote(label)
    result = _sf_api(
        inst,
        tok,
        "GET",
        f"/services/data/v66.0/wave/dashboards?q={encoded_label}",
    )
    for d in result.get("dashboards", []):
        if d.get("label") == label:
            print(f"  Found existing dashboard: {d['id']} ({label})")
            return d["id"]

    # Look up the app folder (list all — q= search doesn't match underscores)
    folder_result = _sf_api(
        inst,
        tok,
        "GET",
        "/services/data/v66.0/wave/folders",
    )
    folder_id = None
    for f in folder_result.get("folders", []):
        if f.get("name") == app_name or f.get("label") == app_name:
            folder_id = f["id"]
            break

    body = {
        "label": label,
        "description": f"Auto-generated: {label}",
        "state": {
            "steps": {},
            "widgets": {},
            "gridLayouts": [{"name": "Default", "pages": []}],
        },
    }
    if folder_id:
        body["folder"] = {"id": folder_id}
    result = _sf_api(
        inst,
        tok,
        "POST",
        "/services/data/v66.0/wave/dashboards",
        body,
    )
    dashboard_id = result["id"]
    print(f"  Created new dashboard: {dashboard_id} ({label})")
    return dashboard_id


# ═══════════════════════════════════════════════════════════════════════════
#  Orchestration helpers (used by deploy_orchestrator.py & smoke_runner.py)
# ═══════════════════════════════════════════════════════════════════════════


class _ApiCheck:
    """Simple result wrapper for validate_api_version."""

    def __init__(self, ok, message=""):
        self.ok = ok
        self.message = message

    def __bool__(self):
        return self.ok


def get_dashboard_id(inst, tok, label):
    """Look up a dashboard ID by its label. Returns the ID string or None."""
    encoded = urllib.request.quote(label)
    result = _sf_api(
        inst,
        tok,
        "GET",
        f"/services/data/v66.0/wave/dashboards?q={encoded}",
    )
    for d in result.get("dashboards", []):
        if d.get("label") == label:
            return d["id"]
    return None


def get_dashboard_state(inst, tok, dashboard_id):
    """Fetch the full dashboard JSON (including state) by ID."""
    return _sf_api(
        inst,
        tok,
        "GET",
        f"/services/data/v66.0/wave/dashboards/{dashboard_id}",
    )


def validate_api_version(inst, tok):
    """Check that the org supports our target API version (v66.0).

    Returns an _ApiCheck (truthy if OK, falsy with .message if not).
    """
    try:
        versions = _sf_api(inst, tok, "GET", "/services/data/")
        known = {v.get("version") for v in versions}
        if "66.0" in known:
            return _ApiCheck(True)
        latest = max(known, default="unknown")
        return _ApiCheck(
            False,
            f"Org does not advertise API v66.0. Latest: {latest}",
        )
    except Exception as exc:
        return _ApiCheck(False, f"Version check failed: {exc}")


def _resolve_dataset_refs(inst, tok, query):
    """Replace bare dataset names in SAQL load statements with id/versionId.

    The Wave query API requires fully-qualified dataset references
    (``dataset_id/version_id``) when executing SAQL via REST.  Dashboard
    steps resolve names at render time, but standalone queries do not.
    """
    import re as _re

    _ds_cache = {}

    def _replacer(m):
        name = m.group(1)
        if name in _ds_cache:
            return f'load "{_ds_cache[name]}"'
        ds_id = get_dataset_id(inst, tok, name)
        if not ds_id:
            return m.group(0)  # leave unchanged
        ds = _sf_api(inst, tok, "GET", f"/services/data/v66.0/wave/datasets/{ds_id}")
        ver = ds.get("currentVersionId", "")
        ref = f"{ds_id}/{ver}" if ver else ds_id
        _ds_cache[name] = ref
        return f'load "{ref}"'

    return _re.sub(r'load\s+"([^"/]+)"', _replacer, query)


def execute_query(inst, tok, query, language="SAQL"):
    """Execute a SAQL or SQL query against the Wave query API.

    Automatically resolves bare dataset names in SAQL ``load`` statements
    to their ``id/versionId`` form so queries work via REST.

    Returns the parsed JSON response body.
    """
    resolved = (
        _resolve_dataset_refs(inst, tok, query) if language.upper() != "SQL" else query
    )
    body = {"query": resolved}
    if language.upper() == "SQL":
        body["queryLanguage"] = "Sql"
    return _sf_api(
        inst,
        tok,
        "POST",
        "/services/data/v66.0/wave/query",
        body,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  ML & Quantitative Analytics Foundation
# ═══════════════════════════════════════════════════════════════════════════


def arr_bridge_fields():
    """Return standard metadata fields for an ARR bridge dataset.

    Grain: month × customer × product × segment × region.
    Components: StartARR, NewARR, ExpansionARR, ContractionARR, ChurnARR, EndARR.
    """
    return [
        _dim("AccountId", "Account ID"),
        _dim("AccountName", "Account Name"),
        _dim("ProductFamily", "Product Family"),
        _dim("Segment", "Segment"),
        _dim("SalesRegion", "Sales Region"),
        _dim("UnitGroup", "Unit Group"),
        _dim("BridgeMonth", "Bridge Month"),
        _dim("BridgeComponent", "Bridge Component"),
        _dim("FYLabel", "Fiscal Year"),
        _measure("StartARR", "Starting ARR"),
        _measure("NewARR", "New ARR"),
        _measure("ExpansionARR", "Expansion ARR"),
        _measure("ContractionARR", "Contraction ARR"),
        _measure("ChurnARR", "Churn ARR"),
        _measure("EndARR", "Ending ARR"),
        _measure("NetNewARR", "Net New ARR"),
        _measure("GrowthRatePct", "Growth Rate %", scale=1, precision=5),
    ]


def cohort_retention_fields():
    """Return standard metadata fields for a cohort retention dataset.

    Grain: cohort_quarter × age_months × dimensions.
    Supports GRR/NRR heatmaps and retention curves.
    """
    return [
        _dim("CohortQuarter", "Cohort Quarter"),
        _dim("ProductFamily", "Product Family"),
        _dim("Segment", "Segment"),
        _dim("SalesRegion", "Sales Region"),
        _dim("UnitGroup", "Unit Group"),
        _measure("AgeMonths", "Age (Months)", scale=0, precision=5),
        _measure("CohortSize", "Cohort Size", scale=0, precision=10),
        _measure("RetainedCount", "Retained Count", scale=0, precision=10),
        _measure("RetainedARR", "Retained ARR"),
        _measure("ExpandedARR", "Expanded ARR"),
        _measure("ChurnedARR", "Churned ARR"),
        _measure("GRR", "Gross Retention Rate %", scale=1, precision=5),
        _measure("NRR", "Net Retention Rate %", scale=1, precision=5),
    ]


def ml_prediction_fields(prefix="Churn"):
    """Return metadata fields for ML prediction outputs.

    Generates standardized prediction + driver fields for any model type.
    Use prefix="Churn", "Expansion", "Renewal", "WinProb", etc.

    Args:
        prefix: model name prefix for field naming

    Returns:
        list of field metadata dicts
    """
    return [
        _measure(f"{prefix}Probability", f"{prefix} Probability %", scale=1, precision=5),
        _dim(f"{prefix}RiskBand", f"{prefix} Risk Band"),
        _dim(f"{prefix}Driver1", f"{prefix} Top Driver"),
        _dim(f"{prefix}Driver2", f"{prefix} Second Driver"),
        _measure(f"{prefix}Score", f"{prefix} Score", scale=0, precision=5),
    ]


def forecast_snapshot_fields():
    """Return metadata fields for a forecast snapshot dataset.

    Grain: snapshot_date × rep × fiscal_period × category.
    Enables forecast error, bias, and accuracy analytics.
    """
    return [
        _dim("SnapshotDate", "Snapshot Date"),
        _dim("OwnerName", "Owner Name"),
        _dim("FiscalQuarter", "Fiscal Quarter"),
        _dim("FYLabel", "Fiscal Year"),
        _dim("UnitGroup", "Unit Group"),
        _dim("ForecastCategory", "Forecast Category"),
        _measure("CommitARR", "Commit ARR"),
        _measure("BestCaseARR", "Best Case ARR"),
        _measure("PipelineARR", "Pipeline ARR"),
        _measure("ClosedWonARR", "Closed Won ARR"),
        _measure("WeightedForecast", "Weighted Forecast"),
        _measure("QuotaAmount", "Quota"),
        _measure("ForecastP50", "Forecast P50"),
        _measure("ForecastP90", "Forecast P90"),
        _measure("ForecastP50_high_95", "P50 Upper Band"),
        _measure("ForecastP50_low_95", "P50 Lower Band"),
    ]


def forecast_error_fields():
    """Return metadata fields for forecast error analytics.

    Computed from snapshot vs actuals comparison.
    """
    return [
        _dim("OwnerName", "Owner Name"),
        _dim("FiscalQuarter", "Fiscal Quarter"),
        _dim("UnitGroup", "Unit Group"),
        _dim("Segment", "Segment"),
        _dim("ProductFamily", "Product Family"),
        _dim("SalesRegion", "Sales Region"),
        _measure("ForecastARR", "Forecast ARR"),
        _measure("ActualARR", "Actual ARR"),
        _measure("AbsoluteError", "Absolute Error"),
        _measure("ErrorPct", "Error %", scale=1, precision=5),
        _measure("BiasPct", "Bias %", scale=1, precision=5),
        _measure("MAPE", "MAPE %", scale=1, precision=5),
        _measure("WAPE", "WAPE %", scale=1, precision=5),
    ]


# ─── ARR bridge SAQL helpers ──────────────────────────────────────────────


def arr_bridge_waterfall_step(ds_name, base_filters="", group_field="all"):
    """Build a SAQL step for ARR bridge waterfall visualization.

    Produces bridge components (New, Expansion, Contraction, Churn)
    suitable for waterfall_chart().

    Args:
        ds_name: ARR bridge dataset name
        base_filters: SAQL filter lines
        group_field: optional segmentation field
    """
    group_clause = f"by '{group_field}'" if group_field != "all" else "by all"
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q {group_clause};\n"
        f"q = foreach q generate "
        f"sum(NewARR) as new_arr, "
        f"sum(ExpansionARR) as expansion_arr, "
        f"sum(ContractionARR) * -1 as contraction_arr, "
        f"sum(ChurnARR) * -1 as churn_arr, "
        f"sum(NetNewARR) as net_new_arr;"
    )
    return sq(q)


def arr_bridge_trend_step(ds_name, base_filters=""):
    """Build a SAQL step for monthly ARR bridge trend (stacked area).

    Shows bridge components over time for trend analysis.
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by BridgeMonth;\n"
        f"q = foreach q generate "
        f"BridgeMonth as BridgeMonth, "
        f"sum(NewARR) as new_arr, "
        f"sum(ExpansionARR) as expansion_arr, "
        f"sum(ContractionARR) * -1 as contraction_arr, "
        f"sum(ChurnARR) * -1 as churn_arr, "
        f"sum(EndARR) as end_arr;\n"
        f"q = order q by BridgeMonth asc;\n"
        f"q = limit q 24;"
    )
    return sq(q)


def growth_cube_step(ds_name, dim1, dim2, base_filters=""):
    """Build a SAQL step for growth cube heatmap (product × segment × region).

    Creates a 2D grid with growth rate as the color dimension.
    Suitable for heatmap_chart().

    Args:
        ds_name: ARR bridge or growth cube dataset name
        dim1: row dimension (e.g. ProductFamily)
        dim2: column dimension (e.g. SalesRegion)
        base_filters: SAQL filter lines
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by ('{dim1}', '{dim2}');\n"
        f"q = foreach q generate "
        f"'{dim1}' as '{dim1}', "
        f"'{dim2}' as '{dim2}', "
        f"(case when sum(StartARR) > 0 then "
        f"((sum(EndARR) - sum(StartARR)) / sum(StartARR)) * 100 "
        f"else 0 end) as growth_rate_pct, "
        f"sum(NetNewARR) as net_new_arr, "
        f"sum(EndARR) as end_arr;\n"
        f"q = order q by '{dim1}' asc, '{dim2}' asc;"
    )
    return sq(q)


def cohort_retention_step(ds_name, base_filters="", measure="NRR"):
    """Build a SAQL step for cohort retention heatmap.

    Creates CohortQuarter × AgeMonths grid with GRR or NRR as color.
    Suitable for heatmap_chart().

    Args:
        ds_name: cohort retention dataset name
        base_filters: SAQL filter lines
        measure: "NRR" or "GRR"
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by (CohortQuarter, AgeMonths);\n"
        f"q = foreach q generate "
        f"CohortQuarter as CohortQuarter, "
        f"AgeMonths as AgeMonths, "
        f"avg({measure}) as {measure.lower()}_pct;\n"
        f"q = order q by CohortQuarter asc, AgeMonths asc;"
    )
    return sq(q)


# ─── Forecast analytics SAQL helpers ─────────────────────────────────────


def forecast_bands_step(ds_name, base_filters=""):
    """Build a SAQL step for forecast timeline with prediction bands.

    Produces columns for timeline_chart() with _high_95/_low_95 suffixes
    that render as shaded prediction interval bands.

    Args:
        ds_name: forecast snapshot dataset name
        base_filters: SAQL filter lines
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by FiscalQuarter;\n"
        f"q = foreach q generate "
        f"FiscalQuarter as FiscalQuarter, "
        f"sum(ClosedWonARR) as actual_arr, "
        f"sum(ForecastP50) as forecast_p50, "
        f"sum(ForecastP50_high_95) as forecast_p50_high_95, "
        f"sum(ForecastP50_low_95) as forecast_p50_low_95, "
        f"sum(ForecastP90) as forecast_p90;\n"
        f"q = order q by FiscalQuarter asc;"
    )
    return sq(q)


def forecast_error_step(ds_name, group_field="OwnerName", base_filters=""):
    """Build a SAQL step for forecast error analytics.

    Computes MAPE, bias, and absolute error by grouping dimension.
    Suitable for heatmap_chart() (rep × quarter) or bar charts.

    Args:
        ds_name: forecast error dataset name
        group_field: dimension to group by (OwnerName, UnitGroup, etc.)
        base_filters: SAQL filter lines
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by '{group_field}';\n"
        f"q = foreach q generate "
        f"'{group_field}' as '{group_field}', "
        f"avg(ErrorPct) as avg_error_pct, "
        f"avg(BiasPct) as avg_bias_pct, "
        f"avg(MAPE) as avg_mape, "
        f"sum(AbsoluteError) as total_abs_error, "
        f"count() as forecast_count;\n"
        f"q = order q by avg_mape desc;\n"
        f"q = limit q 20;"
    )
    return sq(q)


def forecast_error_heatmap_step(ds_name, base_filters=""):
    """Build a SAQL step for forecast error heatmap (rep × quarter).

    Creates a 2D grid of error rates suitable for heatmap_chart().
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f"q = group q by (OwnerName, FiscalQuarter);\n"
        f"q = foreach q generate "
        f"OwnerName as OwnerName, "
        f"FiscalQuarter as FiscalQuarter, "
        f"avg(ErrorPct) as avg_error_pct;\n"
        f"q = order q by OwnerName asc, FiscalQuarter asc;"
    )
    return sq(q)


def forecast_integrity_step(ds_name, base_filters=""):
    """Build a SAQL step for forecast integrity scoring.

    Compares ForecastCategory to WinProbability to find misalignments
    (e.g. Commit with low probability, Pipeline with high probability).
    Used by Sales Compliance dashboard.

    Args:
        ds_name: opportunity dataset with ForecastCategory and WinScore fields
        base_filters: SAQL filter lines
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f'q = filter q by IsClosed == "false";\n'
        f"q = group q by (OwnerName, ForecastCategory);\n"
        f"q = foreach q generate "
        f"OwnerName as OwnerName, "
        f"ForecastCategory as ForecastCategory, "
        f"avg(WinScore) as avg_win_score, "
        f"count() as opp_count, "
        f"sum(ARR) as total_arr, "
        f"(sum(case when "
        f'(ForecastCategory == "Commit" && WinScore < 40) || '
        f'(ForecastCategory == "Pipeline" && WinScore > 70) '
        f"then 1 else 0 end) / count() * 100) as mismatch_pct;\n"
        f"q = order q by mismatch_pct desc;"
    )
    return sq(q)


def forecast_integrity_heatmap_step(ds_name, base_filters=""):
    """Build a SAQL step for forecast integrity heatmap (rep × category).

    Shows mismatch rate by rep and forecast category.
    Suitable for heatmap_chart().
    """
    q = (
        f'q = load "{ds_name}";\n'
        f"{base_filters}"
        f'q = filter q by IsClosed == "false";\n'
        f"q = group q by (OwnerName, ForecastCategory);\n"
        f"q = foreach q generate "
        f"OwnerName as OwnerName, "
        f"ForecastCategory as ForecastCategory, "
        f"(sum(case when "
        f'(ForecastCategory == "Commit" && WinScore < 40) || '
        f'(ForecastCategory == "Pipeline" && WinScore > 70) '
        f"then 1 else 0 end) / count() * 100) as mismatch_rate;\n"
        f"q = order q by OwnerName asc;"
    )
    return sq(q)


# ─── ML scoring pipeline helpers ─────────────────────────────────────────


def compute_churn_probability(account, contracts=None):
    """Compute churn probability for an account using a rule-based model.

    Scoring factors:
      - Health score inversion: 0-25 pts (lower health = higher churn risk)
      - Contract expiry proximity: 0-20 pts (closer expiry = higher risk)
      - Revenue trend: 0-15 pts (declining NRR = higher risk)
      - Engagement decline: 0-15 pts (low recent activity)
      - Risk level: 0-15 pts (mapped from Salesforce risk level)
      - Product concentration: 0-10 pts (single product = higher risk)

    Args:
        account: dict with account fields (HealthScore, NRR_Proxy, etc.)
        contracts: optional list of contract dicts for this account

    Returns:
        (probability: float 0-100, risk_band: str, top_driver: str, second_driver: str)
    """
    scores = {}

    # 1. Health score inversion (0-25)
    health = account.get("HealthScore") or 50
    try:
        health = float(health)
    except (ValueError, TypeError):
        health = 50
    scores["Low Health Score"] = max(0, min(25, round((100 - health) / 4)))

    # 2. Contract expiry proximity (0-20)
    expiring_90d = account.get("ExpiringContracts90d") or 0
    expiring_180d = account.get("ExpiringContracts180d") or 0
    try:
        expiring_90d = int(float(expiring_90d))
        expiring_180d = int(float(expiring_180d))
    except (ValueError, TypeError):
        expiring_90d = expiring_180d = 0
    if expiring_90d > 0:
        scores["Contracts Expiring Soon"] = 20
    elif expiring_180d > 0:
        scores["Contracts Expiring Soon"] = 12
    else:
        scores["Contracts Expiring Soon"] = 0

    # 3. Revenue trend / NRR proxy (0-15)
    nrr = account.get("NRR_Proxy") or 100
    try:
        nrr = float(nrr)
    except (ValueError, TypeError):
        nrr = 100
    if nrr < 80:
        scores["Declining Revenue"] = 15
    elif nrr < 90:
        scores["Declining Revenue"] = 10
    elif nrr < 100:
        scores["Declining Revenue"] = 5
    else:
        scores["Declining Revenue"] = 0

    # 4. Engagement decline (0-15)
    recent_activity = account.get("RecentActivityCount") or 0
    try:
        recent_activity = int(float(recent_activity))
    except (ValueError, TypeError):
        recent_activity = 0
    if recent_activity == 0:
        scores["No Recent Engagement"] = 15
    elif recent_activity < 3:
        scores["No Recent Engagement"] = 8
    else:
        scores["No Recent Engagement"] = 0

    # 5. Risk level (0-15)
    risk = (account.get("RiskLevel") or "").lower()
    if risk in ("high", "critical"):
        scores["High Risk Level"] = 15
    elif risk == "medium":
        scores["High Risk Level"] = 8
    else:
        scores["High Risk Level"] = 0

    # 6. Product concentration (0-10)
    product_count = account.get("ProductCount") or 1
    try:
        product_count = int(float(product_count))
    except (ValueError, TypeError):
        product_count = 1
    if product_count <= 1:
        scores["Single Product"] = 10
    elif product_count == 2:
        scores["Single Product"] = 5
    else:
        scores["Single Product"] = 0

    total = sum(scores.values())
    probability = max(0, min(100, total))

    # Risk band
    if probability >= 70:
        band = "High"
    elif probability >= 40:
        band = "Medium"
    else:
        band = "Low"

    # Top drivers (sorted by contribution)
    sorted_drivers = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    driver1 = sorted_drivers[0][0] if sorted_drivers else ""
    driver2 = sorted_drivers[1][0] if len(sorted_drivers) > 1 else ""

    return probability, band, driver1, driver2


def compute_expansion_probability(account):
    """Compute expansion probability for an account.

    Scoring factors:
      - Expansion score (existing): 0-25 pts
      - NRR trend (growing): 0-20 pts
      - Product whitespace: 0-20 pts (fewer products = more room)
      - Engagement level: 0-15 pts
      - Account size / AuM: 0-10 pts
      - Multi-year contracts: 0-10 pts (stability signal)

    Args:
        account: dict with account fields

    Returns:
        (probability: float 0-100, band: str, top_driver: str, second_driver: str)
    """
    scores = {}

    # 1. Expansion score (0-25)
    exp_score = account.get("ExpansionScore") or 0
    try:
        exp_score = float(exp_score)
    except (ValueError, TypeError):
        exp_score = 0
    scores["High Expansion Score"] = max(0, min(25, round(exp_score / 4)))

    # 2. NRR trend (0-20)
    nrr = account.get("NRR_Proxy") or 100
    try:
        nrr = float(nrr)
    except (ValueError, TypeError):
        nrr = 100
    if nrr > 120:
        scores["Strong Revenue Growth"] = 20
    elif nrr > 110:
        scores["Strong Revenue Growth"] = 15
    elif nrr > 100:
        scores["Strong Revenue Growth"] = 8
    else:
        scores["Strong Revenue Growth"] = 0

    # 3. Product whitespace (0-20)
    product_count = account.get("ProductCount") or 0
    try:
        product_count = int(float(product_count))
    except (ValueError, TypeError):
        product_count = 0
    if product_count <= 1:
        scores["Product Whitespace"] = 20
    elif product_count == 2:
        scores["Product Whitespace"] = 12
    elif product_count == 3:
        scores["Product Whitespace"] = 5
    else:
        scores["Product Whitespace"] = 0

    # 4. Engagement level (0-15)
    recent_activity = account.get("RecentActivityCount") or 0
    try:
        recent_activity = int(float(recent_activity))
    except (ValueError, TypeError):
        recent_activity = 0
    if recent_activity >= 10:
        scores["Active Engagement"] = 15
    elif recent_activity >= 5:
        scores["Active Engagement"] = 10
    elif recent_activity >= 1:
        scores["Active Engagement"] = 5
    else:
        scores["Active Engagement"] = 0

    # 5. Account size (0-10)
    aum = account.get("AuM") or 0
    try:
        aum = float(aum)
    except (ValueError, TypeError):
        aum = 0
    if aum > 50000000000:  # >50B
        scores["Large Account"] = 10
    elif aum > 10000000000:  # >10B
        scores["Large Account"] = 7
    elif aum > 1000000000:  # >1B
        scores["Large Account"] = 4
    else:
        scores["Large Account"] = 0

    # 6. Multi-year contracts (0-10)
    multi_year = account.get("MultiYearCount") or 0
    try:
        multi_year = int(float(multi_year))
    except (ValueError, TypeError):
        multi_year = 0
    scores["Multi-Year Contracts"] = min(10, multi_year * 5)

    total = sum(scores.values())
    probability = max(0, min(100, total))

    if probability >= 65:
        band = "High"
    elif probability >= 35:
        band = "Medium"
    else:
        band = "Low"

    sorted_drivers = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    driver1 = sorted_drivers[0][0] if sorted_drivers else ""
    driver2 = sorted_drivers[1][0] if len(sorted_drivers) > 1 else ""

    return probability, band, driver1, driver2


def compute_forecast_risk(rep_data):
    """Compute forecast risk score for a rep/period.

    Evaluates whether the rep is likely to miss their forecast.

    Scoring factors:
      - Win-probability-weighted pipeline vs commit gap: 0-30 pts
      - Historical accuracy (if available): 0-20 pts
      - Pipeline age/stale ratio: 0-20 pts
      - Push/slip signal count: 0-15 pts
      - Coverage ratio: 0-15 pts

    Args:
        rep_data: dict with rep-level forecast fields

    Returns:
        (risk_score: float 0-100, risk_band: str, top_driver: str)
    """
    scores = {}

    # 1. Weighted pipeline vs commit gap (0-30)
    weighted = rep_data.get("WeightedForecast") or 0
    commit = rep_data.get("CommitARR") or 0
    quota = rep_data.get("QuotaAmount") or 1
    try:
        weighted = float(weighted)
        commit = float(commit)
        quota = float(quota) if float(quota) > 0 else 1
    except (ValueError, TypeError):
        weighted = commit = 0
        quota = 1
    coverage_of_quota = weighted / quota if quota > 0 else 0
    if coverage_of_quota < 0.5:
        scores["Low Weighted Pipeline"] = 30
    elif coverage_of_quota < 0.8:
        scores["Low Weighted Pipeline"] = 20
    elif coverage_of_quota < 1.0:
        scores["Low Weighted Pipeline"] = 10
    else:
        scores["Low Weighted Pipeline"] = 0

    # 2. Historical accuracy (0-20)
    prior_accuracy = rep_data.get("PriorAccuracyPct") or 100
    try:
        prior_accuracy = float(prior_accuracy)
    except (ValueError, TypeError):
        prior_accuracy = 100
    if prior_accuracy < 60:
        scores["Poor Forecast History"] = 20
    elif prior_accuracy < 80:
        scores["Poor Forecast History"] = 12
    elif prior_accuracy < 90:
        scores["Poor Forecast History"] = 5
    else:
        scores["Poor Forecast History"] = 0

    # 3. Pipeline age (0-20)
    stale_pct = rep_data.get("StalePipelinePct") or 0
    try:
        stale_pct = float(stale_pct)
    except (ValueError, TypeError):
        stale_pct = 0
    if stale_pct > 50:
        scores["Stale Pipeline"] = 20
    elif stale_pct > 30:
        scores["Stale Pipeline"] = 12
    elif stale_pct > 15:
        scores["Stale Pipeline"] = 5
    else:
        scores["Stale Pipeline"] = 0

    # 4. Push/slip signals (0-15)
    push_count = rep_data.get("PushCount") or 0
    try:
        push_count = int(float(push_count))
    except (ValueError, TypeError):
        push_count = 0
    if push_count >= 5:
        scores["Frequent Deal Slippage"] = 15
    elif push_count >= 3:
        scores["Frequent Deal Slippage"] = 8
    elif push_count >= 1:
        scores["Frequent Deal Slippage"] = 3
    else:
        scores["Frequent Deal Slippage"] = 0

    # 5. Coverage ratio (0-15)
    total_pipe = rep_data.get("TotalPipelineARR") or 0
    try:
        total_pipe = float(total_pipe)
    except (ValueError, TypeError):
        total_pipe = 0
    pipe_coverage = total_pipe / quota if quota > 0 else 0
    if pipe_coverage < 1.5:
        scores["Insufficient Coverage"] = 15
    elif pipe_coverage < 2.0:
        scores["Insufficient Coverage"] = 8
    elif pipe_coverage < 3.0:
        scores["Insufficient Coverage"] = 3
    else:
        scores["Insufficient Coverage"] = 0

    total = sum(scores.values())
    risk_score = max(0, min(100, total))

    if risk_score >= 60:
        band = "High"
    elif risk_score >= 30:
        band = "Medium"
    else:
        band = "Low"

    sorted_drivers = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    driver1 = sorted_drivers[0][0] if sorted_drivers else ""

    return risk_score, band, driver1


# ─── Reusable dashboard page builders ─────────────────────────────────────


def build_retention_page_widgets(prefix, ds_name, ds_id, base_filters=""):
    """Build a complete retention & growth page with cohort heatmap + ARR bridge.

    Returns (steps, widgets) dicts ready to merge into dashboard.
    Requires cohort retention and ARR bridge datasets.

    Args:
        prefix: page widget prefix (e.g. "p6")
        ds_name: cohort retention dataset name
        ds_id: dataset ID for step metadata
        base_filters: SAQL filter lines
    """
    steps = {
        f"s_{prefix}_cohort_nrr": cohort_retention_step(ds_name, base_filters, "NRR"),
        f"s_{prefix}_cohort_grr": cohort_retention_step(ds_name, base_filters, "GRR"),
    }

    widgets = {
        f"{prefix}_sec_retention": section_label("Cohort Retention Analysis"),
        f"{prefix}_ch_cohort_nrr": heatmap_chart(
            f"s_{prefix}_cohort_nrr", "Net Revenue Retention by Cohort"
        ),
        f"{prefix}_ch_cohort_grr": heatmap_chart(
            f"s_{prefix}_cohort_grr", "Gross Revenue Retention by Cohort"
        ),
    }

    return steps, widgets


def build_forecast_bands_page_widgets(prefix, ds_name, ds_id, base_filters=""):
    """Build a forecast bands & error page with timeline bands + error heatmap.

    Returns (steps, widgets) dicts ready to merge into dashboard.

    Args:
        prefix: page widget prefix (e.g. "p6")
        ds_name: forecast snapshot dataset name
        ds_id: dataset ID
        base_filters: SAQL filter lines
    """
    steps = {
        f"s_{prefix}_forecast_bands": forecast_bands_step(ds_name, base_filters),
    }

    widgets = {
        f"{prefix}_sec_bands": section_label("Forecast Bands & Uncertainty"),
        f"{prefix}_ch_bands": timeline_chart(
            f"s_{prefix}_forecast_bands",
            "Actual vs Forecast with 95% Prediction Interval",
            axis_title="ARR (EUR)",
        ),
    }

    return steps, widgets
