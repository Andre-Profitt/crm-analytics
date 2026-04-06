#!/usr/bin/env python3
"""Shared helpers for CRM Analytics dashboard builders.

Provides reusable functions for:
- Salesforce auth & API calls
- Dataset upload (InsightsExternalData)
- SAQL step builders
- Widget builders (number, chart, gauge, funnel, waterfall, choropleth)
- Layout helpers (header, section label, nav link, page)
- Dashboard deployment
"""

import base64
import copy
import html
import json
import re
import subprocess
import time
import urllib.request
import urllib.error
from datetime import datetime
from typing import Any

# --- Logging config (Builder Modernization 1A) ---------------------------
# basicConfig at module load so any builder importing this module gets
# logging configured automatically. crm_analytics_helpers.py is the de
# facto runtime entry point for the 8 KPI builders, so basicConfig
# belongs here despite stdlib warnings against it in general libraries.
import logging
import os

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(__name__)

TARGET_ORG = "apro@simcorp.com"
APP_NAME = "B2B_MA"
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}
PATCH_COLUMNMAP_REQUIRED_KEYS = ("dimensionAxis", "plots", "trellis", "split")
PATCH_COLUMNMAP_REQUIRED_VIZ = {
    "hbar",
    "column",
    "donut",
    "pie",
    "stackcolumn",
    "stackhbar",
    "stackvbar",
    "vbar",
}
PATCH_COLUMNMAP_NULL_VIZ = {"funnel", "treemap", "waterfall"}
PATCH_NUMBER_WIDGET_BANNED_FIELDS = {"compact", "numberFormat", "title"}
PATCH_NUMBER_WIDGET_INVALID_FIELDS = {"text"}
PATCH_LINK_WIDGET_DESTINATION_TYPES = {"dashboard", "page"}
PATCH_QUERY_STEP_REFERENCE_RE = re.compile(
    r"(?:column|cell)\((?P<step>[A-Za-z0-9_]+)\.(?:selection|result)\s*,"
)

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


def _deep_html_unescape(value):
    """Recursively unescape HTML entities in API-fetched dashboard state.

    Wave dashboard GET responses encode SAQL with entities like &quot; and &amp;&amp;.
    If that state is patched back without normalization, the dashboard stores
    poisoned queries that fail at runtime.
    """
    if isinstance(value, str):
        prev = value
        while True:
            curr = html.unescape(prev)
            if curr == prev:
                return curr
            prev = curr
    if isinstance(value, dict):
        return {k: _deep_html_unescape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deep_html_unescape(v) for v in value]
    return value


def _clean_dashboard_state_for_patch(
    state,
    *,
    strip_page_labels=False,
    strip_number_widget_patch_fields=False,
):
    """Strip read-only fields from API-fetched dashboard state before PATCH."""
    if not isinstance(state, dict):
        return state

    state.pop("layouts", None)

    for grid in state.get("gridLayouts", []):
        if isinstance(grid, dict):
            grid.pop("selectors", None)
            grid.pop("numColumns", None)
            for page in grid.get("pages", []):
                if isinstance(page, dict):
                    if strip_page_labels:
                        page.pop("label", None)
                    page.pop("navigationHidden", None)

    for step in state.get("steps", {}).values():
        if not isinstance(step, dict):
            continue

        if step.get("type") == "aggregateflex":
            step.pop("isFacet", None)
            for ds in step.get("datasets", []):
                if isinstance(ds, dict):
                    ds.pop("label", None)
                    ds.pop("url", None)
            query = step.get("query")
            if isinstance(query, dict):
                for ds in query.get("datasets", []):
                    if isinstance(ds, dict):
                        ds.pop("label", None)
                        ds.pop("url", None)

        for ds in step.get("datasets", []):
            if isinstance(ds, dict):
                ds.pop("label", None)
                ds.pop("url", None)

    if strip_number_widget_patch_fields:
        for widget in state.get("widgets", {}).values():
            if not isinstance(widget, dict) or widget.get("type") != "number":
                continue
            params = widget.get("parameters")
            if not isinstance(params, dict):
                continue
            for field_name in PATCH_NUMBER_WIDGET_BANNED_FIELDS:
                params.pop(field_name, None)

    return state


def normalize_dashboard_state_for_patch(
    state: dict[str, Any],
    *,
    strip_page_labels: bool = True,
    strip_number_widget_patch_fields: bool = False,
) -> dict[str, Any]:
    """Return a deep-copied dashboard state normalized for Wave PATCH.

    This helper intentionally defaults to conservative cleanup:
    - fully unescape GET payload strings
    - strip read-only page metadata and dataset label/url fields
    - keep number widget fields unless the caller explicitly opts in
    """
    normalized_state = copy.deepcopy(_deep_html_unescape(state))
    return _clean_dashboard_state_for_patch(
        normalized_state,
        strip_page_labels=strip_page_labels,
        strip_number_widget_patch_fields=strip_number_widget_patch_fields,
    )


def find_dashboard_patch_contract_violations(
    state: dict[str, Any],
) -> list[dict[str, str]]:
    """Return Wave PATCH contract violations found in a dashboard state."""
    if not isinstance(state, dict):
        return [
            {
                "code": "invalid_state",
                "path": "state",
                "message": "Dashboard state must be a dict.",
            }
        ]

    violations: list[dict[str, str]] = []
    page_names: set[str] = set()
    widget_names = {
        widget_name
        for widget_name, widget in (state.get("widgets", {}) or {}).items()
        if isinstance(widget_name, str) and widget_name and isinstance(widget, dict)
    }
    step_names = {
        step_name
        for step_name, step in (state.get("steps", {}) or {}).items()
        if isinstance(step_name, str) and step_name and isinstance(step, dict)
    }

    for grid in state.get("gridLayouts", []) or []:
        if not isinstance(grid, dict):
            continue
        for page in grid.get("pages", []) or []:
            if not isinstance(page, dict):
                continue
            page_name = page.get("name")
            if isinstance(page_name, str) and page_name:
                page_names.add(page_name)
            for index, page_widget in enumerate(page.get("widgets", []) or []):
                if not isinstance(page_widget, dict):
                    continue
                widget_name = page_widget.get("name")
                if (
                    isinstance(widget_name, str)
                    and widget_name
                    and widget_name not in widget_names
                ):
                    violations.append(
                        {
                            "code": "page_widget_missing",
                            "path": f"gridLayouts.pages.{page_name}.widgets[{index}]",
                            "message": (
                                f"Page {page_name!r} references widget {widget_name!r}, "
                                "but no widget definition exists in state.widgets."
                            ),
                        }
                    )

    for step_name, step in (state.get("steps", {}) or {}).items():
        if not isinstance(step, dict):
            continue

        if step.get("type") == "aggregateflex" and "isFacet" in step:
            violations.append(
                {
                    "code": "aggregateflex_isfacet",
                    "path": f"steps.{step_name}.isFacet",
                    "message": "aggregateflex steps must not include isFacet on PATCH.",
                }
            )

        dataset_locations: list[tuple[str, list[Any]]] = []
        datasets = step.get("datasets")
        if isinstance(datasets, list):
            dataset_locations.append((f"steps.{step_name}.datasets", datasets))
        query = step.get("query")
        if isinstance(query, dict) and isinstance(query.get("datasets"), list):
            dataset_locations.append(
                (f"steps.{step_name}.query.datasets", query["datasets"])
            )

        for base_path, dataset_list in dataset_locations:
            for index, dataset in enumerate(dataset_list):
                if not isinstance(dataset, dict):
                    continue
                banned_keys = sorted(key for key in ("label", "url") if key in dataset)
                if banned_keys:
                    violations.append(
                        {
                            "code": "dataset_readonly_fields",
                            "path": f"{base_path}[{index}]",
                            "message": (
                                "Dataset entries must not include read-only "
                                f"fields on PATCH: {', '.join(banned_keys)}."
                            ),
                        }
                    )

        if isinstance(query, str):
            referenced_steps = sorted(
                {
                    match.group("step")
                    for match in PATCH_QUERY_STEP_REFERENCE_RE.finditer(query)
                    if match.group("step") not in step_names
                }
            )
            for missing_step_name in referenced_steps:
                violations.append(
                    {
                        "code": "step_reference_missing",
                        "path": f"steps.{step_name}.query",
                        "message": (
                            f"Step query references {missing_step_name!r} via column()/cell(), "
                            "but that step is not defined in state.steps."
                        ),
                    }
                )

    for widget_name, widget in (state.get("widgets", {}) or {}).items():
        if not isinstance(widget, dict):
            continue

        params = widget.get("parameters")
        if not isinstance(params, dict):
            continue

        viz = params.get("visualizationType") or widget.get("type")
        column_map = params.get("columnMap")

        if viz in PATCH_COLUMNMAP_REQUIRED_VIZ and column_map is not None:
            if not isinstance(column_map, dict):
                violations.append(
                    {
                        "code": "columnmap_invalid_type",
                        "path": f"widgets.{widget_name}.parameters.columnMap",
                        "message": (
                            f"{viz} widgets must use a 4-key columnMap dict or omit it."
                        ),
                    }
                )
            else:
                missing_keys = [
                    key
                    for key in PATCH_COLUMNMAP_REQUIRED_KEYS
                    if key not in column_map
                ]
                if missing_keys:
                    violations.append(
                        {
                            "code": "columnmap_missing_keys",
                            "path": f"widgets.{widget_name}.parameters.columnMap",
                            "message": (
                                f"{viz} widgets require columnMap keys: "
                                f"{', '.join(missing_keys)}."
                            ),
                        }
                    )

        if viz in PATCH_COLUMNMAP_NULL_VIZ and column_map is not None:
            violations.append(
                {
                    "code": "columnmap_must_be_null",
                    "path": f"widgets.{widget_name}.parameters.columnMap",
                    "message": f"{viz} widgets must use columnMap: null.",
                }
            )

        if widget.get("type") == "number":
            banned_fields = sorted(
                field_name
                for field_name in PATCH_NUMBER_WIDGET_BANNED_FIELDS
                if field_name in params
            )
            if banned_fields:
                violations.append(
                    {
                        "code": "number_widget_banned_fields",
                        "path": f"widgets.{widget_name}.parameters",
                        "message": (
                            "Number widgets must not include PATCH-banned fields: "
                            f"{', '.join(banned_fields)}."
                        ),
                    }
                )
            invalid_fields = sorted(
                field_name
                for field_name in PATCH_NUMBER_WIDGET_INVALID_FIELDS
                if field_name in params
            )
            if invalid_fields:
                violations.append(
                    {
                        "code": "number_widget_invalid_fields",
                        "path": f"widgets.{widget_name}.parameters",
                        "message": (
                            "Number widgets must not include unsupported PATCH fields: "
                            f"{', '.join(invalid_fields)}."
                        ),
                    }
                )

        if widget.get("type") == "link":
            destination_type = params.get("destinationType")
            if (
                isinstance(destination_type, str)
                and destination_type
                and destination_type not in PATCH_LINK_WIDGET_DESTINATION_TYPES
            ):
                violations.append(
                    {
                        "code": "link_widget_invalid_destination_type",
                        "path": f"widgets.{widget_name}.parameters.destinationType",
                        "message": (
                            "Link widgets must use a supported destinationType: "
                            f"{', '.join(sorted(PATCH_LINK_WIDGET_DESTINATION_TYPES))}."
                        ),
                    }
                )

        step_name = params.get("step")
        if isinstance(step_name, str) and step_name and step_name not in step_names:
            violations.append(
                {
                    "code": "widget_step_missing",
                    "path": f"widgets.{widget_name}.parameters.step",
                    "message": (
                        f"Widget {widget_name!r} references step {step_name!r}, "
                        "but that step is not defined in state.steps."
                    ),
                }
            )

        destination_link = params.get("destinationLink")
        if isinstance(destination_link, dict):
            destination_type = params.get("destinationType")
            destination_name = destination_link.get("name")
            if (
                destination_type != "dashboard"
                and isinstance(destination_name, str)
                and destination_name
                and destination_name not in page_names
            ):
                violations.append(
                    {
                        "code": "destination_link_name_mismatch",
                        "path": f"widgets.{widget_name}.parameters.destinationLink.name",
                        "message": (
                            "destinationLink.name must match a gridLayouts page name; "
                            f"found {destination_name!r}."
                        ),
                    }
                )

    return violations


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


def _urlopen_json(req, *, retry_label="request", max_attempts=4, initial_delay=1.5):
    """Open a Salesforce HTTP request and retry transient failures."""
    delay = initial_delay
    for attempt in range(1, max_attempts + 1):
        try:
            with urllib.request.urlopen(req) as resp:
                if getattr(resp, "status", None) == 204:
                    return {}
                return json.loads(resp.read().decode())
        except urllib.error.HTTPError as e:
            err_body = e.read().decode()
            if e.code in TRANSIENT_HTTP_CODES and attempt < max_attempts:
                logger.warning(
                    "%s: transient HTTP %s on attempt %d/%d; retrying in %.1fs",
                    retry_label,
                    e.code,
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
                continue
            raise RuntimeError(
                f"{retry_label} → HTTP {e.code}: {err_body[:500]}"
            ) from e
        except (urllib.error.URLError, ConnectionResetError, TimeoutError) as e:
            if attempt < max_attempts:
                logger.warning(
                    "%s: transient network error on attempt %d/%d; retrying in %.1fs",
                    retry_label,
                    attempt,
                    max_attempts,
                    delay,
                )
                time.sleep(delay)
                delay *= 2
                continue
            raise RuntimeError(
                f"{retry_label} failed after {max_attempts} attempts: {e}"
            ) from e


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
    return _urlopen_json(req, retry_label=f"API {method} {path}")


def _soql(inst, tok, query):
    """Run a SOQL query and return all records (handles pagination)."""
    encoded = urllib.request.quote(query)
    url = f"{inst}/services/data/v66.0/query/?q={encoded}"
    records = []
    while url:
        req = urllib.request.Request(url, headers={"Authorization": f"Bearer {tok}"})
        data = _urlopen_json(req, retry_label="SOQL query")
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
    poll_attempts=80,
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
    logger.info("  Upload header: %s", header_id)

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
    logger.info("  Uploaded %d bytes in %d part(s)", len(csv_bytes), part - 1)

    _sf_api(
        inst,
        tok,
        "PATCH",
        f"/services/data/v66.0/sobjects/InsightsExternalData/{header_id}",
        {"Action": "Process"},
    )
    logger.info("  Processing started...")

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
                logger.info("  Dataset ready! (%s)", s)
                return True
            if s == "Failed":
                logger.error("  FAILED: %s", msg)
                return False
            logger.info("  ... %s", s)
    logger.warning("  Timed out waiting for dataset processing")
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


def af(field, ds_meta, select_mode="multi", start=None):
    """Build an aggregateflex step for filter selectors.

    Args:
        field: Field name to aggregate on
        ds_meta: Dataset metadata list, e.g. [{"id": "...", "name": "DS_Name"}]
        select_mode: "multi" or "single"
        start: optional JSON-array string of default selected values
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
        "start": start or "[]",
        "useGlobal": True,
        "useExternalFilters": True,
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Widget builders
# ═══════════════════════════════════════════════════════════════════════════


def num(
    step,
    field,
    title,
    color,
    compact=False,
    size=None,
    tier=None,
    prefix="",
    suffix="",
    subtitle="",
    subtitle_size=11,
    subtitle_color="#54698D",
    title_color="#54698D",
    title_size=None,
    text_alignment="center",
    icon=None,
    icon_color=None,
    sentiment_color=False,
    conditional_ranges=None,
    widget_style=None,
):
    """Build a number (KPI tile) widget with consulting-grade formatting.

    Tier system (auto-sets sizes if `size` and `title_size` are not specified):
        "primary"   → numberSize=40, titleSize=10  (hero KPIs, 4-col wide)
        "secondary" → numberSize=32, titleSize=10  (supporting KPIs, 3-col wide)
        "tertiary"  → numberSize=24, titleSize=10  (detail KPIs, 2-col wide)

    NOTE: Wave API PATCH rejects 'numberFormat' and 'compactForm'. Formatting
    comes from compact=True, dataset XMD, or SAQL number_to_string().

    Args:
        step: Step name providing the measure
        field: measureField (e.g. "sum_Amount", "avg_Win_Rate")
        title: Primary label text
        color: Number value color (hex)
        compact: Abbreviate large numbers (1.2M instead of 1,200,000)
        size: Override numberSize in points (default: auto from tier)
        tier: "primary", "secondary", or "tertiary" (auto-sizes)
        prefix: Text before value (e.g. "$")
        suffix: Text after value (e.g. "%")
        subtitle: Secondary label (e.g. "+12.4% QoQ")
        subtitle_size: Subtitle font size (default 11)
        subtitle_color: Subtitle color (default #54698D)
        title_color: Title label color (default #54698D)
        title_size: Title font size (default: auto from tier)
        text_alignment: "left", "center", or "right"
        icon: SLDS icon name (e.g. "utility:money", "utility:trending")
        icon_color: Icon color (hex)
        sentiment_color: Auto green/red based on value sign
        conditional_ranges: List of {color, value, label} for threshold coloring
        widget_style: Per-widget style override dict
    """
    # Tier-based auto-sizing (3:1 ratio rule)
    TIER_SIZES = {
        "primary": (40, 10),
        "secondary": (32, 10),
        "tertiary": (24, 10),
    }
    if tier and tier in TIER_SIZES:
        default_num_size, default_title_size = TIER_SIZES[tier]
    else:
        default_num_size, default_title_size = 24, 12

    num_size = size if size is not None else default_num_size
    t_size = title_size if title_size is not None else default_title_size

    params = {
        "step": step,
        "measureField": field,
        "compact": compact,
        "title": title,
        "titleColor": title_color,
        "titleSize": t_size,
        "numberColor": color,
        "numberSize": num_size,
        "textAlignment": text_alignment,
        "exploreLink": True,
        "interactions": [],
    }

    if prefix:
        unit = str(prefix).strip()
        if unit and unit not in params["title"]:
            if unit in {"$", "€", "£"}:
                params["title"] = f"{params['title']} ({unit})"
            else:
                params["title"] = f"{unit} {params['title']}"
    if suffix:
        unit = str(suffix).strip()
        if unit and unit not in params["title"]:
            if unit in {"%", "x", "/day"}:
                params["title"] = f"{params['title']} {unit}"
            else:
                params["title"] = f"{params['title']} ({unit})"
    if subtitle:
        params["subtitle"] = subtitle
        params["subtitleFontSize"] = subtitle_size
    if subtitle_color != "#54698D":
        params["subtitleColor"] = subtitle_color
    if icon:
        params["icon"] = icon
        if icon_color:
            params["iconColor"] = icon_color
    if sentiment_color or conditional_ranges:
        cf = {}
        if sentiment_color:
            cf["sentimentColor"] = True
        if conditional_ranges:
            cf["ranges"] = conditional_ranges
        params["conditionalFormatting"] = cf

    return {"type": "number", "parameters": params}


# Consulting-grade KPI card widget styles
KPI_CARD_STYLE = {
    "backgroundColor": "#FFFFFF",
    "borderColor": "#E0E5EE",
    "borderEdges": ["top", "right", "bottom", "left"],
    "borderRadius": 8,
    "borderWidth": 1,
}

KPI_ACCENT_LEFT_STYLE = {
    "backgroundColor": "#FFFFFF",
    "borderColor": "#0070D2",
    "borderEdges": ["left"],
    "borderRadius": 0,
    "borderWidth": 4,
}

KPI_NO_BORDER_STYLE = {
    "backgroundColor": "transparent",
    "borderColor": "#FFFFFF",
    "borderEdges": [],
    "borderRadius": 0,
    "borderWidth": 0,
}


def kpi_style(variant="card", accent_color=None):
    """Return a widgetStyle dict for KPI number widgets.

    Variants:
        "card"   → Rounded card with subtle border (consulting standard)
        "accent" → Left-accent bar (Revenue Intelligence style)
        "none"   → No border/background (for dense layouts)
    """
    styles = {
        "card": dict(KPI_CARD_STYLE),
        "accent": dict(KPI_ACCENT_LEFT_STYLE),
        "none": dict(KPI_NO_BORDER_STYLE),
    }
    style = styles.get(variant, dict(KPI_CARD_STYLE))
    if accent_color:
        style["borderColor"] = accent_color
    return style


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
    subtitle="",
    number_format=None,
    format_rules=None,
):
    """Build a chart widget with full CRM Analytics configuration.

    normalize: True for 100% stacked charts (all bars sum to 100%).
    show_values: True to show data labels on bars/slices.
    reference_lines: list of {value, label, color} dicts for threshold lines.
    number_format: str — format for measureAxis1 (e.g. "$#,##0", "0.0%", "#,##0").
        If None, auto-inferred from axis_title/title.
    format_rules: list of dicts — conditional formatting rules for compare tables.
        e.g. [{"type":"threshold","field":"risk_score","rules":[{"value":70,"color":"#D4504C","operator":"gte"}]}]
    """
    import re

    # Auto-infer numberFormat if not provided
    if number_format is None:
        combined = f"{axis_title} {title}".lower()
        if re.search(
            r"(%|rate|coverage|confidence|pacing|yoy|conversion|win|attainment)",
            combined,
        ):
            number_format = "0.0%"
        elif re.search(
            r"(\$|arr|revenue|amount|quota|plan|gap|acv|mrr|pipeline|forecast|value)",
            combined,
        ):
            number_format = "$#,##0"
        elif re.search(r"(days|hours|duration|cycle|dwell|velocity|age)", combined):
            number_format = "#,##0.0"
        else:
            number_format = "#,##0"

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
            "subtitleLabel": subtitle,
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
            "numberFormat": number_format,
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
    if format_rules:
        params["formatRules"] = format_rules
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
    return {"type": "chart", "parameters": params}


def choropleth_chart(step, title, geo_field, measure_field):
    """Build a choropleth chart widget using the lens-compatible map config."""
    return {
        "type": "chart",
        "parameters": {
            "applyConditionalFormatting": True,
            "autoZoom": False,
            "bins": {
                "bands": {
                    "high": {"color": "#008000", "label": ""},
                    "low": {"color": "#B22222", "label": ""},
                    "medium": {"color": "#ffa500", "label": ""},
                },
                "breakpoints": {"high": 100, "low": 0},
            },
            "step": step,
            "visualizationType": "choropleth",
            "map": "World Countries",
            "projectionType": "Mercator",
            "binValues": False,
            "lowColor": "#C5DBF7",
            "highColor": "#1674D9",
            "columnMap": {
                "color": [measure_field],
                "plots": [geo_field],
                "trellis": [],
            },
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": "",
            },
            "theme": "wave",
            "legend": {
                "show": True,
                "showHeader": True,
                "position": "right-top",
                "inside": False,
                "customSize": "auto",
                "descOrder": False,
            },
            "trellis": {
                "chartsPerLine": 4,
                "enable": False,
                "flipLabels": False,
                "showGridLines": True,
                "size": [100, 100],
                "type": "x",
            },
            "tooltip": {
                "content": {
                    "legend": {
                        "customizeLegend": False,
                        "dimensions": [],
                        "measures": [],
                        "showBinLabel": True,
                        "showDimensions": True,
                        "showMeasures": True,
                        "showNullValues": True,
                        "showPercentage": True,
                    }
                }
            },
            "exploreLink": True,
            "showActionMenu": True,
            "interactions": [],
        },
    }


def sankey_chart(
    step,
    title,
    source_field="source",
    target_field="target",
    measure_field="cnt",
    subtitle="",
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
                "subtitleLabel": subtitle,
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
    subtitle="",
    reference_lines=None,
    axis1_format=None,
    axis2_format=None,
):
    """Build a combo (bar + line) chart widget.

    Uses columnMap + plotConfiguration array (production-verified format).
    axis1_format/axis2_format: ICU number format strings, e.g. "$#,##0" or "0.0%".
    Each bar measure renders as column, each line measure renders as line.
    """
    plot_config = [{"series": m, "chartType": "column"} for m in bar_measures] + [
        {"series": m, "chartType": "line"} for m in line_measures
    ]
    w = {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "combo",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": subtitle,
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
                **({"numberFormat": axis1_format} if axis1_format else {}),
            },
            "measureAxis2": {
                "showTitle": bool(axis2_title),
                "showAxis": True,
                "title": axis2_title,
                "sqrtScale": False,
                "customDomain": {"showDomain": False},
                **({"numberFormat": axis2_format} if axis2_format else {}),
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
    return w


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


def line_chart(
    step, title, show_legend=True, axis_title="", reference_lines=None, subtitle=""
):
    """Build a line chart widget. Auto-detect columnMap.

    SAQL must produce: dimension, one or more measures.
    """
    w = {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "line",
            "title": {
                "label": title,
                "fontSize": 14,
                "subtitleFontSize": 11,
                "align": "center",
                "subtitleLabel": subtitle,
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
    return w


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
            "tooltip": {
                "content": {
                    "legend": {
                        "customizeLegend": False,
                        "dimensions": [],
                        "measures": [],
                        "showBinLabel": True,
                        "showDimensions": True,
                        "showMeasures": True,
                        "showNullValues": True,
                        "showPercentage": False,
                    }
                }
            },
            "widgetStyle": {
                "backgroundColor": "#FAFBFC",
                "borderColor": "#E4E7EB",
                "borderEdges": ["all"],
                "borderRadius": 6,
                "borderWidth": 1,
            },
            "interactions": [],
        },
    }


def flat_gauge(step, field, title, min_val=0, max_val=100, bands=None):
    """Build a flat gauge widget for target-vs-actual KPIs.

    Use this when the runtime does not support legacy bullet charts.
    """
    if bands is None:
        bands = [
            {"start": 0, "stop": 50, "color": "#D4504C"},
            {"start": 50, "stop": 80, "color": "#FFB75D"},
            {"start": 80, "stop": 100, "color": "#04844B"},
        ]
    return {
        "type": "chart",
        "parameters": {
            "step": step,
            "visualizationType": "flatgauge",
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
            "tooltip": {
                "content": {
                    "legend": {
                        "customizeLegend": False,
                        "dimensions": [],
                        "measures": [],
                        "showBinLabel": True,
                        "showDimensions": True,
                        "showMeasures": True,
                        "showNullValues": True,
                        "showPercentage": False,
                    }
                }
            },
            "widgetStyle": {
                "backgroundColor": "#FAFBFC",
                "borderColor": "#E4E7EB",
                "borderEdges": ["all"],
                "borderRadius": 6,
                "borderWidth": 1,
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


def compare_table(
    step,
    title,
    columns=None,
    column_properties=None,
    row_limit=25,
    header_bg="#F4F6F9",
    header_color="#16325C",
    header_size=12,
    cell_bg="#FFFFFF",
    cell_color="#16325C",
    cell_size=12,
    border_color="#E0E5EE",
    inner_border_color="#E0E5EE",
    show_totals=True,
    show_row_index=False,
    vertical_padding=8,
    mode="variable",
    min_col_width=40,
    max_col_width=300,
    format_rules=None,
    actions=None,
    subtitle="",
):
    """Build a consulting-grade compare table widget.

    Args:
        step: Step name providing the data
        title: Chart title (string or title object)
        columns: Ordered list of column aliases to display (controls visibility/order)
        column_properties: Dict keyed by field name with per-column config:
            {"Amount": {"width": 120, "alignment": "right", "format": "$,.0f"}}
        row_limit: Max rows displayed (25 detail, 10 summary)
        header_bg/header_color/header_size: Header row styling
        cell_bg/cell_color/cell_size: Data cell styling
        border_color: Outer border color
        inner_border_color: Inner grid line color
        show_totals: Show totals row at bottom
        show_row_index: Show row number column
        vertical_padding: Cell padding in px
        mode: "variable" (resizable) or "fixed"
        min_col_width/max_col_width: Column width bounds
        format_rules: Conditional formatting rules
        actions: List of custom bulk action configs
        subtitle: Subtitle text
    """
    title_obj = (
        title
        if isinstance(title, dict)
        else {
            "label": title,
            "fontSize": 14,
            "subtitleFontSize": 11,
            "align": "center",
            "subtitleLabel": subtitle,
        }
    )

    params = {
        "step": step,
        "visualizationType": "comparisontable",
        "title": title_obj,
        "theme": "wave",
        "exploreLink": True,
        "showActionMenu": True,
        "autoFitMode": "fit",
        "borderColor": border_color,
        "borderWidth": 1,
        "cell": {
            "backgroundColor": cell_bg,
            "fontColor": cell_color,
            "fontSize": cell_size,
        },
        "header": {
            "backgroundColor": header_bg,
            "fontColor": header_color,
            "fontSize": header_size,
        },
        "innerMajorBorderColor": "#A8B7C7",
        "innerMinorBorderColor": inner_border_color,
        "maxColumnWidth": max_col_width,
        "minColumnWidth": min_col_width,
        "mode": mode,
        "numberOfLines": 1,
        "rowLimit": row_limit,
        "showRowIndexColumn": show_row_index,
        "totals": show_totals,
        "verticalPadding": vertical_padding,
        "applyConditionalFormatting": True,
        "interactions": [],
    }

    if columns:
        params["columns"] = columns
    if column_properties:
        params["columnProperties"] = column_properties
    if format_rules:
        params["formatRules"] = format_rules
    if actions:
        params["customBulkActions"] = actions

    return {"type": "chart", "parameters": params}


def nav_link_external(dashboard_id, text, include_state=True, font_size=14):
    """Build a link widget for cross-dashboard navigation.

    Args:
        dashboard_id: Target dashboard ID (0FK...)
        text: Display text
        include_state: Carry filter context to destination (default True)
        font_size: Font size in points
    """
    return {
        "type": "link",
        "parameters": {
            "destination": dashboard_id,
            "destinationType": "dashboard",
            "destinationLink": {"name": dashboard_id},
            "includeState": include_state,
            "fontSize": font_size,
            "text": text,
            "textAlignment": "center",
            "textColor": "#0070D2",
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
        logger.warning(
            "  XMD: dataset '%s' not found — skipping record links", dataset_name
        )
        return
    ds_id = ds["id"]
    vid = ds.get("currentVersionId", "")
    if not vid:
        logger.warning("  XMD: no current version for '%s' — skipping", dataset_name)
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
        logger.info(
            "  XMD: record actions applied on %s [%s]", dataset_name, fields_str
        )
    except RuntimeError as e:
        logger.warning("  XMD WARNING (%s): %s", dataset_name, e)


def pg(name, lbl, widgets):
    """Build a page dict for the grid layout."""
    return {"name": name, "label": lbl, "widgets": widgets}


def nav_row(prefix, count):
    """Generate nav bar layout positions across row 0 for `count` pages."""
    if count == 8:
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


def build_dashboard_state(
    steps,
    widgets,
    layout,
    bg_color="#F4F6F9",
    cell_spacing=8,
    row_height="normal",
    widget_style=None,
    data_source_links=None,
    filters=None,
):
    """Build the complete dashboard state dict with consulting-grade theming.

    Args:
        steps: Step definitions dict
        widgets: Widget definitions dict
        layout: gridLayouts page layout
        bg_color: Dashboard background color (default #F4F6F9 light gray)
        cell_spacing: Widget spacing in px (default 8, consulting standard)
        row_height: "normal" or "fine" (fine = denser Bloomberg-style grids)
        widget_style: Default widget style for all widgets (overridable per-widget)
        data_source_links: Cross-dataset field links for faceting
        filters: Global filter definitions
    """
    if widget_style is None:
        widget_style = {
            "backgroundColor": "#FFFFFF",
            "borderColor": "#E0E5EE",
            "borderEdges": ["top", "right", "bottom", "left"],
            "borderRadius": 4,
            "borderWidth": 1,
        }

    # Apply dashboard-level theming to gridLayout
    if isinstance(layout, dict):
        layout.setdefault("style", {})
        layout["style"]["backgroundColor"] = bg_color
        layout["style"]["cellSpacingX"] = cell_spacing
        layout["style"]["cellSpacingY"] = cell_spacing
        layout["style"]["gutterColor"] = "transparent"
        layout["style"]["fit"] = "original"
        if row_height != "normal":
            layout["rowHeight"] = row_height

    state = {
        "steps": steps,
        "widgets": widgets,
        "gridLayouts": [layout],
        "widgetStyle": widget_style,
    }

    if data_source_links:
        state["dataSourceLinks"] = data_source_links
    if filters:
        state["filters"] = filters

    return state


def deploy_dashboard(inst, tok, dashboard_id, state):
    """Deploy dashboard state via PATCH to an existing dashboard."""
    normalized_state = normalize_dashboard_state_for_patch(state)
    body = json.dumps({"state": normalized_state})
    steps = normalized_state["steps"]
    widgets = normalized_state["widgets"]
    pages = normalized_state["gridLayouts"][0]["pages"]
    logger.info("Payload: %d bytes", len(body))
    logger.info(
        "  %d steps | %d widgets | %d pages", len(steps), len(widgets), len(pages)
    )

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
        r = _urlopen_json(req, retry_label=f"Dashboard deploy {dashboard_id}")
        logger.info("OK — %s updated", r.get("name"))
        st = r.get("state", {})
        logger.info("  Steps: %d", len(st.get("steps", {})))
        logger.info("  Widgets: %d", len(st.get("widgets", {})))
        gl = st.get("gridLayouts", [{}])[0]
        for p in gl.get("pages", []):
            logger.info(
                "  Page '%s': %d widgets", p.get("label"), len(p.get("widgets", []))
            )
        return r
    except RuntimeError as e:
        logger.error("FAIL: %s", str(e)[:2000])
        raise


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
            logger.info("  Updated dataflow: %s (%s)", df_id, name)
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
    logger.info("  Created dataflow: %s (%s)", df_id, name)
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
        logger.error("  Failed to start dataflow job")
        return False
    logger.info("  Dataflow job started: %s", job_id)

    for _ in range(poll_attempts):
        time.sleep(poll_interval)
        job = _sf_api(
            inst, tok, "GET", f"/services/data/v66.0/wave/dataflowjobs/{job_id}"
        )
        status = job.get("status", "")
        if status == "Success":
            logger.info("  Dataflow job completed successfully")
            return True
        if status in ("Failure", "Error"):
            msg = job.get("message", "Unknown error")
            logger.error("  Dataflow job FAILED: %s", msg)
            return False
        logger.info("  ... %s", status)

    logger.warning("  Timed out waiting for dataflow job")
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
    exact_matches = [
        d
        for d in result.get("dashboards", [])
        if html.unescape(d.get("label", "")) == label
    ]
    if exact_matches:
        exact_matches.sort(
            key=lambda item: item.get("lastModifiedDate", ""), reverse=True
        )
        dashboard = exact_matches[0]
        logger.info("  Found existing dashboard: %s (%s)", dashboard["id"], label)
        return dashboard["id"]

    # Search index can lag immediately after dashboard creation. Fall back to
    # a full list scan to avoid creating duplicates when an earlier create
    # succeeded but hasn't shown up in q= search yet.
    full_result = _sf_api(
        inst,
        tok,
        "GET",
        "/services/data/v66.0/wave/dashboards?pageSize=200",
    )
    matches = [
        d
        for d in full_result.get("dashboards", [])
        if html.unescape(d.get("label", "")) == label
    ]
    if matches:
        matches.sort(key=lambda item: item.get("lastModifiedDate", ""), reverse=True)
        dashboard = matches[0]
        logger.info(
            "  Found existing dashboard via full scan: %s (%s)", dashboard["id"], label
        )
        return dashboard["id"]

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
    logger.info("  Created new dashboard: %s (%s)", dashboard_id, label)
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
    matches = [
        d
        for d in result.get("dashboards", [])
        if html.unescape(d.get("label", "")) == label
    ]
    if matches:
        matches.sort(key=lambda item: item.get("lastModifiedDate", ""), reverse=True)
        return matches[0]["id"]

    full_result = _sf_api(
        inst,
        tok,
        "GET",
        "/services/data/v66.0/wave/dashboards?pageSize=200",
    )
    matches = [
        d
        for d in full_result.get("dashboards", [])
        if html.unescape(d.get("label", "")) == label
    ]
    if matches:
        matches.sort(key=lambda item: item.get("lastModifiedDate", ""), reverse=True)
        return matches[0]["id"]
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
