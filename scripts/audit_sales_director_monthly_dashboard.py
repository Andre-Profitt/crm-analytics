#!/usr/bin/env python3
"""Sales Director Monthly Dashboard - Phase 1 Audit.

Grades every widget on the standard Salesforce dashboard
01ZTb00000FSP7hMAH (Sales Directors Monthly - Pipeline and Insights)
against the expected-widgets spec at
docs/specs/sales-director-monthly-dashboard-spec.md.

Notebook style: cells separated by `# %%` markers. Re-run any cell
independently in VSCode interactive or via `python3 -i`.

Output: a two-table delta report at
docs/audits/<today>-sales-director-monthly-audit.md.

Design doc: docs/2026-04-06-sales-director-monthly-phase1-audit-design.md
Plan doc:   docs/2026-04-06-sales-director-monthly-phase1-audit-plan.md
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

import requests

# %% Constants

DASHBOARD_ID = "01ZTb00000FSP7hMAH"
SPEC_PATH = Path(
    "/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md"
)
AUDIT_OUTPUT_DIR = Path("/Users/test/crm-analytics/docs/audits")
TARGET_ORG = "apro@simcorp.com"
API_VERSION = "v66.0"
REPORT_RUN_TIMEOUT_SECONDS = 30

# Picklist the audit asserts is current
EXPECTED_PICKLIST_FIELD = "APTS_Primary_Quote_Type__c"
EXPECTED_PICKLIST_VALUES_PRESENT: set[str] = (
    set()
)  # org emptied the picklist; any active values indicate regression
EXPECTED_PICKLIST_VALUES_ABSENT = {"Quote", "Renewal"}

# Severity ranking (ordered most severe to least severe).
# Used everywhere we need to pick "the worst severity" or sort by severity.
# Lexicographic max/min on these strings would be WRONG - always go through this list.
SEVERITY_ORDER = ["BLOCKING", "WRONG-DATA", "ORPHAN", "COSMETIC", "OK"]


def _parse_args():
    """Parse argv for dashboard ID, spec path, and audit output name overrides.

    Defaults preserve Phase 1 backward compatibility.
    """
    p = argparse.ArgumentParser(
        description="Audit a Salesforce dashboard against an expected-widgets spec."
    )
    p.add_argument(
        "--dashboard-id",
        default="01ZTb00000FSP7hMAH",
        help="Salesforce dashboard ID (default: 01ZTb00000FSP7hMAH, the Phase 1 dashboard)",
    )
    p.add_argument(
        "--spec-path",
        default="/Users/test/crm-analytics/docs/specs/sales-director-monthly-dashboard-spec.md",
        help="Absolute path to the expected-widgets spec markdown file",
    )
    p.add_argument(
        "--output-name",
        default="sales-director-monthly-audit",
        help="Audit output filename stem (rundate prepended automatically). Result: docs/audits/{rundate}-{stem}.md",
    )
    p.add_argument(
        "--source-contract",
        default=None,
        help="Optional path to a source contract markdown file. When provided, the matcher uses the contract's pinned report_id -> spec_widget_id mappings as the authoritative match before falling back to stem matching. Clears matcher-vocabulary-gap false positives for widgets the source contract has explicitly pinned.",
    )
    return p.parse_args()


# %% Cell 1: Auth


def get_auth() -> tuple[str, str]:
    """Shell out to `sf org display` and extract instance URL + access token.

    Same pattern as the Option D POC. No .env files. No MCP.
    """
    r = subprocess.run(
        ["sf", "org", "display", "--target-org", TARGET_ORG, "--json"],
        capture_output=True,
        text=True,
        check=True,
    )
    # sf CLI may print a warning before the JSON; trim to the first "{"
    payload = json.loads(r.stdout[r.stdout.find("{") :])
    result = payload["result"]
    return result["instanceUrl"], result["accessToken"]


def _cell1_main() -> tuple[str, str]:
    inst, tok = get_auth()
    print(f"Auth OK - instance: {inst}")
    if not inst.startswith("https://simcorp.my.salesforce.com"):
        print(f"ERROR: unexpected instance URL: {inst}")
        sys.exit(1)
    print(f"Token length: {len(tok)} chars")
    return inst, tok


# %% Cell 2: Picklist freshness assertion


def fetch_picklist_values(inst: str, tok: str, sobject: str, field: str) -> set[str]:
    """GET the active picklist values for a single field via sobject describe."""
    url = f"{inst}/services/data/{API_VERSION}/sobjects/{sobject}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    r.raise_for_status()
    describe: dict[str, Any] = r.json()
    for f in describe.get("fields", []):
        if f.get("name") == field:
            return {
                pv["value"] for pv in f.get("picklistValues", []) if pv.get("active")
            }
    raise KeyError(f"Field {field} not found on {sobject}")


def assert_picklist_fresh(inst: str, tok: str) -> set[str]:
    """Exit the script if the picklist does not match the audit's expectations.

    The org emptied the APTS_Primary_Quote_Type__c picklist entirely in the
    migration (verified 2026-04-06 via sobject describe). This function
    asserts that the stale values (Quote, Renewal) are still absent. If
    they reappear, the audit's stale-picklist rule cannot be trusted and
    the script exits.
    """
    values = fetch_picklist_values(inst, tok, "Opportunity", EXPECTED_PICKLIST_FIELD)
    print(
        f"{EXPECTED_PICKLIST_FIELD} active picklist values: {sorted(values) if values else '(empty)'}"
    )

    missing_required = EXPECTED_PICKLIST_VALUES_PRESENT - values
    unexpected_stale = EXPECTED_PICKLIST_VALUES_ABSENT & values

    if missing_required:
        print(f"ERROR: expected picklist values missing: {sorted(missing_required)}")
        print(
            "The audit's stale-picklist rule cannot be trusted. Update the rule and re-run."
        )
        sys.exit(1)

    if unexpected_stale:
        print(f"ERROR: stale picklist values still present: {sorted(unexpected_stale)}")
        print(
            "The picklist migration the audit assumes did not happen. Update the rule and re-run."
        )
        sys.exit(1)

    print("Picklist freshness: OK")
    return values


def _cell2_main(inst: str, tok: str) -> set[str]:
    return assert_picklist_fresh(inst, tok)


# %% Cell 3: Load expected spec


def load_expected_spec(path: Path) -> dict[str, dict[str, Any]]:
    """Parse the expected-widgets markdown table into a dict keyed by Widget ID.

    Reads the spec file, finds the `## Expected widgets table` section,
    parses the markdown table rows, and returns a dict where each key is
    the Widget ID (column 2, backticks stripped) and each value is a dict
    of the other columns (also backtick-stripped).
    """
    text = path.read_text()

    section_marker = "## Expected widgets table"
    if section_marker not in text:
        raise ValueError(f"Spec missing section: {section_marker}")

    section = text.split(section_marker, 1)[1]
    # Stop at the next `## ` heading
    section = section.split("\n## ", 1)[0]

    # Parse markdown table rows (lines that start with `|`)
    lines = [ln.strip() for ln in section.splitlines() if ln.strip().startswith("|")]
    if len(lines) < 3:
        raise ValueError(
            f"Expected widgets table has fewer than 3 markdown rows: {len(lines)}"
        )

    # First line is header, second is the separator (`| --- | ...`), rest are data
    header = [c.strip() for c in lines[0].strip("|").split("|")]

    def _is_separator(line: str) -> bool:
        cells = [c.strip() for c in line.strip("|").split("|")]
        return all(set(c) <= {"-", ":", " "} for c in cells if c)

    data_lines = [ln for ln in lines[1:] if not _is_separator(ln)]

    spec: dict[str, dict[str, Any]] = {}
    for line in data_lines:
        cells = [c.strip() for c in line.strip("|").split("|")]
        if len(cells) != len(header):
            continue  # skip malformed or totals lines
        row = {k: v.strip("`").strip() for k, v in zip(header, cells)}
        # Keep filters un-stripped of backticks since filter text uses them heavily
        # Re-pull the original filter cell
        raw_cells = [c.strip() for c in line.strip("|").split("|")]
        row["Required filters"] = (
            raw_cells[header.index("Required filters")]
            if "Required filters" in header
            else ""
        )
        row["Aggregation"] = (
            raw_cells[header.index("Aggregation")] if "Aggregation" in header else ""
        )
        row["Grouping"] = (
            raw_cells[header.index("Grouping")] if "Grouping" in header else ""
        )

        widget_id = row.get("Widget ID", "").strip("`").strip()
        if not widget_id:
            continue
        spec[widget_id] = row

    return spec


# %% Cell 4: Dashboard describe


def get_dashboard_describe(inst: str, tok: str, dashboard_id: str) -> dict[str, Any]:
    """GET the standard SF dashboard metadata via Analytics REST API."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/dashboards/{dashboard_id}/describe"
    r = requests.get(
        url,
        headers={"Authorization": f"Bearer {tok}"},
        timeout=30,
    )
    if r.status_code == 404:
        print(f"ERROR: dashboard {dashboard_id} not found (404)")
        sys.exit(1)
    r.raise_for_status()
    return r.json()


def extract_widgets(describe: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract the list of components (widgets) with their effective config.

    Dashboard describe in v66.0 includes per-widget properties
    (reportFormat, groupings, aggregates) so we don't need report describe
    for that. We still need report describe for filter lists.
    """
    components = describe.get("components", [])
    widgets = []
    for c in components:
        props = c.get("properties", {}) or {}
        groupings = props.get("groupings") or []
        aggregates = props.get("aggregates") or []
        widgets.append(
            {
                "component_id": c.get("id"),
                "header": c.get("header") or "",
                "title": c.get("title") or "",
                "display_title": c.get("header") or c.get("title") or "",
                "type": c.get("type"),
                "report_id": c.get("reportId"),
                "footer": c.get("footer") or "",
                "report_format": props.get("reportFormat"),
                "groupings": [g.get("name") for g in groupings],
                "aggregates": [a.get("name") for a in aggregates],
                "raw": c,
            }
        )
    return widgets


# %% Cell 5: Report describes


def get_report_describe(inst: str, tok: str, report_id: str) -> dict[str, Any] | None:
    """GET the report metadata. Returns None on any failure; caller records BLOCKING."""
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}/describe"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {tok}"},
            timeout=30,
        )
    except requests.RequestException as e:
        print(f"  report {report_id} describe failed: {e}")
        return None
    if r.status_code == 404:
        print(f"  report {report_id} describe: 404")
        return None
    if not r.ok:
        print(f"  report {report_id} describe: {r.status_code} {r.text[:200]}")
        return None
    return r.json()


def extract_report_meta(describe: dict[str, Any] | None) -> dict[str, Any]:
    """Flatten a report describe payload into the fields the audit needs."""
    if describe is None:
        return {
            "_failed": True,
            "name": None,
            "report_format": None,
            "filters": [],
            "standard_date_filter": None,
            "groupings_down": [],
            "aggregates": [],
            "detail_columns": [],
        }
    report_metadata = describe.get("reportMetadata", {})
    return {
        "_failed": False,
        "name": report_metadata.get("name"),
        "developer_name": report_metadata.get("developerName"),
        "report_format": report_metadata.get("reportFormat"),
        "filters": report_metadata.get("reportFilters", []) or [],
        "standard_date_filter": report_metadata.get("standardDateFilter"),
        "groupings_down": [
            g.get("name") for g in (report_metadata.get("groupingsDown") or [])
        ],
        "groupings_across": [
            g.get("name") for g in (report_metadata.get("groupingsAcross") or [])
        ],
        "aggregates": report_metadata.get("aggregates", []) or [],
        "detail_columns": report_metadata.get("detailColumns", []) or [],
        "raw": describe,
    }


# %% Cell 6: Run reports


def run_report(inst: str, tok: str, report_id: str) -> dict[str, Any]:
    """Run a report synchronously via GET /analytics/reports/{id}.

    Salesforce Analytics REST has two modes:
    - GET /analytics/reports/{id}?includeDetails=true    - synchronous, one call
    - POST /analytics/reports/{id}/instances             - asynchronous, then poll

    We use the synchronous GET for simplicity. Returns a dict with `_failed`,
    `_timeout`, and the top-line value if the run succeeded.
    """
    url = f"{inst}/services/data/{API_VERSION}/analytics/reports/{report_id}?includeDetails=true"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {tok}"},
            timeout=REPORT_RUN_TIMEOUT_SECONDS,
        )
    except requests.Timeout:
        return {
            "_failed": True,
            "_timeout": True,
            "top_value": None,
            "row_count": None,
            "error": "timeout",
        }
    except requests.RequestException as e:
        return {
            "_failed": True,
            "_timeout": False,
            "top_value": None,
            "row_count": None,
            "error": str(e),
        }

    if not r.ok:
        return {
            "_failed": True,
            "_timeout": False,
            "top_value": None,
            "row_count": None,
            "error": f"{r.status_code}: {r.text[:200]}",
        }

    payload = r.json()
    fact_map = payload.get("factMap", {})
    grand_total = fact_map.get("T!T", {})
    aggregates = grand_total.get("aggregates", [])
    top_value = aggregates[0].get("value") if aggregates else None
    row_count = len(grand_total.get("rows", []) or [])
    return {
        "_failed": False,
        "_timeout": False,
        "top_value": top_value,
        "row_count": row_count,
        "has_detail": payload.get("hasDetailRows", False),
        "error": None,
    }


# %% Cell 7: Static rule scan


def _filter_column(filter_row: dict[str, Any]) -> str:
    return str(filter_row.get("column") or "").lower()


def _filter_value(filter_row: dict[str, Any]) -> str:
    return str(filter_row.get("value") or "")


def _aggregate_field(agg: Any) -> str:
    """Extract the field name from an aggregate reference like 's!AMOUNT' or 's!APTS_Renewal_ACV__c'."""
    if isinstance(agg, str):
        # Format: "<op>!<field>" where op is s(sum), a(avg), mn(min), mx(max), m(count)
        if "!" in agg:
            return agg.split("!", 1)[1]
        return agg
    if isinstance(agg, dict):
        return str(agg.get("name") or agg.get("column") or "")
    return ""


def apply_static_rules(
    report_meta: dict[str, Any],
    widget: dict[str, Any],
    kpi_bullet_hint: str = "",
) -> list[dict[str, str]]:
    """Return a list of issues found on this widget + report pair.

    Each issue is {"severity": ..., "rule": ..., "detail": ...}.
    kpi_bullet_hint is optional context from the matched spec row (if any),
    used to decide rules like "renewal widgets should aggregate ACV".
    """
    issues: list[dict[str, str]] = []
    if report_meta.get("_failed"):
        issues.append(
            {
                "severity": "BLOCKING",
                "rule": "report_describe_failed",
                "detail": "Report describe API call failed",
            }
        )
        return issues

    fmt = (report_meta.get("report_format") or "").upper()
    filters = report_meta.get("filters") or []
    standard_date_filter = report_meta.get("standard_date_filter") or {}
    aggregates = report_meta.get("aggregates") or []
    title = (
        widget.get("display_title") or widget.get("header") or widget.get("title") or ""
    )
    title_lower = title.lower()
    bullet_lower = kpi_bullet_hint.lower()

    # Rule 1: stale picklist on APTS_Primary_Quote_Type__c (the org emptied this field)
    for f in filters:
        col = _filter_column(f)
        if "apts_primary_quote_type" in col or col == "apts_primary_quote_type__c":
            issues.append(
                {
                    "severity": "BLOCKING",
                    "rule": "stale_picklist",
                    "detail": "Filter on APTS_Primary_Quote_Type__c is structurally obsolete (picklist is empty in the org). Switch to Type field (Land/Expand/Renewal).",
                }
            )

    # Rule 2: fiscal date filter instead of calendar
    duration = str(standard_date_filter.get("durationValue") or "")
    if "FISCAL" in duration:
        issues.append(
            {
                "severity": "WRONG-DATA",
                "rule": "fiscal_date_filter",
                "detail": f"Report standard date filter uses {duration} but the KPI brief requires calendar year. Switch to a custom date filter that aligns to the current calendar quarter or year.",
            }
        )

    # Rule 3: renewal widgets must aggregate on APTS_Renewal_ACV__c, not standard AMOUNT
    is_renewal_widget = ("renewal" in title_lower) or ("renewal" in bullet_lower)
    if is_renewal_widget and aggregates:
        first_agg_field = _aggregate_field(aggregates[0])
        if first_agg_field.upper() == "AMOUNT":
            issues.append(
                {
                    "severity": "BLOCKING",
                    "rule": "renewal_uses_amount_not_acv",
                    "detail": f"Renewal widget aggregates on standard AMOUNT (`{first_agg_field}`). Brief requires APTS_Renewal_ACV__c for renewal ACV.",
                }
            )
        elif (
            "APTS_Renewal_ACV__c" not in first_agg_field
            and "ACV" not in first_agg_field.upper()
        ):
            issues.append(
                {
                    "severity": "WRONG-DATA",
                    "rule": "renewal_wrong_acv_field",
                    "detail": f"Renewal widget aggregates on `{first_agg_field}`; expected APTS_Renewal_ACV__c.",
                }
            )

    # Rule 4: pipeline widgets should aggregate on APTS_Opportunity_ARR__c, not standard AMOUNT
    is_pipeline_widget = (
        "pipeline" in title_lower or "pipeline" in bullet_lower
    ) and not is_renewal_widget
    if is_pipeline_widget and aggregates:
        first_agg_field = _aggregate_field(aggregates[0])
        if first_agg_field.upper() == "AMOUNT":
            issues.append(
                {
                    "severity": "WRONG-DATA",
                    "rule": "pipeline_uses_amount_not_arr",
                    "detail": f"Pipeline widget aggregates on standard AMOUNT (`{first_agg_field}`). Brief requires APTS_Opportunity_ARR__c for ARR.",
                }
            )

    # Rule 5: TABULAR format on a Top N widget (broken ranking)
    if (
        "top" in title_lower
        and any(ch.isdigit() for ch in title_lower)
        and fmt == "TABULAR"
    ):
        issues.append(
            {
                "severity": "WRONG-DATA",
                "rule": "tabular_top_n",
                "detail": "Widget name contains 'Top N' but underlying report is TABULAR. Should be SUMMARY grouped by the top-N dimension for proper ranking.",
            }
        )

    # Rule 6: "Missing X" report without showing X in detail columns
    if title_lower.startswith("missing "):
        missing_field_hint = title_lower.replace("missing ", "").strip()
        columns = [str(c).lower() for c in (report_meta.get("detail_columns") or [])]
        if not any(
            missing_field_hint in col or col in missing_field_hint for col in columns
        ):
            issues.append(
                {
                    "severity": "WRONG-DATA",
                    "rule": "missing_field_not_shown",
                    "detail": f"Widget is 'Missing {missing_field_hint}' but the field is not in the detail columns",
                }
            )

    # Rule 7: em-dash or en-dash in widget title
    if "\u2014" in title or "\u2013" in title:
        issues.append(
            {
                "severity": "COSMETIC",
                "rule": "em_dash_in_title",
                "detail": f"Widget title contains an em-dash or en-dash: {title!r}. Replace with a hyphen.",
            }
        )

    # Rule 8: "Fiscal" in widget title violates calendar-year hard rule
    if "fiscal" in title_lower:
        issues.append(
            {
                "severity": "COSMETIC",
                "rule": "fiscal_in_title",
                "detail": f"Widget title contains the word 'fiscal': {title!r}. The brief requires calendar-year framing.",
            }
        )

    return issues


# %% Cell 8: Bidirectional comparison


# Matcher stopwords: stems that are too generic to be a strong signal on their
# own. A title that ONLY shares these with a bullet is not enough to match.
_MATCH_STOPWORDS = {
    "stage",
    "quarter",
    "quarterli",
    "rate",
    "date",
    "month",
    "region",
    "year",
    "data",
    "with",
    "from",
    "analysi",
    "overview",
    "track",
}


def _stem(word: str) -> str:
    """Very naive stemmer: drop trailing 's'/'es' for words longer than 4 chars."""
    w = word.lower()
    if len(w) > 5 and w.endswith("es"):
        return w[:-2]
    if len(w) > 4 and w.endswith("s"):
        return w[:-1]
    return w


def parse_source_contract_pinnings(
    contract_path: Path,
    spec: dict[str, dict[str, Any]],
) -> dict[str, str]:
    """Parse a source contract markdown file and return {report_id: spec_widget_id}.

    Looks for markdown table rows where one column is a backticked SF report ID
    (matching pattern `00O...`) and another column is a known spec widget ID.
    Spec widget IDs are taken from the spec dict keys to avoid false positives
    on arbitrary backticked code.

    Returns an empty dict if the file is missing or no pinnings found.
    """
    if not contract_path or not Path(contract_path).exists():
        return {}
    text = Path(contract_path).read_text(encoding="utf-8")
    spec_ids = set(spec.keys())
    pinnings: dict[str, str] = {}
    # Match SF report IDs in backticks: `00OTb000008...` or `00O2o000007...`
    report_id_pat = re.compile(r"`(00O[A-Za-z0-9]{12,15})`")
    # Walk line by line; for each line that looks like a markdown table row
    # containing a report ID, scan for any spec widget ID in the same row
    for line in text.splitlines():
        if "|" not in line:
            continue
        report_ids = report_id_pat.findall(line)
        if not report_ids:
            continue
        # Look for spec widget IDs in the same line (also backticked)
        for sid in spec_ids:
            # Match `<sid>` or just <sid> as a word in the row
            patterns = [f"`{sid}`", f" {sid} ", f" {sid},", f" {sid}.", f"|{sid}|"]
            if any(p in line for p in patterns):
                # Pin every report id in this line to this spec id (usually 1:1)
                for rid in report_ids:
                    if rid not in pinnings:
                        pinnings[rid] = sid
                break
    return pinnings


def match_widget_to_spec(
    widget: dict[str, Any],
    spec: dict[str, dict[str, Any]],
    pinnings: dict[str, str] | None = None,
) -> str | None:
    """Match a dashboard widget to a spec entry.

    Resolution order:
      1. Explicit source-contract pinning by report_id (authoritative)
      2. Stem-based fuzzy match on title vs spec KPI bullet
      3. None (orphan)

    Threshold for stem matching is 1 (not 2) because most KPI bullets are short
    phrases like 'Renewals tracking' or 'Slipped deals analysis' where a single
    distinctive word is enough signal.
    """
    # Step 1: source-contract pinning (authoritative if present)
    if pinnings:
        rid = widget.get("report_id")
        if rid and rid in pinnings:
            return pinnings[rid]

    title = (
        widget.get("display_title") or widget.get("header") or widget.get("title") or ""
    ).lower()
    if not title:
        return None
    title_stems = {_stem(w) for w in re.findall(r"\w+", title) if len(w) >= 4}
    if not title_stems:
        return None

    best_score = 0.0
    best_wid: str | None = None
    for wid, row in spec.items():
        bullet = (row.get("KPI bullet") or "").lower()
        bullet_stems = {_stem(w) for w in re.findall(r"\w+", bullet) if len(w) >= 4}
        if not bullet_stems:
            continue
        overlap = title_stems & bullet_stems
        # Each overlap stem scores 1.0 normally, 0.2 if it is a stopword.
        hit = sum(0.2 if s in _MATCH_STOPWORDS else 1.0 for s in overlap)
        if hit > best_score:
            best_score = hit
            best_wid = wid
    # Require at least 1.0 of non-stopword signal to match
    return best_wid if best_score >= 1.0 else None


def compare(
    spec: dict[str, dict[str, Any]],
    widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    report_run_by_id: dict[str, dict[str, Any]],
    report_meta_by_id: dict[str, dict[str, Any]],
    pinnings: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Run both passes (dashboard -> spec and spec -> dashboard) and return tagged entries."""
    entries: list[dict[str, Any]] = []
    matched_spec_ids: set[str] = set()

    # Pass 1: dashboard -> spec
    for w in widgets:
        matched = match_widget_to_spec(w, spec, pinnings)
        if matched:
            matched_spec_ids.add(matched)
        static = static_issues_by_widget.get(w["component_id"], [])
        run = report_run_by_id.get(w["report_id"], {})
        meta = report_meta_by_id.get(w["report_id"], {})

        # Build the static-issue summary if any (always include, regardless of match status)
        static_summary = "; ".join(i["detail"] for i in static) if static else ""

        # Determine severity. ORPHAN is its own severity UNLESS a more-severe
        # static rule also applies, in which case the stricter severity wins
        # but the issue text notes both.
        static_worst_severity = None
        if static:
            worst = min(static, key=lambda i: SEVERITY_ORDER.index(i["severity"]))
            static_worst_severity = worst["severity"]

        if not matched:
            if static_worst_severity and SEVERITY_ORDER.index(
                static_worst_severity
            ) < SEVERITY_ORDER.index("ORPHAN"):
                # Static issue is stricter than ORPHAN (BLOCKING or WRONG-DATA)
                severity = static_worst_severity
                issue = f"(also orphan: widget does not map to any spec entry). {static_summary}"
                fix = "Fix the static rule issue AND decide keep/drop/fold. See static rule detail."
            else:
                severity = "ORPHAN"
                issue = "Dashboard widget does not map to any spec entry (likely cruft from BOB/RTB clone, or a legitimate context widget the spec does not account for)."
                if static_summary:
                    issue += f" Also has static rule hits: {static_summary}"
                fix = "Decision needed: keep (add to spec), drop, or fold into an existing spec row."
        elif static_worst_severity:
            severity = static_worst_severity
            issue = static_summary
            fix = "See static rule detail"
        elif run.get("_failed") or run.get("_timeout"):
            severity = "BLOCKING"
            issue = f"Report run failed: {'timeout' if run.get('_timeout') else run.get('error', 'unknown')}"
            fix = "Fix the report or its filter"
        elif meta.get("_failed"):
            severity = "BLOCKING"
            issue = "Report describe failed"
            fix = "Verify report ID is valid and accessible"
        else:
            severity = "OK"
            issue = "Matches spec and passes static rules"
            fix = "n/a"

        entries.append(
            {
                "severity": severity,
                "widget_title": w.get("display_title")
                or w.get("header")
                or w.get("title")
                or "(untitled)",
                "widget_type": w.get("type", ""),
                "component_id": w.get("component_id"),
                "report_id": w.get("report_id"),
                "matched_spec_id": matched,
                "kpi_bullet": spec.get(matched, {}).get("KPI bullet", "")
                if matched
                else "(orphan)",
                "issue": issue,
                "fix": fix,
                "current_value": run.get("top_value"),
                "row_count": run.get("row_count"),
                "report_format": meta.get("report_format"),
                "report_name": meta.get("name"),
                "date_filter": (meta.get("standard_date_filter") or {}).get(
                    "durationValue", ""
                ),
            }
        )

    # Pass 2: spec -> dashboard (find missing spec widgets)
    for wid, row in spec.items():
        if wid not in matched_spec_ids:
            entries.append(
                {
                    "severity": "BLOCKING",
                    "widget_title": f"(MISSING) {wid}",
                    "widget_type": row.get("Type", ""),
                    "component_id": None,
                    "report_id": None,
                    "matched_spec_id": wid,
                    "kpi_bullet": row.get("KPI bullet", ""),
                    "issue": "Spec requires this widget; dashboard does not have it",
                    "fix": f"Add widget {wid} with filters: {row.get('Required filters', '')}",
                    "current_value": None,
                    "row_count": None,
                    "report_format": None,
                    "report_name": None,
                    "date_filter": "",
                }
            )

    return entries


# %% Cell 9: Markdown rendering


def _fmt_value(v: Any) -> str:
    """Human-readable number format; pass through non-numbers."""
    if v is None:
        return ""
    if isinstance(v, (int, float)):
        if abs(v) >= 1_000_000:
            return f"{v / 1_000_000:.2f}M"
        if abs(v) >= 1_000:
            return f"{v / 1_000:.1f}K"
        return f"{v:.0f}"
    return str(v)


def _escape_md_cell(text: Any) -> str:
    """Escape characters that would break a markdown table cell."""
    if text is None:
        return ""
    s = str(text)
    s = s.replace("|", "\\|").replace("\n", " ")
    return s


def render_markdown(
    entries: list[dict[str, Any]],
    dashboard_id: str,
    dashboard_name: str,
    dashboard_last_modified: str,
    spec_path: Path,
    tally: dict[str, int],
    rundate: str,
    spec_commit_hash: str = "",
) -> str:
    """Build the full delta report markdown string."""
    lines: list[str] = []
    lines.append(f"# Sales Director Monthly Dashboard Audit - {rundate}")
    lines.append("")

    lines.append("## Header")
    lines.append("")
    lines.append(f"- **Dashboard ID:** `{dashboard_id}`")
    lines.append(f"- **Dashboard name:** {dashboard_name}")
    lines.append(
        f"- **Lightning URL:** https://simcorp.my.salesforce.com/lightning/r/Dashboard/{dashboard_id}/view"
    )
    lines.append(f"- **Dashboard lastModifiedDate:** {dashboard_last_modified}")
    lines.append(f"- **Audit run date:** {rundate}")
    try:
        rel_spec = spec_path.relative_to(Path("/Users/test/crm-analytics"))
        lines.append(
            f"- **Spec graded against:** `{rel_spec}`"
            + (f" (commit `{spec_commit_hash}`)" if spec_commit_hash else "")
        )
    except ValueError:
        lines.append(f"- **Spec graded against:** `{spec_path}`")
    lines.append(
        "- **Audit script:** `scripts/audit_sales_director_monthly_dashboard.py` (uncommitted by convention)"
    )
    tally_parts = [f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER if tally.get(s, 0)]
    tally_line = " . ".join(tally_parts)
    lines.append(f"- **Tally:** {len(entries)} entries . {tally_line}")
    lines.append("")

    # Sort entries by severity then by KPI bullet for readability
    entries_sorted = sorted(
        entries,
        key=lambda e: (
            SEVERITY_ORDER.index(e["severity"])
            if e["severity"] in SEVERITY_ORDER
            else 99,
            e.get("kpi_bullet", ""),
            e.get("widget_title", ""),
        ),
    )

    lines.append("## Table 1: Executive summary")
    lines.append("")
    lines.append(
        "Sorted by severity then KPI bullet. Read this table first. Fix every BLOCKING item before the deck rebuild."
    )
    lines.append("")
    lines.append("| Severity | Widget | KPI bullet | Issue | Recommended fix |")
    lines.append("|---|---|---|---|---|")
    for e in entries_sorted:
        lines.append(
            "| {sev} | {title} | {bullet} | {issue} | {fix} |".format(
                sev=e["severity"],
                title=_escape_md_cell(e["widget_title"]),
                bullet=_escape_md_cell(e.get("kpi_bullet", "")),
                issue=_escape_md_cell(e["issue"]),
                fix=_escape_md_cell(e["fix"]),
            )
        )
    lines.append("")

    lines.append("### Severity meaning")
    lines.append("")
    lines.append(
        "- **BLOCKING** - must be fixed before deck rebuild. Wrong field, stale picklist, no data, or required-by-spec widget is missing entirely."
    )
    lines.append(
        "- **WRONG-DATA** - must be triaged before this is shown to Sales Directors. Filters partially right but value is suspect."
    )
    lines.append(
        "- **ORPHAN** - widget exists on the dashboard but maps to no spec entry. Decision needed: keep, drop, or fold into spec."
    )
    lines.append(
        "- **COSMETIC** - can ship as a follow-up. Label or column-order issue, em-dash, etc."
    )
    lines.append("- **OK** - matches spec and passes all static rules.")
    lines.append("")

    lines.append("## Table 2: Full appendix")
    lines.append("")
    lines.append(
        "Every entry, all metadata columns. Greppable for any specific widget."
    )
    lines.append("")
    lines.append(
        "| # | Widget | Type | Component | Report ID | Report name | Format | Date filter | Current value | Matched spec ID | KPI bullet | Severity | Issue |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|---|---|---|")
    for i, e in enumerate(entries_sorted, 1):
        lines.append(
            "| {i} | {title} | {wtype} | {cid} | {rid} | {rname} | {fmt} | {dfilt} | {val} | {mid} | {bullet} | {sev} | {issue} |".format(
                i=i,
                title=_escape_md_cell(e["widget_title"]),
                wtype=_escape_md_cell(e.get("widget_type", "")),
                cid=_escape_md_cell(e.get("component_id") or ""),
                rid=_escape_md_cell(e.get("report_id") or ""),
                rname=_escape_md_cell(e.get("report_name") or ""),
                fmt=_escape_md_cell(e.get("report_format") or ""),
                dfilt=_escape_md_cell(e.get("date_filter") or ""),
                val=_escape_md_cell(_fmt_value(e.get("current_value"))),
                mid=_escape_md_cell(e.get("matched_spec_id") or ""),
                bullet=_escape_md_cell(e.get("kpi_bullet", "")),
                sev=e["severity"],
                issue=_escape_md_cell(e["issue"]),
            )
        )
    lines.append("")

    lines.append("## Spec gaps surfaced during audit")
    lines.append("")
    lines.append(
        "Any entries tagged ORPHAN (dashboard widgets with no spec entry) need a keep/drop decision. Any entries with `(MISSING) widget_id` in the widget column are spec entries that the dashboard does not implement."
    )
    lines.append("")

    lines.append("## Phase 2 / 3 / 4 implications")
    lines.append("")
    lines.append(
        "- **Phase 2:** run this audit against `01ZTb00000FSP9JMAX` (Sales Ops Quarterly KPI Dashboard) by updating the DASHBOARD_ID and SPEC_PATH constants at the top of the script. Requires a Report 2 spec to exist first."
    )
    lines.append(
        "- **Phase 3:** for every OK or WRONG-DATA entry in this audit, the current_value should match the corresponding CRMA dashboard step value. Build a cross-check script that runs both and diffs them."
    )
    lines.append(
        "- **Phase 4:** deck rebuild uses Option D. Every slide chart should pull from the CRMA step that matches a NON-BLOCKING widget in this audit. Do not embed values from widgets this audit flagged as BLOCKING."
    )
    lines.append("")

    lines.append("## Reproducibility")
    lines.append("")
    lines.append("```bash")
    lines.append("cd ~/crm-analytics")
    lines.append("python3 scripts/audit_sales_director_monthly_dashboard.py")
    lines.append("```")
    lines.append("")
    if spec_commit_hash:
        lines.append(f"Spec commit graded against: `{spec_commit_hash}`")
        lines.append("")
    return "\n".join(lines) + "\n"


def _cell9_main(
    entries: list[dict[str, Any]],
    dashboard_id: str,
    dashboard_name: str,
    dashboard_last_modified: str,
    tally: dict[str, int],
    rundate: str,
    spec_commit_hash: str,
) -> str:
    # Inline tests for the render function
    _tiny_entries = [
        {
            "severity": "BLOCKING",
            "widget_title": "W1",
            "widget_type": "Report",
            "kpi_bullet": "Pipeline",
            "issue": "stale picklist",
            "fix": "switch to Type",
            "component_id": "c1",
            "report_id": "r1",
            "current_value": None,
            "row_count": 0,
            "matched_spec_id": "w1",
            "report_format": "SUMMARY",
            "report_name": "Pipeline r1",
            "date_filter": "THIS_FISCAL_YEAR",
        },
        {
            "severity": "OK",
            "widget_title": "W2",
            "widget_type": "Report",
            "kpi_bullet": "Renewals",
            "issue": "Matches spec",
            "fix": "n/a",
            "component_id": "c2",
            "report_id": "r2",
            "current_value": 1_000_000,
            "row_count": 1,
            "matched_spec_id": "w2",
            "report_format": "SUMMARY",
            "report_name": "Renewals r2",
            "date_filter": "CUSTOM",
        },
    ]
    _md = render_markdown(
        _tiny_entries,
        "01ZTbTEST",
        "Test Dashboard",
        "2026-04-06",
        SPEC_PATH,
        {"BLOCKING": 1, "OK": 1},
        "2026-04-07",
        spec_commit_hash="abcd1234",
    )
    assert "Table 1: Executive summary" in _md
    assert "Table 2: Full appendix" in _md
    assert "| BLOCKING |" in _md
    assert "| OK |" in _md
    # BLOCKING must come before OK in the sorted output
    assert _md.index("| BLOCKING |") < _md.index("| OK |"), "severity sort order wrong"
    assert "1.00M" in _md, "number formatter not firing on the 1,000,000 test value"

    print("Cell 9 tests: PASS")

    return render_markdown(
        entries,
        dashboard_id=dashboard_id,
        dashboard_name=dashboard_name,
        dashboard_last_modified=dashboard_last_modified,
        spec_path=SPEC_PATH,
        tally=tally,
        rundate=rundate,
        spec_commit_hash=spec_commit_hash,
    )


def _cell8_main(
    spec: dict[str, dict[str, Any]],
    widgets: list[dict[str, Any]],
    static_issues_by_widget: dict[str, list[dict[str, str]]],
    report_run_by_id: dict[str, dict[str, Any]],
    report_meta_by_id: dict[str, dict[str, Any]],
    pinnings: dict[str, str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    # Inline tests with fixture data
    _tiny_spec = {
        "widget_a": {
            "KPI bullet": "Pipeline overview with quarterly focus",
            "Required filters": "IsClosed=false",
            "Type": "chart",
        },
        "widget_b": {
            "KPI bullet": "Renewals tracking",
            "Required filters": "Type=Renewal",
            "Type": "metric",
        },
    }
    _tiny_dashboard = [
        {
            "component_id": "c1",
            "display_title": "Pipeline overview EMEA quarterly",
            "type": "Report",
            "report_id": "r1",
        },
        {
            "component_id": "c2",
            "display_title": "Random unrelated tile",
            "type": "Report",
            "report_id": "r2",
        },
    ]
    _tiny_static: dict[str, list[dict[str, str]]] = {"c1": [], "c2": []}
    _tiny_runs = {
        "r1": {"_failed": False, "_timeout": False, "top_value": 100, "row_count": 0},
        "r2": {"_failed": False, "_timeout": False, "top_value": 50, "row_count": 0},
    }
    _tiny_metas = {
        "r1": {
            "_failed": False,
            "report_format": "SUMMARY",
            "name": "Pipeline Coverage",
            "standard_date_filter": {},
        },
        "r2": {
            "_failed": False,
            "report_format": "SUMMARY",
            "name": "Misc",
            "standard_date_filter": {},
        },
    }

    _entries = compare(
        _tiny_spec, _tiny_dashboard, _tiny_static, _tiny_runs, _tiny_metas
    )

    c1 = [e for e in _entries if e["component_id"] == "c1"]
    assert len(c1) == 1 and c1[0]["severity"] == "OK", f"c1 should be OK, got {c1}"

    c2 = [e for e in _entries if e["component_id"] == "c2"]
    assert len(c2) == 1 and c2[0]["severity"] == "ORPHAN", (
        f"c2 should be ORPHAN, got {c2}"
    )

    missing = [
        e
        for e in _entries
        if e["matched_spec_id"] == "widget_b" and e["component_id"] is None
    ]
    assert len(missing) == 1 and missing[0]["severity"] == "BLOCKING", (
        f"widget_b should be MISSING BLOCKING, got {missing}"
    )

    # Test: static BLOCKING beats WRONG-DATA in severity ranking
    _bug_static = {
        "c1": [
            {"severity": "WRONG-DATA", "rule": "fake", "detail": "not as bad"},
            {"severity": "BLOCKING", "rule": "fake2", "detail": "worse"},
        ],
        "c2": [],
    }
    _bug_entries = compare(
        _tiny_spec, _tiny_dashboard[:1], _bug_static, _tiny_runs, _tiny_metas
    )
    assert _bug_entries[0]["severity"] == "BLOCKING", (
        f"Should pick BLOCKING over WRONG-DATA via SEVERITY_ORDER, got {_bug_entries[0]['severity']}"
    )

    print("Cell 8 tests: PASS")

    # Apply to the real data
    entries = compare(
        spec,
        widgets,
        static_issues_by_widget,
        report_run_by_id,
        report_meta_by_id,
        pinnings,
    )
    tally: dict[str, int] = {}
    for e in entries:
        tally[e["severity"]] = tally.get(e["severity"], 0) + 1
    print(f"Audit entries: {len(entries)} total")
    for sev in SEVERITY_ORDER:
        if tally.get(sev):
            print(f"  {sev:<12}  {tally[sev]}")
    return entries, tally


def _cell7_main(
    widgets: list[dict[str, Any]],
    report_meta_by_id: dict[str, dict[str, Any]],
    expected_spec: dict[str, dict[str, Any]],
) -> dict[str, list[dict[str, str]]]:
    # Inline tests with fixture data
    _fake_report_stale = {
        "_failed": False,
        "report_format": "SUMMARY",
        "filters": [
            {
                "column": "APTS_Primary_Quote_Type__c",
                "operator": "equals",
                "value": "Renewal",
            }
        ],
        "standard_date_filter": None,
        "aggregates": ["s!AMOUNT"],
        "detail_columns": [],
    }
    issues = apply_static_rules(
        _fake_report_stale,
        {"display_title": "Renewal pipeline by quarter"},
        "Renewals tracking",
    )
    assert any(i["rule"] == "stale_picklist" for i in issues), (
        f"Expected stale_picklist, got {issues}"
    )
    assert any(i["rule"] == "renewal_uses_amount_not_acv" for i in issues), (
        f"Expected renewal_uses_amount_not_acv, got {issues}"
    )

    _fake_report_fiscal = {
        "_failed": False,
        "report_format": "SUMMARY",
        "filters": [{"column": "Type", "operator": "equals", "value": "Renewal"}],
        "standard_date_filter": {"durationValue": "THIS_FISCAL_QUARTER"},
        "aggregates": ["s!APTS_Renewal_ACV__c"],
        "detail_columns": [],
    }
    issues = apply_static_rules(
        _fake_report_fiscal,
        {"display_title": "Renewal pipeline this quarter"},
        "Renewals tracking",
    )
    assert any(i["rule"] == "fiscal_date_filter" for i in issues), (
        f"Expected fiscal_date_filter, got {issues}"
    )
    assert not any(i["rule"] == "renewal_uses_amount_not_acv" for i in issues), (
        f"Should not flag renewal_uses_amount_not_acv when agg is APTS_Renewal_ACV__c, got {issues}"
    )

    _fake_report_ok = {
        "_failed": False,
        "report_format": "SUMMARY",
        "filters": [{"column": "Type", "operator": "equals", "value": "Renewal"}],
        "standard_date_filter": {"durationValue": "CUSTOM"},
        "aggregates": ["s!APTS_Renewal_ACV__c"],
        "detail_columns": [],
    }
    issues = apply_static_rules(
        _fake_report_ok,
        {"display_title": "Renewal ACV by quarter"},
        "Renewals tracking",
    )
    assert issues == [], f"Expected no issues, got {issues}"

    print("Cell 7 tests: PASS")

    # Build a quick widget-title to kpi-bullet hint map from the spec.
    # This is a coarse hint used only by rules 3-4; precise matching happens in cell 8.
    def _hint_for(widget: dict[str, Any]) -> str:
        t = (widget.get("display_title") or "").lower()
        for wid, row in expected_spec.items():
            bullet = (row.get("KPI bullet") or "").lower()
            words = [w for w in bullet.split() if len(w) >= 4]
            if any(w in t for w in words):
                return row.get("KPI bullet", "")
        return ""

    static_issues_by_widget: dict[str, list[dict[str, str]]] = {}
    for w in widgets:
        rid = w["report_id"]
        rmeta = report_meta_by_id.get(rid, {"_failed": True})
        hint = _hint_for(w)
        static_issues_by_widget[w["component_id"]] = apply_static_rules(rmeta, w, hint)

    total_static = sum(len(v) for v in static_issues_by_widget.values())
    rule_counts: dict[str, int] = {}
    for issues in static_issues_by_widget.values():
        for i in issues:
            rule_counts[i["rule"]] = rule_counts.get(i["rule"], 0) + 1
    print(f"Static rule scan: {total_static} issues across {len(widgets)} widgets")
    for rule, count in sorted(rule_counts.items(), key=lambda kv: -kv[1]):
        print(f"  {rule:<32}  {count}")

    return static_issues_by_widget


def _cell6_main(
    inst: str, tok: str, widgets: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    report_run_by_id: dict[str, dict[str, Any]] = {}
    unique_report_ids = sorted({w["report_id"] for w in widgets if w["report_id"]})
    print(
        f"Running {len(unique_report_ids)} reports (timeout {REPORT_RUN_TIMEOUT_SECONDS}s each)"
    )
    for rid in unique_report_ids:
        result = run_report(inst, tok, rid)
        report_run_by_id[rid] = result
        if result["_timeout"]:
            print(f"  {rid}  TIMEOUT")
        elif result["_failed"]:
            err = (result.get("error") or "")[:60]
            print(f"  {rid}  FAIL  {err}")
        else:
            top = result["top_value"]
            top_str = f"{top!r}" if top is not None else "-"
            print(f"  {rid}  OK    top={top_str:<24}  rows={result['row_count']}")
    return report_run_by_id


def _cell5_main(
    inst: str, tok: str, widgets: list[dict[str, Any]]
) -> dict[str, dict[str, Any]]:
    report_meta_by_id: dict[str, dict[str, Any]] = {}
    unique_report_ids = sorted({w["report_id"] for w in widgets if w["report_id"]})
    print(f"Fetching describe for {len(unique_report_ids)} unique reports")
    for rid in unique_report_ids:
        describe = get_report_describe(inst, tok, rid)
        meta = extract_report_meta(describe)
        report_meta_by_id[rid] = meta
        failed = meta["_failed"]
        fmt = meta.get("report_format") or "?"
        name = (meta.get("name") or "?")[:50]
        date_col = (meta.get("standard_date_filter") or {}).get("durationValue", "")
        agg_count = len(meta.get("aggregates") or [])
        filt_count = len(meta.get("filters") or [])
        status = "FAIL" if failed else "OK  "
        print(
            f"  {rid}  {status}  {fmt:<8}  filters={filt_count}  aggs={agg_count}  date={date_col:<22}  {name}"
        )
    return report_meta_by_id


def _cell4_main(inst: str, tok: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    describe = get_dashboard_describe(inst, tok, DASHBOARD_ID)
    widgets = extract_widgets(describe)
    print(f"Dashboard {DASHBOARD_ID}: {describe.get('name')!r}")
    print(
        f"  folder: {describe.get('folderName')}  running user: {describe.get('runningUser', {}).get('displayName')}"
    )
    print(f"  {len(widgets)} widgets")
    for i, w in enumerate(widgets, 1):
        title_short = (w["display_title"] or "")[:60]
        fmt = w["report_format"] or ""
        print(
            f"  {i:2d}. {w['type']:<24}  {fmt:<8}  report={w['report_id']}  {title_short}"
        )
    return describe, widgets


def _cell3_main() -> dict[str, dict[str, Any]]:
    """Run cell 3: load the spec and run inline tests."""
    _spec = load_expected_spec(SPEC_PATH)
    print(f"Loaded {len(_spec)} expected widgets from spec")

    # Test 1: at least 14 widgets
    assert len(_spec) >= 14, f"Expected at least 14 widgets in spec, got {len(_spec)}"

    # Tests 2-5 are Report-1-specific (pipeline_overview_global / renewal_acv_this_quarter /
    # commercial_approval_global / land_stage3_no_approval_nam).  Skip them when running
    # against a different spec (e.g. Report 2 / Sales Ops Quarterly).

    # Test 2: pipeline_overview_global exists with correct KPI bullet (Report 1 only)
    if "pipeline_overview_global" in _spec:
        bullet = _spec["pipeline_overview_global"].get("KPI bullet", "")
        assert "Pipeline overview" in bullet, (
            f"pipeline_overview_global KPI bullet wrong: {bullet!r}"
        )

    # Test 3: renewal_acv_this_quarter uses ACV (not ARR) in aggregation (Report 1 only)
    if "renewal_acv_this_quarter" in _spec:
        agg = _spec["renewal_acv_this_quarter"].get("Aggregation", "")
        assert "ACV" in agg, f"renewal_acv_this_quarter must use ACV, got: {agg!r}"

    # Test 4: commercial_approval_global uses Stage_20_Approval__c (Report 1 only)
    if "commercial_approval_global" in _spec:
        ca = _spec["commercial_approval_global"]
        ca_group = ca.get("Grouping", "")
        assert "Stage_20_Approval__c" in ca_group, (
            f"commercial_approval_global Grouping should reference Stage_20_Approval__c, got: {ca_group!r}"
        )

    # Test 5: land_stage3_no_approval_nam filters on Sales_Region__c (Report 1 only)
    if "land_stage3_no_approval_nam" in _spec:
        ls3 = _spec["land_stage3_no_approval_nam"]
        ls3_filters = ls3.get("Required filters", "")
        assert "Sales_Region__c" in ls3_filters, (
            f"land_stage3_no_approval_nam should use Sales_Region__c, got: {ls3_filters!r}"
        )

    # Test 6 (Report 2): verify key Report-2 sections are present when this is the R2 spec
    if "dq_kyc_not_completed" in _spec:
        # Section 1 CRM data quality anchor
        assert "dq_missing_decision_reason" in _spec, (
            "Report 2 spec missing dq_missing_decision_reason"
        )
        # Section 2 process compliance anchor
        assert "pc_next_step_documented" in _spec, (
            "Report 2 spec missing pc_next_step_documented"
        )
        # Section 3 forecast accuracy anchor
        assert "fa_quarterly_realized_vs_commit" in _spec, (
            "Report 2 spec missing fa_quarterly_realized_vs_commit"
        )
        # Section 4 pipeline hygiene anchor
        assert "ph_overdue_opportunities" in _spec, (
            "Report 2 spec missing ph_overdue_opportunities"
        )

    print("Cell 3 tests: PASS")
    return _spec


# %% Cell 10: Composition - write the audit markdown to disk


def _get_spec_commit_hash() -> str:
    """Get the short commit hash of the spec file so the audit records what it graded against."""
    try:
        r = subprocess.run(
            ["git", "log", "-1", "--format=%h", "--", str(SPEC_PATH)],
            cwd="/Users/test/crm-analytics",
            capture_output=True,
            text=True,
            check=True,
        )
        return r.stdout.strip()
    except subprocess.CalledProcessError:
        return ""


def _cell10_main(
    entries: list[dict[str, Any]],
    dashboard_describe: dict[str, Any],
    tally: dict[str, int],
    output_stem: str = "sales-director-monthly-audit",
) -> Path:
    """Render the markdown, write it to docs/audits/<today>.md, return the path."""
    rundate = dt.date.today().isoformat()
    dashboard_name = dashboard_describe.get("name") or "(unnamed)"
    dashboard_last_modified = ""
    components = dashboard_describe.get("components") or []
    if components:
        # Use the max lastModifiedDate across components as a drift signal
        dashboard_last_modified = max(
            (c.get("lastModifiedDate") or "" for c in components),
            default="",
        )

    spec_commit_hash = _get_spec_commit_hash()

    md = _cell9_main(
        entries=entries,
        dashboard_id=DASHBOARD_ID,
        dashboard_name=dashboard_name,
        dashboard_last_modified=dashboard_last_modified,
        tally=tally,
        rundate=rundate,
        spec_commit_hash=spec_commit_hash,
    )

    AUDIT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = AUDIT_OUTPUT_DIR / f"{rundate}-{output_stem}.md"
    out_path.write_text(md)
    print(f"Wrote {out_path}  ({len(md)} bytes)")
    tally_parts = [f"{tally.get(s, 0)} {s}" for s in SEVERITY_ORDER if tally.get(s, 0)]
    print(f"Tally: {len(entries)} entries . " + " . ".join(tally_parts))
    return out_path


def main() -> None:
    global DASHBOARD_ID, SPEC_PATH
    args = _parse_args()
    DASHBOARD_ID = args.dashboard_id
    SPEC_PATH = Path(args.spec_path)
    _OUTPUT_STEM = args.output_name

    inst, tok = _cell1_main()
    picklist_values = _cell2_main(inst, tok)
    expected_spec = _cell3_main()
    dashboard_describe, dashboard_widgets = _cell4_main(inst, tok)
    report_meta_by_id = _cell5_main(inst, tok, dashboard_widgets)
    report_run_by_id = _cell6_main(inst, tok, dashboard_widgets)
    static_issues_by_widget = _cell7_main(
        dashboard_widgets, report_meta_by_id, expected_spec
    )

    # Optional: load source contract pinnings to override matcher
    pinnings: dict[str, str] = {}
    if args.source_contract:
        contract_path = Path(args.source_contract)
        pinnings = parse_source_contract_pinnings(contract_path, expected_spec)
        if pinnings:
            print(
                f"Loaded {len(pinnings)} report_id -> spec_widget_id pinning(s) "
                f"from {contract_path}"
            )
        else:
            print(
                f"WARNING: source contract {contract_path} loaded but no pinnings parsed"
            )

    audit_entries, tally = _cell8_main(
        expected_spec,
        dashboard_widgets,
        static_issues_by_widget,
        report_run_by_id,
        report_meta_by_id,
        pinnings if pinnings else None,
    )
    audit_out_path = _cell10_main(
        audit_entries, dashboard_describe, tally, output_stem=_OUTPUT_STEM
    )
    print(f"Audit complete: {audit_out_path}")


if __name__ == "__main__":
    main()
