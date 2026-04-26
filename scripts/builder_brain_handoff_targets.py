#!/usr/bin/env python3
"""Validate and resolve builder-brain follow-up surface targets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from datetime import datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import report_surface_intelligence

REGISTRY_PATH = ROOT / "config" / "builder_brain_handoff_targets.json"
CONTEXT_REGISTRY_PATH = ROOT / "config" / "context_registry.json"
EXCELLENCE_LIBRARY_PATH = ROOT / "config" / "builder_brain_excellence_library.json"
DEFAULT_TARGET_ORG = "apro@simcorp.com"

ALLOWED_TARGET_SURFACE_TYPES = {
    "salesforce_report",
    "crma_dashboard",
    "salesforce_dashboard",
}
DESTINATION_TYPES = {
    "salesforce_report": "report",
    "crma_dashboard": "dashboard",
    "salesforce_dashboard": "dashboard",
}
REPORT_ID_RE = re.compile(r"^00O[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$")
CRMA_DASHBOARD_ID_RE = re.compile(r"^0FK[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$")
SF_DASHBOARD_ID_RE = re.compile(r"^01Z[A-Za-z0-9]{12}(?:[A-Za-z0-9]{3})?$")
GENERIC_WORDS = {
    "and",
    "control",
    "dashboard",
    "follow",
    "for",
    "list",
    "management",
    "manager",
    "operating",
    "queue",
    "report",
    "rhythm",
    "surface",
    "system",
    "the",
    "tower",
    "view",
}


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    command: str,
    command_class: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "builder_brain_handoff_targets",
        "lane": "intelligence_control",
        "command_class": command_class,
        "command": command,
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def normalize_text(value: str | None) -> str:
    if not value:
        return ""
    lowered = value.lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", lowered)
    return re.sub(r"\s+", " ", cleaned).strip()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def load_registry(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    if "targets" not in payload:
        raise ValueError(f"{path}: missing targets")
    if not isinstance(payload["targets"], list):
        raise ValueError(f"{path}: targets must be a list")
    return payload


def load_context_registry() -> dict[str, Any]:
    return load_json(CONTEXT_REGISTRY_PATH)


def load_excellence_library() -> dict[str, Any]:
    return load_json(EXCELLENCE_LIBRARY_PATH)


def build_source_lookup() -> dict[str, dict[str, Any]]:
    excellence_library = load_excellence_library()
    patterns = excellence_library.get("patterns", {})
    lookup: dict[str, dict[str, Any]] = {}
    for item in load_context_registry().get("dashboards", []):
        surface_id = item.get("id")
        if not surface_id:
            continue
        lookup[surface_id] = {
            "source_surface_id": surface_id,
                "source_surface_label": item.get("name"),
                "source_surface_type": "crma_dashboard",
                "resolution_source": "context_registry",
            }
    for item in excellence_library.get("surface_exemplars", []):
        surface_id = item.get("id")
        if not surface_id:
            continue
        pattern = patterns.get(item.get("pattern_id"), {})
        lookup.setdefault(
            surface_id,
            {
                "source_surface_id": surface_id,
                "source_surface_label": item.get("label"),
                "source_surface_type": item.get("primary_surface"),
                "resolution_source": "builder_brain_excellence_library",
                "pattern_id": item.get("pattern_id"),
                "preferred_follow_up_surface": pattern.get("preferred_secondary_surface"),
                "requires_action_layer": bool(pattern.get("requires_action_layer")),
                "page_model": item.get("page_model") or pattern.get("preferred_page_model") or [],
            },
        )
    return lookup


def extract_dashboard_id_from_url(live_url: str | None) -> str | None:
    if not live_url:
        return None
    match = re.search(r"/analytics/dashboard/([^/?#]+)", live_url)
    if match:
        return match.group(1)
    return None


def context_dashboard_target(target_id: str | None, target_label: str | None) -> dict[str, Any] | None:
    if not target_id and not target_label:
        return None
    normalized_id = normalize_text(target_id)
    normalized_label = normalize_text(target_label)
    for item in load_context_registry().get("dashboards", []):
        item_id = item.get("id")
        live_id = extract_dashboard_id_from_url(item.get("live_url"))
        item_label = item.get("name")
        if normalized_id and normalize_text(item_id) == normalized_id:
            return {
                "Id": live_id,
                "ContextId": item_id,
                "Name": item_label,
                "FolderName": item.get("surface_set_id"),
            }
        if normalized_id and live_id and normalize_text(live_id) == normalized_id:
            return {
                "Id": live_id,
                "ContextId": item_id,
                "Name": item_label,
                "FolderName": item.get("surface_set_id"),
            }
        if normalized_label and normalize_text(item_label) == normalized_label:
            return {
                "Id": live_id,
                "ContextId": item_id,
                "Name": item_label,
                "FolderName": item.get("surface_set_id"),
            }
    return None


def registry_summary(registry: dict[str, Any]) -> dict[str, Any]:
    by_target_surface: dict[str, int] = {}
    resolved = 0
    hinted = 0
    for item in registry.get("targets", []):
        target_surface_type = item.get("target_surface_type") or "unknown"
        by_target_surface[target_surface_type] = by_target_surface.get(target_surface_type, 0) + 1
        if item.get("target_surface_id"):
            resolved += 1
        elif item.get("preferred_search_terms"):
            hinted += 1
    total = len(registry.get("targets", []))
    return {
        "total_targets": total,
        "resolved_targets": resolved,
        "unresolved_targets": total - resolved,
        "discovery_hint_targets": hinted,
        "by_target_surface_type": by_target_surface,
    }


def enrich_target(item: dict[str, Any], source_lookup: dict[str, dict[str, Any]]) -> dict[str, Any]:
    source_metadata = source_lookup.get(item.get("source_surface_id"), {})
    enriched = dict(item)
    if source_metadata:
        enriched.update(
            {
                "source_surface_label": source_metadata.get("source_surface_label"),
                "source_surface_type": source_metadata.get("source_surface_type"),
                "source_resolution_source": source_metadata.get("resolution_source"),
            }
        )
    enriched["resolved"] = bool(item.get("target_surface_id"))
    enriched["has_discovery_hints"] = bool(item.get("preferred_search_terms"))
    return enriched


def _soql_escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def run_sf_query(*, target_org: str, query: str) -> list[dict[str, Any]]:
    result = subprocess.run(
        ["sf", "data", "query", "--target-org", target_org, "--json", "--query", query],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "sf data query failed"
        raise RuntimeError(detail)
    payload = json.loads(result.stdout)
    if payload.get("status") != 0:
        detail = payload.get("message") or payload.get("name") or "sf data query returned non-zero status"
        raise RuntimeError(str(detail))
    records = payload.get("result", {}).get("records", [])
    if not isinstance(records, list):
        raise RuntimeError("sf data query returned invalid records payload")
    return records


def load_org_session(target_org: str) -> dict[str, str]:
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--json"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "sf org display failed"
        raise RuntimeError(detail)
    payload = json.loads(result.stdout)
    org_result = payload.get("result") or {}
    access_token = org_result.get("accessToken")
    instance_url = org_result.get("instanceUrl")
    if not access_token or not instance_url:
        raise RuntimeError("sf org display did not return accessToken and instanceUrl")
    return {"access_token": access_token, "instance_url": instance_url}


def run_rest_json(*, instance_url: str, access_token: str, path: str) -> dict[str, Any]:
    request = Request(
        f"{instance_url}{path}",
        headers={"Authorization": f"Bearer {access_token}"},
    )
    try:
        with urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"REST request failed ({exc.code}): {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"REST request failed: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError("REST response was not a JSON object")
    return payload


def _grouping_count(grouping_payload: Any) -> int:
    if not isinstance(grouping_payload, dict):
        return 0
    groupings = grouping_payload.get("groupings")
    if isinstance(groupings, list):
        return len(groupings)
    return 0


def fingerprint_report_description(payload: dict[str, Any]) -> dict[str, Any]:
    report_metadata = payload.get("reportMetadata", {})
    detail_columns = report_metadata.get("detailColumns", [])
    if not isinstance(detail_columns, list):
        detail_columns = []
    report_filters = report_metadata.get("reportFilters", [])
    if not isinstance(report_filters, list):
        report_filters = []

    return {
        "report_name": payload.get("reportName") or report_metadata.get("name"),
        "report_format": report_metadata.get("reportFormat"),
        "detail_column_count": len(detail_columns),
        "detail_columns_preview": detail_columns[:6],
        "report_filter_count": len(report_filters),
        "groupings_down_count": _grouping_count(report_metadata.get("groupingsDown")),
        "groupings_across_count": _grouping_count(report_metadata.get("groupingsAcross")),
        "has_standard_date_filter": bool(report_metadata.get("standardDateFilter")),
    }


def describe_salesforce_report(*, target_org: str, report_id: str) -> dict[str, Any]:
    session = load_org_session(target_org)
    payload = run_rest_json(
        instance_url=session["instance_url"],
        access_token=session["access_token"],
        path=f"/services/data/v66.0/analytics/reports/{report_id}/describe",
    )
    return fingerprint_report_description(payload)


def fetch_salesforce_report_record(*, target_org: str, report_id: str) -> dict[str, Any]:
    records = query_salesforce_reports(target_org=target_org, report_id=report_id)
    if not records:
        raise RuntimeError(f"Live org did not return report {report_id}.")
    return records[0]


def query_salesforce_reports(
    *,
    target_org: str,
    report_id: str | None = None,
    search_terms: list[str] | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if report_id:
        escaped_id = _soql_escape(report_id)
        query = (
            "SELECT Id, DeveloperName, Name, FolderName, LastModifiedDate, LastViewedDate, LastRunDate "
            f"FROM Report WHERE Id = '{escaped_id}' LIMIT 1"
        )
        return run_sf_query(target_org=target_org, query=query)

    cleaned_terms = [term.strip() for term in search_terms or [] if term.strip()]
    if not cleaned_terms:
        return []

    clauses: list[str] = []
    for term in cleaned_terms:
        escaped = _soql_escape(term)
        clauses.append(
            f"(Name LIKE '%{escaped}%' OR DeveloperName LIKE '%{escaped}%' OR FolderName LIKE '%{escaped}%')"
        )
    where_clause = " OR ".join(clauses)
    query = (
        "SELECT Id, DeveloperName, Name, FolderName, LastModifiedDate, LastViewedDate, LastRunDate "
        f"FROM Report WHERE {where_clause} ORDER BY LastModifiedDate DESC LIMIT {limit}"
    )
    return run_sf_query(target_org=target_org, query=query)


def _tokenize(value: str | None) -> list[str]:
    normalized = normalize_text(value)
    if not normalized:
        return []
    return [item for item in normalized.split(" ") if item and item not in GENERIC_WORDS]


def _parse_sf_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        return None


def _datetime_sort_key(value: str | None) -> float:
    parsed = _parse_sf_datetime(value)
    if not parsed:
        return 0.0
    return parsed.timestamp()


def score_live_report_candidate(
    record: dict[str, Any],
    *,
    source_surface: dict[str, Any],
    search_terms: list[str],
) -> dict[str, Any]:
    name = record.get("Name") or ""
    developer_name = record.get("DeveloperName") or ""
    folder_name = record.get("FolderName") or ""
    normalized_name = normalize_text(name)
    normalized_developer = normalize_text(developer_name)
    source_label = source_surface.get("source_surface_label") or source_surface.get("source_surface_id") or ""
    source_tokens = set(_tokenize(source_label))

    score = 0
    reasons: list[str] = []
    exact_term_hits = 0
    token_hits = 0

    for term in search_terms:
        normalized_term = normalize_text(term)
        term_tokens = set(_tokenize(term))
        if normalized_term and normalized_term == normalized_name:
            score += 50
            exact_term_hits += 1
            reasons.append(f"exact name match: {term}")
        elif normalized_term and normalized_term == normalized_developer:
            score += 45
            exact_term_hits += 1
            reasons.append(f"exact developer name match: {term}")
        elif normalized_term and normalized_term in normalized_name:
            score += 20
            reasons.append(f"name contains term: {term}")
        elif normalized_term and normalized_term in normalized_developer:
            score += 18
            reasons.append(f"developer name contains term: {term}")

        overlap = term_tokens & (set(_tokenize(name)) | set(_tokenize(developer_name)) | set(_tokenize(folder_name)))
        if overlap:
            overlap_score = len(overlap) * 4
            score += overlap_score
            token_hits += len(overlap)
            reasons.append(f"token overlap ({', '.join(sorted(overlap))})")

    source_overlap = source_tokens & (set(_tokenize(name)) | set(_tokenize(developer_name)))
    if source_overlap:
        score += len(source_overlap) * 2
        reasons.append(f"source label overlap ({', '.join(sorted(source_overlap))})")

    last_viewed = _parse_sf_datetime(record.get("LastViewedDate"))
    last_modified = _parse_sf_datetime(record.get("LastModifiedDate"))
    last_run = _parse_sf_datetime(record.get("LastRunDate"))
    if last_viewed:
        score += 6
        reasons.append("has LastViewedDate")
    if last_run:
        score += 8
        reasons.append("has LastRunDate")
    if last_modified:
        score += 2

    return {
        **record,
        "score": score,
        "score_detail": {
            "exact_term_hits": exact_term_hits,
            "token_hits": token_hits,
            "source_overlap_hits": len(source_overlap),
            "has_last_viewed": bool(last_viewed),
            "has_last_run": bool(last_run),
            "has_last_modified": bool(last_modified),
        },
        "score_reasons": reasons,
    }


def rank_live_report_candidates(
    records: list[dict[str, Any]],
    *,
    source_surface: dict[str, Any],
    search_terms: list[str],
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    ranked = [
        score_live_report_candidate(
            record,
            source_surface=source_surface,
            search_terms=search_terms,
        )
        for record in records
    ]
    ranked.sort(
        key=lambda item: (
            item.get("score", 0),
            _datetime_sort_key(item.get("LastViewedDate")),
            _datetime_sort_key(item.get("LastModifiedDate")),
            item.get("Id", ""),
        ),
        reverse=True,
    )
    if not ranked:
        return ranked, None

    top = ranked[0]
    next_score = ranked[1]["score"] if len(ranked) > 1 else -1
    duplicate_name_count = sum(1 for item in ranked if normalize_text(item.get("Name")) == normalize_text(top.get("Name")))
    confidence = "low"
    if top["score"] >= 70 and next_score <= top["score"] - 15 and duplicate_name_count == 1:
        confidence = "high"
    elif top["score"] >= 50:
        confidence = "medium"

    suggestion = {
        "Id": top.get("Id"),
        "Name": top.get("Name"),
        "DeveloperName": top.get("DeveloperName"),
        "FolderName": top.get("FolderName"),
        "destination_type": "report",
        "score": top.get("score"),
        "confidence": confidence,
        "reason_summary": top.get("score_reasons", [])[:4],
        "duplicate_name_count": duplicate_name_count,
    }
    if confidence != "high":
        suggestion["review_required"] = True
    return ranked, suggestion


def summarize_report_comparison(
    reports: list[dict[str, Any]],
) -> dict[str, Any]:
    comparable_fields = (
        "Name",
        "DeveloperName",
        "FolderName",
        "LastViewedDate",
        "LastRunDate",
        "LastModifiedDate",
        "report_format",
        "detail_column_count",
        "report_filter_count",
        "groupings_down_count",
        "groupings_across_count",
        "has_standard_date_filter",
    )
    identical_fields: dict[str, Any] = {}
    differing_fields: dict[str, dict[str, Any]] = {}
    for field in comparable_fields:
        values = {
            item["Id"]: (
                item.get(field)
                if field in item
                else (item.get("report_fingerprint") or {}).get(field)
            )
            for item in reports
        }
        distinct_values = {json.dumps(value, sort_keys=True) for value in values.values()}
        if len(distinct_values) == 1:
            identical_fields[field] = next(iter(values.values()))
        else:
            differing_fields[field] = values
    return {
        "compared_count": len(reports),
        "identical_fields": identical_fields,
        "differing_fields": differing_fields,
    }


def assess_report_handoff_fit(
    report: dict[str, Any],
    *,
    source_surface: dict[str, Any] | None,
) -> dict[str, Any]:
    fingerprint = report.get("report_fingerprint") or {}
    report_format = (report.get("report_format") or fingerprint.get("report_format") or "").upper()
    filter_count = report.get("report_filter_count")
    if filter_count is None:
        filter_count = fingerprint.get("report_filter_count", 0)
    detail_column_count = report.get("detail_column_count")
    if detail_column_count is None:
        detail_column_count = fingerprint.get("detail_column_count", 0)
    detail_columns_preview = report.get("detail_columns_preview")
    if detail_columns_preview is None:
        detail_columns_preview = fingerprint.get("detail_columns_preview", [])
    normalized_columns = [normalize_text(value) for value in detail_columns_preview if isinstance(value, str)]
    has_standard_date_filter = bool(
        report.get("has_standard_date_filter")
        if report.get("has_standard_date_filter") is not None
        else fingerprint.get("has_standard_date_filter")
    )

    source_pattern_id = (source_surface or {}).get("pattern_id")
    requires_action_layer = bool((source_surface or {}).get("requires_action_layer"))
    preferred_follow_up_surface = (source_surface or {}).get("preferred_follow_up_surface")

    owner_accountability_fit = {"TABULAR": 5, "SUMMARY": 2, "MATRIX": 1}.get(report_format, 2)
    handoff_complementarity = {"TABULAR": 3, "SUMMARY": 2, "MATRIX": 0}.get(report_format, 1)
    diagnostic_depth_score = {"MATRIX": 4, "SUMMARY": 3, "TABULAR": 2}.get(report_format, 2)
    field_filter_alignment = 0
    executive_story_substitution_risk = 0
    reasons: list[str] = []

    source_label = normalize_text((source_surface or {}).get("source_surface_label"))
    executive_like = any(token in source_label for token in ("executive", "control tower", "command center", "rhythm"))
    if source_pattern_id == "cross_suite_control_tower":
        if report_format == "TABULAR":
            owner_accountability_fit += 1
            handoff_complementarity += 1
            reasons.append("TABULAR format best matches the owner-accountability follow-up expected from a control tower.")
        elif report_format == "SUMMARY":
            reasons.append("SUMMARY format can support a compact follow-up, but it is weaker than a queue-first report for control-tower handoffs.")
        elif report_format == "MATRIX":
            handoff_complementarity = max(0, handoff_complementarity - 1)
            reasons.append("MATRIX format is too diagnostic-heavy for a control-tower follow-up lane.")
    elif requires_action_layer and report_format == "TABULAR":
        handoff_complementarity += 1
        reasons.append("TABULAR format keeps the action path queue-first.")
    elif report_format == "TABULAR":
        reasons.append("TABULAR format keeps the follow-up path queue-first.")

    if preferred_follow_up_surface == "salesforce_report" and report_format != "MATRIX":
        handoff_complementarity += 1

    if filter_count and filter_count > 0:
        field_filter_alignment += 1
        if report_format != "MATRIX":
            handoff_complementarity += 1
        reasons.append("Existing report filters support scoped follow-up.")
    if has_standard_date_filter:
        field_filter_alignment += 1
        reasons.append("A standard date filter supports period-scoped follow-up.")
    if detail_column_count and detail_column_count >= 5:
        field_filter_alignment += 1
        diagnostic_depth_score += 1
        reasons.append("Visible detail columns support row-level follow-up without additional drill setup.")
    elif detail_column_count and detail_column_count >= 4:
        field_filter_alignment += 1
        reasons.append("Visible detail columns support a usable row-level follow-up path.")

    if any("account" in value for value in normalized_columns) and any(
        "opportunity" in value or value.startswith("opp") for value in normalized_columns
    ):
        field_filter_alignment += 1
        reasons.append("Detail columns include both account and opportunity context for follow-up.")
    if any(
        token in value
        for value in normalized_columns
        for token in ("owner", "manager", "close date", "renewal", "amount", "forecast", "risk", "stage")
    ):
        field_filter_alignment += 1
        reasons.append("Detail columns align with common owner/date/value follow-up cues.")

    if executive_like and report_format == "SUMMARY":
        executive_story_substitution_risk = 2
        reasons.append("SUMMARY format risks substituting for the control-tower story instead of acting as the follow-up queue.")
    elif executive_like and report_format == "MATRIX":
        executive_story_substitution_risk = 3
        reasons.append("MATRIX format is denser than the preferred compact follow-up path for a control tower.")

    overall_score = (
        owner_accountability_fit * 2
        + handoff_complementarity * 2
        + field_filter_alignment
        + diagnostic_depth_score
        - executive_story_substitution_risk
    )
    raw_verdict = "weak_follow_up_fit"
    if overall_score >= 14:
        raw_verdict = "strong_follow_up_fit"
    elif overall_score >= 11:
        raw_verdict = "moderate_follow_up_fit"

    verdict, verdict_cap = report_surface_intelligence.cap_follow_up_fit_verdict(
        report_format,
        raw_verdict,
    )
    if verdict_cap == "summary_caps_follow_up_fit":
        reasons.append("SUMMARY format is useful for compact follow-up, but it should not outrank a queue-first TABULAR report.")
    elif verdict_cap == "matrix_caps_follow_up_fit":
        reasons.append("MATRIX format remains diagnostic-heavy even when the visible cues look strong.")

    return {
        "report_format": report_format or None,
        "source_pattern_id": source_pattern_id,
        "action_path_score": owner_accountability_fit,
        "executive_handoff_score": handoff_complementarity,
        "diagnostic_depth_score": diagnostic_depth_score,
        "owner_accountability_fit": owner_accountability_fit,
        "handoff_complementarity": handoff_complementarity,
        "field_filter_alignment": field_filter_alignment,
        "executive_story_substitution_risk": executive_story_substitution_risk,
        "overall_score": overall_score,
        "raw_verdict": raw_verdict,
        "verdict": verdict,
        "verdict_cap": verdict_cap,
        "queue_ready_format": report_format == "TABULAR",
        "reasons": reasons,
    }


def choose_fit_recommendation(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not reports:
        return None
    ranked = sorted(
        reports,
        key=lambda item: (
            (item.get("fit_assessment") or {}).get("overall_score", 0),
            item.get("score", 0),
            item.get("Id", ""),
        ),
        reverse=True,
    )
    top = ranked[0]
    top_fit = (top.get("fit_assessment") or {}).get("overall_score", 0)
    next_fit = (ranked[1].get("fit_assessment") or {}).get("overall_score", 0) if len(ranked) > 1 else -1
    confidence = "medium"
    if top_fit >= 14 and next_fit <= top_fit - 4:
        confidence = "high"
    return {
        "Id": top.get("Id"),
        "Name": top.get("Name"),
        "confidence": confidence,
        "fit_assessment": top.get("fit_assessment"),
    }


def validate_live_target(
    target: dict[str, Any],
    *,
    target_org: str,
) -> tuple[list[dict[str, str]], dict[str, Any] | None]:
    target_surface_type = target.get("target_surface_type")
    target_id = target.get("target_surface_id")
    target_label = target.get("target_surface_label")

    if target_surface_type == "salesforce_report":
        if not target_id:
            return (
                [
                    make_message(
                        "warning",
                        "report_id_missing",
                        f"{target.get('source_surface_id')} has no target_surface_id to verify live.",
                    )
                ],
                None,
            )
        records = query_salesforce_reports(target_org=target_org, report_id=target_id)
        if not records:
            return (
                [
                    make_message(
                        "error",
                        "live_report_missing",
                        f"Live org did not return report {target_id}.",
                    )
                ],
                {"target_surface_type": target_surface_type, "record": None},
            )
        record = records[0]
        messages: list[dict[str, str]] = [
            make_message(
                "info",
                "live_report_verified",
                f"Verified live report {record.get('Id')} ({record.get('Name')}).",
            )
        ]
        if target_label and normalize_text(record.get("Name")) != normalize_text(target_label):
            messages.append(
                make_message(
                    "warning",
                    "report_label_mismatch",
                    f"Registry label {target_label!r} does not match live report name {record.get('Name')!r}.",
                )
            )
        return messages, {"target_surface_type": target_surface_type, "record": record}

    if target_surface_type in {"crma_dashboard", "salesforce_dashboard"}:
        record = context_dashboard_target(target_id, target_label)
        if not record:
            return (
                [
                    make_message(
                        "warning",
                        "dashboard_context_miss",
                        f"Could not match {target_id or target_label or 'dashboard target'} in local dashboard context.",
                    )
                ],
                {"target_surface_type": target_surface_type, "record": None},
            )
        return (
            [
                make_message(
                    "info",
                    "dashboard_context_verified",
                    f"Matched dashboard target {record.get('Id')} ({record.get('Name')}) in local context.",
                )
            ],
            {"target_surface_type": target_surface_type, "record": record},
        )

    return (
        [
            make_message(
                "warning",
                "live_validation_skipped",
                f"No live validation path for target_surface_type={target_surface_type}.",
            )
        ],
        None,
    )


def default_search_terms(
    source_surface_id: str,
    source_lookup: dict[str, dict[str, Any]],
    *,
    explicit_terms: list[str] | None = None,
) -> list[str]:
    if explicit_terms:
        return [term for term in explicit_terms if term.strip()]

    source_label = (source_lookup.get(source_surface_id) or {}).get("source_surface_label") or source_surface_id
    pieces = re.split(r"[\s_&/-]+", source_label)
    filtered = [piece for piece in pieces if len(piece) >= 4 and normalize_text(piece) not in GENERIC_WORDS]

    terms: list[str] = []
    if source_label:
        terms.append(source_label)
    terms.extend(filtered)

    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        normalized = normalize_text(term)
        if not normalized or normalized in seen:
            continue
        deduped.append(term)
        seen.add(normalized)
    return deduped[:6]


def status_from_messages(messages: list[dict[str, str]]) -> str:
    levels = {item["level"] for item in messages}
    if "error" in levels:
        return "error"
    if "warning" in levels:
        return "warn"
    return "ok"


def validate_registry(
    registry: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    check_live: bool,
    target_org: str,
) -> dict[str, Any]:
    messages: list[dict[str, str]] = []
    seen_pairs: set[tuple[str, str]] = set()
    live_validations: list[dict[str, Any]] = []

    version = registry.get("version")
    if not isinstance(version, int):
        messages.append(make_message("error", "invalid_version", "Registry version must be an integer."))
    updated_on = registry.get("updated_on")
    if not isinstance(updated_on, str) or not updated_on:
        messages.append(make_message("warning", "missing_updated_on", "Registry updated_on should be a non-empty string."))

    for item in registry.get("targets", []):
        source_surface_id = item.get("source_surface_id")
        target_surface_type = item.get("target_surface_type")
        destination_type = item.get("destination_type")
        target_surface_id = item.get("target_surface_id")
        target_surface_label = item.get("target_surface_label")
        preferred_search_terms = item.get("preferred_search_terms") or []
        pair = (str(source_surface_id), str(target_surface_type))

        if not source_surface_id:
            messages.append(make_message("error", "missing_source_surface_id", "Each target requires source_surface_id."))
            continue
        if source_surface_id not in source_lookup:
            messages.append(
                make_message(
                    "warning",
                    "unknown_source_surface",
                    f"source_surface_id {source_surface_id} is not present in the local source lookup.",
                )
            )
        if target_surface_type not in ALLOWED_TARGET_SURFACE_TYPES:
            messages.append(
                make_message(
                    "error",
                    "invalid_target_surface_type",
                    f"{source_surface_id} uses invalid target_surface_type {target_surface_type}.",
                )
            )
            continue
        if pair in seen_pairs:
            messages.append(
                make_message(
                    "error",
                    "duplicate_source_target_pair",
                    f"Duplicate mapping for {source_surface_id} -> {target_surface_type}.",
                )
            )
        seen_pairs.add(pair)

        expected_destination_type = DESTINATION_TYPES[target_surface_type]
        if not destination_type:
            messages.append(
                make_message(
                    "warning",
                    "missing_destination_type",
                    f"{source_surface_id} -> {target_surface_type} is missing destination_type.",
                )
            )
        elif destination_type != expected_destination_type:
            messages.append(
                make_message(
                    "error",
                    "destination_type_mismatch",
                    f"{source_surface_id} -> {target_surface_type} should use destination_type {expected_destination_type}.",
                )
            )

        if not target_surface_id and not target_surface_label:
            if preferred_search_terms:
                messages.append(
                    make_message(
                        "info",
                        "target_discovery_hints_present",
                        f"{source_surface_id} -> {target_surface_type} is unresolved but carries discovery hints.",
                    )
                )
            else:
                messages.append(
                    make_message(
                        "warning",
                        "target_unresolved",
                        f"{source_surface_id} -> {target_surface_type} has neither target_surface_id nor target_surface_label.",
                    )
                )
        if preferred_search_terms and (
            not isinstance(preferred_search_terms, list)
            or not all(isinstance(term, str) and term.strip() for term in preferred_search_terms)
        ):
            messages.append(
                make_message(
                    "error",
                    "invalid_preferred_search_terms",
                    f"{source_surface_id} -> {target_surface_type} has invalid preferred_search_terms.",
                )
            )
        if target_surface_id and target_surface_type == "salesforce_report" and not REPORT_ID_RE.match(target_surface_id):
            messages.append(
                make_message(
                    "error",
                    "invalid_report_id",
                    f"{source_surface_id} -> {target_surface_type} uses invalid report id {target_surface_id}.",
                )
            )
        if target_surface_id and target_surface_type == "crma_dashboard" and not CRMA_DASHBOARD_ID_RE.match(target_surface_id):
            messages.append(
                make_message(
                    "warning",
                    "unexpected_crma_dashboard_id",
                    f"{source_surface_id} -> {target_surface_type} uses non-0FK id {target_surface_id}.",
                )
            )
        if (
            target_surface_id
            and target_surface_type == "salesforce_dashboard"
            and not SF_DASHBOARD_ID_RE.match(target_surface_id)
        ):
            messages.append(
                make_message(
                    "warning",
                    "unexpected_sf_dashboard_id",
                    f"{source_surface_id} -> {target_surface_type} uses non-01Z id {target_surface_id}.",
                )
            )

        if check_live:
            live_messages, live_validation = validate_live_target(item, target_org=target_org)
            messages.extend(live_messages)
            if live_validation is not None:
                live_validations.append(
                    {
                        "source_surface_id": source_surface_id,
                        "target_surface_type": target_surface_type,
                        "validation": live_validation,
                    }
                )

    summary = registry_summary(registry)
    messages.insert(
        0,
        make_message(
            "info",
            "registry_checked",
            f"Validated {summary['total_targets']} handoff target(s).",
        ),
    )
    return make_result(
        status=status_from_messages(messages),
        command="validate",
        command_class="live_read" if check_live else "read_only",
        messages=messages,
        summary=summary,
        live_validations=live_validations,
    )


def build_inventory(
    registry: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    target_surface_type: str | None,
    unresolved_only: bool,
) -> dict[str, Any]:
    targets = [enrich_target(item, source_lookup) for item in registry.get("targets", [])]
    if target_surface_type:
        targets = [item for item in targets if item.get("target_surface_type") == target_surface_type]
    if unresolved_only:
        targets = [item for item in targets if not item.get("resolved")]

    messages = [
        make_message(
            "info",
            "inventory_ready",
            f"Selected {len(targets)} handoff target(s).",
        )
    ]
    return make_result(
        status="ok",
        command="inventory",
        command_class="read_only",
        messages=messages,
        filters={
            "target_surface_type": target_surface_type,
            "unresolved_only": unresolved_only,
        },
        summary=registry_summary({"targets": targets}),
        targets=targets,
    )


def find_registry_target(
    registry: dict[str, Any],
    *,
    source_surface_id: str,
    target_surface_type: str,
) -> dict[str, Any] | None:
    for item in registry.get("targets", []):
        if (
            item.get("source_surface_id") == source_surface_id
            and item.get("target_surface_type") == target_surface_type
        ):
            return item
    return None


def resolve_target(
    registry: dict[str, Any],
    *,
    source_lookup: dict[str, dict[str, Any]],
    source_surface_id: str,
    target_surface_type: str,
    check_live: bool,
    target_org: str,
    search_terms: list[str] | None,
    limit: int,
    describe_top: int,
) -> dict[str, Any]:
    messages: list[dict[str, str]] = []
    source_metadata = source_lookup.get(source_surface_id)
    if not source_metadata:
        messages.append(
            make_message(
                "warning",
                "unknown_source_surface",
                f"source_surface_id {source_surface_id} is not present in local context or exemplar metadata.",
            )
        )
        source_metadata = {
            "source_surface_id": source_surface_id,
            "source_surface_label": None,
            "source_surface_type": None,
            "resolution_source": None,
        }

    registry_target = find_registry_target(
        registry,
        source_surface_id=source_surface_id,
        target_surface_type=target_surface_type,
    )
    resolved_target = enrich_target(registry_target, source_lookup) if registry_target else None
    if registry_target:
        messages.append(
            make_message(
                "info",
                "registry_target_found",
                f"Found registry mapping for {source_surface_id} -> {target_surface_type}.",
            )
        )
    else:
        messages.append(
            make_message(
                "warning",
                "registry_target_missing",
                f"No registry mapping exists for {source_surface_id} -> {target_surface_type}.",
            )
        )

    live_validation: dict[str, Any] | None = None
    candidate_reports: list[dict[str, Any]] = []
    ranked_candidate_reports: list[dict[str, Any]] = []
    suggested_target: dict[str, Any] | None = None
    candidate_fingerprints: dict[str, dict[str, Any]] = {}
    used_search_terms: list[str] = []

    if target_surface_type == "salesforce_report":
        if registry_target and check_live and registry_target.get("target_surface_id"):
            live_messages, live_validation = validate_live_target(registry_target, target_org=target_org)
            messages.extend(live_messages)
        elif check_live or search_terms or (registry_target and registry_target.get("preferred_search_terms")):
            preferred_terms = registry_target.get("preferred_search_terms") if registry_target else None
            explicit_terms = search_terms or preferred_terms
            used_search_terms = default_search_terms(
                source_surface_id,
                source_lookup,
                explicit_terms=explicit_terms,
            )
            if used_search_terms:
                try:
                    candidate_reports = query_salesforce_reports(
                        target_org=target_org,
                        search_terms=used_search_terms,
                        limit=limit,
                    )
                except RuntimeError as exc:
                    messages.append(
                        make_message(
                            "error",
                            "live_report_query_failed",
                            f"Could not query live Salesforce reports: {exc}",
                        )
                    )
                else:
                    ranked_candidate_reports, suggested_target = rank_live_report_candidates(
                        candidate_reports,
                        source_surface=source_metadata,
                        search_terms=used_search_terms,
                    )
                    level = "info" if candidate_reports else "warning"
                    code = "live_candidates_found" if candidate_reports else "no_live_candidates"
                    text = (
                        f"Found {len(candidate_reports)} live report candidate(s) for {source_surface_id}."
                        if candidate_reports
                        else f"No live report candidates found for {source_surface_id}."
                    )
                    messages.append(make_message(level, code, text))
                    if suggested_target:
                        messages.append(
                            make_message(
                                "info" if suggested_target["confidence"] == "high" else "warning",
                                "live_candidate_suggested",
                                f"Top report candidate is {suggested_target['Id']} ({suggested_target['Name']}) with {suggested_target['confidence']} confidence.",
                            )
                        )
                    if describe_top > 0 and ranked_candidate_reports:
                        for item in ranked_candidate_reports[:describe_top]:
                            try:
                                candidate_fingerprints[item["Id"]] = describe_salesforce_report(
                                    target_org=target_org,
                                    report_id=item["Id"],
                                )
                            except RuntimeError as exc:
                                messages.append(
                                    make_message(
                                        "warning",
                                        "report_describe_failed",
                                        f"Could not describe report {item['Id']}: {exc}",
                                    )
                                )
                        if candidate_fingerprints:
                            for item in ranked_candidate_reports:
                                fingerprint = candidate_fingerprints.get(item["Id"])
                                if fingerprint:
                                    item["report_fingerprint"] = fingerprint
                            if suggested_target and suggested_target["Id"] in candidate_fingerprints:
                                suggested_target["report_fingerprint"] = candidate_fingerprints[suggested_target["Id"]]
                            messages.append(
                                make_message(
                                    "info",
                                    "report_describe_complete",
                                    f"Attached report fingerprints for {len(candidate_fingerprints)} live candidate(s).",
                                )
                            )
            else:
                messages.append(
                    make_message(
                        "warning",
                        "missing_search_terms",
                        "No search terms were available for live report discovery.",
                    )
                )

    return make_result(
        status=status_from_messages(messages),
        command="resolve",
        command_class="live_read" if (check_live or bool(search_terms)) else "read_only",
        messages=messages,
        source_surface=source_metadata,
        target_surface_type=target_surface_type,
        resolved_target=resolved_target,
        live_validation=live_validation,
        live_candidates=ranked_candidate_reports or candidate_reports,
        suggested_target=suggested_target,
        search_terms=used_search_terms,
        registry_discovery_hints=(registry_target or {}).get("preferred_search_terms"),
        candidate_fingerprints=candidate_fingerprints,
    )


def compare_targets(
    *,
    source_lookup: dict[str, dict[str, Any]],
    registry: dict[str, Any],
    source_surface_id: str | None,
    report_ids: list[str],
    target_org: str,
    search_terms: list[str] | None,
) -> dict[str, Any]:
    messages: list[dict[str, str]] = []
    source_metadata: dict[str, Any] | None = None
    registry_hints: list[str] | None = None
    if source_surface_id:
        source_metadata = source_lookup.get(source_surface_id)
        if source_metadata:
            messages.append(
                make_message(
                    "info",
                    "source_surface_found",
                    f"Using {source_surface_id} as the comparison source surface.",
                )
            )
            registry_target = find_registry_target(
                registry,
                source_surface_id=source_surface_id,
                target_surface_type="salesforce_report",
            )
            if registry_target:
                registry_hints = registry_target.get("preferred_search_terms")
        else:
            messages.append(
                make_message(
                    "warning",
                    "unknown_source_surface",
                    f"source_surface_id {source_surface_id} is not present in local context or exemplar metadata.",
                )
            )

    effective_search_terms = default_search_terms(
        source_surface_id or "",
        source_lookup,
        explicit_terms=search_terms or registry_hints,
    ) if (search_terms or registry_hints or source_surface_id) else []

    reports: list[dict[str, Any]] = []
    fingerprints: dict[str, dict[str, Any]] = {}
    for report_id in report_ids:
        try:
            record = fetch_salesforce_report_record(target_org=target_org, report_id=report_id)
            fingerprint = describe_salesforce_report(target_org=target_org, report_id=report_id)
        except RuntimeError as exc:
            messages.append(
                make_message(
                    "error",
                    "compare_fetch_failed",
                    f"Could not fetch report {report_id}: {exc}",
                )
            )
            continue
        fingerprints[report_id] = fingerprint
        enriched = dict(record)
        enriched["report_fingerprint"] = fingerprint
        enriched.update(fingerprint)
        if source_metadata:
            enriched["fit_assessment"] = assess_report_handoff_fit(
                enriched,
                source_surface=source_metadata,
            )
        reports.append(enriched)

    suggested_target: dict[str, Any] | None = None
    if source_metadata and effective_search_terms and reports:
        ranked, suggested_target = rank_live_report_candidates(
            reports,
            source_surface=source_metadata,
            search_terms=effective_search_terms,
        )
        reports = ranked
        if suggested_target:
            suggested_target["report_fingerprint"] = fingerprints.get(suggested_target["Id"])
            messages.append(
                make_message(
                    "info" if suggested_target["confidence"] == "high" else "warning",
                    "compare_suggested_target",
                    f"Top compared report is {suggested_target['Id']} ({suggested_target['Name']}) with {suggested_target['confidence']} confidence.",
                )
            )

    comparison = summarize_report_comparison(reports) if reports else {
        "compared_count": 0,
        "identical_fields": {},
        "differing_fields": {},
    }
    fit_recommendation = choose_fit_recommendation(reports) if reports and source_metadata else None
    if reports:
        messages.append(
            make_message(
                "info",
                "compare_complete",
                f"Compared {len(reports)} live report(s).",
            )
        )
    if fit_recommendation:
        messages.append(
            make_message(
                "info" if fit_recommendation["confidence"] == "high" else "warning",
                "compare_fit_recommendation",
                f"Stronger follow-up fit is {fit_recommendation['Id']} ({fit_recommendation['Name']}) with {fit_recommendation['confidence']} confidence.",
            )
        )

    return make_result(
        status=status_from_messages(messages),
        command="compare",
        command_class="live_read",
        messages=messages,
        source_surface=source_metadata,
        report_ids=report_ids,
        search_terms=effective_search_terms,
        registry_discovery_hints=registry_hints,
        reports=reports,
        report_fingerprints=fingerprints,
        comparison=comparison,
        suggested_target=suggested_target,
        fit_recommendation=fit_recommendation,
    )


def format_text_result(payload: dict[str, Any]) -> str:
    lines: list[str] = [f"{payload['tool']} {payload['command']}: {payload['status']}"]
    for message in payload.get("messages", []):
        lines.append(f"- {message['level']}: {message['code']} - {message['text']}")

    if payload["command"] == "inventory":
        for item in payload.get("targets", []):
            lines.append(
                "  "
                + f"{item.get('source_surface_id')} -> {item.get('target_surface_type')}"
                + f" ({item.get('target_surface_id') or item.get('target_surface_label') or 'unresolved'})"
            )
    if payload["command"] == "resolve":
        source_surface = payload.get("source_surface", {})
        if source_surface.get("source_surface_label"):
            lines.append(f"source: {source_surface['source_surface_label']}")
        resolved_target = payload.get("resolved_target")
        if resolved_target:
            lines.append(
                "resolved: "
                + (resolved_target.get("target_surface_id") or resolved_target.get("target_surface_label") or "unresolved")
            )
        if payload.get("search_terms"):
            lines.append(f"search_terms: {', '.join(payload['search_terms'])}")
        if payload.get("live_candidates"):
            lines.append("live_candidates:")
            for item in payload["live_candidates"][:10]:
                lines.append(
                    "  "
                    + f"{item.get('Id')} | {item.get('Name')} | {item.get('FolderName')} | {item.get('DeveloperName')}"
                    + (f" | score={item.get('score')}" if item.get("score") is not None else "")
                )
        if payload.get("suggested_target"):
            target = payload["suggested_target"]
            lines.append(
                "suggested_target: "
                + f"{target.get('Id')} ({target.get('Name')}) confidence={target.get('confidence')}"
            )
            if target.get("report_fingerprint"):
                fingerprint = target["report_fingerprint"]
                lines.append(
                    "  "
                    + f"format={fingerprint.get('report_format')} filters={fingerprint.get('report_filter_count')} "
                    + f"detail_columns={fingerprint.get('detail_column_count')}"
                )
    if payload["command"] == "compare":
        if payload.get("source_surface", {}).get("source_surface_label"):
            lines.append(f"source: {payload['source_surface']['source_surface_label']}")
        if payload.get("search_terms"):
            lines.append(f"search_terms: {', '.join(payload['search_terms'])}")
        if payload.get("reports"):
            lines.append("reports:")
            for item in payload["reports"]:
                fingerprint = item.get("report_fingerprint") or {}
                lines.append(
                    "  "
                    + f"{item.get('Id')} | {item.get('Name')} | {item.get('FolderName')} | "
                    + f"format={fingerprint.get('report_format')} filters={fingerprint.get('report_filter_count')}"
                    + (f" | score={item.get('score')}" if item.get("score") is not None else "")
                )
        if payload.get("suggested_target"):
            target = payload["suggested_target"]
            lines.append(
                "suggested_target: "
                + f"{target.get('Id')} ({target.get('Name')}) confidence={target.get('confidence')}"
            )
        if payload.get("fit_recommendation"):
            target = payload["fit_recommendation"]
            lines.append(
                "fit_recommendation: "
                + f"{target.get('Id')} ({target.get('Name')}) confidence={target.get('confidence')}"
            )
    return "\n".join(lines)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    common.add_argument(
        "--registry-path",
        type=Path,
        default=REGISTRY_PATH,
        help="Path to builder_brain_handoff_targets.json.",
    )

    validate_parser = subparsers.add_parser(
        "validate",
        parents=[common],
        help="Validate the handoff target registry.",
    )
    validate_parser.add_argument("--check-live", action="store_true", help="Verify target ids against live org/context.")
    validate_parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG, help="Salesforce org alias or username.")

    inventory_parser = subparsers.add_parser(
        "inventory",
        parents=[common],
        help="Inventory registered handoff targets.",
    )
    inventory_parser.add_argument(
        "--target-surface-type",
        choices=sorted(ALLOWED_TARGET_SURFACE_TYPES),
        help="Filter by target surface type.",
    )
    inventory_parser.add_argument("--unresolved-only", action="store_true", help="Only show targets missing ids.")

    resolve_parser = subparsers.add_parser(
        "resolve",
        parents=[common],
        help="Resolve one source -> target handoff mapping.",
    )
    resolve_parser.add_argument("--source-surface-id", required=True, help="Builder-brain source surface id.")
    resolve_parser.add_argument(
        "--target-surface-type",
        required=True,
        choices=sorted(ALLOWED_TARGET_SURFACE_TYPES),
        help="Expected target surface type.",
    )
    resolve_parser.add_argument("--check-live", action="store_true", help="Verify registry target or search live org.")
    resolve_parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG, help="Salesforce org alias or username.")
    resolve_parser.add_argument(
        "--search-term",
        action="append",
        default=[],
        help="Optional live report search term. Repeat for multiple terms.",
    )
    resolve_parser.add_argument("--limit", type=int, default=20, help="Max live report candidates to return.")
    resolve_parser.add_argument(
        "--describe-top",
        type=int,
        default=0,
        help="Attach report-format fingerprints for the top N live candidates.",
    )

    compare_parser = subparsers.add_parser(
        "compare",
        parents=[common],
        help="Compare specific live Salesforce reports side by side.",
    )
    compare_parser.add_argument(
        "--report-id",
        action="append",
        required=True,
        help="Live Salesforce report id to compare. Repeat for multiple reports.",
    )
    compare_parser.add_argument(
        "--source-surface-id",
        help="Optional builder-brain source surface id for scoring context.",
    )
    compare_parser.add_argument(
        "--search-term",
        action="append",
        default=[],
        help="Optional scoring/search term. Repeat for multiple terms.",
    )
    compare_parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG, help="Salesforce org alias or username.")

    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        registry = load_registry(args.registry_path)
        source_lookup = build_source_lookup()
        if args.command == "validate":
            payload = validate_registry(
                registry,
                source_lookup=source_lookup,
                check_live=args.check_live,
                target_org=args.target_org,
            )
        elif args.command == "inventory":
            payload = build_inventory(
                registry,
                source_lookup=source_lookup,
                target_surface_type=args.target_surface_type,
                unresolved_only=args.unresolved_only,
            )
        elif args.command == "compare":
            if len(args.report_id) < 2:
                raise ValueError("compare requires at least two --report-id values")
            payload = compare_targets(
                source_lookup=source_lookup,
                registry=registry,
                source_surface_id=args.source_surface_id,
                report_ids=args.report_id,
                target_org=args.target_org,
                search_terms=args.search_term,
            )
        else:
            payload = resolve_target(
                registry,
                source_lookup=source_lookup,
                source_surface_id=args.source_surface_id,
                target_surface_type=args.target_surface_type,
                check_live=args.check_live,
                target_org=args.target_org,
                search_terms=args.search_term,
                limit=args.limit,
                describe_top=args.describe_top,
            )
    except (RuntimeError, ValueError, OSError) as exc:
        payload = make_result(
            status="error",
            command=args.command,
            command_class="live_read" if getattr(args, "check_live", False) else "read_only",
            messages=[make_message("error", "command_failed", str(exc))],
        )

    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(format_text_result(payload))
    return 0 if payload["status"] != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
