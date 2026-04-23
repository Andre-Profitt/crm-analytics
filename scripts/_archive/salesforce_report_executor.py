#!/usr/bin/env python3
from __future__ import annotations

import argparse
import copy
import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import Any


API_VERSION = "66.0"
SUPPORTED_REPORT_FORMATS = {"TABULAR", "SUMMARY", "MATRIX"}
FULLY_SUPPORTED_NATIVE_REPORT_FORMATS = {"TABULAR", "SUMMARY"}
ROOT = Path(__file__).resolve().parents[1]
SCRIPT_DIR = Path(__file__).resolve().parent
AUTOFILL_VOCAB_PATH = ROOT / "config" / "native_surface_autofill_vocab.json"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import executor_policy
import native_surface_browser
import native_surface_io
import native_surface_policy
import report_surface_intelligence


_ORG_SESSION_CACHE = native_surface_io._ORG_SESSION_CACHE


def make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def make_result(
    *,
    status: str,
    command: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    command_class: str = "read_only",
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "salesforce_report_executor",
        "lane": "native_surface_authoring",
        "command_class": command_class,
        "messages": messages,
        "artifacts": artifacts or [],
        "command": command,
    }
    payload.update(extra)
    return executor_policy.apply_policy_exceptions(payload)


def load_build_package(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def _load_json_file(path: Path) -> dict[str, Any]:
    return native_surface_io.load_json_file(path)


def _load_evaluation_gate(path: Path) -> dict[str, Any]:
    return executor_policy.load_evaluation_gate(path)


def _append_evaluation_bypass_artifact(
    *,
    output_dir: Path | None,
    artifacts: list[dict[str, str]],
    command: str,
    target_org: str | None,
    evaluation_gate: dict[str, Any] | None,
    summary: dict[str, Any],
) -> None:
    executor_policy.append_evaluation_bypass_artifact(
        output_dir=output_dir,
        artifacts=artifacts,
        command=command,
        target_org=target_org,
        evaluation_gate=evaluation_gate,
        summary=summary,
    )


def _attach_memory_record(
    *,
    result: dict[str, Any],
    planning_context: dict[str, Any] | None,
    command: str,
    evaluation_gate: dict[str, Any] | None,
    extra_tags: list[str] | None = None,
) -> dict[str, Any]:
    return executor_policy.attach_memory_record(
        result=result,
        planning_context=planning_context,
        command=command,
        evaluation_gate=evaluation_gate,
        script_path="scripts/salesforce_report_executor.py",
        make_message=make_message,
        extra_tags=extra_tags,
    )


def _derive_report_memory_context(
    *,
    build_package: dict[str, Any],
    planning_context: dict[str, Any] | None,
    evaluation_gate: dict[str, Any] | None,
    output_dir: Path | None,
    package_path: Path,
    command: str,
) -> dict[str, Any] | None:
    return native_surface_policy.derive_native_surface_memory_context(
        build_package=build_package,
        planning_context=planning_context,
        evaluation_gate=evaluation_gate,
        output_dir=output_dir,
        package_path=package_path,
        command=command,
        default_goal_prefix="execute salesforce report",
        operation="mutate_report",
    )


def _resolve_report_evaluation_gate(
    *,
    build_package: dict[str, Any],
    evaluation_path: Path | None,
    require_pass: bool,
    allow_missing: bool,
) -> tuple[dict[str, Any] | None, list[dict[str, str]], dict[str, str] | None]:
    return native_surface_policy.resolve_native_surface_evaluation_gate(
        build_package=build_package,
        evaluation_path=evaluation_path,
        require_pass=require_pass,
        allow_missing=allow_missing,
        make_message=make_message,
        surface_name="report",
    )


def load_autofill_vocab() -> dict[str, Any]:
    if not AUTOFILL_VOCAB_PATH.exists():
        return {"version": 1, "report_field_aliases": []}
    return _load_json_file(AUTOFILL_VOCAB_PATH)


def _is_scalar_filter_override_value(value: Any) -> bool:
    return isinstance(value, (str, int, float, bool))


def _normalize_filter_override_spec(
    source_label: str,
    raw_value: Any,
    *,
    source: str,
) -> dict[str, str | None]:
    normalized_label = _normalize_label(source_label)
    if not normalized_label:
        raise ValueError("Filter override labels must not be empty.")

    operator: str | None = None
    value: Any = raw_value
    if isinstance(raw_value, dict):
        operator_value = raw_value.get("operator")
        if operator_value is not None and not isinstance(operator_value, str):
            raise ValueError(f"Filter override {source_label!r} has a non-string operator.")
        operator = operator_value.strip() if isinstance(operator_value, str) and operator_value.strip() else None
        value = raw_value.get("value")

    if not _is_scalar_filter_override_value(value):
        raise ValueError(
            f"Filter override {source_label!r} must be a scalar value or an object with scalar 'value' and optional 'operator'."
        )

    return {
        "source_label": source_label,
        "normalized_label": normalized_label,
        "operator": operator,
        "value": str(value),
        "source": source,
    }


def _load_report_filter_overrides(
    *,
    override_args: list[str] | None,
    override_json_path: str | None,
) -> dict[str, dict[str, str | None]]:
    overrides: dict[str, dict[str, str | None]] = {}

    if override_json_path:
        payload = _load_json_file(Path(override_json_path))
        for source_label, raw_value in payload.items():
            if not isinstance(source_label, str):
                raise ValueError("Filter override JSON keys must be strings.")
            spec = _normalize_filter_override_spec(
                source_label,
                raw_value,
                source=f"json:{override_json_path}",
            )
            overrides[str(spec["normalized_label"])] = spec

    for raw_arg in override_args or []:
        if "=" not in raw_arg:
            raise ValueError(f"Invalid --filter-override value {raw_arg!r}; expected source_label=value.")
        source_label, value = raw_arg.split("=", 1)
        spec = _normalize_filter_override_spec(
            source_label.strip(),
            value,
            source="cli",
        )
        overrides[str(spec["normalized_label"])] = spec

    return overrides


def _format_sf_error(stdout: str, stderr: str, *, path: str) -> str:
    return native_surface_io.format_sf_error(stdout, stderr, path=path)


def _normalize_label(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_report_format(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip().upper()


def _native_authoring_support(report_format: str | None) -> str:
    normalized_format = _normalize_report_format(report_format)
    if normalized_format in FULLY_SUPPORTED_NATIVE_REPORT_FORMATS:
        return "fully_supported"
    if normalized_format == "MATRIX":
        return "manual_required"
    return "unsupported"


def _native_authoring_support_message(native_authoring_support: str) -> tuple[str, str]:
    if native_authoring_support == "manual_required":
        return (
            "native_authoring_manual_required",
            "The packaged report format still requires manual native authoring because the executor does not model the full REST contract.",
        )
    return (
        "native_authoring_unsupported",
        "The packaged report format is not supported by the native report executor.",
    )


def assess_report_action_surface(build_package: dict[str, Any]) -> dict[str, Any]:
    surface_contract = build_package.get("surface_contract")
    return report_surface_intelligence.assess_report_action_surface_contract(surface_contract)


def _run_rest_request(
    path: str,
    *,
    target_org: str | None,
    method: str = "GET",
    body: Any | None = None,
) -> dict[str, Any]:
    if target_org:
        org_session = _get_org_session(target_org)
        if org_session:
            payload = _run_direct_rest_request(
                path,
                org_session=org_session,
                method=method,
                body=body,
            )
            if not isinstance(payload, dict):
                raise RuntimeError(f"unexpected non-object payload returned for {path}")
            return payload
    payload = native_surface_io.run_rest_request(
        path,
        root=ROOT,
        target_org=target_org,
        method=method,
        body=body,
        expect_dict=True,
    )
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected non-object payload returned for {path}")
    return payload


def _get_org_session(target_org: str) -> dict[str, str] | None:
    return native_surface_io.get_org_session(target_org, root=ROOT)


def _run_direct_rest_request(
    path: str,
    *,
    org_session: dict[str, str],
    method: str = "GET",
    body: Any | None = None,
) -> Any:
    return native_surface_io.run_direct_rest_request(
        path,
        org_session=org_session,
        method=method,
        body=body,
    )


def _fetch_rest_json(path: str, *, target_org: str | None) -> dict[str, Any]:
    return native_surface_io.fetch_rest_json(path, root=ROOT, target_org=target_org)


def _is_missing_report_error(exc: Exception) -> bool:
    message = str(exc).upper()
    return "NOT_FOUND" in message or "ENTITY_IS_DELETED" in message


def _wait_for_report_deletion(
    *,
    report_id: str,
    target_org: str,
    verify_attempts: int,
    verify_delay_seconds: float,
) -> dict[str, Any]:
    describe_path = f"/services/data/v{API_VERSION}/analytics/reports/{report_id}/describe"
    attempt_results: list[dict[str, Any]] = []

    for attempt in range(1, max(1, verify_attempts) + 1):
        try:
            describe_payload = _fetch_rest_json(describe_path, target_org=target_org)
            attempt_results.append(
                {
                    "attempt": attempt,
                    "status": "still_exists",
                    "name": describe_payload.get("name"),
                }
            )
        except Exception as exc:
            if _is_missing_report_error(exc):
                attempt_results.append(
                    {
                        "attempt": attempt,
                        "status": "deleted",
                        "detail": str(exc),
                    }
                )
                return {
                    "deleted": True,
                    "attempt_count": attempt,
                    "attempt_results": attempt_results,
                }
            attempt_results.append(
                {
                    "attempt": attempt,
                    "status": "error",
                    "detail": str(exc),
                }
            )
            return {
                "deleted": False,
                "attempt_count": attempt,
                "attempt_results": attempt_results,
                "error": str(exc),
            }
        if attempt < max(1, verify_attempts) and verify_delay_seconds > 0:
            time.sleep(verify_delay_seconds)

    return {
        "deleted": False,
        "attempt_count": max(1, verify_attempts),
        "attempt_results": attempt_results,
        "error": "Report still returned a describe payload after the delete request.",
    }


def _run_json_command(command: list[str]) -> tuple[int, dict[str, Any]]:
    result = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    stdout = result.stdout.strip()
    if not stdout:
        raise RuntimeError(result.stderr.strip() or f"Command produced no JSON output: {' '.join(command)}")
    try:
        payload = json.loads(stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Unable to parse JSON output from command {' '.join(command)}: {exc}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"Expected JSON object from command {' '.join(command)}.")
    return result.returncode, payload


def load_report_describe(
    *,
    report_id: str | None,
    target_org: str | None,
    baseline_describe_json: Path | None,
) -> dict[str, Any] | None:
    if baseline_describe_json is not None:
        return _load_json_file(baseline_describe_json)
    if report_id and target_org:
        return _fetch_rest_json(
            f"/services/data/v{API_VERSION}/analytics/reports/{report_id}/describe",
            target_org=target_org,
        )
    return None


def _normalize_report_filters(filters: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in filters:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "column": item.get("column"),
                "filterType": item.get("filterType"),
                "operator": item.get("operator"),
                "value": item.get("value"),
            }
        )
    return normalized


def _normalize_report_sorts(sort_entries: list[Any]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for item in sort_entries:
        if not isinstance(item, dict):
            continue
        normalized.append(
            {
                "sortColumn": item.get("sortColumn"),
                "sortOrder": item.get("sortOrder"),
            }
        )
    return normalized


def _extract_report_contract(report_describe: dict[str, Any]) -> dict[str, Any]:
    report_metadata = report_describe.get("reportMetadata") or {}
    report_type = report_metadata.get("reportType") or {}
    return {
        "folderId": report_metadata.get("folderId"),
        "reportType": report_type.get("type") if isinstance(report_type, dict) else None,
        "reportFormat": report_metadata.get("reportFormat"),
        "groupingsDown": [
            item.get("name")
            for item in report_metadata.get("groupingsDown") or []
            if isinstance(item, dict) and item.get("name")
        ],
        "detailColumns": [str(item) for item in report_metadata.get("detailColumns") or []],
        "reportFilters": _normalize_report_filters(report_metadata.get("reportFilters") or []),
        "sortBy": _normalize_report_sorts(report_metadata.get("sortBy") or []),
    }


def _make_finding(
    *,
    level: str,
    code: str,
    text: str,
    path: str,
    expected: Any | None = None,
    actual: Any | None = None,
) -> dict[str, Any]:
    finding: dict[str, Any] = {
        "level": level,
        "code": code,
        "text": text,
        "path": path,
    }
    if expected is not None:
        finding["expected"] = expected
    if actual is not None:
        finding["actual"] = actual
    return finding


def verify_report_contract(
    *,
    preview: dict[str, Any],
    report_describe: dict[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    requests = preview.get("requests") or []
    patch_request = requests[-1] if requests else {}
    expected_contract = _extract_report_contract({"reportMetadata": ((patch_request.get("body") or {}).get("reportMetadata") or {})})
    actual_contract = _extract_report_contract(report_describe)

    manual_filter_intents = preview.get("manual_filter_intents") or []
    manual_detail_intents = preview.get("manual_detail_intents") or []
    omitted_sort_intents = preview.get("omitted_sort_intents") or []

    findings: list[dict[str, Any]] = []

    for key, path in (
        ("folderId", "reportMetadata.folderId"),
        ("reportType", "reportMetadata.reportType.type"),
        ("reportFormat", "reportMetadata.reportFormat"),
    ):
        expected_value = expected_contract.get(key)
        actual_value = actual_contract.get(key)
        if expected_value != actual_value:
            findings.append(
                _make_finding(
                    level="warn",
                    code=f"{key}_mismatch",
                    text=f"Expected {key} {expected_value!r}, found {actual_value!r}.",
                    path=path,
                    expected=expected_value,
                    actual=actual_value,
                )
            )

    if expected_contract["groupingsDown"] != actual_contract["groupingsDown"]:
        findings.append(
            _make_finding(
                level="warn",
                code="groupings_down_mismatch",
                text="The live report groupings do not match the packaged report contract.",
                path="reportMetadata.groupingsDown",
                expected=expected_contract["groupingsDown"],
                actual=actual_contract["groupingsDown"],
            )
        )

    expected_detail_columns = expected_contract["detailColumns"]
    actual_detail_columns = actual_contract["detailColumns"]
    missing_detail_columns = [item for item in expected_detail_columns if item not in actual_detail_columns]
    extra_detail_columns = [item for item in actual_detail_columns if item not in expected_detail_columns]
    if missing_detail_columns:
        findings.append(
            _make_finding(
                level="warn",
                code="missing_detail_columns",
                text="The live report is missing packaged detail columns.",
                path="reportMetadata.detailColumns",
                expected=missing_detail_columns,
                actual=actual_detail_columns,
            )
        )
    if extra_detail_columns:
        findings.append(
            _make_finding(
                level="info" if manual_detail_intents else "warn",
                code="extra_live_detail_columns",
                text="The live report contains extra detail columns beyond the explicit packaged payload.",
                path="reportMetadata.detailColumns",
                expected=expected_detail_columns,
                actual=extra_detail_columns,
            )
        )

    expected_filters = expected_contract["reportFilters"]
    actual_filters = actual_contract["reportFilters"]
    missing_filters = [item for item in expected_filters if item not in actual_filters]
    extra_filters = [item for item in actual_filters if item not in expected_filters]
    if missing_filters:
        findings.append(
            _make_finding(
                level="warn",
                code="missing_report_filters",
                text="The live report is missing packaged report filters.",
                path="reportMetadata.reportFilters",
                expected=missing_filters,
                actual=actual_filters,
            )
        )
    if extra_filters:
        findings.append(
            _make_finding(
                level="info" if manual_filter_intents else "warn",
                code="extra_live_report_filters",
                text="The live report contains extra filters beyond the explicit packaged payload.",
                path="reportMetadata.reportFilters",
                expected=expected_filters,
                actual=extra_filters,
            )
        )

    expected_sorts = expected_contract["sortBy"]
    actual_sorts = actual_contract["sortBy"]
    if actual_sorts[: len(expected_sorts)] != expected_sorts:
        findings.append(
            _make_finding(
                level="warn",
                code="sort_by_mismatch",
                text="The live report sort order does not match the packaged report contract.",
                path="reportMetadata.sortBy",
                expected=expected_sorts,
                actual=actual_sorts,
            )
        )
    elif len(actual_sorts) > len(expected_sorts):
        findings.append(
            _make_finding(
                level="info" if omitted_sort_intents else "warn",
                code="extra_live_sorts",
                text="The live report contains additional sort rules beyond the explicit packaged payload.",
                path="reportMetadata.sortBy",
                expected=expected_sorts,
                actual=actual_sorts[len(expected_sorts) :],
            )
        )

    summary = {
        "surface_type": "salesforce_report",
        "grouping_count": len(expected_contract["groupingsDown"]),
        "detail_column_count": len(expected_contract["detailColumns"]),
        "explicit_filter_count": len(expected_contract["reportFilters"]),
        "explicit_sort_count": len(expected_contract["sortBy"]),
        "manual_filter_intent_count": len(manual_filter_intents),
        "manual_detail_intent_count": len(manual_detail_intents),
        "omitted_sort_intent_count": len(omitted_sort_intents),
        "finding_count": len(findings),
        "warn_count": sum(1 for item in findings if item["level"] == "warn"),
        "info_count": sum(1 for item in findings if item["level"] == "info"),
    }
    return findings, expected_contract, {**actual_contract, "summary": summary}


def _collect_report_field_candidates(report_describe: dict[str, Any]) -> dict[str, list[str]]:
    candidates: dict[str, list[str]] = {}

    def add_candidate(label: str | None, api_name: str | None) -> None:
        if not label or not api_name:
            return
        normalized = _normalize_label(label)
        if not normalized:
            return
        bucket = candidates.setdefault(normalized, [])
        if api_name not in bucket:
            bucket.append(api_name)

    extended = report_describe.get("reportExtendedMetadata") or {}
    for api_name, info in (extended.get("detailColumnInfo") or {}).items():
        if isinstance(info, dict):
            add_candidate(info.get("label"), api_name)
    for api_name, info in (extended.get("groupingColumnInfo") or {}).items():
        if isinstance(info, dict):
            add_candidate(info.get("label"), api_name)

    report_type_metadata = report_describe.get("reportTypeMetadata") or {}
    for category in report_type_metadata.get("categories") or []:
        if not isinstance(category, dict):
            continue
        for api_name, info in (category.get("columns") or {}).items():
            if isinstance(info, dict):
                add_candidate(info.get("label"), api_name)

    return candidates


def _resolve_report_field(
    source_label: str,
    candidates: dict[str, list[str]],
    *,
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[str | None, str | None]:
    normalized = _normalize_label(source_label)
    matches = candidates.get(normalized) or []
    if len(matches) == 1:
        return matches[0], f"baseline_report.field_match:{source_label}"

    fallback_matches: list[str] = []
    for label_key, values in candidates.items():
        if label_key == normalized:
            continue
        if label_key.endswith(normalized) or label_key.startswith(normalized):
            for value in values:
                if value not in fallback_matches:
                    fallback_matches.append(value)
    if len(fallback_matches) == 1:
        return fallback_matches[0], f"baseline_report.field_match:{source_label}"

    candidate_values = {value for values in candidates.values() for value in values}
    if autofill_vocab:
        for alias in autofill_vocab.get("report_field_aliases") or []:
            if _normalize_label(str(alias.get("builder_label", ""))) != normalized:
                continue
            target_api_name = alias.get("target_api_name")
            if isinstance(target_api_name, str) and target_api_name in candidate_values:
                target_label = alias.get("target_label") or target_api_name
                return target_api_name, f"repo_vocab.field_alias:{source_label}->{target_label}"
    return None, None


def _report_filter_template(source_label: str, autofill_vocab: dict[str, Any] | None) -> dict[str, Any]:
    normalized = _normalize_label(source_label)
    if autofill_vocab:
        for template in autofill_vocab.get("report_filter_templates") or []:
            if _normalize_label(str(template.get("builder_label", ""))) == normalized:
                return template
    return {"default_operator": "equals", "value_mode": "manual"}


def _report_sort_template(source_label: str, autofill_vocab: dict[str, Any] | None) -> dict[str, Any]:
    normalized = _normalize_label(source_label)
    if autofill_vocab:
        for template in autofill_vocab.get("report_sort_templates") or []:
            if _normalize_label(str(template.get("builder_label", ""))) == normalized:
                return template
    return {"default_sort_order": "Asc"}


def autofill_report_preview(
    preview: dict[str, Any],
    fill_requirements: list[dict[str, str]],
    *,
    report_describe: dict[str, Any],
    autofill_vocab: dict[str, Any] | None = None,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    requests = preview.get("requests") or []
    patch_request = requests[-1] if requests else {}
    report_metadata = (((patch_request.get("body") or {}).get("reportMetadata")) if isinstance(patch_request, dict) else {}) or {}
    request_ids = [request.get("id") for request in requests if isinstance(request, dict)]

    autofills: list[dict[str, str]] = []
    remaining: list[dict[str, str]] = []
    manual_detail_intents: list[dict[str, str]] = []
    manual_detail_indexes: set[int] = set()
    source_metadata = report_describe.get("reportMetadata") or {}
    field_candidates = _collect_report_field_candidates(report_describe)

    folder_id = source_metadata.get("folderId")
    if folder_id:
        for request in requests:
            body = request.get("body") or {}
            request_report_metadata = body.get("reportMetadata")
            if isinstance(request_report_metadata, dict) and request_report_metadata.get("folderId") == "__FILL_FOLDER_ID__":
                request_report_metadata["folderId"] = folder_id
        autofills.append({"category": "folder_id", "value": str(folder_id), "source": "baseline_report.folderId"})

    report_type = ((source_metadata.get("reportType") or {}).get("type")) if isinstance(source_metadata.get("reportType"), dict) else None
    if report_type and report_metadata.get("reportType", {}).get("type") == "__FILL_REPORT_TYPE__":
        report_metadata["reportType"]["type"] = report_type
        autofills.append({"category": "report_type", "value": str(report_type), "source": "baseline_report.reportType.type"})

    for requirement in fill_requirements:
        category = requirement.get("category")
        target_path = requirement.get("target_path", "")
        source_label = requirement.get("source_label")
        resolved = False

        if category in {"grouping_field_mapping", "detail_column_mapping", "filter_column_mapping", "sort_column_mapping"} and source_label:
            resolved_value, resolved_source = _resolve_report_field(
                source_label,
                field_candidates,
                autofill_vocab=autofill_vocab,
            )
            if resolved_value:
                match = re.search(r"\[(\d+)\]", target_path)
                if match:
                    index = int(match.group(1))
                    if category == "grouping_field_mapping" and index < len(report_metadata.get("groupingsDown", [])):
                        report_metadata["groupingsDown"][index]["name"] = resolved_value
                        resolved = True
                    elif category == "detail_column_mapping" and index < len(report_metadata.get("detailColumns", [])):
                        report_metadata["detailColumns"][index] = resolved_value
                        resolved = True
                    elif category == "filter_column_mapping" and index < len(report_metadata.get("reportFilters", [])):
                        report_metadata["reportFilters"][index]["column"] = resolved_value
                        resolved = True
                    elif category == "sort_column_mapping" and index < len(report_metadata.get("sortBy", [])):
                        report_metadata["sortBy"][index]["sortColumn"] = resolved_value
                        resolved = True
                if resolved:
                    autofills.append(
                        {
                            "category": category,
                            "value": resolved_value,
                            "source": resolved_source or f"baseline_report.field_match:{source_label}",
                        }
                    )
            elif category == "detail_column_mapping":
                match = re.search(r"\[(\d+)\]", target_path)
                if match:
                    index = int(match.group(1))
                    detail_columns = report_metadata.get("detailColumns", [])
                    if index < len(detail_columns):
                        manual_detail_indexes.add(index)
                        manual_detail_intents.append(
                            {
                                "source_label": source_label,
                                "current_value": str(detail_columns[index]),
                                "reason": "No native Salesforce report field mapping was found for this packaged semantic column.",
                                "guidance": "Carry this column as manual authoring intent or replace it with a real native report field/formula before live use.",
                            }
                        )
                        resolved = True

        elif category == "cloned_report_id":
            remaining.append(requirement)
            continue
        elif category in {"filter_operator", "filter_value"}:
            remaining.append(requirement)
            continue

        if not resolved:
            if category == "folder_id" and folder_id:
                continue
            if category == "report_type" and report_type:
                continue
            remaining.append(requirement)

    if manual_detail_indexes:
        report_metadata["detailColumns"] = [
            value for index, value in enumerate(report_metadata.get("detailColumns", [])) if index not in manual_detail_indexes
        ]
        preview["manual_detail_intents"] = manual_detail_intents
        notes = preview.setdefault("notes", [])
        manual_detail_note = (
            "Unsupported semantic detail columns are preserved separately and omitted from the REST detailColumns payload."
        )
        if manual_detail_note not in notes:
            notes.append(manual_detail_note)

    summary = {
        "applied_count": len(autofills),
        "remaining_count": len(remaining),
        "applied_categories": sorted({item["category"] for item in autofills}),
        "manual_detail_intent_count": len(manual_detail_intents),
        "request_ids": request_ids,
    }
    return remaining, {
        "artifact_type": "salesforce_report_autofill_summary",
        "autofills": autofills,
        "manual_detail_intents": manual_detail_intents,
        "summary": summary,
    }


def _require_list(
    surface_contract: dict[str, Any],
    key: str,
    *,
    errors: list[str],
    allow_empty: bool = False,
) -> list[Any]:
    value = surface_contract.get(key)
    if not isinstance(value, list):
        errors.append(f"surface_contract.{key} must be a list")
        return []
    if not allow_empty and not value:
        errors.append(f"surface_contract.{key} must not be empty")
    return value


def validate_build_package(build_package: dict[str, Any]) -> tuple[list[str], list[str], dict[str, Any]]:
    errors: list[str] = []
    warnings: list[str] = []

    surface_contract = build_package.get("surface_contract")
    if not isinstance(surface_contract, dict):
        errors.append("surface_contract must be an object")
        return errors, warnings, {}

    if surface_contract.get("surface_type") != "salesforce_report":
        errors.append("surface_contract.surface_type must be salesforce_report")

    report_format = surface_contract.get("report_format")
    normalized_report_format = _normalize_report_format(report_format)
    if not isinstance(report_format, str) or not report_format:
        errors.append("surface_contract.report_format is required")
    elif normalized_report_format not in SUPPORTED_REPORT_FORMATS:
        allowed_formats = ", ".join(sorted(SUPPORTED_REPORT_FORMATS))
        errors.append(f"surface_contract.report_format must be one of: {allowed_formats}")
    elif normalized_report_format == "MATRIX":
        warnings.append(
            "surface_contract.report_format MATRIX requires manual native authoring because the executor does not model groupingsAcross."
        )

    columns = _require_list(surface_contract, "columns", errors=errors)
    filters = _require_list(surface_contract, "filters", errors=errors)
    group_by = _require_list(surface_contract, "group_by", errors=errors, allow_empty=True)
    sort_by = _require_list(surface_contract, "sort_by", errors=errors, allow_empty=True)
    page_blueprint = _require_list(surface_contract, "page_blueprint", errors=errors, allow_empty=True)

    handoff_target = surface_contract.get("handoff_target")
    if handoff_target is not None and not isinstance(handoff_target, dict):
        errors.append("surface_contract.handoff_target must be an object when present")
    elif isinstance(handoff_target, dict) and surface_contract.get("handoff_surface"):
        destination_type = handoff_target.get("destination_type")
        if destination_type != "dashboard":
            warnings.append(
                "surface_contract.handoff_target.destination_type should be dashboard for CRMA follow-up handoffs"
            )

    review_gates = build_package.get("review_gates")
    if not isinstance(review_gates, list) or not review_gates:
        warnings.append("review_gates is empty; validation coverage may be too weak")

    acceptance_criteria = build_package.get("acceptance_criteria")
    if not isinstance(acceptance_criteria, list) or not acceptance_criteria:
        warnings.append("acceptance_criteria is empty; report bundle will be underspecified")

    action_surface_assessment = assess_report_action_surface(build_package)
    warnings.extend(action_surface_assessment.get("warnings", []))

    summary = {
        "surface_type": surface_contract.get("surface_type"),
        "report_format": report_format,
        "normalized_report_format": normalized_report_format or None,
        "group_by_count": len(group_by),
        "column_count": len(columns),
        "filter_count": len(filters),
        "sort_count": len(sort_by),
        "page_blueprint_count": len(page_blueprint),
        "has_handoff_target": isinstance(handoff_target, dict),
        "native_authoring_support": _native_authoring_support(report_format),
        "action_surface_assessment": action_surface_assessment,
    }
    return errors, warnings, summary


def _suggested_report_label(build_package: dict[str, Any]) -> str:
    build_brief = build_package.get("build_brief") or {}
    excellence_target = build_brief.get("excellence_target")
    reference_exemplar = build_brief.get("reference_exemplar")
    persona = build_brief.get("persona")
    domain = build_brief.get("domain")
    parts = [part for part in [persona, domain, excellence_target, reference_exemplar] if isinstance(part, str) and part]
    if parts:
        label = " ".join(parts[:3])
        return label[:40]
    return "Builder Brain Report"


def _suggested_report_developer_name(build_package: dict[str, Any]) -> str:
    label = _suggested_report_label(build_package)
    slug = re.sub(r"[^A-Za-z0-9]+", "_", label).strip("_")
    slug = slug[:40] or "Builder_Brain_Report"
    if slug[0].isdigit():
        slug = f"R_{slug}"
    return slug


def build_report_bundle(build_package: dict[str, Any]) -> dict[str, Any]:
    surface_contract = build_package["surface_contract"]
    execution_plan = build_package.get("execution_plan") or {}
    plan_phases = execution_plan.get("phases") or []
    handoff_target = surface_contract.get("handoff_target")
    action_surface_assessment = assess_report_action_surface(build_package)

    report_definition = {
        "artifact_type": "salesforce_report_definition",
        "suggested_label": _suggested_report_label(build_package),
        "suggested_developer_name": _suggested_report_developer_name(build_package),
        "report_format": surface_contract.get("report_format"),
        "group_by": surface_contract.get("group_by", []),
        "columns": surface_contract.get("columns", []),
        "filters": surface_contract.get("filters", []),
        "sort_by": surface_contract.get("sort_by", []),
        "page_blueprint": surface_contract.get("page_blueprint", []),
    }
    validation_checklist = {
        "artifact_type": "salesforce_report_validation_checklist",
        "review_gates": build_package.get("review_gates", []),
        "acceptance_criteria": build_package.get("acceptance_criteria", []),
        "design_constraints": build_package.get("design_constraints", []),
        "action_surface_assessment": action_surface_assessment,
        "required_checks": [
            "filter vocabulary matches the packaged operating language",
            "grouping keeps row-level owner accountability visible",
            "sort order pushes the action queue to the top",
            "screenshot review confirms scan-fast queue readability",
        ],
    }
    authoring_steps = {
        "artifact_type": "salesforce_report_authoring_steps",
        "delivery_mode": build_package.get("delivery_mode"),
        "steps": [
            {
                "sequence": 1,
                "phase": "report_core",
                "objective": "Author the report core exactly as packaged.",
                "actions": (plan_phases[0].get("actions") if plan_phases else []),
            },
            {
                "sequence": 2,
                "phase": "validation",
                "objective": "Validate the queue and the follow-up path before rollout.",
                "actions": (
                    plan_phases[1].get("actions")
                    if len(plan_phases) > 1
                    else ["Validate the final report against the packaged review gates."]
                ),
            },
        ],
    }
    field_contract = {
        "artifact_type": "salesforce_report_field_contract",
        "grouping_fields": surface_contract.get("group_by", []),
        "display_fields": surface_contract.get("columns", []),
        "filter_fields": surface_contract.get("filters", []),
        "sort_fields": surface_contract.get("sort_by", []),
    }

    return {
        "artifact_type": "salesforce_report_authoring_bundle",
        "build_brief": build_package.get("build_brief", {}),
        "report_definition": report_definition,
        "field_contract": field_contract,
        "validation_checklist": validation_checklist,
        "action_surface_assessment": action_surface_assessment,
        "authoring_steps": authoring_steps,
        "handoff_target": handoff_target,
        "acceptance_criteria": build_package.get("acceptance_criteria", []),
        "revision_summary": build_package.get("revision_summary", []),
        "repo_execution_fit": build_package.get("repo_execution_fit"),
    }


def _placeholder(prefix: str, label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", label.lower()).strip("_")
    return f"__FILL_{prefix}_{slug or 'value'}__"


def _fill_requirement(
    *,
    category: str,
    target_path: str,
    current_value: str,
    guidance: str,
    source_label: str | None = None,
    request_id: str,
) -> dict[str, str]:
    payload = {
        "category": category,
        "target_path": target_path,
        "current_value": current_value,
        "guidance": guidance,
        "request_id": request_id,
    }
    if source_label:
        payload["source_label"] = source_label
    return payload


def build_report_rest_preview(
    build_package: dict[str, Any],
    *,
    report_id: str | None,
    clone_from_report_id: str | None,
    folder_id: str | None,
    report_type: str | None,
    autofill_vocab: dict[str, Any] | None = None,
    filter_overrides: dict[str, dict[str, str | None]] | None = None,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, Any]]:
    authoring_bundle = build_report_bundle(build_package)
    report_definition = authoring_bundle["report_definition"]
    action_surface_assessment = authoring_bundle.get("action_surface_assessment") or {}
    native_report_format = str(report_definition["report_format"]).upper()
    promoted_grouping_format = False
    if native_report_format == "TABULAR" and report_definition["group_by"]:
        native_report_format = "SUMMARY"
        promoted_grouping_format = True
    filter_entries: list[dict[str, str]] = []
    filter_specs: list[dict[str, str]] = []
    manual_filter_intents: list[dict[str, str]] = []
    applied_filter_overrides: list[dict[str, str]] = []
    sort_entries: list[dict[str, str]] = []
    sort_specs: list[dict[str, str]] = []
    omitted_sort_intents: list[dict[str, str]] = []
    grouped_source_labels = {str(item) for item in report_definition["group_by"]}
    omitted_grouped_detail_intents: list[dict[str, str]] = []
    detail_columns: list[str] = []
    detail_specs: list[dict[str, str]] = []

    for label in report_definition["filters"]:
        template = _report_filter_template(label, autofill_vocab)
        operator = str(template.get("default_operator", "equals"))
        value_mode = str(template.get("value_mode", "manual"))
        default_value = template.get("default_value")
        override_spec = (filter_overrides or {}).get(_normalize_label(label))
        if isinstance(override_spec, dict):
            operator = str(override_spec.get("operator") or operator)
            override_value = str(override_spec.get("value"))
            filter_entry = {
                "column": _placeholder("FILTER_COLUMN", label),
                "filterType": "fieldValue",
                "operator": operator,
                "value": override_value,
            }
            filter_entries.append(filter_entry)
            filter_specs.append({"source_label": label, **filter_entry})
            applied_filter_overrides.append(
                {
                    "source_label": label,
                    "operator": operator,
                    "value": override_value,
                    "source": str(override_spec.get("source") or "override"),
                }
            )
            continue
        if value_mode == "manual":
            manual_filter_intents.append(
                {
                    "source_label": label,
                    "operator": operator,
                    "value_mode": value_mode,
                    "reason": "No fixed filter value is packaged for this native report filter intent.",
                    "guidance": "Apply this filter manually during native report authoring when the concrete operator value is known.",
                }
            )
            continue

        filter_entry = {
            "column": _placeholder("FILTER_COLUMN", label),
            "filterType": "fieldValue",
            "operator": operator,
            "value": str(default_value) if default_value is not None else "__FILL_FILTER_VALUE__",
        }
        filter_entries.append(filter_entry)
        filter_specs.append({"source_label": label, **filter_entry})

    for label in report_definition["sort_by"]:
        template = _report_sort_template(label, autofill_vocab)
        target_api_name = template.get("target_api_name")
        if label in grouped_source_labels:
            omitted_sort_intents.append(
                {
                    "source_label": label,
                    "sortOrder": str(template.get("default_sort_order", "Asc")),
                    "reason": "Grouped native report fields are omitted from automated sortBy payloads to keep the REST save contract stable.",
                    "source": "native_summary_safety",
                    "guidance": "Apply this grouped sort manually during native report authoring if the saved report still needs explicit grouping sort order.",
                }
            )
            continue
        if isinstance(target_api_name, str) and target_api_name:
            sort_entry = {
                "sortColumn": target_api_name,
                "sortOrder": str(template.get("default_sort_order", "Asc")),
            }
            sort_entries.append(sort_entry)
            sort_specs.append({"source_label": label, **sort_entry})
            continue

        omitted_sort_intents.append(
            {
                "source_label": label,
                "sortOrder": str(template.get("default_sort_order", "Asc")),
                "reason": "No native Salesforce report field mapping is defined for this builder sort intent.",
                "source": str(template.get("source", "builder_semantic_intent")),
                "guidance": "Carry this sort intent into manual report authoring or omit it from the REST patch body.",
            }
        )

    for label in report_definition["columns"]:
        if label in grouped_source_labels:
            omitted_grouped_detail_intents.append(
                {
                    "source_label": label,
                    "reason": "Grouped native report fields are omitted from detailColumns to avoid invalid summary-report payloads.",
                    "guidance": "Rely on the grouping itself for this field in the automated payload, or add it back manually if native report authoring requires it.",
                }
            )
            continue
        placeholder = _placeholder("COLUMN", label)
        detail_columns.append(placeholder)
        detail_specs.append({"source_label": label, "value": placeholder})

    report_metadata = {
        "name": report_definition["suggested_label"],
        "developerName": report_definition["suggested_developer_name"],
        "reportFormat": native_report_format,
        "folderId": folder_id or "__FILL_FOLDER_ID__",
        "reportType": {"type": report_type or "__FILL_REPORT_TYPE__"},
        "groupingsAcross": [],
        "groupingsDown": [
            {
                "dateGranularity": "None",
                "name": _placeholder("GROUPING", label),
                "sortAggregate": None,
                "sortOrder": "Asc",
            }
            for label in report_definition["group_by"]
        ],
        "detailColumns": detail_columns,
        "reportFilters": filter_entries,
        "sortBy": sort_entries,
    }

    requests: list[dict[str, Any]] = []
    fill_requirements: list[dict[str, str]] = []

    if report_id:
        strategy = "patch_existing"
        patch_request_id = "patch_report"
        requests.append(
            {
                "id": patch_request_id,
                "method": "PATCH",
                "path": f"/services/data/v{API_VERSION}/analytics/reports/{report_id}",
                "body": {"reportMetadata": report_metadata},
                "purpose": "Save changes to an existing report through the Report resource.",
            }
        )
    elif clone_from_report_id:
        strategy = "clone_then_patch"
        clone_request_id = "clone_report"
        patch_request_id = "patch_cloned_report"
        requests.extend(
            [
                {
                    "id": clone_request_id,
                    "method": "POST",
                    "path": f"/services/data/v{API_VERSION}/analytics/reports?cloneId={clone_from_report_id}",
                    "body": {
                        "reportMetadata": {
                            "name": report_definition["suggested_label"],
                            "folderId": folder_id or "__FILL_FOLDER_ID__",
                        }
                    },
                    "purpose": "Clone a baseline report before applying the packaged structure.",
                },
                {
                    "id": patch_request_id,
                    "method": "PATCH",
                    "path": f"/services/data/v{API_VERSION}/analytics/reports/__FILL_CLONED_REPORT_ID__",
                    "body": {"reportMetadata": report_metadata},
                    "purpose": "Save the packaged report contract onto the cloned report.",
                },
            ]
        )
        fill_requirements.append(
            _fill_requirement(
                category="cloned_report_id",
                target_path=f"{patch_request_id}.path",
                current_value="__FILL_CLONED_REPORT_ID__",
                guidance="Fill with the id returned by the clone response before sending the PATCH request.",
                request_id=patch_request_id,
            )
        )
    else:
        strategy = "create_new"
        create_request_id = "create_report"
        requests.append(
            {
                "id": create_request_id,
                "method": "POST",
                "path": f"/services/data/v{API_VERSION}/analytics/reports",
                "body": {"reportMetadata": report_metadata},
                "purpose": "Create a new report through the Report List resource.",
            }
        )

    active_request_id = requests[-1]["id"]
    if folder_id is None:
        fill_requirements.append(
            _fill_requirement(
                category="folder_id",
                target_path=(
                    "clone_report.body.reportMetadata.folderId, "
                    f"{active_request_id}.body.reportMetadata.folderId"
                    if strategy == "clone_then_patch"
                    else f"{active_request_id}.body.reportMetadata.folderId"
                ),
                current_value="__FILL_FOLDER_ID__",
                guidance="Fill with the target Salesforce report folder id.",
                request_id=active_request_id,
            )
        )
    if report_type is None:
        fill_requirements.append(
            _fill_requirement(
                category="report_type",
                target_path=f"{active_request_id}.body.reportMetadata.reportType.type",
                current_value="__FILL_REPORT_TYPE__",
                guidance="Fill with the report type API name, such as Opportunity or AccountList.",
                request_id=active_request_id,
            )
        )

    for index, label in enumerate(report_definition["group_by"]):
        fill_requirements.append(
            _fill_requirement(
                category="grouping_field_mapping",
                target_path=f"{active_request_id}.body.reportMetadata.groupingsDown[{index}].name",
                current_value=_placeholder("GROUPING", label),
                guidance="Replace the placeholder with the report grouping field API name from report metadata.",
                source_label=label,
                request_id=active_request_id,
            )
        )
    for index, detail_spec in enumerate(detail_specs):
        fill_requirements.append(
            _fill_requirement(
                category="detail_column_mapping",
                target_path=f"{active_request_id}.body.reportMetadata.detailColumns[{index}]",
                current_value=detail_spec["value"],
                guidance="Replace the placeholder with the report detail column API name from report metadata.",
                source_label=detail_spec["source_label"],
                request_id=active_request_id,
            )
        )
    for index, filter_spec in enumerate(filter_specs):
        column_placeholder = filter_spec["column"]
        fill_requirements.extend(
            [
                _fill_requirement(
                    category="filter_column_mapping",
                    target_path=f"{active_request_id}.body.reportMetadata.reportFilters[{index}].column",
                    current_value=column_placeholder,
                    guidance="Replace the placeholder with the report filter column API name.",
                    source_label=filter_spec["source_label"],
                    request_id=active_request_id,
                ),
            ]
        )
        if filter_spec["value"].startswith("__FILL_"):
            fill_requirements.append(
                _fill_requirement(
                    category="filter_value",
                    target_path=f"{active_request_id}.body.reportMetadata.reportFilters[{index}].value",
                    current_value=filter_spec["value"],
                    guidance="Fill the report filter value or value list for this packaged filter.",
                    source_label=filter_spec["source_label"],
                    request_id=active_request_id,
                )
            )
    for index, sort_spec in enumerate(sort_specs):
        current_value = sort_spec["sortColumn"]
        if not current_value.startswith("__FILL_"):
            continue
        fill_requirements.append(
            _fill_requirement(
                category="sort_column_mapping",
                target_path=f"{active_request_id}.body.reportMetadata.sortBy[{index}].sortColumn",
                current_value=current_value,
                guidance="Replace the placeholder sort column with the report field API name after field mapping.",
                source_label=sort_spec["source_label"],
                request_id=active_request_id,
            )
        )

    preview = {
        "artifact_type": "salesforce_report_rest_preview",
        "strategy": strategy,
        "api_version": API_VERSION,
        "requests": requests,
        "applied_filter_overrides": applied_filter_overrides,
        "manual_filter_intents": manual_filter_intents,
        "omitted_sort_intents": omitted_sort_intents,
        "notes": [
            "The Reports REST API supports POST create, PATCH save, POST clone, and DELETE on the Report resources.",
            "This preview is intentionally a fill-first contract because the builder package still uses business labels rather than report field API names.",
            *(
                [
                    "The packaged tabular report has grouped rows, so the native REST payload is promoted to SUMMARY format to stay API-valid."
                ]
                if promoted_grouping_format
                else []
            ),
            *(
                [
                    "MATRIX native report authoring remains manual-first because this executor does not package a groupingsAcross contract."
                ]
                if native_report_format == "MATRIX"
                else []
            ),
            *(
                [
                    "Manual filter intents are preserved separately when the builder package names a filter but does not define a fixed native report value."
                ]
                if manual_filter_intents
                else []
            ),
            *(
                [
                    "Applied explicit report filter overrides to resolve packaged native report filter intents into reportFilters entries."
                ]
                if applied_filter_overrides
                else []
            ),
            *(
                [
                    "Unsupported semantic sort intents are preserved separately and omitted from the REST sortBy payload."
                ]
                if omitted_sort_intents
                else []
            ),
            *(
                [
                    "Grouped native-report fields are omitted from detailColumns because Salesforce rejects summary payloads that repeat grouped columns in the selected detail column list."
                ]
                if omitted_grouped_detail_intents
                else []
            ),
        ],
        "omitted_grouped_detail_intents": omitted_grouped_detail_intents,
    }
    summary = {
        "surface_type": "salesforce_report",
        "strategy": strategy,
        "request_count": len(requests),
        "fill_requirement_count": len(fill_requirements),
        "native_report_format": native_report_format,
        "native_authoring_support": _native_authoring_support(native_report_format),
        "resolved_filter_override_count": len(applied_filter_overrides),
        "manual_filter_intent_count": len(manual_filter_intents),
        "manual_detail_intent_count": 0,
        "omitted_sort_intent_count": len(omitted_sort_intents),
        "omitted_grouped_detail_intent_count": len(omitted_grouped_detail_intents),
        "manual_authoring_pressure_score": (
            len(manual_filter_intents)
            + len(omitted_sort_intents)
            + len(omitted_grouped_detail_intents)
        ),
        "action_surface_score": action_surface_assessment.get("overall_score"),
        "action_surface_verdict": action_surface_assessment.get("verdict"),
    }
    return preview, fill_requirements, summary


def prepare_report_preview(
    *,
    build_package: dict[str, Any],
    report_id: str | None,
    clone_from_report_id: str | None,
    folder_id: str | None,
    report_type: str | None,
    baseline_describe_json: Path | None,
    autofill_live: bool,
    target_org: str | None,
    autofill_vocab: dict[str, Any] | None,
    filter_overrides: dict[str, dict[str, str | None]] | None,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, Any], str, dict[str, Any] | None]:
    preview, fill_requirements, preview_summary = build_report_rest_preview(
        build_package,
        report_id=report_id,
        clone_from_report_id=clone_from_report_id,
        folder_id=folder_id,
        report_type=report_type,
        autofill_vocab=autofill_vocab,
        filter_overrides=filter_overrides,
    )
    command_class = "read_only"
    autofill_summary: dict[str, Any] | None = None
    source_report_id = clone_from_report_id or report_id

    if autofill_live and not target_org:
        raise ValueError("--target-org is required with --autofill-live.")

    if baseline_describe_json or autofill_live:
        report_describe = load_report_describe(
            report_id=source_report_id,
            target_org=target_org if autofill_live else None,
            baseline_describe_json=baseline_describe_json,
        )
        if report_describe:
            fill_requirements, autofill_summary = autofill_report_preview(
                preview,
                fill_requirements,
                report_describe=report_describe,
                autofill_vocab=autofill_vocab,
            )
        if autofill_live:
            command_class = "live_read"

    preview_summary["fill_requirement_count"] = len(fill_requirements)
    if autofill_summary is not None:
        preview_summary["autofill_count"] = autofill_summary["summary"]["applied_count"]
        preview_summary["manual_detail_intent_count"] = autofill_summary["summary"]["manual_detail_intent_count"]

    return preview, fill_requirements, preview_summary, command_class, autofill_summary


def _external_fill_requirements(fill_requirements: list[dict[str, str]]) -> list[dict[str, str]]:
    return [item for item in fill_requirements if item.get("category") != "cloned_report_id"]


def _promote_apply_preview_to_create_new(
    preview: dict[str, Any],
    fill_requirements: list[dict[str, str]],
    *,
    report_id: str | None,
) -> tuple[dict[str, Any], list[dict[str, str]], dict[str, Any] | None]:
    if report_id or preview.get("strategy") != "clone_then_patch":
        return preview, fill_requirements, None

    external_fill_requirements = _external_fill_requirements(fill_requirements)
    if external_fill_requirements:
        return preview, fill_requirements, None

    requests = preview.get("requests") or []
    patch_request = next((item for item in requests if isinstance(item, dict) and item.get("id") == "patch_cloned_report"), None)
    if not isinstance(patch_request, dict):
        return preview, fill_requirements, None

    patch_body = patch_request.get("body")
    if not isinstance(patch_body, dict) or not isinstance(patch_body.get("reportMetadata"), dict):
        return preview, fill_requirements, None

    promoted_preview = copy.deepcopy(preview)
    promoted_preview["strategy"] = "create_new"
    promoted_preview["source_strategy"] = "clone_then_patch"
    promoted_preview["requests"] = [
        {
            "id": "create_report",
            "method": "POST",
            "path": f"/services/data/v{API_VERSION}/analytics/reports",
            "body": patch_body,
            "purpose": "Create a new report through the Report List resource using the fully resolved packaged contract.",
        }
    ]
    notes = promoted_preview.setdefault("notes", [])
    promotion_note = (
        "Apply preview promoted from clone_then_patch to create_new because the clone source is only needed for autofill context once the report payload is fully resolved."
    )
    if promotion_note not in notes:
        notes.append(promotion_note)

    promotion_summary = {
        "promoted": True,
        "source_strategy": "clone_then_patch",
        "effective_strategy": "create_new",
        "reason": "No external fill requirements remain; the baseline report is now only an autofill source.",
    }
    return promoted_preview, [], promotion_summary


def _extract_report_id(response_payload: dict[str, Any]) -> str | None:
    for key in ("id", "reportId"):
        value = response_payload.get(key)
        if isinstance(value, str) and value:
            return value
    attributes = response_payload.get("attributes")
    if isinstance(attributes, dict):
        for key in ("id", "reportId"):
            value = attributes.get(key)
            if isinstance(value, str) and value:
                return value
    report_payload = response_payload.get("report")
    if isinstance(report_payload, dict):
        return _extract_report_id(report_payload)
    return None


def execute_report_requests(
    preview: dict[str, Any],
    *,
    target_org: str | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str | None]:
    execution_requests: list[dict[str, Any]] = []
    responses: list[dict[str, Any]] = []
    cloned_report_id: str | None = None

    for request in preview.get("requests") or []:
        if not isinstance(request, dict):
            continue
        request_path = str(request.get("path") or "")
        if "__FILL_CLONED_REPORT_ID__" in request_path:
            if not cloned_report_id:
                raise RuntimeError("clone response did not return a report id for the follow-on PATCH request.")
            request_path = request_path.replace("__FILL_CLONED_REPORT_ID__", cloned_report_id)

        response_payload = _run_rest_request(
            request_path,
            target_org=target_org,
            method=str(request.get("method") or "GET"),
            body=request.get("body"),
        )
        response_report_id = _extract_report_id(response_payload)
        if request.get("id") == "clone_report":
            cloned_report_id = response_report_id
            if not cloned_report_id:
                raise RuntimeError("clone response did not return a usable report id.")

        execution_requests.append(
            {
                "id": request.get("id"),
                "method": request.get("method"),
                "path": request_path,
                "purpose": request.get("purpose"),
            }
        )
        responses.append(
            {
                "request_id": request.get("id"),
                "method": request.get("method"),
                "path": request_path,
                "report_id": response_report_id,
                "name": response_payload.get("name"),
                "payload": response_payload,
            }
        )

    return execution_requests, responses, cloned_report_id


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Salesforce report authoring executor")
    subparsers = parser.add_subparsers(dest="command", required=True)

    validate = subparsers.add_parser("validate", help="Validate a builder_brain salesforce_report package.")
    validate.add_argument("--package", required=True, help="Path to build_package.json")
    validate.add_argument("--json", action="store_true", help="Print JSON output.")

    bundle = subparsers.add_parser("bundle", help="Compile a salesforce_report authoring bundle.")
    bundle.add_argument("--package", required=True, help="Path to build_package.json")
    bundle.add_argument("--output-dir", default=None, help="Optional directory for emitted authoring artifacts.")
    bundle.add_argument("--json", action="store_true", help="Print JSON output.")

    preview = subparsers.add_parser("preview", help="Compile a Reports REST preview contract from a builder package.")
    preview.add_argument("--package", required=True, help="Path to build_package.json")
    preview.add_argument("--report-id", default=None, help="Patch an existing report id.")
    preview.add_argument("--clone-from-report-id", default=None, help="Clone a baseline report before patching.")
    preview.add_argument("--folder-id", default=None, help="Optional target folder id.")
    preview.add_argument("--report-type", default=None, help="Optional report type API name.")
    preview.add_argument(
        "--baseline-report-describe-json",
        default=None,
        help="Optional local report describe JSON used to autofill preview mappings.",
    )
    preview.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load report describe data from the live org to autofill preview mappings.",
    )
    preview.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads.",
    )
    preview.add_argument("--output-dir", default=None, help="Optional directory for emitted REST preview artifacts.")
    preview.add_argument(
        "--filter-override",
        action="append",
        default=None,
        help="Resolve a packaged report filter intent as source_label=value. Repeat as needed.",
    )
    preview.add_argument(
        "--filter-overrides-json",
        default=None,
        help="Optional JSON object mapping packaged filter labels to scalar values or {value, operator} specs.",
    )
    preview.add_argument("--json", action="store_true", help="Print JSON output.")

    verify = subparsers.add_parser(
        "verify",
        help="Verify a live or local report describe payload against the packaged report contract.",
    )
    verify.add_argument("--package", required=True, help="Path to build_package.json")
    verify.add_argument("--report-id", default=None, help="Live report id to verify.")
    verify.add_argument("--clone-from-report-id", default=None, help="Optional baseline report id used for autofill source context.")
    verify.add_argument("--folder-id", default=None, help="Optional target folder id override for expected contract compilation.")
    verify.add_argument("--report-type", default=None, help="Optional report type API name override for expected contract compilation.")
    verify.add_argument(
        "--baseline-report-describe-json",
        default=None,
        help="Optional local report describe JSON used to autofill expected contract mappings.",
    )
    verify.add_argument(
        "--actual-report-describe-json",
        default=None,
        help="Optional local report describe JSON used as the actual verification target.",
    )
    verify.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load report describe data from the live org to autofill expected contract mappings.",
    )
    verify.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live verification and/or autofill reads.",
    )
    verify.add_argument("--output-dir", default=None, help="Optional directory for emitted verify artifacts.")
    verify.add_argument(
        "--filter-override",
        action="append",
        default=None,
        help="Resolve a packaged report filter intent as source_label=value. Repeat as needed.",
    )
    verify.add_argument(
        "--filter-overrides-json",
        default=None,
        help="Optional JSON object mapping packaged filter labels to scalar values or {value, operator} specs.",
    )
    verify.add_argument("--json", action="store_true", help="Print JSON output.")

    apply = subparsers.add_parser(
        "apply",
        help="Preview or execute the packaged Reports REST request sequence.",
    )
    apply.add_argument("--package", required=True, help="Path to build_package.json")
    apply.add_argument("--report-id", default=None, help="Patch an existing report id.")
    apply.add_argument("--clone-from-report-id", default=None, help="Clone a baseline report before patching.")
    apply.add_argument("--folder-id", default=None, help="Optional target folder id.")
    apply.add_argument("--report-type", default=None, help="Optional report type API name.")
    apply.add_argument(
        "--baseline-report-describe-json",
        default=None,
        help="Optional local report describe JSON used to autofill preview mappings.",
    )
    apply.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load report describe data from the live org to autofill preview mappings.",
    )
    apply.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads and REST execution.",
    )
    apply.add_argument(
        "--filter-override",
        action="append",
        default=None,
        help="Resolve a packaged report filter intent as source_label=value. Repeat as needed.",
    )
    apply.add_argument(
        "--filter-overrides-json",
        default=None,
        help="Optional JSON object mapping packaged filter labels to scalar values or {value, operator} specs.",
    )
    apply.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    apply.add_argument(
        "--allow-missing-evaluation",
        action="store_true",
        help="Allow live report mutation to continue without a pass evaluator verdict.",
    )
    apply.add_argument("--apply", action="store_true", help="Execute the REST request sequence instead of previewing it.")
    apply.add_argument("--output-dir", default=None, help="Optional directory for emitted apply preview/apply artifacts.")
    apply.add_argument("--json", action="store_true", help="Print JSON output.")

    complete = subparsers.add_parser(
        "complete",
        help="Apply the packaged report and immediately verify the authored live result.",
    )
    complete.add_argument("--package", required=True, help="Path to build_package.json")
    complete.add_argument("--report-id", default=None, help="Patch an existing report id.")
    complete.add_argument("--clone-from-report-id", default=None, help="Clone a baseline report before patching.")
    complete.add_argument("--folder-id", default=None, help="Optional target folder id.")
    complete.add_argument("--report-type", default=None, help="Optional report type API name.")
    complete.add_argument(
        "--baseline-report-describe-json",
        default=None,
        help="Optional local report describe JSON used to autofill preview/verify mappings.",
    )
    complete.add_argument(
        "--autofill-live",
        action="store_true",
        help="Load report describe data from the live org to autofill preview/verify mappings.",
    )
    complete.add_argument(
        "--target-org",
        default=None,
        help="Target org alias/username for live preview autofill reads, REST execution, and verification.",
    )
    complete.add_argument(
        "--filter-override",
        action="append",
        default=None,
        help="Resolve a packaged report filter intent as source_label=value. Repeat as needed.",
    )
    complete.add_argument(
        "--filter-overrides-json",
        default=None,
        help="Optional JSON object mapping packaged filter labels to scalar values or {value, operator} specs.",
    )
    complete.add_argument("--evaluation", default=None, help="Optional path to evaluation.json from the plan evaluator.")
    complete.add_argument(
        "--allow-missing-evaluation",
        action="store_true",
        help="Allow live report mutation to continue without a pass evaluator verdict.",
    )
    complete.add_argument("--output-dir", default=None, help="Optional directory for emitted apply/verify artifacts.")
    complete.add_argument("--json", action="store_true", help="Print JSON output.")

    delete = subparsers.add_parser(
        "delete",
        help="Delete a live report and confirm the report describe no longer resolves.",
    )
    delete.add_argument("--report-id", required=True, help="Live report id to delete.")
    delete.add_argument("--target-org", required=True, help="Target org alias/username for live report deletion.")
    delete.add_argument("--verify-attempts", type=int, default=5, help="Number of describe checks to confirm deletion.")
    delete.add_argument(
        "--verify-delay-seconds",
        type=float,
        default=1.0,
        help="Delay between delete verification attempts.",
    )
    delete.add_argument("--output-dir", default=None, help="Optional directory for emitted delete artifacts.")
    delete.add_argument("--json", action="store_true", help="Print JSON output.")
    return parser


def _print_text(result: dict[str, Any]) -> None:
    print(f"status: {result['status']}")
    for message in result.get("messages", []):
        print(f"{message['level']}: {message['code']}: {message['text']}")
    if isinstance(result.get("review_artifact"), str) and result["review_artifact"]:
        print(f"review_artifact: {result['review_artifact']}")
    if isinstance(result.get("collection_landing_artifact"), str) and result["collection_landing_artifact"]:
        print(f"salesforce_report_collection_landing_artifact: {result['collection_landing_artifact']}")
    if isinstance(result.get("browser_landing_artifact"), str) and result["browser_landing_artifact"]:
        print(f"ai_os_browser_landing_artifact: {result['browser_landing_artifact']}")
    if isinstance(result.get("browser_health_landing_artifact"), str) and result["browser_health_landing_artifact"]:
        print(f"ai_os_health_landing_artifact: {result['browser_health_landing_artifact']}")


def _emit_result(*, result: dict[str, Any], output_dir: Path | None, json_mode: bool) -> None:
    result = native_surface_browser.attach_native_surface_browser_artifacts(
        result=result,
        output_dir=output_dir,
        surface="report",
        make_message=make_message,
    )
    if json_mode:
        print(json.dumps(result, indent=2))
    else:
        _print_text(result)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    runtime_output_dir = Path(getattr(args, "output_dir", "")) if getattr(args, "output_dir", None) else None

    def emit_result(result: dict[str, Any]) -> None:
        _emit_result(result=result, output_dir=runtime_output_dir, json_mode=args.json)

    if args.command == "delete":
        delete_path = f"/services/data/v{API_VERSION}/analytics/reports/{args.report_id}"
        artifacts: list[dict[str, str]] = []
        try:
            delete_response = _run_rest_request(
                delete_path,
                target_org=args.target_org,
                method="DELETE",
            )
            delete_verification = _wait_for_report_deletion(
                report_id=args.report_id,
                target_org=args.target_org,
                verify_attempts=args.verify_attempts,
                verify_delay_seconds=args.verify_delay_seconds,
            )
        except Exception as exc:
            result = make_result(
                status="error",
                command="delete",
                messages=[make_message("error", "delete_failed", str(exc)[:2000])],
                summary={"deleted_report_id": args.report_id},
                command_class="mutating",
            )
            emit_result(result)
            return 1

        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            delete_response_path = output_dir / "salesforce_report_delete_response.json"
            delete_verify_path = output_dir / "salesforce_report_delete_verify.json"
            delete_response_path.write_text(
                json.dumps(
                    {
                        "artifact_type": "salesforce_report_delete_response",
                        "report_id": args.report_id,
                        "path": delete_path,
                        "response": delete_response,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            delete_verify_path.write_text(
                json.dumps(
                    {
                        "artifact_type": "salesforce_report_delete_verify",
                        "report_id": args.report_id,
                        **delete_verification,
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            artifacts.extend(
                [
                    {"type": "salesforce_report_delete_response", "path": str(delete_response_path)},
                    {"type": "salesforce_report_delete_verify", "path": str(delete_verify_path)},
                ]
            )

        deletion_verified = bool(delete_verification.get("deleted"))
        status = "ok" if deletion_verified else "warn"
        result = make_result(
            status=status,
            command="delete",
            messages=[
                make_message("info", "delete_request_complete", f"Sent DELETE for report {args.report_id}."),
                make_message(
                    "info" if deletion_verified else "warn",
                    "delete_verified" if deletion_verified else "delete_verification_inconclusive",
                    "Confirmed the report no longer resolves from the Reports REST describe endpoint."
                    if deletion_verified
                    else "Delete request completed but follow-on describe checks still resolved the report.",
                ),
            ],
            artifacts=artifacts,
            summary={
                "deleted_report_id": args.report_id,
                "delete_verified": deletion_verified,
                "delete_verify_attempt_count": delete_verification.get("attempt_count"),
            },
            delete_response=delete_response,
            delete_verification=delete_verification,
            command_class="mutating",
        )
        emit_result(result)
        return 0 if deletion_verified else 1

    autofill_vocab = load_autofill_vocab()

    build_package = load_build_package(Path(args.package))
    planning_context = build_package.get("planning_context")
    if not isinstance(planning_context, dict):
        planning_context = None
    errors, warnings, summary = validate_build_package(build_package)
    report_filter_overrides: dict[str, dict[str, str | None]] | None = None
    if args.command in {"preview", "verify", "apply", "complete"}:
        try:
            report_filter_overrides = _load_report_filter_overrides(
                override_args=getattr(args, "filter_override", None),
                override_json_path=getattr(args, "filter_overrides_json", None),
            )
        except ValueError as exc:
            result = make_result(
                status="error",
                command=args.command,
                messages=[make_message("error", "invalid_filter_override", str(exc))],
                summary=summary,
                command_class="mutating" if args.command in {"apply", "complete"} else "read_only",
            )
            emit_result(result)
            return 1

    if args.command == "validate":
        status = "error" if errors else ("warn" if warnings else "ok")
        result = make_result(
            status=status,
            command="validate",
            messages=[
                *[make_message("error", "invalid_build_package", item) for item in errors],
                *[make_message("warn", "build_package_warning", item) for item in warnings],
                make_message(
                    "info" if not errors else "error",
                    "validation_complete",
                    "Validated Salesforce report package." if not errors else "Salesforce report package validation failed.",
                ),
            ],
            summary=summary,
        )
        emit_result(result)
        return 1 if errors else 0

    if errors:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "invalid_build_package", item) for item in errors],
            summary=summary,
        )
        emit_result(result)
        return 1

    if args.command == "bundle":
        authoring_bundle = build_report_bundle(build_package)
        artifacts: list[dict[str, str]] = []
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            bundle_path = output_dir / "salesforce_report_bundle.json"
            definition_path = output_dir / "salesforce_report_definition.json"
            checklist_path = output_dir / "salesforce_report_validation_checklist.json"
            bundle_path.write_text(json.dumps(authoring_bundle, indent=2), encoding="utf-8")
            definition_path.write_text(json.dumps(authoring_bundle["report_definition"], indent=2), encoding="utf-8")
            checklist_path.write_text(json.dumps(authoring_bundle["validation_checklist"], indent=2), encoding="utf-8")
            artifacts.extend(
                [
                    {"type": "salesforce_report_bundle", "path": str(bundle_path)},
                    {"type": "salesforce_report_definition", "path": str(definition_path)},
                    {"type": "salesforce_report_validation_checklist", "path": str(checklist_path)},
                ]
            )

        result = make_result(
            status="warn" if warnings else "ok",
            command="bundle",
            messages=[
                *[make_message("warn", "build_package_warning", item) for item in warnings],
                make_message("info", "bundle_ready", "Compiled Salesforce report authoring bundle."),
            ],
            artifacts=artifacts,
            summary=summary,
            authoring_bundle=authoring_bundle,
        )
        emit_result(result)
        return 0

    if args.command == "verify":
        baseline_describe_json = Path(args.baseline_report_describe_json) if args.baseline_report_describe_json else None
        actual_report_describe_json = Path(args.actual_report_describe_json) if args.actual_report_describe_json else None
        if not args.report_id and actual_report_describe_json is None:
            result = make_result(
                status="error",
                command="verify",
                messages=[
                    make_message(
                        "error",
                        "verification_target_required",
                        "Provide --report-id or --actual-report-describe-json for verify.",
                    )
                ],
                summary=summary,
            )
            emit_result(result)
            return 1

        try:
            preview, fill_requirements, preview_summary, command_class, autofill_summary = prepare_report_preview(
                build_package=build_package,
                report_id=args.report_id,
                clone_from_report_id=args.clone_from_report_id,
                folder_id=args.folder_id,
                report_type=args.report_type,
                baseline_describe_json=baseline_describe_json,
                autofill_live=args.autofill_live,
                target_org=args.target_org,
                autofill_vocab=autofill_vocab,
                filter_overrides=report_filter_overrides,
            )
            actual_report_describe = load_report_describe(
                report_id=args.report_id,
                target_org=args.target_org,
                baseline_describe_json=actual_report_describe_json,
            )
            if actual_report_describe is None:
                raise ValueError("Unable to load the actual report describe payload for verify.")
            if actual_report_describe_json is None and args.report_id and args.target_org:
                command_class = "live_read"
        except ValueError as exc:
            result = make_result(
                status="error",
                command="verify",
                messages=[make_message("error", "verify_setup_failed", str(exc))],
                summary=summary,
                command_class="live_read" if args.autofill_live or (args.report_id and args.target_org) else "read_only",
            )
            emit_result(result)
            return 1
        except Exception as exc:
            result = make_result(
                status="error",
                command="verify",
                messages=[make_message("error", "verify_load_failed", str(exc))],
                summary=summary,
                command_class="live_read" if args.autofill_live or (args.report_id and args.target_org) else "read_only",
            )
            emit_result(result)
            return 1

        findings, expected_contract, actual_contract = verify_report_contract(
            preview=preview,
            report_describe=actual_report_describe,
        )
        verify_summary = actual_contract.pop("summary")
        artifacts: list[dict[str, str]] = []
        verify_artifact = {
            "artifact_type": "salesforce_report_verify",
            "target_report_id": args.report_id,
            "expected_contract": expected_contract,
            "actual_contract": actual_contract,
            "fill_requirements": fill_requirements,
            "findings": findings,
            "summary": verify_summary,
        }
        if args.output_dir:
            output_dir = Path(args.output_dir)
            output_dir.mkdir(parents=True, exist_ok=True)
            verify_path = output_dir / "salesforce_report_verify.json"
            verify_path.write_text(json.dumps(verify_artifact, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_report_verify", "path": str(verify_path)})

        status = "warn" if any(item["level"] == "warn" for item in findings) else "ok"
        result = make_result(
            status=status,
            command="verify",
            messages=[
                *[
                    make_message(item["level"], item["code"], item["text"])
                    for item in findings
                ],
                make_message(
                    "warn" if status == "warn" else "info",
                    "verify_complete",
                    "Verified the report against the packaged contract with findings."
                    if status == "warn"
                    else "Verified the report against the packaged contract with no blocking mismatches.",
                ),
            ],
            artifacts=artifacts,
            summary={**summary, **preview_summary, **verify_summary},
            command_class=command_class,
            expected_contract=expected_contract,
            actual_contract=actual_contract,
            fill_requirements=fill_requirements,
            findings=findings,
            autofill_summary=autofill_summary,
        )
        emit_result(result)
        return 0

    if args.command == "complete":
        if not args.target_org:
            result = make_result(
                status="error",
                command="complete",
                messages=[make_message("error", "target_org_required", "--target-org is required for complete.")],
                summary=summary,
                command_class="mutating",
            )
            emit_result(result)
            return 1

        output_root = Path(args.output_dir) if args.output_dir else None
        apply_output_dir = output_root / "01_apply" if output_root else None
        verify_output_dir = output_root / "02_verify" if output_root else None
        complete_memory_context = _derive_report_memory_context(
            build_package=build_package,
            planning_context=planning_context,
            evaluation_gate=None,
            output_dir=output_root,
            package_path=Path(args.package),
            command="complete",
        )

        apply_command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "apply",
            "--package",
            str(Path(args.package)),
            "--target-org",
            args.target_org,
            "--apply",
            "--json",
        ]
        if args.report_id:
            apply_command.extend(["--report-id", args.report_id])
        if args.clone_from_report_id:
            apply_command.extend(["--clone-from-report-id", args.clone_from_report_id])
        if args.folder_id:
            apply_command.extend(["--folder-id", args.folder_id])
        if args.report_type:
            apply_command.extend(["--report-type", args.report_type])
        if args.baseline_report_describe_json:
            apply_command.extend(["--baseline-report-describe-json", args.baseline_report_describe_json])
        if args.autofill_live:
            apply_command.append("--autofill-live")
        if args.filter_overrides_json:
            apply_command.extend(["--filter-overrides-json", args.filter_overrides_json])
        for override_arg in args.filter_override or []:
            apply_command.extend(["--filter-override", override_arg])
        if args.evaluation:
            apply_command.extend(["--evaluation", args.evaluation])
        if args.allow_missing_evaluation:
            apply_command.append("--allow-missing-evaluation")
        if apply_output_dir is not None:
            apply_output_dir.mkdir(parents=True, exist_ok=True)
            apply_command.extend(["--output-dir", str(apply_output_dir)])

        try:
            apply_exit_code, apply_result = _run_json_command(apply_command)
        except Exception as exc:
            result = make_result(
                status="error",
                command="complete",
                messages=[make_message("error", "complete_apply_failed", str(exc))],
                summary=summary,
                command_class="mutating",
            )
            emit_result(result)
            return 1

        if apply_exit_code != 0 or apply_result.get("status") == "error":
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "complete_apply_failed", "Native report apply failed before verify."),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={**summary, "apply_status": apply_result.get("status")},
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        applied_report_id = (apply_result.get("applied_report") or {}).get("id")
        if not isinstance(applied_report_id, str) or not applied_report_id:
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "missing_applied_report_id", "Apply completed without an applied report id."),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={**summary, "apply_status": apply_result.get("status")},
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        verify_command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "verify",
            "--package",
            str(Path(args.package)),
            "--report-id",
            applied_report_id,
            "--target-org",
            args.target_org,
            "--json",
        ]
        if args.clone_from_report_id:
            verify_command.extend(["--clone-from-report-id", args.clone_from_report_id])
        if args.folder_id:
            verify_command.extend(["--folder-id", args.folder_id])
        if args.report_type:
            verify_command.extend(["--report-type", args.report_type])
        if args.baseline_report_describe_json:
            verify_command.extend(["--baseline-report-describe-json", args.baseline_report_describe_json])
        if args.autofill_live:
            verify_command.append("--autofill-live")
        if args.filter_overrides_json:
            verify_command.extend(["--filter-overrides-json", args.filter_overrides_json])
        for override_arg in args.filter_override or []:
            verify_command.extend(["--filter-override", override_arg])
        if verify_output_dir is not None:
            verify_output_dir.mkdir(parents=True, exist_ok=True)
            verify_command.extend(["--output-dir", str(verify_output_dir)])

        try:
            verify_exit_code, verify_result = _run_json_command(verify_command)
        except Exception as exc:
            result = make_result(
                status="error",
                command="complete",
                messages=[
                    *apply_result.get("messages", []),
                    make_message("error", "complete_verify_failed", str(exc)),
                ],
                artifacts=apply_result.get("artifacts", []),
                summary={
                    **summary,
                    "apply_status": apply_result.get("status"),
                    "applied_report_id": applied_report_id,
                },
                apply_result=apply_result,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=complete_memory_context,
                command="complete",
                evaluation_gate=apply_result.get("evaluation_gate"),
            )
            emit_result(result)
            return 1

        result = make_result(
            status=verify_result.get("status", "error"),
            command="complete",
            messages=[
                *apply_result.get("messages", []),
                *verify_result.get("messages", []),
            ],
            artifacts=[
                *(apply_result.get("artifacts") or []),
                *(verify_result.get("artifacts") or []),
            ],
            summary={
                **summary,
                "apply_status": apply_result.get("status"),
                "verify_status": verify_result.get("status"),
                "applied_report_id": applied_report_id,
                "verify_finding_count": (verify_result.get("summary") or {}).get("finding_count"),
            },
            apply_result=apply_result,
            verify_result=verify_result,
            applied_report={
                "id": applied_report_id,
                "name": (apply_result.get("applied_report") or {}).get("name"),
            },
            command_class="mutating",
        )
        result = _attach_memory_record(
            result=result,
            planning_context=complete_memory_context,
            command="complete",
            evaluation_gate=apply_result.get("evaluation_gate"),
        )
        emit_result(result)
        return 0 if verify_exit_code == 0 and verify_result.get("status") != "error" else 1

    baseline_describe_json = Path(args.baseline_report_describe_json) if args.baseline_report_describe_json else None
    try:
        preview, fill_requirements, preview_summary, command_class, autofill_summary = prepare_report_preview(
            build_package=build_package,
            report_id=args.report_id,
            clone_from_report_id=args.clone_from_report_id,
            folder_id=args.folder_id,
            report_type=args.report_type,
            baseline_describe_json=baseline_describe_json,
            autofill_live=args.autofill_live,
            target_org=args.target_org,
            autofill_vocab=autofill_vocab,
            filter_overrides=report_filter_overrides,
        )
    except ValueError as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "target_org_required", str(exc))],
            summary=summary,
            command_class="live_read" if args.autofill_live else "read_only",
        )
        emit_result(result)
        return 1
    except Exception as exc:
        result = make_result(
            status="error",
            command=args.command,
            messages=[make_message("error", "autofill_load_failed", str(exc))],
            summary=summary,
            command_class="live_read" if args.autofill_live else "read_only",
        )
        emit_result(result)
        return 1

    if args.command == "apply":
        output_dir = Path(args.output_dir) if args.output_dir else None
        evaluation_path = Path(args.evaluation) if args.evaluation else None
        evaluation_gate, gate_messages, gate_error = _resolve_report_evaluation_gate(
            build_package=build_package,
            evaluation_path=evaluation_path,
            require_pass=args.apply,
            allow_missing=args.allow_missing_evaluation,
        )
        apply_memory_context = _derive_report_memory_context(
            build_package=build_package,
            planning_context=planning_context,
            evaluation_gate=evaluation_gate,
            output_dir=output_dir,
            package_path=Path(args.package),
            command="apply",
        )
        if gate_error is not None:
            result = make_result(
                status="error",
                command="apply",
                messages=[make_message("error", gate_error["code"], gate_error["text"])],
                summary=summary,
                command_class="mutating" if args.apply else "read_only",
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=apply_memory_context,
                    command="apply",
                    evaluation_gate=evaluation_gate,
                )
            emit_result(result)
            return 1

        effective_preview, effective_fill_requirements, promotion_summary = _promote_apply_preview_to_create_new(
            preview,
            fill_requirements,
            report_id=args.report_id,
        )
        external_fill_requirements = _external_fill_requirements(effective_fill_requirements)
        native_authoring_support = str(
            preview_summary.get("native_authoring_support")
            or summary.get("native_authoring_support")
            or "unsupported"
        )
        native_authoring_ready = native_authoring_support == "fully_supported"
        apply_ready = not external_fill_requirements and native_authoring_ready
        apply_summary = {
            "mode": "apply" if args.apply else "dry_run",
            "strategy": effective_preview.get("strategy") or preview_summary.get("strategy"),
            "source_strategy": preview_summary.get("strategy"),
            "request_count": len(effective_preview.get("requests") or []),
            "fill_requirement_count": len(effective_fill_requirements),
            "external_fill_requirement_count": len(external_fill_requirements),
            "internal_fill_requirement_count": len(effective_fill_requirements) - len(external_fill_requirements),
            "resolved_filter_override_count": preview_summary.get("resolved_filter_override_count", 0),
            "manual_filter_intent_count": preview_summary.get("manual_filter_intent_count", 0),
            "manual_detail_intent_count": preview_summary.get("manual_detail_intent_count", 0),
            "omitted_sort_intent_count": preview_summary.get("omitted_sort_intent_count", 0),
            "native_authoring_support": native_authoring_support,
            "native_authoring_ready": native_authoring_ready,
            "apply_ready": apply_ready,
            "evaluation_verdict": (evaluation_gate or {}).get("verdict"),
            "evaluation_bypassed": bool((evaluation_gate or {}).get("bypassed")),
        }
        if promotion_summary is not None:
            apply_summary["strategy_promotion"] = promotion_summary
        request_preview = {
            "artifact_type": "salesforce_report_apply_preview",
            "mode": apply_summary["mode"],
            "target_org": args.target_org,
            "strategy": apply_summary["strategy"],
            "source_strategy": apply_summary["source_strategy"],
            "requests": effective_preview.get("requests") or [],
            "fill_requirements": effective_fill_requirements,
            "external_fill_requirements": external_fill_requirements,
            "applied_filter_overrides": effective_preview.get("applied_filter_overrides", []),
            "manual_filter_intents": effective_preview.get("manual_filter_intents", []),
            "manual_detail_intents": effective_preview.get("manual_detail_intents", []),
            "omitted_sort_intents": effective_preview.get("omitted_sort_intents", []),
        }
        if promotion_summary is not None:
            request_preview["strategy_promotion"] = promotion_summary
        artifacts: list[dict[str, str]] = []
        if output_dir:
            output_dir.mkdir(parents=True, exist_ok=True)
            preview_path = output_dir / "salesforce_report_apply_preview.json"
            preview_path.write_text(json.dumps(request_preview, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_report_apply_preview", "path": str(preview_path)})
            _append_evaluation_bypass_artifact(
                output_dir=output_dir,
                artifacts=artifacts,
                command="apply",
                target_org=args.target_org,
                evaluation_gate=evaluation_gate,
                summary=apply_summary,
            )

        if args.apply and not args.target_org:
            result = make_result(
                status="error",
                command="apply",
                messages=[
                    *gate_messages,
                    make_message("error", "target_org_required", "--target-org is required with --apply."),
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=fill_requirements,
                evaluation_gate=evaluation_gate,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=apply_memory_context,
                command="apply",
                evaluation_gate=evaluation_gate,
            )
            emit_result(result)
            return 1

        if not apply_ready:
            blocker_messages: list[dict[str, str]] = [*gate_messages]
            if not native_authoring_ready:
                blocker_code, blocker_text = _native_authoring_support_message(native_authoring_support)
                blocker_messages.append(
                    make_message("error" if args.apply else "warn", blocker_code, blocker_text)
                )
            if external_fill_requirements:
                blocker_messages.append(
                    make_message(
                        "error" if args.apply else "warn",
                        "apply_blocked",
                        "The report request sequence still has unresolved external fill requirements.",
                    )
                )
            result = make_result(
                status="error" if args.apply else "warn",
                command="apply",
                messages=[
                    *blocker_messages,
                    *[
                        make_message(
                            "warn",
                            "external_fill_requirement",
                            f"{item['category']}: {item.get('source_label') or item['target_path']}",
                        )
                        for item in external_fill_requirements
                    ],
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=effective_fill_requirements,
                autofill_summary=autofill_summary,
                evaluation_gate=evaluation_gate,
                command_class="mutating" if args.apply else command_class,
            )
            if args.apply:
                result = _attach_memory_record(
                    result=result,
                    planning_context=apply_memory_context,
                    command="apply",
                    evaluation_gate=evaluation_gate,
                )
            emit_result(result)
            return 1 if args.apply else 0

        if not args.apply:
            result = make_result(
                status="ok",
                command="apply",
                messages=[
                    *gate_messages,
                    *(
                        [
                            make_message(
                                "info",
                                "filter_override_applied",
                                f"Resolved {apply_summary['resolved_filter_override_count']} packaged report filter intent(s) from explicit overrides.",
                            )
                        ]
                        if apply_summary.get("resolved_filter_override_count")
                        else []
                    ),
                    make_message(
                        "info",
                        "apply_preview_ready",
                        "Salesforce report REST apply preview is ready with no external blockers."
                        if promotion_summary is None
                        else "Salesforce report REST apply preview is ready and promoted to create_new because the clone source is now only autofill context.",
                    )
                ],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=effective_fill_requirements,
                autofill_summary=autofill_summary,
                evaluation_gate=evaluation_gate,
                command_class=command_class,
            )
            emit_result(result)
            return 0

        try:
            execution_requests, responses, cloned_report_id = execute_report_requests(
                effective_preview,
                target_org=args.target_org,
            )
        except Exception as exc:
            result = make_result(
                status="error",
                command="apply",
                messages=[*gate_messages, make_message("error", "apply_failed", str(exc)[:2000])],
                artifacts=artifacts,
                summary={**summary, **preview_summary},
                apply_summary=apply_summary,
                request_preview=request_preview,
                fill_requirements=effective_fill_requirements,
                autofill_summary=autofill_summary,
                evaluation_gate=evaluation_gate,
                command_class="mutating",
            )
            result = _attach_memory_record(
                result=result,
                planning_context=apply_memory_context,
                command="apply",
                evaluation_gate=evaluation_gate,
            )
            emit_result(result)
            return 1

        applied_report = responses[-1] if responses else {}
        if args.output_dir:
            output_dir = Path(args.output_dir)
            apply_path = output_dir / "salesforce_report_apply_response.json"
            apply_payload = {
                "artifact_type": "salesforce_report_apply_response",
                "execution_requests": execution_requests,
                "responses": responses,
                "cloned_report_id": cloned_report_id,
            }
            apply_path.write_text(json.dumps(apply_payload, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_report_apply_response", "path": str(apply_path)})

        result = make_result(
            status="ok",
            command="apply",
            messages=[
                *gate_messages,
                *(
                    [
                        make_message(
                            "info",
                            "filter_override_applied",
                            f"Resolved {apply_summary['resolved_filter_override_count']} packaged report filter intent(s) from explicit overrides.",
                        )
                    ]
                    if apply_summary.get("resolved_filter_override_count")
                    else []
                ),
                make_message(
                    "info",
                    "apply_complete",
                    f"Applied the packaged report REST sequence to report {applied_report.get('report_id') or cloned_report_id}.",
                )
            ],
            artifacts=artifacts,
            summary={**summary, **preview_summary},
            apply_summary=apply_summary,
            request_preview=request_preview,
            fill_requirements=effective_fill_requirements,
            autofill_summary=autofill_summary,
            evaluation_gate=evaluation_gate,
            execution_requests=execution_requests,
            responses=responses,
            applied_report={
                "id": applied_report.get("report_id") or cloned_report_id,
                "name": applied_report.get("name"),
            },
            command_class="mutating",
        )
        result = _attach_memory_record(
            result=result,
            planning_context=apply_memory_context,
            command="apply",
            evaluation_gate=evaluation_gate,
        )
        emit_result(result)
        return 0

    artifacts: list[dict[str, str]] = []
    if args.output_dir:
        output_dir = Path(args.output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        preview_path = output_dir / "salesforce_report_rest_preview.json"
        fill_path = output_dir / "salesforce_report_fill_requirements.json"
        autofill_path = output_dir / "salesforce_report_autofill_summary.json"
        preview_path.write_text(json.dumps(preview, indent=2), encoding="utf-8")
        fill_path.write_text(json.dumps({"fill_requirements": fill_requirements}, indent=2), encoding="utf-8")
        artifacts.extend(
            [
                {"type": "salesforce_report_rest_preview", "path": str(preview_path)},
                {"type": "salesforce_report_fill_requirements", "path": str(fill_path)},
            ]
        )
        if autofill_summary is not None:
            autofill_path.write_text(json.dumps(autofill_summary, indent=2), encoding="utf-8")
            artifacts.append({"type": "salesforce_report_autofill_summary", "path": str(autofill_path)})

    result = make_result(
        status=(
            "warn"
            if fill_requirements
            or warnings
            or preview_summary.get("manual_filter_intent_count")
            or preview_summary.get("omitted_sort_intent_count")
            else "ok"
        ),
        command="preview",
        messages=[
            *[make_message("warn", "build_package_warning", item) for item in warnings],
            *(
                [
                    make_message(
                        "info",
                        "filter_override_applied",
                        f"Resolved {preview_summary['resolved_filter_override_count']} packaged report filter intent(s) from explicit overrides.",
                    )
                ]
                if preview_summary.get("resolved_filter_override_count")
                else []
            ),
            *(
                [
                    make_message(
                        "warn",
                        "manual_filter_intent",
                        f"Preserved {preview_summary['manual_filter_intent_count']} manual native-report filter intent(s) outside the REST payload.",
                    )
                ]
                if preview_summary.get("manual_filter_intent_count")
                else []
            ),
            *(
                [
                    make_message(
                        "warn",
                        "manual_detail_intent",
                        f"Preserved {preview_summary['manual_detail_intent_count']} semantic report column intent(s) outside the REST payload.",
                    )
                ]
                if preview_summary.get("manual_detail_intent_count")
                else []
            ),
            *(
                [
                    make_message(
                        "warn",
                        "omitted_sort_intent",
                        f"Omitted {preview_summary['omitted_sort_intent_count']} unsupported semantic sort intent(s) from the Reports REST payload.",
                    )
                ]
                if preview_summary.get("omitted_sort_intent_count")
                else []
            ),
            make_message(
                "warn"
                if fill_requirements
                or preview_summary.get("manual_filter_intent_count")
                or preview_summary.get("manual_detail_intent_count")
                or preview_summary.get("omitted_sort_intent_count")
                else "info",
                "rest_preview_ready",
                "Compiled a Salesforce report REST preview; fill the unresolved mappings and review the preserved manual authoring intents before live use."
                if fill_requirements
                or preview_summary.get("manual_filter_intent_count")
                or preview_summary.get("manual_detail_intent_count")
                or preview_summary.get("omitted_sort_intent_count")
                else "Compiled a Salesforce report REST preview with no unresolved mappings.",
            ),
        ],
        artifacts=artifacts,
        command_class=command_class,
        summary={**summary, **preview_summary},
        rest_preview=preview,
        fill_requirements=fill_requirements,
        autofill_summary=autofill_summary,
    )
    emit_result(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
