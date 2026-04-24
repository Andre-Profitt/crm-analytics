#!/usr/bin/env python3
"""Audit SD Monthly Opportunity list views for safe territory filters."""

from __future__ import annotations

import argparse
import html
import json
import sys
import urllib.parse
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.manage_opportunity_list_views import (  # noqa: E402
    DEFAULT_TARGET_ORG,
    api_request,
    get_auth,
)
from scripts.manage_pipeline_open_list_views import (  # noqa: E402
    DEFAULT_TERRITORY_CONFIG,
    FORECAST_CATEGORIES,
    OPEN_STAGES,
    TERRITORY_FILTERS,
    list_view_filter,
    pipeline_open_filters,
)


SCHEMA_VERSION = "monthly_platform.pi_list_view_filter_audit.v1"
DEFAULT_OUTPUT_PATH = ROOT / "output" / "pi_list_view_filter_audit.json"
DEFAULT_FIELD_GUARDRAILS_PATH = ROOT / "config" / "salesforce_field_guardrails.json"


@dataclass(frozen=True)
class ListViewEntry:
    territory: str
    source_kind: str
    list_view_id: str
    configured_label: str
    quarter_label: str | None = None


def audit_configured_list_views(
    *,
    instance_url: str,
    token: str,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    object_api_name: str = "Opportunity",
    field_guardrails_path: Path = DEFAULT_FIELD_GUARDRAILS_PATH,
    forward_close_date_literal: str = "NEXT FISCAL QUARTER",
) -> dict[str, Any]:
    config = _load_json(territory_config_path)
    entries = configured_list_view_entries(config)
    dead_or_invalid_filter_fields = load_dead_or_invalid_filter_fields(
        field_guardrails_path,
        object_api_name=object_api_name,
    )
    records_by_id = fetch_list_view_records_by_id(
        instance_url=instance_url,
        token=token,
        object_api_name=object_api_name,
        list_view_ids=[entry.list_view_id for entry in entries],
    )
    infos_by_api_name = {
        str(record["DeveloperName"]): fetch_list_info(
            instance_url=instance_url,
            token=token,
            object_api_name=object_api_name,
            api_name=str(record["DeveloperName"]),
        )
        for record in records_by_id.values()
    }
    return audit_list_view_infos(
        entries=entries,
        records_by_id=records_by_id,
        infos_by_api_name=infos_by_api_name,
        territory_config_path=territory_config_path,
        field_guardrails_path=field_guardrails_path,
        dead_or_invalid_filter_fields=dead_or_invalid_filter_fields,
        forward_close_date_literal=forward_close_date_literal,
    )


def configured_list_view_entries(config: dict[str, Any]) -> list[ListViewEntry]:
    territories = config.get("territories")
    if not isinstance(territories, dict) or not territories:
        raise ValueError("expected config.territories object")
    entries: list[ListViewEntry] = []
    for territory, item in territories.items():
        if item.get("pi_list_view_id"):
            entries.append(
                ListViewEntry(
                    territory=territory,
                    source_kind="current_pi",
                    list_view_id=str(item["pi_list_view_id"]),
                    configured_label=str(item.get("pi_list_view_label") or ""),
                )
            )
        if item.get("pipeline_open_list_view_id"):
            entries.append(
                ListViewEntry(
                    territory=territory,
                    source_kind="pipeline_open",
                    list_view_id=str(item["pipeline_open_list_view_id"]),
                    configured_label=str(item.get("pipeline_open_list_view_label") or ""),
                )
            )
        for quarter_label, forward in sorted(
            (item.get("forward_quarter_pi_list_views") or {}).items()
        ):
            if not forward.get("list_view_id"):
                continue
            entries.append(
                ListViewEntry(
                    territory=territory,
                    source_kind="forward_pi",
                    list_view_id=str(forward["list_view_id"]),
                    configured_label=str(forward.get("list_view_label") or ""),
                    quarter_label=str(quarter_label),
                )
            )
    return entries


def fetch_list_view_records_by_id(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    list_view_ids: list[str],
) -> dict[str, dict[str, Any]]:
    if not list_view_ids:
        return {}
    quoted_ids = ", ".join(f"'{list_view_id}'" for list_view_id in sorted(set(list_view_ids)))
    query = (
        "SELECT Id, Name, DeveloperName, SobjectType "
        f"FROM ListView WHERE SobjectType = '{object_api_name}' AND Id IN ({quoted_ids})"
    )
    encoded_query = urllib.parse.quote(query, safe="")
    response = api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/query?q={encoded_query}",
    )
    return {str(record["Id"]): record for record in response.get("records", [])}


def fetch_list_info(
    *,
    instance_url: str,
    token: str,
    object_api_name: str,
    api_name: str,
) -> dict[str, Any]:
    return api_request(
        method="GET",
        instance_url=instance_url,
        token=token,
        path=f"/ui-api/list-info/{object_api_name}/{api_name}",
    )


def audit_list_view_infos(
    *,
    entries: list[ListViewEntry],
    records_by_id: dict[str, dict[str, Any]],
    infos_by_api_name: dict[str, dict[str, Any]],
    territory_config_path: Path | None = None,
    field_guardrails_path: Path | None = DEFAULT_FIELD_GUARDRAILS_PATH,
    dead_or_invalid_filter_fields: set[str] | None = None,
    forward_close_date_literal: str = "NEXT FISCAL QUARTER",
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    views: list[dict[str, Any]] = []
    if dead_or_invalid_filter_fields is None:
        dead_or_invalid_filter_fields = load_dead_or_invalid_filter_fields(
            field_guardrails_path or DEFAULT_FIELD_GUARDRAILS_PATH,
            object_api_name="Opportunity",
        )
    for entry in entries:
        if entry.territory not in TERRITORY_FILTERS:
            findings.append(
                _finding(
                    severity="high",
                    issue="territory_filter_policy_missing",
                    evidence=entry.territory,
                    entry=entry,
                )
            )
            continue
        record = records_by_id.get(entry.list_view_id)
        if not record:
            findings.append(
                _finding(
                    severity="high",
                    issue="list_view_id_missing",
                    evidence=entry.list_view_id,
                    entry=entry,
                )
            )
            continue
        api_name = str(record.get("DeveloperName") or "")
        info = infos_by_api_name.get(api_name)
        if not info:
            findings.append(
                _finding(
                    severity="high",
                    issue="list_view_info_missing",
                    evidence=api_name,
                    entry=entry,
                )
            )
            continue
        filters = _normalized_filters(info.get("filteredByInfo") or [])
        views.append(
            {
                **asdict(entry),
                "api_name": api_name,
                "label": html.unescape(str(info.get("label") or record.get("Name") or "")),
                "filters": filters,
            }
        )
        _validate_no_dead_filters(
            entry=entry,
            filters=filters,
            findings=findings,
            dead_or_invalid_filter_fields=dead_or_invalid_filter_fields,
        )
        _validate_expected_filters(
            entry=entry,
            filters=filters,
            findings=findings,
            forward_close_date_literal=forward_close_date_literal,
        )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if findings else "ok",
        "territory_config_path": str(territory_config_path) if territory_config_path else None,
        "field_guardrails_path": str(field_guardrails_path) if field_guardrails_path else None,
        "dead_or_invalid_filter_field_count": len(dead_or_invalid_filter_fields),
        "view_count": len(views),
        "configured_view_count": len(entries),
        "finding_count": len(findings),
        "high_finding_count": sum(
            1 for finding in findings if finding.get("severity") == "high"
        ),
        "findings": findings,
        "views": views,
    }


def expected_filters_for_entry(
    entry: ListViewEntry,
    *,
    forward_close_date_literal: str = "NEXT FISCAL QUARTER",
) -> list[dict[str, Any]]:
    if entry.source_kind == "pipeline_open":
        return pipeline_open_filters(entry.territory)
    filters = [
        list_view_filter(item["fieldApiName"], list(item["operandLabels"]))
        for item in TERRITORY_FILTERS[entry.territory]
    ]
    filters.extend(
        [
            list_view_filter("StageName", OPEN_STAGES),
            list_view_filter("Type", ["Land"]),
            list_view_filter("ForecastCategoryName", FORECAST_CATEGORIES),
        ]
    )
    if entry.source_kind == "forward_pi":
        filters.append(list_view_filter("CloseDate", [forward_close_date_literal]))
    return filters


def _validate_no_dead_filters(
    *,
    entry: ListViewEntry,
    filters: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    dead_or_invalid_filter_fields: set[str],
) -> None:
    for filter_item in filters:
        field = str(filter_item.get("fieldApiName") or "")
        if field not in dead_or_invalid_filter_fields:
            continue
        findings.append(
            _finding(
                severity="high",
                issue="dead_or_invalid_filter_present",
                evidence=field,
                entry=entry,
            )
        )


def _validate_expected_filters(
    *,
    entry: ListViewEntry,
    filters: list[dict[str, Any]],
    findings: list[dict[str, Any]],
    forward_close_date_literal: str,
) -> None:
    actual_by_field = {str(item.get("fieldApiName") or ""): item for item in filters}
    for expected in expected_filters_for_entry(
        entry,
        forward_close_date_literal=forward_close_date_literal,
    ):
        field = str(expected["fieldApiName"])
        actual = actual_by_field.get(field)
        if not actual:
            findings.append(
                _finding(
                    severity="high",
                    issue="expected_filter_missing",
                    evidence=field,
                    entry=entry,
                )
            )
            continue
        expected_operator = str(expected.get("operator") or "")
        actual_operator = str(actual.get("operator") or "")
        if actual_operator != expected_operator:
            findings.append(
                _finding(
                    severity="high",
                    issue="expected_filter_operator_mismatch",
                    evidence=(
                        f"{field}: expected {expected_operator}, got {actual_operator}"
                    ),
                    entry=entry,
                )
            )
        expected_labels = _normalized_labels(expected.get("operandLabels") or [])
        actual_labels = _normalized_labels(actual.get("operandLabels") or [])
        if actual_labels != expected_labels:
            findings.append(
                _finding(
                    severity="high",
                    issue="expected_filter_value_mismatch",
                    evidence=f"{field}: expected {expected_labels}, got {actual_labels}",
                    entry=entry,
                )
            )


def _normalized_filters(raw_filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "fieldApiName": str(item.get("fieldApiName") or ""),
            "operator": str(item.get("operator") or ""),
            "operandLabels": _normalized_labels(item.get("operandLabels") or []),
        }
        for item in raw_filters
    ]


def _normalized_labels(values: list[Any]) -> list[str]:
    return [html.unescape(str(value)) for value in values]


def _finding(
    *,
    severity: str,
    issue: str,
    evidence: str,
    entry: ListViewEntry,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "issue": issue,
        "evidence": evidence,
        "territory": entry.territory,
        "source_kind": entry.source_kind,
        "list_view_id": entry.list_view_id,
        "quarter_label": entry.quarter_label,
    }


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def load_dead_or_invalid_filter_fields(
    path: Path,
    *,
    object_api_name: str,
) -> set[str]:
    payload = _load_json(path)
    by_object = payload.get("dead_or_invalid_filter_fields_by_object")
    if not isinstance(by_object, dict):
        raise ValueError(f"{path}: expected dead_or_invalid_filter_fields_by_object")
    fields: list[Any] = []
    fields.extend(by_object.get("*") or [])
    fields.extend(by_object.get(object_api_name) or [])
    return {str(field) for field in fields if str(field).strip()}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
    parser.add_argument("--object-api-name", default="Opportunity")
    parser.add_argument(
        "--field-guardrails",
        type=Path,
        default=DEFAULT_FIELD_GUARDRAILS_PATH,
    )
    parser.add_argument(
        "--forward-close-date-literal",
        default="NEXT FISCAL QUARTER",
    )
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args(argv)

    token, instance_url = get_auth(args.target_org)
    result = audit_configured_list_views(
        instance_url=instance_url,
        token=token,
        territory_config_path=args.territory_config,
        object_api_name=args.object_api_name,
        field_guardrails_path=args.field_guardrails,
        forward_close_date_literal=args.forward_close_date_literal,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2) + "\n")
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
