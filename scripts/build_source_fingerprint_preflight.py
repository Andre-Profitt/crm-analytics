#!/usr/bin/env python3
"""Fingerprint configured Salesforce sources before monthly extraction."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.extract_salesforce_sources import load_territories  # noqa: E402
from scripts.monthly_platform.contracts import Finding, utc_now_iso  # noqa: E402
from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.monthly_platform.salesforce_auth import (  # noqa: E402
    DEFAULT_TARGET_ORG,
    build_salesforce_session,
    get_salesforce_auth,
)
from scripts.monthly_platform.salesforce_reports import (  # noqa: E402
    SalesforceSourceClient,
    SalesforceSourceResult,
)
from scripts.monthly_platform.source_requirements import (  # noqa: E402
    SourcePlanItem,
    build_source_requirement_plan,
    filter_plan_items,
    load_source_requirements,
    requirement_summary,
)
from scripts.monthly_platform.storage import sha256_bytes, stable_json_bytes  # noqa: E402


DEFAULT_REQUIREMENTS_PATH = ROOT / "config" / "monthly_source_requirements.json"
DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "source_fingerprint_preflight"
SCHEMA_VERSION = "monthly_platform.source_fingerprint_preflight.v1"


def build_source_fingerprint_preflight(
    *,
    snapshot_date: str,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    target_org: str = DEFAULT_TARGET_ORG,
    dry_run: bool = False,
    only_requirement: str | None = None,
    only_territory: str | None = None,
    max_sources: int | None = None,
    fail_fast: bool = False,
) -> dict[str, Any]:
    started_at = utc_now_iso()
    started = time.monotonic()
    period = resolve_period_context(snapshot_date=snapshot_date)
    registry = load_source_requirements(requirements_path)
    territories = load_territories(territory_config_path)
    plan = build_source_requirement_plan(
        registry=registry,
        territories=territories,
        period=period,
    )
    selected_items = filter_plan_items(
        plan,
        only_requirement=only_requirement,
        only_territory=only_territory,
        max_sources=max_sources,
    )
    source_run_id = run_id or f"source-fingerprint-{snapshot_date}"
    output_dir = output_root / snapshot_date / source_run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "source_fingerprint_manifest.json"

    findings = list(plan.findings)
    fingerprints: list[dict[str, Any]] = []
    probed = 0
    failed = 0
    fallback_probe_count = 0

    if plan.status != "blocked" and not dry_run:
        auth = get_salesforce_auth(target_org=target_org)
        client = SalesforceSourceClient(
            auth=auth,
            session=build_salesforce_session(auth),
        )
        for item in selected_items:
            try:
                fingerprint, item_findings = _fingerprint_item(client, item)
                fallback_probe_count += int(
                    fingerprint.get("describe_mode") == "ui_api_fallback_probe"
                )
                fingerprints.append(fingerprint)
                findings.extend(item_findings)
                probed += 1
            except Exception as exc:
                failed += 1
                findings.append(
                    Finding(
                        severity="high",
                        issue="source_fingerprint_failed",
                        evidence=(
                            f"{item.requirement_id} {item.territory or 'global'} "
                            f"{item.period_role} {item.source_type} {item.source_id}: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
                )
                if fail_fast:
                    break
    elif dry_run:
        fingerprints = [_planned_fingerprint(item) for item in selected_items]

    high_findings = [finding for finding in findings if finding.severity == "high"]
    medium_findings = [finding for finding in findings if finding.severity == "medium"]
    status = "blocked" if high_findings else "ok"
    if dry_run and plan.status != "blocked":
        status = "planned"

    by_source_type = Counter(
        str(fingerprint.get("source_type")) for fingerprint in fingerprints
    )
    by_describe_mode = Counter(
        str(fingerprint.get("describe_mode")) for fingerprint in fingerprints
    )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "started_at": started_at,
        "finished_at": utc_now_iso(),
        "duration_seconds": round(time.monotonic() - started, 3),
        "status": status,
        "snapshot_date": snapshot_date,
        "run_id": source_run_id,
        "target_org": target_org,
        "dry_run": dry_run,
        "period": period.as_dict(),
        "source_plan_summary": requirement_summary(plan),
        "summary": {
            "selected_source_count": len(selected_items),
            "fingerprinted_source_count": len(fingerprints),
            "probed_source_count": probed,
            "failed_source_count": failed,
            "fallback_probe_count": fallback_probe_count,
            "finding_count": len(findings),
            "high_finding_count": len(high_findings),
            "medium_finding_count": len(medium_findings),
            "by_source_type": dict(sorted(by_source_type.items())),
            "by_describe_mode": dict(sorted(by_describe_mode.items())),
        },
        "fingerprints": fingerprints,
        "findings": [finding.model_dump(mode="json") for finding in findings],
    }
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    manifest["output_path"] = str(output_path)
    return manifest


def _fingerprint_item(
    client: SalesforceSourceClient,
    item: SourcePlanItem,
) -> tuple[dict[str, Any], list[Finding]]:
    findings: list[Finding] = []
    describe_mode = "describe"
    try:
        result = _describe_item(client, item)
    except Exception as exc:
        if item.source_type != "salesforce_list_view":
            raise
        result = client.run_list_view(
            list_view_id=item.source_id,
            source_label=item.source_label or item.source_id,
            page_size=1,
            max_records=1,
        )
        describe_mode = "ui_api_fallback_probe"
        findings.append(
            Finding(
                severity="medium",
                issue="list_view_describe_fallback_used",
                evidence=(
                    f"{item.requirement_id} {item.territory or 'global'} "
                    f"{item.period_role} {item.source_id}: {type(exc).__name__}: {exc}"
                ),
            )
        )
    metadata = dict(result.metadata or {})
    if describe_mode == "ui_api_fallback_probe":
        metadata = {
            **metadata,
            "observed_columns": sorted(
                {key for row in result.rows for key in row if not key.endswith("__display")}
            ),
            "row_count_probe": len(result.rows),
        }
    column_tokens = _column_tokens(metadata)
    missing_required_fields = [
        field.name
        for field in item.required_fields
        if field.required and not _field_present(field.name, column_tokens)
    ]
    if missing_required_fields:
        findings.append(
            Finding(
                severity="medium",
                issue="source_fingerprint_required_fields_not_observed",
                evidence=(
                    f"{item.requirement_id} {item.territory or 'global'} "
                    f"{item.period_role} {item.source_id}: "
                    f"{', '.join(missing_required_fields)}"
                ),
            )
        )
    columns_hash = sha256_bytes(stable_json_bytes(_fingerprint_columns(metadata)))
    filter_hash = sha256_bytes(stable_json_bytes(_fingerprint_filters(metadata)))
    fingerprint_input = {
        "source_type": item.source_type,
        "salesforce_object": item.salesforce_object,
        "source_id": item.source_id,
        "source_label": item.source_label,
        "requirement_id": item.requirement_id,
        "dataset": item.dataset,
        "territory": item.territory,
        "period_role": item.period_role,
        "quarter_label": item.quarter_label,
        "columns_hash": columns_hash,
        "filter_hash": filter_hash,
        "metadata": metadata,
    }
    return {
        "source_key": _source_key(item),
        "status": "ok",
        "describe_mode": describe_mode,
        "source_type": item.source_type,
        "salesforce_object": item.salesforce_object,
        "salesforce_id": item.source_id,
        "label": item.source_label,
        "requirement_id": item.requirement_id,
        "dataset": item.dataset,
        "territory": item.territory,
        "director": item.director,
        "region": item.region,
        "period_role": item.period_role,
        "quarter_label": item.quarter_label,
        "quarter_title": item.quarter_title,
        "required_fields": [field.model_dump(mode="json") for field in item.required_fields],
        "missing_required_fields": missing_required_fields,
        "row_count_policy": item.row_count_policy.model_dump(mode="json"),
        "metadata": metadata,
        "columns_hash": columns_hash,
        "filter_hash": filter_hash,
        "fingerprint_hash": sha256_bytes(stable_json_bytes(fingerprint_input)),
        "duration_ms": result.duration_ms,
        "status_code": result.status_code,
    }, findings


def _describe_item(
    client: SalesforceSourceClient,
    item: SourcePlanItem,
) -> SalesforceSourceResult:
    if item.source_type == "salesforce_report":
        return client.describe_report(
            report_id=item.source_id,
            source_label=item.source_label or item.source_id,
        )
    if item.source_type == "salesforce_list_view":
        return client.describe_list_view(
            list_view_id=item.source_id,
            source_label=item.source_label or item.source_id,
            sobject_type=item.salesforce_object,
        )
    raise ValueError(f"Unsupported source type: {item.source_type}")


def _planned_fingerprint(item: SourcePlanItem) -> dict[str, Any]:
    return {
        "source_key": _source_key(item),
        "status": "planned",
        "describe_mode": "dry_run",
        "source_type": item.source_type,
        "salesforce_object": item.salesforce_object,
        "salesforce_id": item.source_id,
        "label": item.source_label,
        "requirement_id": item.requirement_id,
        "dataset": item.dataset,
        "territory": item.territory,
        "director": item.director,
        "period_role": item.period_role,
        "quarter_label": item.quarter_label,
        "quarter_title": item.quarter_title,
    }


def _source_key(item: SourcePlanItem) -> str:
    territory = (item.territory or "global").lower().replace(" ", "_")
    return (
        f"{item.requirement_id}.{territory}."
        f"{item.period_role}.{item.quarter_label}.{item.source_id}"
    )


def _column_tokens(metadata: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for value in metadata.get("detail_columns") or []:
        _add_column_token(tokens, value)
    for value in metadata.get("detail_column_info_keys") or []:
        _add_column_token(tokens, value)
    for column in metadata.get("columns") or []:
        if not isinstance(column, dict):
            continue
        for key in ("fieldName", "name", "label", "selectListItem"):
            _add_column_token(tokens, column.get(key))
    for value in metadata.get("observed_columns") or []:
        _add_column_token(tokens, value)
    return tokens


def _add_column_token(tokens: set[str], value: Any) -> None:
    if value is None:
        return
    raw_value = str(value)
    for token_value in {raw_value, _unwrap_salesforce_function(raw_value)}:
        normalized = _normalize_field_token(token_value)
        if normalized:
            tokens.add(normalized)
            parts = [part for part in normalized.split(".") if part]
            tokens.update(parts)
            for index in range(len(parts) - 1):
                tokens.add(".".join(parts[index : index + 2]))


def _field_present(field_name: str, column_tokens: set[str]) -> bool:
    return any(alias in column_tokens for alias in _field_aliases(field_name))


def _normalize_field_token(value: str) -> str:
    return (
        value.replace("__hd", "")
        .replace("__hst", "")
        .replace("_hst", "")
        .replace("__c", "")
        .replace("__r", "")
        .replace("_", "")
        .replace(" ", "")
        .lower()
    )


def _field_aliases(field_name: str) -> set[str]:
    normalized = _normalize_field_token(field_name)
    aliases = {normalized}
    aliases.update(part for part in normalized.split(".") if part)
    if normalized in {"opportunityname", "opportunity.name"}:
        aliases.update({"opportunityname", "opportunity.name"})
    if normalized in {"accountname", "account.name"}:
        aliases.update({"accountname", "account.name"})
    if normalized in {"stage", "stagename"}:
        aliases.update({"stage", "stagename"})
    if normalized in {"arr"}:
        aliases.update(
            {
                "arr",
                "aptsforecastarr",
                "aptsopportunityarr",
                "forecastarr",
                "opportunityarr",
            }
        )
    if normalized in {"salesregion", "sales.region"}:
        aliases.update({"salesregion", "region", "account.region"})
    return aliases


def _unwrap_salesforce_function(value: str) -> str:
    stripped = value.strip()
    if not stripped.endswith(")") or "(" not in stripped:
        return stripped
    return stripped[stripped.find("(") + 1 : -1].strip()


def _fingerprint_columns(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "detail_columns": metadata.get("detail_columns") or [],
        "detail_column_info_keys": metadata.get("detail_column_info_keys") or [],
        "columns": metadata.get("columns") or [],
        "observed_columns": metadata.get("observed_columns") or [],
    }


def _fingerprint_filters(metadata: dict[str, Any]) -> dict[str, Any]:
    return {
        "report_filters": metadata.get("report_filters") or [],
        "standard_date_filter": metadata.get("standard_date_filter"),
        "cross_filters": metadata.get("cross_filters") or [],
        "scope": metadata.get("scope"),
        "query": metadata.get("query"),
        "where_condition": metadata.get("where_condition"),
        "order_by": metadata.get("order_by"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--only-requirement")
    parser.add_argument("--only-territory")
    parser.add_argument("--max-sources", type=int)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    manifest = build_source_fingerprint_preflight(
        snapshot_date=args.snapshot_date,
        requirements_path=args.requirements,
        territory_config_path=args.territory_config,
        output_root=args.output_root,
        run_id=args.run_id,
        target_org=args.target_org,
        dry_run=args.dry_run,
        only_requirement=args.only_requirement,
        only_territory=args.only_territory,
        max_sources=args.max_sources,
        fail_fast=args.fail_fast,
    )
    if args.json:
        print(json.dumps(manifest, indent=2))
    return 0 if manifest["status"] in {"ok", "planned"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
