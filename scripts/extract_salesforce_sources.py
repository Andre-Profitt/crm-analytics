#!/usr/bin/env python3
"""Extract monthly Salesforce sources through the modular source registry."""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.contracts import (
    Finding,
    FindingSeverity,
    StageResult,
    utc_now_iso,
)  # noqa: E402
from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.monthly_platform.salesforce_auth import (  # noqa: E402
    DEFAULT_TARGET_ORG,
    build_salesforce_session,
    get_salesforce_auth,
)
from scripts.monthly_platform.salesforce_reports import SalesforceSourceClient  # noqa: E402
from scripts.monthly_platform.source_requirements import (  # noqa: E402
    SourcePlanItem,
    action_to_severity,
    build_source_requirement_plan,
    filter_plan_items,
    load_source_requirements,
    requirement_summary,
)
from scripts.monthly_platform.source_quality_baselines import (  # noqa: E402
    baseline_key_for_item,
    compare_run_to_baselines,
    load_baselines,
)
from scripts.monthly_platform.storage import (
    MonthlyStorage,
    sha256_bytes,
    stable_json_bytes,
)  # noqa: E402


DEFAULT_REQUIREMENTS_PATH = ROOT / "config" / "monthly_source_requirements.json"
DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "monthly_salesforce_sources"
DEFAULT_BASELINES_DIR = ROOT / "config" / "source_quality_baselines"
STAGE_NAME = "extract_salesforce_sources"


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def load_territories(path: Path) -> dict[str, Any]:
    payload = load_json(path)
    territories = payload.get("territories")
    if not isinstance(territories, dict) or not territories:
        raise ValueError(f"{path}: expected non-empty territories object")
    return territories


def extract_sources(
    *,
    snapshot_date: str,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    run_id: str | None = None,
    dry_run: bool = False,
    target_org: str = DEFAULT_TARGET_ORG,
    only_requirement: str | None = None,
    only_territory: str | None = None,
    max_sources: int | None = None,
    fail_fast: bool = False,
    baselines_dir: Path | None = None,
    enable_baselines: bool = True,
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
    storage = MonthlyStorage(
        root=output_root,
        snapshot_date=snapshot_date,
        run_id=run_id or f"salesforce-sources-{snapshot_date}",
    )
    storage.create_run(
        period_context=period.as_dict(),
        metadata={
            "requirements_path": str(requirements_path),
            "territory_config_path": str(territory_config_path),
            "dry_run": dry_run,
            "target_org": target_org,
        },
    )
    plan_artifact = storage.register_json_artifact(
        artifact_id="source_requirement_plan",
        artifact_type="source_requirement_plan",
        payload=plan.model_dump(mode="json"),
        relative_path="plans/source_requirement_plan.json",
        stage_name=STAGE_NAME,
        metadata=requirement_summary(plan),
    )

    findings = list(plan.findings)
    source_extracts = []
    output_artifacts = [plan_artifact]
    quality_sources: list[dict[str, Any]] = []
    quality_findings: list[Finding] = []
    executed = 0
    failed = 0
    skipped = 0

    if plan.status == "blocked":
        status = "blocked"
    elif dry_run:
        status = "ok"
        skipped = len(selected_items)
    else:
        auth = get_salesforce_auth(target_org=target_org)
        client = SalesforceSourceClient(
            auth=auth,
            session=build_salesforce_session(auth),
        )
        for item in selected_items:
            try:
                result = _execute_item(client, item)
                source_quality, source_quality_findings = audit_source_extract_quality(
                    item=item,
                    rows=result.rows,
                    source_metadata=result.metadata,
                )
                quality_sources.append(source_quality)
                quality_findings.extend(source_quality_findings)
                findings.extend(
                    finding
                    for finding in source_quality_findings
                    if finding.severity == "high"
                )
                extract = storage.register_source_extract(
                    source_type=result.source_type,
                    source_id=result.source_id,
                    source_label=result.source_label,
                    rows=result.rows,
                    raw_payload=result.raw_payload,
                    stage_name=STAGE_NAME,
                    territory=item.territory,
                    director=item.director,
                    region=item.region,
                    period_role=item.period_role,
                    quarter_label=item.quarter_label,
                    metadata={
                        "requirement_id": item.requirement_id,
                        "dataset": item.dataset,
                        "output_grain": item.output_grain,
                        "consumers": item.consumers,
                        "required_fields": [
                            field.model_dump(mode="json")
                            for field in item.required_fields
                        ],
                        "row_count_policy": item.row_count_policy.model_dump(
                            mode="json"
                        ),
                        "fallback_policy": item.fallback_policy.model_dump(mode="json")
                        if item.fallback_policy
                        else None,
                        "duration_ms": result.duration_ms,
                        "status_code": result.status_code,
                        "source_metadata": result.metadata,
                        "source_quality_status": source_quality["status"],
                        "source_quality_finding_count": len(source_quality_findings),
                        "source_quality_hash": source_quality["quality_hash"],
                    },
                )
                source_extracts.append(extract)
                output_artifacts.append(extract.raw_artifact)
                if extract.normalized_artifact:
                    output_artifacts.append(extract.normalized_artifact)
                executed += 1
            except Exception as exc:
                failed += 1
                findings.append(
                    Finding(
                        severity="high",
                        issue="source_extract_failed",
                        evidence=(
                            f"{item.requirement_id} {item.territory or 'global'} "
                            f"{item.period_role} {item.source_type} {item.source_id}: "
                            f"{type(exc).__name__}: {exc}"
                        ),
                    )
                )
                if fail_fast:
                    break
        quality_high_findings = [
            finding for finding in quality_findings if finding.severity == "high"
        ]
        if quality_high_findings and not failed:
            status = "blocked"
        elif failed:
            status = "failed"
        else:
            status = "ok"

    quality_audit = build_quality_audit(
        snapshot_date=snapshot_date,
        run_id=storage.run_id,
        selected_source_count=len(selected_items),
        dry_run=dry_run,
        sources=quality_sources,
        findings=quality_findings,
    )

    # Track C: compare live source-quality to calibrated baselines.
    # Read-only — never writes to baselines_dir. Baseline drift findings default
    # to ``info`` severity; only contracts that explicitly opt up to ``blocked``
    # via ``RowCountPolicy.baseline_drift_action`` escalate the run status.
    effective_baselines_dir = (
        baselines_dir if baselines_dir is not None else DEFAULT_BASELINES_DIR
    )
    if enable_baselines:
        baselines = load_baselines(effective_baselines_dir)
        contract_overrides = {
            baseline_key_for_item(item): item.row_count_policy.baseline_drift_action
            for item in selected_items
        }
        baseline_findings, baseline_summary = compare_run_to_baselines(
            quality_audit=quality_audit,
            baselines=baselines,
            contract_overrides=contract_overrides,
        )
    else:
        baseline_findings = []
        baseline_summary = {
            "schema_version": (
                "monthly_platform.source_quality_baseline_comparison.v1"
            ),
            "generated_at": utc_now_iso(),
            "baseline_dir_loaded_count": 0,
            "matched_source_count": 0,
            "missing_baseline_source_count": 0,
            "drift_finding_count": 0,
            "info_finding_count": 0,
            "medium_finding_count": 0,
            "high_finding_count": 0,
            "comparisons": [],
            "disabled": True,
        }
    baseline_summary["baselines_dir"] = str(effective_baselines_dir)
    baseline_summary["enabled"] = enable_baselines
    quality_audit["baseline_comparison"] = baseline_summary
    quality_audit["summary"]["baseline_drift_finding_count"] = len(baseline_findings)
    quality_audit["summary"]["baseline_high_finding_count"] = sum(
        1 for f in baseline_findings if f.severity == "high"
    )
    quality_audit["summary"]["baseline_medium_finding_count"] = sum(
        1 for f in baseline_findings if f.severity == "medium"
    )
    quality_audit["summary"]["baseline_info_finding_count"] = sum(
        1 for f in baseline_findings if f.severity == "info"
    )
    quality_audit["summary"]["baseline_matched_source_count"] = baseline_summary[
        "matched_source_count"
    ]
    quality_audit["summary"]["baseline_missing_source_count"] = baseline_summary[
        "missing_baseline_source_count"
    ]
    high_baseline_findings = [f for f in baseline_findings if f.severity == "high"]
    if high_baseline_findings:
        findings.extend(high_baseline_findings)
        if status not in ("blocked", "failed"):
            status = "blocked"

    quality_artifact = storage.register_json_artifact(
        artifact_id="source_extract_quality_audit",
        artifact_type="source_extract_quality_audit",
        payload=quality_audit,
        relative_path="audits/source_extract_quality_audit.json",
        stage_name=STAGE_NAME,
        metadata=quality_audit["summary"],
    )
    output_artifacts.append(quality_artifact)

    finished_at = utc_now_iso()
    quality_summary = quality_audit["summary"]
    stage = StageResult(
        stage_name=STAGE_NAME,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        duration_seconds=round(time.monotonic() - started, 3),
        outputs=output_artifacts,
        source_extracts=source_extracts,
        findings=findings,
        metadata={
            "dry_run": dry_run,
            "plan_summary": requirement_summary(plan),
            "selected_source_count": len(selected_items),
            "executed_source_count": executed,
            "skipped_source_count": skipped,
            "failed_source_count": failed,
            "quality_audit_path": quality_artifact.path,
            "quality_source_count": quality_summary["source_count"],
            "quality_ok_source_count": quality_summary["ok_source_count"],
            "quality_warning_source_count": quality_summary["warning_source_count"],
            "quality_blocked_source_count": quality_summary["blocked_source_count"],
            "quality_finding_count": quality_summary["finding_count"],
            "quality_high_finding_count": quality_summary["high_finding_count"],
            "quality_medium_finding_count": quality_summary["medium_finding_count"],
            "baseline_drift_finding_count": quality_summary[
                "baseline_drift_finding_count"
            ],
            "baseline_high_finding_count": quality_summary[
                "baseline_high_finding_count"
            ],
            "baseline_matched_source_count": quality_summary[
                "baseline_matched_source_count"
            ],
            "baseline_missing_source_count": quality_summary[
                "baseline_missing_source_count"
            ],
            "filters": {
                "only_requirement": only_requirement,
                "only_territory": only_territory,
                "max_sources": max_sources,
            },
        },
    )
    manifest = storage.record_stage_result(stage)

    return {
        "status": status,
        "snapshot_date": snapshot_date,
        "dry_run": dry_run,
        "run_id": storage.run_id,
        "manifest_path": str(storage.manifest_path),
        "ledger_path": str(storage.ledger_path),
        "source_plan_path": plan_artifact.path,
        "quality_audit_path": quality_artifact.path,
        "plan_summary": requirement_summary(plan),
        "selected_source_count": len(selected_items),
        "executed_source_count": executed,
        "skipped_source_count": skipped,
        "failed_source_count": failed,
        "source_extract_count": len(source_extracts),
        "artifact_count": len(manifest.artifacts),
        "finding_count": len(findings),
        "quality_source_count": quality_summary["source_count"],
        "quality_ok_source_count": quality_summary["ok_source_count"],
        "quality_warning_source_count": quality_summary["warning_source_count"],
        "quality_blocked_source_count": quality_summary["blocked_source_count"],
        "quality_finding_count": quality_summary["finding_count"],
        "quality_high_finding_count": quality_summary["high_finding_count"],
        "quality_medium_finding_count": quality_summary["medium_finding_count"],
        "baseline_drift_finding_count": quality_summary["baseline_drift_finding_count"],
        "baseline_high_finding_count": quality_summary["baseline_high_finding_count"],
        "baseline_matched_source_count": quality_summary[
            "baseline_matched_source_count"
        ],
        "baseline_missing_source_count": quality_summary[
            "baseline_missing_source_count"
        ],
    }


def _execute_item(
    client: SalesforceSourceClient,
    item: SourcePlanItem,
):
    if item.source_type == "salesforce_report":
        return client.run_report(
            report_id=item.source_id,
            source_label=item.source_label or item.source_id,
        )
    if item.source_type == "salesforce_list_view":
        return client.run_list_view(
            list_view_id=item.source_id,
            source_label=item.source_label or item.source_id,
        )
    raise ValueError(f"Unsupported source type: {item.source_type}")


def build_quality_audit(
    *,
    snapshot_date: str,
    run_id: str,
    selected_source_count: int,
    dry_run: bool,
    sources: list[dict[str, Any]],
    findings: list[Finding],
) -> dict[str, Any]:
    high_findings = [finding for finding in findings if finding.severity == "high"]
    medium_findings = [finding for finding in findings if finding.severity == "medium"]
    warning_sources = [source for source in sources if source["status"] == "warning"]
    blocked_sources = [source for source in sources if source["status"] == "blocked"]
    return {
        "schema_version": "monthly_platform.source_extract_quality_audit.v1",
        "generated_at": utc_now_iso(),
        "status": "blocked" if high_findings else ("planned" if dry_run else "ok"),
        "snapshot_date": snapshot_date,
        "run_id": run_id,
        "dry_run": dry_run,
        "summary": {
            "selected_source_count": selected_source_count,
            "source_count": len(sources),
            "ok_source_count": sum(1 for source in sources if source["status"] == "ok"),
            "warning_source_count": len(warning_sources),
            "blocked_source_count": len(blocked_sources),
            "finding_count": len(findings),
            "high_finding_count": len(high_findings),
            "medium_finding_count": len(medium_findings),
        },
        "sources": sources,
        "findings": [finding.model_dump(mode="json") for finding in findings],
    }


def audit_source_extract_quality(
    *,
    item: SourcePlanItem,
    rows: list[dict[str, Any]],
    source_metadata: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[Finding]]:
    findings: list[Finding] = []
    row_count = len(rows)
    policy = item.row_count_policy
    row_count_status = "ok"

    def _bump_status(severity: str | None) -> None:
        nonlocal row_count_status
        if severity == "high":
            row_count_status = "blocked"
        elif severity == "medium" and row_count_status != "blocked":
            row_count_status = "warning"

    expected_empty_note = ""
    if policy.expected_empty_conditions:
        expected_empty_note = (
            f"; expected_empty_conditions={','.join(policy.expected_empty_conditions)}"
        )

    # Zero-row checks: gated on zero_row_action (its proper axis).
    if row_count == 0 and (
        not policy.allow_zero or policy.zero_row_action == "blocked"
    ):
        _bump_status("high")
        findings.append(
            _quality_finding(
                item=item,
                severity="high",
                issue="source_row_count_zero_blocked",
                evidence=(
                    f"row_count=0; allow_zero={policy.allow_zero}; "
                    f"zero_row_action={policy.zero_row_action}{expected_empty_note}"
                ),
            )
        )
    elif row_count == 0 and policy.zero_row_action in {"warning", "fallback"}:
        _bump_status("medium")
        findings.append(
            _quality_finding(
                item=item,
                severity="medium",
                issue=f"source_row_count_zero_{policy.zero_row_action}",
                evidence=(
                    f"row_count=0; zero_row_action={policy.zero_row_action}"
                    f"{expected_empty_note}"
                ),
            )
        )

    # Min-rows breach: Track B — uses min_rows_action, no longer derives
    # severity from zero_row_action. Only fires for non-zero row counts;
    # the zero-row branch above handles row_count == 0 to avoid emitting
    # two findings for the same root cause.
    if 0 < row_count < policy.min_rows:
        severity = action_to_severity(policy.min_rows_action)
        if severity is not None:
            _bump_status(severity)
            findings.append(
                _quality_finding(
                    item=item,
                    severity=severity,
                    issue="source_row_count_below_min",
                    evidence=(
                        f"row_count={row_count}; min_rows={policy.min_rows}; "
                        f"min_rows_action={policy.min_rows_action}"
                    ),
                )
            )

    # Max-rows breach (configured upper bound): Track B — uses max_rows_action.
    if policy.max_rows is not None and row_count > policy.max_rows:
        severity = action_to_severity(policy.max_rows_action)
        if severity is not None:
            _bump_status(severity)
            findings.append(
                _quality_finding(
                    item=item,
                    severity=severity,
                    issue="source_row_count_above_max",
                    evidence=(
                        f"row_count={row_count}; max_rows={policy.max_rows}; "
                        f"max_rows_action={policy.max_rows_action}"
                    ),
                )
            )

    # Max-records cap (Salesforce list-view API truncation indicator):
    # Track B — uses max_records_action; was hardcoded medium.
    max_records = int((source_metadata or {}).get("max_records") or 0)
    if max_records and row_count >= max_records:
        severity = action_to_severity(policy.max_records_action)
        if severity is not None:
            _bump_status(severity)
            findings.append(
                _quality_finding(
                    item=item,
                    severity=severity,
                    issue="source_extract_max_records_reached",
                    evidence=(
                        f"row_count={row_count}; max_records={max_records}; "
                        f"max_records_action={policy.max_records_action}"
                    ),
                )
            )

    field_audits = [
        _audit_required_field(item, rows, field.name)
        for field in item.required_fields
        if field.required
    ]
    missing_fields = [
        audit["field_name"]
        for audit in field_audits
        if row_count > 0 and not audit["present"]
    ]
    for field_name in missing_fields:
        findings.append(
            _quality_finding(
                item=item,
                severity="high",
                issue="source_required_field_missing",
                evidence=f"field={field_name}; row_count={row_count}",
            )
        )
    null_threshold = policy.max_required_field_null_pct
    if null_threshold is not None and row_count > 0:
        for audit in field_audits:
            if not audit["present"]:
                continue
            null_pct = float(audit["null_pct"])
            if null_pct > null_threshold:
                # Track B: route through action_to_severity for typing + consistency.
                severity = action_to_severity(policy.required_field_null_action)
                if severity is None:
                    continue
                findings.append(
                    _quality_finding(
                        item=item,
                        severity=severity,
                        issue="source_required_field_null_threshold_exceeded",
                        evidence=(
                            f"field={audit['field_name']}; null_pct={null_pct:.3f}; "
                            f"threshold={null_threshold}; "
                            f"required_field_null_action={policy.required_field_null_action}"
                        ),
                    )
                )

    high_findings = [finding for finding in findings if finding.severity == "high"]
    medium_findings = [finding for finding in findings if finding.severity == "medium"]
    status = "blocked" if high_findings else ("warning" if medium_findings else "ok")
    quality_payload = {
        "source_key": _source_key(item),
        "status": status,
        "requirement_id": item.requirement_id,
        "dataset": item.dataset,
        "source_type": item.source_type,
        "salesforce_id": item.source_id,
        "label": item.source_label,
        "territory": item.territory,
        "director": item.director,
        "period_role": item.period_role,
        "quarter_label": item.quarter_label,
        "row_count": row_count,
        "row_count_status": row_count_status,
        "row_count_policy": policy.model_dump(mode="json"),
        "required_field_count": len(field_audits),
        "required_fields_present": [
            audit["field_name"] for audit in field_audits if audit["present"]
        ],
        "missing_required_fields": missing_fields,
        "field_audits": field_audits,
        "finding_count": len(findings),
        "high_finding_count": len(high_findings),
        "medium_finding_count": len(medium_findings),
    }
    quality_payload["quality_hash"] = sha256_bytes(stable_json_bytes(quality_payload))
    return quality_payload, findings


def _audit_required_field(
    item: SourcePlanItem,
    rows: list[dict[str, Any]],
    field_name: str,
) -> dict[str, Any]:
    values = [_row_field_value(row, field_name) for row in rows]
    present = bool(rows) and any(value is not _MISSING for value in values)
    null_count = sum(1 for value in values if value in (_MISSING, None, ""))
    row_count = len(rows)
    return {
        "field_name": field_name,
        "present": present,
        "null_count": null_count,
        "null_pct": round(null_count / row_count, 6) if row_count else None,
        "semantic_name": next(
            (
                field.semantic_name
                for field in item.required_fields
                if field.name == field_name
            ),
            None,
        ),
    }


_MISSING = object()


def _row_field_value(row: dict[str, Any], field_name: str) -> Any:
    if field_name in row:
        return row[field_name]
    aliases = _field_aliases(field_name)
    for key, value in row.items():
        normalized_key = _normalize_field_token(str(key).removesuffix("__display"))
        key_tokens = _field_key_tokens(normalized_key)
        if aliases.intersection(key_tokens) or any(
            alias and alias in normalized_key for alias in aliases
        ):
            return value
    return _MISSING


def _field_key_tokens(normalized_key: str) -> set[str]:
    return {token for token in re.split(r"[^a-z0-9]+", normalized_key) if token} | {
        normalized_key
    }


def _field_aliases(field_name: str) -> set[str]:
    normalized = _normalize_field_token(field_name)
    aliases = {normalized, *_field_key_tokens(normalized)}
    if normalized in {"opportunityname", "opportunity.name"}:
        aliases.update({"opportunity.name", "opportunityname"})
    if normalized in {"accountname", "account.name"}:
        aliases.update({"account.name", "accountname"})
    if normalized in {"stage", "stagename"}:
        aliases.update({"stage", "stagename"})
    if normalized in {"arr"}:
        aliases.update(
            {
                "arr",
                "forecastarr",
                "opportunityarr",
                "aptsforecastarr",
                "aptsopportunityarr",
            }
        )
    if normalized in {"salesregion", "sales.region"}:
        aliases.update({"salesregion", "region", "account.region"})
    return aliases


def _normalize_field_token(value: str) -> str:
    normalized = (
        _unwrap_salesforce_function(value)
        .replace("__hd", "")
        .replace("__hst", "")
        .replace("_hst", "")
        .replace("__c", "")
        .replace("__r", "")
        .replace("_", "")
        .replace(" ", "")
        .lower()
    )
    return re.sub(r"[^a-z0-9.]+", "", normalized)


def _unwrap_salesforce_function(value: str) -> str:
    stripped = value.strip()
    if not stripped.endswith(")") or "(" not in stripped:
        return stripped
    prefix = stripped[: stripped.find("(")].strip()
    function_name = prefix.replace("_", "").lower()
    if function_name not in {
        "calendarmonth",
        "calendarquarter",
        "calendaryear",
        "convert",
        "convertcurrency",
        "fiscalquarter",
        "fiscalyear",
        "format",
        "tolabel",
    }:
        return stripped
    return stripped[stripped.find("(") + 1 : -1].strip()


def _quality_finding(
    *,
    item: SourcePlanItem,
    severity: FindingSeverity,
    issue: str,
    evidence: str,
) -> Finding:
    return Finding(
        severity=severity,
        issue=issue,
        evidence=(
            f"{item.requirement_id} {item.territory or 'global'} "
            f"{item.period_role} {item.source_type} {item.source_id}: {evidence}"
        ),
    )


def _source_key(item: SourcePlanItem) -> str:
    territory = (item.territory or "global").lower().replace(" ", "_")
    return (
        f"{item.requirement_id}.{territory}."
        f"{item.period_role}.{item.quarter_label}.{item.source_id}"
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument(
        "--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--run-id")
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--require-live",
        action="store_true",
        help="Fail if --dry-run is also supplied; useful for live automation guards.",
    )
    parser.add_argument("--only-requirement")
    parser.add_argument("--only-territory")
    parser.add_argument("--max-sources", type=int)
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--baselines-dir",
        type=Path,
        default=DEFAULT_BASELINES_DIR,
        help=(
            "Source-quality baselines directory (Track C). Read-only — never "
            f"written by the extract step. Default: {DEFAULT_BASELINES_DIR}"
        ),
    )
    parser.add_argument(
        "--no-baselines",
        action="store_true",
        help="Disable Track C baseline comparison entirely.",
    )
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    if args.require_live and args.dry_run:
        parser.error("--require-live cannot be combined with --dry-run")

    result = extract_sources(
        snapshot_date=args.snapshot_date,
        requirements_path=args.requirements,
        territory_config_path=args.territory_config,
        output_root=args.output_root,
        run_id=args.run_id,
        dry_run=args.dry_run,
        target_org=args.target_org,
        only_requirement=args.only_requirement,
        only_territory=args.only_territory,
        max_sources=args.max_sources,
        fail_fast=args.fail_fast,
        baselines_dir=args.baselines_dir,
        enable_baselines=not args.no_baselines,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    else:
        print(result)
    if result["status"] == "blocked":
        return 2
    if result["status"] == "failed":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
