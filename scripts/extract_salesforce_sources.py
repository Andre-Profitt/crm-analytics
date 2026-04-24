#!/usr/bin/env python3
"""Extract monthly Salesforce sources through the modular source registry."""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.contracts import Finding, StageResult, utc_now_iso  # noqa: E402
from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.monthly_platform.salesforce_auth import (  # noqa: E402
    DEFAULT_TARGET_ORG,
    build_salesforce_session,
    get_salesforce_auth,
)
from scripts.monthly_platform.salesforce_reports import SalesforceSourceClient  # noqa: E402
from scripts.monthly_platform.source_requirements import (  # noqa: E402
    SourcePlanItem,
    build_source_requirement_plan,
    filter_plan_items,
    load_source_requirements,
    requirement_summary,
)
from scripts.monthly_platform.storage import MonthlyStorage  # noqa: E402


DEFAULT_REQUIREMENTS_PATH = ROOT / "config" / "monthly_source_requirements.json"
DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_OUTPUT_ROOT = ROOT / "output" / "monthly_salesforce_sources"
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
                        "fallback_policy": item.fallback_policy.model_dump(
                            mode="json"
                        )
                        if item.fallback_policy
                        else None,
                        "duration_ms": result.duration_ms,
                        "status_code": result.status_code,
                        "source_metadata": result.metadata,
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
        status = "failed" if failed else "ok"

    finished_at = utc_now_iso()
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
        "plan_summary": requirement_summary(plan),
        "selected_source_count": len(selected_items),
        "executed_source_count": executed,
        "skipped_source_count": skipped,
        "failed_source_count": failed,
        "source_extract_count": len(source_extracts),
        "artifact_count": len(manifest.artifacts),
        "finding_count": len(findings),
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


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
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
