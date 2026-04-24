#!/usr/bin/env python3
"""Validate a monthly source-backed DirectorBundle run is publish-complete."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.contracts import (  # noqa: E402
    Finding,
    MonthlyRunManifest,
)
from scripts.monthly_platform.dataset_readiness import (  # noqa: E402
    DatasetReadinessReport,
)
from scripts.monthly_platform.director_bundle_builder import (  # noqa: E402
    DirectorBundleBuildManifest,
)
from scripts.monthly_platform.director_bundle_contract import (  # noqa: E402
    DirectorBundleContract,
    coverage_summary,
    load_director_bundle_contract,
    validate_director_bundle_coverage,
)
from scripts.monthly_platform.models import DirectorBundle  # noqa: E402
from scripts.monthly_platform.source_bundles import SourceBundleManifest  # noqa: E402
from scripts.monthly_platform.source_requirements import (  # noqa: E402
    SourceRequirementPlan,
    filter_plan_items,
)


SCHEMA_VERSION = "monthly_platform.source_backed_publish_gate.v1"
DEFAULT_CONTRACT_PATH = ROOT / "config" / "monthly_director_bundle_contract.json"


def validate_run(
    *,
    source_run_dir: Path,
    source_bundle_dir: Path,
    director_bundle_dir: Path,
    readiness_dir: Path | None = None,
    contract_path: Path = DEFAULT_CONTRACT_PATH,
    list_view_audit_path: Path | None = None,
) -> dict[str, Any]:
    findings: list[dict[str, Any]] = []
    counts: dict[str, Any] = {
        "source_extract_count": 0,
        "selected_source_count": 0,
        "missing_selected_source_count": 0,
        "source_bundle_count": 0,
        "director_bundle_count": 0,
        "director_bundle_coverage_count": 0,
        "publish_required_dataset_count": 0,
        "readiness_report_count": 0,
        "readiness_not_ready_count": 0,
        "list_view_audit_view_count": 0,
        "list_view_audit_finding_count": 0,
    }

    contract = _load_contract(contract_path, findings)
    publish_required_datasets = _publish_required_source_backed_datasets(contract)
    counts["publish_required_dataset_count"] = len(publish_required_datasets)

    source_manifest = _load_model(
        source_run_dir / "run_manifest.json",
        MonthlyRunManifest,
        findings,
        source="source_run_manifest",
    )
    if source_manifest:
        counts["source_extract_count"] = len(source_manifest.source_extracts)
        _validate_source_run_manifest(source_manifest, findings)
        selected_items = _selected_source_items(
            source_manifest=source_manifest,
            source_run_dir=source_run_dir,
            findings=findings,
        )
        counts["selected_source_count"] = len(selected_items)
        missing_count = _validate_selected_extracts(
            selected_items=selected_items,
            source_manifest=source_manifest,
            findings=findings,
        )
        counts["missing_selected_source_count"] = missing_count

    source_bundle_manifest = _load_model(
        source_bundle_dir / "source_bundle_manifest.json",
        SourceBundleManifest,
        findings,
        source="source_bundle_manifest",
    )
    if source_bundle_manifest:
        counts["source_bundle_count"] = len(source_bundle_manifest.bundle_paths)
        _validate_stage_manifest(
            status=source_bundle_manifest.status,
            findings=source_bundle_manifest.findings,
            source="source_bundle_manifest",
            not_ok_issue="source_bundle_manifest_not_ok",
            output_findings=findings,
        )
        _validate_manifest_paths(
            paths=source_bundle_manifest.bundle_paths,
            base_dir=source_bundle_dir,
            source="source_bundle_manifest",
            issue="source_bundle_path_missing",
            findings=findings,
        )

    director_manifest = _load_model(
        director_bundle_dir / "director_bundle_manifest.json",
        DirectorBundleBuildManifest,
        findings,
        source="director_bundle_manifest",
    )
    if director_manifest:
        counts["director_bundle_count"] = len(director_manifest.bundle_paths)
        _validate_stage_manifest(
            status=director_manifest.status,
            findings=director_manifest.findings,
            source="director_bundle_manifest",
            not_ok_issue="director_bundle_manifest_not_ok",
            output_findings=findings,
        )
        coverage_count = _validate_director_bundles(
            director_manifest=director_manifest,
            director_bundle_dir=director_bundle_dir,
            contract=contract,
            publish_required_datasets=publish_required_datasets,
            findings=findings,
        )
        counts["director_bundle_coverage_count"] = coverage_count

    if readiness_dir is not None:
        _validate_readiness_reports(
            readiness_dir=readiness_dir,
            publish_required_datasets=publish_required_datasets,
            counts=counts,
            findings=findings,
        )

    if list_view_audit_path is not None:
        _validate_list_view_audit(
            list_view_audit_path=list_view_audit_path,
            counts=counts,
            findings=findings,
        )

    counts["finding_count"] = len(findings)
    counts["high_finding_count"] = sum(
        1 for finding in findings if finding.get("severity") == "high"
    )
    counts["medium_finding_count"] = sum(
        1 for finding in findings if finding.get("severity") == "medium"
    )
    counts["low_finding_count"] = sum(
        1 for finding in findings if finding.get("severity") == "low"
    )
    counts["info_finding_count"] = sum(
        1 for finding in findings if finding.get("severity") == "info"
    )

    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if findings else "ok",
        "source_run_dir": str(source_run_dir),
        "source_bundle_dir": str(source_bundle_dir),
        "director_bundle_dir": str(director_bundle_dir),
        "readiness_dir": str(readiness_dir) if readiness_dir else None,
        "list_view_audit_path": str(list_view_audit_path) if list_view_audit_path else None,
        "contract_path": str(contract_path),
        "publish_required_datasets": publish_required_datasets,
        "counts": counts,
        "findings": findings,
    }


def _load_contract(
    path: Path,
    findings: list[dict[str, Any]],
) -> DirectorBundleContract | None:
    try:
        return load_director_bundle_contract(path)
    except Exception as exc:
        findings.append(
            _finding(
                severity="high",
                issue="director_bundle_contract_load_failed",
                evidence=f"{path}: {exc}",
                source="director_bundle_contract",
            )
        )
        return None


def _publish_required_source_backed_datasets(
    contract: DirectorBundleContract | None,
) -> list[str]:
    if not contract:
        return []
    return sorted(
        dataset.dataset
        for dataset in contract.datasets
        if dataset.policy == "source_backed" and dataset.required_for_publish
    )


def _validate_source_run_manifest(
    manifest: MonthlyRunManifest,
    findings: list[dict[str, Any]],
) -> None:
    if manifest.status != "ok":
        findings.append(
            _finding(
                severity="high",
                issue="source_run_manifest_not_ok",
                evidence=manifest.status,
                source="source_run_manifest",
            )
        )
    for stage in manifest.stages:
        if stage.status != "ok":
            findings.append(
                _finding(
                    severity="high",
                    issue="source_run_stage_not_ok",
                    evidence=f"{stage.stage_name}: {stage.status}",
                    source="source_run_manifest",
                )
            )
        _extend_findings(
            stage.findings,
            source=f"source_run_stage:{stage.stage_name}",
            findings=findings,
        )
    for extract in manifest.source_extracts:
        if extract.status != "ok":
            findings.append(
                _finding(
                    severity="high",
                    issue="source_extract_not_ok",
                    evidence=f"{extract.source_extract_id}: {extract.status}",
                    source="source_run_manifest",
                )
            )


def _selected_source_items(
    *,
    source_manifest: MonthlyRunManifest,
    source_run_dir: Path,
    findings: list[dict[str, Any]],
) -> list[Any]:
    plan = _load_source_requirement_plan(
        source_manifest=source_manifest,
        source_run_dir=source_run_dir,
        findings=findings,
    )
    if not plan:
        return []
    filters = source_manifest.stages[-1].metadata.get("filters") if source_manifest.stages else {}
    filters = filters or {}
    max_sources = filters.get("max_sources")
    if isinstance(max_sources, str) and max_sources.isdigit():
        max_sources = int(max_sources)
    return filter_plan_items(
        plan,
        only_requirement=filters.get("only_requirement"),
        only_territory=filters.get("only_territory"),
        max_sources=max_sources,
    )


def _load_source_requirement_plan(
    *,
    source_manifest: MonthlyRunManifest,
    source_run_dir: Path,
    findings: list[dict[str, Any]],
) -> SourceRequirementPlan | None:
    plan_artifact = next(
        (
            artifact
            for artifact in source_manifest.artifacts
            if artifact.artifact_type == "source_requirement_plan"
        ),
        None,
    )
    if not plan_artifact:
        findings.append(
            _finding(
                severity="high",
                issue="source_requirement_plan_missing",
                evidence="run_manifest artifacts has no source_requirement_plan",
                source="source_run_manifest",
            )
        )
        return None
    plan_path = _resolve_path(plan_artifact.path, source_run_dir)
    return _load_model(
        plan_path,
        SourceRequirementPlan,
        findings,
        source="source_requirement_plan",
    )


def _validate_selected_extracts(
    *,
    selected_items: list[Any],
    source_manifest: MonthlyRunManifest,
    findings: list[dict[str, Any]],
) -> int:
    extracted_keys = {
        (
            str(extract.metadata.get("requirement_id") or ""),
            extract.territory,
            extract.period_role,
            extract.source_type,
            extract.source_id,
        )
        for extract in source_manifest.source_extracts
    }
    missing_count = 0
    for item in selected_items:
        key = (
            item.requirement_id,
            item.territory,
            item.period_role,
            item.source_type,
            item.source_id,
        )
        if key in extracted_keys:
            continue
        missing_count += 1
        findings.append(
            _finding(
                severity="high",
                issue="selected_source_extract_missing",
                evidence=(
                    f"{item.requirement_id} {item.territory or 'global'} "
                    f"{item.period_role} {item.source_type} {item.source_id}"
                ),
                source="source_run_manifest",
            )
        )
    return missing_count


def _validate_stage_manifest(
    *,
    status: str,
    findings: list[Finding],
    source: str,
    not_ok_issue: str,
    output_findings: list[dict[str, Any]],
) -> None:
    if status != "ok":
        output_findings.append(
            _finding(
                severity="high",
                issue=not_ok_issue,
                evidence=status,
                source=source,
            )
        )
    _extend_findings(findings, source=source, findings=output_findings)


def _validate_manifest_paths(
    *,
    paths: list[str],
    base_dir: Path,
    source: str,
    issue: str,
    findings: list[dict[str, Any]],
) -> None:
    for path_text in paths:
        path = _resolve_path(path_text, base_dir)
        if not path.exists():
            findings.append(
                _finding(
                    severity="high",
                    issue=issue,
                    evidence=str(path),
                    source=source,
                )
            )


def _validate_director_bundles(
    *,
    director_manifest: DirectorBundleBuildManifest,
    director_bundle_dir: Path,
    contract: DirectorBundleContract | None,
    publish_required_datasets: list[str],
    findings: list[dict[str, Any]],
) -> int:
    if not director_manifest.bundle_paths:
        findings.append(
            _finding(
                severity="high",
                issue="director_bundle_manifest_empty",
                evidence=str(director_bundle_dir),
                source="director_bundle_manifest",
            )
        )
        return 0
    if not contract:
        return 0

    coverage_count = 0
    required = set(publish_required_datasets)
    for bundle_path_text in director_manifest.bundle_paths:
        bundle_path = _resolve_path(bundle_path_text, director_bundle_dir)
        if not bundle_path.exists():
            findings.append(
                _finding(
                    severity="high",
                    issue="director_bundle_path_missing",
                    evidence=str(bundle_path),
                    source="director_bundle_manifest",
                )
            )
            continue
        try:
            bundle = DirectorBundle.from_json(bundle_path.read_text(encoding="utf-8"))
        except Exception as exc:
            findings.append(
                _finding(
                    severity="high",
                    issue="director_bundle_load_failed",
                    evidence=f"{bundle_path}: {exc}",
                    source="director_bundle_manifest",
                )
            )
            continue

        coverage_count += 1
        bundle_source = f"director_bundle:{bundle.territory}"
        coverage_findings = validate_director_bundle_coverage(
            bundle=bundle,
            contract=contract,
        )
        _extend_findings(
            coverage_findings,
            source=bundle_source,
            findings=findings,
        )
        summary = coverage_summary(bundle=bundle, contract=contract)
        missing_required = sorted(required - set(summary["publish_required"]))
        if missing_required:
            findings.append(
                _finding(
                    severity="high",
                    issue="publish_required_dataset_missing_from_director_coverage",
                    evidence=(
                        f"{bundle.territory}: {', '.join(missing_required)}"
                    ),
                    source=bundle_source,
                )
            )
    return coverage_count


def _validate_readiness_reports(
    *,
    readiness_dir: Path,
    publish_required_datasets: list[str],
    counts: dict[str, Any],
    findings: list[dict[str, Any]],
) -> None:
    if not readiness_dir.exists() or not readiness_dir.is_dir():
        findings.append(
            _finding(
                severity="high",
                issue="readiness_dir_missing",
                evidence=str(readiness_dir),
                source="dataset_readiness",
            )
        )
        return

    required = set(publish_required_datasets)
    reports = sorted(readiness_dir.glob("*.json"))
    counts["readiness_report_count"] = len(reports)
    publish_required_report_count = 0
    for report_path in reports:
        report = _load_model(
            report_path,
            DatasetReadinessReport,
            findings,
            source="dataset_readiness",
        )
        if not report:
            continue
        _extend_findings(
            report.findings,
            source=f"dataset_readiness:{report.dataset}",
            findings=findings,
        )
        if report.dataset not in required:
            continue
        publish_required_report_count += 1
        if report.status != "ready":
            counts["readiness_not_ready_count"] += 1
            findings.append(
                _finding(
                    severity="high",
                    issue="readiness_report_not_ready",
                    evidence=f"{report.dataset}: {report.status}",
                    source=f"dataset_readiness:{report.dataset}",
                )
            )
    counts["publish_required_readiness_report_count"] = publish_required_report_count


def _validate_list_view_audit(
    *,
    list_view_audit_path: Path,
    counts: dict[str, Any],
    findings: list[dict[str, Any]],
) -> None:
    if not list_view_audit_path.exists():
        findings.append(
            _finding(
                severity="high",
                issue="list_view_filter_audit_missing",
                evidence=str(list_view_audit_path),
                source="list_view_filter_audit",
            )
        )
        return
    try:
        audit = json.loads(list_view_audit_path.read_text(encoding="utf-8"))
    except Exception as exc:
        findings.append(
            _finding(
                severity="high",
                issue="list_view_filter_audit_load_failed",
                evidence=f"{list_view_audit_path}: {exc}",
                source="list_view_filter_audit",
            )
        )
        return
    counts["list_view_audit_view_count"] = int(audit.get("view_count") or 0)
    counts["list_view_audit_finding_count"] = int(audit.get("finding_count") or 0)
    if audit.get("status") != "ok":
        findings.append(
            _finding(
                severity="high",
                issue="list_view_filter_audit_not_ok",
                evidence=str(audit.get("status")),
                source="list_view_filter_audit",
            )
        )
    for finding in audit.get("findings") or []:
        if not isinstance(finding, dict):
            continue
        payload = dict(finding)
        payload.setdefault("severity", "high")
        payload.setdefault("issue", "list_view_filter_audit_finding")
        payload.setdefault("evidence", json.dumps(finding, sort_keys=True))
        payload["source"] = "list_view_filter_audit"
        findings.append(payload)


def _load_model(
    path: Path,
    model: Any,
    findings: list[dict[str, Any]],
    *,
    source: str,
) -> Any | None:
    if not path.exists():
        findings.append(
            _finding(
                severity="high",
                issue=f"{source}_missing",
                evidence=str(path),
                source=source,
            )
        )
        return None
    try:
        return model.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as exc:
        findings.append(
            _finding(
                severity="high",
                issue=f"{source}_load_failed",
                evidence=f"{path}: {exc}",
                source=source,
            )
        )
        return None


def _extend_findings(
    source_findings: list[Finding],
    *,
    source: str,
    findings: list[dict[str, Any]],
) -> None:
    for finding in source_findings:
        payload = finding.model_dump(mode="json")
        payload["source"] = source
        findings.append(payload)


def _finding(
    *,
    severity: str,
    issue: str,
    evidence: str,
    source: str,
) -> dict[str, Any]:
    return {
        "severity": severity,
        "issue": issue,
        "evidence": evidence,
        "source": source,
    }


def _resolve_path(path_text: str, base_dir: Path) -> Path:
    raw = Path(path_text)
    if raw.is_absolute():
        return raw
    candidates = [raw, base_dir / raw, ROOT / raw]
    for candidate in candidates:
        if candidate.exists():
            return candidate
    return base_dir / raw


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-run-dir", type=Path, required=True)
    parser.add_argument("--source-bundle-dir", type=Path, required=True)
    parser.add_argument("--director-bundle-dir", type=Path, required=True)
    parser.add_argument("--readiness-dir", type=Path)
    parser.add_argument("--list-view-audit", type=Path)
    parser.add_argument(
        "--contract",
        type=Path,
        default=DEFAULT_CONTRACT_PATH,
        help="Director bundle dataset contract.",
    )
    parser.add_argument("--output-path", type=Path)
    args = parser.parse_args(argv)

    result = validate_run(
        source_run_dir=args.source_run_dir,
        source_bundle_dir=args.source_bundle_dir,
        director_bundle_dir=args.director_bundle_dir,
        readiness_dir=args.readiness_dir,
        contract_path=args.contract,
        list_view_audit_path=args.list_view_audit,
    )
    if args.output_path:
        args.output_path.parent.mkdir(parents=True, exist_ok=True)
        args.output_path.write_text(
            json.dumps(result, indent=2) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(result, indent=2) + "\n")
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
