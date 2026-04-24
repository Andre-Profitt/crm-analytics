#!/usr/bin/env python3
"""Lint monthly source requirements against the DirectorBundle publish contract."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.director_bundle_contract import (  # noqa: E402
    load_director_bundle_contract,
)
from scripts.monthly_platform.source_requirements import load_source_requirements  # noqa: E402

SCHEMA_VERSION = "monthly_platform.monthly_source_contract_lint.v1"
DEFAULT_REQUIREMENTS_PATH = ROOT / "config" / "monthly_source_requirements.json"
DEFAULT_BUNDLE_CONTRACT_PATH = ROOT / "config" / "monthly_director_bundle_contract.json"


def lint_monthly_source_contract(
    *,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    bundle_contract_path: Path = DEFAULT_BUNDLE_CONTRACT_PATH,
    snapshot_date: str | None = None,
) -> dict[str, Any]:
    registry = load_source_requirements(requirements_path)
    contract = load_director_bundle_contract(bundle_contract_path)
    requirements_by_id = {item.requirement_id: item for item in registry.requirements}
    enabled_requirement_ids = {
        item.requirement_id for item in registry.requirements if item.enabled
    }
    findings: list[dict[str, Any]] = []

    for dataset in contract.datasets:
        if dataset.policy != "source_backed":
            if dataset.required_for_publish:
                findings.append(
                    _finding(
                        issue="optional_dataset_required_for_publish",
                        evidence=dataset.dataset,
                    )
                )
            continue
        if not dataset.source_requirement_ids:
            findings.append(
                _finding(
                    issue="source_backed_dataset_missing_requirement_ids",
                    evidence=dataset.dataset,
                )
            )
            continue
        missing_ids = [
            requirement_id
            for requirement_id in dataset.source_requirement_ids
            if requirement_id not in requirements_by_id
        ]
        for requirement_id in missing_ids:
            findings.append(
                _finding(
                    issue="source_requirement_id_missing",
                    evidence=f"{dataset.dataset}: {requirement_id}",
                )
            )
        disabled_ids = [
            requirement_id
            for requirement_id in dataset.source_requirement_ids
            if requirement_id in requirements_by_id
            and requirement_id not in enabled_requirement_ids
        ]
        for requirement_id in disabled_ids:
            findings.append(
                _finding(
                    issue="source_requirement_id_disabled",
                    evidence=f"{dataset.dataset}: {requirement_id}",
                )
            )
        if dataset.required_for_publish and not any(
            requirement_id in enabled_requirement_ids
            for requirement_id in dataset.source_requirement_ids
        ):
            findings.append(
                _finding(
                    issue="publish_required_dataset_has_no_enabled_requirement",
                    evidence=dataset.dataset,
                )
            )

    referenced_requirement_ids = {
        requirement_id
        for dataset in contract.datasets
        for requirement_id in dataset.source_requirement_ids
    }
    unreferenced_source_backed = [
        item.requirement_id
        for item in registry.requirements
        if item.enabled
        and "source_backed" in item.tags
        and "reference" not in item.tags
        and item.requirement_id not in referenced_requirement_ids
    ]
    for requirement_id in unreferenced_source_backed:
        findings.append(
            _finding(
                severity="medium",
                issue="enabled_source_backed_requirement_not_in_bundle_contract",
                evidence=requirement_id,
            )
        )

    high_findings = [
        finding for finding in findings if finding.get("severity") == "high"
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if high_findings else "ok",
        "snapshot_date": snapshot_date,
        "requirements_path": str(requirements_path),
        "bundle_contract_path": str(bundle_contract_path),
        "requirement_count": len(registry.requirements),
        "enabled_requirement_count": len(enabled_requirement_ids),
        "contract_dataset_count": len(contract.datasets),
        "publish_required_source_backed_dataset_count": sum(
            1
            for dataset in contract.datasets
            if dataset.policy == "source_backed" and dataset.required_for_publish
        ),
        "finding_count": len(findings),
        "high_finding_count": len(high_findings),
        "findings": findings,
    }


def _finding(
    *,
    issue: str,
    evidence: str,
    severity: str = "high",
) -> dict[str, str]:
    return {
        "severity": severity,
        "issue": issue,
        "evidence": evidence,
    }


def _write_output(path: Path | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date")
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument(
        "--bundle-contract",
        type=Path,
        default=DEFAULT_BUNDLE_CONTRACT_PATH,
    )
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args(argv)

    result = lint_monthly_source_contract(
        requirements_path=args.requirements,
        bundle_contract_path=args.bundle_contract,
        snapshot_date=args.snapshot_date,
    )
    _write_output(args.output_path, result)
    print(json.dumps(result, indent=2) if args.json_output else result)
    return 0 if result["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
