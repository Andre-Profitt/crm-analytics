"""Coverage contract for source-backed DirectorBundle datasets."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from scripts.monthly_platform.contracts import ContractModel, Finding
from scripts.monthly_platform.models import DirectorBundle

DatasetPolicy = Literal["source_backed", "optional_empty"]


class DirectorBundleDatasetContract(ContractModel):
    dataset: str
    policy: DatasetPolicy
    required_for_publish: bool = False
    source_contract_keys: list[str] = Field(default_factory=list)
    source_requirement_ids: list[str] = Field(default_factory=list)
    rationale: str = ""
    # Track E/M2: explicit flag that the director-monthly deck reads this
    # dataset (via config/director_workbook_contract.yaml). When True, the
    # bundle policy is expected to converge on source_backed; while it
    # remains optional_empty the rationale should call that out.
    deck_consumed: bool = False

    @model_validator(mode="after")
    def validate_source_backed_requirements(self) -> "DirectorBundleDatasetContract":
        if self.policy == "source_backed" and not self.source_contract_keys:
            raise ValueError("source_backed dataset requires source_contract_keys")
        if self.policy == "optional_empty" and self.required_for_publish:
            raise ValueError("optional_empty dataset cannot be required_for_publish")
        if (
            self.deck_consumed
            and self.policy == "optional_empty"
            and "M2" not in self.rationale
            and "deferred" not in self.rationale.lower()
        ):
            raise ValueError(
                f"deck_consumed dataset {self.dataset!r} on policy=optional_empty "
                f"must reference 'M2' or 'deferred' in rationale to acknowledge "
                f"the bundle/deck-contract mismatch"
            )
        return self


class DirectorBundleContract(ContractModel):
    schema_version: str
    description: str = ""
    datasets: list[DirectorBundleDatasetContract]

    @field_validator("datasets")
    @classmethod
    def dataset_ids_unique(
        cls,
        value: list[DirectorBundleDatasetContract],
    ) -> list[DirectorBundleDatasetContract]:
        seen: set[str] = set()
        for dataset in value:
            if dataset.dataset in seen:
                raise ValueError(f"duplicate dataset contract: {dataset.dataset}")
            seen.add(dataset.dataset)
        return value

    def by_dataset(self) -> dict[str, DirectorBundleDatasetContract]:
        return {dataset.dataset: dataset for dataset in self.datasets}


def load_director_bundle_contract(path: Path) -> DirectorBundleContract:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return DirectorBundleContract.model_validate(payload)


def validate_director_bundle_coverage(
    *,
    bundle: DirectorBundle,
    contract: DirectorBundleContract,
) -> list[Finding]:
    findings: list[Finding] = []
    dataset_counts = bundle.dataset_counts
    contract_by_dataset = contract.by_dataset()
    for dataset in dataset_counts:
        dataset_contract = contract_by_dataset.get(dataset)
        if not dataset_contract:
            findings.append(
                Finding(
                    severity="high",
                    issue="director_bundle_dataset_contract_missing",
                    evidence=f"{bundle.territory}: {dataset}",
                )
            )
            continue
        if dataset_contract.policy == "optional_empty" and dataset_counts[dataset] > 0:
            findings.append(
                Finding(
                    severity="medium",
                    issue="optional_dataset_has_rows",
                    evidence=(
                        f"{bundle.territory}: {dataset} has "
                        f"{dataset_counts[dataset]} rows but is optional_empty"
                    ),
                )
            )
        if dataset_contract.policy == "source_backed":
            missing_keys = [
                key
                for key in dataset_contract.source_contract_keys
                if key not in bundle.source_contract.sources
            ]
            if missing_keys:
                findings.append(
                    Finding(
                        severity="high",
                        issue="source_backed_dataset_missing_source_contract",
                        evidence=(
                            f"{bundle.territory}: {dataset} missing "
                            f"{', '.join(missing_keys)}"
                        ),
                    )
                )
    for dataset in contract_by_dataset:
        if dataset not in dataset_counts:
            findings.append(
                Finding(
                    severity="high",
                    issue="director_bundle_dataset_missing_from_output",
                    evidence=f"{bundle.territory}: {dataset}",
                )
            )
    return findings


def coverage_summary(
    *,
    bundle: DirectorBundle,
    contract: DirectorBundleContract,
) -> dict[str, Any]:
    contract_by_dataset = contract.by_dataset()
    source_backed: list[str] = []
    optional_empty: list[str] = []
    publish_required: list[str] = []
    for dataset in bundle.dataset_counts:
        dataset_contract = contract_by_dataset.get(dataset)
        if not dataset_contract:
            continue
        if dataset_contract.policy == "source_backed":
            source_backed.append(dataset)
        else:
            optional_empty.append(dataset)
        if dataset_contract.required_for_publish:
            publish_required.append(dataset)
    return {
        "schema_version": contract.schema_version,
        "source_backed": sorted(source_backed),
        "optional_empty": sorted(optional_empty),
        "publish_required": sorted(publish_required),
        "dataset_counts": dict(bundle.dataset_counts),
        "source_requirement_ids": sorted(
            {
                requirement_id
                for dataset in contract.datasets
                for requirement_id in dataset.source_requirement_ids
            }
        ),
    }


def expected_dataset_names(bundle: DirectorBundle) -> list[str]:
    return sorted(asdict(bundle.datasets).keys())
