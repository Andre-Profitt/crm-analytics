from pathlib import Path

from scripts.monthly_platform.director_bundle_builder import (
    build_director_bundle_from_source_bundle,
)
from scripts.monthly_platform.director_bundle_contract import (
    DirectorBundleContract,
    coverage_summary,
    expected_dataset_names,
    load_director_bundle_contract,
    validate_director_bundle_coverage,
)
from test_director_bundle_builder import _source_bundle


def test_default_director_bundle_contract_covers_all_datasets() -> None:
    contract = load_director_bundle_contract(
        Path("config/monthly_director_bundle_contract.json")
    )
    bundle = build_director_bundle_from_source_bundle(_source_bundle())

    assert sorted(contract.by_dataset()) == expected_dataset_names(bundle)


def test_source_backed_and_optional_empty_datasets_are_explicit() -> None:
    contract = load_director_bundle_contract(
        Path("config/monthly_director_bundle_contract.json")
    )
    bundle = build_director_bundle_from_source_bundle(_source_bundle())

    findings = validate_director_bundle_coverage(bundle=bundle, contract=contract)
    summary = coverage_summary(bundle=bundle, contract=contract)

    assert findings == []
    assert summary["source_backed"] == [
        "pi_current",
        "pi_forward",
        "pipeline_open",
        "snapshot_trend",
    ]
    assert "pipeline_open" not in summary["optional_empty"]
    assert summary["publish_required"] == [
        "pi_current",
        "pi_forward",
        "pipeline_open",
        "snapshot_trend",
    ]


def test_missing_source_backed_contract_key_blocks_coverage() -> None:
    contract = DirectorBundleContract.model_validate(
        {
            "schema_version": "test",
            "datasets": [
                {
                    "dataset": "pipeline_open",
                    "policy": "source_backed",
                    "required_for_publish": True,
                    "source_contract_keys": ["pipeline_open"],
                },
                {
                    "dataset": "pi_current",
                    "policy": "source_backed",
                    "required_for_publish": True,
                    "source_contract_keys": ["missing_pi_current"],
                },
                {
                    "dataset": "pi_forward",
                    "policy": "source_backed",
                    "required_for_publish": True,
                    "source_contract_keys": ["pi_forward"],
                },
                {
                    "dataset": "snapshot_trend",
                    "policy": "source_backed",
                    "required_for_publish": True,
                    "source_contract_keys": ["snapshot_trend"],
                },
                *[
                    {
                        "dataset": dataset,
                        "policy": "optional_empty",
                        "required_for_publish": False,
                    }
                    for dataset in [
                        "won_lost",
                        "renewals",
                        "approvals",
                        "activity",
                        "commit_items",
                        "stage_events",
                        "forecast_category_events",
                        "close_date_events",
                        "movement_prior",
                        "movement_current",
                    ]
                ],
            ],
        }
    )
    bundle = build_director_bundle_from_source_bundle(_source_bundle())

    findings = validate_director_bundle_coverage(bundle=bundle, contract=contract)

    assert any(
        finding.issue == "source_backed_dataset_missing_source_contract"
        and finding.severity == "high"
        for finding in findings
    )
