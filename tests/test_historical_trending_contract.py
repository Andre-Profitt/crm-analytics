from scripts.monthly_platform.historical_trending import (
    resolve_historical_trending_contract,
)


def test_historical_trending_contract_names_prior_and_current_quarter_tabs() -> None:
    contract = resolve_historical_trending_contract(
        retrospective_label="Q3",
        retrospective_title="Q3 2026",
        current_label="Q4",
        current_title="Q4 2026",
    )

    assert contract.retrospective_snapshot_sheet == "Q3 Snapshot Trend"
    assert contract.retrospective_consolidated_sheet == "Q3 Trend Consolidated"
    assert contract.current_snapshot_sheet == "Q4 Snapshot Trend"
    assert contract.current_consolidated_sheet == "Q4 Trend Consolidated"
