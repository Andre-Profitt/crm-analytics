from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class HistoricalTrendingContract:
    retrospective_label: str
    retrospective_title: str
    retrospective_snapshot_sheet: str
    retrospective_consolidated_sheet: str
    current_label: str
    current_title: str
    current_snapshot_sheet: str
    current_consolidated_sheet: str


def resolve_historical_trending_contract(
    *,
    retrospective_label: str,
    retrospective_title: str,
    current_label: str,
    current_title: str,
) -> HistoricalTrendingContract:
    retrospective_label = str(retrospective_label)
    retrospective_title = str(retrospective_title)
    current_label = str(current_label)
    current_title = str(current_title)
    return HistoricalTrendingContract(
        retrospective_label=retrospective_label,
        retrospective_title=retrospective_title,
        retrospective_snapshot_sheet=f"{retrospective_label} Snapshot Trend",
        retrospective_consolidated_sheet=f"{retrospective_label} Trend Consolidated",
        current_label=current_label,
        current_title=current_title,
        current_snapshot_sheet=f"{current_label} Snapshot Trend",
        current_consolidated_sheet=f"{current_label} Trend Consolidated",
    )
