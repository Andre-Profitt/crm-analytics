"""Territory mapping helper.

Loads `config/territory_mappings.json` and exposes lookup functions for:
  - director book rollup (the 9 MD-1 director workbook slices)
  - forecast rollup (CRO/APAC/EMEA/North America, aligned to live SF forecast tree)

Context: `docs/2026-04-10-forecast-subregion-bug-handoff.md`.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
MAPPING_PATH = REPO_ROOT / "config" / "territory_mappings.json"


@lru_cache(maxsize=1)
def _load() -> dict:
    with MAPPING_PATH.open("r", encoding="utf-8") as fh:
        return json.load(fh)


def get_director_book(director_name: str) -> dict:
    """Return the director-book slice definition for a given director.

    Raises KeyError if the director is not in the mapping.
    """
    data = _load()
    book = data.get("director_book_rollup", {})
    if director_name not in book:
        raise KeyError(
            f"Unknown director {director_name!r}. "
            f"Known directors: {sorted(book.keys())}"
        )
    return dict(book[director_name])


def get_forecast_rollup_for_region(sales_region: str) -> str:
    """Return the top-level forecast rollup (APAC / EMEA / North America) for a Sales_Region__c value.

    Middle East & Africa maps to EMEA (NOT APAC) per the live Salesforce CRO
    forecast hierarchy verified on 2026-04-10. See
    docs/2026-04-10-forecast-subregion-bug-handoff.md.

    Raises KeyError if the sales_region is not in any rollup.
    """
    data = _load()
    rollup = data.get("forecast_rollup", {})
    for top_level, cfg in rollup.items():
        if sales_region in cfg.get("sales_regions", []):
            return top_level
    raise KeyError(
        f"Sales region {sales_region!r} is not mapped to any forecast rollup. "
        f"Known regions: {sorted(r for cfg in rollup.values() for r in cfg.get('sales_regions', []))}"
    )


def get_forecast_rollup_config(top_level: str) -> dict:
    """Return the full forecast_rollup entry (IDs, child territories, sales_regions)."""
    data = _load()
    rollup = data.get("forecast_rollup", {})
    if top_level not in rollup:
        raise KeyError(
            f"Unknown forecast rollup {top_level!r}. Known: {sorted(rollup.keys())}"
        )
    return dict(rollup[top_level])


def get_cro() -> dict:
    """Return the CRO forecasting owner/territory/type IDs."""
    return dict(_load().get("cro", {}))


if __name__ == "__main__":
    # Quick self-check / acceptance criterion 4:
    # CRO/APAC/EMEA/NA reconciliation is reproducible from code.
    print("Director book rollup:")
    for name in [
        "Jesper Tyrer",
        "Sarah Pittroff",
        "Francois Thaury",
        "Dan Peppett",
        "Christian Ebbesen",
        "Mourad Essofi",
        "Megan Miceli",
        "Patrick Gaughan",
        "Adam Steinhaus",
    ]:
        print(f"  {name} -> {get_director_book(name)}")

    print("\nForecast rollup for each sales region:")
    for region in [
        "APAC",
        "Central Europe",
        "Northern Europe",
        "Southwestern Europe",
        "United Kingdom & Ireland",
        "Middle East & Africa",
        "North America",
    ]:
        print(f"  {region} -> {get_forecast_rollup_for_region(region)}")

    print("\nCRO:", get_cro())

    # Hard-assert the bug fix.
    assert get_forecast_rollup_for_region("Middle East & Africa") == "EMEA", (
        "Regression: Middle East & Africa must roll up under EMEA, not APAC."
    )
    assert get_forecast_rollup_for_region("APAC") == "APAC"
    print("\nOK: Middle East & Africa correctly rolls up under EMEA.")
