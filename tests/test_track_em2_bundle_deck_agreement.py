"""Track E/M2 — bundle/deck contract agreement tests.

Verifies the director_bundle_contract and the deck workbook contract
agree on which datasets/sheets the deck consumes:

  - every dataset flagged ``deck_consumed: true`` must map to a sheet
    that the workbook contract declares as required (or to a documented
    runtime-only role like movement_prior)
  - every required sheet that has a known bundle-dataset equivalent
    (per the canonical map) must be flagged ``deck_consumed`` in the
    bundle contract
  - any ``deck_consumed`` dataset still on policy ``optional_empty``
    must reference Track E/M2 in its rationale (Pydantic validator
    enforces this; this test pins the actual contract)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml


REPO_ROOT = Path(__file__).resolve().parents[1]
BUNDLE_PATH = REPO_ROOT / "config" / "monthly_director_bundle_contract.json"
WORKBOOK_PATH = REPO_ROOT / "config" / "director_workbook_contract.yaml"

# Canonical bundle-dataset to workbook-sheet mapping. None = the dataset
# does not correspond to a workbook sheet (runtime metadata, etc.).
BUNDLE_TO_SHEET: dict[str, str | None] = {
    "pipeline_open": "Pipeline Open FY26",
    "won_lost": "Won Lost FY26",
    "renewals": "Renewals FY26",
    "approvals": "Commercial Approval",
    "pi_current": "Pipeline Inspection",
    "pi_forward": "Pipeline Inspection",
    "activity": "Activity Volume",
    "commit_items": "Commit Items",
    "stage_events": "Stage History",
    "forecast_category_events": "Forecast Category History",
    "close_date_events": None,
    "movement_prior": None,
    "movement_current": None,
    "snapshot_trend": "Q1 Snapshot Trend",
}


@pytest.fixture(scope="module")
def bundle() -> dict:
    return json.loads(BUNDLE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def workbook_required_sheets() -> set[str]:
    wb = yaml.safe_load(WORKBOOK_PATH.read_text(encoding="utf-8"))
    return {s["name"] for s in wb.get("sheets", [])}


def test_every_deck_consumed_dataset_maps_to_a_required_sheet(
    bundle, workbook_required_sheets
):
    for ds in bundle["datasets"]:
        if not ds.get("deck_consumed"):
            continue
        sheet = BUNDLE_TO_SHEET.get(ds["dataset"])
        assert sheet in workbook_required_sheets, (
            f"dataset {ds['dataset']!r} flagged deck_consumed but maps to "
            f"sheet {sheet!r} which is NOT in the workbook contract's "
            f"required_sheets"
        )


def test_every_required_sheet_with_a_bundle_dataset_is_flagged_consumed(
    bundle, workbook_required_sheets
):
    sheet_to_bundle: dict[str, str] = {}
    for dsname, sheet in BUNDLE_TO_SHEET.items():
        if sheet is not None:
            sheet_to_bundle.setdefault(sheet, dsname)
    by_dataset = {d["dataset"]: d for d in bundle["datasets"]}
    for sheet in workbook_required_sheets:
        dsname = sheet_to_bundle.get(sheet)
        if dsname is None:
            # Sheet has no bundle-dataset equivalent (e.g. Summary,
            # Q1 Movement, Q2 Snapshot Trend). Skip — bundle
            # contract doesn't claim coverage of these.
            continue
        ds = by_dataset.get(dsname)
        assert ds is not None, (
            f"workbook required_sheet {sheet!r} maps to bundle dataset "
            f"{dsname!r} but that dataset is missing from the bundle "
            f"contract"
        )
        assert ds.get("deck_consumed") is True, (
            f"bundle dataset {dsname!r} maps to required workbook sheet "
            f"{sheet!r} but is NOT flagged deck_consumed"
        )


def test_deck_consumed_optional_empty_rationale_acknowledges_m2(bundle):
    for ds in bundle["datasets"]:
        if not ds.get("deck_consumed"):
            continue
        if ds["policy"] != "optional_empty":
            continue
        rationale = ds.get("rationale", "")
        assert ("M2" in rationale) or ("deferred" in rationale.lower()), (
            f"deck_consumed dataset {ds['dataset']!r} on optional_empty "
            f"must reference 'M2' or 'deferred' in rationale; got: {rationale!r}"
        )


def test_bundle_contract_loads_with_pydantic_validator(bundle):
    """Smoke test the Pydantic validator (which now also enforces the
    deck_consumed + optional_empty + rationale rule) against the live
    contract."""
    from scripts.monthly_platform.director_bundle_contract import (
        DirectorBundleContract,
    )

    contract = DirectorBundleContract.model_validate(bundle)
    assert len(contract.datasets) > 0
    deck_consumed_count = sum(1 for d in contract.datasets if d.deck_consumed)
    assert deck_consumed_count > 0, (
        "expected at least one deck_consumed dataset on the live bundle contract"
    )
