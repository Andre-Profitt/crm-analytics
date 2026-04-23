"""Validation rules for DirectorBundle. Run after extract, before consumers."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import DirectorBundle

_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

_VALID_STAGES = {
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
    "7 - Closed Won",
    "8 - Closed Lost",
    "9 - Closed Opt Out",
}

_VALID_FORECAST_CATEGORIES = {
    "Omitted",
    "Pipeline",
    "Best Case",
    "Commit",
    "Closed",
}


def validate_bundle(bundle: DirectorBundle) -> list[str]:
    errors: list[str] = []

    ds = bundle.datasets
    actual = {
        "pipeline_open": len(ds.pipeline_open),
        "won_lost": len(ds.won_lost),
        "renewals": len(ds.renewals),
        "approvals": len(ds.approvals),
        "pi_current": len(ds.pi_current),
        "pi_forward": len(ds.pi_forward),
        "activity": len(ds.activity),
        "commit_items": len(ds.commit_items),
        "stage_events": len(ds.stage_events),
        "forecast_category_events": len(ds.forecast_category_events),
        "close_date_events": len(ds.close_date_events),
        "movement_prior": len(ds.movement_prior),
        "movement_current": len(ds.movement_current),
        "snapshot_trend": len(ds.snapshot_trend),
    }
    for key, count in actual.items():
        declared = bundle.dataset_counts.get(key, -1)
        if declared != count:
            errors.append(
                f"dataset_counts['{key}'] = {declared} but actual count = {count}"
            )

    for i, d in enumerate(ds.pipeline_open):
        if d.arr_unweighted < 0:
            errors.append(
                f"pipeline_open[{i}]: negative arr_unweighted ({d.arr_unweighted})"
            )
        if d.close_date and not _ISO_DATE_RE.match(d.close_date):
            errors.append(f"pipeline_open[{i}]: invalid date format '{d.close_date}'")
        if d.stage and d.stage not in _VALID_STAGES:
            errors.append(f"pipeline_open[{i}]: invalid stage '{d.stage}'")
        if (
            d.forecast_category
            and d.forecast_category not in _VALID_FORECAST_CATEGORIES
        ):
            errors.append(
                f"pipeline_open[{i}]: invalid forecast_category '{d.forecast_category}'"
            )

    for i, d in enumerate(ds.won_lost):
        if d.arr_unweighted < 0:
            errors.append(
                f"won_lost[{i}]: negative arr_unweighted ({d.arr_unweighted})"
            )
        if d.close_date and not _ISO_DATE_RE.match(d.close_date):
            errors.append(f"won_lost[{i}]: invalid date format '{d.close_date}'")

    for i, d in enumerate(ds.renewals):
        if d.acv_unweighted < 0:
            errors.append(
                f"renewals[{i}]: negative acv_unweighted ({d.acv_unweighted})"
            )

    return errors
