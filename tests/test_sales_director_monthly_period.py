from __future__ import annotations

import argparse
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.monthly_platform.period import resolve_period_context  # noqa: E402
from scripts.run_sales_director_monthly_cadence import (  # noqa: E402
    command_plan,
    resolve_runtime_period as cadence_resolve_runtime_period,
)
from scripts.run_sales_director_monthly_master_builder import (  # noqa: E402
    resolve_runtime_period as builder_resolve_runtime_period,
)


def test_resolve_period_context_uses_previous_month_end_for_first_of_month() -> None:
    period = resolve_period_context(as_of_date="2026-05-01")

    assert period.reporting_month == "2026-04"
    assert period.snapshot_date == "2026-04-30"
    assert period.deck_date == "2026-04-30"
    assert period.month_title == "April 2026"
    assert period.quarter_policy.name == "calendar_quarter"
    assert period.quarter_policy.fiscal_year_start_month == 1
    assert period.current_quarter.title == "Q2 2026"
    assert period.prior_quarter.title == "Q1 2026"
    assert period.forward_quarter.title == "Q3 2026"
    assert period.reporting_window_start == "2026-01-01"
    assert period.reporting_window_end == "2026-09-30"


def test_resolve_period_context_handles_year_rollover() -> None:
    period = resolve_period_context(as_of_date="2027-01-01")

    assert period.reporting_month == "2026-12"
    assert period.snapshot_date == "2026-12-31"
    assert period.deck_date == "2026-12-31"
    assert period.month_title == "December 2026"
    assert period.current_quarter.title == "Q4 2026"
    assert period.prior_quarter.title == "Q3 2026"
    assert period.forward_quarter.title == "Q1 2027"
    assert period.reporting_window_end == "2027-03-31"


def test_resolve_period_context_can_use_fiscal_quarters_explicitly() -> None:
    period = resolve_period_context(
        snapshot_date="2026-04-30",
        quarter_policy_name="fiscal_quarter",
        fiscal_year_start_month=2,
    )

    assert period.quarter_policy.name == "fiscal_quarter"
    assert period.quarter_policy.fiscal_year_start_month == 2
    assert period.fiscal_year == "FY26"
    assert period.current_quarter.title == "Q1 2026"
    assert period.current_quarter.start_date == "2026-02-01"
    assert period.current_quarter.end_date == "2026-04-30"
    assert period.forward_quarter.title == "Q2 2026"
    assert period.reporting_window_start == "2026-02-01"
    assert period.reporting_window_end == "2026-07-31"


def test_cadence_resolves_snapshot_date_when_omitted(monkeypatch) -> None:
    captured: dict[str, object] = {}

    def fake_run_builder(args: list[str]) -> dict[str, str]:
        captured["args"] = args
        return {"status": "ok"}

    monkeypatch.setattr("scripts.run_sales_director_monthly_cadence.run_builder", fake_run_builder)

    args = argparse.Namespace(
        snapshot_date=None,
        as_of_date="2026-05-01",
        deck_date=None,
        director="Jane Doe",
        deck_source="canonical-shell",
        workbook_root=None,
        snapshot_root=None,
        fallback_workbook_deck=False,
        allow_generated_shell_fallback=False,
        refresh_snapshots=False,
        fail_fast=False,
        skip_excel_brief=False,
        skip_powerpoint_review=False,
        unattended=False,
        powerpoint_mode="audit",
    )

    payload = command_plan(args)

    assert payload == {"status": "ok"}
    assert captured["args"] == [
        "--snapshot-date",
        "2026-04-30",
        "--powerpoint-mode",
        "audit",
        "--deck-source",
        "canonical-shell",
        "--plan-only",
        "--as-of-date",
        "2026-05-01",
        "--director",
        "Jane Doe",
    ]


def test_builder_resolve_runtime_period_prefers_explicit_snapshot_date() -> None:
    args = argparse.Namespace(
        snapshot_date="2026-04-10",
        as_of_date="2026-05-01",
        deck_date=None,
    )

    period = builder_resolve_runtime_period(args)
    cadence_period = cadence_resolve_runtime_period(args)

    assert period.snapshot_date == "2026-04-10"
    assert period.deck_date == "2026-04-10"
    assert cadence_period["snapshot_date"] == "2026-04-10"
    assert cadence_period["deck_date"] == "2026-04-10"
