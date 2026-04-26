"""Track A — assert the monthly cron is wired to the source-backed runner.

Closes the GPT Pro hazard where the scheduled workflow defaulted the snapshot
date to "today" via `date +%Y-%m-%d` and invoked the legacy lane.

Reference:
    docs/2026-04-25-gpt-pro-feedback-implementation-plan.md (Track A)
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

WORKFLOW_PATH = (
    Path(__file__).resolve().parent.parent
    / ".github"
    / "workflows"
    / "monthly-review.yml"
)


@pytest.fixture(scope="module")
def workflow_text() -> str:
    assert WORKFLOW_PATH.is_file(), f"workflow not found: {WORKFLOW_PATH}"
    return WORKFLOW_PATH.read_text()


def test_workflow_invokes_source_backed_runner(workflow_text: str) -> None:
    assert "scripts/run_source_backed_monthly_pipeline.py" in workflow_text, (
        "Default lane must be the source-backed runner."
    )


def test_workflow_passes_required_runner_args(workflow_text: str) -> None:
    assert "--snapshot-date" in workflow_text
    assert "--run-id" in workflow_text


def test_dispatch_input_snapshot_date_is_required(workflow_text: str) -> None:
    # Match only the lines that belong to the snapshot_date input block:
    # subsequent lines indented strictly deeper than snapshot_date itself.
    # Stop at the first line at the same or lesser indent (next sibling input).
    block_match = re.search(
        r"^(?P<indent> +)snapshot_date:\s*\n"
        r"(?P<body>(?:(?P=indent) +\S[^\n]*\n)+)",
        workflow_text,
        re.MULTILINE,
    )
    assert block_match is not None, "snapshot_date input block missing"
    body = block_match.group("body")
    assert "required: true" in body, "snapshot_date must be required: true"
    assert 'default: ""' not in body, (
        "snapshot_date must NOT carry an empty-string default — that is the GPT Pro hazard."
    )


def test_no_today_default_for_snapshot_date(workflow_text: str) -> None:
    forbidden = [
        "$(date +%Y-%m-%d)",
        "date +%F",
    ]
    for token in forbidden:
        assert token not in workflow_text, (
            f"Workflow must not default snapshot to today via `{token}`. "
            "Cron path should resolve to last day of prior month; dispatch path requires explicit input."
        )


def test_cron_resolves_to_prior_month_end(workflow_text: str) -> None:
    assert "last day of prior month" in workflow_text.lower() or (
        "first_of_this_month" in workflow_text and "timedelta(days=1)" in workflow_text
    ), "Cron path must compute snapshot = last day of prior month."


def test_legacy_lane_is_opt_in_only(workflow_text: str) -> None:
    legacy_calls = re.findall(r"scripts/run_monthly_director_review\.py", workflow_text)
    assert legacy_calls, (
        "Legacy script reference must remain (under legacy_only fallback)."
    )

    legacy_step_match = re.search(
        r"- name: Run legacy lane.*?(?=\n      - name:|\Z)",
        workflow_text,
        re.DOTALL,
    )
    assert legacy_step_match is not None, "Legacy fallback step not found"
    # PR #3 (workflow hotfix) replaced the string comparison
    # ``inputs.legacy_only == 'true'`` with the boolean form
    # ``github.event_name == 'workflow_dispatch' && inputs.legacy_only``,
    # because the input is declared ``type: boolean``.
    assert (
        "github.event_name == 'workflow_dispatch' && inputs.legacy_only"
        in legacy_step_match.group(0)
    ), (
        "Legacy lane must be gated on (workflow_dispatch AND inputs.legacy_only); "
        "see commit 34d34ad."
    )

    source_backed_step_match = re.search(
        r"- name: Run source-backed monthly pipeline.*?(?=\n      - name:|\Z)",
        workflow_text,
        re.DOTALL,
    )
    assert source_backed_step_match is not None, "Source-backed step not found"
    assert (
        "github.event_name != 'workflow_dispatch' || !inputs.legacy_only"
        in source_backed_step_match.group(0)
    ), (
        "Source-backed lane must be the default; gate on "
        "(NOT workflow_dispatch OR NOT inputs.legacy_only); see commit 34d34ad."
    )


def test_cron_schedule_unchanged(workflow_text: str) -> None:
    assert 'cron: "0 0 1 * *"' in workflow_text, (
        "Monthly 1st-of-month UTC cron must be preserved."
    )
