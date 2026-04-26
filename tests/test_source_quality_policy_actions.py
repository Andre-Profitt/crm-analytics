"""Track B — per-axis source-quality policy actions.

Closes the GPT Pro v2 review's "partly theater" finding on extract quality:
each axis (zero-row, min-rows, max-rows, max-records, null-threshold,
distribution) now carries its own action policy. Severity is no longer
derived from ``zero_row_action``.

Reference:
    docs/2026-04-25-gpt-pro-feedback-implementation-plan.md (Track B)
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.compile_monthly_source_contract_config import (
    compile_monthly_source_contract_config,
)
from scripts.extract_salesforce_sources import audit_source_extract_quality
from scripts.monthly_platform.source_requirements import (
    FieldContract,
    RowCountPolicy,
    SourcePlanItem,
    action_to_severity,
)


REPO_ROOT = Path(__file__).resolve().parent.parent


# ---------- action_to_severity ---------------------------------------------


@pytest.mark.parametrize(
    ("action", "expected"),
    [
        ("blocked", "high"),
        ("warning", "medium"),
        ("fallback", "medium"),
        ("ok", None),
    ],
)
def test_action_to_severity_mapping(action: str, expected: str | None) -> None:
    assert action_to_severity(action) == expected  # type: ignore[arg-type]


# ---------- model defaults --------------------------------------------------


def test_row_count_policy_per_axis_action_defaults() -> None:
    policy = RowCountPolicy()
    # Defaults must NOT collapse to a single shared action.
    assert policy.zero_row_action == "ok"
    assert policy.min_rows_action == "warning"
    assert policy.max_rows_action == "blocked"
    assert policy.max_records_action == "warning"
    assert policy.distribution_action == "warning"
    assert policy.expected_empty_conditions == []


def test_row_count_policy_round_trip_includes_new_fields() -> None:
    policy = RowCountPolicy(
        min_rows=5,
        min_rows_action="blocked",
        max_rows=1000,
        max_rows_action="warning",
        max_records_action="blocked",
        distribution_action="fallback",
        expected_empty_conditions=["territory_has_no_forward_pipeline"],
    )
    payload = policy.model_dump()
    assert payload["min_rows_action"] == "blocked"
    assert payload["max_rows_action"] == "warning"
    assert payload["max_records_action"] == "blocked"
    assert payload["distribution_action"] == "fallback"
    assert payload["expected_empty_conditions"] == ["territory_has_no_forward_pipeline"]
    rebuilt = RowCountPolicy.model_validate(payload)
    assert rebuilt == policy


# ---------- yaml→json compile preserves new fields --------------------------


def test_compile_yaml_to_json_preserves_per_axis_actions() -> None:
    """The authoring YAML round-trips into the runtime JSON without drift."""
    result = compile_monthly_source_contract_config(check=True, write=False)
    assert result["status"] == "ok", f"compile drift: {result}"
    assert result["drift_count"] == 0
    assert result["missing_count"] == 0

    # Cross-check: every policy block in the runtime requirements JSON now
    # carries the new per-axis action fields.
    requirements_path = REPO_ROOT / "config" / "monthly_source_requirements.json"
    payload = json.loads(requirements_path.read_text(encoding="utf-8"))
    requirements = payload.get("requirements", payload)
    if isinstance(requirements, dict):
        requirements = list(requirements.values())
    assert isinstance(requirements, list)
    assert requirements, "requirements JSON contains no source requirements"

    required_keys = {
        "min_rows_action",
        "max_rows_action",
        "max_records_action",
        "distribution_action",
    }
    for requirement in requirements:
        policy = requirement.get("row_count_policy", {})
        missing = required_keys - policy.keys()
        assert not missing, f"{requirement.get('requirement_id')} missing {missing}"


# ---------- audit emitter uses per-axis action ------------------------------


def _make_item(policy: RowCountPolicy, *, territory: str = "EMEA") -> SourcePlanItem:
    """Construct a minimal SourcePlanItem for the audit function under test."""
    return SourcePlanItem(
        requirement_id="test_source",
        source_system="salesforce",
        source_type="salesforce_report",
        salesforce_object="Opportunity",
        dataset="pipeline_open",
        output_grain="opportunity",
        scope="territory",
        territory=territory,
        period_role="current_quarter",
        quarter_label="Q2 2026",
        quarter_title="Q2 2026 Land",
        source_id="00O000000000001",
        source_label="Test source",
        status="configured",
        required_fields=[FieldContract(name="Stage", required=True)],
        row_count_policy=policy,
    )


def test_min_rows_breach_uses_min_rows_action_not_zero_row_action() -> None:
    """The Track B fix: min-rows severity no longer inherits zero_row_action."""
    policy = RowCountPolicy(
        allow_zero=True,
        min_rows=10,
        zero_row_action="warning",  # would have produced medium under old code
        min_rows_action="blocked",  # ...but per-axis says block
    )
    item = _make_item(policy)
    rows = [{"Stage": "5 - Negotiating"}] * 3  # 3 < 10
    audit, findings = audit_source_extract_quality(item=item, rows=rows)

    min_findings = [f for f in findings if f.issue == "source_row_count_below_min"]
    assert len(min_findings) == 1
    assert min_findings[0].severity == "high", (
        "min_rows_action=blocked must yield severity=high regardless of zero_row_action"
    )
    assert audit["row_count_status"] == "blocked"


def test_min_rows_breach_action_ok_skips_finding() -> None:
    policy = RowCountPolicy(
        allow_zero=True,
        min_rows=10,
        zero_row_action="warning",
        min_rows_action="ok",
    )
    item = _make_item(policy)
    rows = [{"Stage": "5"}] * 3
    audit, findings = audit_source_extract_quality(item=item, rows=rows)
    assert not [f for f in findings if f.issue == "source_row_count_below_min"]


def test_max_records_reached_uses_max_records_action() -> None:
    """Was hardcoded medium; now respects max_records_action."""
    policy = RowCountPolicy(max_records_action="blocked")
    item = _make_item(policy)
    rows = [{"Stage": "5"}] * 5000
    audit, findings = audit_source_extract_quality(
        item=item,
        rows=rows,
        source_metadata={"max_records": 5000},
    )
    cap_findings = [
        f for f in findings if f.issue == "source_extract_max_records_reached"
    ]
    assert len(cap_findings) == 1
    assert cap_findings[0].severity == "high"


def test_max_rows_breach_uses_max_rows_action() -> None:
    policy = RowCountPolicy(max_rows=10, max_rows_action="warning")
    item = _make_item(policy)
    rows = [{"Stage": "5"}] * 50
    audit, findings = audit_source_extract_quality(item=item, rows=rows)
    above_findings = [f for f in findings if f.issue == "source_row_count_above_max"]
    assert len(above_findings) == 1
    assert above_findings[0].severity == "medium", (
        "max_rows_action=warning must yield medium, not the hardcoded high it used to be"
    )


def test_zero_row_emits_expected_empty_conditions_in_evidence() -> None:
    policy = RowCountPolicy(
        allow_zero=True,
        zero_row_action="fallback",
        expected_empty_conditions=["territory_has_no_forward_pipeline"],
    )
    item = _make_item(policy)
    audit, findings = audit_source_extract_quality(item=item, rows=[])
    fallback_findings = [
        f for f in findings if f.issue == "source_row_count_zero_fallback"
    ]
    assert len(fallback_findings) == 1
    assert "territory_has_no_forward_pipeline" in fallback_findings[0].evidence


def test_zero_row_does_not_double_emit_with_min_rows_breach() -> None:
    """Track B cleanup: a zero-row source with min_rows>0 emits ONE finding,
    not one zero-row finding plus one min_rows-below finding."""
    policy = RowCountPolicy(
        allow_zero=True,
        min_rows=5,
        zero_row_action="warning",
        min_rows_action="blocked",
    )
    item = _make_item(policy)
    audit, findings = audit_source_extract_quality(item=item, rows=[])
    issues = sorted(f.issue for f in findings)
    assert "source_row_count_below_min" not in issues, (
        "min-rows-below must NOT fire when row_count==0 (the zero-row branch already emitted)"
    )
    assert "source_row_count_zero_warning" in issues
