"""Track D — distribution audit tests with hand-crafted negative-control fixtures.

Each scenario isolates exactly one failure mode (per
``docs/2026-04-25-gpt-pro-feedback-implementation-plan.md``) so a regression
points at a specific axis instead of a vague "distribution drifted" message.
The seven required scenarios:

* normal_stage_mix              -> no findings (control)
* stage_5_missing               -> required + disappeared + sentinel findings
* territory_dropped             -> disappeared + share-drift findings
* quarter_missing               -> required + disappeared findings
* owner_concentration_spike     -> concentration finding
* missing_distribution_seed     -> no blocker; only contract-required and
                                   concentration axes evaluate
* contract_opt_up_blocked       -> high-severity finding (blocks release)

Plus coverage of the per-axis policy plumbing (each action level → expected
finding severity), the comparator's purity, and the
``compare_run_distributions`` summary block.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.monthly_platform.source_distribution_audit import (
    DimensionSeed,
    SourceDistributionSeed,
    audit_distribution,
    compare_run_distributions,
    load_distribution_seeds,
)
from scripts.monthly_platform.source_requirements import (
    DimensionPolicy,
    DistributionPolicy,
    SliceSentinel,
    SourcePlanItem,
    distribution_action_to_severity,
)


FIXTURES = Path(__file__).parent / "fixtures" / "source_distribution"
SEED_PATH = FIXTURES / "seed_pipeline_open_apac_current_quarter.json"


# ---------------------------------------------------------------------------
# Builders
# ---------------------------------------------------------------------------


def _load_rows(name: str) -> list[dict[str, Any]]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _seed() -> SourceDistributionSeed:
    return SourceDistributionSeed.model_validate(
        json.loads(SEED_PATH.read_text(encoding="utf-8"))
    )


def _item(distribution_policy: DistributionPolicy | None) -> SourcePlanItem:
    return SourcePlanItem(
        requirement_id="sd_pipeline_open",
        source_system="salesforce",
        source_type="salesforce_list_view",
        salesforce_object="Opportunity",
        dataset="pipeline_open",
        output_grain="opportunity",
        scope="territory",
        territory="APAC",
        director="Test Director",
        region="Asia",
        period_role="current_quarter",
        quarter_label="Q2",
        quarter_title="FY26 Q2",
        source_id="00BTb00000FIXTURE",
        source_label="Fixture List View",
        status="configured",
        distribution_policy=distribution_policy,
    )


def _full_policy(
    *,
    missing_action: str = "warning",
    disappeared_action: str = "warning",
    share_drift_action: str = "info",
    concentration_action: str = "info",
    max_top_category_share: float = 0.60,
    include_sentinels: bool = True,
) -> DistributionPolicy:
    """Standard 4-dimension policy used by most scenarios."""
    return DistributionPolicy(
        default_action="info",
        dimensions=[
            DimensionPolicy(
                field="StageName",
                semantic_name="stage",
                required_categories=[
                    "2 - Validating",
                    "3 - Solutioning",
                    "4 - Proposing",
                    "5 - Negotiating",
                ],
                missing_category_action=missing_action,  # type: ignore[arg-type]
                disappeared_category_action=disappeared_action,  # type: ignore[arg-type]
                share_drift_action=share_drift_action,  # type: ignore[arg-type]
                max_abs_share_delta=0.20,
            ),
            DimensionPolicy(
                field="CloseQuarter",
                semantic_name="close_quarter",
                required_categories=["Q2", "Q3"],
                missing_category_action="warning",
                disappeared_category_action="warning",
                share_drift_action="info",
                max_abs_share_delta=0.25,
            ),
            DimensionPolicy(
                field="Territory",
                semantic_name="territory",
                missing_category_action="warning",
                disappeared_category_action="warning",
                share_drift_action="info",
                max_abs_share_delta=0.25,
            ),
            DimensionPolicy(
                field="Owner.Name",
                semantic_name="owner",
                concentration_action=concentration_action,  # type: ignore[arg-type]
                max_top_category_share=max_top_category_share,
            ),
        ],
        slice_sentinels=(
            [
                SliceSentinel(
                    id="stage_5_presence",
                    field="StageName",
                    category="5 - Negotiating",
                    action="warning",
                    reason=(
                        "Stage 5 disappearance is a high-signal accidental "
                        "filter/scope failure."
                    ),
                ),
            ]
            if include_sentinels
            else []
        ),
    )


# ---------------------------------------------------------------------------
# Scenario 1 — normal_stage_mix (control, no findings)
# ---------------------------------------------------------------------------


def test_normal_stage_mix_emits_zero_findings():
    rows = _load_rows("rows_normal_stage_mix.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    assert findings == [], (
        f"unexpected findings: {[f.issue for f in findings]}\npayload: {payload}"
    )
    assert payload["status"] == "ok"
    assert payload["seed_present"] is True
    # Sentinel still ran and passed.
    sentinel = next(
        s for s in payload["slice_sentinels"] if s["id"] == "stage_5_presence"
    )
    assert sentinel["passed"] is True


# ---------------------------------------------------------------------------
# Scenario 2 — stage_5_missing
# ---------------------------------------------------------------------------


def test_stage_5_missing_emits_required_disappeared_and_sentinel_findings():
    rows = _load_rows("rows_stage_5_missing.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    issues = [f.issue for f in findings]
    assert "source_distribution_required_category_missing" in issues
    assert "source_distribution_category_disappeared" in issues
    assert "source_distribution_sentinel_failed" in issues
    sentinel = next(
        s for s in payload["slice_sentinels"] if s["id"] == "stage_5_presence"
    )
    assert sentinel["passed"] is False
    assert sentinel["observed_rows"] == 0
    # Default actions: missing/disappeared = warning → severity medium.
    severities = {f.severity for f in findings}
    assert severities <= {"medium", "info"}, severities


# ---------------------------------------------------------------------------
# Scenario 3 — territory_dropped
# ---------------------------------------------------------------------------


def test_territory_dropped_emits_disappeared_finding_for_japan():
    rows = _load_rows("rows_territory_dropped.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    territory_findings = [f for f in findings if "field=Territory" in f.evidence]
    assert any(
        "category_disappeared" in f.issue and "'Japan'" in f.evidence
        for f in territory_findings
    )
    territory_payload = next(
        d for d in payload["dimensions"] if d["field"] == "Territory"
    )
    assert "Japan" in territory_payload["disappeared_categories"]
    # APAC is now 100% — share drift should also fire.
    assert any(
        f.issue == "source_distribution_share_drift" and "'APAC'" in f.evidence
        for f in territory_findings
    )


# ---------------------------------------------------------------------------
# Scenario 4 — quarter_missing
# ---------------------------------------------------------------------------


def test_quarter_missing_emits_required_and_disappeared_for_q2():
    rows = _load_rows("rows_quarter_missing.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    quarter_findings = [f for f in findings if "field=CloseQuarter" in f.evidence]
    issues = {f.issue for f in quarter_findings}
    assert "source_distribution_required_category_missing" in issues
    assert "source_distribution_category_disappeared" in issues
    quarter_payload = next(
        d for d in payload["dimensions"] if d["field"] == "CloseQuarter"
    )
    assert "Q2" in quarter_payload["missing_required_categories"]
    assert "Q2" in quarter_payload["disappeared_categories"]


# ---------------------------------------------------------------------------
# Scenario 5 — owner_concentration_spike
# ---------------------------------------------------------------------------


def test_owner_concentration_spike_emits_concentration_finding():
    rows = _load_rows("rows_owner_concentration_spike.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    owner_findings = [f for f in findings if "field=Owner.Name" in f.evidence]
    assert any(
        f.issue == "source_distribution_concentration_drift" and "'Alice'" in f.evidence
        for f in owner_findings
    ), (
        f"expected Alice concentration finding, got: {[f.evidence for f in owner_findings]}"
    )
    owner_payload = next(d for d in payload["dimensions"] if d["field"] == "Owner.Name")
    assert owner_payload["concentration"] is not None
    assert owner_payload["concentration"]["top_category"] == "Alice"
    assert owner_payload["concentration"]["top_share"] == 0.7


# ---------------------------------------------------------------------------
# Scenario 6 — missing_distribution_seed
# ---------------------------------------------------------------------------


def test_missing_distribution_seed_skips_seed_dependent_axes():
    """No seed file → disappeared + share-drift checks must be silent.

    Required-category and concentration axes still evaluate because they do
    not depend on a seed. Using rows that satisfy required + concentration so
    the result is "no findings", proving the absence of a seed is not itself
    a release blocker.
    """
    rows = _load_rows("rows_normal_stage_mix.json")
    item = _item(_full_policy())

    payload, findings = audit_distribution(item=item, rows=rows, seed=None)

    assert findings == []
    assert payload["status"] == "ok"
    assert payload["seed_present"] is False
    for dim in payload["dimensions"]:
        # No source seed → seed_status is "no_source_seed" (not "missing"), so
        # no source_distribution_dimension_seed_missing finding fires.
        assert dim["seed_status"] == "no_source_seed"
        # Disappeared / share_drift outputs should be empty when no seed.
        assert dim["disappeared_categories"] == []
        assert dim["share_drift"] == []


# ---------------------------------------------------------------------------
# Scenario 6b — partial seed coverage (Track D activation patch)
# Source has a seed file, but a configured dimension is not in it. This used
# to be a silent gap: the dimension's seed-dependent axes skipped without a
# finding. The patch emits ``source_distribution_dimension_seed_missing`` and
# records ``seed_status="missing"`` per dimension.
# ---------------------------------------------------------------------------


def _seed_without(field: str) -> SourceDistributionSeed:
    """Helper: load the standard seed and remove one dimension."""
    payload = json.loads(SEED_PATH.read_text(encoding="utf-8"))
    payload["dimensions"] = {
        k: v for k, v in payload["dimensions"].items() if k != field
    }
    return SourceDistributionSeed.model_validate(payload)


def test_partial_seed_emits_missing_dimension_finding_at_info_default():
    rows = _load_rows("rows_normal_stage_mix.json")
    item = _item(_full_policy())
    seed = _seed_without("Owner.Name")

    payload, findings = audit_distribution(item=item, rows=rows, seed=seed)

    owner_payload = next(d for d in payload["dimensions"] if d["field"] == "Owner.Name")
    assert owner_payload["seed_status"] == "missing"
    # Other dimensions should still be present.
    stage_payload = next(d for d in payload["dimensions"] if d["field"] == "StageName")
    assert stage_payload["seed_status"] == "present"

    missing_findings = [
        f
        for f in findings
        if f.issue == "source_distribution_dimension_seed_missing"
        and "field=Owner.Name" in f.evidence
    ]
    assert len(missing_findings) == 1
    assert missing_findings[0].severity == "info"
    assert "seed_status=missing" in missing_findings[0].evidence


def test_partial_seed_contract_can_opt_up_to_blocked():
    """Contract opts ``missing_seed_action=blocked`` for one dimension."""
    rows = _load_rows("rows_normal_stage_mix.json")
    policy = DistributionPolicy(
        default_action="info",
        dimensions=[
            DimensionPolicy(
                field="Owner.Name",
                semantic_name="owner",
                missing_seed_action="blocked",
            ),
        ],
    )
    item = _item(policy)
    seed = _seed_without("Owner.Name")

    _payload, findings = audit_distribution(item=item, rows=rows, seed=seed)

    high = [f for f in findings if f.severity == "high"]
    assert any(f.issue == "source_distribution_dimension_seed_missing" for f in high), (
        "expected high-severity dimension_seed_missing finding"
    )


def test_partial_seed_contract_can_opt_out_to_ok():
    """``missing_seed_action=ok`` suppresses the finding entirely."""
    rows = _load_rows("rows_normal_stage_mix.json")
    policy = DistributionPolicy(
        default_action="info",
        dimensions=[
            DimensionPolicy(
                field="Owner.Name",
                semantic_name="owner",
                missing_seed_action="ok",
            ),
        ],
    )
    item = _item(policy)
    seed = _seed_without("Owner.Name")

    payload, findings = audit_distribution(item=item, rows=rows, seed=seed)

    assert findings == []
    # seed_status is still recorded in the payload even when the finding is
    # suppressed — operators need the visibility regardless of severity.
    owner_payload = next(d for d in payload["dimensions"] if d["field"] == "Owner.Name")
    assert owner_payload["seed_status"] == "missing"


def test_partial_seed_does_not_fire_when_seed_is_entirely_absent():
    """``no_source_seed`` is a separate state and must not emit a missing-dimension finding."""
    rows = _load_rows("rows_normal_stage_mix.json")
    item = _item(_full_policy())

    _payload, findings = audit_distribution(item=item, rows=rows, seed=None)

    assert all(
        f.issue != "source_distribution_dimension_seed_missing" for f in findings
    ), "no_source_seed must not fire missing_dimension findings"


def test_run_summary_counts_missing_seed_dimensions_across_sources():
    """Run-level summary must surface partial-seed gaps as a distinct count."""
    item = _item(_full_policy())
    payload_a, findings_a = audit_distribution(
        item=item,
        rows=_load_rows("rows_normal_stage_mix.json"),
        seed=_seed_without("Owner.Name"),  # 1 missing dim
    )
    payload_b, findings_b = audit_distribution(
        item=item,
        rows=_load_rows("rows_normal_stage_mix.json"),
        seed=_seed_without("StageName"),  # 1 missing dim
    )
    payload_c, findings_c = audit_distribution(
        item=item,
        rows=_load_rows("rows_normal_stage_mix.json"),
        seed=_seed(),  # full seed, 0 missing
    )

    summary = compare_run_distributions(
        per_source_payloads=[payload_a, payload_b, payload_c],
        findings=findings_a + findings_b + findings_c,
    )

    assert summary["missing_seed_dimension_count"] == 2
    # missing_seed_source_count is the existing counter (no source seed at
    # all). All three sources have a seed here, so it stays at 0.
    assert summary["missing_seed_source_count"] == 0


# ---------------------------------------------------------------------------
# Scenario 7 — contract_opt_up_blocked
# ---------------------------------------------------------------------------


def test_contract_opt_up_blocked_escalates_disappeared_to_high():
    """Same data as stage_5_missing, but the contract opts the disappeared axis up.

    The escalated finding must be ``high`` severity so the run-level extract
    audit gate treats it as release-blocking.
    """
    rows = _load_rows("rows_stage_5_missing.json")
    policy = _full_policy(disappeared_action="blocked")
    item = _item(policy)

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    high = [f for f in findings if f.severity == "high"]
    assert high, "expected at least one high-severity finding"
    assert any(f.issue == "source_distribution_category_disappeared" for f in high)


# ---------------------------------------------------------------------------
# Action → severity mapping plumbing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "action,expected",
    [
        ("ok", None),
        ("info", "info"),
        ("warning", "medium"),
        ("blocked", "high"),
    ],
)
def test_distribution_action_to_severity_mapping(action, expected):
    assert distribution_action_to_severity(action) == expected


def test_dimension_with_ok_action_emits_no_finding_for_disappearance():
    rows = _load_rows("rows_stage_5_missing.json")
    policy = _full_policy(disappeared_action="ok", missing_action="ok")
    item = _item(policy)

    _payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    stage_findings = [
        f
        for f in findings
        if "field=StageName" in f.evidence and "category_disappeared" in f.issue
    ]
    assert stage_findings == [], (
        "ok action should suppress the finding entirely, "
        f"got: {[f.issue for f in stage_findings]}"
    )


# ---------------------------------------------------------------------------
# No distribution_policy → no-op
# ---------------------------------------------------------------------------


def test_audit_with_no_distribution_policy_is_noop():
    rows = _load_rows("rows_stage_5_missing.json")
    item = _item(distribution_policy=None)

    payload, findings = audit_distribution(item=item, rows=rows, seed=_seed())

    assert findings == []
    assert payload["status"] == "no_policy"
    assert payload["dimensions"] == []
    assert payload["slice_sentinels"] == []


# ---------------------------------------------------------------------------
# Comparator purity: never mutates inputs
# ---------------------------------------------------------------------------


def test_audit_does_not_mutate_rows_or_seed():
    rows = _load_rows("rows_stage_5_missing.json")
    seed = _seed()
    rows_snapshot = json.dumps(rows, sort_keys=True)
    seed_snapshot = seed.model_dump_json()
    item = _item(_full_policy())

    audit_distribution(item=item, rows=rows, seed=seed)

    assert json.dumps(rows, sort_keys=True) == rows_snapshot
    assert seed.model_dump_json() == seed_snapshot


# ---------------------------------------------------------------------------
# Run-level summary
# ---------------------------------------------------------------------------


def test_compare_run_distributions_summarizes_per_source_payloads():
    item_with_policy = _item(_full_policy())
    item_no_policy = _item(distribution_policy=None)
    payload_a, findings_a = audit_distribution(
        item=item_with_policy,
        rows=_load_rows("rows_stage_5_missing.json"),
        seed=_seed(),
    )
    payload_b, findings_b = audit_distribution(
        item=item_no_policy,
        rows=_load_rows("rows_normal_stage_mix.json"),
        seed=None,
    )
    payload_c, findings_c = audit_distribution(
        item=item_with_policy,
        rows=_load_rows("rows_normal_stage_mix.json"),
        seed=None,  # missing seed
    )
    all_findings = findings_a + findings_b + findings_c

    summary = compare_run_distributions(
        per_source_payloads=[payload_a, payload_b, payload_c],
        findings=all_findings,
    )

    assert summary["matched_source_count"] == 2
    assert summary["no_policy_source_count"] == 1
    assert summary["missing_seed_source_count"] == 1
    assert summary["distribution_finding_count"] == len(all_findings)
    assert summary["high_finding_count"] == 0  # default actions never block


# ---------------------------------------------------------------------------
# Seed loader
# ---------------------------------------------------------------------------


def test_load_distribution_seeds_returns_empty_for_missing_dir(tmp_path: Path):
    assert load_distribution_seeds(tmp_path / "does_not_exist") == {}


def test_load_distribution_seeds_round_trips_a_seed(tmp_path: Path):
    seed = _seed()
    (tmp_path / "x.json").write_text(seed.model_dump_json(), encoding="utf-8")

    loaded = load_distribution_seeds(tmp_path)

    assert set(loaded.keys()) == {seed.baseline_key}
    assert (
        loaded[seed.baseline_key].dimensions["StageName"].share_by_category
        == seed.dimensions["StageName"].share_by_category
    )


def test_load_distribution_seeds_rejects_duplicate_baseline_keys(tmp_path: Path):
    payload = _seed().model_dump_json()
    (tmp_path / "a.json").write_text(payload, encoding="utf-8")
    (tmp_path / "b.json").write_text(payload, encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate seed baseline_key"):
        load_distribution_seeds(tmp_path)


# ---------------------------------------------------------------------------
# Dotted-field extraction (Owner.Name nested vs flat)
# ---------------------------------------------------------------------------


def test_dotted_owner_field_resolves_through_nested_dict():
    rows = _load_rows("rows_owner_concentration_spike.json")
    seed = _seed()
    item = _item(_full_policy())

    payload, _findings = audit_distribution(item=item, rows=rows, seed=seed)

    owner_payload = next(d for d in payload["dimensions"] if d["field"] == "Owner.Name")
    assert "Alice" in owner_payload["current_shares"]
    assert owner_payload["current_shares"]["Alice"] == 0.7


def test_dotted_owner_field_resolves_through_flat_underscore_key():
    """Salesforce list-view extracts sometimes flatten relations to ``Owner_Name``."""
    rows = [
        {"Id": "1", "Owner_Name": "Alice"},
        {"Id": "2", "Owner_Name": "Alice"},
        {"Id": "3", "Owner_Name": "Bob"},
    ]
    item = _item(
        DistributionPolicy(
            default_action="info",
            dimensions=[
                DimensionPolicy(
                    field="Owner.Name",
                    semantic_name="owner",
                    concentration_action="warning",
                    max_top_category_share=0.50,
                ),
            ],
        )
    )

    payload, findings = audit_distribution(item=item, rows=rows, seed=None)

    owner_payload = payload["dimensions"][0]
    assert owner_payload["current_shares"]["Alice"] == pytest.approx(2 / 3, abs=1e-6)
    # 2/3 > 0.50 → concentration finding fires.
    assert any(f.issue == "source_distribution_concentration_drift" for f in findings)


# ---------------------------------------------------------------------------
# Seed validation
# ---------------------------------------------------------------------------


def test_dimension_seed_rejects_share_outside_unit_interval():
    with pytest.raises(ValueError, match="not in"):
        DimensionSeed(
            field="StageName",
            semantic_name="stage",
            sample_count=10,
            share_by_category={"x": 1.5},
        )
