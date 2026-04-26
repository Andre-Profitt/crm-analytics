"""Track D activation — tests for calibrator, promotion ledger, distribution diff,
and the Salesforce-nested-object extractor enhancement.

These exercise the full activation slice end-to-end without spawning a real
Salesforce extract:

* The calibrator overlays the current contract's ``distribution_policy`` onto
  a historical plan and reads matching raw extract rows.
* ``--promote-baselines`` writes seeds and appends a JSONL ledger entry.
* The run-over-run diff handles new sources, dropped sources, share deltas,
  new/dropped categories, and seed-status transitions.
* ``_extract_value`` resolves all four supported row shapes, including the
  Salesforce list-view nested-object envelope (``Owner.fields.Name.value``).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.calibrate_source_distribution_baselines import (
    PROMOTION_LEDGER,
    calibrate,
    main as calibrator_main,
)
from scripts.monthly_platform.distribution_diff import (
    SCHEMA_VERSION as DIFF_SCHEMA,
    diff_distribution_audits,
)
from scripts.monthly_platform.source_distribution_audit import (
    _MISSING,
    _extract_value,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# _extract_value — Salesforce row shapes
# ---------------------------------------------------------------------------


def test_extract_value_resolves_salesforce_nested_envelope():
    """``Owner.Name`` against the real list-view envelope returns ``fields.Name.value``."""
    row = {
        "Owner": {
            "apiName": "User",
            "fields": {
                "Name": {"displayValue": None, "value": "Alvin Luk"},
            },
        },
    }

    assert _extract_value(row, "Owner.Name") == "Alvin Luk"


def test_extract_value_falls_back_to_display_value_when_value_missing():
    """When ``fields[Name]`` carries only ``displayValue``, that is returned."""
    row = {
        "Account": {
            "fields": {"Name": {"displayValue": "ACME Corp", "value": None}},
        }
    }
    # value=None still wins over displayValue per the documented order.
    assert _extract_value(row, "Account.Name") is None

    row2 = {"Account": {"fields": {"Name": {"displayValue": "ACME Corp"}}}}
    assert _extract_value(row2, "Account.Name") == "ACME Corp"


def test_extract_value_resolves_simple_nested_dict_for_test_fixtures():
    """Existing ``{"Owner": {"Name": "Alice"}}`` shape used by Track D fixtures still works."""
    row = {"Owner": {"Name": "Alice"}}

    assert _extract_value(row, "Owner.Name") == "Alice"


def test_extract_value_falls_back_to_head_display_for_relations():
    """List-view payloads sometimes only expose ``Owner__display`` (no nested envelope)."""
    row = {"Owner__display": "Bob"}

    assert _extract_value(row, "Owner.Name") == "Bob"


def test_extract_value_returns_missing_for_truly_absent_paths():
    row = {"Owner": {"apiName": "User"}}  # no Name anywhere

    assert _extract_value(row, "Owner.Name") is _MISSING


def test_extract_value_resolves_flat_top_level_keys():
    row = {"StageName": "3 - Engagement"}

    assert _extract_value(row, "StageName") == "3 - Engagement"


# ---------------------------------------------------------------------------
# Calibrator + promotion ledger
# ---------------------------------------------------------------------------


def _build_run_dir(
    tmp_path: Path,
    *,
    snapshot_date: str = "2026-04-30",
    run_id: str = "test-run",
    rows_by_source_id: dict[str, list[dict[str, Any]]],
    plan_items: list[dict[str, Any]],
) -> Path:
    """Construct a minimal monthly-extract run directory for the calibrator."""
    run_dir = tmp_path / "monthly_salesforce_sources" / snapshot_date / run_id
    (run_dir / "plans").mkdir(parents=True, exist_ok=True)
    (run_dir / "raw").mkdir(parents=True, exist_ok=True)
    plan_payload = {
        "snapshot_date": snapshot_date,
        "status": "ok",
        "items": plan_items,
        "findings": [],
    }
    (run_dir / "plans" / "source_requirement_plan.json").write_text(
        json.dumps(plan_payload), encoding="utf-8"
    )
    for source_id, rows in rows_by_source_id.items():
        # Filename follows src-<snapshot>-<...>-<source_id>-<hash>.json
        stem = f"src-{snapshot_date}-fixture-{source_id.lower()}-deadbeef"
        path = run_dir / "raw" / f"{stem}.json"
        path.write_text(
            json.dumps(
                {
                    "snapshot_date": snapshot_date,
                    "metadata": {"run_id": run_id},
                    "source_id": source_id,
                    "rows": rows,
                }
            ),
            encoding="utf-8",
        )
    return run_dir


def _basic_plan_item(*, source_id: str, territory: str = "APAC") -> dict[str, Any]:
    """A plan-item dict matching SourcePlanItem shape, with no distribution_policy.

    Mirrors what historical (pre-Track D) plan files look like — the calibrator
    overlays the current contract onto these to compute share-by-category.
    """
    return {
        "requirement_id": "sd_pipeline_open",
        "source_system": "salesforce",
        "source_type": "salesforce_list_view",
        "salesforce_object": "Opportunity",
        "dataset": "pipeline_open",
        "output_grain": "opportunity",
        "scope": "territory",
        "territory": territory,
        "director": "Test Director",
        "region": "Asia",
        "period_role": "current_quarter",
        "quarter_label": "Q2",
        "quarter_title": "FY26 Q2",
        "source_id": source_id,
        "source_label": "Fixture List View",
        "status": "configured",
        "required_fields": [],
        "row_count_policy": {"allow_zero": True},
        "consumers": [],
        "tags": [],
    }


def _real_contract_path() -> Path:
    """The compiled contract on main carries Track D's first live opt-in."""
    return REPO_ROOT / "config" / "monthly_source_requirements.json"


def test_calibrator_overlay_picks_up_distribution_policy_from_contract(tmp_path: Path):
    """A historical plan with no distribution_policy still calibrates because
    the calibrator overlays the current contract."""
    rows = [
        {"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"},
        {"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"},
        {"StageName": "4 - Shortlisted", "ForecastCategoryName": "Best Case"},
        {"StageName": "5 - Preferred", "ForecastCategoryName": "Commit"},
    ]
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[_basic_plan_item(source_id="00BTb00000FIXAPAC")],
    )
    baselines_dir = tmp_path / "baselines"

    report = calibrate(
        evidence_runs=[run_dir],
        baselines_dir=baselines_dir,
        promote=False,
        requirements_path=_real_contract_path(),
    )

    assert report["candidate_count"] == 1
    assert report["promoted_count"] == 0  # dry-run
    cand = report["candidates"][0]
    assert cand["baseline_key"] == "sd_pipeline_open.apac.current_quarter"
    assert cand["dimension_count"] == 3  # StageName + ForecastCategoryName + Owner.Name
    assert cand["sample_count"] == 4


def test_calibrator_promote_writes_seed_and_appends_ledger(tmp_path: Path):
    rows = [
        {"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"},
        {"StageName": "5 - Preferred", "ForecastCategoryName": "Commit"},
    ]
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[_basic_plan_item(source_id="00BTb00000FIXAPAC")],
    )
    baselines_dir = tmp_path / "baselines"

    report = calibrate(
        evidence_runs=[run_dir],
        baselines_dir=baselines_dir,
        promote=True,
        requirements_path=_real_contract_path(),
        actor="unit-test",
    )

    assert report["promoted_count"] == 1
    seed_path = baselines_dir / "sd_pipeline_open.apac.current_quarter.json"
    assert seed_path.exists()
    seed = json.loads(seed_path.read_text(encoding="utf-8"))
    assert seed["dimensions"]["StageName"]["share_by_category"] == {
        "3 - Engagement": 0.5,
        "5 - Preferred": 0.5,
    }

    ledger_path = baselines_dir / PROMOTION_LEDGER
    assert ledger_path.exists()
    line = ledger_path.read_text(encoding="utf-8").strip().splitlines()[-1]
    entry = json.loads(line)
    assert entry["actor"] == "unit-test"
    assert entry["baseline_keys"] == ["sd_pipeline_open.apac.current_quarter"]
    assert "promoted_at" in entry


def test_calibrator_dry_run_never_writes_to_baselines_dir(tmp_path: Path):
    rows = [{"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"}]
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[_basic_plan_item(source_id="00BTb00000FIXAPAC")],
    )
    baselines_dir = tmp_path / "baselines"

    calibrate(
        evidence_runs=[run_dir],
        baselines_dir=baselines_dir,
        promote=False,
        requirements_path=_real_contract_path(),
    )

    # Read-only by default — dir must not exist (or must be empty).
    assert not baselines_dir.exists() or not list(baselines_dir.glob("*.json"))


def test_calibrator_ledger_is_append_only(tmp_path: Path):
    rows = [{"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"}]
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[_basic_plan_item(source_id="00BTb00000FIXAPAC")],
    )
    baselines_dir = tmp_path / "baselines"

    for _ in range(3):
        calibrate(
            evidence_runs=[run_dir],
            baselines_dir=baselines_dir,
            promote=True,
            requirements_path=_real_contract_path(),
            actor="repeat-test",
        )

    lines = (baselines_dir / PROMOTION_LEDGER).read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    for line in lines:
        json.loads(line)  # each line is a complete JSON object


def test_calibrator_cli_default_is_read_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    rows = [{"StageName": "3 - Engagement", "ForecastCategoryName": "Pipeline"}]
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[_basic_plan_item(source_id="00BTb00000FIXAPAC")],
    )
    baselines_dir = tmp_path / "baselines"

    rc = calibrator_main(
        [
            "--evidence-run",
            str(run_dir),
            "--baselines-dir",
            str(baselines_dir),
            "--requirements",
            str(_real_contract_path()),
        ]
    )

    assert rc == 0
    assert not baselines_dir.exists() or not list(baselines_dir.glob("*.json"))
    out = capsys.readouterr().out
    assert "read-only" in out


def test_calibrator_skips_items_without_distribution_policy_in_contract(tmp_path: Path):
    """A requirement_id that the contract does NOT opt in produces zero candidates."""
    rows = [{"StageName": "3 - Engagement"}]
    item = _basic_plan_item(source_id="00BTb00000FIXAPAC")
    item["requirement_id"] = "sd_historical_trending"  # not opted in
    run_dir = _build_run_dir(
        tmp_path,
        rows_by_source_id={"00BTb00000FIXAPAC": rows},
        plan_items=[item],
    )

    report = calibrate(
        evidence_runs=[run_dir],
        baselines_dir=tmp_path / "baselines",
        promote=False,
        requirements_path=_real_contract_path(),
    )

    assert report["candidate_count"] == 0


# ---------------------------------------------------------------------------
# Run-over-run distribution diff
# ---------------------------------------------------------------------------


def _audit_with_distribution(
    *,
    snapshot_date: str,
    run_id: str,
    sources: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": "monthly_platform.source_extract_quality_audit.v1",
        "snapshot_date": snapshot_date,
        "run_id": run_id,
        "summary": {},
        "sources": [],
        "findings": [],
        "distribution_comparison": {
            "schema_version": "monthly_platform.source_distribution_comparison.v1",
            "matched_source_count": len(sources),
            "no_policy_source_count": 0,
            "missing_seed_source_count": 0,
            "missing_seed_dimension_count": 0,
            "distribution_finding_count": 0,
            "high_finding_count": 0,
            "comparisons": sources,
        },
    }


def _src(
    *,
    source_key: str = "sd_pipeline_open.apac.current_quarter.Q2.00BTb00000FIX",
    row_count: int,
    dim_shares: dict[str, dict[str, float]],
    seed_status_by_dim: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a per-source distribution_comparison payload for diff tests."""
    seed_status_by_dim = seed_status_by_dim or {}
    dimensions = []
    for field, shares in dim_shares.items():
        dimensions.append(
            {
                "field": field,
                "semantic_name": field.lower(),
                "current_shares": shares,
                "seed_status": seed_status_by_dim.get(field, "present"),
                "missing_required_categories": [],
                "disappeared_categories": [],
                "share_drift": [],
                "concentration": None,
                "row_count": row_count,
            }
        )
    return {
        "source_key": source_key,
        "status": "ok",
        "dimensions": dimensions,
        "slice_sentinels": [],
        "seed_present": True,
        "row_count": row_count,
    }


def test_diff_share_deltas_for_unchanged_source():
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                row_count=10,
                dim_shares={
                    "StageName": {
                        "3 - Engagement": 0.50,
                        "4 - Shortlisted": 0.30,
                        "2 - Discovery": 0.20,
                    }
                },
            )
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(
                row_count=12,
                dim_shares={
                    "StageName": {
                        "3 - Engagement": 0.40,
                        "4 - Shortlisted": 0.40,
                        "2 - Discovery": 0.20,
                    }
                },
            )
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    assert diff["schema_version"] == DIFF_SCHEMA
    assert diff["summary"]["compared_source_count"] == 1
    assert diff["summary"]["new_source_count"] == 0
    assert diff["summary"]["dropped_source_count"] == 0
    src_diff = diff["comparisons"][0]
    assert src_diff["presence"] == "both"
    assert src_diff["row_count_delta"] == 2
    stage_diff = src_diff["dimensions"][0]
    deltas_by_cat = {d["category"]: d for d in stage_diff["share_deltas"]}
    assert deltas_by_cat["3 - Engagement"]["abs_delta"] == 0.1
    assert deltas_by_cat["4 - Shortlisted"]["abs_delta"] == 0.1
    assert deltas_by_cat["2 - Discovery"]["abs_delta"] == 0.0


def test_diff_flags_new_and_dropped_categories():
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                row_count=10,
                dim_shares={"StageName": {"2 - Discovery": 0.5, "3 - Engagement": 0.5}},
            )
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(
                row_count=10,
                dim_shares={
                    "StageName": {"3 - Engagement": 0.5, "4 - Shortlisted": 0.5}
                },
            )
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    stage_diff = diff["comparisons"][0]["dimensions"][0]
    assert stage_diff["new_categories"] == ["4 - Shortlisted"]
    assert stage_diff["dropped_categories"] == ["2 - Discovery"]
    assert diff["summary"]["new_category_count"] == 1
    assert diff["summary"]["dropped_category_count"] == 1


def test_diff_flags_new_and_dropped_sources():
    """Sources present in only one run must be visible at the run-level summary."""
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                source_key="sd_pipeline_open.apac.current_quarter.Q1.OLDID",
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
            ),
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(
                source_key="sd_pipeline_open.canada.current_quarter.Q2.NEWID",
                row_count=5,
                dim_shares={"StageName": {"4 - Shortlisted": 1.0}},
            ),
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    presences = sorted(c["presence"] for c in diff["comparisons"])
    assert presences == ["dropped_source", "new_source"]
    assert diff["summary"]["new_source_count"] == 1
    assert diff["summary"]["dropped_source_count"] == 1


def test_diff_flags_seed_status_transition():
    """A dimension going from no_source_seed → present (or missing → present) shows up."""
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
                seed_status_by_dim={"StageName": "no_source_seed"},
            ),
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
                seed_status_by_dim={"StageName": "present"},
            ),
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    assert diff["summary"]["seed_status_changed_count"] == 1
    stage_diff = diff["comparisons"][0]["dimensions"][0]
    assert stage_diff["seed_status_prior"] == "no_source_seed"
    assert stage_diff["seed_status_current"] == "present"
    assert stage_diff["seed_status_changed"] is True


def test_diff_handles_audit_without_distribution_block():
    """A prior audit predating Track D has no distribution_comparison block."""
    prior_pre_d = {
        "snapshot_date": "2026-02-28",
        "run_id": "pre-d",
        "summary": {},
        "sources": [],
    }
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[_src(row_count=5, dim_shares={"StageName": {"3 - Engagement": 1.0}})],
    )

    diff = diff_distribution_audits(prior=prior_pre_d, current=current)

    assert diff["summary"]["new_source_count"] == 1
    assert diff["summary"]["compared_source_count"] == 1


def _no_policy_src(*, source_key: str) -> dict[str, Any]:
    """Build a status=='no_policy' per-source payload (matches audit_distribution)."""
    return {
        "source_key": source_key,
        "status": "no_policy",
        "dimensions": [],
        "slice_sentinels": [],
        "seed_present": False,
        "row_count": 0,
    }


def test_diff_excludes_no_policy_sources_from_indexing():
    """Regression for Codex P2: status=='no_policy' must not inflate diff counters.

    A source that is not opted into Track D appears in the audit's
    ``distribution_comparison.comparisons`` with ``status='no_policy'`` so the
    audit summary can record ``no_policy_source_count``. The run-over-run diff,
    however, only makes sense for opted-in sources — including ``no_policy``
    rows would add noise to ``compared_source_count`` /
    ``new_source_count`` / ``dropped_source_count`` without any signal.
    """
    opted_in = _src(
        source_key="sd_pipeline_open.apac.current_quarter.Q2.OPTED",
        row_count=10,
        dim_shares={"StageName": {"3 - Engagement": 1.0}},
    )
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            opted_in,
            _no_policy_src(
                source_key="sd_historical_trending.apac.current_quarter.Q2.NOPOL"
            ),
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            opted_in,
            _no_policy_src(
                source_key="sd_historical_trending.apac.current_quarter.Q2.NOPOL"
            ),
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    # Only the opted-in source should appear; the no_policy source must be
    # filtered out of both prior_by_key and current_by_key indexes.
    assert diff["summary"]["compared_source_count"] == 1
    assert diff["summary"]["new_source_count"] == 0
    assert diff["summary"]["dropped_source_count"] == 0
    assert all(
        c["source_key"] != "sd_historical_trending.apac.current_quarter.Q2.NOPOL"
        for c in diff["comparisons"]
    )


def test_diff_excludes_no_policy_when_status_changes_across_runs():
    """A source switching from opted-in to no_policy (contract removed) shows
    as 'dropped_source' once, not as a confused 'both' diff with empty dims."""
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                source_key="sd_pipeline_open.apac.current_quarter.Q2.SWITCHING",
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
            ),
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _no_policy_src(
                source_key="sd_pipeline_open.apac.current_quarter.Q2.SWITCHING"
            ),
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    assert diff["summary"]["compared_source_count"] == 1
    assert diff["summary"]["dropped_source_count"] == 1
    assert diff["summary"]["new_source_count"] == 0
    assert diff["comparisons"][0]["presence"] == "dropped_source"


def test_seed_status_changed_only_counts_like_for_like_dimensions():
    """Regression for Codex P2: new/dropped sources must not spike the counter.

    A new source has dimensions with seed_status='present' on the current side
    but no prior side at all — the counter should NOT treat that as a
    transition. Same for dropped sources and dimensions added/removed mid-cycle.
    Only same-source-same-dimension prior→current changes count.
    """
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[
            _src(
                source_key="sd_pipeline_open.apac.current_quarter.Q2.STABLE",
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
                seed_status_by_dim={"StageName": "present"},
            ),
        ],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(
                source_key="sd_pipeline_open.apac.current_quarter.Q2.STABLE",
                row_count=10,
                dim_shares={"StageName": {"3 - Engagement": 1.0}},
                seed_status_by_dim={"StageName": "present"},
            ),
            _src(  # NEW source — has seed_status='present' but no prior side
                source_key="sd_pipeline_open.canada.current_quarter.Q2.NEW",
                row_count=5,
                dim_shares={"StageName": {"4 - Shortlisted": 1.0}},
                seed_status_by_dim={"StageName": "present"},
            ),
        ],
    )

    diff = diff_distribution_audits(prior=prior, current=current)

    # New source contributes 1 to new_source_count but 0 to seed_status_changed
    # because there is no prior dimension to compare against.
    assert diff["summary"]["new_source_count"] == 1
    assert diff["summary"]["seed_status_changed_count"] == 0
    new_source_dim = next(
        c["dimensions"][0] for c in diff["comparisons"] if c["presence"] == "new_source"
    )
    assert new_source_dim["seed_status_prior"] is None
    assert new_source_dim["seed_status_current"] == "present"
    assert new_source_dim["seed_status_changed"] is False


def test_diff_is_pure_does_not_mutate_inputs():
    prior = _audit_with_distribution(
        snapshot_date="2026-03-31",
        run_id="prior",
        sources=[_src(row_count=10, dim_shares={"StageName": {"3 - Engagement": 1.0}})],
    )
    current = _audit_with_distribution(
        snapshot_date="2026-04-30",
        run_id="current",
        sources=[
            _src(row_count=10, dim_shares={"StageName": {"4 - Shortlisted": 1.0}})
        ],
    )
    prior_snapshot = json.dumps(prior, sort_keys=True)
    current_snapshot = json.dumps(current, sort_keys=True)

    diff_distribution_audits(prior=prior, current=current)

    assert json.dumps(prior, sort_keys=True) == prior_snapshot
    assert json.dumps(current, sort_keys=True) == current_snapshot
