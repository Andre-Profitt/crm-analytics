"""Track C — source-quality baseline loader, comparator, and calibrator tests.

Covers the five Track C modes called out in
``docs/2026-04-25-gpt-pro-feedback-implementation-plan.md``:

1. **normal**     — observed quality matches the baseline → no findings.
2. **low-row**    — row count below the calibrated low threshold → drift finding.
3. **high-row**   — row count above the calibrated high threshold → drift finding.
4. **missing-baseline** — no baseline file present → comparator emits zero findings
                          (read-only first; missing baseline is never a release blocker).
5. **promote**    — calibrator only writes to disk when ``--promote-baselines`` is passed.

Plus the "comparator never mutates" guarantee and the per-source contract
override path that escalates baseline drift severity.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from scripts.calibrate_source_quality_baselines import (
    calibrate,
    main as calibrator_main,
)
from scripts.monthly_platform.contracts import Finding
from scripts.monthly_platform.source_quality_baselines import (
    BaselinePolicy,
    BaselineRowCount,
    SourceQualityBaseline,
    baseline_filename,
    baseline_key_for_quality,
    compare_quality_to_baseline,
    compare_run_to_baselines,
    derive_baseline,
    load_baselines,
    write_baseline,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _quality_source(
    *,
    requirement_id: str = "sd_pipeline_open",
    territory: str = "APAC",
    period_role: str = "current_quarter",
    quarter_label: str = "Q2",
    row_count: int = 13,
    salesforce_id: str = "00BTb00000LJNODMA5",
    field_audits: list[dict[str, Any]] | None = None,
    status: str = "ok",
    row_count_status: str = "ok",
) -> dict[str, Any]:
    return {
        "source_key": (
            f"{requirement_id}.{territory.lower()}."
            f"{period_role}.{quarter_label}.{salesforce_id}"
        ),
        "status": status,
        "requirement_id": requirement_id,
        "dataset": "pipeline_open",
        "source_type": "salesforce_list_view",
        "salesforce_id": salesforce_id,
        "label": "SD Monthly Pipeline Open APAC",
        "territory": territory,
        "director": "Test Director",
        "period_role": period_role,
        "quarter_label": quarter_label,
        "row_count": row_count,
        "row_count_status": row_count_status,
        "row_count_policy": {"allow_zero": True},
        "required_field_count": 2,
        "required_fields_present": ["Name", "StageName"],
        "missing_required_fields": [],
        "field_audits": field_audits
        or [
            {
                "field_name": "Name",
                "present": True,
                "null_count": 0,
                "null_pct": 0.0,
                "semantic_name": "opportunity",
            },
            {
                "field_name": "StageName",
                "present": True,
                "null_count": 0,
                "null_pct": 0.0,
                "semantic_name": "stage",
            },
        ],
        "finding_count": 0,
        "high_finding_count": 0,
        "medium_finding_count": 0,
        "quality_hash": "deadbeef",
    }


def _baseline_with_envelope(
    *,
    expected_min: float = 5.0,
    expected_max: float = 20.0,
    row_count_drift_action: str = "info",
    null_rate_drift_action: str = "info",
    null_rates: dict[str, dict[str, Any]] | None = None,
) -> SourceQualityBaseline:
    null_rates = null_rates or {
        "Name": {"baseline_pct": 0.0, "max_pct": 0.05, "samples": [0.0]},
        "StageName": {"baseline_pct": 0.0, "max_pct": 0.05, "samples": [0.0]},
    }
    return SourceQualityBaseline(
        baseline_key="sd_pipeline_open.apac.current_quarter",
        requirement_id="sd_pipeline_open",
        territory="APAC",
        period_role="current_quarter",
        promoted_at="2026-04-25T00:00:00+00:00",
        promoted_from=[
            {
                "run_id": "test-baseline-1",
                "snapshot_date": "2026-03-31",
                "quarter_label": "Q1",
                "salesforce_id": "00BTb00000XYZ",
                "row_count": 12,
            },
            {
                "run_id": "test-baseline-2",
                "snapshot_date": "2026-04-30",
                "quarter_label": "Q2",
                "salesforce_id": "00BTb00000XYZ",
                "row_count": 13,
            },
        ],
        row_count=BaselineRowCount(
            median=12.5,
            p05=12.0,
            p95=13.0,
            min_observed=12,
            max_observed=13,
            expected_min=expected_min,
            expected_max=expected_max,
            sample_count=2,
            allow_zero=True,
        ),
        null_rates=null_rates,  # type: ignore[arg-type]
        policy=BaselinePolicy(
            row_count_drift_action=row_count_drift_action,  # type: ignore[arg-type]
            null_rate_drift_action=null_rate_drift_action,  # type: ignore[arg-type]
        ),
    )


def _quality_audit(sources: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": "monthly_platform.source_extract_quality_audit.v1",
        "generated_at": "2026-04-25T00:00:00+00:00",
        "status": "ok",
        "snapshot_date": "2026-04-30",
        "run_id": "test-run",
        "dry_run": False,
        "summary": {},
        "sources": sources,
        "findings": [],
    }


# ---------------------------------------------------------------------------
# Mode 1 — normal: row count inside envelope, null rates clean
# ---------------------------------------------------------------------------


def test_compare_normal_row_count_inside_envelope_emits_no_findings():
    baseline = _baseline_with_envelope(expected_min=5.0, expected_max=20.0)
    quality = _quality_source(row_count=13)

    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert findings == []


# ---------------------------------------------------------------------------
# Mode 2 — low-row drift
# ---------------------------------------------------------------------------


def test_compare_low_row_below_low_factor_emits_info_finding_by_default():
    # expected_min=10 * low_factor=0.5 → low_threshold=5; row_count=4 breaches.
    baseline = _baseline_with_envelope(expected_min=10.0, expected_max=20.0)
    quality = _quality_source(row_count=4)

    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert len(findings) == 1
    finding = findings[0]
    assert finding.severity == "info"
    assert "below_low_threshold" in finding.issue
    assert "row_count=4" in finding.evidence


def test_compare_contract_override_blocked_escalates_low_row_to_high():
    baseline = _baseline_with_envelope(expected_min=10.0, expected_max=20.0)
    quality = _quality_source(row_count=4)

    findings = compare_quality_to_baseline(
        quality=quality, baseline=baseline, contract_override="blocked"
    )

    assert len(findings) == 1
    assert findings[0].severity == "high"


def test_compare_contract_override_ok_silences_drift_entirely():
    baseline = _baseline_with_envelope(expected_min=10.0, expected_max=20.0)
    quality = _quality_source(row_count=4)

    findings = compare_quality_to_baseline(
        quality=quality, baseline=baseline, contract_override="ok"
    )

    assert findings == []


# ---------------------------------------------------------------------------
# Mode 3 — high-row drift
# ---------------------------------------------------------------------------


def test_compare_high_row_above_high_factor_emits_info_finding_by_default():
    # expected_max=20 * high_factor=2.0 → high_threshold=40; row_count=50 breaches.
    baseline = _baseline_with_envelope(expected_min=5.0, expected_max=20.0)
    quality = _quality_source(row_count=50)

    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert len(findings) == 1
    assert findings[0].severity == "info"
    assert "above_high_threshold" in findings[0].issue


# ---------------------------------------------------------------------------
# Mode 4 — missing baseline: comparator must emit zero findings
# ---------------------------------------------------------------------------


def test_compare_run_with_no_baselines_loaded_emits_no_findings(tmp_path: Path):
    audit = _quality_audit([_quality_source()])
    baselines = load_baselines(tmp_path)  # empty directory → {}

    findings, summary = compare_run_to_baselines(
        quality_audit=audit, baselines=baselines
    )

    assert baselines == {}
    assert findings == []
    assert summary["matched_source_count"] == 0
    assert summary["missing_baseline_source_count"] == 1
    assert summary["drift_finding_count"] == 0


def test_load_baselines_returns_empty_for_missing_directory(tmp_path: Path):
    missing = tmp_path / "does_not_exist"

    assert load_baselines(missing) == {}


# ---------------------------------------------------------------------------
# Mode 5 — promote: calibrator only writes when --promote-baselines is set
# ---------------------------------------------------------------------------


def _write_audit_fixture(path: Path, sources: list[dict[str, Any]]) -> Path:
    payload = _quality_audit(sources)
    payload["run_id"] = "test-promote-run"
    payload["snapshot_date"] = "2026-04-30"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_calibrate_dry_run_does_not_write_baselines(tmp_path: Path):
    audit_path = _write_audit_fixture(
        tmp_path / "audit.json", [_quality_source(row_count=13)]
    )
    baselines_dir = tmp_path / "baselines"

    report = calibrate(
        evidence_paths=[audit_path],
        baselines_dir=baselines_dir,
        promote=False,
    )

    assert report["candidate_count"] == 1
    assert report["promote"] is False
    assert report["promoted_count"] == 0
    assert not baselines_dir.exists() or not list(baselines_dir.glob("*.json"))


def test_calibrate_promote_writes_baseline_files(tmp_path: Path):
    audit_path = _write_audit_fixture(
        tmp_path / "audit.json", [_quality_source(row_count=13)]
    )
    baselines_dir = tmp_path / "baselines"

    report = calibrate(
        evidence_paths=[audit_path],
        baselines_dir=baselines_dir,
        promote=True,
    )

    assert report["promoted_count"] == 1
    written = list(baselines_dir.glob("*.json"))
    assert len(written) == 1
    payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert payload["baseline_key"] == "sd_pipeline_open.apac.current_quarter"
    assert payload["row_count"]["median"] == 13.0


def test_calibrator_cli_default_is_read_only(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    audit_path = _write_audit_fixture(
        tmp_path / "audit.json", [_quality_source(row_count=13)]
    )
    baselines_dir = tmp_path / "baselines"

    rc = calibrator_main(
        [
            "--evidence",
            str(audit_path),
            "--baselines-dir",
            str(baselines_dir),
        ]
    )

    assert rc == 0
    assert not baselines_dir.exists() or not list(baselines_dir.glob("*.json"))
    captured = capsys.readouterr().out
    assert "read-only" in captured


def test_calibrator_cli_promote_baselines_writes_files(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
):
    audit_path = _write_audit_fixture(
        tmp_path / "audit.json", [_quality_source(row_count=13)]
    )
    baselines_dir = tmp_path / "baselines"

    rc = calibrator_main(
        [
            "--evidence",
            str(audit_path),
            "--baselines-dir",
            str(baselines_dir),
            "--promote-baselines",
        ]
    )

    assert rc == 0
    assert len(list(baselines_dir.glob("*.json"))) == 1
    captured = capsys.readouterr().out
    assert "PROMOTED" in captured


# ---------------------------------------------------------------------------
# Multi-observation calibration (n>=2 produces a real envelope)
# ---------------------------------------------------------------------------


def test_calibrate_multiple_observations_produces_expected_envelope(tmp_path: Path):
    audit_a = _quality_audit([_quality_source(row_count=10, quarter_label="Q1")])
    audit_a["run_id"] = "test-a"
    audit_b = _quality_audit([_quality_source(row_count=20, quarter_label="Q2")])
    audit_b["run_id"] = "test-b"
    path_a = tmp_path / "a.json"
    path_b = tmp_path / "b.json"
    path_a.write_text(json.dumps(audit_a), encoding="utf-8")
    path_b.write_text(json.dumps(audit_b), encoding="utf-8")
    baselines_dir = tmp_path / "baselines"

    calibrate(
        evidence_paths=[path_a, path_b],
        baselines_dir=baselines_dir,
        promote=True,
    )

    written = list(baselines_dir.glob("*.json"))
    assert len(written) == 1
    payload = json.loads(written[0].read_text(encoding="utf-8"))
    assert payload["row_count"]["sample_count"] == 2
    assert payload["row_count"]["min_observed"] == 10
    assert payload["row_count"]["max_observed"] == 20
    # min(10) * 0.5 = 5.0; max(20) * 2.0 = 40.0
    assert payload["row_count"]["expected_min"] == 5.0
    assert payload["row_count"]["expected_max"] == 40.0


def test_calibrated_envelope_catches_row_count_just_below_floor(tmp_path: Path):
    """Regression: prior implementation applied the low-factor twice.

    With observations=[100, 110] and the default low/high envelope factors, the
    calibrator stores ``expected_min=50`` and ``expected_max=220`` as final
    release thresholds. row_count=49 must emit a drift finding; the prior
    double-factor bug would have set the effective floor to 25 and let 49
    through silently.
    """
    audit_a = _quality_audit([_quality_source(row_count=100, quarter_label="Q1")])
    audit_a["run_id"] = "two-sample-a"
    audit_b = _quality_audit([_quality_source(row_count=110, quarter_label="Q2")])
    audit_b["run_id"] = "two-sample-b"
    path_a = tmp_path / "a.json"
    path_b = tmp_path / "b.json"
    path_a.write_text(json.dumps(audit_a), encoding="utf-8")
    path_b.write_text(json.dumps(audit_b), encoding="utf-8")
    baselines_dir = tmp_path / "baselines"

    calibrate(
        evidence_paths=[path_a, path_b], baselines_dir=baselines_dir, promote=True
    )
    baselines = load_baselines(baselines_dir)
    baseline = next(iter(baselines.values()))
    assert baseline.row_count.expected_min == 50.0
    assert baseline.row_count.expected_max == 220.0

    quality = _quality_source(row_count=49)
    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert len(findings) == 1
    assert "below_low_threshold" in findings[0].issue
    assert findings[0].severity == "info"


def test_calibrated_envelope_catches_row_count_just_above_ceiling(tmp_path: Path):
    """Regression for the high-side double-factor bug.

    With observations=[100, 110]: ``expected_max = 110 * 2.0 = 220``.
    row_count=221 must emit drift; the prior bug effectively raised the
    ceiling to 440 and silently accepted >2x deviations.
    """
    audit_a = _quality_audit([_quality_source(row_count=100, quarter_label="Q1")])
    audit_a["run_id"] = "two-sample-a"
    audit_b = _quality_audit([_quality_source(row_count=110, quarter_label="Q2")])
    audit_b["run_id"] = "two-sample-b"
    path_a = tmp_path / "a.json"
    path_b = tmp_path / "b.json"
    path_a.write_text(json.dumps(audit_a), encoding="utf-8")
    path_b.write_text(json.dumps(audit_b), encoding="utf-8")
    baselines_dir = tmp_path / "baselines"

    calibrate(
        evidence_paths=[path_a, path_b], baselines_dir=baselines_dir, promote=True
    )
    baseline = next(iter(load_baselines(baselines_dir).values()))
    assert baseline.row_count.expected_max == 220.0

    quality = _quality_source(row_count=221)
    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert len(findings) == 1
    assert "above_high_threshold" in findings[0].issue
    assert findings[0].severity == "info"


def test_calibrated_envelope_inside_bounds_emits_no_findings(tmp_path: Path):
    """Boundary control for the two prior tests: 50/220 are inclusive."""
    audit_a = _quality_audit([_quality_source(row_count=100, quarter_label="Q1")])
    audit_a["run_id"] = "two-sample-a"
    audit_b = _quality_audit([_quality_source(row_count=110, quarter_label="Q2")])
    audit_b["run_id"] = "two-sample-b"
    (tmp_path / "a.json").write_text(json.dumps(audit_a), encoding="utf-8")
    (tmp_path / "b.json").write_text(json.dumps(audit_b), encoding="utf-8")
    baselines_dir = tmp_path / "baselines"

    calibrate(
        evidence_paths=[tmp_path / "a.json", tmp_path / "b.json"],
        baselines_dir=baselines_dir,
        promote=True,
    )
    baseline = next(iter(load_baselines(baselines_dir).values()))

    for boundary in (50, 100, 110, 220):
        findings = compare_quality_to_baseline(
            quality=_quality_source(row_count=boundary), baseline=baseline
        )
        assert findings == [], f"unexpected drift at boundary row_count={boundary}"


def test_calibrate_excludes_blocked_quality_observations(tmp_path: Path):
    audit = _quality_audit(
        [
            _quality_source(row_count=10),
            _quality_source(
                row_count=0,
                status="blocked",
                row_count_status="blocked",
                quarter_label="Q3",
            ),
        ]
    )
    audit_path = tmp_path / "audit.json"
    audit_path.write_text(json.dumps(audit), encoding="utf-8")
    baselines_dir = tmp_path / "baselines"

    calibrate(evidence_paths=[audit_path], baselines_dir=baselines_dir, promote=True)

    payload = json.loads(next(baselines_dir.glob("*.json")).read_text(encoding="utf-8"))
    # Blocked observation must not poison the calibrated baseline.
    assert payload["row_count"]["sample_count"] == 1
    assert payload["row_count"]["min_observed"] == 10


# ---------------------------------------------------------------------------
# Comparator purity: no mutations of input dicts or baseline objects
# ---------------------------------------------------------------------------


def test_comparator_never_mutates_inputs():
    baseline = _baseline_with_envelope(expected_min=5.0, expected_max=20.0)
    quality = _quality_source(row_count=4)
    quality_snapshot = json.dumps(quality, sort_keys=True)
    baseline_snapshot = baseline.model_dump_json()

    compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert json.dumps(quality, sort_keys=True) == quality_snapshot
    assert baseline.model_dump_json() == baseline_snapshot


# ---------------------------------------------------------------------------
# Null-rate drift
# ---------------------------------------------------------------------------


def test_null_rate_above_max_pct_emits_info_finding_by_default():
    baseline = _baseline_with_envelope(
        expected_min=5.0,
        expected_max=20.0,
        null_rates={
            "StageName": {"baseline_pct": 0.0, "max_pct": 0.05, "samples": [0.0]}
        },
    )
    quality = _quality_source(
        row_count=10,
        field_audits=[
            {
                "field_name": "Name",
                "present": True,
                "null_count": 0,
                "null_pct": 0.0,
                "semantic_name": "opportunity",
            },
            {
                "field_name": "StageName",
                "present": True,
                "null_count": 5,
                "null_pct": 0.5,  # 50% null vs 5% ceiling
                "semantic_name": "stage",
            },
        ],
    )

    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    assert len(findings) == 1
    assert findings[0].severity == "info"
    assert "null_rate_drift" in findings[0].issue
    assert "field=StageName" in findings[0].evidence


def test_null_rate_drift_skipped_when_row_count_zero():
    baseline = _baseline_with_envelope(
        expected_min=5.0,
        expected_max=20.0,
        null_rates={
            "StageName": {"baseline_pct": 0.0, "max_pct": 0.05, "samples": [0.0]}
        },
    )
    quality = _quality_source(row_count=0, field_audits=[])

    findings = compare_quality_to_baseline(quality=quality, baseline=baseline)

    # With baseline.expected_min=5 and row_count=0, only the row-count drift
    # axis fires (default low_factor=0.5 → low_threshold=2.5; 0 < 2.5).
    assert all("null_rate_drift" not in f.issue for f in findings)


# ---------------------------------------------------------------------------
# Baseline key + filename
# ---------------------------------------------------------------------------


def test_baseline_key_for_quality_drops_quarter_and_salesforce_id():
    quality = _quality_source(quarter_label="Q3", salesforce_id="00BTb00000DIFFERENT")

    key = baseline_key_for_quality(quality)

    assert key == "sd_pipeline_open.apac.current_quarter"
    assert baseline_filename(key) == "sd_pipeline_open.apac.current_quarter.json"


def test_baseline_key_normalizes_territory_with_ampersand_and_spaces():
    quality = _quality_source(territory="Middle East & Africa")

    key = baseline_key_for_quality(quality)

    assert key == "sd_pipeline_open.middle_east_and_africa.current_quarter"


# ---------------------------------------------------------------------------
# Loader sanity
# ---------------------------------------------------------------------------


def test_load_baselines_round_trips_a_written_baseline(tmp_path: Path):
    baseline = _baseline_with_envelope()
    baselines_dir = tmp_path / "baselines"

    write_baseline(baselines_dir, baseline)
    loaded = load_baselines(baselines_dir)

    assert set(loaded.keys()) == {baseline.baseline_key}
    assert loaded[baseline.baseline_key].row_count.median == baseline.row_count.median


def test_load_baselines_rejects_duplicate_keys(tmp_path: Path):
    baselines_dir = tmp_path / "baselines"
    baselines_dir.mkdir()
    payload = _baseline_with_envelope().model_dump(mode="json")
    (baselines_dir / "a.json").write_text(json.dumps(payload), encoding="utf-8")
    (baselines_dir / "b.json").write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate baseline_key"):
        load_baselines(baselines_dir)


# ---------------------------------------------------------------------------
# derive_baseline single-sample envelope is conservative (None, None)
# ---------------------------------------------------------------------------


def test_derive_baseline_single_sample_leaves_envelope_none():
    obs = [
        {
            "run_id": "single",
            "snapshot_date": "2026-04-30",
            "row_count": 13,
            "row_count_policy": {"allow_zero": True},
            "field_audits": [
                {
                    "field_name": "Name",
                    "present": True,
                    "null_count": 0,
                    "null_pct": 0.0,
                },
            ],
        }
    ]

    baseline = derive_baseline(
        baseline_key="x.y.current_quarter",
        requirement_id="x",
        territory="Y",
        period_role="current_quarter",
        observations=obs,
    )

    assert baseline.row_count.expected_min is None
    assert baseline.row_count.expected_max is None
    assert baseline.row_count.sample_count == 1


# ---------------------------------------------------------------------------
# Whole-run comparator + summary shape
# ---------------------------------------------------------------------------


def test_compare_run_to_baselines_produces_per_source_comparison_block(tmp_path: Path):
    baseline = _baseline_with_envelope(expected_min=10.0, expected_max=20.0)
    write_baseline(tmp_path, baseline)
    audit = _quality_audit(
        [
            _quality_source(row_count=13),  # in envelope → ok
            _quality_source(row_count=4, quarter_label="Q3"),  # below → drift
        ]
    )

    baselines = load_baselines(tmp_path)
    findings, summary = compare_run_to_baselines(
        quality_audit=audit, baselines=baselines
    )

    assert summary["matched_source_count"] == 2
    assert summary["missing_baseline_source_count"] == 0
    assert summary["drift_finding_count"] == 1
    assert summary["info_finding_count"] == 1
    assert len(findings) == 1
    assert all(isinstance(f, Finding) for f in findings)
    statuses = [c["status"] for c in summary["comparisons"]]
    assert sorted(statuses) == ["drift", "ok"]
