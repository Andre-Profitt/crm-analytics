"""Track H — warehouse skeleton tests.

Cover the four acceptance axes called out in
``docs/2026-04-25-gpt-pro-feedback-implementation-plan.md`` Track H:

1. Pathing: ``WarehousePaths`` resolves a deterministic layout under the
   chosen warehouse root for a given (snapshot_date, run_id).
2. Round-trip: every documented table writes a Parquet file at the
   expected path with the documented column order, and DuckDB can read it
   back into the same row count.
3. Manifest: ``warehouse_manifest.json`` records ``schema_version``, the
   list of tables, row counts, byte counts, and sha256 hashes.
4. Parity: row counts in the warehouse match row counts in the JSON
   evidence; the parity report status is ``pass`` for matching inputs and
   ``fail`` when an input's row count is tampered with.

Plus an end-to-end smoke that runs the full ``build_source_backed_warehouse``
CLI against a small hand-crafted plan/audit/registry triple.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from scripts.monthly_platform.warehouse import (
    WarehousePaths,
    build_warehouse,
    compute_parity,
)
from scripts.monthly_platform.warehouse.marts import (
    TABLE_BUILDERS,
    build_mart_director_source_health,
    build_mart_source_run_summary,
    build_staged_distribution_findings,
    build_staged_source_quality_findings,
    track_finding_distribution,
)
from scripts.monthly_platform.warehouse.parity import write_parity_report
from scripts.monthly_platform.warehouse.paths import (
    MANIFEST_FILENAME,
    PARITY_FILENAME,
    TABLE_RELATIVE_PATHS,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_plan() -> dict[str, Any]:
    return {
        "snapshot_date": "2026-04-30",
        "status": "ok",
        "items": [
            {
                "requirement_id": "sd_pipeline_open",
                "source_system": "salesforce",
                "source_type": "salesforce_list_view",
                "salesforce_object": "Opportunity",
                "dataset": "pipeline_open",
                "output_grain": "opportunity",
                "scope": "territory",
                "territory": "APAC",
                "director": "Jesper Tyrer",
                "region": "Asia",
                "period_role": "current_quarter",
                "quarter_label": "Q2",
                "source_id": "00BTb00000FIXAPAC",
                "source_label": "APAC list view",
                "status": "configured",
                "required_fields": [],
                "row_count_policy": {"allow_zero": True},
                "consumers": [],
                "tags": [],
            },
            {
                "requirement_id": "sd_pipeline_open",
                "source_system": "salesforce",
                "source_type": "salesforce_list_view",
                "salesforce_object": "Opportunity",
                "dataset": "pipeline_open",
                "output_grain": "opportunity",
                "scope": "territory",
                "territory": "Canada",
                "director": "Catherine Howard",
                "region": "Americas",
                "period_role": "current_quarter",
                "quarter_label": "Q2",
                "source_id": "00BTb00000FIXCAN",
                "source_label": "Canada list view",
                "status": "configured",
                "required_fields": [],
                "row_count_policy": {"allow_zero": True},
                "consumers": [],
                "tags": [],
            },
        ],
        "findings": [],
    }


@pytest.fixture
def sample_audit() -> dict[str, Any]:
    """Two sources, three findings (one Track B, one Track C, one Track D)."""
    return {
        "schema_version": "monthly_platform.source_extract_quality_audit.v1",
        "snapshot_date": "2026-04-30",
        "run_id": "test-run",
        "generated_at": "2026-04-26T00:00:00+00:00",
        "status": "ok",
        "summary": {
            "selected_source_count": 2,
            "source_count": 2,
            "ok_source_count": 2,
            "warning_source_count": 0,
            "blocked_source_count": 0,
            "finding_count": 3,
            "high_finding_count": 0,
            "medium_finding_count": 1,
            "baseline_drift_finding_count": 1,
            "baseline_high_finding_count": 0,
            "baseline_matched_source_count": 2,
            "baseline_missing_source_count": 0,
            "distribution_finding_count": 1,
            "distribution_high_finding_count": 0,
            "distribution_matched_source_count": 2,
            "distribution_missing_seed_source_count": 0,
            "distribution_missing_seed_dimension_count": 0,
        },
        "sources": [
            {
                "source_key": "sd_pipeline_open.apac.current_quarter.Q2.AAA",
                "status": "ok",
                "requirement_id": "sd_pipeline_open",
                "dataset": "pipeline_open",
                "source_type": "salesforce_list_view",
                "salesforce_id": "AAA",
                "label": "APAC",
                "territory": "APAC",
                "director": "Jesper Tyrer",
                "period_role": "current_quarter",
                "quarter_label": "Q2",
                "row_count": 13,
                "row_count_status": "ok",
                "required_field_count": 11,
                "required_fields_present": ["a", "b"],
                "missing_required_fields": [],
                "finding_count": 0,
                "high_finding_count": 0,
                "medium_finding_count": 0,
                "quality_hash": "deadbeef",
            },
            {
                "source_key": "sd_pipeline_open.canada.current_quarter.Q2.BBB",
                "status": "warning",
                "requirement_id": "sd_pipeline_open",
                "dataset": "pipeline_open",
                "source_type": "salesforce_list_view",
                "salesforce_id": "BBB",
                "label": "Canada",
                "territory": "Canada",
                "director": "Catherine Howard",
                "period_role": "current_quarter",
                "quarter_label": "Q2",
                "row_count": 3,
                "row_count_status": "ok",
                "required_field_count": 11,
                "required_fields_present": ["a"],
                "missing_required_fields": [],
                "finding_count": 1,
                "high_finding_count": 0,
                "medium_finding_count": 1,
                "quality_hash": "feedface",
            },
        ],
        "findings": [
            {
                "severity": "medium",
                "issue": "source_row_count_zero_warning",
                "evidence": "row_count=0",
                "owner": None,
            },
            {
                "severity": "info",
                "issue": "source_quality_baseline_row_count_below_low_threshold",
                "evidence": "row_count=2; expected_min=5",
                "owner": None,
            },
            {
                "severity": "info",
                "issue": "source_distribution_share_drift",
                "evidence": "field=StageName; abs_delta=0.30",
                "owner": None,
            },
        ],
    }


@pytest.fixture
def sample_registry() -> dict[str, Any]:
    """Real compiled contract — the warehouse must work against the live registry."""
    return json.loads(
        (REPO_ROOT / "config" / "monthly_source_requirements.json").read_text(
            encoding="utf-8"
        )
    )


@pytest.fixture
def warehouse_paths(tmp_path: Path) -> WarehousePaths:
    return WarehousePaths(
        repo_root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="test-run",
        warehouse_root=tmp_path / "wh",
    )


# ---------------------------------------------------------------------------
# 1. Pathing
# ---------------------------------------------------------------------------


def test_paths_table_paths_resolve_under_warehouse_root(warehouse_paths):
    for table_id, rel in TABLE_RELATIVE_PATHS.items():
        path = warehouse_paths.table_path(table_id)
        assert path.is_relative_to(warehouse_paths.root)
        assert path.relative_to(warehouse_paths.root).as_posix() == rel


def test_paths_manifest_and_parity_filenames(warehouse_paths):
    assert warehouse_paths.manifest_path.name == MANIFEST_FILENAME
    assert warehouse_paths.parity_report_path.name == PARITY_FILENAME


def test_paths_unknown_table_raises(warehouse_paths):
    with pytest.raises(KeyError, match="unknown warehouse table_id"):
        warehouse_paths.table_path("does_not_exist")


# ---------------------------------------------------------------------------
# 2. Round-trip: each table writes Parquet that DuckDB reads back
# ---------------------------------------------------------------------------


def _read_parquet_row_count(path: Path) -> int:
    con = duckdb.connect()
    try:
        return con.execute(f"SELECT COUNT(*) FROM read_parquet('{path}')").fetchone()[0]
    finally:
        con.close()


def _read_parquet_columns(path: Path) -> list[str]:
    con = duckdb.connect()
    try:
        return [
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}')"
            ).fetchall()
        ]
    finally:
        con.close()


def test_round_trip_all_tables_write_and_read_cleanly(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    manifest = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )

    for table in manifest.tables:
        path = warehouse_paths.root / table.relative_path
        assert path.exists(), f"missing {table.table_id}"
        assert _read_parquet_row_count(path) == table.row_count
        # Documented column order must be respected by the writer.
        assert _read_parquet_columns(path) == table.columns


def test_round_trip_handles_empty_tables(warehouse_paths, sample_plan, sample_registry):
    """Audit with no findings produces empty distribution-findings table."""
    empty_audit = {
        "schema_version": "monthly_platform.source_extract_quality_audit.v1",
        "snapshot_date": "2026-04-30",
        "run_id": "empty",
        "generated_at": "2026-04-26T00:00:00+00:00",
        "status": "ok",
        "summary": {"source_count": 0},
        "sources": [],
        "findings": [],
    }
    manifest = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=empty_audit,
        registry=sample_registry,
    )
    distribution_table = next(
        t for t in manifest.tables if t.table_id == "staged_distribution_findings"
    )
    assert distribution_table.row_count == 0
    path = warehouse_paths.root / distribution_table.relative_path
    assert _read_parquet_row_count(path) == 0
    # Column order is preserved even on the empty table.
    assert _read_parquet_columns(path) == distribution_table.columns


# ---------------------------------------------------------------------------
# 3. Manifest
# ---------------------------------------------------------------------------


def test_manifest_records_each_table_with_hash_and_byte_count(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    manifest = json.loads(warehouse_paths.manifest_path.read_text(encoding="utf-8"))

    assert manifest["schema_version"].startswith("monthly_platform.")
    assert manifest["snapshot_date"] == "2026-04-30"
    assert manifest["run_id"] == "test-run"
    table_ids = {t["table_id"] for t in manifest["tables"]}
    assert table_ids == set(TABLE_RELATIVE_PATHS)
    for table in manifest["tables"]:
        assert isinstance(table["row_count"], int)
        assert isinstance(table["byte_count"], int)
        assert table["byte_count"] > 0
        assert len(table["sha256"]) == 64
        assert table["columns"], f"empty columns for {table['table_id']}"


def test_manifest_is_deterministic_across_two_builds(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    """Same inputs → identical sha256s, byte counts, and row counts."""
    first = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    # Rebuild into a fresh sibling root so the second pass can't read the first.
    second_paths = WarehousePaths(
        repo_root=warehouse_paths.repo_root,
        snapshot_date=warehouse_paths.snapshot_date,
        run_id=warehouse_paths.run_id,
        warehouse_root=warehouse_paths.warehouse_root.parent / "wh2",
    )
    second = build_warehouse(
        paths=second_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    by_id_first = {t.table_id: t for t in first.tables}
    by_id_second = {t.table_id: t for t in second.tables}
    for table_id, t1 in by_id_first.items():
        t2 = by_id_second[table_id]
        assert t1.sha256 == t2.sha256, f"non-deterministic: {table_id}"
        assert t1.row_count == t2.row_count
        assert t1.byte_count == t2.byte_count


# ---------------------------------------------------------------------------
# 4. Parity
# ---------------------------------------------------------------------------


def test_parity_passes_for_matching_inputs(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    manifest = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    report = compute_parity(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
        manifest=manifest.model_dump(),
    )

    assert report.status == "pass"
    failed = [c for c in report.checks if c.status != "pass"]
    assert failed == [], f"unexpected failures: {[c.name for c in failed]}"
    # Track-split parity: 2 non-distribution + 1 distribution = 3 total findings.
    by_name = {c.name: c for c in report.checks}
    assert (
        by_name["staged_distribution_findings vs distribution_* findings"].observed == 1
    )
    assert (
        by_name["staged_source_quality_findings vs non-distribution findings"].observed
        == 2
    )


def test_parity_fails_when_audit_findings_count_diverges(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    """Tamper with the audit's findings list after the build; parity must fail."""
    manifest = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    tampered = {
        **sample_audit,
        "findings": sample_audit["findings"]
        + [
            {
                "severity": "info",
                "issue": "added_after_build",
                "evidence": "x",
                "owner": None,
            }
        ],
    }

    report = compute_parity(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=tampered,
        registry=sample_registry,
        manifest=manifest.model_dump(),
    )

    assert report.status == "fail"
    failed_names = {c.name for c in report.checks if c.status == "fail"}
    assert "all-track findings vs audit.findings" in failed_names


def test_parity_report_is_writable_and_round_trips(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    manifest = build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )
    report = compute_parity(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
        manifest=manifest.model_dump(),
    )
    write_parity_report(warehouse_paths, report)

    payload = json.loads(warehouse_paths.parity_report_path.read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["snapshot_date"] == "2026-04-30"
    assert payload["run_id"] == "test-run"
    assert all(
        "name" in c and "expected" in c and "observed" in c for c in payload["checks"]
    )


# ---------------------------------------------------------------------------
# Mart-level checks (track split, director aggregation)
# ---------------------------------------------------------------------------


def test_findings_split_by_track_prefix(sample_audit):
    quality = build_staged_source_quality_findings(sample_audit)
    distribution = build_staged_distribution_findings(sample_audit)

    assert all(r["track"] != "D" for r in quality)
    assert all(r["track"] == "D" for r in distribution)
    assert len(quality) + len(distribution) == len(sample_audit["findings"])


def test_track_finding_distribution_counts_track_split(sample_audit):
    counts = track_finding_distribution(sample_audit)
    # one Track B (extract-style), one Track C (baseline), one Track D (distribution)
    assert counts["B"] == 1
    assert counts["C"] == 1
    assert counts["D"] == 1


def test_director_health_groups_sources(sample_audit):
    rows = build_mart_director_source_health(sample_audit)
    by_director = {r["director"]: r for r in rows}
    assert set(by_director) == {"Jesper Tyrer", "Catherine Howard"}
    assert by_director["Jesper Tyrer"]["ok_source_count"] == 1
    assert by_director["Catherine Howard"]["warning_source_count"] == 1


def test_run_summary_carries_summary_block(sample_audit):
    rows = build_mart_source_run_summary(sample_audit)
    assert len(rows) == 1
    row = rows[0]
    assert row["run_id"] == "test-run"
    assert row["distribution_finding_count"] == 1
    assert row["baseline_drift_finding_count"] == 1


# ---------------------------------------------------------------------------
# CLI smoke (subprocess)
# ---------------------------------------------------------------------------


def test_cli_builds_warehouse_against_v20c_evidence(tmp_path):
    """End-to-end against the v20c evidence on disk; parity must pass."""
    audit_path = (
        REPO_ROOT
        / "output"
        / "monthly_salesforce_sources"
        / "2026-04-30"
        / "live-all-sources-pipeline-open-v20c"
        / "audits"
        / "source_extract_quality_audit.json"
    )
    if not audit_path.exists():
        pytest.skip("v20c audit evidence not on disk in this environment")

    rc = subprocess.run(
        [
            sys.executable,
            str(REPO_ROOT / "scripts" / "build_source_backed_warehouse.py"),
            "--snapshot-date",
            "2026-04-30",
            "--run-id",
            "live-all-sources-pipeline-open-v20c",
            "--warehouse-root",
            str(tmp_path / "wh"),
        ],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    assert rc.returncode == 0, f"stdout={rc.stdout}\nstderr={rc.stderr}"
    assert "Parity: pass" in rc.stdout
    # Manifest landed and references all 7 tables.
    manifest_path = (
        tmp_path
        / "wh"
        / "2026-04-30"
        / "live-all-sources-pipeline-open-v20c"
        / MANIFEST_FILENAME
    )
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert {t["table_id"] for t in manifest["tables"]} == set(TABLE_BUILDERS)


# ---------------------------------------------------------------------------
# No-side-effect sanity: building the warehouse never mutates evidence inputs
# ---------------------------------------------------------------------------


def test_build_does_not_mutate_inputs(
    warehouse_paths, sample_plan, sample_audit, sample_registry
):
    plan_snapshot = json.dumps(sample_plan, sort_keys=True)
    audit_snapshot = json.dumps(sample_audit, sort_keys=True)
    registry_snapshot = json.dumps(sample_registry, sort_keys=True)

    build_warehouse(
        paths=warehouse_paths,
        plan=sample_plan,
        audit=sample_audit,
        registry=sample_registry,
    )

    assert json.dumps(sample_plan, sort_keys=True) == plan_snapshot
    assert json.dumps(sample_audit, sort_keys=True) == audit_snapshot
    assert json.dumps(sample_registry, sort_keys=True) == registry_snapshot
