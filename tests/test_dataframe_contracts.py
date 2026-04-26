"""Track I — dataframe contract tests (first slice: ``raw_source_quality_audit``).

Two-tier validation coverage:

1. Positive control (``good.json``) — both Pandera and Frictionless must
   accept it without findings.
2. Six hand-crafted negative-control fixtures — each isolates one
   intentional defect, and the test asserts that the right tier rejects
   it with a finding pointing at the defect.

The Frictionless tier is the runtime check downstream consumers (BI
tooling, Excel, future deck contract) would use; the Pandera tier is
the in-process check the warehouse builder will eventually call before
each Parquet write. Both should agree on whether a row is valid; the
tests treat the two tiers as a redundancy gate.

Live-evidence smoke
-------------------
``test_pandera_accepts_v20d_checkpoint_warehouse`` builds the warehouse
fresh into a tmp_path and validates the resulting
``raw/source_quality_audit.parquet`` against the live Pandera schema.
This guards against schema drift between the v20c-anchored schema and
the warehouse writer's actual output.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import pytest

from scripts.monthly_platform.dataframe_contracts import (
    validate_frictionless,
    validate_pandera,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "raw_source_quality_audit"
)
TABLE_ID = "raw_source_quality_audit"

# Column order must match the Pandera schema's strict-mode declaration so
# DataFrames built from the JSON fixtures present columns in the same
# order the warehouse writer emits.
WAREHOUSE_COLUMNS = [
    "snapshot_date",
    "run_id",
    "source_key",
    "status",
    "requirement_id",
    "dataset",
    "source_type",
    "salesforce_id",
    "label",
    "territory",
    "director",
    "period_role",
    "quarter_label",
    "row_count",
    "row_count_status",
    "required_field_count",
    "required_fields_present_count",
    "missing_required_fields_count",
    "finding_count",
    "high_finding_count",
    "medium_finding_count",
    "quality_hash",
]


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _load_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    # Reindex against the warehouse column order so missing columns show
    # up as NaN (the schema's nullable check then surfaces them) rather
    # than silently breaking the strict-mode column-set check.
    return df.reindex(columns=[c for c in WAREHOUSE_COLUMNS if c in df.columns])


# ---------------------------------------------------------------------------
# Positive control
# ---------------------------------------------------------------------------


def test_pandera_accepts_good_fixture():
    df = _load_fixture("good")
    report = validate_pandera(table_id=TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


# ---------------------------------------------------------------------------
# Negative controls (Pandera tier)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("wrong_quality_hash", "quality_hash"),
        ("negative_row_count", "row_count"),
        ("unknown_status", "status"),
        ("bad_period_role", "period_role"),
    ],
)
def test_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_fixture(fixture)
    report = validate_pandera(table_id=TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), (
        f"{fixture}: expected a finding mentioning {expected_substring!r}; "
        f"got: {[(f.message, f.evidence) for f in report.findings]}"
    )


def test_pandera_rejects_missing_source_key():
    df = _load_fixture("missing_source_key")
    report = validate_pandera(table_id=TABLE_ID, df=df)
    assert report.status == "fail"


def test_pandera_rejects_duplicate_source_key():
    df = _load_fixture("duplicate_source_key")
    report = validate_pandera(table_id=TABLE_ID, df=df)
    assert report.status == "fail"
    # Pandera reports the duplicate-value column in failure_cases.
    assert any("source_key" in (f.evidence or f.message) for f in report.findings)


# ---------------------------------------------------------------------------
# Frictionless tier (subset — equivalence with Pandera)
# ---------------------------------------------------------------------------


def _write_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    """Materialize a fixture as Parquet so Frictionless can read it from disk."""
    df = _load_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def test_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _write_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=TABLE_ID, parquet_path=parquet_path, repo_root=REPO_ROOT
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_frictionless_rejects_unknown_status_enum(tmp_path: Path):
    parquet_path = _write_parquet_from_fixture("unknown_status", tmp_path)
    report = validate_frictionless(
        table_id=TABLE_ID, parquet_path=parquet_path, repo_root=REPO_ROOT
    )
    assert report.status == "fail"


def test_frictionless_rejects_missing_required_column(tmp_path: Path):
    parquet_path = _write_parquet_from_fixture("missing_source_key", tmp_path)
    report = validate_frictionless(
        table_id=TABLE_ID, parquet_path=parquet_path, repo_root=REPO_ROOT
    )
    assert report.status == "fail"


def test_frictionless_returns_explicit_finding_for_missing_schema(tmp_path: Path):
    parquet_path = _write_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id="does_not_exist",
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"
    assert any("missing Frictionless schema" in f.message for f in report.findings)


# ---------------------------------------------------------------------------
# Live-evidence smoke (Pandera against fresh warehouse build)
# ---------------------------------------------------------------------------


def test_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Build the warehouse against the on-disk v20c run; validate the live
    ``raw/source_quality_audit.parquet`` passes Pandera.

    Catches schema drift between the v20c-anchored Pandera schema and what
    the warehouse writer actually emits. Skips if the v20c evidence isn't
    on this machine (CI without it should still let unit tests pass).
    """
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
        pytest.skip("v20c live evidence not on disk")

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
    assert rc.returncode == 0, rc.stderr or rc.stdout

    parquet_path = (
        tmp_path
        / "wh"
        / "2026-04-30"
        / "live-all-sources-pipeline-open-v20c"
        / "raw"
        / "source_quality_audit.parquet"
    )
    assert parquet_path.exists()

    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()

    report = validate_pandera(table_id=TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


# ---------------------------------------------------------------------------
# Sanity: unknown table_id surfaces a clean error rather than crashing
# ---------------------------------------------------------------------------


def test_pandera_unknown_table_id_returns_finding():
    df = _load_fixture("good")
    report = validate_pandera(table_id="does_not_exist", df=df)
    assert report.status == "fail"
    assert any("no Pandera schema registered" in f.message for f in report.findings)


# ===========================================================================
# Slice 2: staged_source_quality_findings
# ===========================================================================
#
# Same template as slice 1 — strict Pandera schema, paired Frictionless
# Table Schema, hand-crafted negative controls (one defect per file),
# positive control matching the v20d checkpoint shape, and a live
# warehouse smoke. The table-specific helpers shadow the
# raw_source_quality_audit constants by suffixing them.


FINDINGS_FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "staged_source_quality_findings"
)
FINDINGS_TABLE_ID = "staged_source_quality_findings"

# Mirror of marts.STAGED_FINDING_COLUMNS in column-order from
# scripts/monthly_platform/warehouse/marts.py.
FINDINGS_COLUMNS = [
    "snapshot_date",
    "run_id",
    "track",
    "severity",
    "issue",
    "evidence",
    "owner",
]


def _load_findings_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (FINDINGS_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    return df.reindex(columns=[c for c in FINDINGS_COLUMNS if c in df.columns])


def _findings_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_findings_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


# --- Positive control ------------------------------------------------------


def test_findings_pandera_accepts_good_fixture():
    df = _load_findings_fixture("good")
    report = validate_pandera(table_id=FINDINGS_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


# --- Pandera negative controls --------------------------------------------


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("unknown_track", "track"),
        ("unknown_severity", "severity"),
        ("non_source_namespace_issue", "issue"),
        ("issue_with_uppercase", "issue"),
        ("issue_with_whitespace", "issue"),
        ("bad_snapshot_date", "snapshot_date"),
    ],
)
def test_findings_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_findings_fixture(fixture)
    report = validate_pandera(table_id=FINDINGS_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), (
        f"{fixture}: expected a finding mentioning {expected_substring!r}; "
        f"got: {[(f.message, f.evidence) for f in report.findings]}"
    )


def test_findings_pandera_rejects_distribution_track_in_quality_table():
    """Cross-table contract: track='D' must be rejected here.

    Track D distribution findings live in ``staged_distribution_findings``.
    The two staged tables are mutually exclusive on the ``track`` column;
    if this fixture passed validation the warehouse could silently route a
    distribution finding into the wrong table.
    """
    df = _load_findings_fixture("distribution_track_in_quality_table")
    report = validate_pandera(table_id=FINDINGS_TABLE_ID, df=df)
    assert report.status == "fail"
    assert any("track" in (f.evidence or f.message) for f in report.findings)


def test_findings_pandera_rejects_missing_run_id():
    df = _load_findings_fixture("missing_run_id")
    report = validate_pandera(table_id=FINDINGS_TABLE_ID, df=df)
    assert report.status == "fail"


# --- Frictionless tier (subset) -------------------------------------------


def test_findings_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _findings_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=FINDINGS_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_findings_frictionless_rejects_unknown_track(tmp_path: Path):
    parquet_path = _findings_parquet_from_fixture("unknown_track", tmp_path)
    report = validate_frictionless(
        table_id=FINDINGS_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_findings_frictionless_rejects_distribution_track(tmp_path: Path):
    parquet_path = _findings_parquet_from_fixture(
        "distribution_track_in_quality_table", tmp_path
    )
    report = validate_frictionless(
        table_id=FINDINGS_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


# --- Live-evidence smoke ---------------------------------------------------


def test_findings_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Build the warehouse fresh against on-disk v20c evidence; the live
    ``staged/source_quality_findings.parquet`` must pass the Pandera schema.

    This catches drift between the schema's enums / regex and what the
    warehouse writer actually emits when the audit has real findings
    routed via ``marts._track_for_issue``. Skips if v20c evidence isn't
    on disk in this environment.
    """
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
        pytest.skip("v20c live evidence not on disk")

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
    assert rc.returncode == 0, rc.stderr or rc.stdout

    parquet_path = (
        tmp_path
        / "wh"
        / "2026-04-30"
        / "live-all-sources-pipeline-open-v20c"
        / "staged"
        / "source_quality_findings.parquet"
    )
    assert parquet_path.exists()

    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()

    report = validate_pandera(table_id=FINDINGS_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]
