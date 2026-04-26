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


# ===========================================================================
# Slice 3: staged_distribution_findings
# ===========================================================================
#
# Track D companion to slice 2. Same column shape, opposite routing rule.
# The schema requires ``track == "D"`` and the issue regex
# ``^source_distribution_[a-z0-9_]+$`` (digits allowed for sentinel /
# slice identifiers). Together with slice 2 these two schemas enforce the
# cross-table contract — a row that passes both is impossible.


DIST_FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "staged_distribution_findings"
)
DIST_TABLE_ID = "staged_distribution_findings"


def _load_dist_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (DIST_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    return df.reindex(columns=[c for c in FINDINGS_COLUMNS if c in df.columns])


def _dist_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_dist_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


# --- Positive control ------------------------------------------------------


def test_dist_pandera_accepts_good_fixture():
    df = _load_dist_fixture("good")
    report = validate_pandera(table_id=DIST_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


# --- Cross-table contract: B / C must be rejected here -------------------


@pytest.mark.parametrize(
    "fixture",
    [
        "track_b_in_distribution_table",
        "track_c_in_distribution_table",
        "unknown_track",
    ],
)
def test_dist_pandera_rejects_non_d_track(fixture):
    """Track D table accepts only ``track == "D"``; B / C / anything else
    is a routing error and must be caught by the schema."""
    df = _load_dist_fixture(fixture)
    report = validate_pandera(table_id=DIST_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    assert any("track" in (f.evidence or f.message) for f in report.findings), [
        (f.message, f.evidence) for f in report.findings
    ]


# --- Pandera negative controls --------------------------------------------


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("unknown_severity", "severity"),
        ("non_distribution_issue", "issue"),
        ("issue_with_uppercase", "issue"),
        ("issue_with_whitespace", "issue"),
        ("bad_snapshot_date", "snapshot_date"),
    ],
)
def test_dist_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_dist_fixture(fixture)
    report = validate_pandera(table_id=DIST_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


def test_dist_pandera_rejects_missing_run_id():
    df = _load_dist_fixture("missing_run_id")
    report = validate_pandera(table_id=DIST_TABLE_ID, df=df)
    assert report.status == "fail"


# --- Cross-table mutual-exclusivity --------------------------------------


def test_findings_and_distribution_schemas_are_mutually_exclusive():
    """A row cannot validly belong to BOTH staged finding tables.

    Use slice 2's positive control (track=B) and slice 3's positive
    control (track=D) and verify each is accepted by exactly one schema.
    """
    quality_good = _load_findings_fixture("good")  # track=B
    dist_good = _load_dist_fixture("good")  # track=D

    assert (
        validate_pandera(table_id=FINDINGS_TABLE_ID, df=quality_good).status == "pass"
    )
    assert validate_pandera(table_id=DIST_TABLE_ID, df=quality_good).status == "fail"
    assert validate_pandera(table_id=DIST_TABLE_ID, df=dist_good).status == "pass"
    assert validate_pandera(table_id=FINDINGS_TABLE_ID, df=dist_good).status == "fail"


# --- Frictionless tier ---------------------------------------------------


def test_dist_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _dist_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=DIST_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_dist_frictionless_rejects_track_b(tmp_path: Path):
    parquet_path = _dist_parquet_from_fixture("track_b_in_distribution_table", tmp_path)
    report = validate_frictionless(
        table_id=DIST_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_dist_frictionless_rejects_non_distribution_issue(tmp_path: Path):
    parquet_path = _dist_parquet_from_fixture("non_distribution_issue", tmp_path)
    report = validate_frictionless(
        table_id=DIST_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


# --- Live-evidence smoke (handles empty distribution table) --------------


def test_dist_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """The v20c checkpoint produces an empty distribution-findings table
    (Track D activation hadn't happened at extract time). The schema must
    accept the typed-but-empty Parquet without claiming a row violation —
    Track H specifically added typed empty-table handling, and slice 3
    is the first schema that exercises that path against live evidence.
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
        / "distribution_findings.parquet"
    )
    assert parquet_path.exists()

    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
        # Verify the typed-empty-table path actually preserved the column
        # contract (Codex P2 fix on PR #6) — column NAMES must be present
        # even for 0 rows; types are all VARCHAR for this finding table.
        schema_rows = con.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{parquet_path}')"
        ).fetchall()
    finally:
        con.close()

    assert len(df) == 0, "expected empty distribution-findings table"
    schema_cols = {r[0] for r in schema_rows}
    assert schema_cols == set(FINDINGS_COLUMNS), (
        f"empty-table schema drift: {schema_cols} vs {set(FINDINGS_COLUMNS)}"
    )

    # Pandera must accept the empty DataFrame (0 rows = no checks fail).
    report = validate_pandera(table_id=DIST_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


# ===========================================================================
# Slice 4: raw_salesforce_extract_plan
# ===========================================================================
#
# Direct mirror of source_requirement_plan.json items[]. The schema
# accepts both ``scope == "territory"`` (territory/director/region
# populated) and ``scope == "global"`` (those three legitimately null).


PLAN_FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "raw_salesforce_extract_plan"
)
PLAN_TABLE_ID = "raw_salesforce_extract_plan"

PLAN_COLUMNS = [
    "snapshot_date",
    "run_id",
    "requirement_id",
    "source_system",
    "source_type",
    "salesforce_object",
    "dataset",
    "output_grain",
    "scope",
    "territory",
    "director",
    "region",
    "period_role",
    "quarter_label",
    "source_id",
    "source_label",
    "status",
]


def _load_plan_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (PLAN_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    return df.reindex(columns=[c for c in PLAN_COLUMNS if c in df.columns])


def _plan_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_plan_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def test_plan_pandera_accepts_good_fixture():
    df = _load_plan_fixture("good")
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_plan_pandera_accepts_missing_source_id_status():
    """``status="missing_source_id"`` with empty source_id/source_label is
    a legitimate row — the extract step records the gap so auditing
    emits a finding instead of dropping the row silently."""
    df = _load_plan_fixture("good_missing_source_id")
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("unknown_source_system", "source_system"),
        ("unknown_source_type", "source_type"),
        ("unknown_scope", "scope"),
        ("unknown_period_role", "period_role"),
        ("unknown_status", "status"),
        ("bad_quarter_label", "quarter_label"),
        ("bad_snapshot_date", "snapshot_date"),
        ("empty_requirement_id", "requirement_id"),
    ],
)
def test_plan_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_plan_fixture(fixture)
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


def test_plan_pandera_rejects_missing_dataset_column():
    df = _load_plan_fixture("missing_dataset")
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "fail"


@pytest.mark.parametrize(
    "fixture",
    [
        "territory_scope_missing_territory",
        "territory_scope_missing_director",
    ],
)
def test_plan_pandera_rejects_territory_scope_metadata_gap(fixture):
    """scope='territory' rows must carry non-empty territory and director.

    The column-level rules permit nulls (legitimate for scope='global'),
    so the contract is enforced by a frame-level check. ``region`` is
    deliberately not part of this gate — v20c live evidence has it null
    on every territory row because region is denormalized metadata
    joined in downstream, not carried on plan items."""
    df = _load_plan_fixture(fixture)
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail Pandera validation"
    blob = " ".join(f"{f.message} {f.evidence}" for f in report.findings)
    assert "scope='territory'" in blob, [
        (f.message, f.evidence) for f in report.findings
    ]


def test_plan_pandera_rejects_configured_with_empty_source_id():
    """status='configured' with an empty source_id would silently fail at
    extract time. The column allows nullable strings (so
    status='missing_source_id' rows pass), but the cross-field check
    forbids the configured/empty combination."""
    df = _load_plan_fixture("configured_missing_source_id")
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "fail"
    blob = " ".join(f"{f.message} {f.evidence}" for f in report.findings)
    assert "status='configured'" in blob and "source_id" in blob, [
        (f.message, f.evidence) for f in report.findings
    ]


def test_plan_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _plan_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=PLAN_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_plan_frictionless_rejects_unknown_status(tmp_path: Path):
    parquet_path = _plan_parquet_from_fixture("unknown_status", tmp_path)
    report = validate_frictionless(
        table_id=PLAN_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_plan_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Live evidence: build the warehouse fresh against v20c, validate the
    actual ``raw/salesforce_extract_plan.parquet`` (55 rows)."""
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
        / "salesforce_extract_plan.parquet"
    )
    assert parquet_path.exists()

    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()

    assert len(df) == 55, f"expected 55 plan rows, got {len(df)}"
    report = validate_pandera(table_id=PLAN_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]
# ===========================================================================
# Slice 5: staged_source_requirements
# ===========================================================================


REQUIREMENTS_FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "staged_source_requirements"
)
REQUIREMENTS_TABLE_ID = "staged_source_requirements"

REQUIREMENTS_COLUMNS = [
    "requirement_id",
    "enabled",
    "owner",
    "source_system",
    "source_type",
    "dataset",
    "output_grain",
    "scope",
    "allow_zero",
    "min_rows",
    "max_rows",
    "has_distribution_policy",
    "distribution_dimension_count",
    "slice_sentinel_count",
    "fallback_policy_present",
    "tag_count",
]


def _load_requirements_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (REQUIREMENTS_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    df = df.reindex(columns=[c for c in REQUIREMENTS_COLUMNS if c in df.columns])
    # Match what DuckDB -> pandas produces from the warehouse Parquet:
    # bools as bool, nullable BIGINT as pandas nullable Int64 (so None
    # survives the round-trip without becoming float64 NaN).
    bool_cols = [
        "enabled",
        "allow_zero",
        "has_distribution_policy",
        "fallback_policy_present",
    ]
    for c in bool_cols:
        if c in df.columns:
            df[c] = df[c].astype(bool)
    int_cols = [
        "min_rows",
        "max_rows",
        "distribution_dimension_count",
        "slice_sentinel_count",
        "tag_count",
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    return df


def _requirements_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_requirements_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def test_requirements_pandera_accepts_good_fixture():
    df = _load_requirements_fixture("good")
    report = validate_pandera(table_id=REQUIREMENTS_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_requirements_pandera_rejects_duplicate_requirement_id():
    df = _load_requirements_fixture("duplicate_requirement_id")
    report = validate_pandera(table_id=REQUIREMENTS_TABLE_ID, df=df)
    assert report.status == "fail"
    assert any("requirement_id" in (f.evidence or f.message) for f in report.findings)


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("unknown_source_system", "source_system"),
        ("unknown_source_type", "source_type"),
        ("unknown_scope", "scope"),
        ("negative_min_rows", "min_rows"),
        ("zero_max_rows", "max_rows"),
        ("negative_count", "distribution_dimension_count"),
        ("empty_owner", "owner"),
    ],
)
def test_requirements_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_requirements_fixture(fixture)
    report = validate_pandera(table_id=REQUIREMENTS_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


def test_requirements_pandera_rejects_missing_column():
    df = _load_requirements_fixture("missing_has_distribution_policy")
    report = validate_pandera(table_id=REQUIREMENTS_TABLE_ID, df=df)
    assert report.status == "fail"


def test_requirements_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _requirements_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=REQUIREMENTS_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_requirements_frictionless_rejects_unknown_scope(tmp_path: Path):
    parquet_path = _requirements_parquet_from_fixture("unknown_scope", tmp_path)
    report = validate_frictionless(
        table_id=REQUIREMENTS_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_requirements_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Live evidence: v20c run produces 4 requirement rows."""
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
        / "source_requirements.parquet"
    )
    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()
    assert len(df) >= 1, f"expected requirement rows, got {len(df)}"
    report = validate_pandera(table_id=REQUIREMENTS_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]
# ===========================================================================
# Slice 6: mart_director_source_health
# ===========================================================================


HEALTH_FIXTURE_DIR = (
    REPO_ROOT / "tests" / "fixtures" / "bad_extracts" / "mart_director_source_health"
)
HEALTH_TABLE_ID = "mart_director_source_health"

HEALTH_COLUMNS = [
    "snapshot_date",
    "run_id",
    "director",
    "source_count",
    "ok_source_count",
    "warning_source_count",
    "blocked_source_count",
    "total_row_count",
    "total_finding_count",
]


def _load_health_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (HEALTH_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    df = df.reindex(columns=[c for c in HEALTH_COLUMNS if c in df.columns])
    int_cols = [
        "source_count",
        "ok_source_count",
        "warning_source_count",
        "blocked_source_count",
        "total_row_count",
        "total_finding_count",
    ]
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    return df


def _health_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_health_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def test_health_pandera_accepts_good_fixture():
    df = _load_health_fixture("good")
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_health_pandera_rejects_duplicate_director():
    df = _load_health_fixture("duplicate_director")
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "fail"


def test_health_pandera_rejects_buckets_exceeding_source_count():
    """Wide-row contract: ok+warning+blocked must not exceed source_count.

    Pandera's frame-level checks land in ``failure_cases`` with the
    check's error message in the finding's ``message`` and the table
    name in ``evidence``. Look in BOTH fields so the assertion is
    robust to which side carries the substring across pandera versions.
    """
    df = _load_health_fixture("buckets_exceed_source_count")
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "fail"
    assert any(
        "source_count" in f"{f.message} {f.evidence}" for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("negative_source_count", "source_count"),
        ("negative_total_row_count", "total_row_count"),
        ("negative_total_finding_count", "total_finding_count"),
        ("bad_snapshot_date", "snapshot_date"),
        ("empty_run_id", "run_id"),
    ],
)
def test_health_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_health_fixture(fixture)
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "fail", f"{fixture} should fail"
    assert any(
        expected_substring in (f.evidence or f.message) for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


def test_health_pandera_rejects_missing_director_column():
    df = _load_health_fixture("missing_director")
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "fail"


def test_health_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _health_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=HEALTH_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_health_frictionless_rejects_duplicate_director(tmp_path: Path):
    parquet_path = _health_parquet_from_fixture("duplicate_director", tmp_path)
    report = validate_frictionless(
        table_id=HEALTH_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_health_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Live evidence: v20c run produces 10 director rollup rows."""
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
        / "marts"
        / "director_source_health.parquet"
    )
    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()
    assert len(df) >= 1, f"expected director rollup rows, got {len(df)}"
    report = validate_pandera(table_id=HEALTH_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]
# ===========================================================================
# Slice 7 (final): mart_source_run_summary
# ===========================================================================


SUMMARY_FIXTURE_DIR = (
    REPO_ROOT
    / "tests"
    / "fixtures"
    / "bad_extracts"
    / "mart_source_run_summary"
)
SUMMARY_TABLE_ID = "mart_source_run_summary"

SUMMARY_COLUMNS = [
    "snapshot_date",
    "run_id",
    "generated_at",
    "status",
    "selected_source_count",
    "source_count",
    "ok_source_count",
    "warning_source_count",
    "blocked_source_count",
    "finding_count",
    "high_finding_count",
    "medium_finding_count",
    "baseline_drift_finding_count",
    "baseline_high_finding_count",
    "baseline_matched_source_count",
    "baseline_missing_source_count",
    "distribution_finding_count",
    "distribution_high_finding_count",
    "distribution_matched_source_count",
    "distribution_missing_seed_source_count",
    "distribution_missing_seed_dimension_count",
]


def _load_summary_fixture(name: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = json.loads(
        (SUMMARY_FIXTURE_DIR / f"{name}.json").read_text(encoding="utf-8")
    )
    df = pd.DataFrame(rows)
    df = df.reindex(columns=[c for c in SUMMARY_COLUMNS if c in df.columns])
    int_cols = [c for c in SUMMARY_COLUMNS if c.endswith("_count")]
    for c in int_cols:
        if c in df.columns:
            df[c] = df[c].astype("Int64")
    return df


def _summary_parquet_from_fixture(name: str, tmp_path: Path) -> Path:
    df = _load_summary_fixture(name)
    parquet_path = tmp_path / f"{name}.parquet"
    df.to_parquet(parquet_path, index=False)
    return parquet_path


def test_summary_pandera_accepts_good_fixture():
    df = _load_summary_fixture("good")
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_summary_pandera_rejects_duplicate_run():
    df = _load_summary_fixture("duplicate_run")
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "fail"


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("buckets_exceed_source_count", "source_count"),
        ("high_medium_exceed_finding_count", "finding_count"),
        ("baseline_high_exceeds_drift", "baseline"),
        ("distribution_high_exceeds_total", "distribution"),
    ],
)
def test_summary_pandera_rejects_wide_row_violation(fixture, expected_substring):
    df = _load_summary_fixture(fixture)
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "fail"
    assert any(
        expected_substring in f"{f.message} {f.evidence}" for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


@pytest.mark.parametrize(
    "fixture,expected_substring",
    [
        ("unknown_status", "status"),
        ("negative_finding_count", "finding_count"),
        ("bad_generated_at", "generated_at"),
    ],
)
def test_summary_pandera_rejects_negative_control(fixture, expected_substring):
    df = _load_summary_fixture(fixture)
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "fail"
    assert any(
        expected_substring in f"{f.message} {f.evidence}" for f in report.findings
    ), [(f.message, f.evidence) for f in report.findings]


def test_summary_pandera_rejects_missing_distribution_count():
    df = _load_summary_fixture("missing_distribution_count")
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "fail"


def test_summary_frictionless_accepts_good_fixture(tmp_path: Path):
    parquet_path = _summary_parquet_from_fixture("good", tmp_path)
    report = validate_frictionless(
        table_id=SUMMARY_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]


def test_summary_frictionless_rejects_unknown_status(tmp_path: Path):
    parquet_path = _summary_parquet_from_fixture("unknown_status", tmp_path)
    report = validate_frictionless(
        table_id=SUMMARY_TABLE_ID,
        parquet_path=parquet_path,
        repo_root=REPO_ROOT,
    )
    assert report.status == "fail"


def test_summary_pandera_accepts_v20d_checkpoint_warehouse(tmp_path: Path):
    """Live evidence: v20c run produces exactly 1 summary row."""
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
        / "marts"
        / "source_run_summary.parquet"
    )
    con = duckdb.connect()
    try:
        df = con.execute(f"SELECT * FROM read_parquet('{parquet_path}')").df()
    finally:
        con.close()
    assert len(df) == 1, f"expected exactly 1 summary row, got {len(df)}"
    report = validate_pandera(table_id=SUMMARY_TABLE_ID, df=df)
    assert report.status == "pass", [
        f"{f.message}: {f.evidence}" for f in report.findings
    ]
