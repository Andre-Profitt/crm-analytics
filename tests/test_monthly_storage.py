import json
from pathlib import Path

import pandas as pd

from scripts.monthly_platform.contracts import StageResult, utc_now_iso
from scripts.monthly_platform.period import resolve_period_context
from scripts.monthly_platform.storage import MonthlyStorage, schema_fingerprint


def test_create_run_manifest_and_ledger(tmp_path: Path) -> None:
    period = resolve_period_context(snapshot_date="2026-04-30")
    storage = MonthlyStorage(
        root=tmp_path,
        snapshot_date="2026-04-30",
        run_id="test-run",
    )

    manifest = storage.create_run(period_context=period.as_dict())

    assert manifest.run_id == "test-run"
    assert manifest.snapshot_date == "2026-04-30"
    assert manifest.period_context["current_quarter"]["title"] == "Q2 2026"
    assert storage.ledger_path.exists()
    assert storage.manifest_path.exists()
    assert storage.load_manifest() == manifest


def test_register_source_extract_writes_raw_parquet_and_hashes(tmp_path: Path) -> None:
    storage = MonthlyStorage(root=tmp_path, snapshot_date="2026-04-30")
    rows = [
        {
            "Opportunity Name": "Alpha",
            "Sales Region": "APAC",
            "ARR": 100000.0,
            "Close Date": "2026-05-15",
        },
        {
            "Opportunity Name": "Beta",
            "Sales Region": "APAC",
            "ARR": 250000.0,
            "Close Date": "2026-08-31",
        },
    ]

    extract = storage.register_source_extract(
        source_type="salesforce_report",
        source_id="00O-test",
        source_label="APAC Historical Trending Q2",
        rows=rows,
        territory="APAC",
        director="Jesper Tyrer",
        period_role="current_quarter",
        quarter_label="Q2",
    )

    assert extract.row_count == 2
    assert extract.raw_artifact.format == "json"
    assert extract.normalized_artifact is not None
    assert extract.normalized_artifact.format == "parquet"
    assert extract.schema_sha256 == extract.raw_artifact.schema_sha256
    assert Path(extract.raw_artifact.path).exists()
    assert Path(extract.normalized_artifact.path).exists()
    assert pd.read_parquet(extract.normalized_artifact.path).shape == (2, 4)
    assert storage.get_source_extract(extract.source_extract_id) == extract
    assert storage.get_artifact(extract.raw_artifact.artifact_id) == extract.raw_artifact


def test_stage_result_records_artifacts_for_lookup(tmp_path: Path) -> None:
    storage = MonthlyStorage(root=tmp_path, snapshot_date="2026-04-30")
    storage.create_run()
    extract = storage.register_source_extract(
        source_type="salesforce_list_view",
        source_id="00B-test",
        source_label="APAC PI Q2",
        rows=[{"Name": "Alpha", "Forecast Category": "Commit"}],
        territory="APAC",
        period_role="current_quarter",
        quarter_label="Q2",
    )
    assert extract.normalized_artifact is not None

    now = utc_now_iso()
    manifest = storage.record_stage_result(
        StageResult(
            stage_name="extract_salesforce_reports",
            status="ok",
            started_at=now,
            finished_at=now,
            duration_seconds=0.1,
            outputs=[extract.raw_artifact, extract.normalized_artifact],
            source_extracts=[extract],
        )
    )

    stage_artifacts = storage.artifacts_for_stage("extract_salesforce_reports")
    assert manifest.source_extracts == [extract]
    assert {artifact.artifact_id for artifact in stage_artifacts} == {
        extract.raw_artifact.artifact_id,
        extract.normalized_artifact.artifact_id,
    }


def test_schema_fingerprint_is_stable_for_field_order() -> None:
    left = [{"b": 2, "a": "x"}, {"a": "y", "b": 3}]
    right = [{"a": "x", "b": 2}, {"b": 3, "a": "y"}]

    assert schema_fingerprint(left) == schema_fingerprint(right)


def test_nested_values_are_safe_for_parquet_while_raw_json_is_preserved(
    tmp_path: Path,
) -> None:
    storage = MonthlyStorage(root=tmp_path, snapshot_date="2026-04-30")
    extract = storage.register_source_extract(
        source_type="salesforce_report",
        source_id="00O-nested",
        source_label="Nested Report",
        rows=[
            {
                "Opportunity": "Alpha",
                "Forecast ARR": {"amount": 100, "currency": "EUR"},
            }
        ],
        raw_payload={"raw": {"nested": True}},
    )

    assert extract.normalized_artifact is not None
    table = pd.read_parquet(extract.normalized_artifact.path)
    assert table["Forecast ARR"].iloc[0] == '{"amount": 100, "currency": "EUR"}'
    raw_payload = json.loads(Path(extract.raw_artifact.path).read_text())
    assert raw_payload["rows"][0]["Forecast ARR"] == {"amount": 100, "currency": "EUR"}
