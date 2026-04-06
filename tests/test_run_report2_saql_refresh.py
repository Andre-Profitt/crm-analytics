"""Unit tests for run_report2_saql_refresh.py."""

from __future__ import annotations

import json
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = REPO_ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from run_report2_saql_refresh import merge_query_results, load_validation_json

FIXTURE_PATH = (
    REPO_ROOT / "tests" / "fixtures" / "sales_ops_live_saql_validation_sample.json"
)


def test_load_validation_json_returns_expected_structure():
    payload = load_validation_json(FIXTURE_PATH)
    assert payload["artifact_type"] == "sales_ops_test_live_saql_validation"
    assert len(payload["results"]) == 2
    assert payload["results"][0]["step_alias"] == "test_kpi_one"


def test_merge_query_results_updates_sample_records_and_row_count():
    original = load_validation_json(FIXTURE_PATH)
    new_results_by_alias = {
        "test_kpi_one": [{"total": 200}, {"total": 250}],
        "test_kpi_two": [{"duns_count": 75}],
    }
    merged = merge_query_results(
        original, new_results_by_alias, validated_at="2026-04-01"
    )

    assert merged["validated_at"] == "2026-04-01"
    assert merged["results"][0]["sample_records"] == [{"total": 200}, {"total": 250}]
    assert merged["results"][0]["row_count"] == 2
    assert merged["results"][0]["status"] == "ok"
    assert merged["results"][1]["sample_records"] == [{"duns_count": 75}]
    assert merged["results"][1]["row_count"] == 1


def test_merge_query_results_marks_missing_aliases_as_error():
    original = load_validation_json(FIXTURE_PATH)
    new_results_by_alias = {
        "test_kpi_one": [{"total": 999}],
    }
    merged = merge_query_results(
        original, new_results_by_alias, validated_at="2026-04-01"
    )

    assert merged["results"][0]["status"] == "ok"
    assert merged["results"][0]["sample_records"] == [{"total": 999}]
    assert merged["results"][1]["status"] == "error"
    assert merged["results"][1]["sample_records"] == [{"duns_count": 50}]


def test_merge_query_results_preserves_query_and_metric_fields():
    original = load_validation_json(FIXTURE_PATH)
    new_results_by_alias = {
        "test_kpi_one": [{"total": 1}],
        "test_kpi_two": [{"duns_count": 1}],
    }
    merged = merge_query_results(
        original, new_results_by_alias, validated_at="2026-04-01"
    )

    assert merged["results"][0]["query"] == original["results"][0]["query"]
    assert merged["results"][0]["metric"] == "Test KPI One"
    assert merged["results"][1]["metric"] == "Test KPI Two"


def test_merge_query_results_does_not_mutate_input():
    original = load_validation_json(FIXTURE_PATH)
    original_copy = json.loads(json.dumps(original))
    _ = merge_query_results(
        original, {"test_kpi_one": [{"total": 1}]}, validated_at="2026-04-01"
    )
    assert original == original_copy
