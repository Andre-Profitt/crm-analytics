# tests/test_bundle_validation.py
import dataclasses
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from scripts.monthly_platform.bundle_validation import validate_bundle
from bundle_factory import make_test_bundle


def test_valid_bundle_has_no_errors():
    bundle = make_test_bundle()
    errors = validate_bundle(bundle)
    assert errors == []


def test_catches_negative_pipeline_arr():
    bundle = make_test_bundle(pipeline_arr=-100.0)
    errors = validate_bundle(bundle)
    assert any("negative" in e.lower() and "arr" in e.lower() for e in errors)


def test_catches_mismatched_dataset_counts():
    bundle = make_test_bundle()
    bad_counts = {**bundle.dataset_counts, "pipeline_open": 999}
    bundle = dataclasses.replace(bundle, dataset_counts=bad_counts)
    errors = validate_bundle(bundle)
    assert any("pipeline_open" in e and "count" in e.lower() for e in errors)


def test_catches_invalid_close_date_format():
    bundle = make_test_bundle()
    bad_deal = dataclasses.replace(
        bundle.datasets.pipeline_open[0], close_date="June 30 2026"
    )
    bad_datasets = dataclasses.replace(bundle.datasets, pipeline_open=[bad_deal])
    bundle = dataclasses.replace(bundle, datasets=bad_datasets)
    errors = validate_bundle(bundle)
    assert any("date" in e.lower() for e in errors)


def test_catches_invalid_stage_name():
    bundle = make_test_bundle()
    bad_deal = dataclasses.replace(
        bundle.datasets.pipeline_open[0], stage="Invalid Stage"
    )
    bad_datasets = dataclasses.replace(bundle.datasets, pipeline_open=[bad_deal])
    bundle = dataclasses.replace(bundle, datasets=bad_datasets)
    errors = validate_bundle(bundle)
    assert any("stage" in e.lower() for e in errors)
