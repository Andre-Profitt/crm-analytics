"""Unit tests for crm_analytics_runtime.py.

All tests use tmp_path to isolate the runs/ dir. No live-org calls.
"""

from __future__ import annotations

import json

import pytest

import crm_analytics_runtime  # pyright: ignore[reportMissingImports]
from crm_analytics_runtime import RunSummary, builder_run  # pyright: ignore[reportMissingImports]


# --- Dataclass + __post_init__ -------------------------------------------


def test_run_summary_external_id_is_deterministic():
    a = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    b = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert a.external_id == b.external_id


def test_run_summary_external_id_is_18_chars():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert len(s.external_id) == 18
    assert all(c in "0123456789abcdef" for c in s.external_id)


def test_run_summary_external_id_differs_per_dataset_or_time():
    a = RunSummary(
        dataset_name="A", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    b = RunSummary(
        dataset_name="B", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    c = RunSummary(
        dataset_name="A", builder_path="x.py", started_at="2026-04-06T00:00:01Z"
    )
    assert len({a.external_id, b.external_id, c.external_id}) == 3


def test_run_summary_default_status_is_running():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert s.status == "running"


def test_run_summary_schema_version_default_is_1():
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    assert s.summary_schema_version == 1


# --- Filename generation -------------------------------------------------


def test_to_json_path_strips_colons_and_dashes(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="My_Dataset",
        builder_path="x.py",
        started_at="2026-04-06T15:47:20Z",
    )
    path = s.to_json_path()
    assert path.name == "20260406T154720Z.json"
    assert path.parent.name == "My_Dataset"
    assert path.parent.parent == tmp_path


# --- Writer --------------------------------------------------------------


def test_write_creates_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="X", builder_path="x.py", started_at="2026-04-06T00:00:00Z"
    )
    path = s.write()
    assert path.exists()
    assert path.parent.name == "X"


def test_written_json_is_parseable_and_sorted(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    s = RunSummary(
        dataset_name="X",
        builder_path="x.py",
        started_at="2026-04-06T00:00:00Z",
        row_count=42,
    )
    path = s.write()
    raw = path.read_text()
    parsed = json.loads(raw)
    keys = list(parsed.keys())
    assert keys == sorted(keys), f"Keys not sorted: {keys}"
    assert parsed["row_count"] == 42
    assert parsed["summary_schema_version"] == 1
    assert parsed["external_id"] == s.external_id


# --- Context manager -----------------------------------------------------


def test_builder_run_success_path(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    with builder_run("X", "x.py") as summary:
        summary.row_count = 100
        summary.dataset_id = "0Fb000000000000"
        summary.dataset_version_id = "0Fc000000000000"
    json_files = list(tmp_path.glob("X/*.json"))
    assert len(json_files) == 1
    parsed = json.loads(json_files[0].read_text())
    assert parsed["status"] == "ok"
    assert parsed["row_count"] == 100
    assert parsed["dataset_id"] == "0Fb000000000000"
    assert parsed["runtime_s"] is not None
    assert parsed["runtime_s"] >= 0
    assert parsed["finished_at"] is not None
    assert parsed["errors"] == []


def test_builder_run_failure_path_writes_json_and_reraises(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    with pytest.raises(ValueError, match="boom"):
        with builder_run("X", "x.py") as summary:
            summary.row_count = 50
            raise ValueError("boom")
    json_files = list(tmp_path.glob("X/*.json"))
    assert len(json_files) == 1
    parsed = json.loads(json_files[0].read_text())
    assert parsed["status"] == "failed"
    assert parsed["row_count"] == 50  # was set before the raise
    assert any("ValueError: boom" in e for e in parsed["errors"])
    assert any("Traceback" in e for e in parsed["errors"])


def test_builder_run_write_failure_does_not_mask_original_exception(
    tmp_path, monkeypatch
):
    # Contract from Error Handling section: when the body raises AND
    # summary.write() also fails, the body exception must be the one
    # that propagates.
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)

    def boom_write(self):
        del self
        raise OSError("disk full")

    monkeypatch.setattr(RunSummary, "write", boom_write)
    with pytest.raises(ValueError, match="original error"):
        with builder_run("X", "x.py"):
            raise ValueError("original error")


def test_builder_run_populates_finished_at_on_both_paths(tmp_path, monkeypatch):
    monkeypatch.setattr(crm_analytics_runtime, "RUNS_ROOT", tmp_path)
    # success
    with builder_run("X", "x.py"):
        pass
    parsed_ok = json.loads(next(tmp_path.glob("X/*.json")).read_text())
    assert parsed_ok["finished_at"] is not None
    # failure (different dataset to avoid filename collision)
    with pytest.raises(ValueError):
        with builder_run("Y", "y.py"):
            raise ValueError("fail")
    parsed_fail = json.loads(next(tmp_path.glob("Y/*.json")).read_text())
    assert parsed_fail["finished_at"] is not None
