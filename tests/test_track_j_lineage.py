"""Track J-Lite — OpenLineage emitter, lineage index, slide-to-source map.

Covers:

- Event shape: START / COMPLETE / FAIL / ABORT events match the
  OpenLineage 2-0-2 envelope (eventType, eventTime, run.runId,
  job.namespace+name, producer, schemaURL).
- LineageEmitter file sink: events serialise to ``lineage_events/*.json``
  with the documented filename convention.
- ``build_lineage_index`` aggregates jobs, datasets, edges, and
  collapses repeated dataset references.
- ``build_slide_to_source_map`` returns one entry per slide, every
  slide carries at least one source binding, and distinct workbook
  sheets are aggregated.
- Live-anchor end-to-end: when ``--lineage-dir`` is passed to
  ``build_release_packet`` against the APAC anchors, all three
  artifacts (lineage_events/, lineage_index.json, slide_to_source_map.json)
  are produced and consistent with the release-packet decision.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.build_release_packet import build_release_packet
from scripts.monthly_platform import deck_binding_resolver, deck_contract, lineage
from scripts.monthly_platform import director_workbook_contract as wb_contract


REPO_ROOT = Path(__file__).resolve().parents[1]
APAC_WORKBOOK = Path("/Users/test/Downloads/jesper-tyrer-2026-04-20.xlsx")
APAC_DECK = Path("/Users/test/Downloads/jesper-tyrer-LAND.pptx")


# ---------------------------------------------------------------------------
# Emitter & event shape
# ---------------------------------------------------------------------------


def test_emitter_writes_start_complete_pair(tmp_path: Path) -> None:
    em = lineage.LineageEmitter(out_dir=tmp_path)
    em.start_job("foo", inputs=[lineage._dataset("ds_in")])
    em.complete_job(
        "foo",
        outputs=[lineage._dataset("ds_out")],
        result_facets={"validator_status": "pass"},
    )

    files = sorted(em.events_dir.iterdir())
    assert [p.name for p in files] == [
        "01-START-foo.json",
        "02-COMPLETE-foo.json",
    ]

    start = json.loads(files[0].read_text())
    complete = json.loads(files[1].read_text())

    # OpenLineage envelope
    for evt in (start, complete):
        assert evt["producer"] == lineage.PRODUCER
        assert evt["schemaURL"] == lineage.SCHEMA_URL
        assert evt["run"]["runId"] == em.run_id
        assert evt["job"]["namespace"] == lineage.JOB_NAMESPACE
        assert evt["job"]["name"] == "foo"
        assert "eventTime" in evt

    assert start["eventType"] == "START"
    assert start["inputs"][0]["name"] == "ds_in"
    assert complete["eventType"] == "COMPLETE"
    assert complete["outputs"][0]["name"] == "ds_out"
    assert complete["run"]["facets"]["result"]["validator_status"] == "pass"
    # Timing facet is wired automatically.
    assert "timing" in complete["run"]["facets"]


def test_emitter_status_must_be_terminal(tmp_path: Path) -> None:
    em = lineage.LineageEmitter(out_dir=tmp_path)
    em.start_job("x")
    with pytest.raises(ValueError):
        em.complete_job("x", status="START")


def test_emitter_handles_fail_and_abort(tmp_path: Path) -> None:
    em = lineage.LineageEmitter(out_dir=tmp_path)
    em.start_job("a")
    em.complete_job("a", status="FAIL", result_facets={"validator_status": "fail"})
    em.start_job("b")
    em.complete_job("b", status="ABORT", result_facets={"validator_status": "skipped"})

    a_complete = json.loads((em.events_dir / "02-FAIL-a.json").read_text())
    b_complete = json.loads((em.events_dir / "04-ABORT-b.json").read_text())
    assert a_complete["eventType"] == "FAIL"
    assert b_complete["eventType"] == "ABORT"


def test_file_dataset_includes_content_digest(tmp_path: Path) -> None:
    p = tmp_path / "x.txt"
    p.write_text("hello")
    ds = lineage.file_dataset(p)
    assert ds is not None
    assert ds["facets"]["contentDigest"]["algorithm"] == "sha256"
    assert ds["facets"]["contentDigest"]["size_bytes"] == 5
    # Missing file gets a lifecycleState facet, not a digest.
    missing = lineage.file_dataset(tmp_path / "absent.txt")
    assert missing is not None
    assert missing["facets"]["lifecycleState"]["state"] == "MISSING"


# ---------------------------------------------------------------------------
# Lineage index aggregation
# ---------------------------------------------------------------------------


def test_lineage_index_aggregates_jobs_and_datasets(tmp_path: Path) -> None:
    em = lineage.LineageEmitter(out_dir=tmp_path)
    em.start_job(
        "stage1",
        inputs=[lineage._dataset("a"), lineage._dataset("b")],
        outputs=[lineage._dataset("c")],
    )
    em.complete_job(
        "stage1",
        outputs=[lineage._dataset("c")],
        result_facets={"validator_status": "pass"},
    )
    em.start_job("stage2", inputs=[lineage._dataset("c")])
    em.complete_job("stage2", result_facets={"validator_status": "pass"})

    idx = lineage.build_lineage_index(em.events_dir, run_id=em.run_id)
    assert idx["schema_version"] == lineage.LINEAGE_INDEX_SCHEMA
    assert idx["run_id"] == em.run_id
    summary = idx["summary"]
    assert summary["job_count"] == 2
    assert summary["dataset_count"] == 3  # a, b, c — c referenced twice
    assert summary["jobs_complete"] == 2
    assert summary["jobs_failed"] == 0
    assert summary["jobs_open"] == 0

    # Edges: stage1 consumes a,b; stage1 produces c (twice — start + complete);
    # stage2 consumes c.
    edges = idx["edges"]
    assert any(
        e["job"] == "stage2" and e["dataset"] == "c" and e["direction"] == "input"
        for e in edges
    )
    # Dataset c shows both produced_by and consumed_by.
    c = next(d for d in idx["datasets"] if d["name"] == "c")
    assert "stage1" in c["produced_by"]
    assert "stage2" in c["consumed_by"]


def test_lineage_index_marks_failed_and_open_jobs(tmp_path: Path) -> None:
    em = lineage.LineageEmitter(out_dir=tmp_path)
    em.start_job("ok")
    em.complete_job("ok", result_facets={"validator_status": "pass"})
    em.start_job("bad")
    em.complete_job("bad", status="FAIL", result_facets={"validator_status": "fail"})
    em.start_job("dangling")  # no completion — open

    idx = lineage.build_lineage_index(em.events_dir)
    assert idx["summary"]["job_count"] == 3
    assert idx["summary"]["jobs_complete"] == 1
    assert idx["summary"]["jobs_failed"] == 1
    assert idx["summary"]["jobs_open"] == 1


# ---------------------------------------------------------------------------
# Slide-to-source map
# ---------------------------------------------------------------------------


def test_slide_map_covers_every_active_slide_in_director_monthly() -> None:
    deck = deck_contract.load()
    sm = lineage.build_slide_to_source_map(deck)
    assert sm["schema_version"] == lineage.SLIDE_MAP_SCHEMA
    assert sm["profile_id"] == "director_monthly"
    assert sm["slide_count"] == len(deck.director_monthly["slides"])
    assert sm["slide_count"] >= 16  # 16 active baseline per Track E

    for entry in sm["slides"]:
        assert entry["slide_id"]
        assert entry["slide_number"] is not None
        assert isinstance(entry["sources"], list)
        # Every active slide must declare at least one binding (table,
        # takeaway, source_note, link, or static text).
        assert entry["sources"], f"slide {entry['slide_id']} has no sources"


def test_slide_map_distinct_datasets_aggregate_workbook_sheets() -> None:
    deck = deck_contract.load()
    sm = lineage.build_slide_to_source_map(deck)
    distinct = sm["distinct_datasets"]
    assert distinct  # at least one workbook sheet feeds at least one slide
    for ds in distinct:
        assert ds["dataset_name"].startswith("director_workbook::")
        assert ds["consumed_by_slides"]
        assert ds["kind"] == "workbook_sheet"


@pytest.mark.skipif(
    not APAC_WORKBOOK.exists(), reason="Live APAC workbook not present."
)
def test_slide_map_resolution_status_uses_binding_report() -> None:
    deck = deck_contract.load()
    workbook = wb_contract.load()
    bindings = deck_binding_resolver.resolve(
        workbook_path=APAC_WORKBOOK, deck=deck, workbook=workbook
    )
    sm = lineage.build_slide_to_source_map(
        deck, binding_report=bindings, workbook_path=APAC_WORKBOOK
    )
    # On the clean APAC anchor every table binding resolves; status
    # comes from the resolver, not a default.
    statuses = {
        src.get("resolution_status")
        for slide in sm["slides"]
        for src in slide["sources"]
        if src.get("kind") == "table"
    }
    assert statuses, "expected at least one table binding to assert against"
    assert "fail" not in statuses, statuses


# ---------------------------------------------------------------------------
# Orchestrator integration (live anchor)
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
def test_release_packet_emits_lineage_artifacts(tmp_path: Path) -> None:
    report = build_release_packet(
        workbook=APAC_WORKBOOK,
        pptx=APAC_DECK,
        skip_visual=True,
        lineage_dir=tmp_path,
    )

    # Same release decision the existing Track G-Lite test asserts.
    assert report["publish_decision"] == "publish_ready", [
        s for s in report["summaries"] if s["status"] != "pass"
    ]

    assert "lineage" in report
    ln = report["lineage"]
    assert ln["run_id"]
    # 8 validator jobs × {START, COMPLETE-or-ABORT} = 16 events.
    assert ln["event_count"] == 16
    assert ln["job_count"] == 8

    # Files materialised.
    events_dir = tmp_path / "lineage_events"
    index_path = tmp_path / "lineage_index.json"
    slide_map_path = tmp_path / "slide_to_source_map.json"
    assert events_dir.exists()
    assert index_path.exists()
    assert slide_map_path.exists()
    assert sum(1 for _ in events_dir.glob("*.json")) == 16

    # Index is internally consistent.
    idx = json.loads(index_path.read_text())
    assert idx["run_id"] == ln["run_id"]
    assert idx["summary"]["jobs_complete"] == 7  # all but the skipped visual job
    assert idx["summary"]["jobs_failed"] == 1  # ABORT counts here
    assert idx["summary"]["jobs_open"] == 0

    # Slide map covers the active deck and references workbook sheets.
    sm = json.loads(slide_map_path.read_text())
    assert sm["slide_count"] >= 16
    assert sm["distinct_dataset_count"] >= 1
