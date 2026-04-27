"""Track J-Lite — OpenLineage event emitter + slide-to-source map.

Lightweight lineage layer scoped to the release-packet orchestrator
(Track G-Lite) and the release-catalog wrapper (Track K). Each
validator stage emits an OpenLineage-compatible START / COMPLETE event
to a local file sink. After the orchestrator finishes we aggregate
events into a single ``lineage_index.json`` (DAG view) and derive a
``slide_to_source_map.json`` from the deck contract + binding report.

Hard NO-GOs preserved:
  * No ETL, no Salesforce live API, no warehouse build.
  * No edits to PPTX / XLSX files.
  * Pure metadata over already-emitted artifacts.

OpenLineage reference: https://openlineage.io/spec/ (2-0-2).
We emit the minimal-required envelope (eventType, eventTime, run,
job, producer, schemaURL) plus inputs[] / outputs[] with optional
``facets`` blobs. Backends like Marquez can ingest these as-is.

Event filename convention:

  lineage_events/{seq:02d}-{eventType}-{job}.json
    e.g. lineage_events/03-START-deck_bindings.json

This makes diffs deterministic and the per-event JSON cheap to grep.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRODUCER = "https://github.com/anthropic-internal/crm-analytics/track-j-lite"
SCHEMA_URL = "https://openlineage.io/spec/2-0-2/OpenLineage.json"
JOB_NAMESPACE = "crm-analytics.monthly-release"
DATASET_NAMESPACE = "file://crm-analytics"

LINEAGE_INDEX_SCHEMA = "monthly_platform.lineage_index.v1"
SLIDE_MAP_SCHEMA = "monthly_platform.slide_to_source_map.v1"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _dataset(name: str, *, facets: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build an OpenLineage dataset reference.

    `name` is the logical dataset name (path or URI). Facets carry
    optional metadata such as content digest, byte size, schema.
    """
    out: dict[str, Any] = {"namespace": DATASET_NAMESPACE, "name": name}
    if facets:
        out["facets"] = facets
    return out


def file_dataset(path: Path | str | None) -> dict[str, Any] | None:
    """Convenience: build a dataset entry for a local file with digest."""
    if path is None:
        return None
    p = Path(path)
    facets: dict[str, Any] = {}
    if p.exists():
        sha = _sha256(p)
        if sha is not None:
            facets["dataSource"] = {
                "_producer": PRODUCER,
                "_schemaURL": SCHEMA_URL,
                "name": str(p),
                "uri": p.absolute().as_uri(),
            }
            facets["contentDigest"] = {
                "_producer": PRODUCER,
                "algorithm": "sha256",
                "value": sha,
                "size_bytes": p.stat().st_size,
            }
    else:
        facets["lifecycleState"] = {
            "_producer": PRODUCER,
            "state": "MISSING",
        }
    return _dataset(str(p), facets=facets or None)


class LineageEmitter:
    """Emits OpenLineage START/COMPLETE/FAIL events to a local file sink.

    One emitter spans a single run_id (uuid4 by default) covering all
    stages of the release packet. Each ``start_job`` / ``complete_job``
    pair writes a JSON event under ``out_dir/lineage_events/``.
    """

    def __init__(
        self,
        out_dir: Path,
        *,
        run_id: str | None = None,
        parent_run_id: str | None = None,
    ) -> None:
        self.out_dir = Path(out_dir)
        self.events_dir = self.out_dir / "lineage_events"
        self.events_dir.mkdir(parents=True, exist_ok=True)
        self.run_id = run_id or str(uuid.uuid4())
        self.parent_run_id = parent_run_id
        self._seq = 0
        self._open_jobs: dict[str, dict[str, Any]] = {}
        self._events: list[dict[str, Any]] = []

    def _next_seq(self) -> int:
        self._seq += 1
        return self._seq

    def _write_event(self, event: dict[str, Any]) -> Path:
        seq = event["_seq"]
        et = event["eventType"]
        job = event["job"]["name"]
        path = self.events_dir / f"{seq:02d}-{et}-{job}.json"
        path.write_text(
            json.dumps(event, indent=2, default=str) + "\n", encoding="utf-8"
        )
        self._events.append(event)
        return path

    def start_job(
        self,
        job_name: str,
        *,
        inputs: list[dict[str, Any]] | None = None,
        outputs: list[dict[str, Any]] | None = None,
        facets: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        seq = self._next_seq()
        event: dict[str, Any] = {
            "eventType": "START",
            "eventTime": _now(),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": self.run_id},
            "job": {"namespace": JOB_NAMESPACE, "name": job_name},
            "inputs": [d for d in (inputs or []) if d is not None],
            "outputs": [d for d in (outputs or []) if d is not None],
            "_seq": seq,
        }
        if self.parent_run_id is not None:
            event["run"]["facets"] = {
                "parent": {
                    "_producer": PRODUCER,
                    "run": {"runId": self.parent_run_id},
                    "job": {"namespace": JOB_NAMESPACE, "name": "release_packet"},
                }
            }
        if facets:
            event.setdefault("run", {}).setdefault("facets", {}).update(facets)
        self._open_jobs[job_name] = {"start_seq": seq, "start_time": event["eventTime"]}
        self._write_event(event)
        return event

    def complete_job(
        self,
        job_name: str,
        *,
        outputs: list[dict[str, Any]] | None = None,
        inputs: list[dict[str, Any]] | None = None,
        status: str = "COMPLETE",
        result_facets: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if status not in ("COMPLETE", "FAIL", "ABORT"):
            raise ValueError(f"invalid completion status: {status!r}")
        seq = self._next_seq()
        opened = self._open_jobs.pop(job_name, None)
        event: dict[str, Any] = {
            "eventType": status,
            "eventTime": _now(),
            "producer": PRODUCER,
            "schemaURL": SCHEMA_URL,
            "run": {"runId": self.run_id},
            "job": {"namespace": JOB_NAMESPACE, "name": job_name},
            "inputs": [d for d in (inputs or []) if d is not None],
            "outputs": [d for d in (outputs or []) if d is not None],
            "_seq": seq,
        }
        run_facets: dict[str, Any] = {}
        if opened is not None:
            run_facets["timing"] = {
                "_producer": PRODUCER,
                "start_seq": opened["start_seq"],
                "start_time": opened["start_time"],
                "end_time": event["eventTime"],
            }
        if result_facets:
            run_facets["result"] = {"_producer": PRODUCER, **result_facets}
        if run_facets:
            event["run"]["facets"] = run_facets
        self._write_event(event)
        return event

    def all_events(self) -> list[dict[str, Any]]:
        return list(self._events)


def build_lineage_index(
    events_dir: Path,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    """Aggregate per-event JSON files into a single DAG-view index.

    Returns a dict containing:
      - run_id (if uniform across events)
      - jobs: list of {name, eventType counts, first_seen, last_seen, status}
      - datasets: union of all input + output datasets with usage counts
      - edges: (job_name, dataset_name, direction) triples
    """
    events_dir = Path(events_dir)
    if not events_dir.exists():
        raise FileNotFoundError(events_dir)

    events: list[dict[str, Any]] = []
    for path in sorted(events_dir.glob("*.json")):
        events.append(json.loads(path.read_text(encoding="utf-8")))

    job_state: dict[str, dict[str, Any]] = {}
    dataset_state: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    run_ids: set[str] = set()

    for e in events:
        run_ids.add(e.get("run", {}).get("runId", ""))
        job_name = e["job"]["name"]
        et = e["eventType"]
        js = job_state.setdefault(
            job_name,
            {
                "name": job_name,
                "namespace": e["job"]["namespace"],
                "events": {"START": 0, "COMPLETE": 0, "FAIL": 0, "ABORT": 0},
                "first_seq": e.get("_seq"),
                "last_seq": e.get("_seq"),
                "status": None,
            },
        )
        js["events"][et] = js["events"].get(et, 0) + 1
        js["first_seq"] = min(js["first_seq"], e.get("_seq", js["first_seq"]))
        js["last_seq"] = max(js["last_seq"], e.get("_seq", js["last_seq"]))
        if et in ("COMPLETE", "FAIL", "ABORT"):
            js["status"] = et
            result = (e.get("run", {}).get("facets", {}) or {}).get("result") or {}
            if result:
                js["last_result"] = {
                    k: v for k, v in result.items() if not k.startswith("_")
                }

        for direction, key in (("input", "inputs"), ("output", "outputs")):
            for ds in e.get(key) or []:
                dname = ds.get("name")
                if not dname:
                    continue
                ds_entry = dataset_state.setdefault(
                    dname,
                    {
                        "name": dname,
                        "namespace": ds.get("namespace", DATASET_NAMESPACE),
                        "produced_by": [],
                        "consumed_by": [],
                        "facets": {},
                    },
                )
                facets = ds.get("facets") or {}
                if facets:
                    ds_entry["facets"].update(
                        {k: v for k, v in facets.items() if k not in ds_entry["facets"]}
                    )
                if direction == "output" and job_name not in ds_entry["produced_by"]:
                    ds_entry["produced_by"].append(job_name)
                if direction == "input" and job_name not in ds_entry["consumed_by"]:
                    ds_entry["consumed_by"].append(job_name)
                edges.append(
                    {"job": job_name, "dataset": dname, "direction": direction}
                )

    if run_id is None and len(run_ids) == 1:
        run_id = next(iter(run_ids))

    jobs = sorted(job_state.values(), key=lambda j: j["first_seq"])
    datasets = sorted(dataset_state.values(), key=lambda d: d["name"])

    summary = {
        "job_count": len(jobs),
        "dataset_count": len(datasets),
        "edge_count": len(edges),
        "jobs_complete": sum(1 for j in jobs if j["status"] == "COMPLETE"),
        "jobs_failed": sum(1 for j in jobs if j["status"] in ("FAIL", "ABORT")),
        "jobs_open": sum(1 for j in jobs if j["status"] is None),
    }

    return {
        "schema_version": LINEAGE_INDEX_SCHEMA,
        "captured_at": _now(),
        "run_id": run_id,
        "summary": summary,
        "jobs": jobs,
        "datasets": datasets,
        "edges": edges,
    }


def build_slide_to_source_map(
    deck: Any,
    *,
    binding_report: dict[str, Any] | None = None,
    workbook_path: Path | str | None = None,
    workbook_contract_path: Path | str | None = None,
) -> dict[str, Any]:
    """Build the slide → source-data map from the deck contract.

    For each active slide, list every concrete data source the
    builder reads (workbook sheet + columns + roles, contract
    references, builder constants). Cross-references the binding
    report's resolved column status when available so consumers can
    tell at a glance whether a slide's sources are wired correctly.

    Output answers: "Which Salesforce extract / workbook sheet /
    snapshot column fed slide N?" — one query.
    """
    profile = deck.director_monthly
    profile_id = "director_monthly"

    bindings_by_slide: dict[str | None, list[dict[str, Any]]] = {}
    for b in (binding_report or {}).get("bindings", []) or []:
        bindings_by_slide.setdefault(b.get("slide_id"), []).append(b)

    slide_entries: list[dict[str, Any]] = []
    for slide in profile.get("slides", []) or []:
        sid = slide.get("id")
        snum = slide.get("slide_number")
        sources: list[dict[str, Any]] = []

        for tbl in slide.get("tables", []) or []:
            entry: dict[str, Any] = {
                "kind": "table",
                "table_id": tbl.get("id"),
                "binding_type": tbl.get("binding_type", "direct_workbook_table"),
                "source": tbl.get("source"),
                "sheet": tbl.get("sheet"),
            }
            if tbl.get("snapshot_roles"):
                entry["snapshot_roles"] = dict(tbl["snapshot_roles"])
            cols: list[str] = []
            for col in tbl.get("columns", []) or []:
                if "source_column" in col:
                    cols.append(col["source_column"])
                elif "snapshot_role" in col:
                    cols.append(f"<role:{col['snapshot_role']}>")
                elif "computed" in col:
                    cols.append(f"<computed:{col['computed']}>")
            if cols:
                entry["physical_columns"] = cols
            if tbl.get("transform_id"):
                entry["transform_id"] = tbl["transform_id"]
            entry["resolution_status"] = _binding_status(
                bindings_by_slide.get(sid, []), kind="table", row_id=tbl.get("id")
            )
            sources.append(entry)

        if slide.get("required_takeaway", {}).get("required"):
            ta = slide["required_takeaway"]
            sources.append(
                {
                    "kind": "takeaway",
                    "binding_type": "generated_takeaway",
                    "required_metrics": list(ta.get("required_metrics", []) or []),
                    "template": ta.get("template"),
                    "resolution_status": _binding_status(
                        bindings_by_slide.get(sid, []),
                        kind="takeaway",
                        row_id=f"{sid}_takeaway",
                    ),
                }
            )

        for note in slide.get("required_source_notes", []) or []:
            sources.append({"kind": "source_note", "id": note})

        for link in slide.get("required_links", []) or []:
            sources.append(
                {
                    "kind": "link",
                    "id": link.get("id"),
                    "label": link.get("label"),
                    "link_kind": link.get("kind"),
                }
            )

        if slide.get("static") is True or sid == "legal_notice":
            sources.append(
                {
                    "kind": "static",
                    "binding_type": "legal_text"
                    if sid == "legal_notice"
                    else "static_text",
                }
            )

        slide_entries.append(
            {
                "slide_id": sid,
                "slide_number": snum,
                "title": slide.get("title"),
                "layout": slide.get("layout"),
                "purpose": slide.get("purpose"),
                "definition": slide.get("definition"),
                "period": slide.get("period"),
                "population": slide.get("population"),
                "sources": sources,
            }
        )

    # Aggregate every distinct source dataset referenced by any slide.
    distinct_datasets: dict[str, dict[str, Any]] = {}
    for slide_entry in slide_entries:
        for src in slide_entry["sources"]:
            if src.get("kind") == "table" and src.get("source") == "director_workbook":
                key = f"director_workbook::{src.get('sheet')}"
                ds = distinct_datasets.setdefault(
                    key,
                    {
                        "dataset_name": key,
                        "kind": "workbook_sheet",
                        "sheet": src.get("sheet"),
                        "consumed_by_slides": [],
                    },
                )
                if slide_entry["slide_id"] not in ds["consumed_by_slides"]:
                    ds["consumed_by_slides"].append(slide_entry["slide_id"])

    return {
        "schema_version": SLIDE_MAP_SCHEMA,
        "captured_at": _now(),
        "profile_id": profile_id,
        "deck_contract_path": str(deck.path),
        "workbook_path": str(workbook_path) if workbook_path else None,
        "workbook_contract_path": (
            str(workbook_contract_path) if workbook_contract_path else None
        ),
        "slide_count": len(slide_entries),
        "distinct_dataset_count": len(distinct_datasets),
        "slides": slide_entries,
        "distinct_datasets": sorted(
            distinct_datasets.values(), key=lambda d: d["dataset_name"]
        ),
    }


def _binding_status(
    rows: list[dict[str, Any]], *, kind: str, row_id: str | None
) -> str:
    if row_id is None:
        return "unknown"
    for r in rows:
        if r.get("kind") == kind and r.get("id") == row_id:
            return r.get("status", "unknown")
    return "unknown"


__all__ = [
    "DATASET_NAMESPACE",
    "JOB_NAMESPACE",
    "LINEAGE_INDEX_SCHEMA",
    "LineageEmitter",
    "PRODUCER",
    "SCHEMA_URL",
    "SLIDE_MAP_SCHEMA",
    "build_lineage_index",
    "build_slide_to_source_map",
    "file_dataset",
]
