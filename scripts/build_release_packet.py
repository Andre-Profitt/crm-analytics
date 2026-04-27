#!/usr/bin/env python3
"""Track G-Lite — release packet orchestrator.

Runs every Track E + Track F validator end-to-end, captures each
report, computes content digests of all input artifacts (deck contract,
workbook contract, schemas, template, anchors), and emits a single
``release_packet.json`` (plus a Markdown summary) that states
publish_decision = publish_ready / blocked / blocked_with_warnings.

This is Track G's orchestration layer ONLY. It does NOT run upstream
ETL, warehouse build, SharePoint upload, source-quality baselines, or
distribution comparison — those depend on inputs not present in
isolated sessions. The full Track G builds on top of this orchestrator.

Validators wired:

  1. deck_contract                (E1, structural)
  2. director_workbook_contract   (E1, structural)
  3. director_workbook_validation (E2, real workbook)
  4. deck_bindings                (E3, binding resolver)
  5. pptx_contract                (E4, produced PPTX)
  6. brand_fingerprint            (F4, template SHA + layouts + theme)
  7. deck_render                  (F5, geometry + footer + legal)
  8. deck_visual_regression       (F6, frozen-region hash diff)

Aggregate decision logic:

  any blocker      -> publish_decision = "blocked"
  any warning      -> publish_decision = "blocked_with_warnings"
                      (still considered safe enough to publish but
                       attention required; called out in the report)
  otherwise        -> publish_decision = "publish_ready"

Usage:
    python3 scripts/build_release_packet.py \\
        --workbook ~/Downloads/jesper-tyrer-2026-04-20.xlsx \\
        --pptx ~/Downloads/jesper-tyrer-LAND.pptx \\
        --out output/track_g/release_packet.json \\
        --md-out output/track_g/release_packet.md
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import brand_contract  # noqa: E402
from scripts.monthly_platform import deck_binding_resolver  # noqa: E402
from scripts.monthly_platform import deck_contract  # noqa: E402
from scripts.monthly_platform import director_workbook_contract  # noqa: E402
from scripts.monthly_platform import lineage  # noqa: E402  (Track J-Lite)
from scripts.validate_deck_render import validate_render  # noqa: E402
from scripts.validate_deck_visual_regression import (  # noqa: E402
    DEFAULT_BASELINE_PATH,
    DEFAULT_REGIONS_PATH,
    _load_regions,
    hash_deck,
    verify_against_baseline,
)
from scripts.validate_director_monthly_pptx import validate_pptx  # noqa: E402
from scripts.validate_track_e_workbook import (  # noqa: E402
    validate_workbook as validate_real_workbook,
)


REPORT_SCHEMA_VERSION = "monthly_platform.release_packet.v1"


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _digest_of(path: Path | str | None) -> dict[str, Any]:
    if path is None:
        return {"path": None, "sha256": None, "size": None}
    p = Path(path)
    if not p.exists():
        return {"path": str(p), "sha256": None, "size": None, "missing": True}
    return {
        "path": str(p),
        "sha256": _sha256(p),
        "size": p.stat().st_size,
    }


def _summarise(
    name: str, status: str, blockers: int, warnings: int, **extra
) -> dict[str, Any]:
    out = {
        "validator": name,
        "status": status,
        "blockers": blockers,
        "warnings": warnings,
    }
    out.update(extra)
    return out


def build_release_packet(
    *,
    workbook: Path,
    pptx: Path,
    deck_contract_path: Path | None = None,
    workbook_contract_path: Path | None = None,
    visual_baseline_path: Path = DEFAULT_BASELINE_PATH,
    visual_regions_path: Path = DEFAULT_REGIONS_PATH,
    skip_visual: bool = False,
    lineage_dir: Path | None = None,
    lineage_emitter: lineage.LineageEmitter | None = None,
) -> dict[str, Any]:
    deck = deck_contract.load(deck_contract_path)
    workbook_c = director_workbook_contract.load(workbook_contract_path)

    summaries: list[dict[str, Any]] = []
    detail_reports: dict[str, Any] = {}

    # Optional Track J-Lite lineage emission. When ``lineage_dir`` is
    # provided (and no caller-supplied emitter), wrap every validator
    # stage in OpenLineage START / COMPLETE events.
    emitter: lineage.LineageEmitter | None = lineage_emitter
    if emitter is None and lineage_dir is not None:
        emitter = lineage.LineageEmitter(out_dir=lineage_dir)

    contract_inputs = [
        ds
        for ds in (
            lineage.file_dataset(deck.path),
            lineage.file_dataset(workbook_c.path),
        )
        if ds is not None
    ]

    def _open(stage: str, *, inputs=None, outputs=None) -> None:
        if emitter is not None:
            emitter.start_job(stage, inputs=inputs or [], outputs=outputs or [])

    def _close(stage: str, status: str, summary: dict[str, Any], **extra) -> None:
        if emitter is None:
            return
        ol_status = "COMPLETE" if status in ("pass", "skipped") else "FAIL"
        emitter.complete_job(
            stage,
            status=ol_status,
            result_facets={
                "validator_status": status,
                "blockers": summary.get("blockers", 0),
                "warnings": summary.get("warnings", 0),
                **{k: v for k, v in extra.items() if v is not None},
            },
        )

    # 1. deck_contract structural
    _open("deck_contract", inputs=contract_inputs)
    _, deck_report = deck_contract.validate(deck, workbook_contract=workbook_c)
    summaries.append(
        _summarise(
            "deck_contract",
            deck_report["status"],
            deck_report["blocker_count"],
            deck_report["warning_count"],
            slides=deck_report["director_monthly_slide_count"],
        )
    )
    detail_reports["deck_contract"] = deck_report
    _close("deck_contract", deck_report["status"], summaries[-1])

    # 2. workbook_contract structural
    _open("director_workbook_contract", inputs=contract_inputs[1:2])
    _, wbc_report = director_workbook_contract.validate(workbook_c)
    summaries.append(
        _summarise(
            "director_workbook_contract",
            wbc_report["status"],
            wbc_report["blocker_count"],
            wbc_report["warning_count"],
            sheets=wbc_report["sheet_count"],
            roles=wbc_report["snapshot_role_count"],
        )
    )
    detail_reports["director_workbook_contract"] = wbc_report
    _close("director_workbook_contract", wbc_report["status"], summaries[-1])

    # 3. real workbook validation
    workbook_ds = lineage.file_dataset(workbook)
    _open(
        "director_workbook_validation",
        inputs=[contract_inputs[1]] + ([workbook_ds] if workbook_ds else []),
    )
    real_wb_report = validate_real_workbook(workbook, contract=workbook_c)
    summaries.append(
        _summarise(
            "director_workbook_validation",
            real_wb_report["status"],
            real_wb_report["blocker_count"],
            real_wb_report["warning_count"],
            sheets_present=f"{real_wb_report['sheet_count_present']}/{real_wb_report['sheet_count_declared']}",
            roles_resolved=sum(
                1
                for r in real_wb_report["resolved_snapshot_roles"]
                if r["status"] == "pass"
            ),
        )
    )
    detail_reports["director_workbook_validation"] = real_wb_report
    _close("director_workbook_validation", real_wb_report["status"], summaries[-1])

    # 4. binding resolver
    _open(
        "deck_bindings",
        inputs=contract_inputs + ([workbook_ds] if workbook_ds else []),
    )
    binding_report = deck_binding_resolver.resolve(
        workbook_path=workbook, deck=deck, workbook=workbook_c
    )
    summaries.append(
        _summarise(
            "deck_bindings",
            binding_report["status"],
            binding_report["fail_count"],
            binding_report["warning_count"],
            bindings=binding_report["binding_count"],
        )
    )
    detail_reports["deck_bindings"] = binding_report
    _close("deck_bindings", binding_report["status"], summaries[-1])

    # 5. PPTX contract
    pptx_ds = lineage.file_dataset(pptx)
    _open(
        "pptx_contract",
        inputs=[contract_inputs[0]] + ([pptx_ds] if pptx_ds else []),
    )
    pptx_report = validate_pptx(pptx, contract=deck)
    summaries.append(
        _summarise(
            "pptx_contract",
            pptx_report["status"],
            pptx_report["blocker_count"],
            pptx_report["warning_count"],
            stable_titles=pptx_report["stable_title_count"],
            slides=f"{pptx_report['actual_slide_count']}/{pptx_report['expected_slide_count']}",
        )
    )
    detail_reports["pptx_contract"] = pptx_report
    _close("pptx_contract", pptx_report["status"], summaries[-1])

    # 6. brand fingerprint
    template_ds = lineage.file_dataset(
        REPO_ROOT / "assets" / "SimCorp_PPT_Template.pptx"
    )
    _open(
        "brand_fingerprint",
        inputs=[contract_inputs[0]] + ([template_ds] if template_ds else []),
    )
    brand_report = brand_contract.validate_brand(deck)
    summaries.append(
        _summarise(
            "brand_fingerprint",
            brand_report.status,
            brand_report.blocker_count,
            brand_report.warning_count,
            sha=(
                "match"
                if brand_report.template_sha256 == brand_report.expected_sha256
                else "MISMATCH"
            ),
            layouts_missing=len(brand_report.layouts_missing),
        )
    )
    detail_reports["brand_fingerprint"] = brand_report.as_dict()
    _close("brand_fingerprint", brand_report.status, summaries[-1])

    # 7. render gates
    _open(
        "deck_render",
        inputs=[contract_inputs[0]] + ([pptx_ds] if pptx_ds else []),
    )
    render_report = validate_render(pptx, contract=deck)
    summaries.append(
        _summarise(
            "deck_render",
            render_report["status"],
            render_report["blocker_count"],
            render_report["warning_count"],
            slides=render_report["slide_count"],
        )
    )
    detail_reports["deck_render"] = render_report
    _close("deck_render", render_report["status"], summaries[-1])

    # 8. visual regression (optional — needs soffice)
    if not skip_visual:
        baseline_ds = lineage.file_dataset(visual_baseline_path)
        regions_ds = lineage.file_dataset(visual_regions_path)
        _open(
            "deck_visual_regression",
            inputs=[ds for ds in (baseline_ds, regions_ds, pptx_ds) if ds is not None],
        )
        regions = _load_regions(visual_regions_path)
        snapshot = hash_deck(pptx, contract=deck, regions=regions, dpi=100)
        visual_report = verify_against_baseline(snapshot, visual_baseline_path)
        summaries.append(
            _summarise(
                "deck_visual_regression",
                visual_report["status"],
                visual_report["blocker_count"],
                visual_report["warning_count"],
                slides_checked=f"{visual_report['slide_count']}/{visual_report['baseline_slide_count']}",
            )
        )
        detail_reports["deck_visual_regression"] = visual_report
        _close("deck_visual_regression", visual_report["status"], summaries[-1])
    else:
        summaries.append(
            _summarise(
                "deck_visual_regression", "skipped", 0, 0, reason="--skip-visual"
            )
        )
        if emitter is not None:
            emitter.start_job("deck_visual_regression")
            emitter.complete_job(
                "deck_visual_regression",
                status="ABORT",
                result_facets={
                    "validator_status": "skipped",
                    "reason": "--skip-visual",
                },
            )

    # Aggregate decision
    total_blockers = sum(int(s["blockers"]) for s in summaries)
    total_warnings = sum(int(s["warnings"]) for s in summaries)
    if total_blockers > 0:
        decision = "blocked"
    elif total_warnings > 0:
        decision = "blocked_with_warnings"
    else:
        decision = "publish_ready"

    # Input artifact digests for provenance.
    artifact_digests = {
        "deck_contract_yaml": _digest_of(deck.path),
        "workbook_contract_yaml": _digest_of(workbook_c.path),
        "deck_contract_schema_json": _digest_of(
            REPO_ROOT / "schemas" / "deck_contract.schema.json"
        ),
        "workbook_contract_schema_json": _digest_of(
            REPO_ROOT / "schemas" / "director_workbook_contract.schema.json"
        ),
        "template_pptx": _digest_of(REPO_ROOT / "assets" / "SimCorp_PPT_Template.pptx"),
        "visual_regions_yaml": _digest_of(visual_regions_path),
        "visual_baseline_json": _digest_of(visual_baseline_path),
        "live_workbook_xlsx": _digest_of(workbook),
        "produced_pptx": _digest_of(pptx),
    }

    report: dict[str, Any] = {
        "schema_version": REPORT_SCHEMA_VERSION,
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "publish_decision": decision,
        "blocker_total": total_blockers,
        "warning_total": total_warnings,
        "validator_count": len(summaries),
        "validators_pass": sum(1 for s in summaries if s["status"] == "pass"),
        "validators_fail": sum(1 for s in summaries if s["status"] == "fail"),
        "validators_skipped": sum(1 for s in summaries if s["status"] == "skipped"),
        "summaries": summaries,
        "artifact_digests": artifact_digests,
        "detail_reports": detail_reports,
    }

    # If lineage was emitted, materialise lineage_index.json and the
    # slide_to_source_map.json into the same out_dir, and surface their
    # paths + run_id in the packet for downstream waiver/release tooling.
    if emitter is not None:
        index = lineage.build_lineage_index(emitter.events_dir, run_id=emitter.run_id)
        index_path = emitter.out_dir / "lineage_index.json"
        index_path.write_text(
            json.dumps(index, indent=2, default=str) + "\n", encoding="utf-8"
        )

        slide_map = lineage.build_slide_to_source_map(
            deck,
            binding_report=binding_report,
            workbook_path=workbook,
            workbook_contract_path=workbook_c.path,
        )
        slide_map_path = emitter.out_dir / "slide_to_source_map.json"
        slide_map_path.write_text(
            json.dumps(slide_map, indent=2, default=str) + "\n", encoding="utf-8"
        )

        report["lineage"] = {
            "run_id": emitter.run_id,
            "out_dir": str(emitter.out_dir),
            "events_dir": str(emitter.events_dir),
            "lineage_index_path": str(index_path),
            "slide_to_source_map_path": str(slide_map_path),
            "event_count": len(emitter.all_events()),
            "job_count": index["summary"]["job_count"],
            "dataset_count": index["summary"]["dataset_count"],
            "slide_count": slide_map["slide_count"],
            "distinct_dataset_count": slide_map["distinct_dataset_count"],
        }

    return report


def render_markdown(report: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Track G — release packet\n")
    lines.append(f"- captured_at: {report['captured_at']}")
    lines.append(f"- **publish_decision: {report['publish_decision']}**")
    lines.append(
        f"- validators: {report['validators_pass']}/{report['validator_count']} pass, "
        f"{report['validators_fail']} fail, {report['validators_skipped']} skipped"
    )
    lines.append(f"- blockers across all validators: {report['blocker_total']}")
    lines.append(f"- warnings across all validators: {report['warning_total']}")
    lines.append("")
    lines.append("## Per-validator summary\n")
    lines.append("| Validator | Status | Blockers | Warnings | Detail |")
    lines.append("| --- | --- | ---: | ---: | --- |")
    for s in report["summaries"]:
        extras = {
            k: v
            for k, v in s.items()
            if k not in ("validator", "status", "blockers", "warnings")
        }
        detail_str = " · ".join(f"{k}={v}" for k, v in extras.items())
        lines.append(
            f"| `{s['validator']}` | {s['status']} | {s['blockers']} | "
            f"{s['warnings']} | {detail_str} |"
        )
    lines.append("")
    lines.append("## Artifact digests\n")
    lines.append("| Artifact | SHA-256 | Size (bytes) |")
    lines.append("| --- | --- | ---: |")
    for name, dig in report["artifact_digests"].items():
        sha = (dig.get("sha256") or "—")[:16]
        size = dig.get("size") if dig.get("size") is not None else "—"
        lines.append(f"| {name} | `{sha}…` | {size} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build release packet (Track G-Lite orchestrator)."
    )
    parser.add_argument("--workbook", required=True, type=Path)
    parser.add_argument("--pptx", required=True, type=Path)
    parser.add_argument("--deck-contract", default=None)
    parser.add_argument("--workbook-contract", default=None)
    parser.add_argument("--visual-baseline", type=Path, default=DEFAULT_BASELINE_PATH)
    parser.add_argument("--visual-regions", type=Path, default=DEFAULT_REGIONS_PATH)
    parser.add_argument("--skip-visual", action="store_true")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--md-out", type=Path, default=None)
    parser.add_argument("--show-summaries", action="store_true")
    parser.add_argument(
        "--lineage-dir",
        type=Path,
        default=None,
        help=(
            "When set, emit OpenLineage START/COMPLETE events per "
            "validator stage plus lineage_index.json + slide_to_source_map.json."
        ),
    )
    args = parser.parse_args(argv)

    if not args.workbook.exists():
        print(f"ERROR: workbook not found: {args.workbook}", file=sys.stderr)
        return 2
    if not args.pptx.exists():
        print(f"ERROR: pptx not found: {args.pptx}", file=sys.stderr)
        return 2

    report = build_release_packet(
        workbook=args.workbook,
        pptx=args.pptx,
        deck_contract_path=args.deck_contract,
        workbook_contract_path=args.workbook_contract,
        visual_baseline_path=args.visual_baseline,
        visual_regions_path=args.visual_regions,
        skip_visual=args.skip_visual,
        lineage_dir=args.lineage_dir,
    )

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )
        print(f"packet: {args.out}")
    if args.md_out:
        args.md_out.parent.mkdir(parents=True, exist_ok=True)
        args.md_out.write_text(render_markdown(report), encoding="utf-8")
        print(f"md: {args.md_out}")

    if args.show_summaries:
        for s in report["summaries"]:
            print(
                f"  {s['validator']:30s} {s['status']:10s} blockers={s['blockers']} warnings={s['warnings']}"
            )

    print(
        f"release_packet: publish_decision={report['publish_decision']} "
        f"({report['validators_pass']}/{report['validator_count']} pass, "
        f"{report['blocker_total']} blockers, {report['warning_total']} warnings)"
    )
    if "lineage" in report:
        ln = report["lineage"]
        print(
            f"lineage: run_id={ln['run_id']} "
            f"events={ln['event_count']} jobs={ln['job_count']} "
            f"datasets={ln['dataset_count']} "
            f"slides={ln['slide_count']} -> {ln['out_dir']}"
        )
    return 0 if report["publish_decision"] == "publish_ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
