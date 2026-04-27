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
) -> dict[str, Any]:
    deck = deck_contract.load(deck_contract_path)
    workbook_c = director_workbook_contract.load(workbook_contract_path)

    summaries: list[dict[str, Any]] = []
    detail_reports: dict[str, Any] = {}

    # 1. deck_contract structural
    findings, deck_report = deck_contract.validate(deck, workbook_contract=workbook_c)
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

    # 2. workbook_contract structural
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

    # 3. real workbook validation
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

    # 4. binding resolver
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

    # 5. PPTX contract
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

    # 6. brand fingerprint
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

    # 7. render gates
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

    # 8. visual regression (optional — needs soffice)
    if not skip_visual:
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
    else:
        summaries.append(
            _summarise(
                "deck_visual_regression", "skipped", 0, 0, reason="--skip-visual"
            )
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

    return {
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
    return 0 if report["publish_decision"] == "publish_ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
