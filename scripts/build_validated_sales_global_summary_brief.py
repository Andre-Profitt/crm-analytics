#!/usr/bin/env python3
"""Build validated global-summary fact packs and structured PowerPoint payloads."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_global_summary_shell.json"


def load_snapshot(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_shell_contract(path: Path = DEFAULT_SHELL_CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def as_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def markdown_section(lines: list[str], heading: str, bullets: list[str]) -> None:
    lines.append(f"## {heading}")
    lines.extend(f"- {bullet}" for bullet in bullets)
    lines.append("")


def build_authoritative_brief(snapshot: dict[str, Any]) -> str:
    global_summary = snapshot.get("global_summary") or {}
    commercial = snapshot.get("commercial_approval") or {}
    regions = snapshot.get("regions") or []
    approved_by_region = ", ".join(
        f"{row['region_name']} {row['arr_eur']} ({row['deal_count']} deals)"
        for row in commercial.get("approved_2026_by_region", [])
    ) or "n/a"
    missing_by_region = ", ".join(
        f"{row['region_name']} {row['arr_eur']} ({row['candidate_count']} deals)"
        for row in commercial.get("missing_approval_by_region", [])
    ) or "n/a"

    lines: list[str] = [
        "# Validated Global Summary Fact Pack",
        "",
        f"Snapshot date: {snapshot.get('snapshot_date')}",
        "",
        "Use this as the authoritative bridge into the global summary deck.",
        "This is a deterministic rollup of validated regional snapshots.",
        "",
    ]
    markdown_section(
        lines,
        "Global Executive Summary",
        [
            f"Global Q2 active pipeline is {as_text(global_summary.get('global_pipeline_arr_q2')) or '—'} ARR.",
            f"Global Q2 renewal book is {as_text(global_summary.get('global_renewal_acv_q2')) or '—'} ACV.",
            f"Global missing commercial-approval candidates total {global_summary.get('global_missing_approval_count', 0)} deals.",
            as_text(global_summary.get("global_top_risk")) or "No global risk summary available.",
            as_text(global_summary.get("global_top_action")) or "No global action summary available.",
        ],
    )
    for region in regions:
        quarter_title = as_text(region.get("quarterly_pipeline_title")) or "Q2"
        footnote = as_text(region.get("quarterly_pipeline_footnote"))
        markdown_section(
            lines,
            f"{as_text(region.get('region_name'))} Summary",
            [
                f"{quarter_title} active pipeline is {as_text(region.get('headline_pipeline_arr_q2')) or '—'} ARR, with Commit at {as_text(region.get('q2_commit_arr')) or '—'} and Best Case at {as_text(region.get('q2_best_case_arr')) or '—'}.",
                f"Approval rate for stage 3+ deals is {as_text(region.get('approval_rate_stage3_plus')) or '—'}, and open renewals total {as_text(region.get('renewal_open_acv')) or '—'} ACV.",
                as_text(region.get("top_risk")) or "No regional risk summary available.",
                as_text(region.get("top_action")) or "No regional action summary available.",
                *([footnote] if footnote else []),
            ],
        )
    markdown_section(
        lines,
        "Global Commercial Approval Overview",
        [
            f"Approved 2026 exposure by region: {approved_by_region}.",
            f"Missing-approval exposure by region: {missing_by_region}.",
        ],
    )
    return "\n".join(lines).strip() + "\n"


def build_structured_fill_payload(snapshot: dict[str, Any]) -> dict[str, Any]:
    shell = load_shell_contract()
    global_summary = snapshot.get("global_summary") or {}
    regions = {row["region_name"]: row for row in (snapshot.get("regions") or [])}
    commercial = snapshot.get("commercial_approval") or {}

    slots_by_slide = {
        "global-executive-summary": {
            "global_pipeline_arr_q2": as_text(global_summary.get("global_pipeline_arr_q2")) or "—",
            "global_renewal_acv_q2": as_text(global_summary.get("global_renewal_acv_q2")) or "—",
            "global_missing_approval_count": str(global_summary.get("global_missing_approval_count", 0)),
            "global_top_risk": as_text(global_summary.get("global_top_risk")),
            "global_top_action": as_text(global_summary.get("global_top_action")),
        },
        "apac-region-summary": regions.get("APAC", {}),
        "emea-region-summary": regions.get("EMEA", {}),
        "north-america-region-summary": regions.get("North America", {}),
        "global-commercial-approval-overview": {
            "approved_2026_by_region": commercial.get("approved_2026_by_region") or [],
            "missing_approval_by_region": commercial.get("missing_approval_by_region") or [],
            "largest_global_missing_candidates": commercial.get("largest_global_missing_candidates") or [],
        },
        "global-appendix": {
            "metric_definition_notes": snapshot.get("metric_definition_notes") or [],
            "region_rollup_notes": snapshot.get("region_rollup_notes") or [],
            "known_gaps": snapshot.get("known_gaps") or [],
        },
    }

    slides: list[dict[str, Any]] = []
    for slide in shell.get("slides", []):
        slide_id = slide["id"]
        slides.append(
            {
                "id": slide_id,
                "title": slide["title"],
                "support_level": (slide.get("data_contract") or {}).get("support_level"),
                "required_slots": slide.get("required_slots", []),
                "known_gaps": (slide.get("data_contract") or {}).get("known_gaps", []),
                "slots": slots_by_slide.get(slide_id, {}),
            }
        )
    return {
        "template_name": shell.get("template_name"),
        "snapshot_date": snapshot.get("snapshot_date"),
        "slides": slides,
    }


def build_powerpoint_build_prompt(snapshot: dict[str, Any], validated_brief: str) -> str:
    shell = load_shell_contract()
    structured_fill_payload = build_structured_fill_payload(snapshot)
    slide_lines = []
    for index, slide in enumerate(shell.get("slides", []), start=1):
        slide_lines.append(f"{index}. {slide['title']} (`{slide['id']}`)")
        slide_lines.append(
            "   Required slots: " + ", ".join(f"`{slot}`" for slot in slide.get("required_slots", []))
        )
    return (
        "Update the current global summary PowerPoint deck.\n\n"
        "Treat the validated fact pack below as the authoritative source of truth. "
        "Preserve the current template, slide master, layouts, fonts, and branding.\n\n"
        "Replace the shell guidance with executive-ready content for these slides:\n"
        + "\n".join(slide_lines)
        + "\n\nRules:\n"
        "- keep pipeline metrics as ARR and renewals as ACV\n"
        "- keep each regional summary slide tied only to its validated regional rollup\n"
        "- use the structured fill payload JSON below as the primary slot map\n"
        "- keep the global deck concise and executive-facing\n"
        "- if a slot is unsupported, leave a crisp placeholder note instead of fabricating content\n\n"
        "Validated fact pack:\n\n"
        f"{validated_brief}\n\n"
        "Structured fill payload (JSON):\n\n"
        f"{json.dumps(structured_fill_payload, indent=2, ensure_ascii=True)}"
    )


def build_validation_artifacts(snapshot: dict[str, Any]) -> dict[str, Any]:
    validated_brief = build_authoritative_brief(snapshot)
    structured_fill_payload = build_structured_fill_payload(snapshot)
    powerpoint_build_prompt = build_powerpoint_build_prompt(snapshot, validated_brief)
    return {
        "validated_brief": validated_brief,
        "structured_fill_payload": structured_fill_payload,
        "powerpoint_build_prompt": powerpoint_build_prompt,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    snapshot = load_snapshot(args.snapshot)
    artifacts = build_validation_artifacts(snapshot)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "validated-fact-pack.md").write_text(artifacts["validated_brief"], encoding="utf-8")
    (args.output_dir / "powerpoint-fill-payload.json").write_text(
        json.dumps(artifacts["structured_fill_payload"], indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    (args.output_dir / "powerpoint-build-prompt.txt").write_text(
        artifacts["powerpoint_build_prompt"], encoding="utf-8"
    )
    print(
        json.dumps(
            {
                "validated_fact_pack": str(args.output_dir / "validated-fact-pack.md"),
                "powerpoint_fill_payload": str(args.output_dir / "powerpoint-fill-payload.json"),
                "powerpoint_build_prompt": str(args.output_dir / "powerpoint-build-prompt.txt"),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
