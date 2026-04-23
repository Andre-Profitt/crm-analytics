#!/usr/bin/env python3
"""Build a publish-grade release packet for the latest director + global sales deck assets."""

from __future__ import annotations

import argparse
import json
import math
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DIRECTOR_RUN_ROOT = (
    REPO_ROOT / "output" / "sales_director_monthly_master_builder"
)
DEFAULT_GLOBAL_RUN_ROOT = REPO_ROOT / "output" / "sales_global_summary_builder"
DEFAULT_GLOBAL_CANONICAL_RUN_ROOT = (
    REPO_ROOT / "output" / "sales_global_canonical_shell_builder"
)
DEFAULT_DIRECTOR_CANONICAL_ROOT = (
    REPO_ROOT / "output" / "sales_director_canonical_shells"
)
DEFAULT_GLOBAL_CANONICAL_ROOT = REPO_ROOT / "output" / "sales_global_canonical_shells"
DEFAULT_EXTERNAL_SOURCE_PACKET_ROOT = (
    REPO_ROOT / "output" / "sales_deck_external_source_packets"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_deck_release_packets"
DEFAULT_SHAREPOINT_ROOT = REPO_ROOT / "output" / "sharepoint"

_TERRITORY_TO_PUBLISH_LABEL: dict[str, str] = {
    "APAC": "APAC",
    "Central Europe": "Central Europe",
    "UK & Ireland": "UK & Ireland",
    "Southern Europe": "Southern Europe",
    "NL & Nordics": "NL & Nordics",
    "Middle East & Africa": "Middle East & Africa",
    "Canada": "Canada",
    "NA Asset Management": "NA Asset Management",
    "Pension & Insurance": "NA Pension & Insurance",
}

_TERRITORY_PUBLISH_LABELS_FALLBACK: dict[str, str] = dict(_TERRITORY_TO_PUBLISH_LABEL)


def _load_territory_publish_labels() -> dict[str, str]:
    cfg_path = (
        Path(__file__).resolve().parents[1] / "config" / "sd_monthly_territories.json"
    )
    if not cfg_path.exists():
        return _TERRITORY_PUBLISH_LABELS_FALLBACK
    try:
        territories = json.loads(cfg_path.read_text(encoding="utf-8")).get(
            "territories", {}
        )
        return {key: _TERRITORY_TO_PUBLISH_LABEL.get(key, key) for key in territories}
    except Exception:
        return _TERRITORY_PUBLISH_LABELS_FALLBACK


_TERRITORY_PUBLISH_LABELS: dict[str, str] = _load_territory_publish_labels()


def _expected_director_count() -> int:
    cfg_path = (
        Path(__file__).resolve().parents[1] / "config" / "sd_monthly_territories.json"
    )
    if cfg_path.exists():
        return len(
            json.loads(cfg_path.read_text(encoding="utf-8")).get("territories", {})
        )
    return 9


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def latest_run_dir(root: Path, snapshot_date: str) -> Path:
    dated_root = root / snapshot_date
    if not dated_root.exists():
        raise FileNotFoundError(
            f"No run directory found for snapshot date under {dated_root}"
        )
    candidates = sorted(
        (path for path in dated_root.iterdir() if path.is_dir()), reverse=True
    )
    if not candidates:
        raise FileNotFoundError(
            f"No run directory found for snapshot date under {dated_root}"
        )
    return candidates[0]


def load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return load_json(path)


def _path_exists(path_value: str | None) -> bool:
    return bool(path_value) and Path(path_value).exists()


def month_year_label(snapshot_date: str) -> str:
    return datetime.strptime(snapshot_date[:10], "%Y-%m-%d").strftime("%B %Y")


def quarter_label(snapshot_date: str) -> str:
    dt = datetime.strptime(snapshot_date[:10], "%Y-%m-%d")
    return f"Q{math.ceil(dt.month / 3)} {dt.year}"


def professional_director_deck_name(*, territory: str, snapshot_date: str) -> str:
    territory_label = _TERRITORY_PUBLISH_LABELS.get(territory, territory)
    return f"Sales Director Monthly - {territory_label} - {month_year_label(snapshot_date)}.pptx"


def professional_global_deck_name(snapshot_date: str) -> str:
    return (
        f"Sales Director Monthly - Exec Rollup - {month_year_label(snapshot_date)}.pptx"
    )


def professional_workbook_name(source_name: str, snapshot_date: str) -> str:
    month_year = month_year_label(snapshot_date)
    if source_name == "Dashboard and Q1 Analysis.xlsx":
        return f"Dashboard and Q1 Analysis - {month_year}.xlsx"
    if (
        source_name.startswith("FY")
        and " Pipeline Review, " in source_name
        and source_name.endswith(".xlsx")
    ):
        stem = source_name[:-5]
        prefix, region = stem.split(" Pipeline Review, ", 1)
        return f"{prefix} Pipeline Review - {region} - {month_year}.xlsx"
    return source_name


def _copy_publish_asset(
    *,
    source_path: Path,
    destination_dir: Path,
    publish_name: str,
    category: str,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    destination_dir.mkdir(parents=True, exist_ok=True)
    publish_path = destination_dir / publish_name
    shutil.copy2(source_path, publish_path)
    payload = {
        "category": category,
        "source_path": str(source_path),
        "publish_name": publish_name,
        "publish_path": str(publish_path),
    }
    if metadata:
        payload.update(metadata)
    return payload


def materialize_publish_assets(
    *,
    run_dir: Path,
    packet: dict[str, Any],
    sharepoint_root: Path | None,
) -> dict[str, Any]:
    publish_root = run_dir / "publish-assets"
    deck_root = publish_root / "decks"
    workbook_root = publish_root / "workbooks"
    snapshot_date = str(packet["snapshot_date"])
    assets: list[dict[str, Any]] = []

    for row in (packet.get("director_release") or {}).get("targets") or []:
        deck_path_value = row.get("deck_path")
        if not deck_path_value:
            continue
        deck_path = Path(str(deck_path_value))
        if not deck_path.exists():
            continue
        assets.append(
            _copy_publish_asset(
                source_path=deck_path,
                destination_dir=deck_root,
                publish_name=professional_director_deck_name(
                    territory=str(row.get("territory") or ""),
                    snapshot_date=snapshot_date,
                ),
                category="director_deck",
                metadata={
                    "director_name": row.get("director_name"),
                    "territory": row.get("territory"),
                },
            )
        )

    global_deck_value = (packet.get("global_release") or {}).get("deck_path")
    if global_deck_value:
        global_deck = Path(str(global_deck_value))
        if global_deck.exists():
            assets.append(
                _copy_publish_asset(
                    source_path=global_deck,
                    destination_dir=deck_root,
                    publish_name=professional_global_deck_name(snapshot_date),
                    category="global_deck",
                )
            )

    if sharepoint_root and sharepoint_root.exists():
        for workbook_path in sorted(sharepoint_root.glob("*.xlsx")):
            if workbook_path.name.startswith("~"):
                continue
            assets.append(
                _copy_publish_asset(
                    source_path=workbook_path,
                    destination_dir=workbook_root,
                    publish_name=professional_workbook_name(
                        workbook_path.name, snapshot_date
                    ),
                    category="sharepoint_workbook",
                )
            )

    packet["publish_assets"] = {
        "asset_root": str(publish_root),
        "quarter_folder": quarter_label(snapshot_date),
        "sharepoint_root": str(sharepoint_root) if sharepoint_root else None,
        "asset_count": len(assets),
        "assets": assets,
    }
    return packet


def _extract_quarterly_pipeline_disclosures(
    fill_payload: dict[str, Any] | None,
) -> dict[str, list[dict[str, Any]]]:
    disclosures = {
        "forward_quarter_fallbacks": [],
        "empty_quarter_regions": [],
    }
    if not fill_payload:
        return disclosures
    for slide in fill_payload.get("slides", []):
        slots = slide.get("slots") or {}
        reason = str(slots.get("quarterly_pipeline_display_reason") or "").strip()
        if not reason:
            continue
        row = {
            "slide_id": slide.get("id"),
            "region_name": str(
                slots.get("region_name") or slide.get("id") or ""
            ).strip(),
            "quarterly_pipeline_title": str(
                slots.get("quarterly_pipeline_title") or ""
            ).strip()
            or None,
            "quarterly_pipeline_footnote": str(
                slots.get("quarterly_pipeline_footnote") or ""
            ).strip()
            or None,
        }
        if reason == "forward_quarter_fallback":
            disclosures["forward_quarter_fallbacks"].append(row)
        elif reason == "empty_current_and_forward":
            disclosures["empty_quarter_regions"].append(row)
    return disclosures


def _extract_director_quarterly_pipeline_disclosure(
    fill_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not fill_payload:
        return None
    director_name = str(fill_payload.get("director_name") or "").strip() or None
    territory = str(fill_payload.get("territory") or "").strip() or None
    candidate_slides = []
    for slide in fill_payload.get("slides", []):
        slots = slide.get("slots") or {}
        reason = str(slots.get("quarterly_pipeline_display_reason") or "").strip()
        if not reason:
            continue
        candidate_slides.append(
            {
                "slide_id": slide.get("id"),
                "display_reason": reason,
                "quarterly_pipeline_title": str(
                    slots.get("quarterly_pipeline_title") or ""
                ).strip()
                or None,
                "quarterly_pipeline_footnote": str(
                    slots.get("quarterly_pipeline_footnote") or ""
                ).strip()
                or None,
            }
        )
    if not candidate_slides:
        return None
    primary = next(
        (
            slide
            for slide in candidate_slides
            if slide.get("slide_id") == "quarterly-pipeline"
        ),
        candidate_slides[0],
    )
    return {
        "director_name": director_name,
        "territory": territory,
        **primary,
    }


def _director_rows(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for target in manifest.get("targets", []):
        stages = target.get("stages", {})
        bridge = stages.get("validated_bridge", {})
        render = stages.get("deterministic_preview_render", {})
        audit = stages.get("deterministic_preview_audit", {})
        layout = stages.get("deterministic_preview_layout_audit", {})
        font_report = render.get("font_report", {}) if isinstance(render, dict) else {}
        fill_payload_path = bridge.get("powerpoint_fill_payload")
        fill_payload = (
            load_optional_json(Path(fill_payload_path)) if fill_payload_path else None
        )
        rows.append(
            {
                "director_name": target.get("director_name"),
                "territory": target.get("territory"),
                "status": target.get("status"),
                "validated_fact_pack": bridge.get("validated_fact_pack"),
                "validation_report": bridge.get("validation_report"),
                "fill_payload": bridge.get("powerpoint_fill_payload"),
                "deck_path": stages.get("deterministic_preview", {}).get("deck_path"),
                "montage_path": render.get("montage_path"),
                "audit_ok": bool(audit.get("ok")),
                "audit_findings": int(audit.get("finding_count") or 0),
                "layout_ok": bool(layout.get("ok")),
                "font_missing_count": len(font_report.get("font_missing_overall", [])),
                "font_substituted_count": len(
                    font_report.get("font_substituted_overall", [])
                ),
                "powerpoint_review_status": stages.get("powerpoint_review", {}).get(
                    "status"
                ),
                "powerpoint_review_reason": stages.get("powerpoint_review", {}).get(
                    "reason"
                ),
                "quarterly_pipeline_disclosure": _extract_director_quarterly_pipeline_disclosure(
                    fill_payload
                ),
            }
        )
    return rows


def build_director_release(director_run_dir: Path) -> dict[str, Any]:
    manifest = load_json(director_run_dir / "manifest.json")
    summary_path = director_run_dir / "summary.json"
    summary = load_json(summary_path) if summary_path.exists() else {}
    promotion_path = director_run_dir / "canonical-promotion-summary.json"
    promotion = load_json(promotion_path) if promotion_path.exists() else {}
    rows = _director_rows(manifest)

    blockers: list[str] = []
    expected = _expected_director_count()
    if len(rows) < expected:
        blockers.append(
            f"Only {len(rows)}/{expected} directors present (expected all {expected})"
        )
    if manifest.get("status") != "ok":
        blockers.append("Director batch manifest is not ok.")
    if any(row["status"] != "ok" for row in rows):
        bad = [row["director_name"] for row in rows if row["status"] != "ok"]
        blockers.append(f"Director targets not ok: {', '.join(bad)}.")
    if any(not _path_exists(row["validated_fact_pack"]) for row in rows):
        bad = [
            row["director_name"]
            for row in rows
            if not _path_exists(row["validated_fact_pack"])
        ]
        blockers.append(f"Director validated fact packs missing: {', '.join(bad)}.")
    if any(not _path_exists(row["validation_report"]) for row in rows):
        bad = [
            row["director_name"]
            for row in rows
            if not _path_exists(row["validation_report"])
        ]
        blockers.append(f"Director validation reports missing: {', '.join(bad)}.")
    if any(not row["audit_ok"] or row["audit_findings"] for row in rows):
        bad = [
            row["director_name"]
            for row in rows
            if (not row["audit_ok"] or row["audit_findings"])
        ]
        blockers.append(f"Director preview audits not clean: {', '.join(bad)}.")
    if any(not row["layout_ok"] for row in rows):
        bad = [row["director_name"] for row in rows if not row["layout_ok"]]
        blockers.append(f"Director layout audits not clean: {', '.join(bad)}.")
    if any(row["font_missing_count"] or row["font_substituted_count"] for row in rows):
        bad = [
            row["director_name"]
            for row in rows
            if row["font_missing_count"] or row["font_substituted_count"]
        ]
        blockers.append(f"Director font reports not clean: {', '.join(bad)}.")
    if any(
        row["powerpoint_review_status"] not in {"ok", "skipped"}
        or (
            row["powerpoint_review_status"] == "skipped"
            and not row["powerpoint_review_reason"]
        )
        for row in rows
    ):
        bad = [
            row["director_name"]
            for row in rows
            if row["powerpoint_review_status"] not in {"ok", "skipped"}
            or (
                row["powerpoint_review_status"] == "skipped"
                and not row["powerpoint_review_reason"]
            )
        ]
        blockers.append(
            f"Director PowerPoint review stage missing or ambiguous: {', '.join(bad)}."
        )
    if promotion:
        promoted_count = int(promotion.get("promoted_count") or 0)
        if promoted_count != len(rows):
            blockers.append(
                f"Director canonical promotion count mismatch: promoted {promoted_count}, expected {len(rows)}."
            )
    else:
        blockers.append("Director canonical promotion summary is missing.")

    return {
        "run_dir": str(director_run_dir),
        "manifest_path": str(director_run_dir / "manifest.json"),
        "summary_path": str(summary_path) if summary_path.exists() else None,
        "summary_markdown_path": str(director_run_dir / "summary.md")
        if (director_run_dir / "summary.md").exists()
        else None,
        "canonical_promotion_summary_path": str(promotion_path)
        if promotion_path.exists()
        else None,
        "target_count": len(rows),
        "targets": rows,
        "blockers": blockers,
        "ok": not blockers,
        "summary": summary,
    }


def build_global_release(
    global_run_dir: Path, global_canonical_run_dir: Path | None
) -> dict[str, Any]:
    manifest = load_json(global_run_dir / "manifest.json")
    audit = manifest.get("deterministic_preview_audit", {})
    render = manifest.get("deterministic_preview_render", {})
    font_report = render.get("font_report", {}) if isinstance(render, dict) else {}
    canonical_manifest = (
        load_json(global_canonical_run_dir / "manifest.json")
        if global_canonical_run_dir
        else None
    )
    fill_payload_path = manifest.get("powerpoint_fill_payload_path")
    fill_payload = (
        load_optional_json(Path(fill_payload_path)) if fill_payload_path else None
    )
    quarterly_pipeline_disclosures = _extract_quarterly_pipeline_disclosures(
        fill_payload
    )

    blockers: list[str] = []
    if manifest.get("status") != "ok":
        blockers.append("Global manifest is not ok.")
    if not _path_exists(manifest.get("validated_fact_pack_path")):
        blockers.append("Global validated fact pack is missing.")
    if not audit.get("ok") or int(audit.get("finding_count") or 0) != 0:
        blockers.append("Global preview audit is not clean.")
    if font_report.get("font_missing_overall") or font_report.get(
        "font_substituted_overall"
    ):
        blockers.append("Global font report is not clean.")
    powerpoint_build = manifest.get("powerpoint_build", {})
    if powerpoint_build.get("status") not in {"ok", "skipped"} or (
        powerpoint_build.get("status") == "skipped"
        and not powerpoint_build.get("reason")
    ):
        blockers.append("Global PowerPoint build stage missing or ambiguous.")
    if global_canonical_run_dir is None:
        blockers.append("Global canonical promotion manifest is missing.")

    return {
        "run_dir": str(global_run_dir),
        "manifest_path": str(global_run_dir / "manifest.json"),
        "deck_path": manifest.get("deterministic_preview", {}).get("deck_path"),
        "montage_path": render.get("montage_path"),
        "audit_report_path": audit.get("report_path"),
        "powerpoint_fill_payload_path": fill_payload_path,
        "audit_ok": bool(audit.get("ok")),
        "audit_findings": int(audit.get("finding_count") or 0),
        "font_missing_count": len(font_report.get("font_missing_overall", [])),
        "font_substituted_count": len(font_report.get("font_substituted_overall", [])),
        "canonical_manifest_path": str(global_canonical_run_dir / "manifest.json")
        if global_canonical_run_dir
        else None,
        "canonical_manifest": canonical_manifest,
        "blockers": blockers,
        "ok": not blockers,
        "regions": manifest.get("regions") or [],
        "quarterly_pipeline_disclosures": quarterly_pipeline_disclosures,
    }


def build_release_packet(
    *,
    snapshot_date: str,
    director_run_dir: Path,
    global_run_dir: Path,
    director_canonical_root: Path,
    global_canonical_root: Path,
    global_canonical_run_dir: Path | None,
    external_source_packet: dict[str, Any] | None,
) -> dict[str, Any]:
    director = build_director_release(director_run_dir)
    global_summary = build_global_release(global_run_dir, global_canonical_run_dir)
    external_sources = external_source_packet or {}
    blockers = (
        director["blockers"]
        + global_summary["blockers"]
        + list(external_sources.get("blockers") or [])
    )
    publish_ready = len(blockers) == 0
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": snapshot_date,
        "publish_ready": publish_ready,
        "blocker_count": len(blockers),
        "blockers": blockers,
        "director_release": director,
        "global_release": global_summary,
        "external_sources": external_sources or None,
        "canonical_paths": {
            "director_canonical_root": str(director_canonical_root),
            "global_canonical_root": str(global_canonical_root),
            "global_canonical_shell": str(
                global_canonical_root / "Sales Global Summary Shell.pptx"
            ),
        },
    }


def build_release_markdown(packet: dict[str, Any]) -> str:
    director = packet["director_release"]
    global_summary = packet["global_release"]
    external_sources = packet.get("external_sources") or {}
    publish_assets = packet.get("publish_assets") or {}
    canonical_paths = packet.get("canonical_paths") or {}
    lines = [
        "# Sales Deck Release Packet",
        "",
        f"- Snapshot date: `{packet['snapshot_date']}`",
        f"- Publish ready: `{packet['publish_ready']}`",
        f"- Blocker count: `{packet['blocker_count']}`",
        f"- Director batch: `{director['run_dir']}`",
        f"- Global summary run: `{global_summary['run_dir']}`",
        f"- Director canonical root: `{canonical_paths.get('director_canonical_root')}`",
        f"- Global canonical shell: `{canonical_paths.get('global_canonical_shell')}`",
    ]
    if publish_assets:
        lines.append(f"- Publish asset root: `{publish_assets.get('asset_root')}`")
        lines.append(f"- Publish asset count: `{publish_assets.get('asset_count', 0)}`")
        lines.append(
            f"- SharePoint quarter folder: `{publish_assets.get('quarter_folder')}`"
        )
    if external_sources:
        lines.append(
            f"- External source packet: `{external_sources.get('packet_dir')}`"
        )
    lines.append("")
    if packet["blockers"]:
        lines.extend(["## Blockers", ""])
        lines.extend(f"- {item}" for item in packet["blockers"])
        lines.append("")
    else:
        lines.extend(["## Blockers", "", "- None.", ""])

    lines.extend(
        [
            "## Director Batch",
            "",
            f"- Target count: `{director['target_count']}`",
            f"- Summary JSON: `{director['summary_path']}`",
            f"- Summary Markdown: `{director['summary_markdown_path']}`",
            f"- Canonical promotion summary: `{director['canonical_promotion_summary_path']}`",
            "",
            "| Director | Territory | Status | Audit | Layout | Fonts | Deck | Montage |",
            "|---|---|---:|---:|---:|---:|---|---|",
        ]
    )
    for row in director["targets"]:
        font_cell = f"m{row['font_missing_count']}/s{row['font_substituted_count']}"
        audit_cell = "ok" if row["audit_ok"] else f"{row['audit_findings']} findings"
        layout_cell = "ok" if row["layout_ok"] else "check"
        lines.append(
            f"| {row['director_name']} | {row['territory']} | {row['status']} | {audit_cell} | {layout_cell} | {font_cell} | "
            f"{row['deck_path'] or ''} | {row['montage_path'] or ''} |"
        )

    lines.extend(["", "## Director Quarter Disclosures", ""])
    director_forward_fallbacks = [
        row
        for row in director["targets"]
        if (row.get("quarterly_pipeline_disclosure") or {}).get("display_reason")
        == "forward_quarter_fallback"
    ]
    director_empty_quarters = [
        row
        for row in director["targets"]
        if (row.get("quarterly_pipeline_disclosure") or {}).get("display_reason")
        == "empty_current_and_forward"
    ]
    if director_forward_fallbacks:
        for row in director_forward_fallbacks:
            disclosure = row["quarterly_pipeline_disclosure"] or {}
            lines.append(
                f"- Forward-quarter fallback: {row['director_name']} ({row['territory']}) -> "
                f"`{disclosure.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
            )
    else:
        lines.append("- Forward-quarter fallbacks: none.")
    if director_empty_quarters:
        for row in director_empty_quarters:
            disclosure = row["quarterly_pipeline_disclosure"] or {}
            lines.append(
                f"- Empty current and forward-quarter director: {row['director_name']} ({row['territory']}). "
                f"{disclosure.get('quarterly_pipeline_footnote') or 'No explicit empty-state note provided.'}"
            )
    else:
        lines.append("- Empty current and forward-quarter directors: none.")
    lines.append("")

    lines.extend(["", "## External Sources", ""])
    if external_sources:
        summary = external_sources.get("status_summary") or {}
        lines.extend(
            [
                f"- Packet: `{external_sources.get('packet_dir')}`",
                f"- Publish ready: `{external_sources.get('publish_ready')}`",
                f"- Provided: `{summary.get('provided', 0)}`",
                f"- Pending: `{summary.get('pending', 0)}`",
                f"- Workbook proxy coverage: `{summary.get('proxy_covered_by_workbook', 0)}`",
                "",
            ]
        )
    else:
        lines.extend(["- None.", ""])

    lines.extend(["## Publish Assets", ""])
    assets = publish_assets.get("assets") or []
    if assets:
        lines.append("| Category | Name | Source | Published |")
        lines.append("|---|---|---|---|")
        for asset in assets:
            lines.append(
                f"| {asset.get('category')} | {asset.get('publish_name')} | {asset.get('source_path')} | {asset.get('publish_path')} |"
            )
    else:
        lines.append("- None.")
    lines.append("")

    lines.extend(
        [
            "## Global Summary",
            "",
            f"- Regions: `{', '.join(global_summary['regions'])}`",
            f"- Deck: `{global_summary['deck_path']}`",
            f"- Montage: `{global_summary['montage_path']}`",
            f"- Audit report: `{global_summary['audit_report_path']}`",
            f"- Fill payload: `{global_summary.get('powerpoint_fill_payload_path')}`",
            f"- Canonical promotion manifest: `{global_summary['canonical_manifest_path']}`",
            "",
        ]
    )
    disclosures = global_summary.get("quarterly_pipeline_disclosures") or {}
    forward_quarter_fallbacks = disclosures.get("forward_quarter_fallbacks") or []
    empty_quarter_regions = disclosures.get("empty_quarter_regions") or []
    lines.extend(["## Global Quarter Disclosures", ""])
    if forward_quarter_fallbacks:
        for row in forward_quarter_fallbacks:
            lines.append(
                f"- Forward-quarter fallback: {row['region_name']} -> "
                f"`{row.get('quarterly_pipeline_title') or 'forward quarter'}`. "
                f"{row.get('quarterly_pipeline_footnote') or 'Fallback disclosure missing.'}"
            )
    else:
        lines.append("- Forward-quarter fallbacks: none.")
    if empty_quarter_regions:
        for row in empty_quarter_regions:
            lines.append(
                f"- Empty current and forward-quarter region: {row['region_name']}. "
                f"{row.get('quarterly_pipeline_footnote') or 'No explicit empty-state note provided.'}"
            )
    else:
        lines.append("- Empty current and forward-quarter regions: none.")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_release_bundle(
    *,
    run_dir: Path,
    packet: dict[str, Any],
    markdown: str,
    output_root: Path,
    snapshot_date: str,
    sharepoint_root: Path | None = None,
) -> None:
    packet = materialize_publish_assets(
        run_dir=run_dir,
        packet=packet,
        sharepoint_root=sharepoint_root,
    )
    markdown = markdown or build_release_markdown(packet)
    save_json(run_dir / "release-packet.json", packet)
    save_text(run_dir / "release-packet.md", markdown)

    snapshot_latest_json = output_root / snapshot_date / "latest.json"
    snapshot_latest_md = output_root / snapshot_date / "latest.md"
    root_latest_json = output_root / "latest.json"
    root_latest_md = output_root / "latest.md"

    latest_payload = {**packet, "release_dir": str(run_dir)}
    save_json(snapshot_latest_json, latest_payload)
    save_text(snapshot_latest_md, markdown)
    save_json(root_latest_json, latest_payload)
    save_text(root_latest_md, markdown)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--director-run-dir", type=Path, default=None)
    parser.add_argument("--global-run-dir", type=Path, default=None)
    parser.add_argument("--global-canonical-run-dir", type=Path, default=None)
    parser.add_argument(
        "--director-run-root", type=Path, default=DEFAULT_DIRECTOR_RUN_ROOT
    )
    parser.add_argument("--global-run-root", type=Path, default=DEFAULT_GLOBAL_RUN_ROOT)
    parser.add_argument(
        "--global-canonical-run-root",
        type=Path,
        default=DEFAULT_GLOBAL_CANONICAL_RUN_ROOT,
    )
    parser.add_argument(
        "--external-source-packet-root",
        type=Path,
        default=DEFAULT_EXTERNAL_SOURCE_PACKET_ROOT,
    )
    parser.add_argument(
        "--director-canonical-root", type=Path, default=DEFAULT_DIRECTOR_CANONICAL_ROOT
    )
    parser.add_argument(
        "--global-canonical-root", type=Path, default=DEFAULT_GLOBAL_CANONICAL_ROOT
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--sharepoint-root", type=Path, default=DEFAULT_SHAREPOINT_ROOT)
    args = parser.parse_args()

    director_run_dir = args.director_run_dir or latest_run_dir(
        args.director_run_root, args.snapshot_date
    )
    global_run_dir = args.global_run_dir or latest_run_dir(
        args.global_run_root, args.snapshot_date
    )
    global_canonical_run_dir = args.global_canonical_run_dir
    if global_canonical_run_dir is None:
        try:
            global_canonical_run_dir = latest_run_dir(
                args.global_canonical_run_root, args.snapshot_date
            )
        except FileNotFoundError:
            global_canonical_run_dir = None
    external_source_packet = load_optional_json(
        args.external_source_packet_root / args.snapshot_date / "latest.json"
    )

    packet = build_release_packet(
        snapshot_date=args.snapshot_date,
        director_run_dir=director_run_dir,
        global_run_dir=global_run_dir,
        director_canonical_root=args.director_canonical_root,
        global_canonical_root=args.global_canonical_root,
        global_canonical_run_dir=global_canonical_run_dir,
        external_source_packet=external_source_packet,
    )

    run_dir = args.output_root / args.snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)
    write_release_bundle(
        run_dir=run_dir,
        packet=packet,
        markdown="",
        output_root=args.output_root,
        snapshot_date=args.snapshot_date,
        sharepoint_root=args.sharepoint_root,
    )
    packet = load_json(run_dir / "release-packet.json")
    print(json.dumps({**packet, "release_dir": str(run_dir)}, indent=2))


if __name__ == "__main__":
    main()
