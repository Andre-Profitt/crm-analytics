#!/usr/bin/env python3
"""Run the source-backed Sales Director monthly pipeline end to end."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TARGET_ORG = "apro@simcorp.com"
DEFAULT_REQUIREMENTS_PATH = ROOT / "config" / "monthly_source_requirements.json"
DEFAULT_TERRITORY_CONFIG = ROOT / "config" / "sd_monthly_territories.json"
DEFAULT_BUNDLE_CONTRACT = ROOT / "config" / "monthly_director_bundle_contract.json"
DEFAULT_FIELD_GUARDRAILS = ROOT / "config" / "salesforce_field_guardrails.json"
DEFAULT_SOURCE_CONTRACT_AUTHORING = (
    ROOT / "config" / "source_contracts" / "sales_director_monthly.yaml"
)
DEFAULT_TEMPLATE_PATH = "sales_director_thinkcell_template.pptx"
SCHEMA_VERSION = "monthly_platform.source_backed_monthly_pipeline.v1"
RELEASE_PACKET_SCHEMA_VERSION = "monthly_platform.source_backed_release_packet.v1"
MAX_CAPTURE_CHARS = 80_000
MIN_VISUAL_SLIDES = 6
MIN_VISUAL_TABLES = 3
MIN_VISUAL_CHARTS = 1
MIN_SEMANTIC_SCORE = 85


@dataclass(frozen=True)
class PipelinePaths:
    snapshot_date: str
    run_id: str
    run_lock: Path
    release_packet: Path
    source_contract_authoring_check: Path
    list_view_audit: Path
    source_contract_root: Path
    source_contract_lint: Path
    source_run_dir: Path
    source_bundle_dir: Path
    source_bundle_manifest: Path
    director_bundle_dir: Path
    director_bundle_manifest: Path
    readiness_dir: Path
    pipeline_open_readiness: Path
    analyst_workbook: Path
    thinkcell_dir: Path
    publish_gate: Path
    gold_root: Path
    deck_truth_root: Path
    deck_truth_packet: Path
    source_backed_deck_dir: Path
    source_backed_deck: Path
    source_backed_deck_manifest: Path
    source_backed_deck_polish_audit: Path
    source_backed_deck_visual_audit: Path
    source_backed_deck_table_contract_audit: Path
    source_backed_deck_semantic_audit: Path
    source_backed_deck_render_audit: Path
    release_bundle_dir: Path
    release_bundle_manifest: Path
    release_bundle_zip: Path
    sharepoint_upload_plan: Path
    sharepoint_upload_result: Path


@dataclass(frozen=True)
class PipelineStage:
    name: str
    command: list[str]
    required: bool = True
    output_path: Path | None = None
    accepted_statuses: frozenset[str] = frozenset({"ok"})


def build_paths(snapshot_date: str, run_id: str) -> PipelinePaths:
    source_run_dir = ROOT / "output" / "monthly_salesforce_sources" / snapshot_date / run_id
    source_bundle_dir = ROOT / "output" / "monthly_source_bundles" / snapshot_date / run_id
    director_bundle_dir = (
        ROOT / "output" / "monthly_director_bundles_from_sources" / snapshot_date / run_id
    )
    deck_dir = ROOT / "output" / "source_backed_decks" / snapshot_date / run_id
    gold_root = ROOT / "output" / "director_gold_analytics_from_sources" / run_id
    deck_truth_root = ROOT / "output" / "deck_truth_packets_from_sources" / run_id
    return PipelinePaths(
        snapshot_date=snapshot_date,
        run_id=run_id,
        run_lock=(
            ROOT
            / "output"
            / "source_backed_monthly_pipeline_runs"
            / snapshot_date
            / run_id
            / "run.lock"
        ),
        release_packet=(
            ROOT
            / "output"
            / "monthly_review_release_packets"
            / snapshot_date
            / run_id
            / "source_backed_release_packet.json"
        ),
        source_contract_authoring_check=(
            ROOT
            / "output"
            / "monthly_source_contract_authoring"
            / snapshot_date
            / run_id
            / "source_contract_authoring_check.json"
        ),
        list_view_audit=(
            ROOT
            / "output"
            / "pi_list_view_filter_audit"
            / snapshot_date
            / run_id
            / "pi_list_view_filter_audit.json"
        ),
        source_contract_root=ROOT / "output" / "monthly_source_contract" / run_id,
        source_contract_lint=(
            ROOT
            / "output"
            / "monthly_source_contract"
            / snapshot_date
            / run_id
            / "source_contract_lint.json"
        ),
        source_run_dir=source_run_dir,
        source_bundle_dir=source_bundle_dir,
        source_bundle_manifest=source_bundle_dir / "source_bundle_manifest.json",
        director_bundle_dir=director_bundle_dir,
        director_bundle_manifest=director_bundle_dir / "director_bundle_manifest.json",
        readiness_dir=ROOT / "output" / "monthly_dataset_readiness" / snapshot_date / run_id,
        pipeline_open_readiness=(
            ROOT / "output" / "monthly_dataset_readiness" / snapshot_date / run_id / "pipeline_open_readiness.json"
        ),
        analyst_workbook=director_bundle_dir / "source_backed_analyst_workbook.xlsx",
        thinkcell_dir=ROOT / "output" / "thinkcell_source_from_bundles" / snapshot_date / run_id,
        publish_gate=(
            ROOT
            / "output"
            / "monthly_source_backed_publish_gate"
            / snapshot_date
            / run_id
            / "source_backed_publish_gate.json"
        ),
        gold_root=gold_root,
        deck_truth_root=deck_truth_root,
        deck_truth_packet=deck_truth_root / snapshot_date / "deck_truth_packet.json",
        source_backed_deck_dir=deck_dir,
        source_backed_deck=deck_dir / "source_backed_monthly_review.pptx",
        source_backed_deck_manifest=deck_dir / "source_backed_deck_manifest.json",
        source_backed_deck_polish_audit=(
            ROOT
            / "output"
            / "source_backed_deck_polish"
            / snapshot_date
            / run_id
            / "source_backed_deck_polish_audit.json"
        ),
        source_backed_deck_visual_audit=(
            ROOT
            / "output"
            / "source_backed_deck_visuals"
            / snapshot_date
            / run_id
            / "source_backed_deck_visual_audit.json"
        ),
        source_backed_deck_table_contract_audit=(
            ROOT
            / "output"
            / "source_backed_deck_table_contract"
            / snapshot_date
            / run_id
            / "source_backed_deck_table_contract_audit.json"
        ),
        source_backed_deck_semantic_audit=(
            ROOT
            / "output"
            / "source_backed_deck_semantics"
            / snapshot_date
            / run_id
            / "source_backed_deck_semantic_audit.json"
        ),
        source_backed_deck_render_audit=(
            ROOT
            / "output"
            / "source_backed_deck_renders"
            / snapshot_date
            / run_id
            / "source_backed_deck_render_audit.json"
        ),
        release_bundle_dir=(
            ROOT / "output" / "source_backed_release_bundles" / snapshot_date / run_id
        ),
        release_bundle_manifest=(
            ROOT
            / "output"
            / "source_backed_release_bundles"
            / snapshot_date
            / run_id
            / "source_backed_release_bundle_manifest.json"
        ),
        release_bundle_zip=(
            ROOT
            / "output"
            / "source_backed_release_bundles"
            / snapshot_date
            / run_id
            / "source_backed_release_bundle.zip"
        ),
        sharepoint_upload_plan=(
            ROOT
            / "output"
            / "source_backed_sharepoint_upload_plans"
            / snapshot_date
            / run_id
            / "sharepoint_upload_plan.json"
        ),
        sharepoint_upload_result=(
            ROOT
            / "output"
            / "source_backed_sharepoint_uploads"
            / snapshot_date
            / run_id
            / "sharepoint_upload_result.json"
        ),
    )


def build_stage_plan(
    *,
    snapshot_date: str,
    run_id: str,
    target_org: str = DEFAULT_TARGET_ORG,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    bundle_contract_path: Path = DEFAULT_BUNDLE_CONTRACT,
    field_guardrails_path: Path = DEFAULT_FIELD_GUARDRAILS,
    source_contract_authoring_path: Path = DEFAULT_SOURCE_CONTRACT_AUTHORING,
    template_path: str = DEFAULT_TEMPLATE_PATH,
    sharepoint_upload: bool = False,
) -> list[PipelineStage]:
    paths = build_paths(snapshot_date, run_id)
    python = sys.executable
    stages = [
        PipelineStage(
            name="source_contract_authoring_config_check",
            output_path=paths.source_contract_authoring_check,
            command=[
                python,
                "scripts/compile_monthly_source_contract_config.py",
                "--authoring",
                str(source_contract_authoring_path),
                "--check",
                "--output-path",
                str(paths.source_contract_authoring_check),
                "--json",
            ],
        ),
        PipelineStage(
            name="pi_list_view_filter_audit",
            output_path=paths.list_view_audit,
            command=[
                python,
                "scripts/audit_pi_list_view_filters.py",
                "--target-org",
                target_org,
                "--territory-config",
                str(territory_config_path),
                "--field-guardrails",
                str(field_guardrails_path),
                "--output-path",
                str(paths.list_view_audit),
            ],
        ),
        PipelineStage(
            name="source_contract_preflight",
            output_path=(
                paths.source_contract_root / snapshot_date / "monthly_source_contract.json"
            ),
            accepted_statuses=frozenset({"ok", "warning"}),
            command=[
                python,
                "scripts/build_monthly_source_contract.py",
                "--snapshot-date",
                snapshot_date,
                "--territory-config",
                str(territory_config_path),
                "--output-root",
                str(paths.source_contract_root),
                "--json",
            ],
        ),
        PipelineStage(
            name="source_contract_requirement_lint",
            output_path=paths.source_contract_lint,
            command=[
                python,
                "scripts/lint_monthly_source_contract.py",
                "--snapshot-date",
                snapshot_date,
                "--requirements",
                str(requirements_path),
                "--bundle-contract",
                str(bundle_contract_path),
                "--output-path",
                str(paths.source_contract_lint),
                "--json",
            ],
        ),
        PipelineStage(
            name="extract_salesforce_sources",
            output_path=paths.source_run_dir / "run_manifest.json",
            command=[
                python,
                "scripts/extract_salesforce_sources.py",
                "--snapshot-date",
                snapshot_date,
                "--requirements",
                str(requirements_path),
                "--territory-config",
                str(territory_config_path),
                "--output-root",
                str(paths.source_run_dir.parents[1]),
                "--run-id",
                run_id,
                "--target-org",
                target_org,
                "--require-live",
                "--fail-fast",
                "--json",
            ],
        ),
        PipelineStage(
            name="build_source_bundles",
            output_path=paths.source_bundle_manifest,
            command=[
                python,
                "scripts/build_source_bundles_from_extracts.py",
                "--snapshot-date",
                snapshot_date,
                "--source-run-dir",
                str(paths.source_run_dir),
                "--output-root",
                str(paths.source_bundle_dir.parents[1]),
                "--run-id",
                run_id,
                "--require-complete",
                "--json",
            ],
        ),
        PipelineStage(
            name="build_director_bundles",
            output_path=paths.director_bundle_manifest,
            command=[
                python,
                "scripts/build_director_bundles_from_sources.py",
                "--snapshot-date",
                snapshot_date,
                "--source-bundle-dir",
                str(paths.source_bundle_dir),
                "--output-root",
                str(paths.director_bundle_dir.parents[1]),
                "--run-id",
                run_id,
                "--contract",
                str(bundle_contract_path),
                "--require-valid",
                "--json",
            ],
        ),
        PipelineStage(
            name="source_contract_final",
            output_path=(
                paths.source_contract_root / snapshot_date / "monthly_source_contract.json"
            ),
            command=[
                python,
                "scripts/build_monthly_source_contract.py",
                "--snapshot-date",
                snapshot_date,
                "--territory-config",
                str(territory_config_path),
                "--bundle-dir",
                str(paths.director_bundle_dir),
                "--output-root",
                str(paths.source_contract_root),
                "--require-bundles",
                "--json",
            ],
        ),
        PipelineStage(
            name="dataset_readiness_pipeline_open",
            output_path=paths.pipeline_open_readiness,
            accepted_statuses=frozenset({"ready"}),
            command=[
                python,
                "scripts/audit_director_dataset_readiness.py",
                "--snapshot-date",
                snapshot_date,
                "--dataset",
                "pipeline_open",
                "--source-run-dir",
                str(paths.source_run_dir),
                "--output-path",
                str(paths.pipeline_open_readiness),
                "--json",
            ],
        ),
        PipelineStage(
            name="build_analyst_workbook",
            output_path=paths.analyst_workbook,
            command=[
                python,
                "scripts/build_source_backed_analyst_workbook.py",
                "--manifest",
                str(paths.director_bundle_manifest),
                "--json",
            ],
        ),
        PipelineStage(
            name="build_thinkcell_source",
            output_path=paths.thinkcell_dir / "thinkcell_source.xlsx",
            command=[
                python,
                "scripts/build_thinkcell_source_from_bundles.py",
                "--manifest",
                str(paths.director_bundle_manifest),
                "--output-dir",
                str(paths.thinkcell_dir),
                "--template",
                template_path,
                "--json",
            ],
        ),
        PipelineStage(
            name="source_backed_publish_gate",
            output_path=paths.publish_gate,
            command=[
                python,
                "scripts/validate_monthly_source_backed_run.py",
                "--source-run-dir",
                str(paths.source_run_dir),
                "--source-bundle-dir",
                str(paths.source_bundle_dir),
                "--director-bundle-dir",
                str(paths.director_bundle_dir),
                "--readiness-dir",
                str(paths.readiness_dir),
                "--list-view-audit",
                str(paths.list_view_audit),
                "--contract",
                str(bundle_contract_path),
                "--output-path",
                str(paths.publish_gate),
            ],
        ),
        PipelineStage(
            name="build_director_gold_analytics",
            output_path=paths.gold_root / snapshot_date / "manifest.json",
            command=[
                python,
                "scripts/build_director_gold_analytics.py",
                "--bundle-dir",
                str(paths.director_bundle_dir),
                "--output-root",
                str(paths.gold_root),
                "--json",
            ],
        ),
        PipelineStage(
            name="build_deck_truth_packet",
            output_path=paths.deck_truth_packet,
            command=[
                python,
                "scripts/build_deck_truth_packet.py",
                "--snapshot-date",
                snapshot_date,
                "--gold-root",
                str(paths.gold_root),
                "--workbook-dir",
                str(paths.director_bundle_dir),
                "--bundle-dir",
                str(paths.director_bundle_dir),
                "--analyst-workbook",
                str(paths.analyst_workbook),
                "--source-backed-publish-gate",
                str(paths.publish_gate),
                "--template-path",
                template_path,
                "--output-root",
                str(paths.deck_truth_root),
                "--json",
            ],
        ),
        PipelineStage(
            name="build_source_backed_deck",
            output_path=paths.source_backed_deck,
            command=[
                python,
                "scripts/build_source_backed_deck.py",
                "--truth-packet",
                str(paths.deck_truth_packet),
                "--source-bundle-manifest",
                str(paths.source_bundle_manifest),
                "--source-backed-publish-gate",
                str(paths.publish_gate),
                "--output-path",
                str(paths.source_backed_deck),
            ],
        ),
        PipelineStage(
            name="polish_source_backed_deck_language",
            output_path=paths.source_backed_deck_polish_audit,
            command=[
                python,
                "scripts/polish_source_backed_deck_language.py",
                "--deck-path",
                str(paths.source_backed_deck),
                "--manifest-path",
                str(paths.source_backed_deck_manifest),
                "--snapshot-date",
                snapshot_date,
                "--source-run-id",
                run_id,
                "--output-path",
                str(paths.source_backed_deck_polish_audit),
            ],
        ),
        PipelineStage(
            name="validate_source_backed_deck_visuals",
            output_path=paths.source_backed_deck_visual_audit,
            command=[
                python,
                "scripts/validate_source_backed_deck_visuals.py",
                "--deck-path",
                str(paths.source_backed_deck),
                "--truth-packet",
                str(paths.deck_truth_packet),
                "--manifest-path",
                str(paths.source_backed_deck_manifest),
                "--min-slides",
                "6",
                "--output-path",
                str(paths.source_backed_deck_visual_audit),
            ],
        ),
        PipelineStage(
            name="validate_source_backed_deck_table_contract",
            output_path=paths.source_backed_deck_table_contract_audit,
            command=[
                python,
                "scripts/validate_source_backed_deck_table_contract.py",
                "--deck-path",
                str(paths.source_backed_deck),
                "--snapshot-date",
                snapshot_date,
                "--source-run-id",
                run_id,
                "--output-path",
                str(paths.source_backed_deck_table_contract_audit),
            ],
        ),
        PipelineStage(
            name="validate_source_backed_deck_semantics",
            output_path=paths.source_backed_deck_semantic_audit,
            command=[
                python,
                "scripts/validate_source_backed_deck_semantics.py",
                "--deck-path",
                str(paths.source_backed_deck),
                "--snapshot-date",
                snapshot_date,
                "--source-run-id",
                run_id,
                "--min-score",
                str(MIN_SEMANTIC_SCORE),
                "--output-path",
                str(paths.source_backed_deck_semantic_audit),
            ],
        ),
        PipelineStage(
            name="validate_source_backed_deck_render",
            output_path=paths.source_backed_deck_render_audit,
            command=[
                python,
                "scripts/validate_source_backed_deck_render.py",
                "--deck-path",
                str(paths.source_backed_deck),
                "--snapshot-date",
                snapshot_date,
                "--source-run-id",
                run_id,
                "--expected-slide-count",
                str(MIN_VISUAL_SLIDES),
                "--output-path",
                str(paths.source_backed_deck_render_audit),
            ],
        ),
        PipelineStage(
            name="build_source_backed_release_bundle",
            output_path=paths.release_bundle_manifest,
            command=[
                python,
                "scripts/build_source_backed_release_bundle.py",
                "--snapshot-date",
                snapshot_date,
                "--source-run-id",
                run_id,
                "--output-dir",
                str(paths.release_bundle_dir),
                "--output-path",
                str(paths.release_bundle_manifest),
                "--zip-path",
                str(paths.release_bundle_zip),
                "--artifact",
                f"source_contract=evidence={paths.source_contract_root / snapshot_date / 'monthly_source_contract.json'}",
                "--artifact",
                f"source_contract_lint=evidence={paths.source_contract_lint}",
                "--artifact",
                f"list_view_audit=evidence={paths.list_view_audit}",
                "--artifact",
                f"salesforce_run_manifest=source={paths.source_run_dir / 'run_manifest.json'}",
                "--artifact",
                f"source_bundle_manifest=source={paths.source_bundle_manifest}",
                "--artifact",
                f"director_bundle_manifest=source={paths.director_bundle_manifest}",
                "--artifact",
                f"pipeline_open_readiness=evidence={paths.pipeline_open_readiness}",
                "--artifact",
                f"analyst_workbook=deliverables={paths.analyst_workbook}",
                "--artifact",
                f"thinkcell_source=deliverables={paths.thinkcell_dir / 'thinkcell_source.xlsx'}",
                "--artifact",
                f"thinkcell_ppttc=deliverables={paths.thinkcell_dir / 'thinkcell_data.ppttc'}",
                "--artifact",
                f"publish_gate=evidence={paths.publish_gate}",
                "--artifact",
                f"deck_truth_packet=evidence={paths.deck_truth_packet}",
                "--artifact",
                f"source_backed_deck_manifest=evidence={paths.source_backed_deck_manifest}",
                "--artifact",
                f"source_backed_monthly_review=deliverables={paths.source_backed_deck}",
                "--artifact",
                f"deck_polish_audit=audits={paths.source_backed_deck_polish_audit}",
                "--artifact",
                f"deck_visual_audit=audits={paths.source_backed_deck_visual_audit}",
                "--artifact",
                f"deck_table_contract_audit=audits={paths.source_backed_deck_table_contract_audit}",
                "--artifact",
                f"deck_semantic_audit=audits={paths.source_backed_deck_semantic_audit}",
                "--artifact",
                f"deck_render_audit=audits={paths.source_backed_deck_render_audit}",
                "--artifact",
                f"deck_render_pdf=render={paths.source_backed_deck_render_audit.parent / 'source_backed_monthly_review.pdf'}",
                "--artifact",
                f"deck_render_montage=render={paths.source_backed_deck_render_audit.parent / 'montage.png'}",
            ],
        ),
        PipelineStage(
            name="plan_source_backed_sharepoint_upload",
            output_path=paths.sharepoint_upload_plan,
            accepted_statuses=frozenset({"planned"}),
            command=[
                python,
                "scripts/upload_sales_deck_release_to_sharepoint.py",
                "--source-backed-bundle-manifest-json",
                str(paths.release_bundle_manifest),
                "--dry-run",
                "--output-path",
                str(paths.sharepoint_upload_plan),
            ],
        ),
    ]
    if sharepoint_upload:
        stages.append(
            PipelineStage(
                name="upload_source_backed_sharepoint_assets",
                output_path=paths.sharepoint_upload_result,
                command=[
                    python,
                    "scripts/upload_sales_deck_release_to_sharepoint.py",
                    "--source-backed-bundle-manifest-json",
                    str(paths.release_bundle_manifest),
                    "--output-path",
                    str(paths.sharepoint_upload_result),
                ],
            )
        )
    return stages


def run_pipeline(
    *,
    snapshot_date: str,
    run_id: str,
    target_org: str,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    territory_config_path: Path = DEFAULT_TERRITORY_CONFIG,
    bundle_contract_path: Path = DEFAULT_BUNDLE_CONTRACT,
    field_guardrails_path: Path = DEFAULT_FIELD_GUARDRAILS,
    source_contract_authoring_path: Path = DEFAULT_SOURCE_CONTRACT_AUTHORING,
    template_path: str,
    plan_only: bool = False,
    keep_going: bool = False,
    start_at: str | None = None,
    sharepoint_upload: bool = False,
) -> dict[str, Any]:
    stages = build_stage_plan(
        snapshot_date=snapshot_date,
        run_id=run_id,
        target_org=target_org,
        requirements_path=requirements_path,
        territory_config_path=territory_config_path,
        bundle_contract_path=bundle_contract_path,
        field_guardrails_path=field_guardrails_path,
        source_contract_authoring_path=source_contract_authoring_path,
        template_path=template_path,
        sharepoint_upload=sharepoint_upload,
    )
    if start_at:
        stage_names = [stage.name for stage in stages]
        if start_at not in stage_names:
            raise ValueError(f"Unknown --start-at stage {start_at!r}; choose one of {stage_names}")
        stages = stages[stage_names.index(start_at) :]
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "status": "planned" if plan_only else "running",
        "snapshot_date": snapshot_date,
        "run_id": run_id,
        "target_org": target_org,
        "requirements_path": str(requirements_path),
        "territory_config_path": str(territory_config_path),
        "bundle_contract_path": str(bundle_contract_path),
        "field_guardrails_path": str(field_guardrails_path),
        "source_contract_authoring_path": str(source_contract_authoring_path),
        "plan_only": plan_only,
        "keep_going": keep_going,
        "sharepoint_upload": sharepoint_upload,
        "paths": _paths_for_manifest(build_paths(snapshot_date, run_id)),
        "stages": [],
    }
    for stage in stages:
        record = {
            "name": stage.name,
            "command": stage.command,
            "required": stage.required,
            "output_path": str(stage.output_path) if stage.output_path else None,
            "accepted_statuses": sorted(stage.accepted_statuses),
            "status": "planned",
        }
        manifest["stages"].append(record)
        if plan_only:
            continue
        started = datetime.now(UTC)
        run = subprocess.run(
            stage.command,
            cwd=ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        payload = _parse_json_payload(run.stdout)
        payload_status = _payload_status(payload)
        artifact_missing = bool(stage.output_path and not stage.output_path.exists())
        stage_ok = (
            run.returncode == 0
            and payload_status in stage.accepted_statuses
            and not artifact_missing
        )
        record.update(
            {
                "status": "ok" if stage_ok else "error",
                "returncode": run.returncode,
                "duration_seconds": round(
                    (datetime.now(UTC) - started).total_seconds(),
                    3,
                ),
                "payload_status": payload_status,
                "payload": payload,
                "artifact_exists": bool(stage.output_path and stage.output_path.exists()),
                "blocking_reason": _blocking_reason(
                    returncode=run.returncode,
                    payload_status=payload_status,
                    accepted_statuses=stage.accepted_statuses,
                    artifact_missing=artifact_missing,
                    output_path=stage.output_path,
                ),
                "stdout": _truncate(run.stdout),
                "stderr": _truncate(run.stderr),
            }
        )
        if not stage_ok and stage.required and not keep_going:
            manifest["status"] = "blocked"
            manifest["failed_stage"] = stage.name
            manifest["summary"] = summarize_manifest(manifest)
            return manifest
    if plan_only:
        manifest["summary"] = summarize_manifest(manifest)
        return manifest
    errors = [
        stage
        for stage in manifest["stages"]
        if stage.get("required") and stage.get("status") != "ok"
    ]
    manifest["status"] = "blocked" if errors else "ok"
    if errors:
        manifest["failed_stage"] = errors[0]["name"]
    manifest["summary"] = summarize_manifest(manifest)
    return manifest


def _paths_for_manifest(paths: PipelinePaths) -> dict[str, str]:
    return {key: str(value) for key, value in asdict(paths).items()}


def summarize_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    stages = list(manifest.get("stages") or [])
    summary: dict[str, Any] = {
        "status": manifest.get("status"),
        "stage_count": len(stages),
        "ok_stage_count": sum(1 for stage in stages if stage.get("status") == "ok"),
        "error_stage_count": sum(
            1 for stage in stages if stage.get("status") == "error"
        ),
        "planned_stage_count": sum(
            1 for stage in stages if stage.get("status") == "planned"
        ),
        "failed_stage": manifest.get("failed_stage"),
    }
    for stage in stages:
        payload = stage.get("payload") or {}
        if not isinstance(payload, dict):
            payload = {}
        stage_name = str(stage.get("name") or "")
        _fold_stage_summary(summary, stage_name, payload, stage=stage)
        if stage.get("blocking_reason") and not summary.get("blocking_reason"):
            summary["blocking_reason"] = stage.get("blocking_reason")
    return summary


def _fold_stage_summary(
    summary: dict[str, Any],
    stage_name: str,
    payload: dict[str, Any],
    *,
    stage: dict[str, Any] | None = None,
) -> None:
    if stage_name == "source_contract_authoring_config_check":
        for key in (
            "target_count",
            "drift_count",
            "missing_count",
            "finding_count",
            "high_finding_count",
        ):
            _copy_metric(payload, summary, key, f"source_contract_authoring_{key}")
    elif stage_name == "pi_list_view_filter_audit":
        _copy_metric(payload, summary, "view_count", "list_view_audit_view_count")
        _copy_metric(payload, summary, "finding_count", "list_view_audit_finding_count")
        _copy_metric(
            payload,
            summary,
            "high_finding_count",
            "list_view_audit_high_finding_count",
        )
        _copy_metric(
            payload,
            summary,
            "dead_or_invalid_filter_field_count",
            "dead_or_invalid_filter_field_count",
        )
    elif stage_name == "source_contract_preflight":
        _copy_metric(
            payload,
            summary,
            "high_finding_count",
            "source_contract_preflight_high_finding_count",
        )
        _copy_metric(
            payload,
            summary,
            "warning_finding_count",
            "source_contract_preflight_warning_finding_count",
        )
        _copy_metric(
            payload,
            summary,
            "missing_report_id_count",
            "source_contract_preflight_missing_report_id_count",
        )
        _copy_metric(
            payload,
            summary,
            "territory_count",
            "source_contract_preflight_territory_count",
        )
    elif stage_name == "source_contract_final":
        _fold_period_policy_summary(
            summary,
            payload,
            artifact_payload=_load_stage_output_payload(stage),
        )
        for key in (
            "high_finding_count",
            "warning_finding_count",
            "missing_report_id_count",
            "missing_bundle_count",
            "territory_count",
            "forward_fallback_count",
            "current_quarter_empty_count",
        ):
            _copy_metric(payload, summary, key, f"source_contract_final_{key}")
    elif stage_name == "source_contract_requirement_lint":
        _copy_metric(payload, summary, "finding_count", "source_contract_lint_finding_count")
        _copy_metric(
            payload,
            summary,
            "high_finding_count",
            "source_contract_lint_high_finding_count",
        )
        _copy_metric(
            payload,
            summary,
            "publish_required_source_backed_dataset_count",
            "publish_required_source_backed_dataset_count",
        )
    elif stage_name == "extract_salesforce_sources":
        for key in (
            "selected_source_count",
            "executed_source_count",
            "source_extract_count",
            "failed_source_count",
            "finding_count",
        ):
            _copy_metric(payload, summary, key, key)
    elif stage_name == "build_source_bundles":
        for key in (
            "bundle_count",
            "territory_count",
            "missing_selected_source_count",
            "forward_fallback_count",
        ):
            _copy_metric(payload, summary, key, key)
    elif stage_name == "build_director_bundles":
        _copy_metric(payload.get("summary") or {}, summary, "bundle_count", "director_bundle_count")
    elif stage_name == "source_backed_publish_gate":
        counts = payload.get("counts") or {}
        for key in (
            "director_bundle_count",
            "publish_required_dataset_count",
            "list_view_audit_view_count",
            "list_view_audit_finding_count",
            "finding_count",
            "high_finding_count",
        ):
            _copy_metric(counts, summary, key, f"publish_gate_{key}")
    elif stage_name == "build_deck_truth_packet":
        for key in (
            "high_blocker_count",
            "tieout_mismatch_count",
            "metric_count",
            "claim_count",
        ):
            _copy_metric(payload, summary, key, key)
    elif stage_name == "build_source_backed_deck":
        deck_summary = payload.get("summary") or {}
        for key in (
            "director_count",
            "metric_count",
            "claim_count",
            "high_blocker_count",
            "tieout_mismatch_count",
            "forward_fallback_count",
        ):
            _copy_metric(deck_summary, summary, key, key)
        _copy_metric(payload, summary, "slide_count", "deck_slide_count")
    elif stage_name == "polish_source_backed_deck_language":
        polish_summary = payload.get("summary") or {}
        checks = payload.get("checks") or {}
        _copy_metric(polish_summary, summary, "finding_count", "polish_finding_count")
        _copy_metric(
            polish_summary,
            summary,
            "high_finding_count",
            "polish_high_finding_count",
        )
        _copy_metric(
            polish_summary,
            summary,
            "replacements_applied_count",
            "polish_replacements_applied_count",
        )
        _copy_metric(
            checks,
            summary,
            "polished_text_checked",
            "polished_text_checked",
        )
    elif stage_name == "validate_source_backed_deck_visuals":
        visual_summary = payload.get("summary") or {}
        checks = payload.get("checks") or {}
        _copy_metric(visual_summary, summary, "finding_count", "visual_finding_count")
        _copy_metric(
            visual_summary,
            summary,
            "high_finding_count",
            "visual_high_finding_count",
        )
        _copy_metric(checks, summary, "slide_count", "visual_slide_count")
        _copy_metric(checks, summary, "table_count", "visual_table_count")
        _copy_metric(checks, summary, "chart_count", "visual_chart_count")
    elif stage_name == "validate_source_backed_deck_table_contract":
        table_summary = payload.get("summary") or {}
        checks = payload.get("checks") or {}
        _copy_metric(
            table_summary,
            summary,
            "finding_count",
            "table_contract_finding_count",
        )
        _copy_metric(
            table_summary,
            summary,
            "high_finding_count",
            "table_contract_high_finding_count",
        )
        _copy_metric(
            table_summary,
            summary,
            "medium_finding_count",
            "table_contract_medium_finding_count",
        )
        _copy_metric(
            table_summary,
            summary,
            "table_count",
            "table_contract_table_count",
        )
        _copy_metric(
            checks,
            summary,
            "expected_table_count",
            "table_contract_expected_table_count",
        )
        _copy_metric(
            checks,
            summary,
            "table_contract_checked",
            "table_contract_checked",
        )
    elif stage_name == "validate_source_backed_deck_semantics":
        semantic_summary = payload.get("summary") or {}
        checks = payload.get("checks") or {}
        _copy_metric(
            semantic_summary,
            summary,
            "finding_count",
            "semantic_finding_count",
        )
        _copy_metric(
            semantic_summary,
            summary,
            "high_finding_count",
            "semantic_high_finding_count",
        )
        _copy_metric(
            semantic_summary,
            summary,
            "medium_finding_count",
            "semantic_medium_finding_count",
        )
        _copy_metric(
            semantic_summary,
            summary,
            "human_style_score",
            "semantic_human_style_score",
        )
        _copy_metric(
            checks,
            summary,
            "business_readiness_checked",
            "business_readiness_checked",
        )
    elif stage_name == "validate_source_backed_deck_render":
        render_summary = payload.get("summary") or {}
        checks = payload.get("checks") or {}
        _copy_metric(render_summary, summary, "finding_count", "render_finding_count")
        _copy_metric(
            render_summary,
            summary,
            "high_finding_count",
            "render_high_finding_count",
        )
        _copy_metric(checks, summary, "deck_slide_count", "render_deck_slide_count")
        _copy_metric(
            checks,
            summary,
            "expected_slide_count",
            "render_expected_slide_count",
        )
        _copy_metric(
            checks,
            summary,
            "rendered_slide_count",
            "rendered_slide_count",
        )
        _copy_metric(
            checks,
            summary,
            "rendered_png_checked",
            "rendered_png_checked",
        )
    elif stage_name == "build_source_backed_release_bundle":
        bundle_summary = payload.get("summary") or {}
        handoff = payload.get("sharepoint_handoff") or {}
        _copy_metric(
            bundle_summary,
            summary,
            "artifact_count",
            "release_bundle_artifact_count",
        )
        _copy_metric(
            bundle_summary,
            summary,
            "copied_artifact_count",
            "release_bundle_copied_artifact_count",
        )
        _copy_metric(
            bundle_summary,
            summary,
            "required_artifact_count",
            "release_bundle_required_artifact_count",
        )
        _copy_metric(
            bundle_summary,
            summary,
            "missing_required_artifact_count",
            "release_bundle_missing_required_artifact_count",
        )
        _copy_metric(payload, summary, "zip_size_bytes", "release_bundle_zip_size_bytes")
        _copy_metric(handoff, summary, "upload_ready", "release_bundle_upload_ready")
    elif stage_name == "plan_source_backed_sharepoint_upload":
        _copy_metric(payload, summary, "status", "sharepoint_upload_plan_status")
        _copy_metric(payload, summary, "planned_count", "sharepoint_upload_planned_count")
        _copy_metric(payload, summary, "missing_count", "sharepoint_upload_missing_count")
        _copy_metric(payload, summary, "publish_ready", "sharepoint_upload_publish_ready")
        _copy_metric(payload, summary, "source_backed", "sharepoint_upload_source_backed")
        _copy_metric(payload, summary, "folder", "sharepoint_upload_folder")
    elif stage_name == "upload_source_backed_sharepoint_assets":
        _copy_metric(payload, summary, "status", "sharepoint_upload_status")
        _copy_metric(payload, summary, "uploaded_count", "sharepoint_uploaded_count")
        _copy_metric(payload, summary, "skipped_count", "sharepoint_upload_skipped_count")
        _copy_metric(payload, summary, "publish_ready", "sharepoint_upload_publish_ready")
        _copy_metric(payload, summary, "source_backed", "sharepoint_upload_source_backed")
        _copy_metric(payload, summary, "folder", "sharepoint_upload_folder")


def _fold_period_policy_summary(
    summary: dict[str, Any],
    payload: dict[str, Any],
    *,
    artifact_payload: dict[str, Any] | None = None,
) -> None:
    for candidate in (payload, artifact_payload or {}):
        period = candidate.get("period") or {}
        quarter_policy = candidate.get("quarter_policy") or period.get("quarter_policy") or {}
        quarter_mapping = candidate.get("quarter_mapping") or period.get("quarter_mapping") or {}
        business_period = candidate.get("business_period") or period.get("business_period") or {}
        source_registry_period = (
            candidate.get("source_registry_period") or period.get("source_registry_period") or {}
        )
        display_period = candidate.get("display_period") or period.get("display_period") or {}
        current_quarter = period.get("current_quarter") or {}
        forward_quarter = period.get("forward_quarter") or {}
        _copy_metric(quarter_policy, summary, "name", "quarter_policy_name")
        _copy_metric(
            quarter_policy,
            summary,
            "fiscal_year_start_month",
            "quarter_policy_fiscal_year_start_month",
        )
        _copy_metric(current_quarter, summary, "title", "current_quarter_title")
        _copy_metric(forward_quarter, summary, "title", "forward_quarter_title")
        _copy_metric(quarter_mapping, summary, "approved", "quarter_mapping_approved")
        _copy_metric(quarter_mapping, summary, "approved_by", "quarter_mapping_approved_by")
        _copy_metric(quarter_mapping, summary, "approved_at", "quarter_mapping_approved_at")
        _copy_metric(quarter_mapping, summary, "reason", "quarter_mapping_reason")
        _copy_metric(
            quarter_mapping,
            summary,
            "business_current_quarter_label",
            "business_current_quarter_label",
        )
        _copy_metric(
            quarter_mapping,
            summary,
            "source_current_quarter_label",
            "source_current_quarter_label",
        )
        _copy_metric(
            quarter_mapping,
            summary,
            "display_current_quarter_label",
            "display_current_quarter_label",
        )
        _copy_metric(
            business_period,
            summary,
            "fiscal_year_naming_policy",
            "business_fiscal_year_naming_policy",
        )
        _copy_metric(
            source_registry_period,
            summary,
            "quarter_label_style",
            "source_quarter_label_style",
        )
        _copy_metric(display_period, summary, "label_source", "display_label_source")
        _copy_metric(period, summary, "reporting_window_start", "reporting_window_start")
        _copy_metric(period, summary, "reporting_window_end", "reporting_window_end")


def _load_stage_output_payload(stage: dict[str, Any] | None) -> dict[str, Any] | None:
    if not stage:
        return None
    output_path = stage.get("output_path")
    if not output_path:
        return None
    path = Path(str(output_path))
    if not path.is_absolute():
        path = ROOT / path
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload


def _copy_metric(
    source: dict[str, Any],
    target: dict[str, Any],
    source_key: str,
    target_key: str,
) -> None:
    if source_key in source:
        target[target_key] = source[source_key]


def _parse_json_payload(text: str) -> dict[str, Any] | None:
    stripped = text.strip()
    if not stripped:
        return None
    decoder = json.JSONDecoder()
    for index, char in enumerate(stripped):
        if char != "{":
            continue
        try:
            payload, end = decoder.raw_decode(stripped[index:])
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and not stripped[index + end :].strip():
            return payload
    return None


def _payload_status(payload: dict[str, Any] | None) -> str:
    if not payload:
        return "missing_json_payload"
    return str(payload.get("status") or "missing_status")


def _blocking_reason(
    *,
    returncode: int,
    payload_status: str,
    accepted_statuses: frozenset[str],
    artifact_missing: bool,
    output_path: Path | None,
) -> str | None:
    if returncode != 0:
        return f"command_returned_{returncode}"
    if payload_status not in accepted_statuses:
        return f"payload_status_{payload_status}_not_in_{sorted(accepted_statuses)}"
    if artifact_missing and output_path:
        return f"expected_artifact_missing:{output_path}"
    return None


def _truncate(text: str) -> str:
    if len(text) <= MAX_CAPTURE_CHARS:
        return text
    return text[-MAX_CAPTURE_CHARS:]


def default_manifest_path(snapshot_date: str, run_id: str) -> Path:
    return (
        ROOT
        / "output"
        / "source_backed_monthly_pipeline_runs"
        / snapshot_date
        / run_id
        / "pipeline_run_manifest.json"
    )


class RunLock:
    def __init__(self, path: Path, *, snapshot_date: str, run_id: str) -> None:
        self.path = path
        self.snapshot_date = snapshot_date
        self.run_id = run_id

    def __enter__(self) -> "RunLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": f"{SCHEMA_VERSION}.lock",
            "snapshot_date": self.snapshot_date,
            "run_id": self.run_id,
            "pid": os.getpid(),
            "acquired_at": datetime.now(UTC).isoformat(),
        }
        try:
            file_descriptor = os.open(
                self.path,
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
        except FileExistsError as exc:
            raise RuntimeError(f"Run lock already exists: {self.path}") from exc
        with os.fdopen(file_descriptor, "w", encoding="utf-8") as lock_file:
            lock_file.write(json.dumps(payload, indent=2) + "\n")
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def acquire_run_lock(snapshot_date: str, run_id: str) -> RunLock:
    return RunLock(
        build_paths(snapshot_date, run_id).run_lock,
        snapshot_date=snapshot_date,
        run_id=run_id,
    )


def release_packet_from_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
    paths = manifest.get("paths") or {}
    summary = manifest.get("summary") or {}
    release_checks = release_checks_from_manifest(manifest)
    publish_ready = all(check["status"] == "pass" for check in release_checks)
    return {
        "schema_version": RELEASE_PACKET_SCHEMA_VERSION,
        "status": "ok" if publish_ready else "blocked",
        "publish_recommendation": "publish" if publish_ready else "do_not_publish",
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": manifest.get("snapshot_date"),
        "run_id": manifest.get("run_id"),
        "runner_manifest_path": manifest.get("manifest_path"),
        "summary": summary,
        "artifacts": {
            "analyst_workbook": paths.get("analyst_workbook"),
            "thinkcell_workbook": str(Path(paths["thinkcell_dir"]) / "thinkcell_source.xlsx")
            if paths.get("thinkcell_dir")
            else None,
            "thinkcell_ppttc": str(Path(paths["thinkcell_dir"]) / "thinkcell_data.ppttc")
            if paths.get("thinkcell_dir")
            else None,
            "publish_gate": paths.get("publish_gate"),
            "deck_truth_packet": paths.get("deck_truth_packet"),
            "source_backed_deck": paths.get("source_backed_deck"),
            "polish_audit": paths.get("source_backed_deck_polish_audit"),
            "visual_audit": paths.get("source_backed_deck_visual_audit"),
            "table_contract_audit": paths.get("source_backed_deck_table_contract_audit"),
            "semantic_audit": paths.get("source_backed_deck_semantic_audit"),
            "render_audit": paths.get("source_backed_deck_render_audit"),
            "release_bundle_manifest": paths.get("release_bundle_manifest"),
            "release_bundle_zip": paths.get("release_bundle_zip"),
            "sharepoint_upload_plan": paths.get("sharepoint_upload_plan"),
            "sharepoint_upload_result": paths.get("sharepoint_upload_result"),
            "list_view_audit": paths.get("list_view_audit"),
        },
        "release_checks": release_checks,
        "blocking_reasons": [
            check["evidence"]
            for check in release_checks
            if check["status"] != "pass"
        ],
        "gates": [
            {
                "name": stage.get("name"),
                "status": stage.get("status"),
                "payload_status": stage.get("payload_status"),
                "output_path": stage.get("output_path"),
                "blocking_reason": stage.get("blocking_reason"),
            }
            for stage in manifest.get("stages") or []
        ],
    }


def release_checks_from_manifest(manifest: dict[str, Any]) -> list[dict[str, str]]:
    summary = manifest.get("summary") or {}
    required_stage_errors = [
        str(stage.get("name"))
        for stage in manifest.get("stages") or []
        if stage.get("required") and stage.get("status") != "ok"
    ]
    selected_sources = _summary_int(summary, "selected_source_count")
    executed_sources = _summary_int(summary, "executed_source_count")
    extracted_sources = _summary_int(summary, "source_extract_count")
    territory_count = _summary_int(summary, "territory_count")
    source_bundle_count = _summary_int(summary, "bundle_count")
    director_bundle_count = _summary_int(summary, "director_bundle_count")
    publish_gate_director_count = _summary_int(summary, "publish_gate_director_bundle_count")
    return [
        _release_check(
            "runner_status_ok",
            manifest.get("status") == "ok",
            f"runner_status={manifest.get('status')}",
        ),
        _release_check(
            "all_required_stages_ok",
            not required_stage_errors,
            "required_stage_errors=" + ",".join(required_stage_errors),
        ),
        _release_check(
            "source_contract_authoring_synced",
            _summary_int(summary, "source_contract_authoring_target_count") > 0
            and _summary_int(summary, "source_contract_authoring_drift_count") == 0
            and _summary_int(summary, "source_contract_authoring_missing_count") == 0
            and _summary_int(summary, "source_contract_authoring_high_finding_count") == 0,
            (
                "source_contract_authoring_target_count="
                f"{summary.get('source_contract_authoring_target_count')}; "
                "source_contract_authoring_drift_count="
                f"{summary.get('source_contract_authoring_drift_count')}; "
                "source_contract_authoring_missing_count="
                f"{summary.get('source_contract_authoring_missing_count')}; "
                "source_contract_authoring_high_finding_count="
                f"{summary.get('source_contract_authoring_high_finding_count')}"
            ),
        ),
        _release_check(
            "source_contract_preflight_clean",
            _summary_int(summary, "source_contract_preflight_high_finding_count") == 0
            and _summary_int(summary, "source_contract_preflight_missing_report_id_count") == 0,
            (
                "source_contract_preflight_high_finding_count="
                f"{summary.get('source_contract_preflight_high_finding_count')}; "
                "source_contract_preflight_missing_report_id_count="
                f"{summary.get('source_contract_preflight_missing_report_id_count')}"
            ),
        ),
        _release_check(
            "source_contract_lint_clean",
            _summary_int(summary, "source_contract_lint_finding_count") == 0
            and _summary_int(summary, "source_contract_lint_high_finding_count") == 0,
            (
                "source_contract_lint_finding_count="
                f"{summary.get('source_contract_lint_finding_count')}; "
                "source_contract_lint_high_finding_count="
                f"{summary.get('source_contract_lint_high_finding_count')}"
            ),
        ),
        _release_check(
            "source_contract_final_clean",
            _summary_int(summary, "source_contract_final_high_finding_count") == 0
            and _summary_int(summary, "source_contract_final_warning_finding_count") == 0
            and _summary_int(summary, "source_contract_final_missing_report_id_count") == 0
            and _summary_int(summary, "source_contract_final_missing_bundle_count") == 0
            and _summary_int(summary, "source_contract_final_territory_count") > 0,
            (
                "source_contract_final_high_finding_count="
                f"{summary.get('source_contract_final_high_finding_count')}; "
                "source_contract_final_warning_finding_count="
                f"{summary.get('source_contract_final_warning_finding_count')}; "
                "source_contract_final_missing_report_id_count="
                f"{summary.get('source_contract_final_missing_report_id_count')}; "
                "source_contract_final_missing_bundle_count="
                f"{summary.get('source_contract_final_missing_bundle_count')}; "
                "source_contract_final_territory_count="
                f"{summary.get('source_contract_final_territory_count')}"
            ),
        ),
        _release_check(
            "quarter_policy_locked",
            summary.get("quarter_policy_name") == "calendar_quarter"
            and _summary_int(summary, "quarter_policy_fiscal_year_start_month") == 1
            and bool(summary.get("current_quarter_title"))
            and bool(summary.get("forward_quarter_title")),
            (
                f"quarter_policy_name={summary.get('quarter_policy_name')}; "
                "quarter_policy_fiscal_year_start_month="
                f"{summary.get('quarter_policy_fiscal_year_start_month')}; "
                f"current_quarter_title={summary.get('current_quarter_title')}; "
                f"forward_quarter_title={summary.get('forward_quarter_title')}"
            ),
        ),
        _release_check(
            "quarter_mapping_approved",
            summary.get("quarter_mapping_approved") is True
            and bool(summary.get("business_current_quarter_label"))
            and bool(summary.get("source_current_quarter_label"))
            and bool(summary.get("display_current_quarter_label"))
            and bool(summary.get("quarter_mapping_reason")),
            (
                f"quarter_mapping_approved={summary.get('quarter_mapping_approved')}; "
                "business_current_quarter_label="
                f"{summary.get('business_current_quarter_label')}; "
                "source_current_quarter_label="
                f"{summary.get('source_current_quarter_label')}; "
                "display_current_quarter_label="
                f"{summary.get('display_current_quarter_label')}; "
                f"quarter_mapping_reason={summary.get('quarter_mapping_reason')}"
            ),
        ),
        _release_check(
            "list_view_audit_clean",
            _summary_int(summary, "list_view_audit_view_count") > 0
            and _summary_int(summary, "list_view_audit_finding_count") == 0
            and _summary_int(summary, "list_view_audit_high_finding_count") == 0,
            (
                f"list_view_audit_view_count={summary.get('list_view_audit_view_count')}; "
                f"list_view_audit_finding_count={summary.get('list_view_audit_finding_count')}; "
                "list_view_audit_high_finding_count="
                f"{summary.get('list_view_audit_high_finding_count')}"
            ),
        ),
        _release_check(
            "salesforce_extracts_complete",
            selected_sources > 0
            and executed_sources == selected_sources
            and extracted_sources >= selected_sources
            and _summary_int(summary, "failed_source_count") == 0,
            (
                f"selected_source_count={selected_sources}; "
                f"executed_source_count={executed_sources}; "
                f"source_extract_count={extracted_sources}; "
                f"failed_source_count={summary.get('failed_source_count')}"
            ),
        ),
        _release_check(
            "source_bundles_complete",
            source_bundle_count > 0
            and territory_count > 0
            and source_bundle_count == territory_count
            and _summary_int(summary, "missing_selected_source_count") == 0,
            (
                f"bundle_count={source_bundle_count}; "
                f"territory_count={territory_count}; "
                "missing_selected_source_count="
                f"{summary.get('missing_selected_source_count')}"
            ),
        ),
        _release_check(
            "director_bundles_complete",
            director_bundle_count > 0
            and publish_gate_director_count == director_bundle_count
            and (territory_count == 0 or director_bundle_count == territory_count),
            (
                f"director_bundle_count={director_bundle_count}; "
                f"publish_gate_director_bundle_count={publish_gate_director_count}; "
                f"territory_count={territory_count}"
            ),
        ),
        _release_check(
            "publish_gate_clean",
            _summary_int(summary, "publish_gate_finding_count") == 0
            and _summary_int(summary, "publish_gate_high_finding_count") == 0,
            (
                f"publish_gate_finding_count={summary.get('publish_gate_finding_count')}; "
                f"publish_gate_high_finding_count={summary.get('publish_gate_high_finding_count')}"
            ),
        ),
        _release_check(
            "truth_packet_clean",
            _summary_int(summary, "high_blocker_count") == 0
            and _summary_int(summary, "tieout_mismatch_count") == 0
            and _summary_int(summary, "claim_count") > 0,
            (
                f"high_blocker_count={summary.get('high_blocker_count')}; "
                f"tieout_mismatch_count={summary.get('tieout_mismatch_count')}; "
                f"claim_count={summary.get('claim_count')}"
            ),
        ),
        _release_check(
            "deck_visuals_clean",
            _summary_int(summary, "visual_finding_count") == 0
            and _summary_int(summary, "visual_high_finding_count") == 0
            and _summary_int(summary, "visual_slide_count") >= MIN_VISUAL_SLIDES
            and _summary_int(summary, "visual_table_count") >= MIN_VISUAL_TABLES
            and _summary_int(summary, "visual_chart_count") >= MIN_VISUAL_CHARTS,
            (
                f"visual_finding_count={summary.get('visual_finding_count')}; "
                f"visual_high_finding_count={summary.get('visual_high_finding_count')}; "
                f"visual_slide_count={summary.get('visual_slide_count')}; "
                f"visual_table_count={summary.get('visual_table_count')}; "
                f"visual_chart_count={summary.get('visual_chart_count')}"
            ),
        ),
        _release_check(
            "deck_polish_clean",
            _summary_int(summary, "polish_finding_count") == 0
            and _summary_int(summary, "polish_high_finding_count") == 0
            and bool(summary.get("polished_text_checked")) is True,
            (
                f"polish_finding_count={summary.get('polish_finding_count')}; "
                f"polish_high_finding_count={summary.get('polish_high_finding_count')}; "
                "polish_replacements_applied_count="
                f"{summary.get('polish_replacements_applied_count')}; "
                f"polished_text_checked={summary.get('polished_text_checked')}"
            ),
        ),
        _release_check(
            "deck_table_contract_clean",
            _summary_int(summary, "table_contract_finding_count") == 0
            and _summary_int(summary, "table_contract_high_finding_count") == 0
            and _summary_int(summary, "table_contract_medium_finding_count") == 0
            and _summary_int(summary, "table_contract_table_count") == _summary_int(
                summary,
                "table_contract_expected_table_count",
            )
            and bool(summary.get("table_contract_checked")) is True,
            (
                f"table_contract_finding_count={summary.get('table_contract_finding_count')}; "
                "table_contract_high_finding_count="
                f"{summary.get('table_contract_high_finding_count')}; "
                "table_contract_medium_finding_count="
                f"{summary.get('table_contract_medium_finding_count')}; "
                f"table_contract_table_count={summary.get('table_contract_table_count')}; "
                "table_contract_expected_table_count="
                f"{summary.get('table_contract_expected_table_count')}; "
                f"table_contract_checked={summary.get('table_contract_checked')}"
            ),
        ),
        _release_check(
            "deck_semantics_clean",
            _summary_int(summary, "semantic_high_finding_count") == 0
            and _summary_int(summary, "semantic_human_style_score") >= MIN_SEMANTIC_SCORE
            and bool(summary.get("business_readiness_checked")) is True,
            (
                f"semantic_finding_count={summary.get('semantic_finding_count')}; "
                f"semantic_high_finding_count={summary.get('semantic_high_finding_count')}; "
                f"semantic_medium_finding_count={summary.get('semantic_medium_finding_count')}; "
                f"semantic_human_style_score={summary.get('semantic_human_style_score')}; "
                f"business_readiness_checked={summary.get('business_readiness_checked')}"
            ),
        ),
        _release_check(
            "deck_render_clean",
            _summary_int(summary, "render_finding_count") == 0
            and _summary_int(summary, "render_high_finding_count") == 0
            and _summary_int(summary, "rendered_slide_count") >= MIN_VISUAL_SLIDES
            and _summary_int(summary, "render_deck_slide_count") == _summary_int(
                summary, "rendered_slide_count"
            )
            and bool(summary.get("rendered_png_checked")) is True,
            (
                f"render_finding_count={summary.get('render_finding_count')}; "
                f"render_high_finding_count={summary.get('render_high_finding_count')}; "
                f"render_deck_slide_count={summary.get('render_deck_slide_count')}; "
                f"rendered_slide_count={summary.get('rendered_slide_count')}; "
                f"rendered_png_checked={summary.get('rendered_png_checked')}"
            ),
        ),
        _release_check(
            "release_bundle_complete",
            _summary_int(summary, "release_bundle_missing_required_artifact_count") == 0
            and _summary_int(summary, "release_bundle_artifact_count") > 0
            and _summary_int(summary, "release_bundle_copied_artifact_count")
            == _summary_int(summary, "release_bundle_artifact_count")
            and _summary_int(summary, "release_bundle_zip_size_bytes") > 0
            and bool(summary.get("release_bundle_upload_ready")) is True,
            (
                "release_bundle_missing_required_artifact_count="
                f"{summary.get('release_bundle_missing_required_artifact_count')}; "
                f"release_bundle_artifact_count={summary.get('release_bundle_artifact_count')}; "
                "release_bundle_copied_artifact_count="
                f"{summary.get('release_bundle_copied_artifact_count')}; "
                f"release_bundle_zip_size_bytes={summary.get('release_bundle_zip_size_bytes')}; "
                f"release_bundle_upload_ready={summary.get('release_bundle_upload_ready')}"
            ),
        ),
        _release_check(
            "sharepoint_upload_plan_clean",
            summary.get("sharepoint_upload_plan_status") == "planned"
            and _summary_int(summary, "sharepoint_upload_planned_count") > 0
            and _summary_int(summary, "sharepoint_upload_missing_count") == 0
            and bool(summary.get("sharepoint_upload_publish_ready")) is True
            and bool(summary.get("sharepoint_upload_source_backed")) is True,
            (
                f"sharepoint_upload_plan_status={summary.get('sharepoint_upload_plan_status')}; "
                "sharepoint_upload_planned_count="
                f"{summary.get('sharepoint_upload_planned_count')}; "
                f"sharepoint_upload_missing_count={summary.get('sharepoint_upload_missing_count')}; "
                "sharepoint_upload_publish_ready="
                f"{summary.get('sharepoint_upload_publish_ready')}; "
                "sharepoint_upload_source_backed="
                f"{summary.get('sharepoint_upload_source_backed')}"
            ),
        ),
    ]


def _release_check(name: str, passed: bool, evidence: str) -> dict[str, str]:
    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "evidence": evidence,
    }


def _summary_int(summary: dict[str, Any], key: str) -> int:
    value = summary.get(key)
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return 0


def write_manifest_and_release_packet(
    *,
    manifest: dict[str, Any],
    output_path: Path,
    plan_only: bool,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    manifest["manifest_path"] = str(output_path)
    if not plan_only:
        release_packet_path = build_paths(
            str(manifest["snapshot_date"]),
            str(manifest["run_id"]),
        ).release_packet
        release_packet = release_packet_from_manifest(manifest)
        release_packet_path.parent.mkdir(parents=True, exist_ok=True)
        release_packet_path.write_text(
            json.dumps(release_packet, indent=2) + "\n",
            encoding="utf-8",
        )
        manifest["release_packet_path"] = str(release_packet_path)
        write_latest_release_index(
            manifest=manifest,
            release_packet=release_packet,
            release_packet_path=release_packet_path,
        )
    output_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")


def latest_release_index_from_manifest(
    *,
    manifest: dict[str, Any],
    release_packet: dict[str, Any],
    release_packet_path: Path,
) -> dict[str, Any]:
    summary = dict(manifest.get("summary") or {})
    artifacts = dict(release_packet.get("artifacts") or {})
    return {
        "schema_version": f"{RELEASE_PACKET_SCHEMA_VERSION}.latest_index",
        "generated_at": datetime.now(UTC).isoformat(),
        "status": release_packet.get("status"),
        "publish_recommendation": release_packet.get("publish_recommendation"),
        "snapshot_date": manifest.get("snapshot_date"),
        "run_id": manifest.get("run_id"),
        "target_org": manifest.get("target_org"),
        "runner_manifest_path": manifest.get("manifest_path"),
        "release_packet_path": str(release_packet_path),
        "source_backed_deck": artifacts.get("source_backed_deck"),
        "analyst_workbook": artifacts.get("analyst_workbook"),
        "thinkcell_workbook": artifacts.get("thinkcell_workbook"),
        "thinkcell_ppttc": artifacts.get("thinkcell_ppttc"),
        "render_audit": artifacts.get("render_audit"),
        "polish_audit": artifacts.get("polish_audit"),
        "table_contract_audit": artifacts.get("table_contract_audit"),
        "semantic_audit": artifacts.get("semantic_audit"),
        "release_bundle_manifest": artifacts.get("release_bundle_manifest"),
        "release_bundle_zip": artifacts.get("release_bundle_zip"),
        "sharepoint_upload_plan": artifacts.get("sharepoint_upload_plan"),
        "sharepoint_upload_result": artifacts.get("sharepoint_upload_result"),
        "visual_audit": artifacts.get("visual_audit"),
        "publish_gate": artifacts.get("publish_gate"),
        "release_check_count": len(release_packet.get("release_checks") or []),
        "failed_release_checks": [
            check
            for check in release_packet.get("release_checks") or []
            if check.get("status") != "pass"
        ],
        "summary": {
            key: summary.get(key)
            for key in (
                "stage_count",
                "ok_stage_count",
                "source_contract_authoring_target_count",
                "source_contract_authoring_drift_count",
                "source_contract_authoring_missing_count",
                "selected_source_count",
                "executed_source_count",
                "source_extract_count",
                "bundle_count",
                "director_bundle_count",
                "quarter_policy_name",
                "quarter_policy_fiscal_year_start_month",
                "quarter_mapping_approved",
                "business_current_quarter_label",
                "source_current_quarter_label",
                "display_current_quarter_label",
                "source_quarter_label_style",
                "current_quarter_title",
                "forward_quarter_title",
                "reporting_window_start",
                "reporting_window_end",
                "source_contract_final_warning_finding_count",
                "publish_gate_finding_count",
                "high_blocker_count",
                "tieout_mismatch_count",
                "visual_finding_count",
                "polish_finding_count",
                "table_contract_finding_count",
                "table_contract_table_count",
                "semantic_finding_count",
                "semantic_human_style_score",
                "render_finding_count",
                "rendered_slide_count",
                "release_bundle_artifact_count",
                "release_bundle_missing_required_artifact_count",
                "release_bundle_zip_size_bytes",
                "release_bundle_upload_ready",
                "sharepoint_upload_plan_status",
                "sharepoint_upload_planned_count",
                "sharepoint_upload_missing_count",
                "sharepoint_upload_publish_ready",
                "sharepoint_upload_source_backed",
                "sharepoint_upload_status",
                "sharepoint_uploaded_count",
                "sharepoint_upload_skipped_count",
            )
        },
    }


def latest_release_markdown(index: dict[str, Any]) -> str:
    summary = index.get("summary") or {}
    failed_checks = index.get("failed_release_checks") or []
    lines = [
        f"# Source-Backed Monthly Latest — {index.get('snapshot_date')}",
        "",
        f"- Status: `{index.get('status')}`",
        f"- Publish recommendation: `{index.get('publish_recommendation')}`",
        f"- Run ID: `{index.get('run_id')}`",
        f"- Runner manifest: `{index.get('runner_manifest_path')}`",
        f"- Release packet: `{index.get('release_packet_path')}`",
        f"- Deck: `{index.get('source_backed_deck')}`",
        f"- Analyst workbook: `{index.get('analyst_workbook')}`",
        f"- think-cell workbook: `{index.get('thinkcell_workbook')}`",
        f"- Polish audit: `{index.get('polish_audit')}`",
        f"- Table contract audit: `{index.get('table_contract_audit')}`",
        f"- Semantic audit: `{index.get('semantic_audit')}`",
        f"- Render audit: `{index.get('render_audit')}`",
        f"- Release bundle: `{index.get('release_bundle_zip')}`",
        f"- SharePoint upload plan: `{index.get('sharepoint_upload_plan')}`",
        f"- SharePoint upload result: `{index.get('sharepoint_upload_result')}`",
        "",
        "## Gate Summary",
        "",
        f"- Stages: `{summary.get('ok_stage_count')}` / `{summary.get('stage_count')}`",
        (
            "- YAML authoring sync: "
            f"`{summary.get('source_contract_authoring_target_count')}` targets, "
            f"`{summary.get('source_contract_authoring_drift_count')}` drifted"
        ),
        (
            f"- Sources: `{summary.get('executed_source_count')}` / "
            f"`{summary.get('selected_source_count')}`"
        ),
        f"- Bundles: `{summary.get('bundle_count')}` source / `{summary.get('director_bundle_count')}` director",
        (
            "- Final source contract warnings: "
            f"`{summary.get('source_contract_final_warning_finding_count')}`"
        ),
        (
            f"- Quarter policy: `{summary.get('quarter_policy_name')}`, "
            f"current `{summary.get('current_quarter_title')}`, "
            f"forward `{summary.get('forward_quarter_title')}`"
        ),
        (
            f"- Quarter mapping: business `{summary.get('business_current_quarter_label')}` → "
            f"source `{summary.get('source_current_quarter_label')}` → "
            f"display `{summary.get('display_current_quarter_label')}`, "
            f"approved `{summary.get('quarter_mapping_approved')}`"
        ),
        (
            f"- Truth blockers / tie-out mismatches: `{summary.get('high_blocker_count')}` / "
            f"`{summary.get('tieout_mismatch_count')}`"
        ),
        (
            f"- Visual/polish/table/semantic/render findings: `{summary.get('visual_finding_count')}` / "
            f"`{summary.get('polish_finding_count')}` / "
            f"`{summary.get('table_contract_finding_count')}` / "
            f"`{summary.get('semantic_finding_count')}` / "
            f"`{summary.get('render_finding_count')}`"
        ),
        f"- Semantic score: `{summary.get('semantic_human_style_score')}`",
        f"- Rendered slides: `{summary.get('rendered_slide_count')}`",
        (
            f"- Release bundle artifacts: `{summary.get('release_bundle_artifact_count')}`, "
            f"missing `{summary.get('release_bundle_missing_required_artifact_count')}`, "
            f"upload-ready `{summary.get('release_bundle_upload_ready')}`"
        ),
        (
            f"- SharePoint upload plan: `{summary.get('sharepoint_upload_plan_status')}`, "
            f"planned `{summary.get('sharepoint_upload_planned_count')}`, "
            f"missing `{summary.get('sharepoint_upload_missing_count')}`"
        ),
        (
            f"- SharePoint upload: `{summary.get('sharepoint_upload_status')}`, "
            f"uploaded `{summary.get('sharepoint_uploaded_count')}`, "
            f"skipped `{summary.get('sharepoint_upload_skipped_count')}`"
        ),
        "",
        "## Failed Release Checks",
        "",
    ]
    if failed_checks:
        lines.extend(
            f"- `{check.get('name')}`: {check.get('evidence')}" for check in failed_checks
        )
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def write_latest_release_index(
    *,
    manifest: dict[str, Any],
    release_packet: dict[str, Any],
    release_packet_path: Path,
) -> None:
    index = latest_release_index_from_manifest(
        manifest=manifest,
        release_packet=release_packet,
        release_packet_path=release_packet_path,
    )
    markdown = latest_release_markdown(index)
    output_root = ROOT / "output" / "source_backed_monthly_pipeline_runs"
    snapshot_date = str(manifest["snapshot_date"])
    for base in (output_root, output_root / snapshot_date):
        base.mkdir(parents=True, exist_ok=True)
        (base / "latest.json").write_text(
            json.dumps(index, indent=2) + "\n",
            encoding="utf-8",
        )
        (base / "latest.md").write_text(markdown, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--target-org", default=DEFAULT_TARGET_ORG)
    parser.add_argument("--requirements", type=Path, default=DEFAULT_REQUIREMENTS_PATH)
    parser.add_argument("--territory-config", type=Path, default=DEFAULT_TERRITORY_CONFIG)
    parser.add_argument("--bundle-contract", type=Path, default=DEFAULT_BUNDLE_CONTRACT)
    parser.add_argument("--field-guardrails", type=Path, default=DEFAULT_FIELD_GUARDRAILS)
    parser.add_argument(
        "--source-contract-authoring",
        type=Path,
        default=DEFAULT_SOURCE_CONTRACT_AUTHORING,
    )
    parser.add_argument("--template-path", default=DEFAULT_TEMPLATE_PATH)
    parser.add_argument("--plan-only", action="store_true")
    parser.add_argument("--keep-going", action="store_true")
    parser.add_argument("--start-at")
    parser.add_argument("--sharepoint-upload", action="store_true")
    parser.add_argument("--output-path", type=Path)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    output_path = args.output_path or default_manifest_path(args.snapshot_date, args.run_id)
    try:
        if args.plan_only:
            manifest = run_pipeline(
                snapshot_date=args.snapshot_date,
                run_id=args.run_id,
                target_org=args.target_org,
                requirements_path=args.requirements,
                territory_config_path=args.territory_config,
                bundle_contract_path=args.bundle_contract,
                field_guardrails_path=args.field_guardrails,
                source_contract_authoring_path=args.source_contract_authoring,
                template_path=args.template_path,
                plan_only=args.plan_only,
                keep_going=args.keep_going,
                start_at=args.start_at,
                sharepoint_upload=args.sharepoint_upload,
            )
        else:
            with acquire_run_lock(args.snapshot_date, args.run_id):
                manifest = run_pipeline(
                    snapshot_date=args.snapshot_date,
                    run_id=args.run_id,
                    target_org=args.target_org,
                    requirements_path=args.requirements,
                    territory_config_path=args.territory_config,
                    bundle_contract_path=args.bundle_contract,
                    field_guardrails_path=args.field_guardrails,
                    source_contract_authoring_path=args.source_contract_authoring,
                    template_path=args.template_path,
                    plan_only=args.plan_only,
                    keep_going=args.keep_going,
                    start_at=args.start_at,
                    sharepoint_upload=args.sharepoint_upload,
                )
    except RuntimeError as exc:
        manifest = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": datetime.now(UTC).isoformat(),
            "status": "blocked",
            "snapshot_date": args.snapshot_date,
            "run_id": args.run_id,
            "target_org": args.target_org,
            "failed_stage": "run_lock",
            "blocking_reason": str(exc),
            "paths": _paths_for_manifest(build_paths(args.snapshot_date, args.run_id)),
            "stages": [],
        }
        manifest["summary"] = summarize_manifest(manifest)
    write_manifest_and_release_packet(
        manifest=manifest,
        output_path=output_path,
        plan_only=args.plan_only,
    )
    print(json.dumps(manifest, indent=2) + "\n")
    return 0 if manifest["status"] in {"ok", "planned"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
