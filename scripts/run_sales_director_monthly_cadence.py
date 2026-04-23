#!/usr/bin/env python3
"""Thin cadence wrapper around the Sales Director monthly master builder."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

try:
    from monthly_platform.period import resolve_period_context
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform.period import resolve_period_context


REPO_ROOT = Path(__file__).resolve().parents[1]
SOURCE_CONTRACT_PREFLIGHT = (
    REPO_ROOT / "scripts" / "audit_sales_director_source_contract.py"
)
SOURCE_CONTRACT_SNAPSHOT_DIFF = (
    REPO_ROOT / "scripts" / "diff_source_contract_snapshots.py"
)
FORWARD_QUARTER_REGISTRY_REFRESH = (
    REPO_ROOT / "scripts" / "refresh_forward_quarter_registry.py"
)
DIRECTOR_LIVE_EXTRACT = REPO_ROOT / "scripts" / "extract_director_live.py"
DIRECTOR_LIVE_EXTRACT_SNAPSHOT_DIFF = (
    REPO_ROOT / "scripts" / "diff_director_live_extract_snapshots.py"
)
HISTORICAL_TRENDING_EXTRACT = REPO_ROOT / "scripts" / "extract_historical_trending.py"
HISTORICAL_TRENDING_SNAPSHOT_DIFF = (
    REPO_ROOT / "scripts" / "diff_historical_trending_snapshots.py"
)
DIRECTOR_WORKBOOK_CONTRACT = (
    REPO_ROOT / "scripts" / "validate_director_workbook_contract.py"
)
DIRECTOR_WORKBOOK_CONTRACT_SNAPSHOT_DIFF = (
    REPO_ROOT / "scripts" / "diff_director_workbook_contract_snapshots.py"
)
SHAREPOINT_ANALYSIS_BUILDER = REPO_ROOT / "scripts" / "build_sharepoint_analysis.py"
SHAREPOINT_DASHBOARD_ANALYSIS_BUILDER = (
    REPO_ROOT / "scripts" / "build_dashboard_analysis_excel.py"
)
SHAREPOINT_ANALYSIS_CONTRACT = (
    REPO_ROOT / "scripts" / "validate_sharepoint_analysis_contract.py"
)
MASTER_BUILDER = REPO_ROOT / "scripts" / "run_sales_director_monthly_master_builder.py"
PROMOTE_BATCH = REPO_ROOT / "scripts" / "promote_sales_director_batch_canonical.py"
REGION_MONTHLY_BUILDER = REPO_ROOT / "scripts" / "run_sales_region_monthly_builder.py"
GLOBAL_SUMMARY_BUILDER = REPO_ROOT / "scripts" / "run_sales_global_summary_builder.py"
GLOBAL_CANONICAL_SHELL_BUILDER = (
    REPO_ROOT / "scripts" / "run_sales_global_canonical_shell_builder.py"
)
RELEASE_PACKET = REPO_ROOT / "scripts" / "build_sales_deck_release_packet.py"
SHAREPOINT_UPLOAD = REPO_ROOT / "scripts" / "upload_sales_deck_release_to_sharepoint.py"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_director_monthly_cadence"
DEFAULT_WORKBOOK_ROOT = REPO_ROOT / "output" / "director_live_workbooks"
DEFAULT_SHAREPOINT_ROOT = REPO_ROOT / "output" / "sharepoint"
MONTHLY_REGION_NAMES = ("APAC", "EMEA", "North America")

_TERRITORY_TO_SHAREPOINT_LABEL: dict[str, str] = {
    "APAC": "APAC",
    "Central Europe": "EMEA Central",
    "UK & Ireland": "EMEA UK & Ireland",
    "Southern Europe": "EMEA South West",
    "NL & Nordics": "EMEA NE",
    "Middle East & Africa": "EMEA MEA",
    "Canada": "NA Canada",
    "NA Asset Management": "NA Asset Mgmt",
    "Pension & Insurance": "NA Insurance",
}

_SHAREPOINT_TERRITORY_LABELS_FALLBACK = (
    "APAC",
    "EMEA Central",
    "EMEA UK & Ireland",
    "EMEA NE",
    "EMEA South West",
    "EMEA MEA",
    "NA Asset Mgmt",
    "NA Canada",
    "NA Insurance",
)


def _load_sharepoint_territory_labels() -> tuple[str, ...]:
    cfg_path = REPO_ROOT / "config" / "sd_monthly_territories.json"
    if not cfg_path.exists():
        return _SHAREPOINT_TERRITORY_LABELS_FALLBACK
    try:
        territories = json.loads(cfg_path.read_text(encoding="utf-8")).get(
            "territories", {}
        )
        return tuple(
            _TERRITORY_TO_SHAREPOINT_LABEL.get(key, key) for key in territories
        )
    except Exception:
        return _SHAREPOINT_TERRITORY_LABELS_FALLBACK


SHAREPOINT_TERRITORY_LABELS = _load_sharepoint_territory_labels()
DECK_SOURCE_CHOICES = (
    "canonical-shell",
    "shell",
    "template",
    "existing",
    "workbook-native",
    "skip",
)


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8"
    )


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def display_path(path: Path | None) -> str | None:
    if path is None:
        return None
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def trim_output(text: str, *, max_lines: int = 40) -> str:
    lines = [line.rstrip() for line in str(text or "").splitlines()]
    if len(lines) <= max_lines:
        return "\n".join(lines)
    return "\n".join(["...", *lines[-max_lines:]])


def parse_json_output(stdout: str) -> dict[str, Any] | None:
    stripped = stdout.strip()
    if not stripped:
        return None
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, dict):
        return payload

    decoder = json.JSONDecoder()
    for index in range(len(stripped) - 1, -1, -1):
        if stripped[index] != "{":
            continue
        try:
            payload, end = decoder.raw_decode(stripped[index:])
        except json.JSONDecodeError:
            continue
        if index + end == len(stripped) and isinstance(payload, dict):
            return payload
    return None


def run_json_command(cmd: list[str]) -> dict[str, Any]:
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    payload = parse_json_output(proc.stdout)

    if proc.returncode == 0:
        if payload is None:
            raise ValueError("Monthly master builder returned non-JSON output.")
        return payload

    if payload is not None:
        payload["builder_returncode"] = proc.returncode
        if proc.stderr.strip():
            payload["builder_stderr"] = proc.stderr.strip()
        return payload

    raise subprocess.CalledProcessError(
        proc.returncode,
        proc.args,
        output=proc.stdout,
        stderr=proc.stderr,
    )


def run_script_command(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )


def summarize_extraction_artifact(
    stage_name: str,
    artifact_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    if not artifact_payload:
        return {}
    if stage_name == "0_source_contract_preflight":
        active_lane = artifact_payload.get("active_lane") or {}
        candidate_lane = (
            artifact_payload.get("candidate_forward_quarter")
            or artifact_payload.get("candidate_q3")
            or {}
        )
        return {
            "run_date": artifact_payload.get("run_date"),
            "active_lane_status": active_lane.get("status"),
            "candidate_lane_status": candidate_lane.get("status"),
            "candidate_quarter_title": candidate_lane.get("quarter_title"),
        }
    if stage_name == "0b_source_contract_snapshot_diff":
        return {
            "status": artifact_payload.get("status"),
            "baseline_run_date": artifact_payload.get("baseline_run_date"),
            "current_run_date": artifact_payload.get("current_run_date"),
            "active_issue_delta": (
                (artifact_payload.get("active_lane") or {}).get("issue_delta") or {}
            ),
        }
    if stage_name == "0c_forward_quarter_registry_refresh":
        return {
            "run_date": artifact_payload.get("run_date"),
            "quarter_title": artifact_payload.get("quarter_title"),
            "promoted_count": artifact_payload.get("promoted_count"),
            "already_current_count": artifact_payload.get("already_current_count"),
            "conflict_count": artifact_payload.get("conflict_count"),
        }
    if stage_name == "1a_extract_salesforce":
        return {
            "status": artifact_payload.get("status"),
            "processed_count": len(list(artifact_payload.get("processed") or [])),
            "failure_count": len(list(artifact_payload.get("failures") or [])),
            "query_telemetry_totals": artifact_payload.get("query_telemetry_totals")
            or {},
        }
    if stage_name == "1a2_director_live_extract_snapshot_diff":
        extract = artifact_payload.get("extract") or {}
        return {
            "status": artifact_payload.get("status"),
            "baseline_run_date": artifact_payload.get("baseline_run_date"),
            "current_run_date": artifact_payload.get("current_run_date"),
            "processed_count_before": extract.get("processed_count_before"),
            "processed_count_after": extract.get("processed_count_after"),
            "failure_count_before": extract.get("failure_count_before"),
            "failure_count_after": extract.get("failure_count_after"),
        }
    if stage_name == "1b_extract_historical_trending":
        return {
            "status": artifact_payload.get("status"),
            "processed_count": len(list(artifact_payload.get("processed") or [])),
            "failure_count": len(list(artifact_payload.get("failures") or [])),
            "current_quarter_title": artifact_payload.get("current_quarter_title"),
        }
    if stage_name == "1b2_historical_trending_snapshot_diff":
        historical = artifact_payload.get("historical_trending") or {}
        return {
            "status": artifact_payload.get("status"),
            "baseline_run_date": artifact_payload.get("baseline_run_date"),
            "current_run_date": artifact_payload.get("current_run_date"),
            "processed_count_before": historical.get("processed_count_before"),
            "processed_count_after": historical.get("processed_count_after"),
            "failure_count_before": historical.get("failure_count_before"),
            "failure_count_after": historical.get("failure_count_after"),
        }
    if stage_name == "1b3_validate_director_workbook_contract":
        return {
            "status": artifact_payload.get("status"),
            "validated_count": len(list(artifact_payload.get("validated") or [])),
            "failure_count": len(list(artifact_payload.get("failures") or [])),
            "warning_count": len(list(artifact_payload.get("warnings") or [])),
        }
    if stage_name == "1b4_director_workbook_contract_snapshot_diff":
        contract = artifact_payload.get("workbook_contract") or {}
        return {
            "status": artifact_payload.get("status"),
            "baseline_run_date": artifact_payload.get("baseline_run_date"),
            "current_run_date": artifact_payload.get("current_run_date"),
            "validated_count_before": contract.get("validated_count_before"),
            "validated_count_after": contract.get("validated_count_after"),
            "failure_count_before": contract.get("failure_count_before"),
            "failure_count_after": contract.get("failure_count_after"),
            "warning_count_before": contract.get("warning_count_before"),
            "warning_count_after": contract.get("warning_count_after"),
        }
    return {
        "status": artifact_payload.get("status"),
        "run_date": artifact_payload.get("run_date"),
    }


def run_audited_stage(
    *,
    stage_name: str,
    cmd: list[str],
    artifact_json_path: Path | None = None,
    artifact_summary_path: Path | None = None,
) -> dict[str, Any]:
    proc = run_script_command(cmd)
    artifact_payload = None
    artifact_error = None
    if artifact_json_path is not None and artifact_json_path.exists():
        try:
            artifact_payload = load_json(artifact_json_path)
        except Exception as exc:  # pragma: no cover
            artifact_error = str(exc)
    stage_status = "ok"
    if proc.returncode != 0:
        stage_status = "failed" if artifact_payload is not None else "error"
    elif artifact_payload is not None and isinstance(
        artifact_payload.get("status"), str
    ):
        stage_status = str(artifact_payload.get("status"))
    return {
        "name": stage_name,
        "status": stage_status,
        "returncode": proc.returncode,
        "command": cmd,
        "stdout_tail": trim_output(proc.stdout),
        "stderr_tail": trim_output(proc.stderr),
        "artifact_json_path": display_path(artifact_json_path),
        "artifact_summary_path": display_path(artifact_summary_path),
        "artifact_error": artifact_error,
        "summary": summarize_extraction_artifact(stage_name, artifact_payload),
    }


def extraction_stage_index(
    stages: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    return {str(stage.get("name")): stage for stage in stages}


def run_extraction_chain(
    *,
    snapshot_date: str,
    workbook_root: Path | None = None,
    skip_extract: bool = False,
) -> dict[str, Any]:
    resolved_workbook_root = Path(workbook_root or DEFAULT_WORKBOOK_ROOT)
    workbook_dir = resolved_workbook_root / snapshot_date
    if skip_extract:
        return {
            "status": "skipped",
            "reason": "skip_extract=true",
            "snapshot_date": snapshot_date,
            "workbook_root": str(resolved_workbook_root),
            "workbook_dir": str(workbook_dir),
            "stages": [],
        }

    stages: list[dict[str, Any]] = []

    source_contract_stage = run_audited_stage(
        stage_name="0_source_contract_preflight",
        cmd=["python3", str(SOURCE_CONTRACT_PREFLIGHT), "--date", snapshot_date],
        artifact_json_path=(
            REPO_ROOT
            / "output"
            / "source_contract_audit"
            / snapshot_date
            / "source_contract_audit.json"
        ),
        artifact_summary_path=(
            REPO_ROOT
            / "output"
            / "source_contract_audit"
            / snapshot_date
            / "summary.md"
        ),
    )
    stages.append(source_contract_stage)
    stages.append(
        run_audited_stage(
            stage_name="0b_source_contract_snapshot_diff",
            cmd=[
                "python3",
                str(SOURCE_CONTRACT_SNAPSHOT_DIFF),
                "--current-date",
                snapshot_date,
            ],
            artifact_json_path=(
                REPO_ROOT
                / "output"
                / "source_contract_snapshot_diff"
                / snapshot_date
                / "source_contract_snapshot_diff.json"
            ),
            artifact_summary_path=(
                REPO_ROOT
                / "output"
                / "source_contract_snapshot_diff"
                / snapshot_date
                / "summary.md"
            ),
        )
    )
    stages.append(
        run_audited_stage(
            stage_name="0c_forward_quarter_registry_refresh",
            cmd=[
                "python3",
                str(FORWARD_QUARTER_REGISTRY_REFRESH),
                "--date",
                snapshot_date,
            ],
            artifact_json_path=(
                REPO_ROOT
                / "output"
                / "source_contract_registry_refresh"
                / snapshot_date
                / "registry_refresh.json"
            ),
            artifact_summary_path=(
                REPO_ROOT
                / "output"
                / "source_contract_registry_refresh"
                / snapshot_date
                / "summary.md"
            ),
        )
    )

    if source_contract_stage["returncode"] != 0:
        return {
            "status": "blocked",
            "failed_stage": source_contract_stage["name"],
            "snapshot_date": snapshot_date,
            "workbook_root": str(resolved_workbook_root),
            "workbook_dir": str(workbook_dir),
            "stages": stages,
        }

    live_extract_stage = run_audited_stage(
        stage_name="1a_extract_salesforce",
        cmd=[
            "python3",
            str(DIRECTOR_LIVE_EXTRACT),
            "--all",
            "--snapshot-date",
            snapshot_date,
            "--output-root",
            str(resolved_workbook_root),
        ],
        artifact_json_path=(
            REPO_ROOT
            / "output"
            / "director_live_extract"
            / snapshot_date
            / "director_live_extract_audit.json"
        ),
        artifact_summary_path=(
            REPO_ROOT
            / "output"
            / "director_live_extract"
            / snapshot_date
            / "summary.md"
        ),
    )
    stages.append(live_extract_stage)
    live_extract_audit_path = (
        REPO_ROOT
        / "output"
        / "director_live_extract"
        / snapshot_date
        / "director_live_extract_audit.json"
    )
    if live_extract_audit_path.exists():
        stages.append(
            run_audited_stage(
                stage_name="1a2_director_live_extract_snapshot_diff",
                cmd=[
                    "python3",
                    str(DIRECTOR_LIVE_EXTRACT_SNAPSHOT_DIFF),
                    "--current-date",
                    snapshot_date,
                ],
                artifact_json_path=(
                    REPO_ROOT
                    / "output"
                    / "director_live_extract_snapshot_diff"
                    / snapshot_date
                    / "director_live_extract_snapshot_diff.json"
                ),
                artifact_summary_path=(
                    REPO_ROOT
                    / "output"
                    / "director_live_extract_snapshot_diff"
                    / snapshot_date
                    / "summary.md"
                ),
            )
        )
    if live_extract_stage["returncode"] != 0:
        return {
            "status": "blocked",
            "failed_stage": live_extract_stage["name"],
            "snapshot_date": snapshot_date,
            "workbook_root": str(resolved_workbook_root),
            "workbook_dir": str(workbook_dir),
            "stages": stages,
        }

    historical_stage = run_audited_stage(
        stage_name="1b_extract_historical_trending",
        cmd=[
            "python3",
            str(HISTORICAL_TRENDING_EXTRACT),
            "--snapshot-date",
            snapshot_date,
            "--workbooks-dir",
            str(workbook_dir),
        ],
        artifact_json_path=(
            REPO_ROOT
            / "output"
            / "historical_trending_extract"
            / snapshot_date
            / "historical_trending_extract_audit.json"
        ),
        artifact_summary_path=(
            REPO_ROOT
            / "output"
            / "historical_trending_extract"
            / snapshot_date
            / "summary.md"
        ),
    )
    stages.append(historical_stage)
    historical_audit_path = (
        REPO_ROOT
        / "output"
        / "historical_trending_extract"
        / snapshot_date
        / "historical_trending_extract_audit.json"
    )
    if historical_audit_path.exists():
        stages.append(
            run_audited_stage(
                stage_name="1b2_historical_trending_snapshot_diff",
                cmd=[
                    "python3",
                    str(HISTORICAL_TRENDING_SNAPSHOT_DIFF),
                    "--current-date",
                    snapshot_date,
                ],
                artifact_json_path=(
                    REPO_ROOT
                    / "output"
                    / "historical_trending_snapshot_diff"
                    / snapshot_date
                    / "historical_trending_snapshot_diff.json"
                ),
                artifact_summary_path=(
                    REPO_ROOT
                    / "output"
                    / "historical_trending_snapshot_diff"
                    / snapshot_date
                    / "summary.md"
                ),
            )
        )
    if historical_stage["returncode"] != 0:
        return {
            "status": "blocked",
            "failed_stage": historical_stage["name"],
            "snapshot_date": snapshot_date,
            "workbook_root": str(resolved_workbook_root),
            "workbook_dir": str(workbook_dir),
            "stages": stages,
        }

    workbook_contract_stage = run_audited_stage(
        stage_name="1b3_validate_director_workbook_contract",
        cmd=[
            "python3",
            str(DIRECTOR_WORKBOOK_CONTRACT),
            "--snapshot-date",
            snapshot_date,
            "--workbooks-dir",
            str(workbook_dir),
            "--require-historical",
        ],
        artifact_json_path=(
            REPO_ROOT
            / "output"
            / "director_workbook_contract"
            / snapshot_date
            / "director_workbook_contract_audit.json"
        ),
        artifact_summary_path=(
            REPO_ROOT
            / "output"
            / "director_workbook_contract"
            / snapshot_date
            / "summary.md"
        ),
    )
    stages.append(workbook_contract_stage)
    workbook_contract_audit_path = (
        REPO_ROOT
        / "output"
        / "director_workbook_contract"
        / snapshot_date
        / "director_workbook_contract_audit.json"
    )
    if workbook_contract_audit_path.exists():
        stages.append(
            run_audited_stage(
                stage_name="1b4_director_workbook_contract_snapshot_diff",
                cmd=[
                    "python3",
                    str(DIRECTOR_WORKBOOK_CONTRACT_SNAPSHOT_DIFF),
                    "--current-date",
                    snapshot_date,
                ],
                artifact_json_path=(
                    REPO_ROOT
                    / "output"
                    / "director_workbook_contract_snapshot_diff"
                    / snapshot_date
                    / "director_workbook_contract_snapshot_diff.json"
                ),
                artifact_summary_path=(
                    REPO_ROOT
                    / "output"
                    / "director_workbook_contract_snapshot_diff"
                    / snapshot_date
                    / "summary.md"
                ),
            )
        )
    final_status = "ok"
    failed_stage = None
    if workbook_contract_stage["returncode"] != 0:
        final_status = "blocked"
        failed_stage = workbook_contract_stage["name"]

    return {
        "status": final_status,
        "failed_stage": failed_stage,
        "snapshot_date": snapshot_date,
        "workbook_root": str(resolved_workbook_root),
        "workbook_dir": str(workbook_dir),
        "stages": stages,
    }


def run_builder(args: list[str]) -> dict[str, Any]:
    return run_json_command(["python3", str(MASTER_BUILDER), *args])


def run_promotion(
    *,
    run_dir: Path,
    allow_audit_findings: bool = False,
) -> dict[str, Any]:
    cmd = ["python3", str(PROMOTE_BATCH), "--run-dir", str(run_dir)]
    if allow_audit_findings:
        cmd.append("--allow-audit-findings")
    return run_json_command(cmd)


def run_release_packet(
    *,
    snapshot_date: str,
    director_run_dir: Path,
    global_run_dir: Path | None = None,
    global_canonical_run_dir: Path | None = None,
    sharepoint_root: Path | None = None,
) -> dict[str, Any]:
    cmd = [
        "python3",
        str(RELEASE_PACKET),
        "--snapshot-date",
        snapshot_date,
        "--director-run-dir",
        str(director_run_dir),
    ]
    if global_run_dir is not None:
        cmd.extend(["--global-run-dir", str(global_run_dir)])
    if global_canonical_run_dir is not None:
        cmd.extend(["--global-canonical-run-dir", str(global_canonical_run_dir)])
    if sharepoint_root is not None:
        cmd.extend(["--sharepoint-root", str(sharepoint_root)])
    return run_json_command(cmd)


def run_sharepoint_analysis_chain(
    *,
    snapshot_date: str,
    workbook_dir: Path,
    sharepoint_root: Path,
) -> dict[str, Any]:
    stages: list[dict[str, Any]] = []

    def run_step(
        name: str, cmd: list[str], outputs: list[Path] | None = None
    ) -> subprocess.CompletedProcess[str]:
        proc = run_script_command(cmd)
        stages.append(
            {
                "name": name,
                "command": [str(part) for part in cmd],
                "returncode": proc.returncode,
                "stdout_tail": trim_output(proc.stdout),
                "stderr_tail": trim_output(proc.stderr),
                "outputs": [
                    display_path(path) for path in outputs or [] if path.exists()
                ],
            }
        )
        return proc

    period = resolve_period_context(snapshot_date=snapshot_date)
    fy = period.fiscal_year

    if sharepoint_root.exists():
        for stale in sharepoint_root.glob("*.xlsx"):
            if not stale.name.startswith("~"):
                stale.unlink()
    sharepoint_root.mkdir(parents=True, exist_ok=True)

    master_output = sharepoint_root / f"{fy} Pipeline Review, All Territories.xlsx"
    proc = run_step(
        "2a_sharepoint_master",
        [
            "python3",
            str(SHAREPOINT_ANALYSIS_BUILDER),
            "--workbooks-dir",
            str(workbook_dir),
            "--date",
            snapshot_date,
            "--output",
            str(master_output),
        ],
        outputs=[master_output],
    )
    if proc.returncode != 0:
        return {
            "status": "blocked",
            "snapshot_date": snapshot_date,
            "sharepoint_root": str(sharepoint_root),
            "failed_stage": "2a_sharepoint_master",
            "stages": stages,
        }

    for territory in SHAREPOINT_TERRITORY_LABELS:
        safe_name = territory.replace(" ", "_").replace("&", "and").replace("/", "-")
        output_path = sharepoint_root / f"{fy} Pipeline Review, {territory}.xlsx"
        proc = run_step(
            f"2a2_sharepoint_{safe_name}",
            [
                "python3",
                str(SHAREPOINT_ANALYSIS_BUILDER),
                "--workbooks-dir",
                str(workbook_dir),
                "--date",
                snapshot_date,
                "--territory",
                territory,
                "--output",
                str(output_path),
            ],
            outputs=[output_path],
        )
        if proc.returncode != 0:
            return {
                "status": "blocked",
                "snapshot_date": snapshot_date,
                "sharepoint_root": str(sharepoint_root),
                "failed_stage": f"2a2_sharepoint_{safe_name}",
                "stages": stages,
            }

    dashboard_output = sharepoint_root / "Dashboard and Q1 Analysis.xlsx"
    proc = run_step(
        "2b_dashboard_analysis",
        [
            "python3",
            str(SHAREPOINT_DASHBOARD_ANALYSIS_BUILDER),
            "--workbooks-dir",
            str(workbook_dir),
            "--output",
            str(dashboard_output),
        ],
        outputs=[dashboard_output],
    )
    if proc.returncode != 0:
        return {
            "status": "blocked",
            "snapshot_date": snapshot_date,
            "sharepoint_root": str(sharepoint_root),
            "failed_stage": "2b_dashboard_analysis",
            "stages": stages,
        }

    proc = run_step(
        "2b2_validate_sharepoint_analysis_contract",
        [
            "python3",
            str(SHAREPOINT_ANALYSIS_CONTRACT),
            "--date",
            snapshot_date,
            "--sharepoint-root",
            str(sharepoint_root),
        ],
        outputs=[
            REPO_ROOT
            / "output"
            / "sharepoint_analysis_contract"
            / snapshot_date
            / "sharepoint_analysis_contract_audit.json",
            REPO_ROOT
            / "output"
            / "sharepoint_analysis_contract"
            / snapshot_date
            / "summary.md",
        ],
    )
    contract_payload: dict[str, Any] = {}
    contract_path = (
        REPO_ROOT
        / "output"
        / "sharepoint_analysis_contract"
        / snapshot_date
        / "sharepoint_analysis_contract_audit.json"
    )
    if contract_path.exists():
        contract_payload = load_json(contract_path)
    if proc.returncode != 0:
        return {
            "status": "blocked",
            "snapshot_date": snapshot_date,
            "sharepoint_root": str(sharepoint_root),
            "failed_stage": "2b2_validate_sharepoint_analysis_contract",
            "stages": stages,
            "contract": {
                "status": contract_payload.get("status"),
                "validated_count": len(contract_payload.get("validated") or []),
                "failure_count": len(contract_payload.get("failures") or []),
                "warning_count": len(contract_payload.get("warnings") or []),
            },
        }

    return {
        "status": "ok",
        "snapshot_date": snapshot_date,
        "sharepoint_root": str(sharepoint_root),
        "failed_stage": None,
        "stages": stages,
        "generated_workbook_count": len(list(sharepoint_root.glob("*.xlsx"))),
        "contract": {
            "status": contract_payload.get("status"),
            "validated_count": len(contract_payload.get("validated") or []),
            "failure_count": len(contract_payload.get("failures") or []),
            "warning_count": len(contract_payload.get("warnings") or []),
        },
    }


def run_sharepoint_upload(*, release_dir: Path) -> dict[str, Any]:
    return run_json_command(
        [
            "python3",
            str(SHAREPOINT_UPLOAD),
            "--release-dir",
            str(release_dir),
        ]
    )


def run_region_monthly_builder(
    *, snapshot_date: str, region_name: str
) -> dict[str, Any]:
    return run_json_command(
        [
            "python3",
            str(REGION_MONTHLY_BUILDER),
            "--snapshot-date",
            snapshot_date,
            "--region-name",
            region_name,
            "--shell-source",
            "generated",
            "--allow-generated-shell-fallback",
        ]
    )


def run_global_summary_builder(*, snapshot_date: str) -> dict[str, Any]:
    return run_json_command(
        [
            "python3",
            str(GLOBAL_SUMMARY_BUILDER),
            "--snapshot-date",
            snapshot_date,
        ]
    )


def run_global_canonical_shell_builder(
    *,
    snapshot_date: str,
    baseline_deck_path: Path,
) -> dict[str, Any]:
    return run_json_command(
        [
            "python3",
            str(GLOBAL_CANONICAL_SHELL_BUILDER),
            "--snapshot-date",
            snapshot_date,
            "--baseline-deck-path",
            str(baseline_deck_path),
            "--promote-on-success",
        ]
    )


def shared_args(*, include_director: bool) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--snapshot-date")
    parser.add_argument("--as-of-date")
    parser.add_argument("--deck-date")
    parser.add_argument(
        "--workbook-root",
        type=Path,
        default=None,
        help="Override the workbook root passed through to the master builder.",
    )
    parser.add_argument(
        "--snapshot-root",
        type=Path,
        default=None,
        help="Override the snapshot root passed through to the master builder.",
    )
    if include_director:
        parser.add_argument("--director")
    parser.add_argument(
        "--deck-source",
        choices=DECK_SOURCE_CHOICES,
        default="canonical-shell",
    )
    parser.add_argument("--fallback-workbook-deck", action="store_true")
    parser.add_argument("--allow-generated-shell-fallback", action="store_true")
    parser.add_argument("--skip-excel-brief", action="store_true")
    parser.add_argument("--skip-powerpoint-review", action="store_true")
    parser.add_argument("--refresh-snapshots", action="store_true")
    parser.add_argument("--fail-fast", action="store_true")
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Reuse existing workbook artifacts instead of running Stage 0/1 extraction.",
    )
    parser.add_argument(
        "--unattended",
        action="store_true",
        help=(
            "Run the native deterministic preview lane without interactive "
            "Excel or PowerPoint agent steps."
        ),
    )
    parser.add_argument(
        "--powerpoint-mode", choices=("audit", "build"), default="audit"
    )
    parser.add_argument(
        "--build-release-packet",
        dest="build_release_packet",
        action="store_true",
        help="Build the Sales Director release packet after promotion.",
    )
    parser.add_argument(
        "--skip-release-packet",
        dest="build_release_packet",
        action="store_false",
        help="Skip release-packet generation even for monthly-run.",
    )
    parser.set_defaults(build_release_packet=False)
    parser.add_argument(
        "--sharepoint-upload",
        dest="sharepoint_upload",
        action="store_true",
        help="Upload publish assets to SharePoint after a clean release packet.",
    )
    parser.add_argument(
        "--skip-sharepoint-upload",
        dest="sharepoint_upload",
        action="store_false",
        help="Skip SharePoint upload even for monthly-run.",
    )
    parser.set_defaults(sharepoint_upload=False)
    parser.add_argument("--sharepoint-root", type=Path, default=DEFAULT_SHAREPOINT_ROOT)
    parser.add_argument("--global-run-dir", type=Path, default=None)
    parser.add_argument("--global-canonical-run-dir", type=Path, default=None)
    parser.add_argument("--allow-audit-findings", action="store_true")
    return parser


def builder_command_args(
    args: argparse.Namespace,
    *,
    period: dict[str, str],
    plan_only: bool = False,
    director: str | None = None,
) -> list[str]:
    cmd = [
        "--snapshot-date",
        period["snapshot_date"],
        "--powerpoint-mode",
        args.powerpoint_mode,
        "--deck-source",
        args.deck_source,
    ]
    if plan_only:
        cmd.append("--plan-only")
    if args.as_of_date:
        cmd.extend(["--as-of-date", args.as_of_date])
    if args.deck_date:
        cmd.extend(["--deck-date", period["deck_date"]])
    if args.workbook_root is not None:
        cmd.extend(["--workbook-root", str(args.workbook_root)])
    if args.snapshot_root is not None:
        cmd.extend(["--snapshot-root", str(args.snapshot_root)])
    if director:
        cmd.extend(["--director", director])
    if args.fallback_workbook_deck:
        cmd.append("--fallback-workbook-deck")
    if args.allow_generated_shell_fallback:
        cmd.append("--allow-generated-shell-fallback")
    if args.refresh_snapshots:
        cmd.append("--refresh-snapshots")
    if args.fail_fast:
        cmd.append("--fail-fast")
    if args.skip_excel_brief or args.unattended:
        cmd.append("--skip-excel-brief")
    if args.skip_powerpoint_review or args.unattended:
        cmd.append("--skip-powerpoint-review")
    return cmd


def resolve_runtime_period(args: argparse.Namespace) -> dict[str, str]:
    period = resolve_period_context(
        as_of_date=getattr(args, "as_of_date", None),
        snapshot_date=getattr(args, "snapshot_date", None),
        deck_date=getattr(args, "deck_date", None),
    )
    return {
        "snapshot_date": period.snapshot_date,
        "deck_date": getattr(args, "deck_date", None) or period.deck_date,
        "reporting_month": period.reporting_month,
    }


def command_plan(args: argparse.Namespace) -> dict[str, Any]:
    period = resolve_runtime_period(args)
    cmd = builder_command_args(
        args,
        period=period,
        plan_only=True,
        director=args.director,
    )
    return run_builder(cmd)


def command_pilot(args: argparse.Namespace) -> dict[str, Any]:
    period = resolve_runtime_period(args)
    cmd = builder_command_args(
        args,
        period=period,
        director=args.director,
    )
    return run_builder(cmd)


def command_batch(args: argparse.Namespace) -> dict[str, Any]:
    period = resolve_runtime_period(args)
    cmd = builder_command_args(args, period=period)
    return run_builder(cmd)


def build_monthly_run_status_packet(payload: dict[str, Any]) -> dict[str, Any]:
    extraction = payload.get("extraction") or {}
    analysis = payload.get("analysis") or {}
    builder = payload.get("builder") or {}
    publish_gate = payload.get("publish_gate") or {}
    promotion = payload.get("promotion") or {}
    release_packet = payload.get("release_packet") or {}
    sharepoint_upload = payload.get("sharepoint_upload") or {}
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "command": "monthly-run",
        "snapshot_date": payload.get("snapshot_date"),
        "reporting_month": payload.get("reporting_month"),
        "status": payload.get("status"),
        "exit_code": payload.get("exit_code"),
        "unattended": bool(payload.get("unattended")),
        "extraction": extraction,
        "analysis": analysis,
        "builder_run_dir": builder.get("run_dir"),
        "builder_status": builder.get("status"),
        "builder_target_count": builder.get("target_count"),
        "builder_returncode": builder.get("builder_returncode", 0),
        "publish_gate": {
            "ok_count": publish_gate.get("ok_count", 0),
            "partial_count": publish_gate.get("partial_count", 0),
            "error_count": publish_gate.get("error_count", 0),
            "publish_blockers": publish_gate.get("publish_blockers", []),
        },
        "promotion": promotion,
        "release_packet": release_packet,
        "sharepoint_upload": sharepoint_upload,
        "payload": payload,
    }


def build_monthly_run_status_markdown(packet: dict[str, Any]) -> str:
    extraction = packet.get("extraction") or {}
    extraction_stages = extraction_stage_index(list(extraction.get("stages") or []))
    analysis = packet.get("analysis") or {}
    publish_gate = packet.get("publish_gate") or {}
    release_packet = packet.get("release_packet") or {}
    promotion = packet.get("promotion") or {}
    sharepoint_upload = packet.get("sharepoint_upload") or {}
    lines = [
        "# Sales Director Monthly Cadence Status",
        "",
        f"- Snapshot date: `{packet.get('snapshot_date')}`",
        f"- Reporting month: `{packet.get('reporting_month')}`",
        f"- Run status: `{packet.get('status')}`",
        f"- Exit code: `{packet.get('exit_code')}`",
        f"- Unattended: `{packet.get('unattended')}`",
        f"- Extraction status: `{extraction.get('status') or 'not-run'}`",
        f"- Workbook root: `{extraction.get('workbook_root') or '—'}`",
        f"- Builder status: `{packet.get('builder_status')}`",
        f"- Builder target count: `{packet.get('builder_target_count')}`",
        f"- Builder run dir: `{packet.get('builder_run_dir') or '—'}`",
        f"- Publish gate: `{publish_gate.get('ok_count', 0)} ok / {publish_gate.get('partial_count', 0)} partial / {publish_gate.get('error_count', 0)} error`",
        "",
        "## Extraction",
        "",
    ]
    if extraction.get("status") == "skipped":
        lines.append(f"- Skipped: {extraction.get('reason')}")
    else:
        source_preflight = (
            extraction_stages.get("0_source_contract_preflight", {}).get("summary")
            or {}
        )
        live_extract = (
            extraction_stages.get("1a_extract_salesforce", {}).get("summary") or {}
        )
        historical = (
            extraction_stages.get("1b_extract_historical_trending", {}).get("summary")
            or {}
        )
        workbook_contract = (
            extraction_stages.get("1b3_validate_director_workbook_contract", {}).get(
                "summary"
            )
            or {}
        )
        if source_preflight:
            lines.append(
                "- Source contract: "
                f"active=`{source_preflight.get('active_lane_status')}`, "
                f"candidate=`{source_preflight.get('candidate_lane_status')}`"
            )
        if live_extract:
            query_totals = live_extract.get("query_telemetry_totals") or {}
            lines.append(
                "- Live extract: "
                f"processed=`{live_extract.get('processed_count', 0)}`, "
                f"failures=`{live_extract.get('failure_count', 0)}`, "
                f"queries=`{query_totals.get('queries', 0)}`, "
                f"rows=`{query_totals.get('rows', 0)}`"
            )
        if historical:
            lines.append(
                "- Historical trending: "
                f"processed=`{historical.get('processed_count', 0)}`, "
                f"failures=`{historical.get('failure_count', 0)}`"
            )
        if workbook_contract:
            lines.append(
                "- Workbook contract: "
                f"validated=`{workbook_contract.get('validated_count', 0)}`, "
                f"failures=`{workbook_contract.get('failure_count', 0)}`, "
                f"warnings=`{workbook_contract.get('warning_count', 0)}`"
            )
        if extraction.get("failed_stage"):
            lines.append(f"- Failed stage: `{extraction.get('failed_stage')}`")
        if not extraction_stages:
            lines.append("- Not run.")

    lines.extend(
        [
            "",
            "## Analysis",
            "",
        ]
    )
    if analysis:
        lines.append(f"- Status: `{analysis.get('status')}`")
        lines.append(f"- SharePoint root: `{analysis.get('sharepoint_root') or '—'}`")
        contract = analysis.get("contract") or {}
        if contract:
            lines.append(
                "- Contract: "
                f"status=`{contract.get('status')}`, "
                f"validated=`{contract.get('validated_count', 0)}`, "
                f"failures=`{contract.get('failure_count', 0)}`, "
                f"warnings=`{contract.get('warning_count', 0)}`"
            )
        if analysis.get("generated_workbook_count") is not None:
            lines.append(
                f"- Generated workbooks: `{analysis.get('generated_workbook_count')}`"
            )
        if analysis.get("failed_stage"):
            lines.append(f"- Failed stage: `{analysis.get('failed_stage')}`")
    else:
        lines.append("- Not run.")

    lines.extend(
        [
            "",
            "## Publish Blockers",
            "",
        ]
    )
    blockers = publish_gate.get("publish_blockers") or []
    if blockers:
        for blocker in blockers:
            lines.append(
                f"- {blocker.get('director_name') or 'unknown'}: status={blocker.get('status')}, "
                f"bridge={blocker.get('bridge_status')}, powerpoint={blocker.get('powerpoint_status')}"
            )
    else:
        lines.append("- None.")

    lines.extend(
        [
            "",
            "## Promotion",
            "",
        ]
    )
    if promotion:
        lines.append(f"- Promoted count: `{promotion.get('promoted_count', 0)}`")
        if promotion.get("skipped_count") is not None:
            lines.append(f"- Skipped count: `{promotion.get('skipped_count')}`")
    else:
        lines.append("- Not run.")

    lines.extend(
        [
            "",
            "## Release Packet",
            "",
        ]
    )
    if release_packet:
        if release_packet.get("status") == "skipped":
            lines.append(f"- Skipped: {release_packet.get('reason')}")
        else:
            lines.append(f"- Publish ready: `{release_packet.get('publish_ready')}`")
            publish_assets = release_packet.get("publish_assets") or {}
            if publish_assets:
                lines.append(
                    f"- Publish assets: `{publish_assets.get('asset_count', 0)}`"
                )
            for blocker in release_packet.get("blockers") or []:
                lines.append(f"- {blocker}")
    else:
        lines.append("- Not run.")
    lines.extend(
        [
            "",
            "## SharePoint Upload",
            "",
        ]
    )
    if sharepoint_upload:
        lines.append(f"- Status: `{sharepoint_upload.get('status')}`")
        if sharepoint_upload.get("status") == "skipped":
            lines.append(f"- Reason: {sharepoint_upload.get('reason')}")
        else:
            lines.append(f"- Folder: `{sharepoint_upload.get('folder') or '—'}`")
            lines.append(f"- Uploaded: `{sharepoint_upload.get('uploaded_count', 0)}`")
            lines.append(f"- Skipped: `{sharepoint_upload.get('skipped_count', 0)}`")
    else:
        lines.append("- Not run.")
    return "\n".join(lines) + "\n"


def write_monthly_run_status_bundle(
    *,
    output_root: Path,
    packet: dict[str, Any],
) -> Path:
    snapshot_date = str(packet["snapshot_date"])
    run_dir = output_root / snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)
    markdown = build_monthly_run_status_markdown(packet)
    save_json(run_dir / "monthly-run-status.json", packet)
    save_text(run_dir / "monthly-run-status.md", markdown)

    latest_payload = {**packet, "packet_dir": str(run_dir)}
    save_json(output_root / snapshot_date / "latest.json", latest_payload)
    save_text(output_root / snapshot_date / "latest.md", markdown)
    save_json(output_root / "latest.json", latest_payload)
    save_text(output_root / "latest.md", markdown)
    return run_dir


def maybe_write_monthly_run_status(
    args: argparse.Namespace,
    payload: dict[str, Any],
) -> dict[str, Any]:
    output_root = getattr(args, "output_root", None)
    if output_root is None:
        return payload
    packet = build_monthly_run_status_packet(payload)
    packet_dir = write_monthly_run_status_bundle(
        output_root=Path(output_root),
        packet=packet,
    )
    payload["cadence_packet_dir"] = str(packet_dir)
    return payload


def command_monthly_run(args: argparse.Namespace) -> dict[str, Any]:
    period = resolve_runtime_period(args)
    extraction = run_extraction_chain(
        snapshot_date=period["snapshot_date"],
        workbook_root=args.workbook_root,
        skip_extract=bool(getattr(args, "skip_extract", False)),
    )
    result: dict[str, Any] = {
        "status": "running",
        "snapshot_date": period["snapshot_date"],
        "reporting_month": period["reporting_month"],
        "unattended": bool(args.unattended),
        "extraction": extraction,
    }
    if extraction.get("status") == "blocked":
        result["status"] = "blocked"
        result["exit_code"] = 2
        return maybe_write_monthly_run_status(args, result)

    sharepoint_upload_enabled = bool(getattr(args, "sharepoint_upload", False))
    sharepoint_root = Path(getattr(args, "sharepoint_root", DEFAULT_SHAREPOINT_ROOT))

    if sharepoint_upload_enabled:
        workbook_dir_value = extraction.get("workbook_dir")
        if not workbook_dir_value:
            result["analysis"] = {
                "status": "blocked",
                "reason": "missing_workbook_dir_from_extraction",
            }
            result["status"] = "blocked"
            result["exit_code"] = 2
            return maybe_write_monthly_run_status(args, result)
        analysis = run_sharepoint_analysis_chain(
            snapshot_date=period["snapshot_date"],
            workbook_dir=Path(str(workbook_dir_value)),
            sharepoint_root=sharepoint_root,
        )
        result["analysis"] = analysis
        if analysis.get("status") != "ok":
            result["status"] = "blocked"
            result["exit_code"] = 2
            return maybe_write_monthly_run_status(args, result)
    else:
        result["analysis"] = {
            "status": "skipped",
            "reason": "sharepoint_upload=false",
            "sharepoint_root": str(sharepoint_root),
        }

    builder_args = argparse.Namespace(**vars(args))
    if extraction.get("workbook_root"):
        builder_args.workbook_root = Path(str(extraction["workbook_root"]))
    builder = command_batch(builder_args)
    result["builder"] = builder

    builder_returncode = int(builder.get("builder_returncode", 0) or 0)
    run_dir_value = builder.get("run_dir")
    if builder_returncode != 0 or not run_dir_value or builder.get("status") == "error":
        result["status"] = "error"
        result["exit_code"] = builder_returncode or 2
        return maybe_write_monthly_run_status(args, result)

    run_dir = Path(run_dir_value)
    publish_gate = command_publish_gate(
        argparse.Namespace(manifest=str(run_dir / "manifest.json"))
    )
    result["publish_gate"] = publish_gate

    has_publish_blockers = bool(
        publish_gate["partial_count"]
        or publish_gate["error_count"]
        or publish_gate["publish_blockers"]
    )
    if has_publish_blockers:
        result["status"] = "blocked"
        result["exit_code"] = 3
        return maybe_write_monthly_run_status(args, result)

    promotion = run_promotion(
        run_dir=run_dir,
        allow_audit_findings=args.allow_audit_findings,
    )
    result["promotion"] = promotion
    promotion_returncode = int(promotion.get("builder_returncode", 0) or 0)
    expected_promotions = int(builder.get("target_count") or 0)
    promoted_count = int(promotion.get("promoted_count") or 0)
    if promotion_returncode != 0:
        result["status"] = "error"
        result["exit_code"] = 4
        return maybe_write_monthly_run_status(args, result)
    if promoted_count != expected_promotions:
        result["status"] = "blocked"
        result["exit_code"] = 4
        return maybe_write_monthly_run_status(args, result)

    if args.build_release_packet:
        global_run_dir = args.global_run_dir
        global_canonical_run_dir = args.global_canonical_run_dir
        if global_run_dir is None or global_canonical_run_dir is None:
            regional_builds: list[dict[str, Any]] = []
            for region_name in MONTHLY_REGION_NAMES:
                region_build = run_region_monthly_builder(
                    snapshot_date=period["snapshot_date"],
                    region_name=region_name,
                )
                regional_builds.append(region_build)
                region_returncode = int(region_build.get("builder_returncode", 0) or 0)
                if region_returncode != 0 or region_build.get("status") == "error":
                    result["regional_builds"] = regional_builds
                    result["status"] = "error"
                    result["exit_code"] = 5
                    return maybe_write_monthly_run_status(args, result)
            result["regional_builds"] = regional_builds

            global_summary = run_global_summary_builder(
                snapshot_date=period["snapshot_date"]
            )
            result["global_summary"] = global_summary
            global_summary_returncode = int(
                global_summary.get("builder_returncode", 0) or 0
            )
            global_summary_run_dir_value = global_summary.get("run_dir")
            global_summary_deck_path_value = (
                global_summary.get("deterministic_preview") or {}
            ).get("deck_path")
            if (
                global_summary_returncode != 0
                or global_summary.get("status") == "error"
                or not global_summary_run_dir_value
                or not global_summary_deck_path_value
            ):
                result["status"] = "error"
                result["exit_code"] = 5
                return maybe_write_monthly_run_status(args, result)
            global_run_dir = Path(str(global_summary_run_dir_value))

            global_canonical_shell = run_global_canonical_shell_builder(
                snapshot_date=period["snapshot_date"],
                baseline_deck_path=Path(str(global_summary_deck_path_value)),
            )
            result["global_canonical_shell"] = global_canonical_shell
            global_canonical_returncode = int(
                global_canonical_shell.get("builder_returncode", 0) or 0
            )
            global_canonical_run_dir_value = global_canonical_shell.get("run_dir")
            if (
                global_canonical_returncode != 0
                or global_canonical_shell.get("status") == "error"
                or not global_canonical_run_dir_value
            ):
                result["status"] = "error"
                result["exit_code"] = 5
                return maybe_write_monthly_run_status(args, result)
            global_canonical_run_dir = Path(str(global_canonical_run_dir_value))

        release_packet = run_release_packet(
            snapshot_date=period["snapshot_date"],
            director_run_dir=run_dir,
            global_run_dir=global_run_dir,
            global_canonical_run_dir=global_canonical_run_dir,
            sharepoint_root=sharepoint_root,
        )
        result["release_packet"] = release_packet
        release_returncode = int(release_packet.get("builder_returncode", 0) or 0)
        if release_returncode != 0:
            result["status"] = "error"
            result["exit_code"] = 5
            return maybe_write_monthly_run_status(args, result)
        if not release_packet.get("publish_ready", False):
            result["status"] = "blocked"
            result["exit_code"] = 5
            return maybe_write_monthly_run_status(args, result)
        if sharepoint_upload_enabled:
            release_dir_value = release_packet.get("release_dir")
            if not release_dir_value:
                result["sharepoint_upload"] = {
                    "status": "blocked",
                    "reason": "release_packet_missing_release_dir",
                }
                result["status"] = "blocked"
                result["exit_code"] = 6
                return maybe_write_monthly_run_status(args, result)
            sharepoint_upload = run_sharepoint_upload(
                release_dir=Path(str(release_dir_value)),
            )
            result["sharepoint_upload"] = sharepoint_upload
            sharepoint_returncode = int(
                sharepoint_upload.get("builder_returncode", 0) or 0
            )
            if sharepoint_returncode != 0 or sharepoint_upload.get("status") in {
                "blocked",
                "error",
            }:
                result["status"] = (
                    "blocked"
                    if sharepoint_upload.get("status") == "blocked"
                    else "error"
                )
                result["exit_code"] = 6
                return maybe_write_monthly_run_status(args, result)
        else:
            result["sharepoint_upload"] = {
                "status": "skipped",
                "reason": "sharepoint_upload=false",
            }
    else:
        result["release_packet"] = {
            "status": "skipped",
            "reason": "build_release_packet=false",
        }
        result["sharepoint_upload"] = {
            "status": "skipped",
            "reason": "build_release_packet=false",
        }

    result["status"] = "ok"
    result["exit_code"] = 0
    return maybe_write_monthly_run_status(args, result)


def command_publish_gate(args: argparse.Namespace) -> dict[str, Any]:
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    summary = {
        "snapshot_date": manifest.get("snapshot_date"),
        "run_dir": manifest.get("run_dir"),
        "ok_count": 0,
        "partial_count": 0,
        "error_count": 0,
        "publish_blockers": [],
    }
    for target in manifest.get("targets", []):
        status = target.get("status")
        if status == "ok":
            summary["ok_count"] += 1
        elif status == "partial":
            summary["partial_count"] += 1
        else:
            summary["error_count"] += 1
        powerpoint = (target.get("stages") or {}).get("powerpoint_review") or {}
        bridge = (target.get("stages") or {}).get("validated_bridge") or {}
        if status != "ok":
            summary["publish_blockers"].append(
                {
                    "director_name": target.get("director_name"),
                    "status": status,
                    "bridge_status": bridge.get("status"),
                    "powerpoint_status": powerpoint.get("status"),
                }
            )
    return summary


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser(
        "plan", parents=[shared_args(include_director=True)]
    )
    plan_parser.set_defaults(func=command_plan)

    pilot_parser = subparsers.add_parser(
        "pilot", parents=[shared_args(include_director=False)]
    )
    pilot_parser.add_argument("--director", required=True)
    pilot_parser.set_defaults(func=command_pilot)

    batch_parser = subparsers.add_parser(
        "batch", parents=[shared_args(include_director=False)]
    )
    batch_parser.set_defaults(func=command_batch)

    monthly_run_parser = subparsers.add_parser(
        "monthly-run",
        parents=[shared_args(include_director=False)],
    )
    monthly_run_parser.add_argument(
        "--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT
    )
    monthly_run_parser.set_defaults(
        func=command_monthly_run,
        build_release_packet=True,
        sharepoint_upload=True,
    )

    gate_parser = subparsers.add_parser("publish-gate")
    gate_parser.add_argument("--manifest", required=True)
    gate_parser.set_defaults(func=command_publish_gate)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    payload = args.func(args)
    print(json.dumps(payload, indent=2))
    return int(payload.get("exit_code", payload.get("builder_returncode", 0)))


if __name__ == "__main__":
    raise SystemExit(main())
