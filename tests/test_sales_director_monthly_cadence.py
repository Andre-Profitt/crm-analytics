from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_sales_director_monthly_cadence import (
    build_monthly_run_status_markdown,
    build_monthly_run_status_packet,
    build_parser,
    command_batch,
    command_monthly_run,
    command_publish_gate,
    command_plan,
    parse_json_output,
    run_builder,
    write_monthly_run_status_bundle,
)


def monthly_run_args(**overrides) -> argparse.Namespace:
    payload = {
        "snapshot_date": "2026-04-30",
        "as_of_date": None,
        "deck_date": None,
        "workbook_root": None,
        "snapshot_root": None,
        "deck_source": "canonical-shell",
        "fallback_workbook_deck": False,
        "allow_generated_shell_fallback": False,
        "skip_excel_brief": False,
        "skip_powerpoint_review": False,
        "refresh_snapshots": False,
        "fail_fast": False,
        "skip_extract": False,
        "unattended": False,
        "powerpoint_mode": "audit",
        "build_release_packet": False,
        "sharepoint_upload": False,
        "sharepoint_root": Path("/tmp/sharepoint"),
        "global_run_dir": None,
        "global_canonical_run_dir": None,
        "allow_audit_findings": False,
        "output_root": None,
    }
    payload.update(overrides)
    return argparse.Namespace(**payload)


def extraction_ok_payload(workbook_root: Path | None = None) -> dict[str, object]:
    resolved_workbook_root = workbook_root or Path("/tmp/director_live_workbooks")
    return {
        "status": "ok",
        "snapshot_date": "2026-04-30",
        "workbook_root": str(resolved_workbook_root),
        "workbook_dir": str(resolved_workbook_root / "2026-04-30"),
        "failed_stage": None,
        "stages": [
            {
                "name": "0_source_contract_preflight",
                "status": "ok",
                "summary": {
                    "active_lane_status": "ok",
                    "candidate_lane_status": "ok",
                },
            },
            {
                "name": "1a_extract_salesforce",
                "status": "ok",
                "summary": {
                    "processed_count": 9,
                    "failure_count": 0,
                    "query_telemetry_totals": {"queries": 76, "rows": 24525},
                },
            },
            {
                "name": "1b_extract_historical_trending",
                "status": "ok",
                "summary": {"processed_count": 9, "failure_count": 0},
            },
            {
                "name": "1b3_validate_director_workbook_contract",
                "status": "ok",
                "summary": {
                    "validated_count": 9,
                    "failure_count": 0,
                    "warning_count": 0,
                },
            },
        ],
    }


def test_publish_gate_summarizes_non_ok_targets(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "snapshot_date": "2026-04-10",
                "run_dir": "/tmp/run",
                "targets": [
                    {
                        "director_name": "Jane Doe",
                        "status": "ok",
                        "stages": {
                            "validated_bridge": {"status": "ok"},
                            "powerpoint_review": {"status": "ok"},
                        },
                    },
                    {
                        "director_name": "John Doe",
                        "status": "partial",
                        "stages": {
                            "validated_bridge": {"status": "ok"},
                            "powerpoint_review": {"status": "error"},
                        },
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    class Args:
        manifest = str(manifest_path)

    payload = command_publish_gate(Args())
    assert payload["ok_count"] == 1
    assert payload["partial_count"] == 1
    assert payload["error_count"] == 0
    assert payload["publish_blockers"] == [
        {
            "director_name": "John Doe",
            "status": "partial",
            "bridge_status": "ok",
            "powerpoint_status": "error",
        }
    ]


def test_run_builder_returns_structured_error_payload(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    class Proc:
        returncode = 2
        stdout = json.dumps({"status": "error", "phase": "preflight"})
        stderr = ""
        args = ["python3", "builder"]

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.subprocess.run",
        lambda *args, **kwargs: Proc(),
    )

    payload = run_builder(["--plan-only"])
    assert payload["status"] == "error"
    assert payload["phase"] == "preflight"
    assert payload["builder_returncode"] == 2


def test_parse_json_output_handles_log_prefix() -> None:
    payload = parse_json_output(
        'Saved: /tmp/deck.pptx\n{\n  "status": "ok",\n  "run_dir": "/tmp/run"\n}\n'
    )
    assert payload == {"status": "ok", "run_dir": "/tmp/run"}


def test_command_batch_unattended_passes_noninteractive_flags(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, list[str]] = {}

    def fake_run_builder(cmd):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        return {"status": "ok"}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_builder",
        fake_run_builder,
    )

    command_batch(
        argparse.Namespace(
            snapshot_date="2026-04-30",
            as_of_date=None,
            deck_date=None,
            workbook_root=None,
            snapshot_root=None,
            deck_source="canonical-shell",
            fallback_workbook_deck=False,
            allow_generated_shell_fallback=False,
            skip_excel_brief=False,
            skip_powerpoint_review=False,
            refresh_snapshots=False,
            fail_fast=False,
            unattended=True,
            powerpoint_mode="audit",
        )
    )

    cmd = captured["cmd"]
    assert "--deck-source" in cmd
    assert "canonical-shell" in cmd
    assert "--skip-excel-brief" in cmd
    assert "--skip-powerpoint-review" in cmd


def test_command_batch_passes_explicit_workbook_and_snapshot_roots(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, list[str]] = {}

    def fake_run_builder(cmd):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        return {"status": "ok"}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_builder",
        fake_run_builder,
    )

    workbook_root = tmp_path / "director_live_workbooks"
    snapshot_root = tmp_path / "director_workbook_snapshots"

    command_batch(
        argparse.Namespace(
            snapshot_date="2026-04-30",
            as_of_date=None,
            deck_date=None,
            workbook_root=workbook_root,
            snapshot_root=snapshot_root,
            deck_source="canonical-shell",
            fallback_workbook_deck=False,
            allow_generated_shell_fallback=False,
            skip_excel_brief=False,
            skip_powerpoint_review=False,
            refresh_snapshots=False,
            fail_fast=False,
            unattended=False,
            powerpoint_mode="audit",
        )
    )

    cmd = captured["cmd"]
    assert "--workbook-root" in cmd
    assert str(workbook_root) in cmd
    assert "--snapshot-root" in cmd
    assert str(snapshot_root) in cmd


def test_command_plan_respects_explicit_shell_fallback_flags(
    monkeypatch,
) -> None:  # type: ignore[no-untyped-def]
    captured: dict[str, list[str]] = {}

    def fake_run_builder(cmd):  # type: ignore[no-untyped-def]
        captured["cmd"] = cmd
        return {"status": "ok"}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_builder",
        fake_run_builder,
    )

    command_plan(
        argparse.Namespace(
            snapshot_date="2026-04-30",
            as_of_date=None,
            deck_date=None,
            director="Jesper Tyrer",
            workbook_root=None,
            snapshot_root=None,
            deck_source="shell",
            fallback_workbook_deck=False,
            allow_generated_shell_fallback=True,
            skip_excel_brief=True,
            skip_powerpoint_review=False,
            refresh_snapshots=True,
            fail_fast=True,
            unattended=False,
            powerpoint_mode="audit",
            build_release_packet=False,
            global_run_dir=None,
            global_canonical_run_dir=None,
            allow_audit_findings=False,
        )
    )

    cmd = captured["cmd"]
    assert "--plan-only" in cmd
    assert "--director" in cmd and "Jesper Tyrer" in cmd
    assert "--deck-source" in cmd and "shell" in cmd
    assert "--allow-generated-shell-fallback" in cmd
    assert "--skip-excel-brief" in cmd
    assert "--refresh-snapshots" in cmd
    assert "--fail-fast" in cmd


def test_monthly_run_defaults_release_packet_on_and_allows_skip() -> None:
    parser = build_parser()

    monthly_run_args = parser.parse_args(
        ["monthly-run", "--snapshot-date", "2026-04-30"]
    )
    assert monthly_run_args.build_release_packet is True

    monthly_run_skip_args = parser.parse_args(
        ["monthly-run", "--snapshot-date", "2026-04-30", "--skip-release-packet"]
    )
    assert monthly_run_skip_args.build_release_packet is False

    batch_args = parser.parse_args(["batch", "--snapshot-date", "2026-04-30"])
    assert batch_args.build_release_packet is False


def test_command_monthly_run_blocks_on_extraction_before_builder(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: {
            "status": "blocked",
            "snapshot_date": "2026-04-30",
            "workbook_root": "/tmp/director_live_workbooks",
            "workbook_dir": "/tmp/director_live_workbooks/2026-04-30",
            "failed_stage": "0_source_contract_preflight",
            "stages": [],
        },
    )

    called: dict[str, bool] = {}

    def fake_command_batch(args):  # type: ignore[no-untyped-def]
        called["builder"] = True
        return {"status": "ok"}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        fake_command_batch,
    )

    payload = command_monthly_run(monthly_run_args())

    assert payload["status"] == "blocked"
    assert payload["exit_code"] == 2
    assert payload["extraction"]["failed_stage"] == "0_source_contract_preflight"
    assert "builder" not in called


def test_command_monthly_run_passes_extracted_workbook_root_to_builder(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    extracted_root = tmp_path / "director_live_workbooks"
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(extracted_root),
    )

    captured: dict[str, object] = {}

    def fake_command_batch(args):  # type: ignore[no-untyped-def]
        captured["workbook_root"] = args.workbook_root
        return {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 1,
            "run_dir": str(run_dir),
        }

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        fake_command_batch,
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 1,
            "partial_count": 0,
            "error_count": 0,
            "publish_blockers": [],
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        lambda **kwargs: {"promoted_count": 1, "skipped_count": 0},
    )

    payload = command_monthly_run(monthly_run_args())

    assert payload["status"] == "ok"
    assert captured["workbook_root"] == extracted_root


def test_command_monthly_run_blocks_before_promotion(
    monkeypatch, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        lambda args: {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 2,
            "run_dir": str(run_dir),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 1,
            "partial_count": 1,
            "error_count": 0,
            "publish_blockers": [{"director_name": "Jane Doe"}],
        },
    )

    called: dict[str, bool] = {}

    def fake_run_promotion(**kwargs):  # type: ignore[no-untyped-def]
        called["promotion"] = True
        return {"promoted_count": 2, "skipped_count": 0}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        fake_run_promotion,
    )

    payload = command_monthly_run(monthly_run_args())

    assert payload["status"] == "blocked"
    assert payload["exit_code"] == 3
    assert "promotion" not in called


def test_command_monthly_run_promotes_when_clean(monkeypatch, tmp_path: Path) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        lambda args: {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 2,
            "run_dir": str(run_dir),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 2,
            "partial_count": 0,
            "error_count": 0,
            "publish_blockers": [],
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        lambda **kwargs: {
            "promoted_count": 2,
            "skipped_count": 0,
        },
    )

    payload = command_monthly_run(monthly_run_args())

    assert payload["status"] == "ok"
    assert payload["exit_code"] == 0
    assert payload["promotion"]["promoted_count"] == 2
    assert payload["release_packet"]["status"] == "skipped"


def test_command_monthly_run_blocks_on_release_packet(
    monkeypatch, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        lambda args: {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 1,
            "run_dir": str(run_dir),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 1,
            "partial_count": 0,
            "error_count": 0,
            "publish_blockers": [],
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        lambda **kwargs: {
            "promoted_count": 1,
            "skipped_count": 0,
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_region_monthly_builder",
        lambda *, snapshot_date, region_name: {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "region_name": region_name,
            "run_dir": str(
                tmp_path / f"region-{region_name.lower().replace(' ', '-')}"
            ),
            "manifest_path": str(
                tmp_path
                / f"region-{region_name.lower().replace(' ', '-')}"
                / "manifest.json"
            ),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_global_summary_builder",
        lambda *, snapshot_date: {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "run_dir": str(tmp_path / "global-run"),
            "manifest_path": str(tmp_path / "global-run" / "manifest.json"),
            "deterministic_preview": {
                "deck_path": str(
                    tmp_path
                    / "global-run"
                    / "Sales Global Summary Validated Baseline.pptx"
                )
            },
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_global_canonical_shell_builder",
        lambda *, snapshot_date, baseline_deck_path: {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "baseline_deck_path": str(baseline_deck_path),
            "run_dir": str(tmp_path / "global-canonical-run"),
            "manifest_path": str(tmp_path / "global-canonical-run" / "manifest.json"),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_release_packet",
        lambda **kwargs: {
            "publish_ready": False,
        },
    )

    payload = command_monthly_run(monthly_run_args(build_release_packet=True))

    assert payload["status"] == "blocked"
    assert payload["exit_code"] == 5


def test_command_monthly_run_builds_region_and_global_release_inputs(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "director-run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        lambda args: {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 2,
            "run_dir": str(run_dir),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 2,
            "partial_count": 0,
            "error_count": 0,
            "publish_blockers": [],
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        lambda **kwargs: {
            "promoted_count": 2,
            "skipped_count": 0,
        },
    )

    region_calls: list[str] = []

    def fake_run_region_monthly_builder(*, snapshot_date: str, region_name: str):  # type: ignore[no-untyped-def]
        region_calls.append(region_name)
        return {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "region_name": region_name,
            "run_dir": str(
                tmp_path / f"region-{region_name.lower().replace(' ', '-')}"
            ),
            "manifest_path": str(
                tmp_path
                / f"region-{region_name.lower().replace(' ', '-')}"
                / "manifest.json"
            ),
        }

    global_run_dir = tmp_path / "global-run"
    global_run_dir.mkdir()
    global_canonical_run_dir = tmp_path / "global-canonical-run"
    global_canonical_run_dir.mkdir()

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_region_monthly_builder",
        fake_run_region_monthly_builder,
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_global_summary_builder",
        lambda *, snapshot_date: {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "run_dir": str(global_run_dir),
            "manifest_path": str(global_run_dir / "manifest.json"),
            "deterministic_preview": {
                "deck_path": str(
                    global_run_dir / "Sales Global Summary Validated Baseline.pptx"
                )
            },
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_global_canonical_shell_builder",
        lambda *, snapshot_date, baseline_deck_path: {
            "status": "ok",
            "snapshot_date": snapshot_date,
            "baseline_deck_path": str(baseline_deck_path),
            "run_dir": str(global_canonical_run_dir),
            "manifest_path": str(global_canonical_run_dir / "manifest.json"),
        },
    )

    release_packet_calls: list[dict[str, object]] = []

    def fake_run_release_packet(**kwargs):  # type: ignore[no-untyped-def]
        release_packet_calls.append(kwargs)
        return {"publish_ready": True}

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_release_packet",
        fake_run_release_packet,
    )

    # The cadence runner constructs a per-run SharePoint subdir from the
    # snapshot date and a wall-clock timestamp slug:
    #     <sharepoint-root> / <snapshot-date> / <timestamp-slug>
    # Pin the slug so the assertion is deterministic. Per-run isolation is
    # the contract; this test only verifies that the runner forwards the
    # *constructed* path, not that the slug is generated a particular way.
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.timestamp_slug",
        lambda: "20260426-120000",
    )

    payload = command_monthly_run(monthly_run_args(build_release_packet=True))

    assert payload["status"] == "ok"
    assert region_calls == ["APAC", "EMEA", "North America"]
    assert payload["global_summary"]["run_dir"] == str(global_run_dir)
    assert payload["global_canonical_shell"]["run_dir"] == str(global_canonical_run_dir)
    expected_sharepoint_root = (
        Path("/tmp/sharepoint") / "2026-04-30" / "20260426-120000"
    )
    assert release_packet_calls == [
        {
            "snapshot_date": "2026-04-30",
            "director_run_dir": run_dir,
            "global_run_dir": global_run_dir,
            "global_canonical_run_dir": global_canonical_run_dir,
            "sharepoint_root": expected_sharepoint_root,
        }
    ]


def test_command_monthly_run_writes_status_bundle_and_latest_aliases(
    monkeypatch,
    tmp_path: Path,
) -> None:  # type: ignore[no-untyped-def]
    run_dir = tmp_path / "builder-run"
    run_dir.mkdir()
    (run_dir / "manifest.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_extraction_chain",
        lambda **kwargs: extraction_ok_payload(tmp_path / "director_live_workbooks"),
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_batch",
        lambda args: {
            "status": "ok",
            "snapshot_date": "2026-04-30",
            "target_count": 2,
            "run_dir": str(run_dir),
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.command_publish_gate",
        lambda args: {
            "snapshot_date": "2026-04-30",
            "run_dir": str(run_dir),
            "ok_count": 2,
            "partial_count": 0,
            "error_count": 0,
            "publish_blockers": [],
        },
    )
    monkeypatch.setattr(
        "scripts.run_sales_director_monthly_cadence.run_promotion",
        lambda **kwargs: {
            "promoted_count": 2,
            "skipped_count": 0,
        },
    )

    payload = command_monthly_run(
        monthly_run_args(output_root=tmp_path / "cadence-status")
    )

    assert payload["status"] == "ok"
    assert payload["cadence_packet_dir"]

    latest_json = json.loads(
        (tmp_path / "cadence-status" / "2026-04-30" / "latest.json").read_text(
            encoding="utf-8"
        )
    )
    latest_md = (tmp_path / "cadence-status" / "latest.md").read_text(encoding="utf-8")

    assert latest_json["status"] == "ok"
    assert latest_json["builder_run_dir"] == str(run_dir)
    assert latest_json["publish_gate"]["ok_count"] == 2
    assert latest_json["packet_dir"] == payload["cadence_packet_dir"]
    assert "- Builder run dir: " in latest_md
    assert "build_release_packet=false" in latest_md


def test_build_monthly_run_status_packet_and_bundle(tmp_path: Path) -> None:
    payload = {
        "status": "blocked",
        "exit_code": 3,
        "snapshot_date": "2026-04-30",
        "reporting_month": "2026-04",
        "builder": {
            "status": "ok",
            "target_count": 2,
            "run_dir": str(tmp_path / "builder-run"),
        },
        "publish_gate": {
            "ok_count": 1,
            "partial_count": 1,
            "error_count": 0,
            "publish_blockers": [
                {
                    "director_name": "Jane Doe",
                    "status": "partial",
                    "bridge_status": "ok",
                    "powerpoint_status": "error",
                }
            ],
        },
        "release_packet": {"status": "skipped", "reason": "build_release_packet=false"},
    }

    packet = build_monthly_run_status_packet(payload)
    markdown = build_monthly_run_status_markdown(packet)
    packet_dir = write_monthly_run_status_bundle(
        output_root=tmp_path / "cadence-status",
        packet=packet,
    )

    assert packet["builder_run_dir"] == str(tmp_path / "builder-run")
    assert packet["publish_gate"]["partial_count"] == 1
    assert "Jane Doe: status=partial, bridge=ok, powerpoint=error" in markdown
    assert "Skipped: build_release_packet=false" in markdown
    assert json.loads(
        (tmp_path / "cadence-status" / "latest.json").read_text(encoding="utf-8")
    )["packet_dir"] == str(packet_dir)
