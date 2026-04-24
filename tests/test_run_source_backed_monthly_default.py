from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from scripts import run_source_backed_monthly_default as launcher


def test_build_launch_plan_defaults_run_id_and_manifest_path() -> None:
    args = launcher.parse_args(["--snapshot-date", "2026-04-30", "--plan-only"])

    launch = launcher.build_launch_plan(
        args,
        generated_at=datetime(2026, 5, 1, 9, 30, tzinfo=UTC),
    )

    assert launch.snapshot_date == "2026-04-30"
    assert launch.run_id == "source-backed-2026-04-30-20260501T093000Z"
    assert launch.manifest_path == (
        launcher.ROOT
        / "output"
        / "source_backed_monthly_pipeline_runs"
        / "2026-04-30"
        / "source-backed-2026-04-30-20260501T093000Z"
        / "pipeline_run_manifest.json"
    )
    command_text = " ".join(launch.command)
    assert "scripts/run_source_backed_monthly_pipeline.py" in command_text
    assert "--plan-only" in command_text
    assert "--snapshot-date 2026-04-30" in command_text


def test_print_command_does_not_execute(monkeypatch, capsys) -> None:
    def fail_run(*args: Any, **kwargs: Any) -> None:
        raise AssertionError("print-command must not execute subprocess")

    monkeypatch.setattr(launcher.subprocess, "run", fail_run)

    exit_code = launcher.main(
        [
            "--snapshot-date",
            "2026-04-30",
            "--run-id",
            "dry-run",
            "--print-command",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["status"] == "planned"
    assert payload["snapshot_date"] == "2026-04-30"
    assert payload["run_id"] == "dry-run"
