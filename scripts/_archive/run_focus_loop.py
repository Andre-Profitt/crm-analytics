#!/usr/bin/env python3
"""Run a focused dashboard autopilot loop for a fixed duration.

This wraps `run_dashboard_autopilot.py` so we can keep iterating on one or a
small set of dashboards for hours without manually reissuing the same command.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
AUTOPILOT = REPO_ROOT / "scripts" / "run_dashboard_autopilot.py"
DEFAULT_QUEUE = REPO_ROOT / "config" / "dashboard_autopilot_queue.json"
ELITE_PROTOCOL = REPO_ROOT / "docs" / "ELITE_EXECUTION_PROTOCOL.md"
CONSULTANT_PLAYBOOK = REPO_ROOT / "docs" / "CONSULTANT_GRADE_CRMA_PLAYBOOK.md"
COMMERCIAL_RHYTHM = REPO_ROOT / "docs" / "COMMERCIAL_OPERATING_RHYTHM.md"
SALES_PROCESS_MODEL = REPO_ROOT / "docs" / "generated" / "SALES_PROCESS_OPERATING_MODEL_2026-03-11.md"
WIDGET_LIBRARY = REPO_ROOT / "docs" / "WIDGET_DECISION_LIBRARY.md"


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def run_command(command: list[str], *, cwd: Path = REPO_ROOT) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=cwd, capture_output=True, text=True)


def select_items(queue_config: dict[str, Any], keys: list[str]) -> list[dict[str, Any]]:
    wanted = set(keys)
    items = [item for item in queue_config["items"] if item.get("key") in wanted]
    missing = sorted(wanted - {item["key"] for item in items})
    if missing:
        raise SystemExit(f"Unknown queue keys: {', '.join(missing)}")
    return items


def make_loop_manifest(
    *,
    queue_path: Path,
    output_dir: Path,
    keys: list[str],
    hours: float,
    pause_seconds: int,
    session: str,
    label: str,
    dry_run: bool,
) -> dict[str, Any]:
    return {
        "queue": str(queue_path),
        "output_dir": str(output_dir),
        "keys": keys,
        "hours": hours,
        "pause_seconds": pause_seconds,
        "session": session,
        "label": label,
        "dry_run": dry_run,
        "started_at": datetime.now().isoformat(timespec="seconds"),
        "cycles": [],
    }


def build_research_brief(selected_items: list[dict[str, Any]]) -> str:
    lines = [
        "# Focus Loop Research Brief",
        "",
        "This run is research-first. Do not treat it as blind rebuild automation.",
        "",
        "## Required References",
        "",
        f"- `{ELITE_PROTOCOL}`",
        f"- `{CONSULTANT_PLAYBOOK}`",
        f"- `{COMMERCIAL_RHYTHM}`",
        f"- `{SALES_PROCESS_MODEL}`",
        f"- `{WIDGET_LIBRARY}`",
        "",
        "## Shared Preflight Questions",
        "",
        "1. What decision should this dashboard change for its primary persona?",
        "2. What operating cadence should the page support?",
        "3. Which metrics are trustworthy and which are still semantic-risky?",
        "4. Which widget forms are truly best for the questions on the page?",
        "5. What action queue or drill target should close the loop?",
        "",
        "## Selected Dashboard Focus",
        "",
    ]
    for item in selected_items:
        lines.extend(
            [
                f"### {item['key']}",
                f"- Personas: {', '.join(item.get('personas', []))}",
                f"- Domains: {', '.join(item.get('domains', []))}",
                f"- KPI focus: {', '.join(item.get('kpi_focus', []))}",
                f"- Notes: {item.get('notes', '')}",
                "- Elite checks:",
                "  - confirm persona fit",
                "  - confirm metric spine",
                "  - confirm widget choice",
                "  - confirm operating rhythm",
                "  - confirm action/drill path",
                "",
            ]
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queue",
        default=str(DEFAULT_QUEUE),
        help="Queue config JSON path",
    )
    parser.add_argument(
        "--keys",
        nargs="+",
        default=["forecast_revenue_motions"],
        help="Queue item keys to keep in the focus loop",
    )
    parser.add_argument(
        "--hours",
        type=float,
        default=8.0,
        help="How long to keep launching focus cycles",
    )
    parser.add_argument(
        "--pause-seconds",
        type=int,
        default=60,
        help="Pause between completed cycles",
    )
    parser.add_argument(
        "--session",
        default="default",
        help="Playwright session name to pass through to the autopilot runner",
    )
    parser.add_argument(
        "--label",
        default=None,
        help="Optional label for the run directory",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional explicit output directory for the loop root",
    )
    parser.add_argument(
        "--max-cycles",
        type=int,
        default=None,
        help="Optional hard cap on the number of cycles",
    )
    parser.add_argument(
        "--stop-on-failure",
        action="store_true",
        help="Stop the loop immediately if a cycle returns non-zero",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Resolve the focus loop and write the manifest without executing cycles",
    )
    args = parser.parse_args()

    queue_path = Path(args.queue).resolve()
    queue_config = json.loads(queue_path.read_text(encoding="utf-8"))
    selected_items = select_items(queue_config, args.keys)

    label = args.label or "_".join(args.keys)
    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    output_dir = (
        Path(args.output_dir).resolve()
        if args.output_dir
        else (REPO_ROOT / "output" / "focus_loops" / f"{timestamp}_{label}").resolve()
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = make_loop_manifest(
        queue_path=queue_path,
        output_dir=output_dir,
        keys=args.keys,
        hours=args.hours,
        pause_seconds=args.pause_seconds,
        session=args.session,
        label=label,
        dry_run=args.dry_run,
    )
    write_text(output_dir / "focus_loop_manifest.json", json.dumps(manifest, indent=2))

    loop_queue = dict(queue_config)
    loop_queue["default_playwright_session"] = args.session
    loop_queue["items"] = selected_items
    write_text(output_dir / "focus_queue.json", json.dumps(loop_queue, indent=2))
    write_text(output_dir / "RESEARCH_BRIEF.md", build_research_brief(selected_items))

    if args.dry_run:
        print(f"[focus-loop] dry_run output_dir={output_dir}")
        print(f"[focus-loop] keys={args.keys}")
        print(f"[focus-loop] hours={args.hours}")
        print(f"[focus-loop] session={args.session}")
        print(f"[focus-loop] research_brief={output_dir / 'RESEARCH_BRIEF.md'}")
        return 0

    deadline = time.monotonic() + (args.hours * 3600)
    cycle_index = 0

    while True:
        if args.max_cycles is not None and cycle_index >= args.max_cycles:
            break
        if time.monotonic() >= deadline:
            break

        cycle_index += 1
        cycle_name = f"cycle_{cycle_index:03d}"
        cycle_dir = output_dir / cycle_name
        cycle_dir.mkdir(parents=True, exist_ok=True)

        command = [
            sys.executable,
            str(AUTOPILOT),
            "--queue",
            str(output_dir / "focus_queue.json"),
            "--session",
            args.session,
            "--output-dir",
            str(cycle_dir),
        ]

        started_at = datetime.now().isoformat(timespec="seconds")
        print(f"[focus-loop] start {cycle_name} keys={args.keys}", flush=True)
        result = run_command(command)
        finished_at = datetime.now().isoformat(timespec="seconds")

        write_text(cycle_dir / "loop.stdout.log", result.stdout)
        write_text(cycle_dir / "loop.stderr.log", result.stderr)

        manifest["cycles"].append(
            {
                "cycle": cycle_name,
                "started_at": started_at,
                "finished_at": finished_at,
                "returncode": result.returncode,
                "command": command,
                "cycle_dir": str(cycle_dir),
            }
        )
        write_text(output_dir / "focus_loop_manifest.json", json.dumps(manifest, indent=2))

        print(
            f"[focus-loop] done {cycle_name} status={'ok' if result.returncode == 0 else 'failed'}",
            flush=True,
        )

        if result.returncode != 0 and args.stop_on_failure:
            break

        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        if args.pause_seconds > 0:
            time.sleep(min(args.pause_seconds, max(0, int(remaining))))

    summary_lines = [
        "# Focus Loop Summary",
        "",
        f"- Output dir: `{output_dir}`",
        f"- Keys: `{', '.join(args.keys)}`",
        f"- Hours: `{args.hours}`",
        f"- Session: `{args.session}`",
        f"- Research brief: `{output_dir / 'RESEARCH_BRIEF.md'}`",
        f"- Cycles completed: `{len(manifest['cycles'])}`",
        "",
        "## Cycles",
        "",
    ]
    for cycle in manifest["cycles"]:
        summary_lines.extend(
            [
                f"### {cycle['cycle']}",
                f"- Status: `{'ok' if cycle['returncode'] == 0 else 'failed'}`",
                f"- Started: `{cycle['started_at']}`",
                f"- Finished: `{cycle['finished_at']}`",
                f"- Dir: `{cycle['cycle_dir']}`",
                "",
            ]
        )
    write_text(output_dir / "README.md", "\n".join(summary_lines))

    print("[focus-loop] complete", flush=True)
    print(output_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
