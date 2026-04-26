#!/usr/bin/env python3
"""Run a durable build/export/review loop for the dashboard queue."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import textwrap
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
EXTRACTOR = REPO_ROOT / "scripts" / "extract_kpi_workbook.py"
EXPORTER = REPO_ROOT / "scripts" / "export_live_crma_assets.py"
PLAYWRIGHT = Path.home() / ".codex" / "skills" / "playwright" / "scripts" / "playwright_cli.sh"
URL_RE = re.compile(r"https://[^\s]+/analytics/dashboard/[A-Za-z0-9]+")


@dataclass
class CommandResult:
    command: list[str]
    returncode: int
    stdout: str
    stderr: str

    @property
    def ok(self) -> bool:
        return self.returncode == 0


@dataclass
class ItemRun:
    key: str
    notes: str
    kpi_focus: list[str]
    domains: list[str]
    personas: list[str]
    build: CommandResult | None = None
    export: CommandResult | None = None
    audit: CommandResult | None = None
    review: list[dict[str, Any]] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)


def run_command(
    command: list[str],
    *,
    cwd: Path = REPO_ROOT,
    timeout: int | None = None,
    attempts: int = 1,
) -> CommandResult:
    attempt_logs: list[str] = []
    last_result: CommandResult | None = None
    for attempt in range(1, attempts + 1):
        try:
            proc = subprocess.run(
                command,
                cwd=cwd,
                capture_output=True,
                text=True,
                timeout=timeout,
            )
            result = CommandResult(
                command=command,
                returncode=proc.returncode,
                stdout=proc.stdout,
                stderr=proc.stderr,
            )
        except subprocess.TimeoutExpired as exc:
            result = CommandResult(
                command=command,
                returncode=124,
                stdout=exc.stdout or "",
                stderr=(exc.stderr or "") + f"\nTimed out after {timeout} seconds.",
            )
        except OSError as exc:
            result = CommandResult(
                command=command,
                returncode=1,
                stdout="",
                stderr=str(exc),
            )

        if result.ok:
            if attempt_logs:
                prefix = "\n".join(attempt_logs) + "\n"
                result = CommandResult(
                    command=result.command,
                    returncode=result.returncode,
                    stdout=prefix + result.stdout,
                    stderr=result.stderr,
                )
            return result

        combined = f"{result.stdout}\n{result.stderr}".lower()
        transient = any(
            token in combined
            for token in (
                "connection reset by peer",
                "remote end closed connection",
                "remote disconnected",
                "temporarily unavailable",
                "temporary failure",
                "timed out",
                "timeout",
                "ssl",
                "urlopen error",
                "502",
                "503",
                "504",
                "429",
            )
        )
        last_result = result
        if attempt >= attempts or not transient:
            break
        attempt_logs.append(
            f"[retry] attempt {attempt}/{attempts} failed transiently for: {' '.join(command)}"
        )

    if last_result and attempt_logs:
        prefix = "\n".join(attempt_logs) + "\n"
        last_result = CommandResult(
            command=last_result.command,
            returncode=last_result.returncode,
            stdout=prefix + last_result.stdout,
            stderr=last_result.stderr,
        )
    return last_result if last_result else CommandResult(command=command, returncode=1, stdout="", stderr="Unknown command failure.")


def write_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(value, encoding="utf-8")


def persist_manifest(run_dir: Path, manifest: dict[str, Any]) -> None:
    write_text(run_dir / "manifest.json", json.dumps(manifest, indent=2))


def persist_item_status(item_dir: Path, item_run: ItemRun) -> None:
    payload = {
        "key": item_run.key,
        "notes": item_run.notes,
        "kpi_focus": item_run.kpi_focus,
        "domains": item_run.domains,
        "personas": item_run.personas,
        "urls": item_run.urls,
        "build_ok": item_run.build.ok if item_run.build else None,
        "export_ok": item_run.export.ok if item_run.export else None,
        "audit_ok": item_run.audit.ok if item_run.audit else None,
        "review_count": len(item_run.review),
    }
    write_text(item_dir / "status.json", json.dumps(payload, indent=2))


def parse_urls(*texts: str) -> list[str]:
    seen: list[str] = []
    for text in texts:
        for match in URL_RE.findall(text or ""):
            if match not in seen:
                seen.append(match)
    return seen


def run_playwright_review(url: str, session: str, out_dir: Path) -> dict[str, Any]:
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = re.sub(r"[^a-z0-9]+", "_", url.lower()).strip("_")[-80:]
    screenshot_path = out_dir / f"{safe_name}.png"
    text_path = out_dir / f"{safe_name}.txt"
    snapshot_path = out_dir / f"{safe_name}.snapshot.txt"

    review: dict[str, Any] = {"url": url}

    goto = run_command(["bash", str(PLAYWRIGHT), f"-s={session}", "goto", url], timeout=180)
    review["goto_ok"] = goto.ok
    write_text(out_dir / f"{safe_name}.goto.log", goto.stdout + ("\nSTDERR:\n" + goto.stderr if goto.stderr else ""))
    if not goto.ok:
        review["error"] = "goto_failed"
        return review

    resize = run_command(["bash", str(PLAYWRIGHT), f"-s={session}", "resize", "1600", "2200"], timeout=60)
    write_text(out_dir / f"{safe_name}.resize.log", resize.stdout + ("\nSTDERR:\n" + resize.stderr if resize.stderr else ""))

    shot = run_command(
        ["bash", str(PLAYWRIGHT), f"-s={session}", "screenshot"],
        timeout=180,
    )
    write_text(out_dir / f"{safe_name}.screenshot.log", shot.stdout + ("\nSTDERR:\n" + shot.stderr if shot.stderr else ""))
    match = re.search(r"\((\.playwright-cli/[^\)]+\.png)\)", shot.stdout)
    if match:
        source = (REPO_ROOT / match.group(1)).resolve()
        if source.exists():
            shutil.copy2(source, screenshot_path)
    review["screenshot_ok"] = shot.ok and screenshot_path.exists()
    review["screenshot"] = str(screenshot_path) if screenshot_path.exists() else ""

    body = run_command(
        ["bash", str(PLAYWRIGHT), f"-s={session}", "eval", "() => document.body.innerText"],
        timeout=180,
    )
    write_text(text_path, body.stdout + ("\nSTDERR:\n" + body.stderr if body.stderr else ""))
    review["body_text"] = str(text_path)

    snap = run_command(
        ["bash", str(PLAYWRIGHT), f"-s={session}", "snapshot"],
        timeout=180,
    )
    write_text(snapshot_path, snap.stdout + ("\nSTDERR:\n" + snap.stderr if snap.stderr else ""))
    review["snapshot"] = str(snapshot_path)

    return review


def write_item_summary(path: Path, item_run: ItemRun) -> None:
    lines = [
        f"# {item_run.key}",
        "",
        f"- Domains: {', '.join(item_run.domains)}",
        f"- Personas: {', '.join(item_run.personas)}",
        f"- KPI focus: {', '.join(item_run.kpi_focus)}",
        f"- Notes: {item_run.notes}",
        "",
    ]
    if item_run.build:
        lines.extend(
            [
                "## Build",
                "",
                f"- Status: {'ok' if item_run.build.ok else 'failed'}",
                f"- Command: `{' '.join(item_run.build.command)}`",
                "",
            ]
        )
    if item_run.urls:
        lines.extend(["## Dashboard URLs", ""])
        for url in item_run.urls:
            lines.append(f"- {url}")
        lines.append("")
    if item_run.export:
        lines.extend(
            [
                "## Export",
                "",
                f"- Status: {'ok' if item_run.export.ok else 'failed'}",
                f"- Command: `{' '.join(item_run.export.command)}`",
                "",
            ]
        )
    if item_run.audit:
        lines.extend(
            [
                "## Audit",
                "",
                f"- Status: {'ok' if item_run.audit.ok else 'failed'}",
                f"- Command: `{' '.join(item_run.audit.command)}`",
                "",
            ]
        )
    if item_run.review:
        lines.extend(["## Review Artifacts", ""])
        for artifact in item_run.review:
            lines.append(f"- URL: {artifact['url']}")
            if artifact.get("screenshot"):
                lines.append(f"  Screenshot: `{artifact['screenshot']}`")
            if artifact.get("body_text"):
                lines.append(f"  Body text: `{artifact['body_text']}`")
        lines.append("")
    path.write_text("\n".join(lines), encoding="utf-8")


def maybe_refresh_kpi_catalog(queue_config: dict[str, Any], run_dir: Path) -> CommandResult | None:
    workbook_path = queue_config.get("workbook_path")
    if not workbook_path:
        return None
    return run_command(
        [
            sys.executable,
            str(EXTRACTOR),
            workbook_path,
            "--output-dir",
            str(run_dir / "kpi_catalog"),
            "--docs-path",
            str(run_dir / "KPI_WORKBOOK_SUMMARY.md"),
        ],
        timeout=600,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--queue",
        default="config/dashboard_autopilot_queue.json",
        help="Queue config JSON path",
    )
    parser.add_argument(
        "--session",
        default=None,
        help="Playwright browser session to reuse for screenshot review",
    )
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Optional maximum number of queue items to process",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Override run output directory",
    )
    parser.add_argument(
        "--resume-run",
        default=None,
        help="Resume an existing run directory and skip completed items",
    )
    args = parser.parse_args()

    queue_path = (REPO_ROOT / args.queue).resolve() if not Path(args.queue).is_absolute() else Path(args.queue)
    queue_config = json.loads(queue_path.read_text(encoding="utf-8"))
    session = args.session or queue_config.get("default_playwright_session")

    timestamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    if args.resume_run:
        run_dir = Path(args.resume_run).resolve()
    else:
        run_dir = (
            Path(args.output_dir).resolve()
            if args.output_dir
            else (REPO_ROOT / "output" / "autopilot" / "runs" / timestamp).resolve()
        )
    run_dir.mkdir(parents=True, exist_ok=True)

    manifest_path = run_dir / "manifest.json"
    if args.resume_run and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["session"] = session
    else:
        manifest = {
            "queue": str(queue_path),
            "run_dir": str(run_dir),
            "started_at": timestamp,
            "session": session,
            "items": [],
        }
    persist_manifest(run_dir, manifest)
    print(f"[autopilot] run_dir={run_dir}", flush=True)

    catalog_refresh = maybe_refresh_kpi_catalog(queue_config, run_dir)
    if catalog_refresh is not None:
        write_text(run_dir / "kpi_catalog_refresh.log", catalog_refresh.stdout + ("\nSTDERR:\n" + catalog_refresh.stderr if catalog_refresh.stderr else ""))
        manifest["kpi_catalog_refresh"] = {
            "ok": catalog_refresh.ok,
            "command": catalog_refresh.command,
        }
        persist_manifest(run_dir, manifest)
        print(f"[autopilot] kpi_catalog_refresh={'ok' if catalog_refresh.ok else 'failed'}", flush=True)

    enabled_items = [item for item in queue_config["items"] if item.get("enabled", True)]
    enabled_items.sort(key=lambda item: item.get("priority", 999))
    if args.max_items is not None:
        enabled_items = enabled_items[: args.max_items]

    audit_required = {
        item["key"]: bool(item.get("audit_script"))
        for item in enabled_items
    }
    completed = {
        item["key"]
        for item in manifest.get("items", [])
        if item.get("build_ok")
        and item.get("export_ok")
        and ((not audit_required.get(item["key"])) or item.get("audit_ok"))
    }

    for item in enabled_items:
        if item["key"] in completed:
            print(f"[autopilot] skip_completed key={item['key']}", flush=True)
            continue

        item_dir = run_dir / item["key"]
        item_dir.mkdir(parents=True, exist_ok=True)
        print(f"[autopilot] start key={item['key']}", flush=True)

        item_run = ItemRun(
            key=item["key"],
            notes=item.get("notes", ""),
            kpi_focus=item.get("kpi_focus", []),
            domains=item.get("domains", []),
            personas=item.get("personas", []),
        )
        item_manifest = {
            "key": item_run.key,
            "build_ok": None,
            "export_ok": None,
            "audit_ok": None,
            "review_count": 0,
            "urls": [],
        }
        manifest["items"] = [entry for entry in manifest["items"] if entry.get("key") != item_run.key]
        manifest["items"].append(item_manifest)
        persist_manifest(run_dir, manifest)
        persist_item_status(item_dir, item_run)

        builder_path = (REPO_ROOT / item["builder"]).resolve()
        build = run_command(
            [sys.executable, str(builder_path)],
            timeout=item.get("build_timeout_sec", 7200),
            attempts=item.get("build_attempts", 2),
        )
        item_run.build = build
        write_text(item_dir / "build.stdout.log", build.stdout)
        write_text(item_dir / "build.stderr.log", build.stderr)
        item_run.urls = parse_urls(build.stdout, build.stderr)
        item_manifest["build_ok"] = build.ok
        item_manifest["urls"] = item_run.urls
        persist_item_status(item_dir, item_run)
        persist_manifest(run_dir, manifest)
        print(
            f"[autopilot] build key={item['key']} status={'ok' if build.ok else 'failed'} urls={len(item_run.urls)}",
            flush=True,
        )

        labels = item.get("dashboard_labels", [])
        if labels:
            export = run_command(
                [
                    sys.executable,
                    str(EXPORTER),
                    "--output-dir",
                    str(item_dir / "live_export"),
                    *labels,
                ],
                timeout=item.get("export_timeout_sec", 1800),
                attempts=item.get("export_attempts", 2),
            )
            item_run.export = export
            write_text(item_dir / "export.stdout.log", export.stdout)
            write_text(item_dir / "export.stderr.log", export.stderr)
            item_manifest["export_ok"] = export.ok
            persist_item_status(item_dir, item_run)
            persist_manifest(run_dir, manifest)
            print(
                f"[autopilot] export key={item['key']} status={'ok' if export.ok else 'failed'}",
                flush=True,
            )

        audit_script = item.get("audit_script")
        if audit_script and labels:
            audit = run_command(
                [
                    sys.executable,
                    str((REPO_ROOT / audit_script).resolve()),
                    "--live-export-dir",
                    str(item_dir / "live_export"),
                    "--output-dir",
                    str(item_dir / "audit"),
                ],
                timeout=item.get("audit_timeout_sec", 900),
                attempts=item.get("audit_attempts", 1),
            )
            item_run.audit = audit
            write_text(item_dir / "audit.stdout.log", audit.stdout)
            write_text(item_dir / "audit.stderr.log", audit.stderr)
            item_manifest["audit_ok"] = audit.ok
            persist_item_status(item_dir, item_run)
            persist_manifest(run_dir, manifest)
            print(
                f"[autopilot] audit key={item['key']} status={'ok' if audit.ok else 'failed'}",
                flush=True,
            )

        if session and item_run.urls:
            review_dir = item_dir / "review"
            for url in item_run.urls:
                print(f"[autopilot] review key={item['key']} url={url}", flush=True)
                item_run.review.append(run_playwright_review(url, session, review_dir))
                item_manifest["review_count"] = len(item_run.review)
                persist_item_status(item_dir, item_run)
                persist_manifest(run_dir, manifest)
            print(
                f"[autopilot] review_complete key={item['key']} count={len(item_run.review)}",
                flush=True,
            )

        write_item_summary(item_dir / "README.md", item_run)
        persist_item_status(item_dir, item_run)
        persist_manifest(run_dir, manifest)
        print(f"[autopilot] done key={item['key']}", flush=True)

    lines = [
        f"# Dashboard Autopilot Run ({timestamp})",
        "",
        f"- Queue: `{queue_path}`",
        f"- Run dir: `{run_dir}`",
        f"- Session: `{session}`",
        "",
        "## Items",
        "",
    ]
    for item in manifest["items"]:
        audit_status = "n/a"
        if item.get("audit_ok") is True:
            audit_status = "ok"
        elif item.get("audit_ok") is False:
            audit_status = "failed"
        lines.append(
            textwrap.dedent(
                f"""\
                ### {item['key']}
                - Build: {'ok' if item['build_ok'] else 'failed'}
                - Export: {'ok' if item['export_ok'] else 'failed'}
                - Audit: {audit_status}
                - Review artifacts: {item['review_count']}
                """
            ).strip()
        )
        if item["urls"]:
            for url in item["urls"]:
                lines.append(f"- URL: {url}")
        lines.append("")
    write_text(run_dir / "RUN_SUMMARY.md", "\n".join(lines))
    print("[autopilot] complete", flush=True)
    print(run_dir)


if __name__ == "__main__":
    main()
