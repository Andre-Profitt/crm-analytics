#!/usr/bin/env python3
"""Export a PowerPoint deck to PDF using Microsoft PowerPoint on macOS."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import os
import tempfile
from pathlib import Path


POWERPOINT_APP = Path("/Applications/Microsoft PowerPoint.app")
DEFAULT_TIMEOUT_SECONDS = 45


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Input .pptx path.")
    parser.add_argument("--output", required=True, help="Output .pdf path.")
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help=f"Maximum seconds to wait for the PowerPoint export before failing. Defaults to {DEFAULT_TIMEOUT_SECONDS}.",
    )
    parser.add_argument("--json", action="store_true", help="Print a JSON result payload.")
    return parser.parse_args()


def best_effort_cleanup() -> None:
    cleanup_script = """
tell application "Microsoft PowerPoint"
  try
    repeat with p in presentations
      close p saving no
    end repeat
  end try
end tell
""".strip()
    try:
        subprocess.run(
            ["osascript", "-e", cleanup_script],
            capture_output=True,
            text=True,
            timeout=10,
        )
    except Exception:
        pass


def build_export_applescript() -> str:
    return """
on run argv
  set inputPath to item 1 of argv
  set outputPath to item 2 of argv
  set outputFile to POSIX file outputPath
  tell application "Microsoft PowerPoint"
    activate
    open POSIX file inputPath
    delay 2
    set p to active presentation
    try
      save p in outputFile as save as PDF
      delay 2
      close p saving no
    on error errMsg number errNum
      try
        close p saving no
      end try
      error errMsg number errNum
    end try
  end tell
end run
""".strip()


def export_pdf(input_path: Path, output_path: Path, *, timeout_seconds: int) -> dict[str, str]:
    if not POWERPOINT_APP.exists():
        raise RuntimeError(f"Microsoft PowerPoint is not installed at {POWERPOINT_APP}.")
    if not input_path.exists():
        raise RuntimeError(f"Input deck not found: {input_path}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()
    applescript = build_export_applescript()
    best_effort_cleanup()
    fd, temp_name = tempfile.mkstemp(prefix="ppt-export-", suffix=".pdf", dir="/private/tmp")
    os.close(fd)
    temp_output_path = Path(temp_name)
    temp_output_path.unlink(missing_ok=True)

    try:
        try:
            proc = subprocess.run(
                ["osascript", "-", str(input_path), str(temp_output_path)],
                input=applescript,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            best_effort_cleanup()
            raise RuntimeError(f"PowerPoint PDF export timed out after {timeout_seconds}s.") from exc
        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip() or proc.stdout.strip() or "PowerPoint PDF export failed.")
        if not temp_output_path.exists():
            raise RuntimeError(f"PowerPoint reported success but no PDF was created at {temp_output_path}.")
        temp_output_path.replace(output_path)
    finally:
        temp_output_path.unlink(missing_ok=True)
    return {
        "status": "ok",
        "input_path": str(input_path),
        "output_path": str(output_path),
    }


def main() -> int:
    args = parse_args()
    try:
        payload = export_pdf(
            Path(args.input).resolve(),
            Path(args.output).resolve(),
            timeout_seconds=args.timeout_seconds,
        )
    except Exception as exc:  # pragma: no cover - exercised indirectly by the runner
        print(f"PowerPoint PDF export failed: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(payload, indent=2))
    else:
        print(payload["output_path"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
