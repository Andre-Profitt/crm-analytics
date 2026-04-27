#!/usr/bin/env python3
"""Track E — CLI for the deck contract structural validator.

Usage:
    python scripts/validate_deck_contract.py
    python scripts/validate_deck_contract.py --contract config/deck_contract.yaml
    python scripts/validate_deck_contract.py --report-out output/track_e/deck_contract_report.json

Exit code 0 on pass (no blockers), 1 on fail. Warnings do not block.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running as a script from the repo root without an editable install.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import deck_contract  # noqa: E402
from scripts.monthly_platform import director_workbook_contract  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate the Track E deck contract.")
    parser.add_argument("--contract", default=None, help="Path to deck_contract.yaml")
    parser.add_argument(
        "--workbook-contract",
        default=None,
        help="Path to director_workbook_contract.yaml",
    )
    parser.add_argument(
        "--report-out", default=None, help="Write JSON report to this path"
    )
    parser.add_argument(
        "--show-findings", action="store_true", help="Print findings to stdout"
    )
    args = parser.parse_args(argv)

    deck = deck_contract.load(args.contract)
    workbook = director_workbook_contract.load(args.workbook_contract)
    findings, report = deck_contract.validate(deck, workbook_contract=workbook)

    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"report: {out}")

    if args.show_findings or report["status"] == "fail":
        for f in findings:
            print(f"[{f.severity}] {f.code} {f.path}: {f.message}")

    print(
        f"deck_contract: {report['status']} "
        f"(blockers={report['blocker_count']} warnings={report['warning_count']} "
        f"slides={report['director_monthly_slide_count']})"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
