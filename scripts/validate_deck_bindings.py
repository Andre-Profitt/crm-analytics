#!/usr/bin/env python3
"""Track E — CLI for the deck binding resolver (E3).

Resolves every slide/table/takeaway/link on the active
director_monthly profile against a real director workbook + the
workbook contract. Emits ``deck_binding_report.json`` and
``deck_binding_report.md``.

Usage:
    python scripts/validate_deck_bindings.py \\
        --workbook /Users/test/Downloads/jesper-tyrer-2026-04-20.xlsx \\
        --report-out output/track_e/deck_binding_report.json \\
        --md-out output/track_e/deck_binding_report.md
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import deck_binding_resolver  # noqa: E402
from scripts.monthly_platform import deck_contract  # noqa: E402
from scripts.monthly_platform import director_workbook_contract  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Resolve deck contract bindings against a real director workbook."
    )
    parser.add_argument("--workbook", required=True)
    parser.add_argument("--deck-contract", default=None)
    parser.add_argument("--workbook-contract", default=None)
    parser.add_argument("--report-out", default=None)
    parser.add_argument("--md-out", default=None)
    parser.add_argument("--show-blockers", action="store_true")
    args = parser.parse_args(argv)

    deck = deck_contract.load(args.deck_contract)
    workbook = director_workbook_contract.load(args.workbook_contract)
    workbook_path = Path(args.workbook)
    if not workbook_path.exists():
        print(f"ERROR: workbook not found: {workbook_path}", file=sys.stderr)
        return 2

    report = deck_binding_resolver.resolve(
        workbook_path=workbook_path, deck=deck, workbook=workbook
    )

    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"report: {out}")
    if args.md_out:
        out = Path(args.md_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(deck_binding_resolver.render_markdown(report), encoding="utf-8")
        print(f"md: {out}")

    if args.show_blockers or report["status"] == "fail":
        for blk in report["blockers"]:
            print(f"BLOCKER: {blk}")

    print(
        f"deck_bindings: {report['status']} "
        f"({report['pass_count']}/{report['binding_count']} pass, "
        f"warn={report['warning_count']} fail={report['fail_count']})"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
