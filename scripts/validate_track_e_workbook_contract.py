#!/usr/bin/env python3
"""Track E — CLI for the director workbook contract structural validator.

Validates ``config/director_workbook_contract.yaml`` against its JSON
Schema and runs cross-reference checks (snapshot_role sheet refs,
regex date-group presence). Does NOT touch a real .xlsx file — that
is the workbook validator's job (E2,
``scripts/validate_track_e_workbook.py``).

Named ``validate_track_e_workbook_contract.py`` rather than
``validate_director_workbook_contract.py`` because the latter is
already taken by the original cadence-pipeline workbook validator;
this script is the new Track E structural validator and must stay
distinct from that.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import director_workbook_contract  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the Track E director workbook contract."
    )
    parser.add_argument("--contract", default=None)
    parser.add_argument("--report-out", default=None)
    parser.add_argument("--show-findings", action="store_true")
    args = parser.parse_args(argv)

    contract = director_workbook_contract.load(args.contract)
    findings, report = director_workbook_contract.validate(contract)

    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(f"report: {out}")

    if args.show_findings or report["status"] == "fail":
        for f in findings:
            print(f"[{f['severity']}] {f['code']} {f.get('path', '')}: {f['message']}")

    print(
        f"director_workbook_contract: {report['status']} "
        f"(blockers={report['blocker_count']} warnings={report['warning_count']} "
        f"sheets={report['sheet_count']} roles={report['snapshot_role_count']})"
    )
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
