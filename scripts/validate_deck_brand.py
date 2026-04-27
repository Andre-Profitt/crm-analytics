#!/usr/bin/env python3
"""Track F / F4 — brand fingerprint CLI.

Verifies the SimCorp PowerPoint template referenced by
``config/deck_contract.yaml::brand`` against the contract's pinned
fingerprint: SHA-256, file size, slide_master count, required layouts,
theme color validity.

Usage:
    python3 scripts/validate_deck_brand.py
    python3 scripts/validate_deck_brand.py \\
        --contract config/deck_contract.yaml \\
        --report-out output/track_f/brand_fingerprint_report.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform import brand_contract  # noqa: E402
from scripts.monthly_platform import deck_contract  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate the SimCorp template fingerprint."
    )
    parser.add_argument("--contract", default=None)
    parser.add_argument("--report-out", default=None)
    parser.add_argument("--show-findings", action="store_true")
    args = parser.parse_args(argv)

    contract = deck_contract.load(args.contract)
    report = brand_contract.validate_brand(contract)

    if args.report_out:
        out = Path(args.report_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(report.as_dict(), indent=2) + "\n", encoding="utf-8")
        print(f"report: {out}")

    if args.show_findings or report.status == "fail":
        for f in report.findings:
            print(f"[{f.severity}] {f.code} {f.path}: {f.message}")

    sha_status = (
        "match" if report.template_sha256 == report.expected_sha256 else "MISMATCH"
    )
    layouts_required = (
        len(report.layouts_present) - len(report.layouts_missing)
        if report.layouts_present
        else 0
    )
    print(
        f"brand_fingerprint: {report.status} "
        f"(blockers={report.blocker_count} warnings={report.warning_count} "
        f"sha={sha_status} "
        f"layouts_missing={len(report.layouts_missing)})"
    )
    return 0 if report.status == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
