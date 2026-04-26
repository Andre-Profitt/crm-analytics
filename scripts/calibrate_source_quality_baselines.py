#!/usr/bin/env python3
"""Track C — source-quality baseline calibrator.

Reads one or more ``source_extract_quality_audit.json`` files (e.g. the v20c
review artifact, or recent approved monthly runs), computes baseline
candidates per ``(requirement_id, territory, period_role)`` tuple, and either:

* prints a comparison report against any existing baselines (default), or
* writes promoted baselines to ``config/source_quality_baselines/`` when the
  caller passes ``--promote-baselines``.

Read-only is the default. ``--promote-baselines`` is the only path that mutates
``config/source_quality_baselines/``.

Usage::

    # Dry-run (default): inspect what would be promoted from v20c.
    python3 scripts/calibrate_source_quality_baselines.py

    # Promote the v20c-derived baselines into the repo.
    python3 scripts/calibrate_source_quality_baselines.py --promote-baselines

    # Calibrate from multiple approved runs.
    python3 scripts/calibrate_source_quality_baselines.py \\
        --evidence docs/review-artifacts/v20c/source_extract_quality_audit.json \\
        --evidence output/.../source_extract_quality_audit.json \\
        --promote-baselines
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.source_quality_baselines import (  # noqa: E402
    SourceQualityBaseline,
    collect_observations,
    derive_baseline,
    load_baselines,
    write_baseline,
)


DEFAULT_BASELINES_DIR = REPO_ROOT / "config" / "source_quality_baselines"
DEFAULT_EVIDENCE = (
    REPO_ROOT
    / "docs"
    / "review-artifacts"
    / "v20c"
    / "source_extract_quality_audit.json"
)


def _load_audit(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    if "sources" not in payload:
        raise ValueError(f"{path}: missing 'sources' key (not a quality audit?)")
    return payload


def _compare_baselines(
    candidate: SourceQualityBaseline,
    existing: SourceQualityBaseline | None,
) -> dict[str, Any]:
    if existing is None:
        return {"action": "new", "diff": {}}
    diff: dict[str, Any] = {}
    if candidate.row_count.median != existing.row_count.median:
        diff["row_count_median"] = {
            "before": existing.row_count.median,
            "after": candidate.row_count.median,
        }
    if candidate.row_count.expected_min != existing.row_count.expected_min:
        diff["row_count_expected_min"] = {
            "before": existing.row_count.expected_min,
            "after": candidate.row_count.expected_min,
        }
    if candidate.row_count.expected_max != existing.row_count.expected_max:
        diff["row_count_expected_max"] = {
            "before": existing.row_count.expected_max,
            "after": candidate.row_count.expected_max,
        }
    candidate_fields = set(candidate.null_rates.keys())
    existing_fields = set(existing.null_rates.keys())
    if candidate_fields != existing_fields:
        diff["null_rate_fields"] = {
            "added": sorted(candidate_fields - existing_fields),
            "removed": sorted(existing_fields - candidate_fields),
        }
    return {"action": "update" if diff else "noop", "diff": diff}


def calibrate(
    *,
    evidence_paths: list[Path],
    baselines_dir: Path,
    promote: bool,
    include_statuses: tuple[str, ...] = ("ok", "warning"),
) -> dict[str, Any]:
    audits = [_load_audit(p) for p in evidence_paths]
    grouped = collect_observations(audits, include_statuses=include_statuses)
    existing = load_baselines(baselines_dir)

    proposals: list[dict[str, Any]] = []
    for baseline_key, observations in sorted(grouped.items()):
        first = observations[0]
        candidate = derive_baseline(
            baseline_key=baseline_key,
            requirement_id=first["requirement_id"],
            territory=first.get("territory"),
            period_role=first["period_role"],
            observations=observations,
        )
        comparison = _compare_baselines(candidate, existing.get(baseline_key))
        proposals.append(
            {
                "baseline_key": baseline_key,
                "sample_count": len(observations),
                "row_count_median": candidate.row_count.median,
                "expected_range": [
                    candidate.row_count.expected_min,
                    candidate.row_count.expected_max,
                ],
                "null_rate_field_count": len(candidate.null_rates),
                "comparison": comparison,
                "_candidate": candidate,
            }
        )

    promoted: list[str] = []
    if promote:
        for proposal in proposals:
            candidate: SourceQualityBaseline = proposal["_candidate"]
            write_baseline(baselines_dir, candidate)
            promoted.append(candidate.baseline_key)

    return {
        "evidence_paths": [str(p) for p in evidence_paths],
        "baselines_dir": str(baselines_dir),
        "include_statuses": list(include_statuses),
        "promote": promote,
        "candidate_count": len(proposals),
        "promoted_count": len(promoted),
        "promoted_keys": promoted,
        "proposals": [
            {k: v for k, v in p.items() if k != "_candidate"} for p in proposals
        ],
    }


def _print_report(report: dict[str, Any]) -> None:
    print(
        f"Calibration: {report['candidate_count']} candidates from "
        f"{len(report['evidence_paths'])} audit(s); "
        f"baselines_dir={report['baselines_dir']}"
    )
    new_count = sum(
        1 for p in report["proposals"] if p["comparison"]["action"] == "new"
    )
    update_count = sum(
        1 for p in report["proposals"] if p["comparison"]["action"] == "update"
    )
    noop_count = sum(
        1 for p in report["proposals"] if p["comparison"]["action"] == "noop"
    )
    print(f"  new={new_count}  update={update_count}  noop={noop_count}")
    if report["promote"]:
        print(
            f"  PROMOTED {report['promoted_count']} baseline(s) -> {report['baselines_dir']}"
        )
    else:
        print("  (read-only — pass --promote-baselines to write)")
    for proposal in report["proposals"]:
        action = proposal["comparison"]["action"]
        marker = "+" if action == "new" else ("~" if action == "update" else " ")
        print(
            f"  {marker} {proposal['baseline_key']} "
            f"n={proposal['sample_count']} median={proposal['row_count_median']} "
            f"range={proposal['expected_range']} "
            f"null_fields={proposal['null_rate_field_count']}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Track C — source-quality baseline calibrator (read-only by default)."
    )
    parser.add_argument(
        "--evidence",
        action="append",
        type=Path,
        default=None,
        help=(
            "Path to a source_extract_quality_audit.json. May be repeated. "
            f"Defaults to {DEFAULT_EVIDENCE.relative_to(REPO_ROOT)}"
        ),
    )
    parser.add_argument(
        "--baselines-dir",
        type=Path,
        default=DEFAULT_BASELINES_DIR,
        help=f"Baseline directory (default: {DEFAULT_BASELINES_DIR.relative_to(REPO_ROOT)})",
    )
    parser.add_argument(
        "--promote-baselines",
        action="store_true",
        help="REQUIRED to write baselines. Default is read-only report.",
    )
    parser.add_argument(
        "--include-status",
        action="append",
        default=None,
        help=(
            "Quality status(es) to include when calibrating. May be repeated. "
            "Default: ok, warning. Blocked sources are excluded so failed "
            "extracts do not poison the baseline."
        ),
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Optional path to write the full proposal report as JSON.",
    )
    args = parser.parse_args(argv)

    evidence_paths = args.evidence or [DEFAULT_EVIDENCE]
    for path in evidence_paths:
        if not path.exists():
            print(f"ERROR: evidence not found: {path}", file=sys.stderr)
            return 2
    include_statuses = tuple(args.include_status or ("ok", "warning"))

    report = calibrate(
        evidence_paths=evidence_paths,
        baselines_dir=args.baselines_dir,
        promote=bool(args.promote_baselines),
        include_statuses=include_statuses,
    )
    _print_report(report)
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
