#!/usr/bin/env python3
"""Track D — source-distribution baseline calibrator.

Mirrors ``scripts/calibrate_source_quality_baselines.py``: reads one or more
monthly extract runs, computes ``share_by_category`` per dimension declared
in each source's ``distribution_policy``, and either prints a comparison
report (default) or writes promoted seeds when ``--promote-baselines`` is
passed.

Read-only is the default. ``--promote-baselines`` is the only path that
writes to ``config/source_distribution_baselines/``.

Inputs
------
``--evidence-run`` points at a directory produced by
``scripts/extract_salesforce_sources.py`` (e.g.
``output/monthly_salesforce_sources/<snapshot_date>/<run_id>``). The
calibrator walks ``plans/source_requirement_plan.json`` to learn which
sources have a ``distribution_policy`` and pairs each with its raw extract
under ``raw/src-*.json``.

Multiple ``--evidence-run`` flags can be repeated; the calibrator merges
samples across runs the same way the C calibrator merges row-count
observations. Each sample contributes to ``sample_count`` and the
share-by-category averaging.

Promotion ledger
----------------
Every promotion appends one JSON line to
``config/source_distribution_baselines/promotions.jsonl`` with:

    {"promoted_at": "...", "run_ids": [...], "snapshot_dates": [...],
     "baseline_keys": [...], "dimension_count": N, "actor": "...",
     "evidence_paths": [...]}

The ledger is append-only; a stale or wrong promotion is corrected by
adding a new line, never by editing or removing prior lines.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.monthly_platform.contracts import utc_now_iso  # noqa: E402
from scripts.monthly_platform.source_distribution_audit import (  # noqa: E402
    DimensionSeed,
    SourceDistributionSeed,
    _category_counts,
    baseline_key_for_item,
)
from scripts.monthly_platform.source_requirements import (  # noqa: E402
    SourcePlanItem,
    SourceRequirementPlan,
    SourceRequirementsRegistry,
    load_source_requirements,
)


DEFAULT_BASELINES_DIR = REPO_ROOT / "config" / "source_distribution_baselines"
DEFAULT_REQUIREMENTS_PATH = REPO_ROOT / "config" / "monthly_source_requirements.json"
PROMOTION_LEDGER = "promotions.jsonl"


def _overlay_distribution_policy(
    plan: SourceRequirementPlan,
    registry: SourceRequirementsRegistry,
) -> SourceRequirementPlan:
    """Re-attach current-contract ``distribution_policy`` to historical plan items.

    The v16-era plans pre-date the Track D opt-in, so their ``items[*].distribution_policy``
    is ``None``. The calibrator overlays the *current* contract's policy onto each
    historical plan item so the share-by-category math runs against the same dimensions
    operators are about to enable. Plan-item identity (source_id, territory, etc.) is
    preserved verbatim — only the policy is re-attached.
    """
    by_requirement = {req.requirement_id: req for req in registry.requirements}
    overlaid_items: list[SourcePlanItem] = []
    for item in plan.items:
        req = by_requirement.get(item.requirement_id)
        if req is None or req.distribution_policy is None:
            overlaid_items.append(item)
            continue
        overlaid_items.append(
            item.model_copy(update={"distribution_policy": req.distribution_policy})
        )
    return plan.model_copy(update={"items": overlaid_items})


def _load_plan(run_dir: Path) -> SourceRequirementPlan:
    plan_path = run_dir / "plans" / "source_requirement_plan.json"
    if not plan_path.exists():
        raise FileNotFoundError(f"missing source_requirement_plan.json under {run_dir}")
    payload = json.loads(plan_path.read_text(encoding="utf-8"))
    return SourceRequirementPlan.model_validate(payload)


def _find_extract_path(run_dir: Path, item: SourcePlanItem) -> Path | None:
    """Locate the raw extract JSON for ``item`` under ``run_dir/raw/``.

    Salesforce extract filenames embed source_id (lowercased), so we match on
    that suffix. Falls back to scanning the directory if the deterministic
    pattern misses (e.g. directory layout drift across runs).
    """
    raw_dir = run_dir / "raw"
    if not raw_dir.exists():
        return None
    needle = item.source_id.lower()
    candidates = sorted(raw_dir.glob(f"src-*{needle}*.json"))
    return candidates[0] if candidates else None


def _load_rows(extract_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(extract_path.read_text(encoding="utf-8"))
    rows = payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(
            f"{extract_path}: expected 'rows' list, got {type(rows).__name__}"
        )
    return rows


def _seed_from_samples(
    *,
    baseline_key: str,
    item: SourcePlanItem,
    samples: list[dict[str, Any]],  # one entry per evidence run
    promoted_at: str,
) -> SourceDistributionSeed:
    """Merge per-run dimension counts into one seed.

    ``samples`` is a list of run-level samples; each sample is a dict with:
    - ``run_id``, ``snapshot_date``, ``source_id``, ``row_count``
    - ``dim_counts``: ``{field_name: Counter[category]}``
    """
    if not samples:
        raise ValueError("at least one sample required")
    promoted_from = [
        {
            "run_id": s["run_id"],
            "snapshot_date": s["snapshot_date"],
            "row_count": s["row_count"],
            "source_id": s["source_id"],
        }
        for s in samples
    ]
    # Aggregate counts across samples per dimension. Total samples means total
    # rows seen for that dimension across all evidence runs.
    dimensions: dict[str, DimensionSeed] = {}
    policy = item.distribution_policy
    if policy is None:
        return SourceDistributionSeed(
            baseline_key=baseline_key,
            requirement_id=item.requirement_id,
            territory=item.territory or "global",
            period_role=item.period_role,
            promoted_at=promoted_at,
            promoted_from=promoted_from,
            dimensions={},
        )
    for dim in policy.dimensions:
        merged: dict[str, int] = defaultdict(int)
        sample_total = 0
        for s in samples:
            counts = s["dim_counts"].get(dim.field) or {}
            for category, count in counts.items():
                merged[str(category)] += int(count)
                sample_total += int(count)
        share_by_category = {
            category: round(count / sample_total, 6)
            for category, count in merged.items()
            if sample_total > 0
        }
        dimensions[dim.field] = DimensionSeed(
            field=dim.field,
            semantic_name=dim.semantic_name,
            sample_count=sample_total,
            share_by_category=share_by_category,
        )
    return SourceDistributionSeed(
        baseline_key=baseline_key,
        requirement_id=item.requirement_id,
        territory=item.territory or "global",
        period_role=item.period_role,
        promoted_at=promoted_at,
        promoted_from=promoted_from,
        dimensions=dimensions,
    )


def _collect_samples(
    run_dirs: list[Path],
    *,
    registry: SourceRequirementsRegistry,
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, SourcePlanItem]]:
    """Walk runs and collect per-baseline-key samples.

    Returns:
      ``samples_by_key``: {baseline_key: [sample_dict, ...]}
      ``items_by_key``:   {baseline_key: SourcePlanItem}  (last seen wins;
        items are equivalent across runs for the same baseline_key by
        construction.)
    """
    samples_by_key: dict[str, list[dict[str, Any]]] = defaultdict(list)
    items_by_key: dict[str, SourcePlanItem] = {}
    for run_dir in run_dirs:
        plan = _overlay_distribution_policy(_load_plan(run_dir), registry)
        for item in plan.items:
            if item.distribution_policy is None or item.status != "configured":
                continue
            extract_path = _find_extract_path(run_dir, item)
            if extract_path is None:
                continue
            try:
                rows = _load_rows(extract_path)
            except (ValueError, json.JSONDecodeError):
                continue
            payload = json.loads(extract_path.read_text(encoding="utf-8"))
            run_id = str(payload.get("metadata", {}).get("run_id") or run_dir.name)
            snapshot_date = str(payload.get("snapshot_date") or run_dir.parent.name)
            dim_counts: dict[str, Any] = {}
            for dim in item.distribution_policy.dimensions:
                dim_counts[dim.field] = _category_counts(rows, dim.field)
            key = baseline_key_for_item(item)
            samples_by_key[key].append(
                {
                    "run_id": run_id,
                    "snapshot_date": snapshot_date,
                    "source_id": item.source_id,
                    "row_count": len(rows),
                    "dim_counts": dim_counts,
                    "extract_path": str(extract_path),
                }
            )
            items_by_key[key] = item
    return samples_by_key, items_by_key


def calibrate(
    *,
    evidence_runs: list[Path],
    baselines_dir: Path,
    promote: bool,
    requirements_path: Path = DEFAULT_REQUIREMENTS_PATH,
    actor: str | None = None,
) -> dict[str, Any]:
    promoted_at = utc_now_iso()
    registry = load_source_requirements(requirements_path)
    samples_by_key, items_by_key = _collect_samples(evidence_runs, registry=registry)

    candidates: list[dict[str, Any]] = []
    for key in sorted(samples_by_key.keys()):
        item = items_by_key[key]
        seed = _seed_from_samples(
            baseline_key=key,
            item=item,
            samples=samples_by_key[key],
            promoted_at=promoted_at,
        )
        candidates.append(
            {
                "baseline_key": key,
                "requirement_id": item.requirement_id,
                "territory": item.territory or "global",
                "period_role": item.period_role,
                "sample_count": sum(s["row_count"] for s in samples_by_key[key]),
                "evidence_paths": [s["extract_path"] for s in samples_by_key[key]],
                "dimension_count": len(seed.dimensions),
                "_seed": seed,
            }
        )

    promoted: list[str] = []
    if promote:
        baselines_dir.mkdir(parents=True, exist_ok=True)
        for cand in candidates:
            seed: SourceDistributionSeed = cand["_seed"]
            path = baselines_dir / f"{seed.baseline_key}.json"
            payload = seed.model_dump(mode="json")
            path.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            promoted.append(seed.baseline_key)
        if promoted:
            ledger_path = baselines_dir / PROMOTION_LEDGER
            ledger_entry = {
                "promoted_at": promoted_at,
                "actor": actor or os.environ.get("USER") or "unknown",
                "evidence_runs": [str(p) for p in evidence_runs],
                "baseline_keys": promoted,
                "dimension_count": sum(c["dimension_count"] for c in candidates),
                "snapshot_dates": sorted(
                    {
                        s["snapshot_date"]
                        for samples in samples_by_key.values()
                        for s in samples
                    }
                ),
                "run_ids": sorted(
                    {
                        s["run_id"]
                        for samples in samples_by_key.values()
                        for s in samples
                    }
                ),
            }
            with ledger_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps(ledger_entry, sort_keys=True) + "\n")

    return {
        "evidence_runs": [str(p) for p in evidence_runs],
        "baselines_dir": str(baselines_dir),
        "promote": promote,
        "candidate_count": len(candidates),
        "promoted_count": len(promoted),
        "promoted_keys": promoted,
        "candidates": [
            {k: v for k, v in c.items() if k != "_seed"} for c in candidates
        ],
    }


def _print_report(report: dict[str, Any]) -> None:
    print(
        f"Calibration: {report['candidate_count']} candidate(s) from "
        f"{len(report['evidence_runs'])} run(s); "
        f"baselines_dir={report['baselines_dir']}"
    )
    if report["promote"]:
        print(
            f"  PROMOTED {report['promoted_count']} seed(s) -> {report['baselines_dir']}"
        )
    else:
        print("  (read-only — pass --promote-baselines to write)")
    for cand in report["candidates"]:
        print(
            f"  + {cand['baseline_key']} "
            f"rows={cand['sample_count']} dims={cand['dimension_count']}"
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Track D — source-distribution baseline calibrator (read-only by default)."
    )
    parser.add_argument(
        "--evidence-run",
        action="append",
        type=Path,
        required=True,
        help=(
            "Path to a monthly extract run directory "
            "(e.g. output/monthly_salesforce_sources/<snapshot>/<run_id>). "
            "May be repeated to merge samples across runs."
        ),
    )
    parser.add_argument(
        "--requirements",
        type=Path,
        default=DEFAULT_REQUIREMENTS_PATH,
        help=(
            "Compiled monthly_source_requirements.json (source-of-truth for "
            "distribution_policy). The calibrator overlays the current contract's "
            "policy onto historical plan items so old runs become re-callable when "
            "a new dimension is added. Default: "
            f"{DEFAULT_REQUIREMENTS_PATH.relative_to(REPO_ROOT)}"
        ),
    )
    parser.add_argument(
        "--baselines-dir",
        type=Path,
        default=DEFAULT_BASELINES_DIR,
        help=(
            "Distribution seeds directory "
            f"(default: {DEFAULT_BASELINES_DIR.relative_to(REPO_ROOT)})"
        ),
    )
    parser.add_argument(
        "--promote-baselines",
        action="store_true",
        help="REQUIRED to write seeds. Default is read-only report.",
    )
    parser.add_argument(
        "--actor",
        default=None,
        help="Override the actor recorded in the promotion ledger (default: $USER).",
    )
    parser.add_argument(
        "--report-json",
        type=Path,
        default=None,
        help="Optional path to write the full calibration report as JSON.",
    )
    args = parser.parse_args(argv)

    for run_dir in args.evidence_run:
        if not run_dir.exists() or not run_dir.is_dir():
            print(f"ERROR: evidence run not found: {run_dir}", file=sys.stderr)
            return 2

    report = calibrate(
        evidence_runs=args.evidence_run,
        baselines_dir=args.baselines_dir,
        promote=bool(args.promote_baselines),
        requirements_path=args.requirements,
        actor=args.actor,
    )
    _print_report(report)
    if args.report_json:
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
