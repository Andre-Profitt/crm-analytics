#!/usr/bin/env python3
"""Promote validated-baseline director decks from a clean batch run into canonical shells."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from run_sales_director_canonical_shell_builder import promote_canonical_shell
except ModuleNotFoundError:  # pragma: no cover
    from scripts.run_sales_director_canonical_shell_builder import promote_canonical_shell


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CANONICAL_ROOT = REPO_ROOT / "output" / "sales_director_canonical_shells"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def promote_from_batch(
    *,
    run_dir: Path,
    canonical_root: Path,
    require_clean_audit: bool = True,
) -> dict[str, Any]:
    manifest_path = run_dir / "manifest.json"
    manifest = load_json(manifest_path)
    snapshot_date = manifest["snapshot_date"]
    promoted: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for target in manifest.get("targets", []):
        stages = target.get("stages", {})
        preview = stages.get("deterministic_preview", {})
        audit = stages.get("deterministic_preview_audit", {})
        if target.get("status") != "ok" or preview.get("status") != "ok":
            skipped.append(
                {
                    "director_name": target.get("director_name"),
                    "territory": target.get("territory"),
                    "reason": "target_not_ok",
                }
            )
            continue
        if require_clean_audit and (not audit.get("ok") or audit.get("finding_count", 0) != 0):
            skipped.append(
                {
                    "director_name": target.get("director_name"),
                    "territory": target.get("territory"),
                    "reason": "audit_not_clean",
                    "finding_count": audit.get("finding_count"),
                }
            )
            continue
        result = promote_canonical_shell(
            working_deck_path=Path(preview["deck_path"]),
            canonical_root=canonical_root,
            director_name=target["director_name"],
            territory=target["territory"],
            snapshot_date=snapshot_date,
        )
        promoted.append(
            {
                "director_name": target["director_name"],
                "territory": target["territory"],
                "source_deck_path": preview["deck_path"],
                **result,
            }
        )

    out = {
        "snapshot_date": snapshot_date,
        "run_dir": str(run_dir),
        "canonical_root": str(canonical_root),
        "require_clean_audit": require_clean_audit,
        "promoted_count": len(promoted),
        "skipped_count": len(skipped),
        "promoted": promoted,
        "skipped": skipped,
    }
    (run_dir / "canonical-promotion-summary.json").write_text(
        json.dumps(out, indent=2, ensure_ascii=True),
        encoding="utf-8",
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--canonical-root", type=Path, default=DEFAULT_CANONICAL_ROOT)
    parser.add_argument(
        "--allow-audit-findings",
        action="store_true",
        help="Promote even when the deterministic preview audit is not clean.",
    )
    args = parser.parse_args()

    result = promote_from_batch(
        run_dir=args.run_dir,
        canonical_root=args.canonical_root,
        require_clean_audit=not args.allow_audit_findings,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
