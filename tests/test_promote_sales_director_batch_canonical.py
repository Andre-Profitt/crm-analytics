from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.promote_sales_director_batch_canonical as module


def test_promote_from_batch_promotes_clean_targets_only(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)
    clean_deck = tmp_path / "clean.pptx"
    clean_deck.write_bytes(b"clean")
    dirty_deck = tmp_path / "dirty.pptx"
    dirty_deck.write_bytes(b"dirty")
    manifest = {
        "snapshot_date": "2026-04-10",
        "targets": [
            {
                "director_name": "Jane Doe",
                "territory": "APAC",
                "status": "ok",
                "stages": {
                    "deterministic_preview": {"status": "ok", "deck_path": str(clean_deck)},
                    "deterministic_preview_audit": {"ok": True, "finding_count": 0},
                },
            },
            {
                "director_name": "John Roe",
                "territory": "EMEA",
                "status": "ok",
                "stages": {
                    "deterministic_preview": {"status": "ok", "deck_path": str(dirty_deck)},
                    "deterministic_preview_audit": {"ok": False, "finding_count": 2},
                },
            },
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = module.promote_from_batch(
        run_dir=run_dir,
        canonical_root=tmp_path / "canonical",
        require_clean_audit=True,
    )

    assert result["promoted_count"] == 1
    assert result["skipped_count"] == 1
    promoted = result["promoted"][0]
    assert promoted["director_name"] == "Jane Doe"
    assert Path(promoted["stable_path"]).exists()
    assert Path(promoted["stable_path"]).read_bytes() == b"clean"
    skipped = result["skipped"][0]
    assert skipped["reason"] == "audit_not_clean"


def test_promote_from_batch_can_allow_audit_findings(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    run_dir.mkdir(parents=True)
    deck = tmp_path / "deck.pptx"
    deck.write_bytes(b"deck")
    manifest = {
        "snapshot_date": "2026-04-10",
        "targets": [
            {
                "director_name": "John Roe",
                "territory": "EMEA",
                "status": "ok",
                "stages": {
                    "deterministic_preview": {"status": "ok", "deck_path": str(deck)},
                    "deterministic_preview_audit": {"ok": False, "finding_count": 2},
                },
            }
        ],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    result = module.promote_from_batch(
        run_dir=run_dir,
        canonical_root=tmp_path / "canonical",
        require_clean_audit=False,
    )

    assert result["promoted_count"] == 1
    assert result["skipped_count"] == 0
