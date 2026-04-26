import sys
from pathlib import Path

from scripts import diff_deck_delivery_contract_snapshots as diff_script


def test_build_snapshot_diff_surfaces_slide_and_sidecar_metric_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "ok",
        "expected_director_count": 1,
        "validated_director_count": 1,
        "failures": [],
        "warnings": [],
        "directors": [
            {
                "slug": "jesper-tyrer",
                "deck_path": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.pptx",
                "sidecar_path": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.json",
                "slide_count": 18,
                "file_size_bytes": 100,
                "sidecar_metrics": {"open_land_deals": 8, "q1_land_lost": 14},
            }
        ],
        "exec_rollup": {"slide_count": 7, "file_size_bytes": 200},
    }
    current_payload = {
        "run_date": "2026-05-22",
        "status": "failed",
        "expected_director_count": 1,
        "validated_director_count": 1,
        "failures": [{"scope": "exec_rollup", "issue": "missing_sidecar", "message": "missing"}],
        "warnings": [],
        "directors": [
            {
                "slug": "jesper-tyrer",
                "deck_path": "output/simcorp_director_decks/2026-05-22/land-only/jesper-tyrer-LAND.pptx",
                "sidecar_path": "output/simcorp_director_decks/2026-05-22/land-only/jesper-tyrer-LAND.json",
                "slide_count": 17,
                "file_size_bytes": 120,
                "sidecar_metrics": {"open_land_deals": 7, "q1_land_lost": 14},
            }
        ],
        "exec_rollup": {"slide_count": 6, "file_size_bytes": 210},
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["deck_delivery"]["status_before"] == "ok"
    assert payload["deck_delivery"]["status_after"] == "failed"
    assert payload["deck_delivery"]["failure_count_before"] == 0
    assert payload["deck_delivery"]["failure_count_after"] == 1
    assert payload["deck_delivery"]["director_changes"] == [
        {
            "change": "modified",
            "slug": "jesper-tyrer",
            "changes": {
                "metadata": {
                    "deck_path": {
                        "before": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.pptx",
                        "after": "output/simcorp_director_decks/2026-05-22/land-only/jesper-tyrer-LAND.pptx",
                    },
                    "file_size_bytes": {"before": 100, "after": 120, "delta": 20.0},
                    "sidecar_path": {
                        "before": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.json",
                        "after": "output/simcorp_director_decks/2026-05-22/land-only/jesper-tyrer-LAND.json",
                    },
                    "slide_count": {"before": 18, "after": 17, "delta": -1.0},
                },
                "sidecar_metrics": {
                    "open_land_deals": {"before": 8, "after": 7, "delta": -1.0}
                },
            },
        }
    ]
    assert payload["deck_delivery"]["exec_rollup_changes"] == {
        "file_size_bytes": {"before": 200, "after": 210, "delta": 10.0},
        "slide_count": {"before": 7, "after": 6, "delta": -1.0},
    }


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    audit_root = tmp_path / "output" / "deck_delivery_contract"
    output_root = tmp_path / "output" / "deck_delivery_contract_snapshot_diff"
    current_dir = audit_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "deck_delivery_contract_audit.json").write_text(
        '{"run_date":"2026-04-22","status":"ok"}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "AUDIT_ROOT", audit_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_deck_delivery_contract_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = (
        output_root
        / "2026-04-22"
        / "deck_delivery_contract_snapshot_diff.json"
    )

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
