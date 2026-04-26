import sys
from pathlib import Path

from scripts import diff_deck_fill_payload_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_payload_set(
    tmp_path: Path, monkeypatch
) -> None:
    decks_root = tmp_path / "output" / "simcorp_director_decks"
    for run_date in ["2026-04-20", "2026-04-22", "2026-08-10"]:
        payload_dir = decks_root / run_date / "land-only"
        payload_dir.mkdir(parents=True, exist_ok=True)
        (payload_dir / "jesper-tyrer-LAND.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(diff_script, "DECKS_ROOT", decks_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-04-22"
    assert diff_script._resolve_baseline_date("2026-04-20") is None


def test_build_snapshot_diff_surfaces_director_and_exec_rollup_deltas() -> None:
    baseline_snapshot = {
        "run_date": "2026-04-20",
        "payload_dir": "output/simcorp_director_decks/2026-04-20/land-only",
        "directors": {
            "jesper-tyrer": {
                "payload_path": "output/simcorp_director_decks/2026-04-20/land-only/jesper-tyrer-LAND.json",
                "payload": {
                    "director": "Jesper Tyrer",
                    "open_land_deals": 6,
                    "open_land_arr": 5000000.0,
                    "q1_land_lost": 10,
                },
            }
        },
        "exec_rollup": {
            "payload_path": "output/simcorp_director_decks/2026-04-20/land-only/Exec Rollup.json",
            "payload": {
                "director_count": 9,
                "open_land_arr": 12000000.0,
                "by_director": {
                    "Jesper Tyrer": {
                        "open_land_deals": 6,
                        "open_land_arr": 5000000.0,
                    }
                },
            },
        },
    }
    current_snapshot = {
        "run_date": "2026-04-22",
        "payload_dir": "output/simcorp_director_decks/2026-04-22/land-only",
        "directors": {
            "jesper-tyrer": {
                "payload_path": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.json",
                "payload": {
                    "director": "Jesper Tyrer",
                    "open_land_deals": 8,
                    "open_land_arr": 9653343.94,
                    "q1_land_lost": 14,
                },
            },
            "sarah-pittroff": {
                "payload_path": "output/simcorp_director_decks/2026-04-22/land-only/sarah-pittroff-LAND.json",
                "payload": {
                    "director": "Sarah Pittroff",
                    "open_land_deals": 7,
                    "open_land_arr": 4278553.9,
                    "q1_land_lost": 8,
                },
            },
        },
        "exec_rollup": {
            "payload_path": "output/simcorp_director_decks/2026-04-22/land-only/Exec Rollup.json",
            "payload": {
                "director_count": 9,
                "open_land_arr": 13475789.61,
                "by_director": {
                    "Jesper Tyrer": {
                        "open_land_deals": 6,
                        "open_land_arr": 5036604.35,
                    },
                    "Sarah Pittroff": {
                        "open_land_deals": 7,
                        "open_land_arr": 4278553.9,
                    },
                },
            },
        },
    }

    payload = diff_script.build_snapshot_diff(baseline_snapshot, current_snapshot)

    assert payload["director_payloads"]["count_before"] == 1
    assert payload["director_payloads"]["count_after"] == 2
    assert payload["director_payloads"]["changes"] == [
        {
            "change": "modified",
            "slug": "jesper-tyrer",
            "payload_path_before": "output/simcorp_director_decks/2026-04-20/land-only/jesper-tyrer-LAND.json",
            "payload_path_after": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.json",
            "field_changes": {
                "open_land_arr": {
                    "before": 5000000.0,
                    "after": 9653343.94,
                    "delta": 4653343.94,
                },
                "open_land_deals": {
                    "before": 6,
                    "after": 8,
                    "delta": 2.0,
                },
                "q1_land_lost": {
                    "before": 10,
                    "after": 14,
                    "delta": 4.0,
                },
            },
        },
        {
            "change": "added",
            "slug": "sarah-pittroff",
            "payload_path_after": "output/simcorp_director_decks/2026-04-22/land-only/sarah-pittroff-LAND.json",
            "after": {
                "director": "Sarah Pittroff",
                "open_land_deals": 7,
                "open_land_arr": 4278553.9,
                "q1_land_lost": 8,
            },
        },
    ]
    assert payload["exec_rollup"] == {
        "change": "modified",
        "payload_path_before": "output/simcorp_director_decks/2026-04-20/land-only/Exec Rollup.json",
        "payload_path_after": "output/simcorp_director_decks/2026-04-22/land-only/Exec Rollup.json",
        "scalar_changes": {
            "open_land_arr": {
                "before": 12000000.0,
                "after": 13475789.61,
                "delta": 1475789.61,
            }
        },
        "by_director_changes": [
            {
                "change": "modified",
                "director": "Jesper Tyrer",
                "field_changes": {
                    "open_land_arr": {
                        "before": 5000000.0,
                        "after": 5036604.35,
                        "delta": 36604.35,
                    }
                },
            },
            {
                "change": "added",
                "director": "Sarah Pittroff",
                "after": {
                    "open_land_deals": 7,
                    "open_land_arr": 4278553.9,
                },
            },
        ],
    }


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    decks_root = tmp_path / "output" / "simcorp_director_decks"
    output_root = tmp_path / "output" / "deck_fill_payload_snapshot_diff"
    payload_dir = decks_root / "2026-04-22" / "land-only"
    payload_dir.mkdir(parents=True, exist_ok=True)
    (payload_dir / "jesper-tyrer-LAND.json").write_text(
        '{"director":"Jesper Tyrer","open_land_deals":8}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "DECKS_ROOT", decks_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_deck_fill_payload_snapshots.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "deck_fill_payload_snapshot_diff.json"

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
