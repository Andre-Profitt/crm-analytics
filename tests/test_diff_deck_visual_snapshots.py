import sys
from pathlib import Path

from PIL import Image

from scripts import diff_deck_visual_snapshots as diff_script


def test_resolve_baseline_date_picks_latest_prior_run(tmp_path: Path, monkeypatch) -> None:
    decks_root = tmp_path / "output" / "simcorp_director_decks"
    for run_date in ["2026-04-20", "2026-04-22", "2026-08-10"]:
        (decks_root / run_date / "land-only").mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(diff_script, "DECKS_ROOT", decks_root)

    assert diff_script._resolve_baseline_date("2026-08-10") == "2026-04-22"
    assert diff_script._resolve_baseline_date("2026-04-20") is None


def test_build_snapshot_diff_surfaces_modified_decks_and_slide_deltas(
    tmp_path: Path, monkeypatch
) -> None:
    monkeypatch.setattr(diff_script, "ROOT", tmp_path)
    before_image = tmp_path / "before.png"
    after_image = tmp_path / "after.png"
    before_montage = tmp_path / "before-montage.png"
    after_montage = tmp_path / "after-montage.png"
    Image.new("RGB", (4, 4), (10, 10, 10)).save(before_image)
    Image.new("RGB", (4, 4), (20, 20, 20)).save(after_image)
    Image.new("RGB", (8, 4), (255, 255, 255)).save(before_montage)
    Image.new("RGB", (8, 4), (240, 240, 240)).save(after_montage)

    baseline_snapshot = {
        "run_date": "2026-04-20",
        "deck_count": 1,
        "render_failures": [],
        "decks": {
            "jesper-tyrer-LAND": {
                "deck_path": "output/simcorp_director_decks/2026-04-20/land-only/jesper-tyrer-LAND.pptx",
                "montage_path": "before-montage.png",
                "slide_count": 1,
                "slides": {
                    "slide-1.png": {
                        "image_path": "before.png",
                        "sha256": "before",
                    }
                },
            }
        },
    }
    current_snapshot = {
        "run_date": "2026-04-22",
        "deck_count": 1,
        "render_failures": [],
        "decks": {
            "jesper-tyrer-LAND": {
                "deck_path": "output/simcorp_director_decks/2026-04-22/land-only/jesper-tyrer-LAND.pptx",
                "montage_path": "after-montage.png",
                "slide_count": 1,
                "slides": {
                    "slide-1.png": {
                        "image_path": "after.png",
                        "sha256": "after",
                    }
                },
            }
        },
    }

    payload = diff_script.build_snapshot_diff(
        baseline_snapshot,
        current_snapshot,
        tmp_path / "comparisons",
    )

    assert payload["status"] == "ok"
    assert payload["visual_diff"]["modified_decks"] == 1
    assert payload["visual_diff"]["changed_slides"] == 1
    deck_change = payload["visual_diff"]["deck_changes"][0]
    assert deck_change["deck"] == "jesper-tyrer-LAND"
    assert deck_change["slide_changes"] == [
        {
            "change": "modified",
            "slide": "slide-1.png",
            "before_path": "before.png",
            "after_path": "after.png",
            "mean_channel_delta": 10.0,
        }
    ]
    assert deck_change["comparison_montage_path"].endswith(
        "comparisons/jesper-tyrer-land/comparison_montage.png"
    )


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    decks_root = tmp_path / "output" / "simcorp_director_decks"
    output_root = tmp_path / "output" / "deck_visual_snapshot_diff"
    current_dir = decks_root / "2026-04-22" / "land-only"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "jesper-tyrer-LAND.pptx").write_text("", encoding="utf-8")

    monkeypatch.setattr(diff_script, "DECKS_ROOT", decks_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["diff_deck_visual_snapshots.py", "--current-date", "2026-04-22"],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "deck_visual_snapshot_diff.json"
    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
