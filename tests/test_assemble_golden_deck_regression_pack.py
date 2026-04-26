import sys
from pathlib import Path

from PIL import Image

from scripts import assemble_golden_deck_regression_pack as pack_script


def test_assemble_pack_builds_curated_items(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(pack_script, "ROOT", tmp_path)
    monkeypatch.setattr(pack_script, "CONFIG_PATH", tmp_path / "config" / "golden_deck_regression_pack.json")
    monkeypatch.setattr(pack_script, "DECKS_ROOT", tmp_path / "output" / "simcorp_director_decks")
    monkeypatch.setattr(pack_script, "MASTER_BUILDER_ROOT", tmp_path / "output" / "sales_director_monthly_master_builder")

    config_path = pack_script.CONFIG_PATH
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        """
        {
          "roles": [
            {"role": "clean_book", "slug": "patrick-gaughan", "selection_basis": "clean"},
            {"role": "approval_heavy", "slug": "jesper-tyrer", "selection_basis": "approval"}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    for run_date, slug, director in [
        ("2026-04-20", "patrick-gaughan", "Patrick Gaughan"),
        ("2026-04-20", "jesper-tyrer", "Jesper Tyrer"),
        ("2026-04-22", "patrick-gaughan", "Patrick Gaughan"),
        ("2026-04-22", "jesper-tyrer", "Jesper Tyrer"),
    ]:
        sidecar = pack_script.DECKS_ROOT / run_date / "land-only" / f"{slug}-LAND.json"
        sidecar.parent.mkdir(parents=True, exist_ok=True)
        sidecar.write_text(
            (
                "{"
                f"\"director\":\"{director}\","
                "\"territory\":\"T\","
                "\"open_land_deals\":3,"
                "\"open_land_arr\":100.0,"
                "\"approved_2026\":1,"
                "\"conditionally_approved\":0,"
                "\"missing_stage3\":0"
                "}"
            ),
            encoding="utf-8",
        )

    comparison_one = tmp_path / "comparison-patrick.png"
    comparison_two = tmp_path / "comparison-jesper.png"
    Image.new("RGB", (20, 10), (255, 255, 255)).save(comparison_one)
    Image.new("RGB", (20, 10), (200, 200, 200)).save(comparison_two)

    visual_diff = {
        "status": "ok",
        "baseline_run_date": "2026-04-20",
        "current_run_date": "2026-04-22",
        "visual_diff": {
            "deck_changes": [
                {
                    "deck": "patrick-gaughan-LAND",
                    "change": "modified",
                    "slide_count_before": 10,
                    "slide_count_after": 10,
                    "slide_changes": [{"slide": "slide-1.png"}],
                    "comparison_montage_path": pack_script._display_path(comparison_one),
                },
                {
                    "deck": "jesper-tyrer-LAND",
                    "change": "modified",
                    "slide_count_before": 12,
                    "slide_count_after": 13,
                    "slide_changes": [{"slide": "slide-2.png"}, {"slide": "slide-3.png"}],
                    "comparison_montage_path": pack_script._display_path(comparison_two),
                },
            ]
        },
    }
    fill_diff = {
        "status": "ok",
        "director_payloads": {
            "changes": [
                {
                    "slug": "patrick-gaughan",
                    "change": "modified",
                    "field_changes": {"open_land_arr": {"before": 1, "after": 2}},
                },
                {
                    "slug": "jesper-tyrer",
                    "change": "modified",
                    "field_changes": {
                        "approved_2026": {"before": 1, "after": 2},
                        "conditionally_approved": {"before": 0, "after": 1},
                    },
                },
            ]
        },
    }

    modular_payload = (
        pack_script.MASTER_BUILDER_ROOT
        / "2026-04-10"
        / "20260422-163844"
        / "jesper-tyrer"
        / "validated_bridge"
        / "powerpoint-fill-payload.json"
    )
    modular_payload.parent.mkdir(parents=True, exist_ok=True)
    modular_payload.write_text(
        """
        {
          "slides": [
            {"slots": {"q2_omitted_arr": "€1.1M"}}
          ]
        }
        """.strip(),
        encoding="utf-8",
    )

    output_dir = tmp_path / "output" / "golden_deck_regression_pack" / "2026-04-22"
    payload = pack_script.assemble_pack(
        run_date="2026-04-22",
        visual_diff=visual_diff,
        fill_diff=fill_diff,
        output_dir=output_dir,
    )

    assert payload["status"] == "ok"
    assert payload["baseline_run_date"] == "2026-04-20"
    assert payload["overview_montage_path"].endswith("golden_pack_overview.png")
    assert [item["role"] for item in payload["items"]] == ["clean_book", "approval_heavy"]
    assert payload["items"][0]["fill_payload_change"]["changed_fields"] == ["open_land_arr"]
    assert payload["items"][1]["modular_omitted_metric"]["value"] == "€1.1M"


def test_main_writes_skipped_pack_when_visual_diff_has_no_baseline(
    tmp_path: Path, monkeypatch
) -> None:
    output_root = tmp_path / "output" / "golden_deck_regression_pack"
    visual_root = tmp_path / "output" / "deck_visual_snapshot_diff"
    fill_root = tmp_path / "output" / "deck_fill_payload_snapshot_diff"
    visual_dir = visual_root / "2026-04-22"
    fill_dir = fill_root / "2026-04-22"
    visual_dir.mkdir(parents=True, exist_ok=True)
    fill_dir.mkdir(parents=True, exist_ok=True)
    (visual_dir / "deck_visual_snapshot_diff.json").write_text(
        '{"status":"skipped","reason":"baseline_not_found","current_run_date":"2026-04-22"}',
        encoding="utf-8",
    )
    (fill_dir / "deck_fill_payload_snapshot_diff.json").write_text(
        '{"status":"ok","director_payloads":{"changes":[]}}',
        encoding="utf-8",
    )

    monkeypatch.setattr(pack_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(pack_script, "VISUAL_DIFF_ROOT", visual_root)
    monkeypatch.setattr(pack_script, "FILL_DIFF_ROOT", fill_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["assemble_golden_deck_regression_pack.py", "--date", "2026-04-22"],
    )

    assert pack_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = output_root / "2026-04-22" / "golden_deck_regression_pack.json"
    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
