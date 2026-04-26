from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.build_sales_deck_release_packet as module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def _touch(path: Path) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ok\n", encoding="utf-8")
    return str(path)


def test_build_release_packet_is_publish_ready_when_artifacts_are_clean(
    tmp_path: Path, monkeypatch
) -> None:
    # The test verifies happy-path *logic* with a single synthetic director
    # (Jane Doe). The production gate consults
    # ``config/sd_monthly_territories.json`` and expects all 9 territories,
    # which is a deployment concern (and changes when territories are added /
    # removed) — not a release-packet logic concern. Pin the expected count
    # to match what the fixture sets up so the test stays meaningful even
    # when the live territory config is edited.
    monkeypatch.setattr(module, "_expected_director_count", lambda: 1)

    director_run_dir = tmp_path / "director-run"
    global_run_dir = tmp_path / "global-run"
    global_canonical_run_dir = tmp_path / "global-canonical-run"
    director_canonical_root = tmp_path / "director-canonical"
    global_canonical_root = tmp_path / "global-canonical-root"

    fact_pack = _touch(
        director_run_dir / "jane" / "validated_bridge" / "validated-fact-pack.md"
    )
    validation_report = _touch(
        director_run_dir / "jane" / "validated_bridge" / "validation-report.json"
    )
    fill_payload = str(
        director_run_dir / "jane" / "validated_bridge" / "powerpoint-fill-payload.json"
    )
    _write_json(
        Path(fill_payload),
        {
            "director_name": "Jane Doe",
            "territory": "APAC",
            "slides": [
                {
                    "id": "quarterly-pipeline",
                    "slots": {
                        "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                        "quarterly_pipeline_title": "Q3 2026",
                        "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                    },
                }
            ],
        },
    )
    deck_path = _touch(
        director_run_dir / "jane" / "deterministic_preview" / "deck.pptx"
    )
    montage_path = _touch(director_run_dir / "jane" / "render" / "montage.png")

    _write_json(
        director_run_dir / "manifest.json",
        {
            "status": "ok",
            "targets": [
                {
                    "director_name": "Jane Doe",
                    "territory": "APAC",
                    "status": "ok",
                    "stages": {
                        "validated_bridge": {
                            "validated_fact_pack": fact_pack,
                            "validation_report": validation_report,
                            "powerpoint_fill_payload": fill_payload,
                        },
                        "deterministic_preview": {"deck_path": deck_path},
                        "deterministic_preview_render": {
                            "montage_path": montage_path,
                            "font_report": {
                                "font_missing_overall": [],
                                "font_substituted_overall": [],
                            },
                        },
                        "deterministic_preview_audit": {"ok": True, "finding_count": 0},
                        "deterministic_preview_layout_audit": {"ok": True},
                        "powerpoint_review": {
                            "status": "skipped",
                            "reason": "PowerPoint Claude review skipped for this run.",
                        },
                    },
                }
            ],
        },
    )
    _write_json(director_run_dir / "summary.json", {"status": "ok", "targets": []})
    _write_json(
        director_run_dir / "canonical-promotion-summary.json",
        {"promoted_count": 1, "promoted": [], "skipped": []},
    )

    global_fact_pack = _touch(
        global_run_dir / "validated_bridge" / "validated-fact-pack.md"
    )
    global_deck_path = _touch(global_run_dir / "deterministic_preview" / "deck.pptx")
    global_montage_path = _touch(global_run_dir / "render" / "montage.png")
    audit_report_path = _touch(global_run_dir / "audit" / "audit-report.json")
    _write_json(
        global_run_dir / "manifest.json",
        {
            "status": "ok",
            "regions": ["APAC", "EMEA", "North America"],
            "validated_fact_pack_path": global_fact_pack,
            "powerpoint_fill_payload_path": str(
                global_run_dir / "validated_bridge" / "powerpoint-fill-payload.json"
            ),
            "deterministic_preview": {"deck_path": global_deck_path},
            "deterministic_preview_render": {
                "montage_path": global_montage_path,
                "font_report": {
                    "font_missing_overall": [],
                    "font_substituted_overall": [],
                },
            },
            "deterministic_preview_audit": {
                "ok": True,
                "finding_count": 0,
                "report_path": audit_report_path,
            },
            "powerpoint_build": {"status": "skipped", "reason": "powerpoint_mode=skip"},
        },
    )
    _write_json(
        global_run_dir / "validated_bridge" / "powerpoint-fill-payload.json",
        {
            "slides": [
                {
                    "id": "apac-region-summary",
                    "slots": {
                        "region_name": "APAC",
                        "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                        "quarterly_pipeline_title": "Q3 2026",
                        "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                    },
                }
            ]
        },
    )
    _write_json(
        global_canonical_run_dir / "manifest.json",
        {"canonical_promotion": {"status": "ok"}},
    )

    packet = module.build_release_packet(
        snapshot_date="2026-04-10",
        director_run_dir=director_run_dir,
        global_run_dir=global_run_dir,
        director_canonical_root=director_canonical_root,
        global_canonical_root=global_canonical_root,
        global_canonical_run_dir=global_canonical_run_dir,
        external_source_packet=None,
    )

    assert packet["publish_ready"] is True
    assert packet["blockers"] == []
    assert packet["director_release"]["ok"] is True
    assert packet["global_release"]["ok"] is True
    assert packet["director_release"]["targets"][0][
        "quarterly_pipeline_disclosure"
    ] == {
        "director_name": "Jane Doe",
        "territory": "APAC",
        "slide_id": "quarterly-pipeline",
        "display_reason": "forward_quarter_fallback",
        "quarterly_pipeline_title": "Q3 2026",
        "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
    }
    assert packet["global_release"]["quarterly_pipeline_disclosures"][
        "forward_quarter_fallbacks"
    ] == [
        {
            "slide_id": "apac-region-summary",
            "region_name": "APAC",
            "quarterly_pipeline_title": "Q3 2026",
            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        }
    ]


def test_build_release_packet_blocks_missing_validation_and_ambiguous_skips(
    tmp_path: Path,
) -> None:
    director_run_dir = tmp_path / "director-run"
    global_run_dir = tmp_path / "global-run"

    fact_pack = _touch(
        director_run_dir / "jane" / "validated_bridge" / "validated-fact-pack.md"
    )
    deck_path = _touch(
        director_run_dir / "jane" / "deterministic_preview" / "deck.pptx"
    )
    montage_path = _touch(director_run_dir / "jane" / "render" / "montage.png")

    _write_json(
        director_run_dir / "manifest.json",
        {
            "status": "ok",
            "targets": [
                {
                    "director_name": "Jane Doe",
                    "territory": "APAC",
                    "status": "ok",
                    "stages": {
                        "validated_bridge": {
                            "validated_fact_pack": fact_pack,
                            "validation_report": str(
                                director_run_dir / "missing-validation.json"
                            ),
                            "powerpoint_fill_payload": str(
                                director_run_dir / "missing-fill.json"
                            ),
                        },
                        "deterministic_preview": {"deck_path": deck_path},
                        "deterministic_preview_render": {
                            "montage_path": montage_path,
                            "font_report": {
                                "font_missing_overall": [],
                                "font_substituted_overall": [],
                            },
                        },
                        "deterministic_preview_audit": {"ok": True, "finding_count": 0},
                        "deterministic_preview_layout_audit": {"ok": True},
                        "powerpoint_review": {"status": "skipped"},
                    },
                }
            ],
        },
    )
    _write_json(
        director_run_dir / "canonical-promotion-summary.json",
        {"promoted_count": 1, "promoted": [], "skipped": []},
    )

    global_deck_path = _touch(global_run_dir / "deterministic_preview" / "deck.pptx")
    global_montage_path = _touch(global_run_dir / "render" / "montage.png")
    audit_report_path = _touch(global_run_dir / "audit" / "audit-report.json")
    _write_json(
        global_run_dir / "manifest.json",
        {
            "status": "ok",
            "regions": ["EMEA"],
            "validated_fact_pack_path": str(global_run_dir / "missing-fact-pack.md"),
            "deterministic_preview": {"deck_path": global_deck_path},
            "deterministic_preview_render": {
                "montage_path": global_montage_path,
                "font_report": {
                    "font_missing_overall": [],
                    "font_substituted_overall": [],
                },
            },
            "deterministic_preview_audit": {
                "ok": True,
                "finding_count": 0,
                "report_path": audit_report_path,
            },
            "powerpoint_build": {"status": "skipped"},
        },
    )

    packet = module.build_release_packet(
        snapshot_date="2026-04-10",
        director_run_dir=director_run_dir,
        global_run_dir=global_run_dir,
        director_canonical_root=tmp_path / "director-canonical",
        global_canonical_root=tmp_path / "global-canonical-root",
        global_canonical_run_dir=None,
        external_source_packet=None,
    )

    assert packet["publish_ready"] is False
    assert "Director validation reports missing: Jane Doe." in packet["blockers"]
    assert (
        "Director PowerPoint review stage missing or ambiguous: Jane Doe."
        in packet["blockers"]
    )
    assert "Global validated fact pack is missing." in packet["blockers"]
    assert "Global PowerPoint build stage missing or ambiguous." in packet["blockers"]
    assert "Global canonical promotion manifest is missing." in packet["blockers"]


def test_write_release_bundle_updates_latest_aliases(tmp_path: Path) -> None:
    run_dir = tmp_path / "release-packets" / "2026-04-10" / "20260412-203038"
    output_root = tmp_path / "release-packets"
    packet = {
        "snapshot_date": "2026-04-10",
        "publish_ready": True,
        "blocker_count": 0,
        "blockers": [],
        "director_release": {"run_dir": "/tmp/director"},
        "global_release": {"run_dir": "/tmp/global"},
        "canonical_paths": {},
    }
    markdown = "# Packet\n"

    module.write_release_bundle(
        run_dir=run_dir,
        packet=packet,
        markdown=markdown,
        output_root=output_root,
        snapshot_date="2026-04-10",
    )

    release_packet = json.loads((run_dir / "release-packet.json").read_text())
    snapshot_latest = json.loads(
        (output_root / "2026-04-10" / "latest.json").read_text()
    )
    root_latest = json.loads((output_root / "latest.json").read_text())

    assert release_packet["snapshot_date"] == "2026-04-10"
    assert snapshot_latest["release_dir"] == str(run_dir)
    assert root_latest["release_dir"] == str(run_dir)
    assert (output_root / "2026-04-10" / "latest.md").read_text() == markdown
    assert (output_root / "latest.md").read_text() == markdown


def test_build_release_markdown_lists_global_quarter_disclosures() -> None:
    packet = {
        "snapshot_date": "2026-04-10",
        "publish_ready": True,
        "blocker_count": 0,
        "blockers": [],
        "director_release": {
            "run_dir": "/tmp/director",
            "target_count": 0,
            "summary_path": None,
            "summary_markdown_path": None,
            "canonical_promotion_summary_path": None,
            "targets": [
                {
                    "director_name": "Jane Doe",
                    "territory": "APAC",
                    "status": "ok",
                    "audit_ok": True,
                    "layout_ok": True,
                    "font_missing_count": 0,
                    "font_substituted_count": 0,
                    "deck_path": "/tmp/director/deck.pptx",
                    "montage_path": "/tmp/director/montage.png",
                    "quarterly_pipeline_disclosure": {
                        "director_name": "Jane Doe",
                        "territory": "APAC",
                        "slide_id": "quarterly-pipeline",
                        "display_reason": "forward_quarter_fallback",
                        "quarterly_pipeline_title": "Q3 2026",
                        "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                    },
                }
            ],
        },
        "global_release": {
            "run_dir": "/tmp/global",
            "regions": ["APAC"],
            "deck_path": "/tmp/global/deck.pptx",
            "montage_path": "/tmp/global/montage.png",
            "audit_report_path": "/tmp/global/audit.json",
            "powerpoint_fill_payload_path": "/tmp/global/payload.json",
            "canonical_manifest_path": "/tmp/global/manifest.json",
            "quarterly_pipeline_disclosures": {
                "forward_quarter_fallbacks": [
                    {
                        "slide_id": "apac-region-summary",
                        "region_name": "APAC",
                        "quarterly_pipeline_title": "Q3 2026",
                        "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                    }
                ],
                "empty_quarter_regions": [],
            },
        },
        "canonical_paths": {
            "director_canonical_root": "/tmp/director-canonical",
            "global_canonical_root": "/tmp/global-canonical",
            "global_canonical_shell": "/tmp/global-canonical/Sales Global Summary Shell.pptx",
        },
    }

    markdown = module.build_release_markdown(packet)

    assert "## Director Quarter Disclosures" in markdown
    assert "- Forward-quarter fallback: Jane Doe (APAC) -> `Q3 2026`." in markdown
    assert "## Global Quarter Disclosures" in markdown
    assert "- Forward-quarter fallback: APAC -> `Q3 2026`." in markdown
    assert (
        "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook."
        in markdown
    )
