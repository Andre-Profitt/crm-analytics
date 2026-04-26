from pathlib import Path
import sys
import json


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_sales_director_monthly_master_builder import (  # noqa: E402
    run_deck_truth_packet_gate,
    run_source_backed_deck_gate,
)


def test_run_deck_truth_packet_gate_skips_until_inputs_exist(tmp_path: Path) -> None:
    result = run_deck_truth_packet_gate(
        snapshot_date="2026-04-22",
        gold_root=tmp_path / "gold",
        workbook_dir=tmp_path / "workbooks",
        bundle_dir=tmp_path / "bundles",
        decks_dir=tmp_path / "decks",
        tieout_path=tmp_path / "tieout.json",
        output_root=tmp_path / "truth",
        template_path="template.pptx",
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "deck truth packet inputs are not complete"
    assert len(result["missing_inputs"]) == 5


def test_run_deck_truth_packet_gate_can_require_inputs(tmp_path: Path) -> None:
    result = run_deck_truth_packet_gate(
        snapshot_date="2026-04-22",
        gold_root=tmp_path / "gold",
        workbook_dir=tmp_path / "workbooks",
        bundle_dir=tmp_path / "bundles",
        decks_dir=tmp_path / "decks",
        tieout_path=tmp_path / "tieout.json",
        output_root=tmp_path / "truth",
        template_path="template.pptx",
        require=True,
    )

    assert result["status"] == "error"


def test_run_deck_truth_packet_gate_supports_source_backed_inputs(tmp_path: Path) -> None:
    snapshot_date = "2026-04-22"
    gold_root = tmp_path / "gold"
    bundle_dir = tmp_path / "bundles"
    output_root = tmp_path / "truth"
    pack_dir = gold_root / snapshot_date / "jesper-tyrer"
    pack_dir.mkdir(parents=True)
    bundle_dir.mkdir()
    analyst_workbook = tmp_path / "source_backed_analyst_workbook.xlsx"
    analyst_workbook.write_bytes(b"placeholder")
    source_gate = tmp_path / "source_backed_publish_gate.json"
    source_gate.write_text('{"status": "ok"}\n', encoding="utf-8")
    pack_path = pack_dir / "gold_analytics.json"
    pack_path.write_text(
        json.dumps(
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "summary": {
                    "open_deals": 1,
                    "open_arr": 100.0,
                    "deal_risk_rows": 0,
                    "close_date_event_count": 0,
                    "top_20_pipeline_concentration_pct": 100.0,
                    "high_stage_zero_arr_count": 0,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (gold_root / snapshot_date / "manifest.json").write_text(
        json.dumps(
            {
                "directors": [
                    {
                        "director": "Jesper Tyrer",
                        "territory": "APAC",
                        "json_path": str(pack_path),
                        "bundle_path": str(bundle_dir / "missing-bundle.json"),
                    }
                ],
                "regional_rollups": [],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = run_deck_truth_packet_gate(
        snapshot_date=snapshot_date,
        gold_root=gold_root,
        workbook_dir=tmp_path / "missing-workbooks",
        analyst_workbook_path=analyst_workbook,
        bundle_dir=bundle_dir,
        decks_dir=tmp_path / "missing-decks",
        tieout_path=tmp_path / "missing-tieout.json",
        output_root=output_root,
        template_path="template.pptx",
        source_backed_publish_gate_path=source_gate,
        require_decks_tieout=False,
    )

    assert result["status"] == "blocked"
    assert result["returncode"] == 2
    assert result["high_blocker_count"] >= 1
    assert "--analyst-workbook" in result["command"]
    assert "--source-backed-publish-gate" in result["command"]
    assert not result.get("missing_inputs")


def test_run_source_backed_deck_gate_builds_and_audits(tmp_path: Path) -> None:
    truth_packet_path = _source_backed_truth_packet(tmp_path)
    result = run_source_backed_deck_gate(
        truth_stage={
            "status": "ok",
            "manifest_path": str(truth_packet_path),
        },
        deck_output_root=tmp_path / "decks",
        visual_output_root=tmp_path / "visuals",
        source_backed_publish_gate_path=tmp_path / "source_backed_publish_gate.json",
    )

    assert result["status"] == "ok"
    assert Path(result["deck_build"]["deck_path"]).exists()
    assert Path(result["deck_build"]["manifest_path"]).exists()
    assert Path(result["visual_audit"]["output_path"]).exists()
    assert result["visual_audit"]["summary"]["finding_count"] == 0


def test_run_source_backed_deck_gate_skips_without_source_backed_inputs(
    tmp_path: Path,
) -> None:
    truth_packet_path = _source_backed_truth_packet(tmp_path)
    result = run_source_backed_deck_gate(
        truth_stage={
            "status": "ok",
            "manifest_path": str(truth_packet_path),
        },
        deck_output_root=tmp_path / "decks",
        visual_output_root=tmp_path / "visuals",
    )

    assert result["status"] == "skipped"
    assert result["reason"] == "source-backed deck inputs were not requested"


def _source_backed_truth_packet(tmp_path: Path) -> Path:
    source_gate = tmp_path / "source_backed_publish_gate.json"
    source_bundle_dir = tmp_path / "source_bundles"
    source_bundle_dir.mkdir()
    source_gate.write_text(
        json.dumps(
            {
                "status": "ok",
                "source_run_dir": str(tmp_path / "sources" / "run-a"),
                "source_bundle_dir": str(source_bundle_dir),
                "counts": {
                    "source_extract_count": 12,
                    "selected_source_count": 12,
                    "director_bundle_count": 2,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    (source_bundle_dir / "source_bundle_manifest.json").write_text(
        json.dumps(
            {
                "source_run_id": "run-a",
                "summary": {
                    "forward_fallback_count": 1,
                    "territories": [
                        {
                            "director": "Jesper Tyrer",
                            "territory": "APAC",
                            "display_quarter_title": "Q2 2026",
                            "display_reason": "current_quarter",
                            "current_quarter_active_deals": 2,
                            "forward_quarter_active_deals": 1,
                        },
                        {
                            "director": "Megan Miceli",
                            "territory": "Canada",
                            "display_quarter_title": "Q3 2026",
                            "display_reason": "forward_quarter_fallback",
                            "current_quarter_active_deals": 0,
                            "forward_quarter_active_deals": 2,
                        },
                    ],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    truth_packet = tmp_path / "deck_truth_packet.json"
    truth_packet.write_text(
        json.dumps(
            {
                "snapshot_date": "2026-04-30",
                "status": "ok",
                "summary": {
                    "director_count": 2,
                    "metric_count": 12,
                    "claim_count": 12,
                    "high_blocker_count": 0,
                    "tieout_mismatch_count": 0,
                },
                "sources": {
                    "source_backed_publish_gate": str(source_gate),
                },
                "thinkcell": {
                    "recommended_element_names": [
                        "TruthStatus",
                        "RegionalRollupsTable",
                        "DirectorKpiTable",
                        "PublishBlockersTable",
                    ]
                },
                "regional_rollups": [
                    {
                        "region": "APAC",
                        "territories": ["APAC"],
                        "totals": {
                            "open_arr": 1_500_000,
                            "open_deals": 3,
                            "deal_risk_rows": 1,
                        },
                    },
                    {
                        "region": "NAM",
                        "territories": ["Canada"],
                        "totals": {
                            "open_arr": 900_000,
                            "open_deals": 2,
                            "deal_risk_rows": 0,
                        },
                    },
                ],
                "directors": [
                    {
                        "director": "Jesper Tyrer",
                        "territory": "APAC",
                        "open_arr": 1_500_000,
                        "open_deals": 3,
                        "deal_risk_rows": 1,
                        "tieout_mismatch_count": 0,
                        "bundle_issue_count": 0,
                    },
                    {
                        "director": "Megan Miceli",
                        "territory": "Canada",
                        "open_arr": 900_000,
                        "open_deals": 2,
                        "deal_risk_rows": 0,
                        "tieout_mismatch_count": 0,
                        "bundle_issue_count": 0,
                    },
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return truth_packet
