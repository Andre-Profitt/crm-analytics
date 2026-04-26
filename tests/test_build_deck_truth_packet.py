import json
from pathlib import Path

import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.build_deck_truth_packet import (
    build_packet,
    classify_bundle_issue,
    write_ppttc,
    write_rag_corpus,
    write_workbook,
)


def _gold_pack(director: str = "Jesper Tyrer", territory: str = "APAC") -> dict:
    return {
        "schema_version": "1",
        "artifact_type": "director_gold_analytics_pack",
        "snapshot_date": "2026-04-22",
        "director": director,
        "territory": territory,
        "summary": {
            "open_deals": 5,
            "open_arr": 1_250_000.0,
            "close_date_event_count": 12,
            "deal_risk_rows": 4,
            "high_stage_zero_arr_count": 1,
            "top_20_pipeline_concentration_pct": 80.0,
        },
        "analytics": {},
        "deck_ready_insights": [],
    }


def test_deck_truth_packet_writes_grounded_outputs(tmp_path):
    snapshot_date = "2026-04-22"
    gold_root = tmp_path / "gold"
    pack_dir = gold_root / snapshot_date / "jesper-tyrer"
    pack_dir.mkdir(parents=True)
    pack_path = pack_dir / "gold_analytics.json"
    pack_path.write_text(json.dumps(_gold_pack()) + "\n", encoding="utf-8")
    manifest = {
        "snapshot_date": snapshot_date,
        "directors": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_date": snapshot_date,
                "bundle_path": str(tmp_path / "bundles" / "jesper-tyrer.json"),
                "json_path": str(pack_path),
            }
        ],
        "regional_rollups": [
            {
                "region": "APAC",
                "director_count": 1,
                "territories": ["APAC"],
                "totals": {
                    "open_deals": 5,
                    "open_arr": 1_250_000.0,
                    "deal_risk_rows": 4,
                    "close_date_event_count": 12,
                },
            }
        ],
    }
    (gold_root / snapshot_date / "manifest.json").write_text(
        json.dumps(manifest) + "\n",
        encoding="utf-8",
    )
    workbook_dir = tmp_path / "workbooks"
    workbook_dir.mkdir()
    (workbook_dir / "jesper-tyrer.xlsx").write_bytes(b"placeholder")

    packet = build_packet(
        snapshot_date=snapshot_date,
        gold_root=gold_root,
        workbook_dir=workbook_dir,
        bundle_dir=tmp_path / "bundles",
        decks_dir=None,
        tieout_path=None,
        template_path="template.pptx",
    )

    assert packet["summary"]["director_count"] == 1
    assert packet["summary"]["metric_count"] == 8
    assert packet["summary"]["claim_count"] == 8
    assert {claim["claim_id"] for claim in packet["claims"]} >= {
        "director.jesper-tyrer.open_arr",
        "region.apac.open_arr",
    }
    assert {metric["metric_id"] for metric in packet["metrics"]} >= {
        "gold.director.jesper-tyrer.open_arr",
        "gold.region.apac.open_arr",
    }
    open_arr_claim = next(
        claim
        for claim in packet["claims"]
        if claim["claim_id"] == "director.jesper-tyrer.open_arr"
    )
    assert open_arr_claim["metric_id"] == "gold.director.jesper-tyrer.open_arr"

    output_dir = tmp_path / "packet"
    workbook_path = output_dir / "thinkcell_source.xlsx"
    ppttc_path = output_dir / "thinkcell_data.ppttc"
    rag_path = output_dir / "rag_corpus.jsonl"
    write_workbook(workbook_path, packet)
    write_ppttc(ppttc_path, packet)
    write_rag_corpus(rag_path, packet)

    assert workbook_path.exists()
    assert json.loads(ppttc_path.read_text(encoding="utf-8"))[0]["data"][0] == {
        "name": "TruthStatus",
        "table": [[{"string": "blocked"}]],
    }
    assert "director.jesper-tyrer.open_arr" in rag_path.read_text(encoding="utf-8")


def test_deck_truth_packet_compiles_sidecar_claim_refs(tmp_path):
    snapshot_date = "2026-04-22"
    gold_root = tmp_path / "gold"
    pack_dir = gold_root / snapshot_date / "jesper-tyrer"
    pack_dir.mkdir(parents=True)
    pack_path = pack_dir / "gold_analytics.json"
    pack_path.write_text(json.dumps(_gold_pack()) + "\n", encoding="utf-8")
    manifest = {
        "snapshot_date": snapshot_date,
        "directors": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_date": snapshot_date,
                "bundle_path": str(tmp_path / "bundles" / "jesper-tyrer.json"),
                "json_path": str(pack_path),
            }
        ],
        "regional_rollups": [],
    }
    (gold_root / snapshot_date / "manifest.json").write_text(
        json.dumps(manifest) + "\n",
        encoding="utf-8",
    )
    workbook_dir = tmp_path / "workbooks"
    workbook_dir.mkdir()
    (workbook_dir / "jesper-tyrer.xlsx").write_bytes(b"placeholder")
    decks_dir = tmp_path / "decks"
    decks_dir.mkdir()
    sidecar_fields = {
        "open_land_deals": 8,
        "open_land_arr": 9_700_000.0,
    }
    (decks_dir / "jesper-tyrer-LAND.json").write_text(
        json.dumps(
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                **sidecar_fields,
                "claim_ids": {
                    key: f"deck.jesper-tyrer.{key}" for key in sidecar_fields
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    packet = build_packet(
        snapshot_date=snapshot_date,
        gold_root=gold_root,
        workbook_dir=workbook_dir,
        bundle_dir=tmp_path / "bundles",
        decks_dir=decks_dir,
        tieout_path=None,
        template_path="template.pptx",
    )

    assert packet["deck_sidecar_claim_refs"] == [
        {
            "slug": "jesper-tyrer",
            "sidecar_path": str(decks_dir / "jesper-tyrer-LAND.json"),
            "claim_ids": {
                "open_land_deals": "deck.jesper-tyrer.open_land_deals",
                "open_land_arr": "deck.jesper-tyrer.open_land_arr",
            },
            "embedded": True,
            "missing_embedded_fields": [],
        }
    ]
    assert {claim["claim_id"] for claim in packet["claims"]} >= {
        "deck.jesper-tyrer.open_land_deals",
        "deck.jesper-tyrer.open_land_arr",
    }
    assert {metric["metric_id"] for metric in packet["metrics"]} >= {
        "deck_sidecar.jesper-tyrer.open_land_deals",
        "deck_sidecar.jesper-tyrer.open_land_arr",
    }


def test_deck_truth_packet_accepts_single_source_backed_analyst_workbook(tmp_path):
    snapshot_date = "2026-04-22"
    gold_root = tmp_path / "gold"
    pack_dir = gold_root / snapshot_date / "jesper-tyrer"
    pack_dir.mkdir(parents=True)
    pack_path = pack_dir / "gold_analytics.json"
    pack_path.write_text(json.dumps(_gold_pack()) + "\n", encoding="utf-8")
    manifest = {
        "snapshot_date": snapshot_date,
        "directors": [
            {
                "director": "Jesper Tyrer",
                "territory": "APAC",
                "snapshot_date": snapshot_date,
                "bundle_path": str(tmp_path / "bundles" / "jesper-tyrer.json"),
                "json_path": str(pack_path),
            }
        ],
        "regional_rollups": [],
    }
    (gold_root / snapshot_date / "manifest.json").write_text(
        json.dumps(manifest) + "\n",
        encoding="utf-8",
    )
    analyst_workbook = tmp_path / "source_backed_analyst_workbook.xlsx"
    analyst_workbook.write_bytes(b"placeholder")
    source_gate = tmp_path / "source_backed_publish_gate.json"
    source_gate.write_text('{"status": "ok"}\n', encoding="utf-8")

    packet = build_packet(
        snapshot_date=snapshot_date,
        gold_root=gold_root,
        workbook_dir=tmp_path / "missing-per-director-workbooks",
        bundle_dir=tmp_path / "bundles",
        decks_dir=None,
        tieout_path=None,
        template_path="template.pptx",
        analyst_workbook_path=analyst_workbook,
        source_backed_publish_gate_path=source_gate,
    )

    assert not any(blocker["issue"] == "missing_workbook" for blocker in packet["blockers"])
    assert not any(
        blocker["issue"] == "missing_etl_intelligence_audit"
        for blocker in packet["blockers"]
    )
    assert packet["directors"][0]["workbook_path"] == str(analyst_workbook)
    assert packet["sources"]["analyst_workbook_path"] == str(analyst_workbook)
    assert packet["sources"]["source_backed_publish_gate"] == str(source_gate)
    assert packet["summary"]["source_backed_publish_gate_status"] == "ok"


def test_small_negative_arr_bundle_issue_is_publish_warning():
    finding = classify_bundle_issue(
        "pipeline_open[106]: negative arr_unweighted (-3624.36)"
    )

    assert finding["severity"] == "medium"
    assert finding["issue"] == "bundle_validation_warning"
