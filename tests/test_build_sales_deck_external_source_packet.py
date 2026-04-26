from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.build_sales_deck_external_source_packet as module


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def test_build_status_packet_marks_provided_and_pending(tmp_path: Path) -> None:
    contract = {
        "sources": [
            {
                "id": "kyc",
                "title": "KYC",
                "owner": "Sales Ops",
                "system": "Salesforce Report",
                "default_status": "pending_ingest",
                "required_for_publish": False,
                "current_coverage": "gap",
                "expected_files": ["export.csv"],
            },
            {
                "id": "finance",
                "title": "Finance churn overlay",
                "owner": "Finance",
                "system": "Finance",
                "default_status": "pending_owner_intake",
                "required_for_publish": False,
                "current_coverage": "placeholder_only",
                "expected_files": ["overlay.json"],
            },
        ]
    }
    source_dir = tmp_path / "inputs" / "2026-04-10" / "kyc"
    source_dir.mkdir(parents=True)
    (source_dir / "export.csv").write_text("ok\n", encoding="utf-8")

    packet = module.build_status_packet(
        snapshot_date="2026-04-10",
        contract=contract,
        contract_path=tmp_path / "contract.json",
        input_root=tmp_path / "inputs",
    )

    assert packet["publish_ready"] is True
    assert packet["status_summary"]["provided"] == 1
    assert packet["status_summary"]["pending"] == 1
    provided = next(row for row in packet["sources"] if row["id"] == "kyc")
    pending = next(row for row in packet["sources"] if row["id"] == "finance")
    assert provided["status"] == "provided"
    assert pending["status"] == "pending_owner_intake"


def test_write_latest_aliases_writes_snapshot_and_root_latest(tmp_path: Path) -> None:
    packet = {"snapshot_date": "2026-04-10", "publish_ready": True}
    markdown = "# status\n"
    run_dir = tmp_path / "packets" / "2026-04-10" / "20260412-210000"

    module.write_latest_aliases(
        output_root=tmp_path / "packets",
        snapshot_date="2026-04-10",
        packet=packet,
        markdown=markdown,
        run_dir=run_dir,
    )

    snapshot_latest = json.loads((tmp_path / "packets" / "2026-04-10" / "latest.json").read_text())
    root_latest = json.loads((tmp_path / "packets" / "latest.json").read_text())
    assert snapshot_latest["packet_dir"] == str(run_dir)
    assert root_latest["packet_dir"] == str(run_dir)
    assert (tmp_path / "packets" / "latest.md").read_text() == markdown


def test_main_writes_finance_request_templates(tmp_path: Path, monkeypatch) -> None:
    contract_path = tmp_path / "contract.json"
    _write_json(
        contract_path,
        {
            "sources": [
                {
                    "id": "finance",
                    "title": "Finance churn overlay",
                    "owner": "Finance",
                    "system": "Finance",
                    "default_status": "pending_owner_intake",
                    "required_for_publish": False,
                    "current_coverage": "placeholder_only",
                    "expected_files": ["overlay.json"],
                }
            ]
        },
    )

    monkeypatch.setattr(
        sys,
        "argv",
        [
            "build_sales_deck_external_source_packet.py",
            "--snapshot-date",
            "2026-04-10",
            "--contract-path",
            str(contract_path),
            "--input-root",
            str(tmp_path / "inputs"),
            "--output-root",
            str(tmp_path / "outputs"),
        ],
    )

    module.main()

    dated_root = tmp_path / "outputs" / "2026-04-10"
    runs = [path for path in dated_root.iterdir() if path.is_dir()]
    assert runs
    run_dir = runs[0]
    assert (run_dir / "external-source-status.json").exists()
    assert (run_dir / "finance_churn_request.json").exists()
    assert (run_dir / "finance_churn_request.csv").exists()
    assert (tmp_path / "outputs" / "latest.json").exists()
