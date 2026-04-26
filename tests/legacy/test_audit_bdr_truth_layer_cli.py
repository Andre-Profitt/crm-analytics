from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "audit_bdr_truth_layer.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "audit_bdr_truth_layer_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_audit_command_warns_and_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_payload = {
        "dataset": "BDR_Operating_Rhythm",
        "dataset_id": "0FbTEST",
        "record_type_counts": {"account_product_target": 12, "opportunity_detail": 8},
        "account_product_stats": {
            "total_accounts": 6,
            "multi_product_accounts": 2,
            "avg_products_per_account": 1.5,
            "max_products_per_account": 3,
            "product_source_counts": {"High Intent": 8},
            "product_confidence_counts": {"High": 6, "Medium": 4},
            "top_targeted_products": [{"product": "SimCorp One", "row_count": 5}],
            "dominant_product_share": 0.42,
        },
        "opportunity_product_stats": {
            "opportunity_rows": 8,
            "nonempty_raw_rows": 7,
            "fallback_rows": 1,
        },
        "checks": [
            {
                "category": "truth_layer",
                "name": "sample_check",
                "passed": False,
                "detail": "sample detail",
            }
        ],
        "pass_count": 0,
        "fail_count": 1,
    }

    monkeypatch.setattr(module, "run_audit", lambda: fake_payload)

    result, exit_code = module.run_audit_command(
        tmp_path / "audit",
        emit_text=False,
    )

    assert exit_code == 1
    assert result["status"] == "warn"
    assert result["lane"] == "wave_data_validations"
    assert result["command_class"] == "live_read"
    assert result["summary"]["dataset"] == "BDR_Operating_Rhythm"
    assert result["summary"]["fail_count"] == 1
    assert result["summary"]["multi_product_accounts"] == 2
    assert (tmp_path / "audit" / "audit.json").exists()
    assert (tmp_path / "audit" / "audit.md").exists()
    assert any(artifact["path"].endswith("audit.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("audit.md") for artifact in result["artifacts"])
