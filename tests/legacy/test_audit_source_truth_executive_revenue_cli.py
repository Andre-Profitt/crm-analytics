from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "audit_source_truth_executive_revenue.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "audit_source_truth_executive_revenue_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_audit_command_warns_and_preserves_zero_exit(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_payload = {
        "checked_at": "2026-03-26",
        "dashboard_label": "Executive Revenue Source Truth",
        "live_export_dir": str(tmp_path / "live_export"),
        "source_truth_values": {
            "fy25_closed_won_arr": 10.0,
            "fy26_closed_won_arr": 11.0,
        },
        "checks": [
            {
                "category": "metric",
                "name": "sample_check",
                "passed": False,
                "detail": "sample detail",
            }
        ],
        "pass_count": 0,
        "fail_count": 1,
    }

    monkeypatch.setattr(module, "run_audit", lambda live_export_dir: fake_payload)

    result, exit_code = module.run_audit_command(
        tmp_path / "live_export",
        tmp_path / "audit",
        emit_text=False,
    )

    assert exit_code == 0
    assert result["status"] == "warn"
    assert result["lane"] == "wave_data_validations"
    assert result["command_class"] == "live_read"
    assert result["summary"]["fail_count"] == 1
    assert result["summary"]["source_truth_values"]["fy26_closed_won_arr"] == 11.0
    assert (tmp_path / "audit" / "audit.json").exists()
    assert (tmp_path / "audit" / "audit.md").exists()
    assert any(artifact["path"].endswith("audit.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("audit.md") for artifact in result["artifacts"])
