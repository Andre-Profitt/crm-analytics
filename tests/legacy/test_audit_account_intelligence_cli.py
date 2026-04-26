from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "audit_account_intelligence.py"


def load_module():
    spec = importlib.util.spec_from_file_location("audit_account_intelligence_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_audit_command_warns_and_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_payload = {
        "live_export_dir": str(tmp_path / "live_export"),
        "dashboard": "Account Intelligence KPIs",
        "page_labels": ["Data Quality"],
        "widget_count": 12,
        "step_count": 9,
        "chrome_ratio": 0.24,
        "checks": [
            {
                "category": "story",
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

    assert exit_code == 1
    assert result["status"] == "warn"
    assert result["summary"]["fail_count"] == 1
    assert (tmp_path / "audit" / "audit.json").exists()
    assert (tmp_path / "audit" / "audit.md").exists()
    assert any(artifact["path"].endswith("audit.json") for artifact in result["artifacts"])
