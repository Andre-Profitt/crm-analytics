from __future__ import annotations

import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "audit_revenue_retention_health.py"


def load_module():
    spec = importlib.util.spec_from_file_location("audit_revenue_retention_health_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_audit_command_warns_and_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_payload = {
        "live_export_dir": str(tmp_path / "live_export"),
        "dashboard": "Revenue Retention & Health",
        "page_labels": ["Retention Summary"],
        "widget_count": 18,
        "step_count": 12,
        "chrome_ratio": 0.3,
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
    assert result["summary"]["check_count"] == 1
    audit_json_path = tmp_path / "audit" / "audit.json"
    audit_md_path = tmp_path / "audit" / "audit.md"
    assert audit_json_path.exists()
    assert audit_md_path.exists()
    assert json.loads(audit_json_path.read_text(encoding="utf-8")) == fake_payload
    assert audit_md_path.read_text(encoding="utf-8").startswith("# Revenue Retention & Health Audit")
    assert result["artifacts"] == [
        {"kind": "json", "path": str(audit_json_path)},
        {"kind": "markdown", "path": str(audit_md_path)},
    ]
