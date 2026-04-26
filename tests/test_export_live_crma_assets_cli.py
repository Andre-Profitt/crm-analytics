from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "export_live_crma_assets.py"


def load_module():
    spec = importlib.util.spec_from_file_location("export_live_crma_assets_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_export_warns_on_dataset_warning(monkeypatch, tmp_path: Path) -> None:
    module = load_module()

    def fake_get_auth():
        return ("https://example.my.salesforce.com", "token")

    def fake_export_dashboard(inst, tok, label, output_root):
        asset_dir = output_root / "test_dashboard"
        asset_dir.mkdir(parents=True, exist_ok=True)
        return {
            "label": label,
            "id": "0FKTEST",
            "name": "Test Dashboard",
            "pages": ["Overview"],
            "widgetCount": 3,
            "stepCount": 2,
            "datasets": [
                {"name": "DatasetA", "status": "ok", "warning": None, "error": None},
                {"name": "DatasetB", "status": "missing", "warning": "dataset has no currentVersionId", "error": None},
            ],
            "assetDir": str(asset_dir),
        }

    monkeypatch.setattr(module, "get_auth", fake_get_auth)
    monkeypatch.setattr(module, "export_dashboard", fake_export_dashboard)

    payload, exit_code = module.run_export(["Test Dashboard"], tmp_path, emit_text=False)
    assert exit_code == 0
    assert payload["status"] == "warn"
    assert payload["summary"]["dashboards_exported"] == 1
    assert payload["summary"]["dataset_warning_count"] == 1
    assert any(artifact["path"].endswith("manifest.json") for artifact in payload["artifacts"])


def test_run_export_errors_on_failed_label(monkeypatch, tmp_path: Path) -> None:
    module = load_module()

    def fake_get_auth():
        return ("https://example.my.salesforce.com", "token")

    def fake_export_dashboard(inst, tok, label, output_root):
        if label == "Broken":
            raise RuntimeError("dashboard not found")
        asset_dir = output_root / "ok_dashboard"
        asset_dir.mkdir(parents=True, exist_ok=True)
        return {
            "label": label,
            "id": "0FKOK",
            "name": label,
            "pages": [],
            "widgetCount": 1,
            "stepCount": 1,
            "datasets": [],
            "assetDir": str(asset_dir),
        }

    monkeypatch.setattr(module, "get_auth", fake_get_auth)
    monkeypatch.setattr(module, "export_dashboard", fake_export_dashboard)

    payload, exit_code = module.run_export(["Good", "Broken"], tmp_path, emit_text=False)
    assert exit_code == 1
    assert payload["status"] == "error"
    assert payload["summary"]["dashboard_errors"] == 1
    assert payload["errors"][0]["label"] == "Broken"
