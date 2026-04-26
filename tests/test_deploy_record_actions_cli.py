from __future__ import annotations

import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "deploy_record_actions.py"


def load_module():
    spec = importlib.util.spec_from_file_location("deploy_record_actions_test", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_run_deployment_dry_run(monkeypatch) -> None:
    module = load_module()

    sample_dashboard = {
        "state": {
            "gridLayouts": [{"pages": [{"name": "overview", "widgets": []}]}],
            "widgets": {
                "w_table": {
                    "parameters": {
                        "step": "s_table",
                        "visualizationType": "comparisontable",
                    }
                }
            },
            "steps": {
                "s_table": {
                    "query": 'q = load "Dataset"; q = foreach q generate Name as Name;'
                }
            },
        }
    }

    def fake_get_token():
        return ("https://example.my.salesforce.com", "token")

    def fake_get_dashboard(inst, tok, dashboard_id):
        return sample_dashboard

    def fake_patch_dashboard(inst, tok, dashboard_id, state):
        raise AssertionError("patch_dashboard should not run during dry-run")

    monkeypatch.setattr(module, "get_token", fake_get_token)
    monkeypatch.setattr(module, "get_dashboard", fake_get_dashboard)
    monkeypatch.setattr(module, "patch_dashboard", fake_patch_dashboard)

    config = [
        {
            "id": "0FKTEST",
            "name": "Test Dashboard",
            "tables": [
                {"widget": "w_table", "object": "Opportunity", "id_field": "Id"}
            ],
        }
    ]

    payload, exit_code = module.run_deployment(
        config,
        dry_run=True,
        sleep_seconds=0,
        emit_text=False,
    )
    assert exit_code == 0
    assert payload["status"] == "ok"
    assert payload["summary"]["dry_run"] is True
    assert payload["summary"]["record_actions_added"] == 1
    assert payload["summary"]["saql_steps_modified"] == 1
    assert payload["dashboards"][0]["status"] == "dry_run"
