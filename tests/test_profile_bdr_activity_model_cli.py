from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_bdr_activity_model.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_bdr_activity_model_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    def fake_fetch_live_rows():
        users = [
            {
                "Id": "005A",
                "Name": "Alex BDR",
                "Title": "Business Development Representative",
                "Department": "North America",
                "UserRole": {"Name": "SC BDR"},
                "Manager": {"Name": "Pat Manager"},
            }
        ]
        leads = [
            {
                "Id": "00QA",
                "Name": "Lead A",
                "Company": "Acme",
                "Status": "Open",
                "CreatedDate": "2025-01-01T00:00:00Z",
                "Owner": {"Name": "Alex BDR"},
                "CreatedBy": {"Name": "Alex BDR"},
                "IsConverted": False,
                "ConvertedDate": None,
                "LeadSource": "Web",
                "Dimension_Persona__c": "CIO",
                "Industry": "Asset Manager",
            }
        ]
        tasks = [
            {
                "Id": "00TA",
                "CreatedDate": "2025-01-01T03:00:00Z",
                "WhoId": "00QA",
                "WhatId": "",
            }
        ]
        events: list[dict[str, str]] = []
        return users, leads, tasks, events

    monkeypatch.setattr(module, "fetch_live_rows", fake_fetch_live_rows)

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["bdr_count"] == 1
    assert result["summary"]["lead_count"] == 1
    assert result["summary"]["direct_touch_24h_count"] == 1
    assert result["summary"]["associated_touch_24h_count"] == 1
    assert result["summary"]["lead_row_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
