from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_bdr_response_integrity.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_bdr_response_integrity_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_users = [module.BdrUser(id="005TEST", name="Alex BDR", manager="Pat Manager")]
    fake_leads = [
        {
            "Id": "00QTEST1",
            "Owner": {"Name": "Alex BDR"},
            "CreatedDate": "2025-01-01T00:00:00Z",
            "Company": "Acme",
            "Status": "Open",
            "ConvertedContactId": "",
            "ConvertedAccountId": "",
        }
    ]
    fake_tasks = [
        {
            "OwnerId": "005TEST",
            "CreatedDate": "2025-01-01T06:00:00Z",
            "WhoId": "00QTEST1",
            "WhatId": "",
            "Subject": "Call",
        }
    ]
    fake_events: list[dict[str, str]] = []
    queries: list[str] = []

    def fake_run_sf(query: str):
        queries.append(query)
        if "FROM Lead " in query:
            return fake_leads
        if "FROM Task " in query:
            return fake_tasks
        if "FROM Event " in query:
            return fake_events
        return []

    monkeypatch.setattr(module, "get_na_bdr_users", lambda: fake_users)
    monkeypatch.setattr(module, "run_sf", fake_run_sf)

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["bdr_count"] == 1
    assert result["summary"]["lead_count"] == 1
    assert result["summary"]["lead_touch_24h"] == 1
    assert result["summary"]["lead_touch_any"] == 1
    assert result["summary"]["missing_direct_touch_examples"] == 0
    assert any("FROM Lead " in query for query in queries)
    assert any("FROM Task " in query for query in queries)
    assert any("FROM Event " in query for query in queries)
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
