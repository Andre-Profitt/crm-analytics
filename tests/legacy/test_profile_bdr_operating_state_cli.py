from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_bdr_operating_state.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_bdr_operating_state_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_users = [
        module.BdrUser(
            id="005TEST",
            name="Alex BDR",
            title="Business Development Representative",
            department="North America",
            role="SC BDR",
            manager="Pat Manager",
        )
    ]
    fake_metrics = {"Alex BDR": {"leads": 4, "converted": 1, "converted_with_opp": 0}}
    fake_tools = {
        "org_permission_set_licenses": [],
        "org_permission_sets": [],
        "user_permission_sets": {},
        "user_permission_set_licenses": {},
        "cadence_and_engagement_entities": [],
        "sales_work_queue_settings_count": 1,
    }
    fake_campaigns = {"recent_na_campaigns": [{"Name": "NA Q1 Outreach"}], "recent_campaigns": []}

    monkeypatch.setattr(module, "get_na_bdr_users", lambda: fake_users)
    monkeypatch.setattr(module, "profile_owner_metrics", lambda users: fake_metrics)
    monkeypatch.setattr(module, "get_org_tool_access", lambda users: fake_tools)
    monkeypatch.setattr(module, "get_campaign_snapshot", lambda: fake_campaigns)
    monkeypatch.setattr(
        module,
        "write_markdown",
        lambda payload, output_path: output_path.write_text("# North America BDR Operating State\n", encoding="utf-8"),
    )

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["bdr_count"] == 1
    assert result["summary"]["recent_na_campaign_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
