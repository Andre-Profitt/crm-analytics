from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_bdr_quote_product_signals.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_bdr_quote_product_signals_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    users = [
        {
            "Id": "005A",
            "Department": "North America Sales",
        }
    ]
    accounts = [
        {
            "Id": "001A",
            "OwnerId": "005A",
            "Name": "Acme",
            "Industry": "Asset Manager",
            "Product_Opportunity__c": "",
        }
    ]
    opps = [
        {
            "Id": "006A",
            "AccountId": "001A",
            "APTS_RH_Product_Family__c": "",
            "Stage_with_Product_Scope__c": "",
        }
    ]
    line_items = [
        {
            "Apttus_Proposal__Proposal__r": {
                "Apttus_Proposal__Opportunity__c": "006A",
                "Apttus_Proposal__Opportunity__r": {"AccountId": "001A"},
            },
            "APTS_Product_Area__c": "SimCorp SaaS",
            "APTS_Strategic_Product__c": "Analytics Services",
            "Apttus_Proposal__Product__r": {"Name": "SimCorp SaaS Platform"},
        }
    ]

    calls: list[str] = []

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))

    def fake_soql(inst: str, tok: str, query: str):
        calls.append(query)
        if query == module.BDR_USER_SOQL:
            return users
        if query == module.build_account_query(["005A"]):
            return accounts
        if query == module.build_opportunity_query(["001A"]):
            return opps
        if "FROM Apttus_Proposal__Proposal_Line_Item__c" in query:
            return line_items
        raise AssertionError(query)

    monkeypatch.setattr(module, "_soql", fake_soql)

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["amers_bdr_user_count"] == 1
    assert result["summary"]["amers_account_count"] == 1
    assert result["summary"]["amers_opportunity_count"] == 1
    assert result["summary"]["quote_line_item_count"] == 1
    assert result["summary"]["quote_area_only_account_count"] == 1
    assert any("FROM Apttus_Proposal__Proposal_Line_Item__c" in query for query in calls)
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
