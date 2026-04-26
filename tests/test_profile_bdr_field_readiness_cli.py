from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_bdr_field_readiness.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_bdr_field_readiness_test",
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
    fake_payload = {
        "lead_count": 12,
        "coverage": {
            "persona": {"field": "Dimension_Persona__c", "populated": 12, "total": 12, "coverage_pct": 100.0},
            "industry": {"field": "Industry", "populated": 10, "total": 12, "coverage_pct": 83.3},
        },
        "campaign_summary": [{"name": "NA Q1 Campaign", "members": 5, "responses": 2}],
        "matched_account_samples": [{"account_name": "Acme", "region": "NA"}],
    }

    monkeypatch.setattr(module, "get_na_bdr_users", lambda: fake_users)
    monkeypatch.setattr(module, "profile_fields", lambda users: fake_payload)
    monkeypatch.setattr(
        module,
        "write_markdown",
        lambda users, payload, output_path: output_path.write_text("# BDR Field Readiness\n", encoding="utf-8"),
    )

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["bdr_count"] == 1
    assert result["summary"]["lead_count"] == 12
    assert result["summary"]["coverage_dimension_count"] == 2
    assert result["summary"]["campaign_summary_count"] == 1
    assert result["summary"]["matched_account_sample_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
