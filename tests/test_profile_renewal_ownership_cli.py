from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_renewal_ownership.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_renewal_ownership_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_profile = {
        "by_manager": {"Pat Manager": {"won_acv_sum": 100.0}},
        "by_owner": {"Alex Owner": {"won_acv_sum": 80.0}},
        "annual_manager": {"2025": {"Pat Manager": {"won_acv_sum": 100.0}}},
    }

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))
    monkeypatch.setattr(module, "run_soql", lambda inst, tok, query: [])
    monkeypatch.setattr(module, "summarize", lambda rows: fake_profile)
    monkeypatch.setattr(module, "render_markdown", lambda profile: "# Renewal Ownership Profile\n")

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["manager_count"] == 1
    assert result["summary"]["owner_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
