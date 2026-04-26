from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_retention_owner_validation.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_retention_owner_validation_test",
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
        "owners": {
            "Stefan Persson": {"by_year": {"2025": {}}, "by_account": {"Account A": {}}},
            "Jesper Aagaard": {"by_year": {"2025": {}}, "by_account": {"Account B": {}, "Account C": {}}},
        }
    }

    monkeypatch.setattr(module, "fetch_rows", lambda owners: [])
    monkeypatch.setattr(module, "build_profile", lambda rows, owners: fake_profile)
    monkeypatch.setattr(module, "render_markdown", lambda profile: "# Retention Owner Validation\n")

    result, exit_code = module.run_profile_command(
        ("Stefan Persson", "Jesper Aagaard"),
        tmp_path / "profile",
        emit_text=False,
    )

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["owner_count"] == 2
    assert result["summary"]["account_story_count"] == 3
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
