from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_role_structure.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_role_structure_test",
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
        "active_user_type_counts": {"Standard": 12},
        "internal_slice_count": 10,
        "persona_counts": {"Sales": 4, "CX": 3, "Services": 3},
        "region_counts": {"EMEA": 6, "North America": 4},
        "top_user_roles": [["SC Sales", 4]],
        "top_divisions": [["Commercial Management", 5]],
        "top_opportunity_owners": [],
        "top_account_owners": [],
        "opportunity_persona_by_type": {"Land": {"Sales": 4}},
        "duplicate_names": [{"Name": "Alex Smith", "Count": 2}],
        "mismatch_examples": [{"Name": "Jamie Doe"}],
        "manager_rollup": [],
    }

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))
    monkeypatch.setattr(module, "run_soql", lambda inst, tok, query: [])
    monkeypatch.setattr(
        module,
        "build_profile",
        lambda users, opp_owner_rows, account_owner_rows, user_type_rows, opp_owner_type_rows: fake_profile,
    )
    monkeypatch.setattr(module, "render_markdown", lambda profile: "# Role Structure Profile\n")

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["internal_slice_count"] == 10
    assert result["summary"]["duplicate_name_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
