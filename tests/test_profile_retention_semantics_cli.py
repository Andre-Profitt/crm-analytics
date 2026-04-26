from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_retention_semantics.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_retention_semantics_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_report = {
        "row_count": 25,
        "analysis_start_year": 2024,
        "field_quality": {},
        "customer_closed_won": {
            "Expand": {"count": 4, "arr": 100.0, "renewal_acv": 0.0, "amount": 100.0},
            "Renewal": {"count": 3, "arr": 0.0, "renewal_acv": 80.0, "amount": 90.0},
        },
        "expand_customer": {
            "subtype": {},
            "quote_type": {},
            "renewalish_name_slice": {"count": 1, "arr": 20.0},
        },
        "renewal_amount_ratios": {"median_amount_to_renewal_acv": 1.1},
        "potential_protection_matches": {
            "renewal_losses_with_match_count": 2,
            "renewal_value_with_match": 50.0,
            "candidate_arr_on_best_matches": 60.0,
            "confidence_counts": {"high": 1, "medium": 1},
            "sample_matches": [],
        },
    }

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))
    monkeypatch.setattr(module, "run_soql", lambda inst, tok, query: [])
    monkeypatch.setattr(module, "summarize_field_quality", lambda rows: fake_report["field_quality"])
    monkeypatch.setattr(module, "summarize_customer_closed_won", lambda rows: fake_report["customer_closed_won"])
    monkeypatch.setattr(module, "summarize_expand_customer", lambda rows: fake_report["expand_customer"])
    monkeypatch.setattr(module, "renewal_amount_ratios", lambda rows: fake_report["renewal_amount_ratios"])
    monkeypatch.setattr(module, "build_candidate_matches", lambda rows: [])
    monkeypatch.setattr(module, "summarize_matches", lambda matches: fake_report["potential_protection_matches"])
    monkeypatch.setattr(
        module,
        "write_markdown",
        lambda report, output_path: output_path.write_text("# Retention Semantics Profile\n", encoding="utf-8"),
    )

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["row_count"] == 0
    assert result["summary"]["renewal_loss_match_count"] == 2
    assert result["summary"]["expand_customer_count"] == 4
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
