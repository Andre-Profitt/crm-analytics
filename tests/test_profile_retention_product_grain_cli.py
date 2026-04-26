from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_retention_product_grain.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_retention_product_grain_test",
        MODULE_PATH,
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_run_profile_command_writes_artifacts(tmp_path: Path, monkeypatch) -> None:
    module = load_module()

    fake_opp_summary = {
        "by_type": {
            "Renewal": {
                "total": 3,
                "both": 2,
                "quote_only": 1,
                "opp_only": 0,
                "neither": 0,
                "both_pct": 2 / 3,
                "quote_only_pct": 1 / 3,
                "opp_only_pct": 0.0,
                "neither_pct": 0.0,
            },
            "Expand": {
                "total": 2,
                "both": 0,
                "quote_only": 1,
                "opp_only": 1,
                "neither": 0,
                "both_pct": 0.0,
                "quote_only_pct": 0.5,
                "opp_only_pct": 0.5,
                "neither_pct": 0.0,
            },
        },
        "by_state": {},
    }
    fake_line_summary = {
        "Renewal": {
            "line_count": 10,
            "with_product_area": 9,
            "with_product_area_pct": 0.9,
            "with_start": 10,
            "with_start_pct": 1.0,
            "with_end": 10,
            "with_end_pct": 1.0,
        }
    }
    fake_account_summary = {"AllianceBernstein L.P.": {"opp_count": 2}}
    fake_recommendation = module.GrainRecommendation(
        account_level_role="Account truth",
        opportunity_level_role="Opportunity classification",
        quote_level_role="Quote evidence",
        primary_grain="Account-year / account-timeline",
        model_shape="account narrative + opportunity event classification + quote-assisted product evidence",
    )

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))
    monkeypatch.setattr(module, "run_soql", lambda inst, tok, query: [])
    monkeypatch.setattr(module, "summarize_opp_coverage", lambda rows: fake_opp_summary)
    monkeypatch.setattr(module, "summarize_line_coverage", lambda rows: fake_line_summary)
    monkeypatch.setattr(module, "summarize_representative_accounts", lambda opp_rows, line_rows: fake_account_summary)
    monkeypatch.setattr(module, "build_recommendation", lambda opp_summary, line_summary: fake_recommendation)
    monkeypatch.setattr(
        module,
        "render_markdown",
        lambda opp_summary, line_summary, account_summary, recommendation: "# Retention Product Grain Recommendation\n",
    )

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["primary_grain"] == "Account-year / account-timeline"
    assert result["summary"]["representative_account_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
