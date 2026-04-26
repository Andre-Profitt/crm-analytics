from __future__ import annotations

import importlib.util
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "profile_renewal_outcomes.py"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "profile_renewal_outcomes_test",
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
        "by_year": {
            "2025": {
                "total_count": 3,
                "total_acv_sum": 150.0,
                "total_amount_sum": 200.0,
                "outcomes": {
                    "won": {
                        "count": 1,
                        "acv_nonzero": 1,
                        "acv_sum": 100.0,
                        "amount_nonzero": 1,
                        "amount_sum": 120.0,
                    }
                },
            }
        },
        "annual_rollup": {
            "2025": {
                "total_count": 3,
                "closed_count": 2,
                "won_count": 1,
                "lost_count": 1,
                "no_opportunity_count": 0,
                "open_count": 1,
                "closed_acv_coverage_pct": 1.0,
                "won_acv_sum": 100.0,
                "at_risk_acv_sum": 50.0,
            }
        },
    }

    monkeypatch.setattr(module, "get_auth", lambda: ("inst", "tok"))
    monkeypatch.setattr(module, "run_soql", lambda inst, tok, query: [])
    monkeypatch.setattr(module, "summarize", lambda rows: dict(fake_profile))
    monkeypatch.setattr(
        module,
        "top_examples",
        lambda rows, outcomes, limit=10: [{"close_date": "2025-01-01", "outcome": next(iter(outcomes))}],
    )
    monkeypatch.setattr(module, "render_markdown", lambda profile: "# Renewal Outcome Profile\n")

    result, exit_code = module.run_profile_command(tmp_path / "profile", emit_text=False)

    assert exit_code == 0
    assert result["status"] == "ok"
    assert result["lane"] == "salesforce_data_profiles"
    assert result["command_class"] == "live_read"
    assert result["summary"]["year_count"] == 1
    assert result["summary"]["closed_year_count"] == 1
    assert result["summary"]["top_year"] == "2025"
    assert result["summary"]["won_example_count"] == 1
    assert result["summary"]["at_risk_example_count"] == 1
    assert (tmp_path / "profile" / "profile.json").exists()
    assert (tmp_path / "profile" / "profile.md").exists()
    assert any(artifact["path"].endswith("profile.json") for artifact in result["artifacts"])
    assert any(artifact["path"].endswith("profile.md") for artifact in result["artifacts"])
