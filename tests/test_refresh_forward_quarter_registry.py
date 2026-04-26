from __future__ import annotations

import json
import sys
from pathlib import Path

from scripts import refresh_forward_quarter_registry as refresh_script


def test_build_refresh_plan_promotes_discovered_sources() -> None:
    territory_config = {
        "territories": {
            "APAC": {"director": "Jesper Tyrer"},
            "Canada": {"director": "Megan Miceli"},
        }
    }
    audit_payload = {
        "run_date": "2026-08-10",
        "candidate_forward_quarter": {
            "quarter_label": "Q4",
            "quarter_title": "Q4 2026",
            "pi_list_views": [
                {
                    "territory": "APAC",
                    "list_view_id": "00B-q4-apac",
                    "list_view_label": "PI ARR Forecast APAC Q4 2026 Land",
                    "status": "ok",
                    "source_origin": "discovered",
                }
            ],
            "historical_reports": [
                {
                    "director_slug": "APAC",
                    "report_id": "00OT-q4-apac",
                    "status": "ok",
                    "source_origin": "discovered",
                }
            ],
        },
    }

    payload = refresh_script.build_refresh_plan(territory_config, audit_payload)

    assert payload["promoted_count"] == 2
    assert payload["updated_config"]["territories"]["APAC"][
        "forward_quarter_pi_list_views"
    ]["Q4"] == {
        "list_view_id": "00B-q4-apac",
        "list_view_label": "PI ARR Forecast APAC Q4 2026 Land",
    }
    assert payload["updated_config"]["territories"]["APAC"][
        "forward_quarter_historical_trending_report_ids"
    ]["Q4"] == "00OT-q4-apac"


def test_build_refresh_plan_detects_conflict_without_overwriting() -> None:
    territory_config = {
        "territories": {
            "APAC": {
                "forward_quarter_historical_trending_report_ids": {
                    "Q4": "00OT-existing"
                }
            }
        }
    }
    audit_payload = {
        "run_date": "2026-08-10",
        "candidate_forward_quarter": {
            "quarter_label": "Q4",
            "quarter_title": "Q4 2026",
            "pi_list_views": [],
            "historical_reports": [
                {
                    "director_slug": "APAC",
                    "report_id": "00OT-discovered",
                    "status": "ok",
                    "source_origin": "discovered",
                }
            ],
        },
    }

    payload = refresh_script.build_refresh_plan(territory_config, audit_payload)

    assert payload["promoted_count"] == 0
    assert payload["conflict_count"] == 1
    assert payload["updated_config"]["territories"]["APAC"][
        "forward_quarter_historical_trending_report_ids"
    ]["Q4"] == "00OT-existing"


def test_main_writes_registry_refresh_artifacts(
    tmp_path: Path, monkeypatch
) -> None:
    config_path = tmp_path / "config" / "sd_monthly_territories.json"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        json.dumps({"territories": {"APAC": {"director": "Jesper Tyrer"}}}),
        encoding="utf-8",
    )
    audit_path = (
        tmp_path
        / "output"
        / "source_contract_audit"
        / "2026-08-10"
        / "source_contract_audit.json"
    )
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    audit_path.write_text(
        json.dumps(
            {
                "run_date": "2026-08-10",
                "candidate_forward_quarter": {
                    "quarter_label": "Q4",
                    "quarter_title": "Q4 2026",
                    "pi_list_views": [
                        {
                            "territory": "APAC",
                            "list_view_id": "00B-q4-apac",
                            "list_view_label": "PI ARR Forecast APAC Q4 2026 Land",
                            "status": "ok",
                            "source_origin": "discovered",
                        }
                    ],
                    "historical_reports": [],
                },
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(refresh_script, "TERRITORY_CONFIG_PATH", config_path)
    monkeypatch.setattr(
        refresh_script,
        "SOURCE_CONTRACT_AUDIT_ROOT",
        tmp_path / "output" / "source_contract_audit",
    )
    monkeypatch.setattr(
        refresh_script,
        "OUTPUT_ROOT",
        tmp_path / "output" / "source_contract_registry_refresh",
    )
    monkeypatch.setattr(
        sys,
        "argv",
        ["refresh_forward_quarter_registry.py", "--date", "2026-08-10"],
    )

    assert refresh_script.main() == 0
    output_dir = tmp_path / "output" / "source_contract_registry_refresh" / "2026-08-10"
    assert (output_dir / "registry_refresh.json").exists()
    assert (output_dir / "summary.md").exists()
    proposed = json.loads(
        (output_dir / "proposed_sd_monthly_territories.json").read_text(encoding="utf-8")
    )
    assert proposed["territories"]["APAC"]["forward_quarter_pi_list_views"]["Q4"][
        "list_view_id"
    ] == "00B-q4-apac"
