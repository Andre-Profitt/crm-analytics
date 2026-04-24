import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_monthly_source_contract import build_manifest, markdown_summary  # noqa: E402


def _write_config(path: Path) -> None:
    path.write_text(
        json.dumps(
            {
                "territories": {
                    "APAC": {
                        "director": "Jesper Tyrer",
                        "soql_where": "Account_Unit_Group__c = 'SC Asia'",
                        "pi_list_view_id": "00B-current-apac",
                        "pi_list_view_label": "PI APAC",
                        "forward_quarter_pi_list_views": {
                            "Q3": {
                                "list_view_id": "00B-forward-apac",
                                "list_view_label": "PI APAC Q3",
                            }
                        },
                        "historical_trending_report_ids": {
                            "Q1": "00O-apac-q1",
                            "Q2": "00O-apac-q2",
                        },
                        "forward_quarter_historical_trending_report_ids": {
                            "Q3": "00O-apac-q3"
                        },
                    },
                    "Central Europe": {
                        "director": "Sarah Pittroff",
                        "soql_where": "Sales_Region__c = 'Central Europe'",
                        "pi_list_view_id": "00B-current-ce",
                        "pi_list_view_label": "PI CE",
                        "forward_quarter_pi_list_views": {
                            "Q3": {
                                "list_view_id": "00B-forward-ce",
                                "list_view_label": "PI CE Q3",
                            }
                        },
                        "historical_trending_report_ids": {
                            "Q1": "00O-ce-q1",
                            "Q2": "00O-ce-q2",
                        },
                        "forward_quarter_historical_trending_report_ids": {
                            "Q3": "00O-ce-q3"
                        },
                    },
                }
            }
        )
        + "\n",
        encoding="utf-8",
    )


def _write_bundle(path: Path, *, current_deals: int, forward_deals: int) -> None:
    rows = []
    for idx in range(current_deals):
        rows.append(
            {
                "opportunity": f"Current {idx}",
                "deal_type": "Land",
                "forecast_category": "Commit",
                "close_date": "2026-05-15",
                "arr_unweighted": 100_000,
            }
        )
    for idx in range(forward_deals):
        rows.append(
            {
                "opportunity": f"Forward {idx}",
                "deal_type": "Land",
                "forecast_category": "Best Case",
                "close_date": "2026-08-15",
                "arr_unweighted": 250_000,
            }
        )
    rows.append(
        {
            "opportunity": "Omitted Current",
            "deal_type": "Land",
            "forecast_category": "Omitted",
            "close_date": "2026-05-31",
            "arr_unweighted": 999_999,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "snapshot_date": "2026-04-30",
                "datasets": {"pipeline_open": rows},
            }
        )
        + "\n",
        encoding="utf-8",
    )


def test_monthly_source_contract_resolves_sources_and_forward_fallback(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "sd_monthly_territories.json"
    bundle_dir = tmp_path / "bundles"
    _write_config(config_path)
    _write_bundle(bundle_dir / "jesper-tyrer.json", current_deals=0, forward_deals=2)
    _write_bundle(bundle_dir / "sarah-pittroff.json", current_deals=1, forward_deals=3)

    manifest = build_manifest(
        snapshot_date="2026-04-30",
        territory_config_path=config_path,
        bundle_dir=bundle_dir,
        require_bundles=True,
    )

    assert manifest["status"] == "ok"
    assert manifest["period"]["current_quarter"]["title"] == "Q2 2026"
    assert manifest["period"]["forward_quarter"]["title"] == "Q3 2026"
    assert manifest["period"]["quarter_policy"]["name"] == "calendar_quarter"
    assert manifest["quarter_policy"]["name"] == "calendar_quarter"
    assert manifest["quarter_policy"] == manifest["period"]["quarter_policy"]
    assert manifest["summary"]["historical_report_count"] == 6
    assert manifest["summary"]["forward_fallback_count"] == 1

    apac = next(row for row in manifest["territories"] if row["territory"] == "APAC")
    assert apac["pipeline_display_decision"]["display_reason"] == "forward_quarter_fallback"
    assert apac["pipeline_display_decision"]["current_quarter_active_deals"] == 0
    assert apac["pipeline_display_decision"]["forward_quarter_active_deals"] == 2
    assert [
        source["role"] for source in apac["sources"]["historical_trending"]
    ] == ["prior_quarter", "current_quarter", "forward_quarter"]
    assert markdown_summary(manifest).startswith("# Monthly Source Contract")


def test_monthly_source_contract_accepts_territory_slugged_source_bundles(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "sd_monthly_territories.json"
    bundle_dir = tmp_path / "bundles"
    _write_config(config_path)
    _write_bundle(bundle_dir / "apac.json", current_deals=0, forward_deals=2)
    _write_bundle(bundle_dir / "central-europe.json", current_deals=1, forward_deals=3)

    manifest = build_manifest(
        snapshot_date="2026-04-30",
        territory_config_path=config_path,
        bundle_dir=bundle_dir,
        require_bundles=True,
    )

    assert manifest["status"] == "ok"
    assert manifest["summary"]["missing_bundle_count"] == 0
    assert manifest["summary"]["warning_finding_count"] == 0
    apac = next(row for row in manifest["territories"] if row["territory"] == "APAC")
    assert apac["pipeline_display_decision"]["display_reason"] == "forward_quarter_fallback"


def test_monthly_source_contract_blocks_when_required_bundle_missing(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "sd_monthly_territories.json"
    _write_config(config_path)

    manifest = build_manifest(
        snapshot_date="2026-04-30",
        territory_config_path=config_path,
        bundle_dir=tmp_path / "missing-bundles",
        require_bundles=True,
    )

    assert manifest["status"] == "blocked"
    assert manifest["summary"]["missing_bundle_count"] == 2
    assert {
        finding["issue"] for finding in manifest["findings"]
    } >= {"bundle_missing_for_quarter_decision"}


def test_monthly_source_contract_folds_in_source_audit_failures(
    tmp_path: Path,
) -> None:
    config_path = tmp_path / "sd_monthly_territories.json"
    bundle_dir = tmp_path / "bundles"
    audit_path = tmp_path / "source_contract_audit.json"
    _write_config(config_path)
    _write_bundle(bundle_dir / "jesper-tyrer.json", current_deals=1, forward_deals=0)
    _write_bundle(bundle_dir / "sarah-pittroff.json", current_deals=1, forward_deals=0)
    audit_path.write_text(
        json.dumps(
            {
                "run_date": "2026-04-30",
                "active_lane": {
                    "historical_reports": [
                        {
                            "director_slug": "jesper-tyrer",
                            "quarter_label": "Q2",
                            "report_id": "00O-apac-q2",
                            "status": "failed",
                            "issues": ["standard_date_filter_mismatch"],
                        }
                    ],
                    "pi_list_views": [],
                },
                "candidate_forward_quarter": {
                    "historical_reports": [],
                    "pi_list_views": [],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    manifest = build_manifest(
        snapshot_date="2026-04-30",
        territory_config_path=config_path,
        bundle_dir=bundle_dir,
        source_audit_path=audit_path,
    )

    assert manifest["status"] == "blocked"
    assert manifest["summary"]["source_probe_issue_count"] == 1
    assert any(
        finding["issue"] == "historical_report_probe_failed"
        for finding in manifest["findings"]
    )
