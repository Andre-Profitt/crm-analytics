from __future__ import annotations

import csv
import json
from pathlib import Path
import subprocess
import sys

from scripts.run_sales_director_monthly_report import (
    build_latest_status_markdown,
    build_latest_status_packet,
    build_commentary_owner_packet_bundle,
    build_commentary_owner_rollup,
    build_commentary_owner_send_list_rows,
    build_finance_churn_request_csv_rows,
    build_finance_churn_request_pack,
    build_internal_review_packet,
    build_internal_review_packet_markdown,
    build_publish_checklist,
    build_publish_checklist_markdown,
    build_run_summary,
    prepare_overlay_input,
    write_latest_aliases,
)


ROOT = Path(__file__).resolve().parents[1]
MERGE_SCRIPT = ROOT / "scripts" / "merge_sales_director_overlay.py"


def run_merge(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(MERGE_SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_merge_sales_director_overlay_promotes_commentary_csv(tmp_path: Path) -> None:
    base_overlay = tmp_path / "report1_overlay.fill.json"
    base_overlay.write_text(
        json.dumps(
            {
                "finance_churn": {
                    "status": "pending",
                    "provenance": "pending",
                    "owner": "",
                    "source_name": "",
                    "headline": "",
                    "summary_note": "",
                    "top_accounts": [],
                },
                "slipped_commentary": {
                    "status": "pending",
                    "provenance": "external",
                    "summary_note": "",
                    "root_cause_bullets": [],
                    "owner_comments": [],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    commentary_csv = tmp_path / "owner_commentary.csv"
    with commentary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "priority",
                "region",
                "account_name",
                "opportunity_name",
                "owner_name",
                "stage_name",
                "forecast_category",
                "weighted_open_arr",
                "push_count",
                "days_in_stage",
                "theme",
                "comment",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "priority": "1",
                "region": "North America",
                "account_name": "Example Account",
                "opportunity_name": "Example Opportunity",
                "owner_name": "Example Owner",
                "stage_name": "3 - Engagement",
                "forecast_category": "Pipeline",
                "weighted_open_arr": "1000000",
                "push_count": "3",
                "days_in_stage": "120",
                "theme": "Procurement delay",
                "comment": "Customer legal review pushed signature into next month.",
            }
        )

    output_path = tmp_path / "overlay.owner_commentary.json"
    result = run_merge(
        "--base-overlay",
        str(base_overlay),
        "--commentary-csv",
        str(commentary_csv),
        "--output",
        str(output_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["owner_comment_count"] == 1

    overlay = json.loads(output_path.read_text(encoding="utf-8"))
    slipped = overlay["slipped_commentary"]
    assert slipped["status"] == "provided"
    assert slipped["provenance"] == "external"
    assert slipped["coverage_status"] == "complete"
    assert slipped["requested_item_count"] == 1
    assert slipped["pending_comment_count"] == 0
    assert slipped["owner_comments"][0]["theme"] == "Procurement delay"
    assert slipped["root_cause_bullets"] == ["Procurement delay appears in 1 owner update."]


def test_merge_sales_director_overlay_promotes_finance_csv(tmp_path: Path) -> None:
    base_overlay = tmp_path / "report1_overlay.fill.json"
    base_overlay.write_text(
        json.dumps(
            {
                "finance_churn": {
                    "status": "pending",
                    "provenance": "pending",
                    "owner": "",
                    "source_name": "",
                    "headline": "",
                    "summary_note": "",
                    "top_accounts": [],
                },
                "slipped_commentary": {
                    "status": "pending",
                    "provenance": "pending",
                    "summary_note": "",
                    "root_cause_bullets": [],
                    "owner_comments": [],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    finance_csv = tmp_path / "finance_overlay.csv"
    with finance_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "priority",
                "account_name",
                "opportunity_name",
                "owner_name",
                "historical_quarter",
                "historical_outcome",
                "historical_stage",
                "historical_churn_acv",
                "historical_amount",
                "overlay_owner",
                "overlay_source_name",
                "overlay_headline",
                "overlay_summary_note",
                "include_in_forward_risk",
                "region",
                "signal",
                "amount",
                "note",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "priority": "1",
                "account_name": "Account A",
                "opportunity_name": "Renewal A",
                "owner_name": "Owner A",
                "historical_quarter": "2026-Q1",
                "historical_outcome": "Churned",
                "historical_stage": "0 - Lost",
                "historical_churn_acv": "100",
                "historical_amount": "300",
                "overlay_owner": "Finance Partner",
                "overlay_source_name": "Finance Renewal Risk Pack",
                "overlay_headline": "Finance sees concentrated risk in two live renewals.",
                "overlay_summary_note": "Example Finance file for proofing.",
                "include_in_forward_risk": "yes",
                "region": "EMEA",
                "signal": "Delayed procurement",
                "amount": "4200000",
                "note": "",
            }
        )
        writer.writerow(
            {
                "priority": "2",
                "account_name": "Account B",
                "opportunity_name": "Renewal B",
                "owner_name": "Owner B",
                "historical_quarter": "2025-Q4",
                "historical_outcome": "Lost",
                "historical_stage": "0 - No Opportunity",
                "historical_churn_acv": "80",
                "historical_amount": "210",
                "overlay_owner": "",
                "overlay_source_name": "",
                "overlay_headline": "",
                "overlay_summary_note": "",
                "include_in_forward_risk": "true",
                "region": "North America",
                "signal": "Budget approval risk",
                "amount": "3100000",
                "note": "",
            }
        )

    output_path = tmp_path / "overlay.finance.json"
    result = run_merge(
        "--base-overlay",
        str(base_overlay),
        "--finance-csv",
        str(finance_csv),
        "--json",
        "--output",
        str(output_path),
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["finance_top_account_count"] == 2
    assert payload["finance_status"] == "provided"

    overlay = json.loads(output_path.read_text(encoding="utf-8"))
    finance = overlay["finance_churn"]
    assert finance["status"] == "provided"
    assert finance["provenance"] == "external"
    assert finance["owner"] == "Finance Partner"
    assert finance["source_name"] == "Finance Renewal Risk Pack"
    assert len(finance["top_accounts"]) == 2
    assert finance["top_accounts"][0]["amount"] == 4200000


def test_build_publish_checklist_blocks_only_missing_finance_when_commentary_is_real() -> None:
    snapshot = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "quarterly_pipeline_display": {
            "display_quarter": {
                "reason": "forward_quarter_fallback",
                "title": "Q2 2026",
                "footnote": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
                "active_deal_count": 3,
            }
        },
        "external_inputs": {
            "finance_churn": {"status": "pending", "provenance": "pending"},
            "slipped_commentary": {
                "status": "provided",
                "provenance": "external",
                "coverage_status": "complete",
                "requested_item_count": 2,
                "provided_comment_count": 2,
                "pending_comment_count": 0,
            },
        },
        "commercial_approval": {
            "rule_contract": {
                "status": "aligned_with_report_target",
                "label": "Open current-quarter Land stage 3 deals without commercial approval",
            }
        },
        "renewals": {
            "metric_contract": {
                "status": "aligned_with_simcorp_methodology",
                "label": "Renewals and churn use Renewal ACV; land and expand pipeline stays in ARR",
            },
            "selection_scope": {
                "status": "aligned_with_report_target",
                "label": "Open renewals due by quarter end, including overdue renewals",
            }
        },
        "churn": {
            "metric_contract": {
                "status": "aligned_with_simcorp_methodology",
                "label": "Churn is measured on closed-lost renewals using Renewal ACV",
            }
        },
    }
    deck_summary = {"output": "/tmp/deck.pptx", "slide_count": 9}

    checklist = build_publish_checklist(
        snapshot=snapshot,
        deck_summary=deck_summary,
        thumbnail_path="/tmp/thumb.png",
        validation_summary={"status": "ok", "detail": "Render and overflow checks passed."},
    )

    assert checklist["internal_review_ready"] is True
    assert checklist["publish_ready"] is False
    assert checklist["blocked_item_count"] == 1

    checks = {item["name"]: item for item in checklist["checks"]}
    assert checks["Finance churn overlay publishable"]["status"] == "blocked"
    assert checks["Slipped commentary publishable"]["status"] == "pass"
    assert checks["Approval rule aligned to target"]["status"] == "pass"
    assert checks["Renewals scoped to quarter"]["status"] == "pass"
    assert checks["Renewal and churn methodology aligned"]["status"] == "pass"
    assert checks["Quarter fallback disclosed"]["status"] == "pass"
    assert checks["Rendered validation bundle generated"]["status"] == "pass"
    assert checklist["quarterly_pipeline_disclosure"]["display_reason"] == "forward_quarter_fallback"
    assert (
        checklist["quarterly_pipeline_disclosure"]["quarterly_pipeline_footnote"]
        == "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook."
    )

    markdown = build_publish_checklist_markdown(checklist=checklist)
    assert "## Quarter Disclosure" in markdown
    assert "Forward-quarter fallback: `Q2 2026`." in markdown


def test_build_publish_checklist_blocks_hidden_forward_quarter_fallback() -> None:
    snapshot = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "quarterly_pipeline_display": {
            "display_quarter": {
                "reason": "forward_quarter_fallback",
                "title": "Q2 2026",
                "footnote": "",
            }
        },
        "external_inputs": {
            "finance_churn": {"status": "provided", "provenance": "external"},
            "slipped_commentary": {
                "status": "provided",
                "provenance": "external",
                "coverage_status": "complete",
                "requested_item_count": 1,
                "provided_comment_count": 1,
                "pending_comment_count": 0,
            },
        },
        "commercial_approval": {
            "rule_contract": {
                "status": "aligned_with_report_target",
                "label": "Open current-quarter Land stage 3 deals without commercial approval",
            }
        },
        "renewals": {
            "metric_contract": {
                "status": "aligned_with_simcorp_methodology",
                "label": "Renewals and churn use Renewal ACV; land and expand pipeline stays in ARR",
            },
            "selection_scope": {
                "status": "aligned_with_report_target",
                "label": "Open renewals due by quarter end, including overdue renewals",
            },
        },
        "churn": {
            "metric_contract": {
                "status": "aligned_with_simcorp_methodology",
                "label": "Churn is measured on closed-lost renewals using Renewal ACV",
            }
        },
    }

    checklist = build_publish_checklist(
        snapshot=snapshot,
        deck_summary={"output": "/tmp/deck.pptx", "slide_count": 9},
        thumbnail_path="/tmp/thumb.png",
        validation_summary={"status": "ok", "detail": "Render and overflow checks passed."},
    )

    checks = {item["name"]: item for item in checklist["checks"]}
    assert checklist["publish_ready"] is False
    assert checks["Quarter fallback disclosed"]["status"] == "blocked"
    assert "missing an explicit footnote" in checks["Quarter fallback disclosed"]["detail"]


def test_prepare_overlay_input_builds_merged_overlay_from_commentary_only(tmp_path: Path) -> None:
    commentary_csv = tmp_path / "owner_commentary.sample.csv"
    with commentary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "priority",
                "region",
                "account_name",
                "opportunity_name",
                "owner_name",
                "stage_name",
                "forecast_category",
                "weighted_open_arr",
                "push_count",
                "days_in_stage",
                "theme",
                "comment",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "priority": "1",
                "region": "EMEA",
                "account_name": "Example Account",
                "opportunity_name": "Example Opportunity",
                "owner_name": "Example Owner",
                "stage_name": "4 - Shortlisted",
                "forecast_category": "Best Case",
                "weighted_open_arr": "2500000",
                "push_count": "2",
                "days_in_stage": "80",
                "theme": "Customer timing",
                "comment": "Budget committee moved the signature into next month.",
            }
        )

    overlay_prep = prepare_overlay_input(
        run_dir=tmp_path,
        overlay_json=None,
        commentary_csv=str(commentary_csv),
        finance_csv=None,
        base_overlay=None,
        commentary_summary_note="Owner commentary collected for slipped deals.",
        commentary_provenance="auto",
        finance_provenance="auto",
    )

    assert overlay_prep["commentary_csv_path"] == str(commentary_csv.resolve())
    assert overlay_prep["requested_overlay_path"] is None
    assert overlay_prep["base_overlay_path"] is None
    assert overlay_prep["owner_comment_count"] == 1
    assert overlay_prep["merged_overlay_path"] is not None

    merged_overlay = json.loads(Path(overlay_prep["merged_overlay_path"]).read_text(encoding="utf-8"))
    assert merged_overlay["finance_churn"]["status"] == "pending"
    assert merged_overlay["slipped_commentary"]["status"] == "provided"
    assert merged_overlay["slipped_commentary"]["provenance"] == "example"
    assert merged_overlay["slipped_commentary"]["coverage_status"] == "complete"
    assert merged_overlay["slipped_commentary"]["owner_comments"][0]["theme"] == "Customer timing"


def test_prepare_overlay_input_builds_merged_overlay_from_finance_only(tmp_path: Path) -> None:
    finance_csv = tmp_path / "finance_overlay.sample.csv"
    with finance_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "priority",
                "account_name",
                "opportunity_name",
                "owner_name",
                "historical_quarter",
                "historical_outcome",
                "historical_stage",
                "historical_churn_acv",
                "historical_amount",
                "overlay_owner",
                "overlay_source_name",
                "overlay_headline",
                "overlay_summary_note",
                "include_in_forward_risk",
                "region",
                "signal",
                "amount",
                "note",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "priority": "1",
                "account_name": "Example Account",
                "opportunity_name": "Example Renewal",
                "owner_name": "Finance Owner",
                "historical_quarter": "2026-Q1",
                "historical_outcome": "Churned",
                "historical_stage": "0 - Lost",
                "historical_churn_acv": "150",
                "historical_amount": "500",
                "overlay_owner": "Finance Partner",
                "overlay_source_name": "Finance Renewal Risk Pack",
                "overlay_headline": "Forward churn risk is concentrated in one account.",
                "overlay_summary_note": "Example Finance overlay for merge testing.",
                "include_in_forward_risk": "yes",
                "region": "APAC",
                "signal": "Scope reset",
                "amount": "1900000",
                "note": "",
            }
        )

    overlay_prep = prepare_overlay_input(
        run_dir=tmp_path,
        overlay_json=None,
        commentary_csv=None,
        finance_csv=str(finance_csv),
        base_overlay=None,
        commentary_summary_note="Owner commentary collected for slipped deals.",
        commentary_provenance="auto",
        finance_provenance="auto",
    )

    assert overlay_prep["finance_csv_path"] == str(finance_csv.resolve())
    assert overlay_prep["finance_top_account_count"] == 1
    assert overlay_prep["owner_comment_count"] == 0

    merged_overlay = json.loads(Path(overlay_prep["merged_overlay_path"]).read_text(encoding="utf-8"))
    assert merged_overlay["finance_churn"]["status"] == "provided"
    assert merged_overlay["finance_churn"]["provenance"] == "example"
    assert merged_overlay["finance_churn"]["top_accounts"][0]["signal"] == "Scope reset"
    assert merged_overlay["slipped_commentary"]["status"] == "pending"


def test_merge_sales_director_overlay_marks_partial_coverage_when_some_rows_are_blank(tmp_path: Path) -> None:
    base_overlay = tmp_path / "report1_overlay.fill.json"
    base_overlay.write_text(
        json.dumps(
            {
                "finance_churn": {"status": "pending", "provenance": "pending"},
                "slipped_commentary": {"status": "pending", "provenance": "external", "owner_comments": []},
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    commentary_csv = tmp_path / "owner_commentary.csv"
    with commentary_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "priority",
                "region",
                "account_name",
                "opportunity_name",
                "owner_name",
                "stage_name",
                "forecast_category",
                "weighted_open_arr",
                "push_count",
                "days_in_stage",
                "theme",
                "comment",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "priority": "1",
                "region": "North America",
                "account_name": "Account 1",
                "opportunity_name": "Deal 1",
                "owner_name": "Owner A",
                "stage_name": "3 - Engagement",
                "forecast_category": "Pipeline",
                "weighted_open_arr": "100",
                "push_count": "1",
                "days_in_stage": "10",
                "theme": "Budget hold",
                "comment": "Budget moved.",
            }
        )
        writer.writerow(
            {
                "priority": "2",
                "region": "EMEA",
                "account_name": "Account 2",
                "opportunity_name": "Deal 2",
                "owner_name": "Owner B",
                "stage_name": "4 - Shortlisted",
                "forecast_category": "Best Case",
                "weighted_open_arr": "200",
                "push_count": "2",
                "days_in_stage": "20",
                "theme": "",
                "comment": "",
            }
        )

    output_path = tmp_path / "overlay.partial.json"
    result = run_merge(
        "--base-overlay",
        str(base_overlay),
        "--commentary-csv",
        str(commentary_csv),
        "--output",
        str(output_path),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["coverage_status"] == "partial"
    assert payload["pending_comment_count"] == 1

    overlay = json.loads(output_path.read_text(encoding="utf-8"))
    slipped = overlay["slipped_commentary"]
    assert slipped["coverage_status"] == "partial"
    assert slipped["requested_item_count"] == 2
    assert slipped["provided_comment_count"] == 1
    assert slipped["pending_comment_count"] == 1
    assert slipped["pending_owner_names"] == ["Owner B"]


def test_build_publish_checklist_warns_when_validation_bundle_is_missing() -> None:
    snapshot = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "external_inputs": {
            "finance_churn": {"status": "pending", "provenance": "pending"},
            "slipped_commentary": {"status": "pending", "provenance": "pending"},
        },
        "commercial_approval": {"rule_contract": {"status": "aligned_with_report_target", "label": "Rule"}},
        "renewals": {
            "metric_contract": {"status": "aligned_with_simcorp_methodology", "label": "Renewal ACV"},
            "selection_scope": {"status": "aligned_with_report_target", "label": "Quarter scope"},
        },
        "churn": {
            "metric_contract": {"status": "aligned_with_simcorp_methodology", "label": "Renewal ACV"}
        },
    }
    deck_summary = {"output": "/tmp/deck.pptx", "slide_count": 9}

    checklist = build_publish_checklist(
        snapshot=snapshot,
        deck_summary=deck_summary,
        thumbnail_path=None,
        validation_summary={"status": "unavailable", "detail": "Validation helper not found."},
    )

    checks = {item["name"]: item for item in checklist["checks"]}
    assert checks["Quick Look thumbnail generated"]["status"] == "warn"
    assert checks["Rendered validation bundle generated"]["status"] == "warn"
    assert checks["PowerPoint-first review bundle generated"]["status"] == "warn"


def test_build_commentary_owner_rollup_groups_priority_items() -> None:
    pack = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "items": [
            {
                "owner_name": "Owner A",
                "region": "APAC",
                "opportunity_name": "Deal 1",
                "weighted_open_arr": 100.0,
            },
            {
                "owner_name": "Owner A",
                "region": "APAC",
                "opportunity_name": "Deal 2",
                "weighted_open_arr": 250.0,
            },
            {
                "owner_name": "Owner B",
                "region": "EMEA",
                "opportunity_name": "Deal 3",
                "weighted_open_arr": 150.0,
            },
        ],
    }

    rollup = build_commentary_owner_rollup(pack=pack)

    assert rollup["owner_count"] == 2
    assert rollup["owners"][0]["owner_name"] == "Owner A"
    assert rollup["owners"][0]["item_count"] == 2
    assert rollup["owners"][0]["weighted_open_arr_total"] == 350.0
    assert rollup["owners"][0]["regions"] == ["APAC"]
    assert rollup["owners"][0]["opportunities"] == ["Deal 1", "Deal 2"]


def test_build_commentary_owner_send_list_rows_flattens_rollup() -> None:
    rollup = {
        "owners": [
            {
                "owner_name": "Owner A",
                "item_count": 2,
                "weighted_open_arr_total": 350.0,
                "regions": ["APAC", "EMEA"],
                "opportunities": ["Deal 1", "Deal 2", "Deal 3"],
            }
        ]
    }
    packet_bundle = {
        "packets": [
            {
                "owner_name": "Owner A",
                "packet_markdown_path": "/tmp/owner-a.md",
            }
        ]
    }

    rows = build_commentary_owner_send_list_rows(rollup=rollup, packet_bundle=packet_bundle)

    assert rows == [
        {
            "owner_name": "Owner A",
            "item_count": 2,
            "weighted_open_arr_total": 350.0,
            "regions": "APAC, EMEA",
            "example_opportunities": "Deal 1, Deal 2, Deal 3",
            "response_status": "pending",
            "provided_comment_count": 0,
            "pending_comment_count": 2,
            "suggested_subject": "Input needed: slipped-deal commentary for Owner A",
            "owner_packet_markdown_path": "/tmp/owner-a.md",
        }
    ]


def test_build_commentary_owner_send_list_rows_marks_partial_response_status() -> None:
    rollup = {
        "owners": [
            {
                "owner_name": "Owner A",
                "item_count": 2,
                "weighted_open_arr_total": 350.0,
                "regions": ["APAC"],
                "opportunities": ["Deal 1", "Deal 2"],
            }
        ]
    }
    slipped_overlay = {
        "owner_comments": [
            {
                "owner_name": "Owner A",
                "opportunity_name": "Deal 1",
                "theme": "Procurement delay",
                "comment": "Legal review moved.",
            }
        ]
    }

    rows = build_commentary_owner_send_list_rows(
        rollup=rollup,
        packet_bundle=None,
        slipped_overlay=slipped_overlay,
    )

    assert rows[0]["response_status"] == "partial"
    assert rows[0]["provided_comment_count"] == 1
    assert rows[0]["pending_comment_count"] == 1


def test_build_commentary_owner_packet_bundle_writes_per_owner_packets(tmp_path: Path) -> None:
    pack = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "items": [
            {
                "priority": 1,
                "owner_name": "Owner A",
                "region": "APAC",
                "account_name": "Account 1",
                "opportunity_name": "Deal 1",
                "stage_name": "3 - Engagement",
                "forecast_category": "Best Case",
                "weighted_open_arr": 100.0,
                "push_count": 2,
                "days_in_stage": 30,
                "questions": ["Why did it slip?"],
            },
            {
                "priority": 2,
                "owner_name": "Owner B",
                "region": "EMEA",
                "account_name": "Account 2",
                "opportunity_name": "Deal 2",
                "stage_name": "4 - Shortlisted",
                "forecast_category": "Pipeline",
                "weighted_open_arr": 250.0,
                "push_count": 3,
                "days_in_stage": 45,
                "questions": ["What happens next?"],
            },
        ],
    }

    bundle = build_commentary_owner_packet_bundle(
        pack=pack,
        packet_dir=tmp_path / "owner_packets",
    )

    assert bundle["owner_count"] == 2
    packet_paths = [Path(packet["packet_markdown_path"]) for packet in bundle["packets"]]
    assert all(path.exists() for path in packet_paths)
    assert "Commentary Packet: Owner A" in packet_paths[0].read_text(encoding="utf-8")


def test_build_finance_churn_request_pack_prefills_historical_anchors() -> None:
    snapshot = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "external_inputs": {"finance_churn": {"status": "pending"}},
        "churn": {
            "finance_feed_status": "pending_external_finance_source",
            "top_owners": [
                {"owner_name": "Owner A", "churned_acv": 120.0, "churned_deals": 2},
                {"owner_name": "Owner B", "churned_acv": 80.0, "churned_deals": 1},
            ],
            "top_churned_renewals": [
                {
                    "account_name": "Account A",
                    "opportunity_name": "Renewal A",
                    "owner_name": "Owner A",
                    "quarter_label": "2026-Q1",
                    "outcome": "Churned",
                    "stage": "0 - Lost",
                    "renewal_acv": 100.0,
                    "amount": 300.0,
                },
                {
                    "account_name": "Account B",
                    "opportunity_name": "Renewal B",
                    "owner_name": "Owner B",
                    "quarter_label": "2025-Q4",
                    "outcome": "Lost",
                    "stage": "0 - No Opportunity",
                    "renewal_acv": 80.0,
                    "amount": 210.0,
                },
            ],
        },
    }

    pack = build_finance_churn_request_pack(snapshot=snapshot)

    assert pack["artifact_type"] == "sales_director_finance_churn_request_pack"
    assert pack["finance_feed_status"] == "pending_external_finance_source"
    assert pack["historical_anchor_count"] == 2
    assert pack["owner_concentration"][0]["owner_name"] == "Owner A"
    assert pack["items"][0]["account_name"] == "Account A"
    assert "current Finance forward-risk view" in pack["items"][0]["questions"][0]

    rows = build_finance_churn_request_csv_rows(pack=pack)
    assert rows[0]["account_name"] == "Account A"
    assert rows[0]["include_in_forward_risk"] == ""
    assert rows[0]["signal"] == ""


def test_build_internal_review_packet_surfaces_review_assets_and_blockers() -> None:
    manifest = {
        "run_dir": "/tmp/run",
        "deck_path": "/tmp/run/deck.pptx",
        "thumbnail_path": "/tmp/run/thumb.png",
        "powerpoint_review_summary": {
            "pdf_path": "/tmp/run/powerpoint_review/deck.pdf",
            "montage_path": "/tmp/run/powerpoint_review/montage.png",
        },
        "publish_checklist_markdown_path": "/tmp/run/publish_checklist.md",
        "owner_commentary_markdown_path": "/tmp/run/commentary.md",
        "owner_commentary_owner_send_list_path": "/tmp/run/owner_send_list.csv",
        "owner_commentary_owner_packet_index_markdown_path": "/tmp/run/owner_packets.md",
        "finance_churn_request_markdown_path": "/tmp/run/finance.md",
        "finance_churn_request_csv_path": "/tmp/run/finance.csv",
        "finance_churn_request_email_path": "/tmp/run/finance_email.md",
        "approval_rule_markdown_path": "/tmp/run/approval_rule.md",
    }
    snapshot = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "quarterly_pipeline_display": {
            "display_quarter": {
                "reason": "forward_quarter_fallback",
                "title": "Q2 2026",
                "footnote": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
            }
        },
        "slipped_deals": {"top_repeat_push": [{"account_name": "Example Account"}]},
    }
    deck_summary = {
        "biggest_gap_region": "North America",
        "biggest_gap_arr": 100.0,
        "weakest_confidence_region": "EMEA",
        "weakest_confidence_pct": 90.0,
        "approval_candidate_count": 0,
        "total_open_renewal_pipeline_acv": 200.0,
        "biggest_slipped_region": "APAC",
        "biggest_slipped_arr": 300.0,
        "publish_blockers": ["Finance churn input is still missing"],
    }
    publish_checklist = {
        "internal_review_ready": True,
        "publish_ready": False,
        "blocked_item_count": 1,
        "quarterly_pipeline_disclosure": {
            "display_reason": "forward_quarter_fallback",
            "quarterly_pipeline_title": "Q2 2026",
            "quarterly_pipeline_footnote": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
        },
    }
    validation_summary = {"montage_path": "/tmp/run/montage.png"}

    packet = build_internal_review_packet(
        manifest=manifest,
        snapshot=snapshot,
        deck_summary=deck_summary,
        publish_checklist=publish_checklist,
        validation_summary=validation_summary,
    )

    assert packet["deck_path"] == "/tmp/run/deck.pptx"
    assert packet["validation_montage_path"] == "/tmp/run/montage.png"
    assert packet["powerpoint_review_pdf_path"] == "/tmp/run/powerpoint_review/deck.pdf"
    assert packet["powerpoint_review_montage_path"] == "/tmp/run/powerpoint_review/montage.png"
    assert packet["owner_commentary_send_list_path"] == "/tmp/run/owner_send_list.csv"
    assert packet["owner_commentary_packet_index_path"] == "/tmp/run/owner_packets.md"
    assert packet["finance_churn_request_path"] == "/tmp/run/finance.md"
    assert packet["finance_churn_request_csv_path"] == "/tmp/run/finance.csv"
    assert packet["finance_churn_request_email_path"] == "/tmp/run/finance_email.md"
    assert packet["publish_blockers"] == ["Finance churn input is still missing"]
    assert packet["primary_readout"]["top_repeat_push_account"] == "Example Account"
    assert packet["quarterly_pipeline_disclosure"]["display_reason"] == "forward_quarter_fallback"

    markdown = build_internal_review_packet_markdown(packet=packet)
    assert "## Quarter Disclosure" in markdown
    assert "Forward-quarter fallback: `Q2 2026`." in markdown


def test_build_run_summary_surfaces_quarter_fallback_from_publish_checklist() -> None:
    manifest = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "deck_path": "/tmp/run/deck.pptx",
        "snapshot_path": "/tmp/run/report1_snapshot.json",
        "deck_summary_path": "/tmp/run/deck-summary.json",
        "deck_summary": {
            "biggest_gap_region": "North America",
            "biggest_gap_arr": 100.0,
            "weakest_confidence_region": "EMEA",
            "weakest_confidence_pct": 90.0,
            "approval_candidate_count": 0,
            "total_open_renewal_pipeline_acv": 200.0,
            "critical_renewal_acv": 50.0,
            "biggest_slipped_region": "APAC",
            "biggest_slipped_arr": 300.0,
            "value_methodology": "ARR for pipeline, ACV for renewals",
            "finance_churn_status": "pending",
            "slipped_commentary_status": "provided",
            "publish_status": "internal_only",
            "publish_blockers": [],
        },
        "publish_checklist": {
            "internal_review_ready": True,
            "publish_ready": False,
            "quarterly_pipeline_disclosure": {
                "display_reason": "forward_quarter_fallback",
                "quarterly_pipeline_title": "Q2 2026",
                "quarterly_pipeline_footnote": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
            },
        },
        "validation_summary": {"status": "ok"},
        "powerpoint_review_summary": {"status": "ok"},
    }

    markdown = build_run_summary(manifest=manifest)

    assert "## Gate Readiness" in markdown
    assert "Forward-quarter fallback: `Q2 2026`." in markdown


def test_build_latest_status_packet_and_aliases_surface_quarter_disclosure(tmp_path: Path) -> None:
    run_dir = tmp_path / "2026-04-22T_test_run"
    manifest = {
        "snapshot_date": "2026-03-31",
        "quarter_focus": "Q1",
        "run_dir": str(run_dir),
        "deck_path": str(run_dir / "deck.pptx"),
        "publish_checklist_markdown_path": str(run_dir / "publish_checklist.md"),
        "internal_review_packet_markdown_path": str(run_dir / "INTERNAL_REVIEW_PACKET.md"),
        "validation_summary": {"status": "ok"},
        "powerpoint_review_summary": {"status": "ok"},
        "publish_checklist": {
            "publish_ready": False,
            "blocked_item_count": 1,
            "quarterly_pipeline_disclosure": {
                "display_reason": "forward_quarter_fallback",
                "quarterly_pipeline_title": "Q2 2026",
                "quarterly_pipeline_footnote": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
            },
            "checks": [
                {
                    "name": "Quarter fallback disclosed",
                    "status": "pass",
                    "detail": "No Q1 2026 in-scope pipeline; showing Q2 2026 forward-quarter outlook.",
                },
                {
                    "name": "Finance churn overlay publishable",
                    "status": "blocked",
                    "detail": "Status=pending provenance=pending",
                },
            ],
        },
    }

    packet = build_latest_status_packet(manifest=manifest)

    assert packet["run_dir"] == str(run_dir)
    assert packet["blocked_checks"] == [
        {
            "name": "Finance churn overlay publishable",
            "detail": "Status=pending provenance=pending",
        }
    ]
    assert packet["quarterly_pipeline_disclosure"]["display_reason"] == "forward_quarter_fallback"

    markdown = build_latest_status_markdown(packet=packet)
    assert "# Latest Sales Director Monthly Run" in markdown
    assert "Forward-quarter fallback: `Q2 2026`." in markdown
    assert "Finance churn overlay publishable: Status=pending provenance=pending" in markdown

    write_latest_aliases(output_root=tmp_path, packet=packet, markdown=markdown)

    latest_json = json.loads((tmp_path / "latest.json").read_text(encoding="utf-8"))
    latest_md = (tmp_path / "latest.md").read_text(encoding="utf-8")
    assert latest_json["run_dir"] == str(run_dir)
    assert latest_json["quarterly_pipeline_disclosure"]["quarterly_pipeline_title"] == "Q2 2026"
    assert latest_md == markdown
