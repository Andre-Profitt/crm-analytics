from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_sales_region_snapshot import build_region_snapshot


def write_snapshot(root: Path, snapshot_date: str, director_slug: str, payload: dict) -> None:
    path = root / snapshot_date / f"{director_slug}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def minimal_snapshot(director_name: str, territory: str) -> dict:
    return {
        "director_name": director_name,
        "territory": territory,
        "snapshot_date": "2026-04-10",
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — All Open (any close date)": "€10.0M",
                        "Pipeline ARR — FY26 Close Dates Only (excl. Omitted)": "€8.0M",
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": "€2.0M",
                        "Deal Count": 10,
                        "Weighted Pipeline (probability-adj)": "€1.0M",
                        "New Pipeline This Quarter (excl. Omitted)": "€0.5M",
                    }
                },
                "risk": {
                    "metrics": {
                        "Stale 30d+ (count)": 1,
                        "Stale 30d+ (ARR)": "€1.0M",
                        "Pushed 5+ (count)": 2,
                        "Pushed 5+ (ARR)": "€0.5M",
                        "Aging 365+ (count)": 3,
                        "Aging 365+ (ARR)": "€2.0M",
                    }
                },
            }
        },
        "pipeline_detail": {
            "records": [
                {
                    "Opportunity": f"{territory} Q2 Deal",
                    "ARR (€ converted)": 3000000,
                    "Owner": "Owner A",
                    "Close Date": "2026-05-30",
                    "Forecast Category": "Best Case",
                },
                {
                    "Opportunity": f"{territory} Omitted Deal",
                    "ARR (€ converted)": 9000000,
                    "Owner": "Owner B",
                    "Close Date": "2026-05-30",
                    "Forecast Category": "Omitted",
                },
            ],
            "top_opportunities": [
                {"Opportunity": f"{territory} Deal", "ARR (€ converted)": 3000000, "Owner": "Owner A"}
            ]
        },
        "q2_outlook": {
            "breakdown": [
                {"Forecast Category": "Commit", "Deal Count": 1, "ARR (€ converted)": 500000, "ACV (€ converted)": 600000},
                {"Forecast Category": "Best Case", "Deal Count": 2, "ARR (€ converted)": 700000, "ACV (€ converted)": 900000},
                {"Forecast Category": "Omitted", "Deal Count": 1, "ARR (€ converted)": 100000, "ACV (€ converted)": 120000},
            ],
            "commit_deals": [{"Opportunity": "Commit 1", "ARR (€ converted)": 500000}],
            "best_case_deals": [{"Opportunity": "Best 1", "ARR (€ converted)": 700000}],
        },
        "commercial_approval": {
            "summary": [
                {"Category": "Approved", "Deal Count": 1, "ARR (€ converted)": 200000},
                {"Category": "Pending / Missing Approval", "Deal Count": 2, "ARR (€ converted)": 300000},
                {"Category": "No Approval Needed", "Deal Count": 1, "ARR (€ converted)": 0},
            ],
            "missing_candidates": [{"Opportunity": "Missing 1", "ARR (€ converted)": 400000}],
            "approved_ytd": [{"Opportunity": "Approved 1", "ARR (€ converted)": 200000}],
        },
        "renewals": {
            "open_renewals": [
                {"Opportunity": "Renewal 1", "Renewal ACV (€ converted)": 250000, "Close Date": "2026-06-30"},
                {"Opportunity": "Renewal 2", "Renewal ACV (€ converted)": 100000, "Close Date": "2026-12-31"},
            ],
            "risk_levels": [{"Risk Level": "Medium", "Deal Count": 1, "ACV (€ converted)": 250000}],
        },
        "q1_review": {
            "actuals": {
                "won_count": 1,
                "won_arr": 100000,
                "lost_count": 2,
                "lost_arr": 300000,
                "slipped_count": 3,
                "slipped_arr": 400000,
            },
            "promise_baseline": [{"Category": "Commit", "Count": 1, "ARR (€ converted)": 500000}],
            "pushed_deals": [{"Opportunity": "Slip 1", "ARR (€ converted)": 400000}],
            "forecast_movements": [{"Opportunity": "Move 1", "ARR (€ converted)": 200000}],
            "forecast_movement_summary": [{"from": "Commit", "to": "Pipeline", "count": 1, "arr": 200000}],
        },
        "won_lost": {
            "won": [{"Opportunity": "Won 1", "ARR (€ converted)": 100000}],
            "lost": [{"Opportunity": "Lost 1", "ARR (€ converted)": 300000, "Reason Won/Lost": "External competitor chosen"}],
        },
        "data_quality": {
            "total": {"Rep": "TOTAL", "Missing Amount": 1, "Total Issues": 5},
            "records": [{"Rep": "Rep A", "Missing Amount": 1, "Total Issues": 5}],
        },
    }


def test_build_region_snapshot_rolls_emEA_correctly(tmp_path: Path) -> None:
    root = tmp_path / "snapshots"
    date = "2026-04-10"
    write_snapshot(root, date, "sarah-pittroff", minimal_snapshot("Sarah Pittroff", "Central Europe"))
    write_snapshot(root, date, "francois-thaury", minimal_snapshot("Francois Thaury", "Southern Europe"))
    write_snapshot(root, date, "dan-peppett", minimal_snapshot("Dan Peppett", "UK & Ireland"))
    write_snapshot(root, date, "christian-ebbesen", minimal_snapshot("Christian Ebbesen", "NL & Nordics"))
    write_snapshot(root, date, "mourad-essofi", minimal_snapshot("Mourad Essofi", "Middle East & Africa"))
    write_snapshot(root, date, "jesper-tyrer", minimal_snapshot("Jesper Tyrer", "APAC"))

    snapshot = build_region_snapshot(region_name="EMEA", snapshot_date=date, director_snapshot_root=root)

    names = [row["director_name"] for row in snapshot["component_books"]]
    assert names == [
        "Sarah Pittroff",
        "Francois Thaury",
        "Dan Peppett",
        "Christian Ebbesen",
        "Mourad Essofi",
    ]
    assert snapshot["component_books"][0]["q2_arr"] == 2_000_000.0
    assert snapshot["component_books"][0]["renewal_open_acv"] == 350_000.0
    assert snapshot["forecast_hierarchy_note"] == "Middle East & Africa is included under EMEA in the forecast hierarchy."
    assert snapshot["scorecard"]["sections"]["pipeline-health"]["metrics"]["Deal Count"] == 50
    assert snapshot["commercial_approval"]["summary"][0]["Deal Count"] == 5
    assert snapshot["pipeline_detail"]["q2_active_opportunities"][0]["Opportunity"] == "Central Europe Q2 Deal"
    assert snapshot["renewals"]["summary_metrics"]["q2_open_deal_count"] == 5
    assert snapshot["renewals"]["summary_metrics"]["q2_open_acv"] == 1_250_000.0


def test_build_region_snapshot_falls_forward_when_current_quarter_is_empty(tmp_path: Path) -> None:
    root = tmp_path / "snapshots"
    date = "2026-04-30"
    payload = minimal_snapshot("Jesper Tyrer", "APAC")
    payload["snapshot_date"] = date
    payload["scorecard"]["sections"]["pipeline-health"]["metrics"][
        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)"
    ] = "€0"
    payload["pipeline_detail"]["records"] = [
        {
            "Opportunity": "APAC Q3 Deal",
            "ARR (€ converted)": 900000,
            "Owner": "Owner A",
            "Close Date": "2026-07-15",
            "Forecast Category": "Commit",
        }
    ]
    payload["pipeline_detail"]["top_opportunities"] = payload["pipeline_detail"]["records"]
    payload["q2_outlook"]["breakdown"] = [
        {
            "Forecast Category": "Omitted",
            "Deal Count": 1,
            "ARR (€ converted)": 200000,
            "ACV (€ converted)": 200000,
        }
    ]
    payload["q2_outlook"]["commit_deals"] = []
    payload["q2_outlook"]["best_case_deals"] = []
    write_snapshot(root, date, "jesper-tyrer", payload)

    snapshot = build_region_snapshot(
        region_name="APAC", snapshot_date=date, director_snapshot_root=root
    )

    display = snapshot["quarterly_pipeline_display"]["display_quarter"]
    assert display["label"] == "Q3"
    assert display["title"] == "Q3 2026"
    assert display["reason"] == "forward_quarter_fallback"
    assert (
        display["footnote"]
        == "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook."
    )
    assert display["top_active_opportunities"][0]["Opportunity"] == "APAC Q3 Deal"
