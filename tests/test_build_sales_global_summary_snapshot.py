from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_sales_global_summary_snapshot import build_global_summary_snapshot


def write_region_snapshot(path: Path, *, region_name: str, q2_arr: str, q2_renewal_acv: float, missing_count: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "region_name": region_name,
        "scorecard": {
            "sections": {
                "pipeline-health": {
                    "metrics": {
                        "Pipeline ARR — Q2 2026 Close Dates Only (excl. Omitted)": q2_arr,
                    }
                },
                "process-compliance": {
                    "metrics": {
                        "Approval Rate (stage 3+)": "25.0%",
                        "Missing Approval (Land, stage 3+)": missing_count,
                    }
                },
                "risk": {
                    "metrics": {
                        "Stale 30d+ (ARR)": "€1.0M",
                        "Aging 365+ (ARR)": "€0.5M",
                    }
                },
            }
        },
        "q2_outlook": {
            "by_category": {
                "Commit": {"ARR (€ converted)": 100000},
                "Best Case": {"ARR (€ converted)": 200000},
                "Omitted": {"ARR (€ converted)": 30000},
            }
        },
        "renewals": {
            "summary_metrics": {
                "open_acv": 500000,
                "q2_open_acv": q2_renewal_acv,
            },
            "q2_open_renewals": [],
        },
        "commercial_approval": {
            "approved_ytd": [{"Opportunity": f"{region_name} Approved", "ARR (€ converted)": 400000}],
            "missing_candidates": [
                {
                    "Opportunity": f"{region_name} Missing {i}",
                    "ARR (€ converted)": 100000 * (missing_count - i + 1),
                    "Owner": "Rep A",
                    "Stage": "3 - Solution Fit",
                }
                for i in range(1, missing_count + 1)
            ],
        },
        "forecast_hierarchy_note": "Middle East & Africa is included under EMEA in the forecast hierarchy."
        if region_name == "EMEA"
        else None,
    }
    path.write_text(json.dumps(snapshot), encoding="utf-8")


def test_build_global_summary_snapshot_rolls_up_regions(tmp_path: Path) -> None:
    root = tmp_path / "regions" / "2026-04-10"
    write_region_snapshot(root / "apac.json", region_name="APAC", q2_arr="€4.0M", q2_renewal_acv=150000, missing_count=1)
    write_region_snapshot(root / "emea.json", region_name="EMEA", q2_arr="€10.0M", q2_renewal_acv=250000, missing_count=2)
    write_region_snapshot(root / "north-america.json", region_name="North America", q2_arr="€3.0M", q2_renewal_acv=100000, missing_count=1)

    snapshot = build_global_summary_snapshot(
        snapshot_date="2026-04-10",
        region_snapshot_root=tmp_path / "regions",
        director_snapshot_root=tmp_path / "directors",
    )

    assert snapshot["global_summary"]["global_pipeline_arr_q2"] == "€17.0M"
    assert snapshot["global_summary"]["global_renewal_acv_q2"] == "€500K"
    assert snapshot["global_summary"]["global_missing_approval_count"] == 4
    assert len(snapshot["regions"]) == 3
    assert any("EMEA" in note for note in snapshot["region_rollup_notes"])
    assert snapshot["commercial_approval"]["approved_2026_by_region"][0]["deal_count"] == 1


def test_build_global_summary_snapshot_carries_forward_quarter_region_metadata(tmp_path: Path) -> None:
    root = tmp_path / "regions" / "2026-04-30"
    write_region_snapshot(root / "apac.json", region_name="APAC", q2_arr="€0", q2_renewal_acv=150000, missing_count=1)
    write_region_snapshot(root / "emea.json", region_name="EMEA", q2_arr="€10.0M", q2_renewal_acv=250000, missing_count=2)
    write_region_snapshot(root / "north-america.json", region_name="North America", q2_arr="€3.0M", q2_renewal_acv=100000, missing_count=1)

    apac_snapshot = json.loads((root / "apac.json").read_text(encoding="utf-8"))
    apac_snapshot["quarterly_pipeline_display"] = {
        "display_quarter": {
            "label": "Q3",
            "title": "Q3 2026",
            "by_category": {
                "Commit": {"ARR (€ converted)": 700000},
                "Best Case": {"ARR (€ converted)": 300000},
                "Omitted": {"ARR (€ converted)": 0},
            },
            "active_arr": 1000000,
            "reason": "forward_quarter_fallback",
            "footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        }
    }
    (root / "apac.json").write_text(json.dumps(apac_snapshot), encoding="utf-8")

    snapshot = build_global_summary_snapshot(
        snapshot_date="2026-04-30",
        region_snapshot_root=tmp_path / "regions",
        director_snapshot_root=tmp_path / "directors",
    )

    apac = next(region for region in snapshot["regions"] if region["region_name"] == "APAC")
    assert apac["quarterly_pipeline_label"] == "Q3"
    assert apac["quarterly_pipeline_title"] == "Q3 2026"
    assert apac["headline_pipeline_arr_q2"] == "€1.0M"
    assert apac["q2_commit_arr"] == "€700K"
    assert (
        apac["quarterly_pipeline_footnote"]
        == "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook."
    )
