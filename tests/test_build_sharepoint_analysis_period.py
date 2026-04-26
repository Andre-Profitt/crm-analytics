from pathlib import Path

from scripts import build_sharepoint_analysis as sharepoint_analysis


def test_sharepoint_period_context_infers_report_date_from_workbooks_dir() -> None:
    context = sharepoint_analysis._resolve_runtime_period_context(
        workbooks_dir=Path("/tmp/director_live_workbooks/2026-04-22")
    )

    assert context["report_date"] == "2026-04-22"
    assert context["fy_label"] == "FY26"
    assert context["q1_title"] == "Q1 2026"
    assert context["prior_quarter_title"] == "Q1 2026"
    assert context["current_quarter_title"] == "Q2 2026"
    assert context["current_quarter_months_title"] == "April through June 2026"


def test_gather_director_data_uses_runtime_current_quarter_for_renewals(monkeypatch) -> None:
    previous = dict(sharepoint_analysis.RUNTIME_PERIOD)
    sharepoint_analysis._configure_runtime_period(as_of_date="2026-10-15")

    monkeypatch.setattr(
        sharepoint_analysis,
        "_load",
        lambda _path: {
            "Pipeline Open FY26": [
                {
                    "Type": "Land",
                    "Forecast Category": "Pipeline",
                    "ARR Unweighted (EUR)": 1_000_000,
                    "ARR Weighted (EUR)": 500_000,
                    "Opportunity": "Alpha",
                    "Account": "Acme",
                    "Owner": "Rep",
                    "Stage": "3 - Engagement",
                    "Push Count": 0,
                }
            ],
            "Won Lost FY26": [
                {
                    "Type": "Land",
                    "Stage": "8 - Won",
                    "Close Date": "2026-02-15",
                    "ARR Unweighted (EUR)": 250_000,
                    "Account": "Acme",
                    "Opportunity": "Won Deal",
                    "Owner": "Rep",
                    "Sales Region": "APAC",
                }
            ],
            "Q1 Movement": [],
            "Commercial Approval": [],
            "Renewals FY26": [
                {
                    "Close Date": "2026-11-01",
                    "Account": "Renew Co",
                    "Opportunity": "Q4 Renewal",
                    "Owner": "Rep",
                    "Stage": "2 - Qualification",
                    "Probability %": 60,
                    "ACV Unweighted (EUR)": 125_000,
                },
                {
                    "Close Date": "2026-08-01",
                    "Account": "Out Of Quarter",
                    "Opportunity": "Q3 Renewal",
                    "Owner": "Rep",
                    "Stage": "2 - Qualification",
                    "Probability %": 50,
                    "ACV Unweighted (EUR)": 90_000,
                },
            ],
        },
    )
    monkeypatch.setattr(
        sharepoint_analysis,
        "forecast_page_fy26",
        lambda *_args, **_kwargs: {
            "Commit": 0,
            "Best Case": 0,
            "Pipeline": 0,
            "Closed": 0,
        },
    )

    try:
        data = sharepoint_analysis.gather_director_data(
            Path("/tmp/fake.xlsx"),
            oid=None,
            tid=None,
            session=None,
            instance=None,
        )
    finally:
        sharepoint_analysis.RUNTIME_PERIOD = previous

    assert data["q1_won_count"] == 1
    assert [row["opportunity"] for row in data["renewals_q2"]] == ["Q4 Renewal"]
    assert data["renewals_q2"][0]["acv"] == 125_000
