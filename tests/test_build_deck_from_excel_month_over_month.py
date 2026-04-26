import json
from pathlib import Path

from scripts import build_deck_from_excel as deck


def test_read_prior_snapshot_from_history_prefers_retrospective_metadata(
    tmp_path: Path,
) -> None:
    ledger_dir = tmp_path / "obsidian"
    ledger_dir.mkdir(parents=True)
    (tmp_path / "scripts").mkdir()
    (ledger_dir / "snapshot_history.json").write_text(
        json.dumps(
            {
                "snapshots": [
                    {
                        "run_date": "2026-10-01",
                        "period": "2026-10",
                        "retrospective_quarter_label": "Q3",
                        "retrospective_quarter_title": "Q3 2026",
                        "directors": {
                            "Jesper Tyrer": {
                                "territory": "APAC",
                                "open_land_deals": 4,
                                "open_land_arr_unwtd": 400.0,
                                "retrospective_land_label": "Q3",
                                "retrospective_land_title": "Q3 2026",
                                "retrospective_land_won_count": 2,
                                "retrospective_land_won_arr": 250.0,
                                "q1_won_count": 99,
                                "q1_won_arr": 999.0,
                                "approved_2026": 1,
                                "missing_approval": 0,
                            }
                        },
                    },
                    {
                        "run_date": "2026-10-10",
                        "period": "2026-10",
                        "retrospective_quarter_label": "Q2",
                        "retrospective_quarter_title": "Q2 2026",
                        "directors": {
                            "Jesper Tyrer": {
                                "territory": "APAC",
                                "open_land_deals": 8,
                                "open_land_arr_unwtd": 800.0,
                                "retrospective_land_label": "Q2",
                                "retrospective_land_title": "Q2 2026",
                                "retrospective_land_won_count": 8,
                                "retrospective_land_won_arr": 800.0,
                                "approved_2026": 3,
                                "missing_approval": 1,
                            }
                        },
                    },
                ]
            },
            indent=2,
        )
        + "\n"
    )

    previous_file = deck.__file__
    previous_fq = dict(deck.FQ)
    deck.__file__ = str(tmp_path / "scripts" / "build_deck_from_excel.py")
    deck.FQ = deck._resolve_runtime_period_context(as_of_date="2026-10-15")
    try:
        prior = deck._read_prior_snapshot_from_history("Jesper Tyrer", "2026-10-15")
    finally:
        deck.__file__ = previous_file
        deck.FQ = previous_fq

    assert prior == {
        "period": "2026-10-01",
        "retrospective_label": "Q3",
        "retrospective_title": "Q3 2026",
        "open_deals": 4,
        "open_unwtd": 400.0,
        "q1_won_count": 2,
        "q1_won_arr": 250.0,
        "approved_2026": 1,
        "missing_approval": 0,
    }


def test_read_prior_snapshot_parses_runtime_retrospective_label(
    tmp_path: Path,
) -> None:
    monthly = tmp_path / "obsidian" / "Monthly" / "2026-09"
    monthly.mkdir(parents=True)
    (tmp_path / "scripts").mkdir()
    (monthly / "jesper-tyrer.auto.md").write_text(
        "\n".join(
            [
                "# September 2026 review",
                "",
                "- Open Land pipeline: 5 deals, EUR 1.2M unweighted, EUR 900K weighted.",
                "- Q3 Land outcome: 2 wins (EUR 3.5M), 1 losses (EUR 750K).",
                "- Commercial approvals: 1 approved 2026, 0 pending approval, 2 missing Stage 3+.",
            ]
        )
        + "\n"
    )

    previous_file = deck.__file__
    previous_fq = dict(deck.FQ)
    deck.__file__ = str(tmp_path / "scripts" / "build_deck_from_excel.py")
    deck.FQ = deck._resolve_runtime_period_context(as_of_date="2026-10-15")
    try:
        prior = deck._read_prior_snapshot("jesper-tyrer", "2026-10")
    finally:
        deck.__file__ = previous_file
        deck.FQ = previous_fq

    assert prior == {
        "period": "2026-09",
        "retrospective_label": "Q3",
        "retrospective_title": "Q3 2026",
        "open_deals": 5,
        "open_unwtd": 1_200_000.0,
        "q1_won_count": 2,
        "q1_won_arr": 3_500_000.0,
        "approved_2026": 1,
        "missing_approval": 2,
    }
