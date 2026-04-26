from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_sales_global_summary_preview import analyze_preview


def test_analyze_preview_flags_shell_style_labels() -> None:
    contract = {
        "slides": [
            {"id": "global-executive-summary", "title": "Global Executive Summary"},
        ]
    }
    fill_payload = {"slides": [{"id": "global-executive-summary"}]}
    slide_texts = [
        ["Sales Global Summary"],
        ["Agenda"],
        ["Global Q2 active ARR is €40.2M", "Leadership use", "Meeting rules"],
    ]

    report = analyze_preview(
        shell_contract=contract,
        fill_payload=fill_payload,
        slide_texts=slide_texts,
    )

    assert any(item["type"] == "shell_style_label" for item in report["findings"])


def test_analyze_preview_records_disclosed_forward_quarter_fallback() -> None:
    contract = {
        "slides": [
            {"id": "apac-region-summary", "title": "APAC Summary"},
        ]
    }
    fill_payload = {
        "slides": [
            {
                "id": "apac-region-summary",
                "slots": {
                    "region_name": "APAC",
                    "quarterly_pipeline_title": "Q3 2026",
                    "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                    "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                },
            }
        ]
    }
    slide_texts = [
        ["Sales Global Summary"],
        ["Agenda"],
        [
            "APAC has €1.0M Q3 active ARR and €300K open renewal ACV",
            "Showing Q3 2026 because the current quarter is empty.",
            "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        ],
    ]

    report = analyze_preview(
        shell_contract=contract,
        fill_payload=fill_payload,
        slide_texts=slide_texts,
    )

    assert report["ok"] is True
    assert report["quarterly_pipeline_disclosures"] == [
        {
            "slide_id": "apac-region-summary",
            "slide_number": 3,
            "region_name": "APAC",
            "display_reason": "forward_quarter_fallback",
            "quarterly_pipeline_title": "Q3 2026",
            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        }
    ]
    assert not any(item["type"] == "forward_quarter_fallback_hidden" for item in report["findings"])


def test_analyze_preview_fails_hidden_forward_quarter_fallback() -> None:
    contract = {
        "slides": [
            {"id": "apac-region-summary", "title": "APAC Summary"},
        ]
    }
    fill_payload = {
        "slides": [
            {
                "id": "apac-region-summary",
                "slots": {
                    "region_name": "APAC",
                    "quarterly_pipeline_title": "Q3 2026",
                    "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                    "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                },
            }
        ]
    }
    slide_texts = [
        ["Sales Global Summary"],
        ["Agenda"],
        [
            "APAC has €1.0M Q3 active ARR and €300K open renewal ACV",
            "Showing Q3 2026 because the current quarter is empty.",
        ],
    ]

    report = analyze_preview(
        shell_contract=contract,
        fill_payload=fill_payload,
        slide_texts=slide_texts,
    )

    assert report["ok"] is False
    assert any(item["type"] == "forward_quarter_fallback_hidden" for item in report["findings"])
