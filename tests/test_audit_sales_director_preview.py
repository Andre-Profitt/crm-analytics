from __future__ import annotations

from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.audit_sales_director_preview import analyze_preview


def test_analyze_preview_flags_cover_shell_and_title_leaks() -> None:
    contract = {
        "slides": [
            {"id": "executive-summary", "title": "Executive Summary", "data_contract": {"support_level": "strong"}},
            {"id": "churn-finance", "title": "Churn Risk and Finance Inputs", "data_contract": {"support_level": "placeholder"}},
        ]
    }
    fill_payload = {
        "slides": [
            {"id": "executive-summary", "support_level": "strong"},
            {"id": "churn-finance", "support_level": "placeholder"},
        ]
    }
    slide_texts = [
        ["Sales Director Monthly Shell"],  # cover
        ["Agenda"],
        ["Executive Summary", "Validated read"],
        ["Churn Risk and Finance Inputs", "Populate after Finance data arrives"],
    ]

    report = analyze_preview(
        shell_contract=contract,
        fill_payload=fill_payload,
        slide_texts=slide_texts,
    )

    assert report["ok"] is False
    assert any(item["type"] == "cover_shell_language" for item in report["findings"])
    assert any(item["type"] == "title_not_rewritten" for item in report["findings"])
    assert any(item["type"] == "placeholder_leak" for item in report["findings"])


def test_analyze_preview_ignores_placeholder_slide_token_leaks() -> None:
    contract = {
        "slides": [
            {"id": "churn-finance", "title": "Churn Risk and Finance Inputs", "data_contract": {"support_level": "placeholder"}},
        ]
    }
    fill_payload = {"slides": [{"id": "churn-finance", "support_level": "placeholder"}]}
    slide_texts = [
        ["Sales Director Monthly"],
        ["Agenda"],
        ["Churn Risk and Finance Inputs", "Populate after Finance input is available"],
    ]

    report = analyze_preview(
        shell_contract=contract,
        fill_payload=fill_payload,
        slide_texts=slide_texts,
    )

    assert not any(item["type"] == "placeholder_leak" for item in report["findings"])


def test_analyze_preview_flags_shell_style_labels() -> None:
    contract = {
        "slides": [
            {"id": "executive-summary", "title": "Executive Summary", "data_contract": {"support_level": "strong"}},
        ]
    }
    fill_payload = {"slides": [{"id": "executive-summary", "support_level": "strong"}]}
    slide_texts = [
        ["Sales Director Monthly"],
        ["Agenda"],
        ["Q2 active ARR is €8.9M", "Meeting rules", "Month in view"],
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
            {"id": "quarterly-pipeline", "title": "Quarterly Pipeline", "data_contract": {"support_level": "strong"}},
        ]
    }
    fill_payload = {
        "director_name": "Jane Doe",
        "territory": "APAC",
        "slides": [
            {
                "id": "quarterly-pipeline",
                "support_level": "strong",
                "slots": {
                    "quarterly_pipeline_title": "Q3 2026",
                    "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                    "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                },
            }
        ],
    }
    slide_texts = [
        ["Sales Director Monthly"],
        ["Agenda"],
        [
            "Q3 active ARR is €1.0M with €700K Commit and €300K Best Case",
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
            "slide_id": "quarterly-pipeline",
            "slide_number": 3,
            "director_name": "Jane Doe",
            "territory": "APAC",
            "display_reason": "forward_quarter_fallback",
            "quarterly_pipeline_title": "Q3 2026",
            "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
        }
    ]
    assert not any(item["type"] == "forward_quarter_fallback_hidden" for item in report["findings"])


def test_analyze_preview_fails_hidden_forward_quarter_fallback() -> None:
    contract = {
        "slides": [
            {"id": "quarterly-pipeline", "title": "Quarterly Pipeline", "data_contract": {"support_level": "strong"}},
        ]
    }
    fill_payload = {
        "director_name": "Jane Doe",
        "territory": "APAC",
        "slides": [
            {
                "id": "quarterly-pipeline",
                "support_level": "strong",
                "slots": {
                    "quarterly_pipeline_title": "Q3 2026",
                    "quarterly_pipeline_display_reason": "forward_quarter_fallback",
                    "quarterly_pipeline_footnote": "No Q2 2026 in-scope pipeline; showing Q3 2026 forward-quarter outlook.",
                },
            }
        ],
    }
    slide_texts = [
        ["Sales Director Monthly"],
        ["Agenda"],
        [
            "Q3 active ARR is €1.0M with €700K Commit and €300K Best Case",
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
