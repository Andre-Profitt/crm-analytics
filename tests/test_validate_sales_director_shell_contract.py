from __future__ import annotations

import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.validate_sales_director_shell_contract as module


def test_default_contract_is_valid() -> None:
    contract = module.load_contract(module.DEFAULT_CONTRACT_PATH)
    result = module.validate_contract(contract)

    assert result["ok"] is True
    assert result["slide_count"] >= 10
    assert result["support_summary"]["strong"] >= 1
    assert result["support_summary"]["placeholder"] == 1


def test_validator_rejects_missing_data_contract() -> None:
    contract = {
        "production_rules": {
            "canonical_shell_required": True,
            "generated_shell_publish_safe": False,
            "population_mode": "structured-fill-only",
        },
        "presentation_standard": {
            "message_title_required_on_populated_deck": True,
            "one_question_per_slide": True,
            "prefer_visual_evidence_over_bullets": True,
            "required_fields_per_slide": [
                "management_question",
                "visual_family",
                "action_seam",
                "title_rewrite_rule",
                "density_limit",
                "anti_patterns",
            ],
        },
        "coverage_standard": {
            "support_levels": ["strong", "qualified", "placeholder"],
            "minimum_fields_per_slide": [
                "support_level",
                "source_tabs",
                "source_fields",
                "metric_rules",
                "known_gaps",
            ],
        },
        "slides": [
            {
                "id": "executive-summary",
                "management_question": "What is the operating position and action?",
                "visual_family": "four-card-kpi-strip",
                "action_seam": "Name the next action.",
                "title_rewrite_rule": "Rewrite as a takeaway title.",
                "density_limit": {"max_cards": 4},
                "anti_patterns": ["Do not show unlabeled numbers."],
                "required_slots": ["headline_pipeline_arr_all_open"],
            }
        ],
    }

    result = module.validate_contract(contract)

    assert result["ok"] is False
    assert "executive-summary is missing data_contract." in result["errors"]
