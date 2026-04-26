#!/usr/bin/env python3
"""Validate the director shell contract required for production deck builds."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT_PATH = REPO_ROOT / "config" / "sales_director_monthly_shell.json"


def load_contract(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_contract(contract: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    slides = contract.get("slides") or []
    production_rules = contract.get("production_rules") or {}
    presentation_standard = contract.get("presentation_standard") or {}
    coverage_standard = contract.get("coverage_standard") or {}
    required_contract_fields = set(coverage_standard.get("minimum_fields_per_slide") or [])
    allowed_support_levels = set(coverage_standard.get("support_levels") or [])
    required_presentation_fields = set(presentation_standard.get("required_fields_per_slide") or [])

    if production_rules.get("canonical_shell_required") is not True:
        errors.append("production_rules.canonical_shell_required must be true.")
    if production_rules.get("generated_shell_publish_safe") is not False:
        errors.append("production_rules.generated_shell_publish_safe must be false.")
    if production_rules.get("population_mode") != "structured-fill-only":
        errors.append("production_rules.population_mode must be 'structured-fill-only'.")
    if presentation_standard.get("message_title_required_on_populated_deck") is not True:
        errors.append("presentation_standard.message_title_required_on_populated_deck must be true.")
    if presentation_standard.get("one_question_per_slide") is not True:
        errors.append("presentation_standard.one_question_per_slide must be true.")
    if presentation_standard.get("prefer_visual_evidence_over_bullets") is not True:
        errors.append("presentation_standard.prefer_visual_evidence_over_bullets must be true.")

    slide_ids: set[str] = set()
    support_summary: dict[str, int] = {}
    for index, slide in enumerate(slides, start=1):
        slide_id = slide.get("id")
        if not slide_id:
            errors.append(f"slides[{index}] is missing id.")
            continue
        if slide_id in slide_ids:
            errors.append(f"duplicate slide id: {slide_id}")
        slide_ids.add(slide_id)
        if not slide.get("required_slots"):
            errors.append(f"{slide_id} must declare required_slots.")

        missing_presentation_fields = sorted(
            field for field in required_presentation_fields if field not in slide
        )
        if missing_presentation_fields:
            errors.append(
                f"{slide_id} missing presentation fields: {', '.join(missing_presentation_fields)}"
            )
        if not as_non_empty_text(slide.get("management_question")):
            errors.append(f"{slide_id} management_question must not be empty.")
        if not as_non_empty_text(slide.get("visual_family")):
            errors.append(f"{slide_id} visual_family must not be empty.")
        if not as_non_empty_text(slide.get("action_seam")):
            errors.append(f"{slide_id} action_seam must not be empty.")
        if not as_non_empty_text(slide.get("title_rewrite_rule")):
            errors.append(f"{slide_id} title_rewrite_rule must not be empty.")
        if not isinstance(slide.get("density_limit"), dict) or not slide.get("density_limit"):
            errors.append(f"{slide_id} density_limit must be a non-empty object.")
        anti_patterns = slide.get("anti_patterns")
        if not isinstance(anti_patterns, list) or not anti_patterns:
            errors.append(f"{slide_id} anti_patterns must be a non-empty list.")

        data_contract = slide.get("data_contract")
        if not isinstance(data_contract, dict):
            errors.append(f"{slide_id} is missing data_contract.")
            continue

        missing_fields = sorted(field for field in required_contract_fields if field not in data_contract)
        if missing_fields:
            errors.append(f"{slide_id} data_contract missing fields: {', '.join(missing_fields)}")
            continue

        support_level = data_contract.get("support_level")
        support_summary[support_level] = support_summary.get(support_level, 0) + 1
        if support_level not in allowed_support_levels:
            errors.append(f"{slide_id} has invalid support_level: {support_level}")

        for field_name in ("source_tabs", "source_fields", "metric_rules", "known_gaps"):
            field_value = data_contract.get(field_name)
            if not isinstance(field_value, list):
                errors.append(f"{slide_id} data_contract.{field_name} must be a list.")
                continue
            if field_name != "known_gaps" and not field_value:
                errors.append(f"{slide_id} data_contract.{field_name} must not be empty.")

        if support_level == "placeholder" and not data_contract.get("known_gaps"):
            errors.append(f"{slide_id} is placeholder but does not document the gap.")

    return {
        "ok": not errors,
        "slide_count": len(slides),
        "support_summary": support_summary,
        "errors": errors,
    }


def as_non_empty_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--contract", type=Path, default=DEFAULT_CONTRACT_PATH)
    args = parser.parse_args()

    result = validate_contract(load_contract(args.contract))
    print(json.dumps(result, indent=2))
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
