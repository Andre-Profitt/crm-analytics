#!/usr/bin/env python3
"""Build a PowerPoint-Claude prompt for authoring the canonical director shell."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_director_monthly_shell.json"
DEFAULT_SOURCE_MAP_PATH = REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-deck-family-source-map.md"
DEFAULT_EXECUTION_PLAN_PATH = REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-deck-execution-plan.md"


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def shell_outline(shell: dict[str, Any]) -> str:
    lines: list[str] = []
    for index, slide in enumerate(shell.get("slides", []), start=1):
        lines.append(f"{index}. {slide['title']} (`{slide['id']}`)")
        if slide.get("subtitle"):
            lines.append(f"   Purpose: {slide['subtitle']}")
        if slide.get("required_slots"):
            lines.append("   Required slots: " + ", ".join(f"`{slot}`" for slot in slide["required_slots"]))
        if slide.get("body_guidance"):
            lines.append("   Guidance: " + " ".join(slide["body_guidance"]))
    return "\n".join(lines)


def build_prompt(
    *,
    director_name: str,
    territory: str,
    shell_contract: dict[str, Any],
    source_map_path: Path,
    execution_plan_path: Path,
) -> str:
    slide_count = len(shell_contract.get("slides", []))
    return (
        f"Transform the current PowerPoint file into the canonical monthly shell for the Sales Director review of {director_name} ({territory}).\n\n"
        "This is shell authoring only, not data population.\n"
        f"Build a polished {slide_count}-slide executive operating deck that will be reused every month.\n"
        "Preserve SimCorp branding, slide masters, layouts, fonts, and visual identity.\n"
        "Do not populate numbers from invented data. Use editorial placeholder guidance only.\n"
        "Make the changes directly in the presentation. Do not spend time drafting a long written response.\n\n"
        "Required slide sequence and contract:\n"
        f"{shell_outline(shell_contract)}\n\n"
        "Design rules:\n"
        "- the director deck is the main product; design for MD-1 and Sales Ops operating review\n"
        "- one management question per slide\n"
        "- use message-first titles, not topic labels\n"
        "- use editorial placeholder language, not raw field names or implementation markers\n"
        "- keep slides fact-driven, Salesforce-oriented, and leadership-readable\n"
        "- keep approvals, renewals, slipped deals, hygiene controls, and appendix visually distinct\n"
        "- do not fabricate churn, KYC, or quota content where the contract says the source is missing\n"
        "- do not add extra slides beyond the contract unless absolutely required for shell quality\n\n"
        "Required shell outcomes:\n"
        "- polished cover slide and agenda\n"
        "- clear slide rhythm across summary, control, watchlist, and appendix slides\n"
        "- consistent placeholder boxes, table shells, and chart regions sized for monthly data injection\n"
        "- no visible bracketed IDs, field labels, or system scaffolding text\n"
        "- leave concise placeholder instructions on each slide for later population\n\n"
        "Do not analyze the repo documents in detail during this run. Use the slide contract above as the source of truth.\n"
        "- respond with exactly these headings:\n"
        "## Shell Changes\n"
        "## Remaining Gaps\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--director-name", required=True)
    parser.add_argument("--territory", required=True)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--source-map-path", type=Path, default=DEFAULT_SOURCE_MAP_PATH)
    parser.add_argument("--execution-plan-path", type=Path, default=DEFAULT_EXECUTION_PLAN_PATH)
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()

    shell_contract = load_json(args.shell_contract_path)
    prompt = build_prompt(
        director_name=args.director_name,
        territory=args.territory,
        shell_contract=shell_contract,
        source_map_path=args.source_map_path,
        execution_plan_path=args.execution_plan_path,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(prompt, encoding="utf-8")
    print(json.dumps({"prompt_path": str(args.output_path)}, indent=2))


if __name__ == "__main__":
    main()
