#!/usr/bin/env python3
"""Build a PowerPoint-Claude prompt for authoring the canonical regional shell."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_region_monthly_shell.json"
DEFAULT_DESIGN_SYSTEM_PATH = (
    REPO_ROOT / "docs" / "specs" / "2026-04-11-sales-region-shell-design-system.md"
)
DEFAULT_GOLD_REFERENCE_PATH = (
    REPO_ROOT / "output" / "sales_region_gold_decks" / "2026-04-10" / "Sales Region Monthly - EMEA Gold Example.pptx"
)


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


def build_prompt(*, region_name: str, shell_contract: dict[str, Any], design_system_path: Path) -> str:
    gold_note = (
        f"Gold benchmark reference deck: {DEFAULT_GOLD_REFERENCE_PATH}\n"
        if DEFAULT_GOLD_REFERENCE_PATH.exists()
        else ""
    )
    design_excerpt = design_system_path.read_text(encoding="utf-8")
    return (
        f"Transform the current PowerPoint file into the canonical monthly shell for the {region_name} regional sales review.\n\n"
        "This is shell authoring, not data population. Build a polished executive operating deck that will be reused every month.\n"
        "Preserve SimCorp branding, slide masters, layouts, fonts, and visual identity.\n"
        "Do not populate numbers from invented data. Use editorial placeholder guidance only.\n\n"
        f"{gold_note}"
        "Required slide sequence and contract:\n"
        f"{shell_outline(shell_contract)}\n\n"
        "Authoring rules:\n"
        "- use recommendation-first placeholder titles and guidance\n"
        "- keep placeholders visible as editorial prompts, not raw field names\n"
        "- preserve one decision per slide\n"
        "- use approved visual families only: cover, agenda, KPI strip, hero + stacked mini-stats, 3-column cards, watchlist table, side-by-side panels, appendix\n"
        "- make the shell feel polished enough for MD and Sales Ops review before any numbers are inserted\n"
        "- do not leave generic lorem ipsum or system-style scaffolding\n"
        "- do not add appendix clutter beyond what the shell contract requires\n"
        "- respond with exactly these headings:\n"
        "## Shell Changes\n"
        "## Remaining Gaps\n\n"
        "Design-system reference:\n\n"
        f"{design_excerpt}\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-name", required=True)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--design-system-path", type=Path, default=DEFAULT_DESIGN_SYSTEM_PATH)
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()

    shell_contract = load_json(args.shell_contract_path)
    prompt = build_prompt(
        region_name=args.region_name,
        shell_contract=shell_contract,
        design_system_path=args.design_system_path,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(prompt, encoding="utf-8")
    print(json.dumps({"prompt_path": str(args.output_path)}, indent=2))


if __name__ == "__main__":
    main()
