#!/usr/bin/env python3
"""Build a PowerPoint-Claude prompt for authoring the canonical global summary shell."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_global_summary_shell.json"
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
    return "\n".join(lines)


def build_prompt(
    *,
    shell_contract: dict[str, Any],
    source_map_path: Path,
    execution_plan_path: Path,
) -> str:
    source_map_excerpt = source_map_path.read_text(encoding="utf-8")
    execution_plan_excerpt = execution_plan_path.read_text(encoding="utf-8")
    return (
        "Transform the current PowerPoint file into the canonical monthly shell for the Sales Global Summary review.\n\n"
        "This is shell authoring, not data population. Build a polished executive deck that will be reused every month.\n"
        "Preserve SimCorp branding, slide masters, layouts, fonts, and visual identity.\n"
        "Do not populate numbers from invented data. Use editorial placeholder guidance only.\n\n"
        "Required slide sequence and contract:\n"
        f"{shell_outline(shell_contract)}\n\n"
        "Authoring rules:\n"
        "- the global summary deck is the executive rollup, not a replacement for the director deck\n"
        "- keep one operating slide per region plus a global control summary and appendix\n"
        "- use editorial placeholder language, not raw system-field labels\n"
        "- keep the deck concise and boardroom-readable\n"
        "- do not add a standalone regional narrative layer inside the global shell\n"
        "- do not fabricate metrics or unsupported controls\n"
        "- respond with exactly these headings:\n"
        "## Shell Changes\n"
        "## Remaining Gaps\n\n"
        "Source-map reference:\n\n"
        f"{source_map_excerpt}\n\n"
        "Execution-plan reference:\n\n"
        f"{execution_plan_excerpt}\n"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--source-map-path", type=Path, default=DEFAULT_SOURCE_MAP_PATH)
    parser.add_argument("--execution-plan-path", type=Path, default=DEFAULT_EXECUTION_PLAN_PATH)
    parser.add_argument("--output-path", type=Path, required=True)
    args = parser.parse_args()

    shell_contract = load_json(args.shell_contract_path)
    prompt = build_prompt(
        shell_contract=shell_contract,
        source_map_path=args.source_map_path,
        execution_plan_path=args.execution_plan_path,
    )
    args.output_path.parent.mkdir(parents=True, exist_ok=True)
    args.output_path.write_text(prompt, encoding="utf-8")
    print(json.dumps({"prompt_path": str(args.output_path)}, indent=2))


if __name__ == "__main__":
    main()
