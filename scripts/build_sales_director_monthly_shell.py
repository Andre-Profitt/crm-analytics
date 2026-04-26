#!/usr/bin/env python3
"""Build the Sales Director monthly shell using the native SimCorp template path."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

SCRIPT_DIR = Path(__file__).resolve().parent
ARCHIVE_DIR = SCRIPT_DIR / "_archive"
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))
if str(ARCHIVE_DIR) not in sys.path:
    sys.path.append(str(ARCHIVE_DIR))

from build_simcorp_director_deck import build_deck as build_simcorp_director_deck


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MASTER_TEMPLATE_PATH = (
    Path.home()
    / "archive"
    / "simcorp-deck-agent-backup"
    / "reference-decks"
    / "SimCorp_PPT_Template.pptx"
)
DEFAULT_SHELL_CONTRACT_PATH = REPO_ROOT / "config" / "sales_director_monthly_shell.json"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_director_monthly_shells"
DEFAULT_JS_BUILDER_PATH = REPO_ROOT / "scripts" / "build_sales_director_monthly_shell_v2.js"
DEFAULT_NODE_MODULES_PATH = REPO_ROOT / "output" / "sales_director_monthly_deck_2026-03-31" / "node_modules"


def load_shell_contract(path: Path = DEFAULT_SHELL_CONTRACT_PATH) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def merge_fill_payload_with_shell_contract(
    fill_payload: dict[str, Any],
    shell_contract: dict[str, Any],
) -> dict[str, Any]:
    payload_by_id = {
        slide["id"]: slide
        for slide in fill_payload.get("slides", [])
        if isinstance(slide, dict) and slide.get("id")
    }
    merged_slides: list[dict[str, Any]] = []
    for slide_def in shell_contract.get("slides", []):
        payload_slide = payload_by_id.get(slide_def.get("id"), {})
        merged_slide = dict(slide_def)
        for key, value in payload_slide.items():
            if key == "slots":
                continue
            merged_slide[key] = value
        merged_slide["slots"] = payload_slide.get("slots", {})
        if not merged_slide.get("support_level"):
            merged_slide["support_level"] = (
                slide_def.get("data_contract", {}) or {}
            ).get("support_level")
        merged_slides.append(merged_slide)

    merged_payload = dict(fill_payload)
    merged_payload["template_name"] = shell_contract.get(
        "template_name", fill_payload.get("template_name")
    )
    merged_payload["slides"] = merged_slides
    return merged_payload


def base_fill_payload(
    *,
    director_name: str,
    territory: str,
    snapshot_date: str,
) -> dict[str, Any]:
    return {
        "director_name": director_name,
        "territory": territory,
        "snapshot_date": snapshot_date,
        "slides": [],
    }


def build_shell_deck(
    *,
    director_name: str,
    territory: str,
    snapshot_date: str,
    output_path: Path,
    master_template_path: Path = DEFAULT_MASTER_TEMPLATE_PATH,
    shell_contract_path: Path = DEFAULT_SHELL_CONTRACT_PATH,
    js_builder_path: Path = DEFAULT_JS_BUILDER_PATH,
    node_modules_path: Path = DEFAULT_NODE_MODULES_PATH,
    fill_payload_path: Path | None = None,
    allow_legacy_js_builder: bool = False,
) -> dict[str, Any]:
    shell = load_shell_contract(shell_contract_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if fill_payload_path is not None or not allow_legacy_js_builder:
        if fill_payload_path is not None and not fill_payload_path.exists():
            raise FileNotFoundError(f"Fill payload not found: {fill_payload_path}")
        if not master_template_path.exists():
            raise FileNotFoundError(
                f"Master template not found: {master_template_path}"
            )
        fill_payload = (
            json.loads(fill_payload_path.read_text(encoding="utf-8"))
            if fill_payload_path
            else base_fill_payload(
                director_name=director_name,
                territory=territory,
                snapshot_date=snapshot_date,
            )
        )
        merged_payload = merge_fill_payload_with_shell_contract(fill_payload, shell)
        build_simcorp_director_deck(
            merged_payload,
            master_template_path,
            output_path,
        )
        return {
            "deck_path": str(output_path),
            "slide_count": len(shell.get("slides", [])) + 2,
            "master_template_path": str(master_template_path),
            "shell_contract_path": str(shell_contract_path),
            "template_version": shell.get("template_version"),
            "builder": (
                "simcorp-native" if fill_payload_path else "simcorp-native-shell"
            ),
            "publish_safe": bool(fill_payload_path),
            "js_builder_path": str(js_builder_path),
            "fill_payload_path": str(fill_payload_path) if fill_payload_path else None,
        }

    if not js_builder_path.exists():
        raise FileNotFoundError(f"JS shell builder not found: {js_builder_path}")
    if not node_modules_path.exists():
        raise FileNotFoundError(
            f"Node modules path not found for shell builder dependencies: {node_modules_path}"
        )

    env = os.environ.copy()
    existing_node_path = env.get("NODE_PATH", "")
    env["NODE_PATH"] = (
        f"{node_modules_path}:{existing_node_path}"
        if existing_node_path
        else str(node_modules_path)
    )
    env["SD_SHELL_VALIDATE_LAYOUT"] = "1"

    cmd = [
        "node",
        str(js_builder_path),
        "--director-name",
        director_name,
        "--territory",
        territory,
        "--snapshot-date",
        snapshot_date,
        "--contract",
        str(shell_contract_path),
        "--output",
        str(output_path),
    ]
    subprocess.run(cmd, check=True, env=env, cwd=REPO_ROOT)

    return {
        "deck_path": str(output_path),
        "slide_count": len(shell.get("slides", [])) + 2,
        "master_template_path": str(master_template_path),
        "shell_contract_path": str(shell_contract_path),
        "template_version": shell.get("template_version"),
        "builder": "js-v2",
        "publish_safe": False,
        "js_builder_path": str(js_builder_path),
        "fill_payload_path": None,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--director-name", required=True)
    parser.add_argument("--territory", required=True)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--output-path", type=Path, required=True)
    parser.add_argument("--master-template-path", type=Path, default=DEFAULT_MASTER_TEMPLATE_PATH)
    parser.add_argument("--shell-contract-path", type=Path, default=DEFAULT_SHELL_CONTRACT_PATH)
    parser.add_argument("--js-builder-path", type=Path, default=DEFAULT_JS_BUILDER_PATH)
    parser.add_argument("--node-modules-path", type=Path, default=DEFAULT_NODE_MODULES_PATH)
    parser.add_argument("--fill-payload-path", type=Path, default=None)
    parser.add_argument(
        "--allow-legacy-js-builder",
        action="store_true",
        help="Explicitly allow the non-publish-safe JS shell renderer.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = build_shell_deck(
        director_name=args.director_name,
        territory=args.territory,
        snapshot_date=args.snapshot_date,
        output_path=args.output_path,
        master_template_path=args.master_template_path,
        shell_contract_path=args.shell_contract_path,
        js_builder_path=args.js_builder_path,
        node_modules_path=args.node_modules_path,
        fill_payload_path=args.fill_payload_path,
        allow_legacy_js_builder=args.allow_legacy_js_builder,
    )
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
