#!/usr/bin/env python3
"""Build a dated status packet for external Salesforce and Finance source gaps."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONTRACT_PATH = REPO_ROOT / "config" / "sales_deck_external_source_contract.json"
DEFAULT_INPUT_ROOT = REPO_ROOT / "output" / "sales_deck_external_inputs"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "sales_deck_external_source_packets"


def timestamp_slug() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")


def save_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def source_status_for_snapshot(*, source: dict[str, Any], snapshot_date: str, input_root: Path) -> dict[str, Any]:
    source_dir = input_root / snapshot_date / str(source["id"])
    provided_files = []
    if source_dir.exists():
        provided_files = sorted(
            str(path) for path in source_dir.iterdir() if path.is_file() and not path.name.startswith(".")
        )
    status = "provided" if provided_files else str(source.get("default_status") or "pending")
    publish_blocking = bool(source.get("required_for_publish")) and not provided_files
    return {
        "id": source["id"],
        "title": source.get("title"),
        "owner": source.get("owner"),
        "system": source.get("system"),
        "status": status,
        "required_for_publish": bool(source.get("required_for_publish")),
        "publish_blocking": publish_blocking,
        "current_coverage": source.get("current_coverage"),
        "affects_decks": source.get("affects_decks") or [],
        "affects_slides": source.get("affects_slides") or [],
        "report_id": source.get("report_id"),
        "expected_files": source.get("expected_files") or [],
        "provided_files": provided_files,
        "notes": source.get("notes") or [],
        "source_dir": str(source_dir),
    }


def build_finance_churn_request_pack(*, snapshot_date: str) -> dict[str, Any]:
    return {
        "artifact_type": "sales_deck_finance_churn_request_pack",
        "snapshot_date": snapshot_date,
        "owner": "",
        "source_name": "",
        "headline": "",
        "summary_note": "",
        "required_account_fields": ["account_name", "region", "signal", "amount"],
        "instructions": [
            "Fill the overlay fields from the current Finance-owned churn view.",
            "Provide 3-5 live accounts with short signal text and amount.",
            "Use this to replace the churn placeholder slide, not to restate workbook numbers."
        ],
    }


def build_finance_churn_request_markdown(*, pack: dict[str, Any]) -> str:
    lines = [
        "# Finance Churn Overlay Request",
        "",
        f"- Snapshot date: `{pack['snapshot_date']}`",
        "",
        "## Required overlay fields",
        "",
        "- `owner`: Finance contact or team name",
        "- `source_name`: name of the Finance source used for this month",
        "- `headline`: one-line Finance risk readout for the deck",
        "- `summary_note`: scope, refresh date, and caveats to carry on the slide",
        "- `top_accounts`: 3-5 live accounts with `account_name`, `region`, `signal`, and `amount`",
        "",
        "## Operating rule",
        "",
        "- Do not infer churn risk from the workbook alone.",
        "- Replace the deck placeholder only when Finance provides the live overlay.",
        "",
    ]
    return "\n".join(lines)


def build_finance_churn_request_email(*, snapshot_date: str) -> str:
    return (
        "Subject: Input needed: Finance churn overlay for the monthly sales deck\n\n"
        "Hi Finance team,\n\n"
        f"We are preparing the {snapshot_date} monthly sales deck and need the live Finance churn overlay.\n\n"
        "Please update the attached `finance_churn_request.csv` and confirm:\n"
        "- owner\n"
        "- source_name\n"
        "- headline\n"
        "- summary_note\n"
        "- 3-5 live accounts with region, signal, and amount\n"
    )


def build_status_packet(
    *,
    snapshot_date: str,
    contract: dict[str, Any],
    contract_path: Path,
    input_root: Path,
) -> dict[str, Any]:
    rows = [
        source_status_for_snapshot(source=source, snapshot_date=snapshot_date, input_root=input_root)
        for source in contract.get("sources", [])
    ]
    blockers = [
        f"{row['title']} is required for publish but no intake files are present."
        for row in rows
        if row["publish_blocking"]
    ]
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "snapshot_date": snapshot_date,
        "contract_path": str(contract_path),
        "input_root": str(input_root),
        "publish_ready": len(blockers) == 0,
        "blocker_count": len(blockers),
        "blockers": blockers,
        "source_count": len(rows),
        "sources": rows,
        "status_summary": {
            "provided": sum(1 for row in rows if row["status"] == "provided"),
            "pending": sum(1 for row in rows if row["status"].startswith("pending")),
            "proxy_covered_by_workbook": sum(1 for row in rows if row["status"] == "proxy_covered_by_workbook"),
        },
    }


def build_status_markdown(packet: dict[str, Any]) -> str:
    lines = [
        "# Sales Deck External Source Status",
        "",
        f"- Snapshot date: `{packet['snapshot_date']}`",
        f"- Publish ready: `{packet['publish_ready']}`",
        f"- Blocker count: `{packet['blocker_count']}`",
        f"- Input root: `{packet['input_root']}`",
        "",
        "## Sources",
        "",
        "| Source | System | Status | Required | Report ID | Coverage | Input dir |",
        "|---|---|---|---:|---|---|---|",
    ]
    for row in packet["sources"]:
        lines.append(
            f"| {row['title']} | {row['system']} | {row['status']} | "
            f"{'yes' if row['required_for_publish'] else 'no'} | {row.get('report_id') or ''} | "
            f"{row.get('current_coverage') or ''} | {row['source_dir']} |"
        )
    lines.extend(["", "## Blockers", ""])
    if packet["blockers"]:
        lines.extend(f"- {item}" for item in packet["blockers"])
    else:
        lines.append("- None.")
    return "\n".join(lines) + "\n"


def write_latest_aliases(*, output_root: Path, snapshot_date: str, packet: dict[str, Any], markdown: str, run_dir: Path) -> None:
    latest_payload = {**packet, "packet_dir": str(run_dir)}
    save_json(output_root / snapshot_date / "latest.json", latest_payload)
    save_text(output_root / snapshot_date / "latest.md", markdown)
    save_json(output_root / "latest.json", latest_payload)
    save_text(output_root / "latest.md", markdown)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-date", required=True)
    parser.add_argument("--contract-path", type=Path, default=DEFAULT_CONTRACT_PATH)
    parser.add_argument("--input-root", type=Path, default=DEFAULT_INPUT_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    contract = load_json(args.contract_path)
    packet = build_status_packet(
        snapshot_date=args.snapshot_date,
        contract=contract,
        contract_path=args.contract_path,
        input_root=args.input_root,
    )

    run_dir = args.output_root / args.snapshot_date / timestamp_slug()
    run_dir.mkdir(parents=True, exist_ok=True)
    markdown = build_status_markdown(packet)
    save_json(run_dir / "external-source-status.json", packet)
    save_text(run_dir / "external-source-status.md", markdown)

    finance_pack = build_finance_churn_request_pack(snapshot_date=args.snapshot_date)
    save_json(run_dir / "finance_churn_request.json", finance_pack)
    save_text(run_dir / "finance_churn_request.md", build_finance_churn_request_markdown(pack=finance_pack))
    save_text(run_dir / "finance_churn_request_email.md", build_finance_churn_request_email(snapshot_date=args.snapshot_date))
    write_csv(
        run_dir / "finance_churn_request.csv",
        [
            {
                "owner": "",
                "source_name": "",
                "headline": "",
                "summary_note": "",
                "account_name": "",
                "region": "",
                "signal": "",
                "amount": "",
            }
        ],
        ["owner", "source_name", "headline", "summary_note", "account_name", "region", "signal", "amount"],
    )

    write_latest_aliases(
        output_root=args.output_root,
        snapshot_date=args.snapshot_date,
        packet=packet,
        markdown=markdown,
        run_dir=run_dir,
    )
    print(json.dumps({**packet, "packet_dir": str(run_dir)}, indent=2))


if __name__ == "__main__":
    main()
