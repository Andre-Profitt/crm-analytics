#!/usr/bin/env python3
"""Merge filled Sales Director overlay CSV inputs into a Sales Director overlay JSON."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-overlay",
        required=True,
        help="Base overlay JSON to extend, usually report1_overlay.fill.json.",
    )
    parser.add_argument(
        "--commentary-csv",
        default=None,
        help="Filled commentary CSV with theme/comment columns.",
    )
    parser.add_argument(
        "--finance-csv",
        default=None,
        help="Filled Finance churn CSV with overlay header fields plus per-account rows.",
    )
    parser.add_argument(
        "--summary-note",
        default="Owner commentary collected for current-quarter slipped deals.",
        help="Summary note for the slipped commentary overlay.",
    )
    parser.add_argument(
        "--commentary-provenance",
        choices=["auto", "external", "example"],
        default="auto",
        help="Provenance to stamp on the commentary payload. Defaults to auto, which marks sample files as example.",
    )
    parser.add_argument(
        "--finance-provenance",
        choices=["auto", "external", "example"],
        default="auto",
        help="Provenance to stamp on the Finance churn payload. Defaults to auto, which marks sample files as example.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output overlay JSON path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print merge summary as JSON.",
    )
    return parser.parse_args()


def load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected object JSON in {path}")
    return payload


def load_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def first_non_empty_value(csv_rows: list[dict[str, str]], fieldname: str) -> str:
    for row in csv_rows:
        value = " ".join(str(row.get(fieldname, "")).split()).strip()
        if value:
            return value
    return ""


def parse_numeric_value(value: str) -> float | int | None:
    normalized = str(value or "").replace(",", "").strip()
    if not normalized:
        return None
    try:
        number = float(normalized)
    except ValueError:
        return None
    if number.is_integer():
        return int(number)
    return number


def truthy_csv_flag(value: str) -> bool:
    normalized = " ".join(str(value or "").lower().split()).strip()
    return normalized in {"y", "yes", "true", "1", "include", "included"}


def short_theme(theme: str) -> str:
    normalized = " ".join(str(theme or "").split()).strip()
    return normalized or "Unknown"


def build_root_cause_bullets(comments: list[dict[str, str]]) -> list[str]:
    theme_counts = Counter(short_theme(row.get("theme", "")) for row in comments if row.get("theme"))
    if not theme_counts:
        return []
    bullets = []
    for theme, count in theme_counts.most_common(3):
        bullets.append(f"{theme} appears in {count} owner update{'s' if count != 1 else ''}.")
    return bullets


def build_owner_comments(csv_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    owner_comments = []
    for row in csv_rows:
        theme = " ".join(str(row.get("theme", "")).split()).strip()
        comment = " ".join(str(row.get("comment", "")).split()).strip()
        if not theme and not comment:
            continue
        owner_comments.append(
            {
                "owner_name": row.get("owner_name", "").strip(),
                "region": row.get("region", "").strip(),
                "opportunity_name": row.get("opportunity_name", "").strip(),
                "theme": theme,
                "comment": comment,
            }
        )
    return owner_comments


def requested_commentary_items(csv_rows: list[dict[str, str]]) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    for row in csv_rows:
        owner_name = " ".join(str(row.get("owner_name", "")).split()).strip()
        opportunity_name = " ".join(str(row.get("opportunity_name", "")).split()).strip()
        region = " ".join(str(row.get("region", "")).split()).strip()
        if not owner_name and not opportunity_name and not region:
            continue
        items.append(
            {
                "owner_name": owner_name,
                "region": region,
                "opportunity_name": opportunity_name,
            }
        )
    return items


def build_coverage_metrics(
    *,
    requested_items: list[dict[str, str]],
    owner_comments: list[dict[str, str]],
) -> dict[str, Any]:
    requested_keys = {
        (
            str(item.get("owner_name", "")).strip().lower(),
            str(item.get("opportunity_name", "")).strip().lower(),
        )
        for item in requested_items
    }
    responded_keys = {
        (
            str(item.get("owner_name", "")).strip().lower(),
            str(item.get("opportunity_name", "")).strip().lower(),
        )
        for item in owner_comments
    }
    pending_keys = requested_keys - responded_keys
    requested_owner_names = sorted(
        {
            str(item.get("owner_name", "")).strip()
            for item in requested_items
            if str(item.get("owner_name", "")).strip()
        }
    )
    responded_owner_names = sorted(
        {
            str(item.get("owner_name", "")).strip()
            for item in owner_comments
            if str(item.get("owner_name", "")).strip()
        }
    )
    pending_owner_names = sorted(set(requested_owner_names) - set(responded_owner_names))
    requested_count = len(requested_items)
    provided_count = len(owner_comments)
    pending_count = max(requested_count - provided_count, 0)
    if requested_count == 0 and provided_count == 0:
        coverage_status = "pending"
    elif requested_count > 0 and provided_count == 0:
        coverage_status = "pending"
    elif requested_count > 0 and pending_count > 0:
        coverage_status = "partial"
    else:
        coverage_status = "complete"
    return {
        "coverage_status": coverage_status,
        "requested_item_count": requested_count,
        "provided_comment_count": provided_count,
        "pending_comment_count": pending_count,
        "requested_owner_count": len(requested_owner_names),
        "responded_owner_count": len(responded_owner_names),
        "pending_owner_count": len(pending_owner_names),
        "pending_owner_names": pending_owner_names,
    }


def infer_commentary_provenance(*, requested: str, commentary_csv_path: Path | None = None) -> str:
    if requested != "auto":
        return requested
    filename = commentary_csv_path.name.lower() if commentary_csv_path else ""
    if "sample" in filename or "example" in filename:
        return "example"
    return "external"


def infer_finance_provenance(*, requested: str, finance_csv_path: Path | None = None) -> str:
    if requested != "auto":
        return requested
    filename = finance_csv_path.name.lower() if finance_csv_path else ""
    if "sample" in filename or "example" in filename:
        return "example"
    return "external"


def build_finance_top_accounts(csv_rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    top_accounts: list[dict[str, Any]] = []
    for row in csv_rows:
        if not truthy_csv_flag(row.get("include_in_forward_risk", "")):
            continue
        account_name = " ".join(str(row.get("account_name", "")).split()).strip()
        if not account_name:
            continue
        top_account = {
            "account_name": account_name,
            "region": " ".join(str(row.get("region", "")).split()).strip(),
            "signal": " ".join(str(row.get("signal", "")).split()).strip(),
        }
        amount = parse_numeric_value(str(row.get("amount", "")))
        if amount is not None:
            top_account["amount"] = amount
        top_accounts.append(top_account)
    return top_accounts


def merge_finance_overlay_payload(
    *,
    overlay: dict[str, Any],
    csv_rows: list[dict[str, str]],
    finance_provenance: str,
) -> tuple[dict[str, Any], int]:
    finance = overlay.get("finance_churn")
    if not isinstance(finance, dict):
        finance = {}
        overlay["finance_churn"] = finance

    top_accounts = build_finance_top_accounts(csv_rows)
    owner = first_non_empty_value(csv_rows, "overlay_owner") or str(finance.get("owner") or "").strip()
    source_name = first_non_empty_value(csv_rows, "overlay_source_name") or str(finance.get("source_name") or "").strip()
    headline = first_non_empty_value(csv_rows, "overlay_headline") or str(finance.get("headline") or "").strip()
    summary_note = first_non_empty_value(csv_rows, "overlay_summary_note") or str(finance.get("summary_note") or "").strip()

    has_payload = bool(top_accounts or owner or source_name or headline or summary_note)
    if has_payload:
        finance["status"] = "provided"
        finance["provenance"] = finance_provenance
        finance["owner"] = owner
        finance["source_name"] = source_name
        finance["headline"] = headline
        finance["summary_note"] = summary_note
        finance["top_accounts"] = top_accounts
    else:
        finance.setdefault("status", "pending")
        finance.setdefault("provenance", "pending")
        finance.setdefault("owner", "")
        finance.setdefault("source_name", "")
        finance.setdefault("headline", "")
        finance.setdefault("summary_note", "")
        finance.setdefault("top_accounts", [])
    return overlay, len(top_accounts)


def merge_overlay_payload(
    *,
    overlay: dict[str, Any],
    csv_rows: list[dict[str, str]],
    summary_note: str,
    commentary_provenance: str,
) -> tuple[dict[str, Any], int]:
    slipped = overlay.get("slipped_commentary")
    if not isinstance(slipped, dict):
        slipped = {}
        overlay["slipped_commentary"] = slipped

    requested_items = requested_commentary_items(csv_rows)
    owner_comments = build_owner_comments(csv_rows)
    slipped["owner_comments"] = owner_comments
    slipped["root_cause_bullets"] = build_root_cause_bullets(owner_comments)
    slipped["summary_note"] = summary_note if owner_comments else ""
    slipped["status"] = "provided" if owner_comments else "pending"
    slipped["provenance"] = commentary_provenance if owner_comments else slipped.get("provenance", commentary_provenance)
    slipped.update(
        build_coverage_metrics(
            requested_items=requested_items,
            owner_comments=owner_comments,
        )
    )
    return overlay, len(owner_comments)


def main() -> int:
    args = parse_args()
    base_overlay_path = Path(args.base_overlay).resolve()
    output_path = Path(args.output).resolve()
    commentary_csv_path = Path(args.commentary_csv).resolve() if args.commentary_csv else None
    finance_csv_path = Path(args.finance_csv).resolve() if args.finance_csv else None

    if not commentary_csv_path and not finance_csv_path:
        raise SystemExit("At least one of --commentary-csv or --finance-csv is required.")

    overlay = load_json(base_overlay_path)
    owner_comment_count = 0
    finance_top_account_count = 0
    if finance_csv_path:
        finance_rows = load_csv_rows(finance_csv_path)
        overlay, finance_top_account_count = merge_finance_overlay_payload(
            overlay=overlay,
            csv_rows=finance_rows,
            finance_provenance=infer_finance_provenance(
                requested=args.finance_provenance,
                finance_csv_path=finance_csv_path,
            ),
        )
    if commentary_csv_path:
        commentary_rows = load_csv_rows(commentary_csv_path)
        overlay, owner_comment_count = merge_overlay_payload(
            overlay=overlay,
            csv_rows=commentary_rows,
            summary_note=args.summary_note,
            commentary_provenance=infer_commentary_provenance(
                requested=args.commentary_provenance,
                commentary_csv_path=commentary_csv_path,
            ),
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(overlay, indent=2) + "\n", encoding="utf-8")

    summary = {
        "status": "ok",
        "base_overlay": str(base_overlay_path),
        "commentary_csv": str(commentary_csv_path) if commentary_csv_path else None,
        "finance_csv": str(finance_csv_path) if finance_csv_path else None,
        "output": str(output_path),
        "owner_comment_count": owner_comment_count,
        "finance_top_account_count": finance_top_account_count,
        "root_cause_bullet_count": len((overlay.get("slipped_commentary") or {}).get("root_cause_bullets") or []),
        "coverage_status": (overlay.get("slipped_commentary") or {}).get("coverage_status"),
        "pending_comment_count": (overlay.get("slipped_commentary") or {}).get("pending_comment_count"),
        "finance_status": (overlay.get("finance_churn") or {}).get("status"),
    }
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print(f"Wrote {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
