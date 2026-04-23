#!/usr/bin/env python3
"""Validate retention semantics on selected owners.

This is analysis-only. It creates a judgment-oriented profile for a small set of
owners so retention rules can be tested against real account stories before the
dashboard semantics change.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crm_analytics_helpers import get_auth
from build_revenue_retention_health import run_soql, safe_float


DEFAULT_OWNERS = ("Stefan Persson", "Jesper Aagaard")


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_artifact(kind: str, path: Path) -> dict[str, str]:
    return {"kind": kind, "path": str(path)}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    artifacts: list[dict[str, str]] | None = None,
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "profile_retention_owner_validation",
        "lane": "salesforce_data_profiles",
        "command_class": "live_read",
        "messages": messages,
        "artifacts": artifacts or [],
    }
    payload.update(extra)
    return payload


def classify_outcome(row: dict[str, Any]) -> str:
    if row.get("IsWon"):
        return "won"
    if not row.get("IsClosed"):
        return "open"
    stage = row.get("StageName") or ""
    if "No Opportunity" in stage:
        return "no_opportunity"
    if "Lost" in stage:
        return "lost"
    return "closed_not_won"


def format_money(value: float) -> str:
    return f"{value:,.2f}"


def fetch_rows(owners: tuple[str, ...]) -> list[dict[str, Any]]:
    owners_clause = "(" + ",".join(f"'{owner}'" for owner in owners) + ")"
    query = (
        "SELECT Id, Name, Type, StageName, IsClosed, IsWon, CloseDate, "
        "Account.Name, Owner.Name, "
        "APTS_Renewal_ACV__c, APTS_Opportunity_ARR__c, Amount, APTS_RH_Product_Family__c "
        "FROM Opportunity "
        f"WHERE Owner.Name IN {owners_clause} "
        "AND Type IN ('Land','Expand','Renewal') "
        "AND CloseDate >= 2021-01-01 "
        "ORDER BY Owner.Name, Account.Name, CloseDate"
    )
    inst, tok = get_auth()
    return run_soql(inst, tok, query)


def build_profile(rows: list[dict[str, Any]], owners: tuple[str, ...]) -> dict[str, Any]:
    owner_year = defaultdict(lambda: defaultdict(lambda: {
        "renewal_won_acv": 0.0,
        "renewal_at_risk_acv": 0.0,
        "expand_won_arr": 0.0,
        "land_won_arr": 0.0,
        "renewal_count": 0,
        "expand_count": 0,
        "land_count": 0,
    }))
    owner_account = defaultdict(lambda: defaultdict(lambda: {
        "renewal_won_acv": 0.0,
        "renewal_at_risk_acv": 0.0,
        "expand_won_arr": 0.0,
        "land_won_arr": 0.0,
        "events": [],
    }))

    for row in rows:
        owner = (row.get("Owner") or {}).get("Name") or "<unassigned>"
        if owner not in owners:
            continue
        year = (row.get("CloseDate") or "")[:4]
        account = (row.get("Account") or {}).get("Name") or "<unknown>"
        typ = row.get("Type") or ""
        outcome = classify_outcome(row)
        acv = safe_float(row.get("APTS_Renewal_ACV__c"))
        arr = safe_float(row.get("APTS_Opportunity_ARR__c"))

        y = owner_year[owner][year]
        a = owner_account[owner][account]

        if typ == "Renewal":
            y["renewal_count"] += 1
            if outcome == "won":
                y["renewal_won_acv"] += acv
                a["renewal_won_acv"] += acv
            elif outcome in ("lost", "no_opportunity"):
                y["renewal_at_risk_acv"] += acv
                a["renewal_at_risk_acv"] += acv
        elif typ == "Expand":
            y["expand_count"] += 1
            if outcome == "won":
                y["expand_won_arr"] += arr
                a["expand_won_arr"] += arr
        elif typ == "Land":
            y["land_count"] += 1
            if outcome == "won":
                y["land_won_arr"] += arr
                a["land_won_arr"] += arr

        a["events"].append(
            {
                "close_date": row.get("CloseDate"),
                "type": typ,
                "outcome": outcome,
                "renewal_acv": acv,
                "arr": arr,
                "amount": safe_float(row.get("Amount")),
                "name": row.get("Name"),
                "product_family": row.get("APTS_RH_Product_Family__c") or "",
            }
        )

    result = {"owners": {}}
    for owner in owners:
        years = dict(sorted(owner_year[owner].items()))
        accounts = dict(
            sorted(
                owner_account[owner].items(),
                key=lambda item: (
                    item[1]["renewal_won_acv"] + item[1]["renewal_at_risk_acv"] + item[1]["expand_won_arr"]
                ),
                reverse=True,
            )
        )
        result["owners"][owner] = {"by_year": years, "by_account": accounts}
    return result


def render_markdown(profile: dict[str, Any]) -> str:
    lines = [
        "# Retention Owner Validation",
        "",
        "Validated against live Salesforce `Opportunity` data on March 11, 2026.",
        "",
        "This note compares a cleaner renewal owner and a messier owner so the retention model can be tested against real account stories.",
        "",
    ]

    for owner, data in profile["owners"].items():
        lines.extend([f"## {owner}", ""])
        lines.extend(
            [
                "### Year Shape",
                "",
                "| Year | Renewal Won ACV | Renewal At-Risk ACV | Expand Won ARR | Land Won ARR | Renewal Count | Expand Count |",
                "|---|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for year, row in data["by_year"].items():
            lines.append(
                "| "
                + " | ".join(
                    [
                        year,
                        format_money(row["renewal_won_acv"]),
                        format_money(row["renewal_at_risk_acv"]),
                        format_money(row["expand_won_arr"]),
                        format_money(row["land_won_arr"]),
                        f"{row['renewal_count']:,}",
                        f"{row['expand_count']:,}",
                    ]
                )
                + " |"
            )

        lines.extend(
            [
                "",
                "### Largest Account Stories",
                "",
                "| Account | Renewal Won ACV | Renewal At-Risk ACV | Expand Won ARR | Land Won ARR |",
                "|---|---:|---:|---:|---:|",
            ]
        )
        top_accounts = list(data["by_account"].items())[:8]
        for account, row in top_accounts:
            lines.append(
                "| "
                + " | ".join(
                    [
                        account,
                        format_money(row["renewal_won_acv"]),
                        format_money(row["renewal_at_risk_acv"]),
                        format_money(row["expand_won_arr"]),
                        format_money(row["land_won_arr"]),
                    ]
                )
                + " |"
            )

        lines.extend(["", "### Representative Event Walkthroughs", ""])
        for account, row in top_accounts[:4]:
            lines.extend([f"#### {account}", ""])
            for event in row["events"][:10]:
                lines.append(
                    "- "
                    + f"`{event['close_date']}` `{event['type']}` `{event['outcome']}` "
                    + f"`ACV {format_money(event['renewal_acv'])}` "
                    + f"`ARR {format_money(event['arr'])}` "
                    + f"`Amount {format_money(event['amount'])}` "
                    + f"{event['name']}"
                )
            lines.append("")

    lines.extend(
        [
            "## Judgment",
            "",
            "- `Stefan Persson` looks more renewal-native: large strict-renewal books with some accounts that also carry substantial expansion.",
            "- `Jesper Aagaard` is more mixed: some accounts are clean renewal stories, while others are clearly expansion-led despite renewal presence.",
            "- This supports using the same semantic framework across owners, but not assuming the same process maturity or interpretation quality for everyone.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate retention semantics on selected owners.")
    parser.add_argument(
        "--owners",
        nargs="+",
        default=list(DEFAULT_OWNERS),
        help="Owner names to analyze",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/retention_owner_validation_2026-03-11"),
        help="Directory for profile.md and profile.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_profile_command(
    owners: tuple[str, ...],
    out_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    rows = fetch_rows(owners)
    profile = build_profile(rows, owners)
    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(profile), encoding="utf-8")

    if emit_text:
        print(f"Wrote retention owner validation to {out_dir}")

    account_story_count = sum(len(owner_data["by_account"]) for owner_data in profile["owners"].values())
    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote retention owner validation to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "owner_count": len(profile["owners"]),
            "owners": list(profile["owners"]),
            "account_story_count": account_story_count,
            "output_dir": str(out_dir),
        },
        profile=profile,
    )
    return result, 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    owners = tuple(args.owners)
    result, exit_code = run_profile_command(
        owners,
        args.out_dir,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
