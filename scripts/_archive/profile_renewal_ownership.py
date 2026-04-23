#!/usr/bin/env python3
"""Profile renewal history by manager and owner.

This is analysis-only. It shows whether renewal process maturity is concentrated
in particular teams or broadly distributed across the org.
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


QUERY = (
    "SELECT Id, Name, CloseDate, StageName, IsClosed, IsWon, "
    "Owner.Name, Owner.Manager.Name, "
    "APTS_Renewal_ACV__c, Amount "
    "FROM Opportunity "
    "WHERE Type = 'Renewal' "
    "AND CloseDate >= 2021-01-01 "
    "ORDER BY CloseDate"
)


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
        "tool": "profile_renewal_ownership",
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


def pct(numerator: int, denominator: int) -> float:
    if not denominator:
        return 0.0
    return numerator / denominator


def bucket_factory() -> dict[str, Any]:
    return {
        "count": 0,
        "closed_count": 0,
        "won_count": 0,
        "lost_count": 0,
        "no_opportunity_count": 0,
        "open_count": 0,
        "closed_acv_nonzero": 0,
        "won_acv_sum": 0.0,
        "at_risk_acv_sum": 0.0,
        "total_acv_sum": 0.0,
        "total_amount_sum": 0.0,
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_manager: dict[str, dict[str, Any]] = defaultdict(bucket_factory)
    by_owner: dict[str, dict[str, Any]] = defaultdict(bucket_factory)
    annual_manager: dict[str, dict[str, dict[str, Any]]] = defaultdict(lambda: defaultdict(bucket_factory))

    for row in rows:
        outcome = classify_outcome(row)
        year = (row.get("CloseDate") or "Unknown")[:4]
        owner_data = row.get("Owner") or {}
        owner = owner_data.get("Name") or "<unassigned>"
        manager = (owner_data.get("Manager") or {}).get("Name") or "<no manager>"
        acv = safe_float(row.get("APTS_Renewal_ACV__c"))
        amount = safe_float(row.get("Amount"))

        for bucket in (by_manager[manager], by_owner[owner], annual_manager[year][manager]):
            bucket["count"] += 1
            bucket["total_acv_sum"] += acv
            bucket["total_amount_sum"] += amount
            if outcome != "open":
                bucket["closed_count"] += 1
                bucket["closed_acv_nonzero"] += int(acv > 0)
            if outcome == "won":
                bucket["won_count"] += 1
                bucket["won_acv_sum"] += acv
            elif outcome == "lost":
                bucket["lost_count"] += 1
                bucket["at_risk_acv_sum"] += acv
            elif outcome == "no_opportunity":
                bucket["no_opportunity_count"] += 1
                bucket["at_risk_acv_sum"] += acv
            elif outcome == "open":
                bucket["open_count"] += 1

    def enrich(source: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
        enriched: dict[str, dict[str, Any]] = {}
        for name, bucket in source.items():
            closed_count = bucket["closed_count"]
            row = dict(bucket)
            row["closed_acv_coverage_pct"] = pct(bucket["closed_acv_nonzero"], closed_count)
            enriched[name] = row
        return dict(
            sorted(
                enriched.items(),
                key=lambda item: (item[1]["won_acv_sum"], item[1]["at_risk_acv_sum"], item[1]["count"]),
                reverse=True,
            )
        )

    annual = {
        year: enrich(managers)
        for year, managers in sorted(annual_manager.items())
    }

    return {
        "by_manager": enrich(by_manager),
        "by_owner": enrich(by_owner),
        "annual_manager": annual,
    }


def top_rows(source: dict[str, dict[str, Any]], limit: int = 12) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for name, data in list(source.items())[:limit]:
        row = {"name": name}
        row.update(data)
        rows.append(row)
    return rows


def format_money(value: float) -> str:
    return f"{value:,.2f}"


def render_markdown(profile: dict[str, Any]) -> str:
    by_manager = profile["by_manager"]
    by_owner = profile["by_owner"]
    annual_manager = profile["annual_manager"]

    lines = [
        "# Renewal Ownership Profile",
        "",
        "This report profiles renewal maturity by manager and owner from `2021+`.",
        "",
        "## Key Findings",
        "",
    ]

    top_managers = top_rows(by_manager, limit=5)
    top_owners = top_rows(by_owner, limit=5)
    if top_managers:
        lines.append(
            f"- Top manager by won renewal ACV is `{top_managers[0]['name']}` with `{format_money(top_managers[0]['won_acv_sum'])}`."
        )
    if top_owners:
        lines.append(
            f"- Top owner by won renewal ACV is `{top_owners[0]['name']}` with `{format_money(top_owners[0]['won_acv_sum'])}`."
        )
    lines.extend(
        [
            "- Team maturity is uneven; some managers own large, ACV-rich renewal books while others mostly surface at-risk or sparse histories.",
            "- This should inform how much we trust owner/manager retention KPIs versus using them as directional coaching surfaces.",
            "",
            "## Top Managers by Won Renewal ACV",
            "",
            "| Manager | Renewals | Closed | Won | Lost | No Opportunity | Open | Closed ACV Coverage | Won ACV | At-Risk ACV |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for row in top_managers:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["name"],
                    f"{row['count']:,}",
                    f"{row['closed_count']:,}",
                    f"{row['won_count']:,}",
                    f"{row['lost_count']:,}",
                    f"{row['no_opportunity_count']:,}",
                    f"{row['open_count']:,}",
                    f"{row['closed_acv_coverage_pct'] * 100:.1f}%",
                    format_money(row["won_acv_sum"]),
                    format_money(row["at_risk_acv_sum"]),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Top Owners by Won Renewal ACV",
            "",
            "| Owner | Renewals | Closed | Won | Lost | No Opportunity | Open | Closed ACV Coverage | Won ACV | At-Risk ACV |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in top_owners:
        lines.append(
            "| "
            + " | ".join(
                [
                    row["name"],
                    f"{row['count']:,}",
                    f"{row['closed_count']:,}",
                    f"{row['won_count']:,}",
                    f"{row['lost_count']:,}",
                    f"{row['no_opportunity_count']:,}",
                    f"{row['open_count']:,}",
                    f"{row['closed_acv_coverage_pct'] * 100:.1f}%",
                    format_money(row["won_acv_sum"]),
                    format_money(row["at_risk_acv_sum"]),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Annual Manager Rollup",
            "",
            "| Year | Manager | Won | Lost | No Opportunity | Open | Closed ACV Coverage | Won ACV | At-Risk ACV |",
            "|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )

    for year, managers in annual_manager.items():
        for row in top_rows(managers, limit=8):
            lines.append(
                "| "
                + " | ".join(
                    [
                        year,
                        row["name"],
                        f"{row['won_count']:,}",
                        f"{row['lost_count']:,}",
                        f"{row['no_opportunity_count']:,}",
                        f"{row['open_count']:,}",
                        f"{row['closed_acv_coverage_pct'] * 100:.1f}%",
                        format_money(row["won_acv_sum"]),
                        format_money(row["at_risk_acv_sum"]),
                    ]
                )
                + " |"
            )

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Use manager/owner retention KPIs as coaching views, but qualify them with data maturity and process consistency.",
            "- Prioritize higher-confidence years and teams when validating strict-renewal vs effective-retention logic.",
            "- Expect manager-level variation to matter; renewal process maturity is not uniform across the org.",
        ]
    )

    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile renewal history by manager and owner.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/renewal_ownership_profile_2026-03-11"),
        help="Directory for profile.md and profile.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_profile_command(
    out_dir: Path,
    *,
    emit_text: bool = True,
) -> tuple[dict[str, Any], int]:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    inst, tok = get_auth()
    rows = run_soql(inst, tok, QUERY)
    profile = summarize(rows)
    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"

    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(profile), encoding="utf-8")

    if emit_text:
        print(f"Wrote renewal ownership profile to {out_dir}")

    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote renewal ownership profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "manager_count": len(profile["by_manager"]),
            "owner_count": len(profile["by_owner"]),
            "annual_year_count": len(profile["annual_manager"]),
            "top_manager": next(iter(profile["by_manager"]), None),
            "top_owner": next(iter(profile["by_owner"]), None),
            "output_dir": str(out_dir),
        },
        profile=profile,
    )
    return result, 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    result, exit_code = run_profile_command(
        args.out_dir,
        emit_text=not args.json,
    )
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
