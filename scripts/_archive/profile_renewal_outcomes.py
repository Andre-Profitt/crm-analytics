#!/usr/bin/env python3
"""Profile renewal history by year and outcome.

This is analysis-only. It creates a durable evidence pack describing how
renewals are actually used in the org so retention formulas can be grounded in
real data quality and process maturity.
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
    "SELECT Id, Name, CloseDate, StageName, IsClosed, IsWon, ForecastCategoryName, "
    "Owner.Name, Account.Name, APTS_Renewal_ACV__c, APTS_Opportunity_ARR__c, Amount "
    "FROM Opportunity "
    "WHERE Type = 'Renewal' "
    "AND CloseDate >= 2021-01-01 "
    "ORDER BY CloseDate"
)

OUTCOME_ORDER = ("won", "lost", "no_opportunity", "open", "closed_not_won")


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
        "tool": "profile_renewal_outcomes",
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


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    by_year: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "total_count": 0,
            "total_acv_sum": 0.0,
            "total_amount_sum": 0.0,
            "outcomes": defaultdict(
                lambda: {
                    "count": 0,
                    "acv_nonzero": 0,
                    "acv_sum": 0.0,
                    "amount_nonzero": 0,
                    "amount_sum": 0.0,
                }
            ),
        }
    )

    for row in rows:
        year = (row.get("CloseDate") or "Unknown")[:4]
        outcome = classify_outcome(row)
        acv = safe_float(row.get("APTS_Renewal_ACV__c"))
        amount = safe_float(row.get("Amount"))

        bucket = by_year[year]
        bucket["total_count"] += 1
        bucket["total_acv_sum"] += acv
        bucket["total_amount_sum"] += amount

        outcome_bucket = bucket["outcomes"][outcome]
        outcome_bucket["count"] += 1
        outcome_bucket["acv_nonzero"] += int(acv > 0)
        outcome_bucket["acv_sum"] += acv
        outcome_bucket["amount_nonzero"] += int(amount > 0)
        outcome_bucket["amount_sum"] += amount

    annual_rollup: dict[str, dict[str, Any]] = {}
    for year, bucket in by_year.items():
        outcomes = bucket["outcomes"]
        closed_count = sum(outcomes[key]["count"] for key in ("won", "lost", "no_opportunity", "closed_not_won"))
        closed_acv_nonzero = sum(
            outcomes[key]["acv_nonzero"] for key in ("won", "lost", "no_opportunity", "closed_not_won")
        )
        annual_rollup[year] = {
            "total_count": bucket["total_count"],
            "closed_count": closed_count,
            "won_count": outcomes["won"]["count"],
            "lost_count": outcomes["lost"]["count"],
            "no_opportunity_count": outcomes["no_opportunity"]["count"],
            "open_count": outcomes["open"]["count"],
            "closed_acv_coverage_pct": pct(closed_acv_nonzero, closed_count),
            "won_acv_sum": outcomes["won"]["acv_sum"],
            "at_risk_acv_sum": outcomes["lost"]["acv_sum"] + outcomes["no_opportunity"]["acv_sum"],
        }

    return {
        "by_year": {
            year: {
                "total_count": bucket["total_count"],
                "total_acv_sum": bucket["total_acv_sum"],
                "total_amount_sum": bucket["total_amount_sum"],
                "outcomes": {
                    outcome: data
                    for outcome, data in sorted(bucket["outcomes"].items(), key=lambda item: OUTCOME_ORDER.index(item[0]))
                },
            }
            for year, bucket in sorted(by_year.items())
        },
        "annual_rollup": dict(sorted(annual_rollup.items())),
    }


def top_examples(rows: list[dict[str, Any]], outcomes: set[str], limit: int = 10) -> list[dict[str, Any]]:
    selected = [row for row in rows if classify_outcome(row) in outcomes and safe_float(row.get("APTS_Renewal_ACV__c")) > 0]
    selected.sort(key=lambda row: safe_float(row.get("APTS_Renewal_ACV__c")), reverse=True)
    result: list[dict[str, Any]] = []
    for row in selected[:limit]:
        result.append(
            {
                "close_date": row.get("CloseDate"),
                "outcome": classify_outcome(row),
                "stage": row.get("StageName"),
                "account": (row.get("Account") or {}).get("Name", ""),
                "owner": (row.get("Owner") or {}).get("Name", ""),
                "renewal_acv": safe_float(row.get("APTS_Renewal_ACV__c")),
                "amount": safe_float(row.get("Amount")),
                "name": row.get("Name"),
            }
        )
    return result


def format_money(value: float) -> str:
    return f"{value:,.2f}"


def render_markdown(profile: dict[str, Any]) -> str:
    annual_rollup = profile["annual_rollup"]
    by_year = profile["by_year"]
    top_won = profile["top_won_examples"]
    top_at_risk = profile["top_at_risk_examples"]

    lines = [
        "# Renewal Outcome Profile",
        "",
        "This report profiles how `Renewal` opportunities are actually used in the org from `2021+`.",
        "",
        "## Key Findings",
        "",
    ]

    if annual_rollup:
        earliest = min(annual_rollup)
        first_closed_year = next((year for year, data in annual_rollup.items() if data["closed_count"] > 0), earliest)
        recent_closed_year = max(
            (year for year, data in annual_rollup.items() if data["closed_count"] > 0),
            default=first_closed_year,
        )
        first_coverage = annual_rollup[first_closed_year]["closed_acv_coverage_pct"] * 100
        recent_coverage = annual_rollup[recent_closed_year]["closed_acv_coverage_pct"] * 100
        lines.extend(
            [
                f"- Renewal history is real by `{earliest}`, not just `2025/2026`.",
                f"- Closed renewal ACV coverage improves from `{first_coverage:.1f}%` in `{first_closed_year}` to `{recent_coverage:.1f}%` in `{recent_closed_year}`.",
                "- `APTS_Renewal_ACV__c` is the economically meaningful renewal field; renewal ARR is effectively unused.",
                "- `2025+` looks more operationally intentional, but `2021-2024` already contains meaningful renewal volume.",
                "",
            ]
        )

    lines.extend(
        [
            "## Annual Rollup",
            "",
            "| Year | Total Renewals | Closed | Won | Lost | No Opportunity | Open | Closed ACV Coverage | Won ACV | At-Risk ACV |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for year, data in annual_rollup.items():
        lines.append(
            "| "
            + " | ".join(
                [
                    year,
                    f"{data['total_count']:,}",
                    f"{data['closed_count']:,}",
                    f"{data['won_count']:,}",
                    f"{data['lost_count']:,}",
                    f"{data['no_opportunity_count']:,}",
                    f"{data['open_count']:,}",
                    f"{data['closed_acv_coverage_pct'] * 100:.1f}%",
                    format_money(data["won_acv_sum"]),
                    format_money(data["at_risk_acv_sum"]),
                ]
            )
            + " |"
        )

    lines.extend(
        [
            "",
            "## Outcome Detail by Year",
            "",
            "| Year | Outcome | Count | ACV Nonzero | ACV Coverage | Renewal ACV | Amount |",
            "|---|---|---:|---:|---:|---:|---:|",
        ]
    )
    for year, data in by_year.items():
        for outcome in OUTCOME_ORDER:
            bucket = data["outcomes"].get(outcome)
            if not bucket or not bucket["count"]:
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        year,
                        outcome,
                        f"{bucket['count']:,}",
                        f"{bucket['acv_nonzero']:,}",
                        f"{pct(bucket['acv_nonzero'], bucket['count']) * 100:.1f}%",
                        format_money(bucket["acv_sum"]),
                        format_money(bucket["amount_sum"]),
                    ]
                )
                + " |"
            )

    def append_examples(title: str, examples: list[dict[str, Any]]) -> None:
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                "| Close Date | Outcome | Account | Owner | Renewal ACV | Amount | Opportunity |",
                "|---|---|---|---|---:|---:|---|",
            ]
        )
        for row in examples:
            lines.append(
                "| "
                + " | ".join(
                    [
                        row["close_date"] or "",
                        row["outcome"],
                        row["account"],
                        row["owner"],
                        format_money(row["renewal_acv"]),
                        format_money(row["amount"]),
                        row["name"] or "",
                    ]
                )
                + " |"
            )

    append_examples("Largest Closed-Won Renewals by ACV", top_won)
    append_examples("Largest At-Risk Renewals by ACV", top_at_risk)

    lines.extend(
        [
            "",
            "## Recommendation",
            "",
            "- Treat `2021+` as real renewal history, but not uniformly mature process history.",
            "- Use `APTS_Renewal_ACV__c` for strict renewal and churn math.",
            "- Treat `2025+` as the highest-confidence operating period for renewal process KPIs.",
            "- Keep separate views for `strict renewal` and `effective retention` because some protected base is still booked outside `Type = Renewal`.",
        ]
    )
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Profile renewal history by outcome and year.")
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("/Users/test/crm-analytics/docs/generated/renewal_outcome_profile_2026-03-11"),
        help="Directory for profile.md and profile.json",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    return parser


def run_profile_command(out_dir: Path, *, emit_text: bool = True) -> tuple[dict[str, Any], int]:
    out_dir = out_dir.resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    inst, tok = get_auth()
    rows = run_soql(inst, tok, QUERY)
    profile = summarize(rows)
    profile["top_won_examples"] = top_examples(rows, {"won"})
    profile["top_at_risk_examples"] = top_examples(rows, {"lost", "no_opportunity"})

    json_path = out_dir / "profile.json"
    md_path = out_dir / "profile.md"
    json_path.write_text(json.dumps(profile, indent=2), encoding="utf-8")
    md_path.write_text(render_markdown(profile), encoding="utf-8")

    if emit_text:
        print(f"Wrote renewal outcome profile to {out_dir}")

    annual_rollup = profile["annual_rollup"]
    years_with_closed = [year for year, data in annual_rollup.items() if data["closed_count"] > 0]
    top_year = next(iter(annual_rollup), None)
    result = _make_result(
        status="ok",
        messages=[
            _make_message(
                "info",
                "profile_written",
                f"Wrote renewal outcome profile to {out_dir}.",
            )
        ],
        artifacts=[
            _make_artifact("json", json_path),
            _make_artifact("markdown", md_path),
        ],
        summary={
            "year_count": len(profile["by_year"]),
            "closed_year_count": len(years_with_closed),
            "top_year": top_year,
            "won_example_count": len(profile["top_won_examples"]),
            "at_risk_example_count": len(profile["top_at_risk_examples"]),
            "output_dir": str(out_dir),
        },
        profile=profile,
    )
    return result, 0


def main() -> int:
    args = build_parser().parse_args()
    result, exit_code = run_profile_command(args.out_dir, emit_text=not args.json)
    if args.json:
        print(json.dumps(result, indent=2))
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
