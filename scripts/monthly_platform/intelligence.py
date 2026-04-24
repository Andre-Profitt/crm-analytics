"""Gold analytics derived from DirectorBundle datasets."""

from __future__ import annotations

import dataclasses
import re
from collections import Counter, defaultdict
from datetime import date, datetime
from statistics import mean
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .models import DirectorBundle


def as_rows(bundle: DirectorBundle) -> dict[str, list[dict[str, Any]]]:
    raw = dataclasses.asdict(bundle.datasets)
    return {key: list(value or []) for key, value in raw.items()}


def stage_number(stage: str | None) -> int:
    if not stage:
        return -1
    match = re.match(r"^(\d+)", str(stage))
    return int(match.group(1)) if match else -1


def money(value: Any) -> float:
    return float(value or 0)


def parse_date(value: Any) -> date | None:
    if not value:
        return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def build_deal_risk_table(
    rows: dict[str, list[dict[str, Any]]], *, limit: int | None = 25
) -> list[dict[str, Any]]:
    activity = {r.get("opportunity"): r for r in rows["activity"]}
    stage_counts = Counter(r.get("opportunity") for r in rows["stage_events"])
    forecast_counts = Counter(r.get("opportunity") for r in rows["forecast_category_events"])
    close_counts = Counter(r.get("opportunity") for r in rows["close_date_events"])

    risk_rows: list[dict[str, Any]] = []
    for deal in rows["pipeline_open"]:
        opportunity = deal.get("opportunity")
        reasons = []
        score = 0
        arr = money(deal.get("arr_unweighted"))
        activity_row = activity.get(opportunity) or {}
        touches = activity_row.get("total_touches_90d")
        activity_flag = activity_row.get("flag")
        push_count = int(deal.get("push_count") or 0)
        age_days = int(deal.get("age_days") or 0)

        if arr >= 500_000 and (touches == 0 or activity_flag):
            score += 35
            reasons.append("large pipeline with weak/no recent activity")
        elif touches == 0 or activity_flag:
            score += 20
            reasons.append("weak/no recent activity")
        if push_count >= 3:
            score += min(25, push_count * 5)
            reasons.append(f"{push_count} pushes")
        if close_counts[opportunity] >= 3:
            score += 20
            reasons.append(f"{close_counts[opportunity]} close-date changes")
        if forecast_counts[opportunity] >= 2:
            score += 12
            reasons.append(f"{forecast_counts[opportunity]} forecast-category changes")
        if stage_counts[opportunity] >= 4:
            score += 12
            reasons.append(f"{stage_counts[opportunity]} stage changes")
        if not deal.get("approved") and stage_number(deal.get("stage")) >= 3:
            score += 20
            reasons.append("stage 3+ without approval flag")
        if not deal.get("next_step"):
            score += 8
            reasons.append("blank next step")
        if age_days >= 365:
            score += 10
            reasons.append(f"{age_days} days old")

        if score:
            risk_rows.append(
                {
                    "opportunity": opportunity,
                    "account": deal.get("account"),
                    "owner": deal.get("owner"),
                    "stage": deal.get("stage"),
                    "forecast_category": deal.get("forecast_category"),
                    "arr_unweighted": round(arr, 2),
                    "close_date": deal.get("close_date"),
                    "risk_score": score,
                    "risk_reasons": reasons,
                }
            )
    sorted_rows = sorted(
        risk_rows,
        key=lambda row: (row["risk_score"], row["arr_unweighted"]),
        reverse=True,
    )
    return sorted_rows if limit is None else sorted_rows[:limit]


def owner_metrics(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    activity = {r.get("opportunity"): r for r in rows["activity"]}
    grouped: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "open_deals": 0,
            "arr_unweighted": 0.0,
            "weighted_arr": 0.0,
            "no_touch_deals": 0,
            "no_touch_arr": 0.0,
            "stage3_plus_without_approval": 0,
            "push_count": 0,
            "ages": [],
        }
    )
    for deal in rows["pipeline_open"]:
        owner = deal.get("owner") or "(blank)"
        metric = grouped[owner]
        arr = money(deal.get("arr_unweighted"))
        metric["open_deals"] += 1
        metric["arr_unweighted"] += arr
        metric["weighted_arr"] += money(deal.get("arr_weighted"))
        metric["push_count"] += int(deal.get("push_count") or 0)
        metric["ages"].append(int(deal.get("age_days") or 0))
        activity_row = activity.get(deal.get("opportunity")) or {}
        touches = activity_row.get("total_touches_90d")
        if touches == 0 or activity_row.get("flag"):
            metric["no_touch_deals"] += 1
            metric["no_touch_arr"] += arr
        if not deal.get("approved") and stage_number(deal.get("stage")) >= 3:
            metric["stage3_plus_without_approval"] += 1

    out = []
    for owner, metric in grouped.items():
        ages = metric.pop("ages")
        metric["avg_age_days"] = round(mean(ages), 1) if ages else 0
        metric["arr_unweighted"] = round(metric["arr_unweighted"], 2)
        metric["weighted_arr"] = round(metric["weighted_arr"], 2)
        metric["no_touch_arr"] = round(metric["no_touch_arr"], 2)
        out.append({"owner": owner, **metric})
    return sorted(out, key=lambda row: row["arr_unweighted"], reverse=True)


def movement_summary(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = defaultdict(lambda: {"count": 0, "arr": 0.0})
    for dataset in ("movement_prior", "movement_current"):
        for row in rows[dataset]:
            key = f"{dataset}:{row.get('movement_type') or 'Unknown'}"
            grouped[key]["count"] += 1
            grouped[key]["arr"] += money(row.get("arr_unweighted"))
    return [
        {"movement": key, "count": val["count"], "arr_unweighted": round(val["arr"], 2)}
        for key, val in sorted(grouped.items())
    ]


def churn_table(
    rows: dict[str, list[dict[str, Any]]], dataset: str, label: str
) -> list[dict[str, Any]]:
    counts = Counter(row.get("opportunity") for row in rows[dataset])
    arr_by_opp: dict[str, float] = defaultdict(float)
    owners: dict[str, str] = {}
    accounts: dict[str, str] = {}
    for row in rows[dataset]:
        opp = row.get("opportunity")
        arr_by_opp[opp] = max(arr_by_opp[opp], money(row.get("arr_unweighted")))
        owners[opp] = row.get("owner") or ""
        accounts[opp] = row.get("account") or ""
    return [
        {
            "opportunity": opp,
            "account": accounts.get(opp),
            "owner": owners.get(opp),
            "event_type": label,
            "event_count": count,
            "max_arr_unweighted": round(arr_by_opp.get(opp, 0), 2),
        }
        for opp, count in counts.most_common(25)
        if opp
    ]


def transition_matrix(rows: list[dict[str, Any]], *, limit: int = 25) -> list[dict[str, Any]]:
    matrix = Counter((row.get("old_value") or "", row.get("new_value") or "") for row in rows)
    return [
        {"from": old, "to": new, "count": count}
        for (old, new), count in matrix.most_common(limit)
    ]


def pipeline_quality_by_quarter(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    activity = {row.get("opportunity"): row for row in rows["activity"]}
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for deal in rows["pipeline_open"]:
        grouped[deal.get("quarter") or "Unknown"].append(deal)

    out = []
    for quarter, deals in sorted(grouped.items()):
        total_arr = sum(money(deal.get("arr_unweighted")) for deal in deals)
        weighted_arr = sum(money(deal.get("arr_weighted")) for deal in deals)
        omitted_arr = sum(
            money(deal.get("arr_unweighted"))
            for deal in deals
            if deal.get("forecast_category") == "Omitted"
        )
        commit_arr = sum(
            money(deal.get("arr_unweighted"))
            for deal in deals
            if deal.get("forecast_category") == "Commit"
        )
        no_touch = sum(
            1
            for deal in deals
            if (activity.get(deal.get("opportunity")) or {}).get("flag")
        )
        out.append(
            {
                "quarter": quarter,
                "deal_count": len(deals),
                "arr_unweighted": round(total_arr, 2),
                "weighted_arr": round(weighted_arr, 2),
                "active_arr_ex_omitted": round(total_arr - omitted_arr, 2),
                "omitted_arr": round(omitted_arr, 2),
                "commit_arr": round(commit_arr, 2),
                "weighted_coverage_pct": round(weighted_arr / total_arr * 100, 1)
                if total_arr
                else 0,
                "omitted_pct": round(omitted_arr / total_arr * 100, 1)
                if total_arr
                else 0,
                "no_touch_deal_count": no_touch,
            }
        )
    return out


def pipeline_concentration(rows: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    deals = sorted(
        rows["pipeline_open"],
        key=lambda row: money(row.get("arr_unweighted")),
        reverse=True,
    )
    total_arr = sum(money(row.get("arr_unweighted")) for row in deals)
    bands = []
    for n in (5, 10, 20):
        top = deals[:n]
        arr = sum(money(row.get("arr_unweighted")) for row in top)
        bands.append(
            {
                "top_n": n,
                "arr_unweighted": round(arr, 2),
                "pct_of_open_pipeline": round(arr / total_arr * 100, 1)
                if total_arr
                else 0,
            }
        )
    return {
        "total_open_arr": round(total_arr, 2),
        "bands": bands,
        "largest_deals": [
            {
                "opportunity": row.get("opportunity"),
                "account": row.get("account"),
                "owner": row.get("owner"),
                "stage": row.get("stage"),
                "forecast_category": row.get("forecast_category"),
                "quarter": row.get("quarter"),
                "arr_unweighted": round(money(row.get("arr_unweighted")), 2),
                "arr_weighted": round(money(row.get("arr_weighted")), 2),
                "close_date": row.get("close_date"),
            }
            for row in deals[:20]
        ],
    }


def close_date_volatility(rows: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    directions = Counter()
    deltas: list[int] = []
    by_owner: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "event_count": 0,
            "net_days": 0,
            "gross_days": 0,
            "pushed_out": 0,
            "pulled_in": 0,
            "event_arr_sum": 0.0,
        }
    )
    by_opportunity: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "event_count": 0,
            "net_days": 0,
            "gross_days": 0,
            "pushed_out": 0,
            "pulled_in": 0,
            "max_arr_unweighted": 0.0,
            "latest_changed_on": "",
            "owner": "",
            "account": "",
        }
    )
    extreme_events = []
    anomalies = []

    for row in rows["close_date_events"]:
        old = parse_date(row.get("old_value"))
        new = parse_date(row.get("new_value"))
        if not old or not new:
            continue
        delta = (new - old).days
        deltas.append(delta)
        direction = "pushed_out" if delta > 0 else "pulled_in" if delta < 0 else "unchanged"
        directions[direction] += 1

        owner = row.get("owner") or "(blank)"
        owner_bucket = by_owner[owner]
        owner_bucket["event_count"] += 1
        owner_bucket["net_days"] += delta
        owner_bucket["gross_days"] += abs(delta)
        owner_bucket[direction] += 1
        owner_bucket["event_arr_sum"] += money(row.get("arr_unweighted"))

        opportunity = row.get("opportunity") or "(blank)"
        opp_bucket = by_opportunity[opportunity]
        opp_bucket["event_count"] += 1
        opp_bucket["net_days"] += delta
        opp_bucket["gross_days"] += abs(delta)
        opp_bucket[direction] += 1
        opp_bucket["max_arr_unweighted"] = max(
            opp_bucket["max_arr_unweighted"], money(row.get("arr_unweighted"))
        )
        opp_bucket["latest_changed_on"] = max(
            str(opp_bucket["latest_changed_on"] or ""),
            str(row.get("created_date") or ""),
        )
        opp_bucket["owner"] = row.get("owner") or opp_bucket["owner"]
        opp_bucket["account"] = row.get("account") or opp_bucket["account"]

        event = {
            "opportunity": opportunity,
            "account": row.get("account"),
            "owner": row.get("owner"),
            "old_close": row.get("old_value"),
            "new_close": row.get("new_value"),
            "changed_on": row.get("created_date"),
            "delta_days": delta,
            "arr_unweighted": round(money(row.get("arr_unweighted")), 2),
            "direction": direction,
        }
        extreme_events.append(event)
        if abs(delta) > 730:
            anomalies.append(event)

    return {
        "event_count": len(deltas),
        "direction_counts": dict(directions),
        "avg_net_days": round(mean(deltas), 1) if deltas else 0,
        "avg_gross_days": round(mean(abs(delta) for delta in deltas), 1)
        if deltas
        else 0,
        "max_push_days": max(deltas) if deltas else 0,
        "max_pull_days": min(deltas) if deltas else 0,
        "by_owner": sorted(
            (
                {"owner": owner, **_round_volatility_bucket(values)}
                for owner, values in by_owner.items()
            ),
            key=lambda row: (row["event_count"], row["gross_days"]),
            reverse=True,
        )[:20],
        "by_opportunity": sorted(
            (
                {"opportunity": opportunity, **_round_volatility_bucket(values)}
                for opportunity, values in by_opportunity.items()
            ),
            key=lambda row: (row["event_count"], row["max_arr_unweighted"]),
            reverse=True,
        )[:25],
        "extreme_events": sorted(
            extreme_events,
            key=lambda row: abs(row["delta_days"]),
            reverse=True,
        )[:25],
        "date_delta_anomalies": sorted(
            anomalies,
            key=lambda row: abs(row["delta_days"]),
            reverse=True,
        )[:25],
    }


def _round_volatility_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    rounded = dict(bucket)
    for key in ("event_arr_sum", "max_arr_unweighted"):
        if key in rounded:
            rounded[key] = round(float(rounded[key]), 2)
    return rounded


def high_stage_zero_arr(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    out = []
    for deal in rows["pipeline_open"]:
        if money(deal.get("arr_unweighted")) != 0:
            continue
        if stage_number(deal.get("stage")) < 3:
            continue
        out.append(
            {
                "opportunity": deal.get("opportunity"),
                "account": deal.get("account"),
                "owner": deal.get("owner"),
                "stage": deal.get("stage"),
                "forecast_category": deal.get("forecast_category"),
                "close_date": deal.get("close_date"),
                "approved": deal.get("approved"),
                "next_step_blank": not bool(deal.get("next_step")),
            }
        )
    return out


def owner_portfolio_health(rows: dict[str, list[dict[str, Any]]]) -> list[dict[str, Any]]:
    base = owner_metrics(rows)
    risk_rows = build_deal_risk_table(rows, limit=None)
    risk_by_owner: dict[str, dict[str, float]] = defaultdict(
        lambda: {"risk_rows": 0, "risk_score_sum": 0.0, "risk_arr": 0.0}
    )
    for risk in risk_rows:
        bucket = risk_by_owner[risk.get("owner") or "(blank)"]
        bucket["risk_rows"] += 1
        bucket["risk_score_sum"] += money(risk.get("risk_score"))
        bucket["risk_arr"] += money(risk.get("arr_unweighted"))

    out = []
    for row in base:
        total_arr = money(row.get("arr_unweighted"))
        owner_risk = risk_by_owner[row["owner"]]
        out.append(
            {
                **row,
                "weighted_coverage_pct": round(
                    money(row.get("weighted_arr")) / total_arr * 100, 1
                )
                if total_arr
                else 0,
                "no_touch_arr_pct": round(
                    money(row.get("no_touch_arr")) / total_arr * 100, 1
                )
                if total_arr
                else 0,
                "risk_rows": int(owner_risk["risk_rows"]),
                "risk_arr": round(owner_risk["risk_arr"], 2),
                "avg_risk_score": round(
                    owner_risk["risk_score_sum"] / owner_risk["risk_rows"], 1
                )
                if owner_risk["risk_rows"]
                else 0,
            }
        )
    return out


def build_gold_analytics_pack(bundle: DirectorBundle) -> dict[str, Any]:
    rows = as_rows(bundle)
    close_volatility = close_date_volatility(rows)
    concentration = pipeline_concentration(rows)
    risk_index = build_deal_risk_table(rows, limit=None)
    zero_arr = high_stage_zero_arr(rows)
    return {
        "schema_version": 1,
        "artifact_type": "director_gold_analytics_pack",
        "snapshot_date": bundle.snapshot_date,
        "director": bundle.director,
        "territory": bundle.territory,
        "summary": {
            "open_deals": len(rows["pipeline_open"]),
            "open_arr": concentration["total_open_arr"],
            "close_date_event_count": close_volatility["event_count"],
            "deal_risk_rows": len(risk_index),
            "high_stage_zero_arr_count": len(zero_arr),
            "top_20_pipeline_concentration_pct": next(
                band["pct_of_open_pipeline"]
                for band in concentration["bands"]
                if band["top_n"] == 20
            ),
        },
        "analytics": {
            "pipeline_concentration": concentration,
            "pipeline_quality_by_quarter": pipeline_quality_by_quarter(rows),
            "owner_portfolio_health": owner_portfolio_health(rows),
            "close_date_volatility": close_volatility,
            "forecast_transition_matrix": transition_matrix(
                rows["forecast_category_events"]
            ),
            "stage_transition_matrix": transition_matrix(rows["stage_events"]),
            "deal_risk_index": risk_index,
            "movement_summary": movement_summary(rows),
            "high_stage_zero_arr": zero_arr,
        },
        "deck_ready_insights": build_deck_ready_insights(
            rows=rows,
            concentration=concentration,
            close_volatility=close_volatility,
            risk_index=risk_index,
            zero_arr=zero_arr,
        ),
    }


def build_deck_ready_insights(
    *,
    rows: dict[str, list[dict[str, Any]]],
    concentration: dict[str, Any],
    close_volatility: dict[str, Any],
    risk_index: list[dict[str, Any]],
    zero_arr: list[dict[str, Any]],
) -> list[dict[str, str]]:
    quality = pipeline_quality_by_quarter(rows)
    top_20 = next(
        band for band in concentration["bands"] if band["top_n"] == 20
    )
    q4 = next((row for row in quality if row["quarter"].startswith("Q4")), None)
    insights = [
        {
            "theme": "Pipeline concentration",
            "insight": (
                f"Top 20 deals carry {top_20['pct_of_open_pipeline']}% of open ARR, "
                "so executive review should be deal-specific, not only aggregate."
            ),
        },
        {
            "theme": "Close-date volatility",
            "insight": (
                f"{close_volatility['direction_counts'].get('pushed_out', 0)} close-date moves pushed out "
                f"versus {close_volatility['direction_counts'].get('pulled_in', 0)} pulled in; "
                f"average net movement is {close_volatility['avg_net_days']} days."
            ),
        },
    ]
    if q4:
        insights.append(
            {
                "theme": "Backload quality",
                "insight": (
                    f"{q4['quarter']} holds {q4['arr_unweighted']:,.0f} ARR, "
                    f"with {q4['omitted_pct']}% omitted and {q4['no_touch_deal_count']} no-touch deals."
                ),
            }
        )
    if risk_index:
        top = risk_index[0]
        insights.append(
            {
                "theme": "Top deal risk",
                "insight": (
                    f"{top['opportunity']} is the top scored risk at {top['risk_score']}, "
                    f"driven by {', '.join(top['risk_reasons'][:3])}."
                ),
            }
        )
    if zero_arr:
        insights.append(
            {
                "theme": "Data hygiene",
                "insight": (
                    f"{len(zero_arr)} stage 3+ opportunities carry zero ARR; "
                    "these should be separated from revenue forecast claims."
                ),
            }
        )
    return insights
