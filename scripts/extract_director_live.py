#!/usr/bin/env python3
"""Extract live Salesforce data into a director Excel workbook.

Uses Alex P's methodology: Opportunity ARR (unweighted), reporting year only,
stages 1-6 for open pipeline, account/owner exclusions.

The Excel workbook is the single editable source of truth.
The SimCorp deck renderer reads from this Excel.

Usage:
    python3 scripts/extract_director_live.py --territory APAC
    python3 scripts/extract_director_live.py --territory "UK & Ireland"
    python3 scripts/extract_director_live.py --all
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any


import requests

try:
    from monthly_platform import SF_API_VERSION
    from monthly_platform.period import resolve_period_context
    from monthly_platform.models import (
        ActivitySignal,
        ApprovalDeal,
        BundleManifestEntry,
        CloseDateEvent,
        CommitItem,
        DatasetSource,
        Datasets,
        DirectorBundle,
        ForecastEvent,
        MovementEvent,
        PIDeal,
        PipelineDeal,
        RenewalDeal,
        RunManifest,
        SourceContract,
        StageEvent,
        WonLostDeal,
    )
    from monthly_platform.excel_renderer import render_bundle_to_excel
except ModuleNotFoundError:  # pragma: no cover
    from scripts.monthly_platform import SF_API_VERSION  # noqa: F811
    from scripts.monthly_platform.period import resolve_period_context  # noqa: F811
    from scripts.monthly_platform.models import (  # noqa: F811
        ActivitySignal,
        ApprovalDeal,
        BundleManifestEntry as BundleManifestEntry,  # noqa: F811
        CloseDateEvent,
        CommitItem,
        DatasetSource,
        Datasets,
        DirectorBundle,
        ForecastEvent,
        MovementEvent,
        PIDeal,
        PipelineDeal,
        RenewalDeal,
        RunManifest as RunManifest,  # noqa: F811
        SourceContract,
        StageEvent,
        WonLostDeal,
    )
    from scripts.monthly_platform.excel_renderer import render_bundle_to_excel  # noqa: F811

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "director_live_workbooks"
SOURCE_CONTRACT_AUDIT_ROOT = REPO_ROOT / "output" / "source_contract_audit"
AUDIT_OUTPUT_ROOT = REPO_ROOT / "output" / "director_live_extract"
BUNDLE_OUTPUT_ROOT = REPO_ROOT / "output" / "director_bundles"

# ── Territory config ──
# Loaded from config/sd_monthly_territories.json so that onboarding a new
# director is a config edit, not a code change. The keys below preserve the
# exact territory labels downstream builders expect (APAC, Central Europe, ...).
_TERRITORY_CONFIG_PATH = REPO_ROOT / "config" / "sd_monthly_territories.json"
try:
    _TERR_DATA = json.loads(_TERRITORY_CONFIG_PATH.read_text())
    TERRITORIES = _TERR_DATA.get("territories", {})
    if not TERRITORIES:
        raise ValueError(
            "config/sd_monthly_territories.json has no 'territories' block"
        )
except (OSError, ValueError, json.JSONDecodeError) as _exc:
    raise SystemExit(
        f"Unable to load territory config at {_TERRITORY_CONFIG_PATH}: {_exc}"
    )

# ── Alex P's filter methodology ──
ACCOUNT_EXCLUDE = "AND (NOT Account.Name LIKE '%simcorp%') AND (NOT Account.Name LIKE '%test%') AND (NOT Account.Name LIKE '%delete%')"
OWNER_EXCLUDE = (
    "AND (NOT Owner.Name LIKE '%Sabiniewicz%') AND (NOT Owner.Name LIKE '%Profit%')"
)
TYPE_FILTER = "AND Type IN ('Land', 'Expand', 'Renewal')"
OPEN_STAGES = "AND StageName IN ('1 - Prospecting', '2 - Discovery', '3 - Engagement', '4 - Shortlisted', '5 - Preferred', '6 - Contracting')"
CLOSED_STAGES = "AND IsClosed = true"

# ── Shared columns (matches Alex's report) ──
# ARR fields wrapped in convertCurrency() so multi-currency books (APAC USD,
# NA USD, EMEA CHF/SEK) report in the corporate currency (EUR) rather than
# the deal's native currency. Alias back to the original field name so the
# response-parsing code does not change.
PIPELINE_FIELDS = """
    Id, Account.Name, Name, Owner.Name, StageName, CloseDate,
    convertCurrency(APTS_Opportunity_ARR__c) APTS_Opportunity_ARR__c,
    convertCurrency(APTS_Forecast_ARR__c) APTS_Forecast_ARR__c,
    ForecastCategoryName, Probability, PushCount, Type,
    Lead_Scope__c, Account.Industry, Account.Tier_Calculation__c,
    Account_Unit_Group__c, Sales_Region__c,
    CreatedDate, LastActivityDate, NextStep, LastModifiedDate,
    Stage_20_Approval__c, Stage_20_Approval_Date__c,
    Submit_for_Stage_20_Review__c, Submit_for_Stage_20_Review_Date__c,
    Approval_Status__c, Lost_to_Competitor__c
""".strip()


def _display_path(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def _runtime_period(snapshot_date: str):
    period = resolve_period_context(
        as_of_date=snapshot_date,
        snapshot_date=snapshot_date,
        deck_date=snapshot_date,
    )
    analysis_year = period.current_quarter.year
    return {
        "snapshot_date": snapshot_date,
        "analysis_year": analysis_year,
        "fy_label": period.fiscal_year,
        "fy_close_filter": (
            f"AND CloseDate >= {analysis_year}-01-01 "
            f"AND CloseDate <= {analysis_year}-12-31"
        ),
        "q1_start": f"{analysis_year}-01-01",
        "q1_end": f"{analysis_year}-03-31",
        "q2_start": f"{analysis_year}-04-01",
        "q2_end": f"{analysis_year}-06-30",
        "q3_start": f"{analysis_year}-07-01",
        "current_quarter_label": period.current_quarter.label,
        "current_quarter_title": period.current_quarter.title,
        "forward_quarter_label": period.forward_quarter.label,
        "forward_quarter_title": period.forward_quarter.title,
        "forward_start": period.forward_quarter.start_date,
        "forward_end": period.forward_quarter.end_date,
    }


def _quarter_label(close_date: str, analysis_year: int) -> str:
    token = str(close_date or "")[:10]
    if len(token) < 7 or not token.startswith(str(analysis_year)):
        return ""
    try:
        month = int(token[5:7])
    except ValueError:
        return ""
    return f"Q{(month - 1) // 3 + 1} {analysis_year}"


def _resolve_forward_quarter_pi_source(
    config: dict,
    period: dict[str, str | int],
    audit_fallback: dict[str, str] | None = None,
) -> dict[str, str] | None:
    source = (config.get("forward_quarter_pi_list_views") or {}).get(
        period["forward_quarter_label"]
    )
    list_view_id = ""
    list_view_label = ""
    if isinstance(source, dict):
        list_view_id = str(source.get("list_view_id") or source.get("id") or "").strip()
        list_view_label = str(
            source.get("list_view_label") or source.get("label") or ""
        ).strip()
    if not list_view_id and isinstance(audit_fallback, dict):
        list_view_id = str(audit_fallback.get("list_view_id") or "").strip()
        list_view_label = str(audit_fallback.get("list_view_label") or "").strip()
    if not list_view_id:
        return None
    return {
        "list_view_id": list_view_id,
        "list_view_label": list_view_label or f"PI {period['forward_quarter_title']}",
        "quarter_label": str(period["forward_quarter_label"]),
        "quarter_title": str(period["forward_quarter_title"]),
        "start_date": str(period["forward_start"]),
        "end_date": str(period["forward_end"]),
    }


def _load_forward_quarter_pi_audit_fallback(
    snapshot_date: str,
    *,
    quarter_label: str,
) -> dict[str, dict[str, str]]:
    audit_path = (
        SOURCE_CONTRACT_AUDIT_ROOT
        / str(snapshot_date)[:10]
        / "source_contract_audit.json"
    )
    if not audit_path.exists():
        return {}
    try:
        payload = json.loads(audit_path.read_text())
    except (OSError, ValueError, json.JSONDecodeError):
        return {}
    candidate = (
        payload.get("candidate_forward_quarter") or payload.get("candidate_q3") or {}
    )
    if str(candidate.get("quarter_label") or "").strip() != str(quarter_label).strip():
        return {}
    fallback: dict[str, dict[str, str]] = {}
    for item in candidate.get("pi_list_views") or []:
        territory = str(item.get("territory") or "").strip()
        list_view_id = str(item.get("list_view_id") or "").strip()
        if territory and list_view_id and str(item.get("status") or "").strip() == "ok":
            fallback[territory] = {
                "list_view_id": list_view_id,
                "list_view_label": str(item.get("list_view_label") or "").strip(),
            }
    return fallback


def _build_pipeline_inspection_rows(
    pi_raw: list[dict],
    *,
    analysis_year: int,
    close_start: str | None = None,
    close_end: str | None = None,
    corp_ccy: str = "EUR",
) -> list[list]:
    rows = []
    for rec in pi_raw:
        f = rec.get("fields", {})
        name = str(f.get("Name", {}).get("value", ""))
        close = str(f.get("CloseDate", {}).get("value", ""))
        is_closed = f.get("IsClosed", {}).get("value", False)
        fc = str(f.get("ForecastCategoryName", {}).get("value", ""))
        opp_type = str(f.get("Type", {}).get("value", ""))
        if opp_type and opp_type != "Land":
            continue
        if is_closed or fc in ("Omitted", "Closed"):
            continue
        if close and close[:4] != str(analysis_year):
            continue
        if close_start or close_end:
            if not close:
                continue
            if close_start and close < close_start:
                continue
            if close_end and close > close_end:
                continue
        if any(p.lower() in name.lower() for p in ("simcorp", "test account")):
            continue
        owner_obj = f.get("Owner", {}).get("value")
        owner = (
            owner_obj.get("fields", {}).get("Name", {}).get("value", "")
            if isinstance(owner_obj, dict)
            else ""
        )
        score_obj = f.get("OpportunityScore", {}).get("value")
        score = (
            score_obj.get("fields", {}).get("Score", {}).get("value")
            if isinstance(score_obj, dict)
            else None
        )
        ccy = f.get("CurrencyIsoCode", {}).get("value", corp_ccy)
        rows.append(
            [
                name,
                owner,
                str(f.get("StageName", {}).get("value", "")),
                fc,
                f.get("APTS_Forecast_ARR__c", {}).get("value") or 0,
                ccy,
                close,
                f.get("PushCount", {}).get("value") or 0,
                score,
                "Yes" if f.get("IsPriorityRecord", {}).get("value") else "",
            ]
        )
    rows.sort(key=lambda x: -(x[4] or 0))
    return rows


def get_auth() -> tuple[str, str]:
    try:
        result = subprocess.run(
            ["sf", "org", "display", "--target-org", "apro@simcorp.com", "--json"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr or "(no stderr)"
        raise SystemExit(f"SF auth failed: {stderr.strip()}") from exc
    data = json.loads(result.stdout)["result"]
    return data["accessToken"], data["instanceUrl"]


def get_corporate_currency(session, instance_url):
    resp = session.get(
        f"{instance_url}/services/data/{SF_API_VERSION}/query",
        params={"q": "SELECT IsoCode FROM CurrencyType WHERE IsCorporate=true"},
    )
    records = resp.json().get("records", [])
    return records[0]["IsoCode"] if records else "EUR"


# ── SF HTTP: retry + backoff + telemetry ──
# Telemetry bucket shared across threads. Each query appends
# {label, rows, duration_ms, attempts, status}.
QUERY_TELEMETRY: list[dict] = []
_TELEMETRY_LOCK = __import__("threading").Lock()


def _sf_get_with_retry(session, url, params=None, timeout=120, attempts=3, label=""):
    """GET with exponential backoff on 429/5xx/connection errors.

    Salesforce returns 429 (rate-limit) or 503 (shed load) under sustained
    traffic. Transient network errors also happen. A 3-attempt retry with
    1s / 2s / 4s backoff covers all realistic cases without masking real
    server failures (which persist past 3 attempts).
    """
    import time

    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = session.get(url, params=params, timeout=timeout)
            if resp.status_code == 401:
                new_token, _ = get_auth()
                session.headers.update({"Authorization": f"Bearer {new_token}"})
                if attempt < attempts:
                    continue
            if resp.status_code == 400:
                body = resp.text[:500]
                raise requests.HTTPError(
                    f"400 from SF ({label}): {body}", response=resp
                )
            if resp.status_code in (429, 500, 502, 503, 504) and attempt < attempts:
                time.sleep(2 ** (attempt - 1))
                continue
            resp.raise_for_status()
            return resp, attempt
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < attempts:
                time.sleep(2 ** (attempt - 1))
                continue
            raise
    if last_exc:
        raise last_exc
    raise RuntimeError(f"SF GET exhausted retries: {label or url}")


def run_soql(session, instance_url: str, query: str, label: str = "") -> list[dict]:
    """SOQL with pagination, retry, and telemetry.

    Caller passes a prepared requests.Session (auth header already set).
    Records query timing + row count into QUERY_TELEMETRY for manifest export.
    """
    import time

    start = time.monotonic()
    records = []
    url = f"{instance_url}/services/data/{SF_API_VERSION}/query"
    params: dict | None = {"q": query}
    total_attempts = 0
    while True:
        resp, attempts = _sf_get_with_retry(
            session, url, params=params, label=label or "soql"
        )
        total_attempts += attempts
        d = resp.json()
        records.extend(d.get("records", []))
        next_url = d.get("nextRecordsUrl")
        if not next_url:
            break
        url = f"{instance_url}{next_url}"
        params = None
    duration_ms = int((time.monotonic() - start) * 1000)
    with _TELEMETRY_LOCK:
        QUERY_TELEMETRY.append(
            {
                "label": label or "soql",
                "rows": len(records),
                "duration_ms": duration_ms,
                "attempts": total_attempts,
                "status": "ok",
            }
        )
    return records


def fetch_pi(session, instance_url: str, lv_id: str, label: str = "") -> list[dict]:
    """Pipeline Inspection list-view records with retry + telemetry."""
    import time

    start = time.monotonic()
    url = f"{instance_url}/services/data/{SF_API_VERSION}/ui-api/list-records/{lv_id}?pageSize=200"
    records = []
    total_attempts = 0
    while url and len(records) < 2000:
        resp, attempts = _sf_get_with_retry(
            session, url, timeout=60, label=label or f"pi:{lv_id}"
        )
        total_attempts += attempts
        if resp.status_code != 200:
            break
        d = resp.json()
        records.extend(d.get("records", []))
        next_url = d.get("nextPageUrl")
        url = f"{instance_url}{next_url}" if next_url else None
    duration_ms = int((time.monotonic() - start) * 1000)
    with _TELEMETRY_LOCK:
        QUERY_TELEMETRY.append(
            {
                "label": label or f"pi:{lv_id}",
                "rows": len(records),
                "duration_ms": duration_ms,
                "attempts": total_attempts,
                "status": "ok",
            }
        )
    return records


def _write_run_audit(output_dir: Path, payload: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "director_live_extract_audit.json").write_text(
        json.dumps(payload, indent=2) + "\n",
        encoding="utf-8",
    )
    query_totals = payload.get("query_telemetry_totals") or {}
    lines = [
        f"# Director Live Extract Audit — {payload.get('run_date', '')}",
        "",
        f"- Status: `{payload.get('status', 'unknown')}`",
        f"- Scope: `{payload.get('scope', 'unknown')}`",
        f"- Territories requested: `{len(payload.get('territories_requested') or [])}`",
        f"- Territories processed: `{len(payload.get('processed') or [])}`",
        f"- Failures: `{len(payload.get('failures') or [])}`",
        f"- Query count: `{query_totals.get('queries', 0)}`",
        f"- Query rows: `{query_totals.get('rows', 0)}`",
        f"- Query duration ms: `{query_totals.get('duration_ms', 0)}`",
        "",
        "## Processed",
        "",
    ]
    processed = payload.get("processed") or []
    if not processed:
        lines.append("- none")
    else:
        for item in processed:
            forward_pi = item.get("forward_quarter_pi") or {}
            forward_fragment = ""
            if forward_pi.get("status") != "unavailable":
                forward_fragment = (
                    f", PI forward {forward_pi.get('quarter_title', '')}: "
                    f"{forward_pi.get('deal_count', 0)}"
                )
            lines.append(
                f"- `{item.get('territory', '')}` / `{item.get('director', '')}`: "
                f"pipeline `{item.get('counts', {}).get('pipeline_open', 0)}`, "
                f"PI `{item.get('counts', {}).get('pipeline_inspection', 0)}`"
                f"{forward_fragment}, "
                f"workbook `{item.get('workbook_path', '')}`"
            )
    lines.extend(["", "## Failures", ""])
    failures = payload.get("failures") or []
    if not failures:
        lines.append("- none")
    else:
        for item in failures:
            lines.append(
                f"- `{item.get('territory', '')}`: "
                f"`{item.get('error_type', 'error')}` {item.get('message', '')}"
            )
    (output_dir / "summary.md").write_text(
        "\n".join(lines) + "\n",
        encoding="utf-8",
    )


def _write_run_manifest(
    manifest_path: Path,
    *,
    processed: list[dict],
    failures: list[dict],
    durations: dict[str, float],
    snapshot_date: str,
    started_at: str,
    finished_at: str,
    query_telemetry_totals: dict,
) -> None:
    import dataclasses as _dc

    directors = [
        BundleManifestEntry(
            name=item["director"],
            territory=item["territory"],
            status="ok",
            bundle_path=item.get("bundle_path", ""),
            workbook_path=item.get("workbook_path", ""),
            row_counts=item.get("counts", {}),
            duration_seconds=durations.get(item["territory"], 0.0),
        )
        for item in processed
    ]
    failed = [
        BundleManifestEntry(
            name=item.get("territory", ""),
            territory=item.get("territory", ""),
            status="failed",
            bundle_path="",
            workbook_path="",
            row_counts={},
            duration_seconds=0.0,
            failure_reason=item.get("message", ""),
        )
        for item in failures
    ]
    manifest = RunManifest(
        schema_version="1",
        run_date=snapshot_date,
        started_at=started_at,
        finished_at=finished_at,
        directors=directors,
        failures=failed,
        telemetry={
            "total_queries": query_telemetry_totals.get("queries", 0),
            "total_rows": query_telemetry_totals.get("rows", 0),
            "total_duration_seconds": query_telemetry_totals.get("duration_ms", 0)
            / 1000.0,
        },
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(_dc.asdict(manifest), indent=2) + "\n",
        encoding="utf-8",
    )


def build_session(token: str) -> requests.Session:
    """Build a requests.Session with the SF bearer token set.

    Sessions are thread-safe for read-only use (our pipeline), so a single
    session can drive all 9 concurrent territory extractions.
    """
    s = requests.Session()
    s.headers.update({"Authorization": f"Bearer {token}"})
    return s


def _val(record, field):
    """Safely get a field value, handling relationship fields."""
    parts = field.split(".")
    obj = record
    for p in parts:
        if obj is None:
            return ""
        if isinstance(obj, dict):
            obj = obj.get(p)
        else:
            return ""
    return obj if obj is not None else ""


def extract_territory(
    territory: str,
    snapshot_date: str,
    output_path: Path,
    session=None,
    instance_url: str | None = None,
    corp_ccy: str = "EUR",
):
    """Extract one territory to a director workbook.

    Accepts a pre-built session + instance_url so the caller can share a
    single auth across concurrent extractions. Falls back to computing auth
    locally if not provided (backward-compatible for single-territory use).
    """
    config = TERRITORIES[territory]
    director = config["director"]
    where = config["soql_where"]
    pi_lv = config["pi_list_view_id"]
    period = _runtime_period(snapshot_date)
    configured_forward_pi_source = (
        config.get("forward_quarter_pi_list_views") or {}
    ).get(period["forward_quarter_label"]) or {}
    audit_forward_pi_sources = _load_forward_quarter_pi_audit_fallback(
        snapshot_date,
        quarter_label=str(period["forward_quarter_label"]),
    )
    forward_pi_source = _resolve_forward_quarter_pi_source(
        config,
        period,
        audit_fallback=audit_forward_pi_sources.get(territory),
    )
    forward_pi_source_origin = "unavailable"
    if (
        isinstance(configured_forward_pi_source, dict)
        and str(
            configured_forward_pi_source.get("list_view_id")
            or configured_forward_pi_source.get("id")
            or ""
        ).strip()
    ):
        forward_pi_source_origin = "configured"
    elif territory in audit_forward_pi_sources:
        forward_pi_source_origin = "audit_fallback"

    print(f"\n{'=' * 60}")
    print(f"  {director} ({territory})")
    print(f"{'=' * 60}")

    if session is None or instance_url is None:
        token, instance_url = get_auth()
        session = build_session(token)

    # ── 1+2. All FY deals (single query to avoid race condition) ──
    print("  All FY deals...", end=" ", flush=True)
    q = (
        f"SELECT {PIPELINE_FIELDS}, Reason_Won_Lost__c FROM Opportunity WHERE {where} "
        f"{period['fy_close_filter']} {TYPE_FILTER} {ACCOUNT_EXCLUDE} {OWNER_EXCLUDE} "
        "ORDER BY APTS_Opportunity_ARR__c DESC NULLS LAST"
    )
    all_deals = run_soql(session, instance_url, q, label=f"{territory}:all_fy_deals")

    _open_stage_names = {
        "1 - Prospecting",
        "2 - Discovery",
        "3 - Engagement",
        "4 - Shortlisted",
        "5 - Preferred",
        "6 - Contracting",
    }
    pipeline = [
        r
        for r in all_deals
        if not r.get("IsClosed") and r.get("StageName") in _open_stage_names
    ]
    won_lost = [r for r in all_deals if r.get("IsClosed")]
    print(f"{len(all_deals)} total ({len(pipeline)} open, {len(won_lost)} closed)")

    pipeline_models = []
    for r in pipeline:
        created = (r.get("CreatedDate") or "")[:10]
        close = r.get("CloseDate", "")
        age = 0
        if created and close:
            try:
                age = (
                    date.fromisoformat(snapshot_date) - date.fromisoformat(created)
                ).days
            except ValueError:
                pass
        pipeline_models.append(
            PipelineDeal(
                account=_val(r, "Account.Name"),
                opportunity=r.get("Name", ""),
                owner=_val(r, "Owner.Name"),
                stage=r.get("StageName", ""),
                forecast_category=r.get("ForecastCategoryName", ""),
                close_date=close,
                arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
                arr_weighted=r.get("APTS_Forecast_ARR__c") or 0,
                probability=r.get("Probability") or 0,
                push_count=r.get("PushCount") or 0,
                deal_type=r.get("Type", ""),
                lead_scope=r.get("Lead_Scope__c", ""),
                industry=_val(r, "Account.Industry"),
                tier=_val(r, "Account.Tier_Calculation__c"),
                sales_region=r.get("Sales_Region__c", ""),
                created_date=created,
                last_activity_date=r.get("LastActivityDate") or None,
                next_step=r.get("NextStep", ""),
                last_modified_date=(r.get("LastModifiedDate") or "")[:10],
                approved=bool(r.get("Stage_20_Approval__c")),
                approval_date=r.get("Stage_20_Approval_Date__c") or None,
                competitor=r.get("Lost_to_Competitor__c", "") or "",
                currency=corp_ccy,
                age_days=age,
                quarter=_quarter_label(close, int(period["analysis_year"])),
            )
        )

    # ── 2. Won/Lost ──
    won_lost_models = []
    for r in won_lost:
        created = (r.get("CreatedDate") or "")[:10]
        close = r.get("CloseDate", "")
        age = 0
        if created and close:
            try:
                age = (
                    date.fromisoformat(snapshot_date) - date.fromisoformat(created)
                ).days
            except ValueError:
                pass
        won_lost_models.append(
            WonLostDeal(
                account=_val(r, "Account.Name"),
                opportunity=r.get("Name", ""),
                owner=_val(r, "Owner.Name"),
                stage=r.get("StageName", ""),
                close_date=close,
                arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
                deal_type=r.get("Type", ""),
                industry=_val(r, "Account.Industry"),
                sales_region=r.get("Sales_Region__c", ""),
                reason_won_lost=r.get("Reason_Won_Lost__c", ""),
                competitor=r.get("Lost_to_Competitor__c", ""),
                created_date=created,
                currency=corp_ccy,
                age_days=age,
                quarter=_quarter_label(close, int(period["analysis_year"])),
            )
        )

    # ── 3. Commercial Approval ──
    print("  Approvals... (filtering from pipeline)", end=" ", flush=True)
    approvals = [r for r in pipeline if str(r.get("Type") or "").strip() == "Land"]
    approved_2026, approved_prior, pending, missing = [], [], [], []
    for r in approvals:
        approval_status = str(r.get("Approval_Status__c") or "").strip()
        if r.get("Stage_20_Approval__c"):
            if str(r.get("Stage_20_Approval_Date__c", ""))[:4] == str(
                period["analysis_year"]
            ):
                approved_2026.append(r)
            else:
                approved_prior.append(r)
        elif (
            r.get("Submit_for_Stage_20_Review__c")
            or approval_status == "Needs Approval"
        ):
            pending.append(r)
        elif (
            r.get("StageName", "") >= "3" and approval_status != "No Approval Necessary"
        ):
            missing.append(r)
    print(
        f"{len(approvals)} land ({len(approved_2026)} approved 2026, {len(approved_prior)} prior, {len(pending)} pending, {len(missing)} missing stage 3+)"
    )

    def _approval_model(r, status, date_field="Stage_20_Approval_Date__c"):
        return ApprovalDeal(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
            status=status,
            approval_date=r.get(date_field) or None,
            next_step=r.get("NextStep", ""),
            lead_scope=r.get("Lead_Scope__c", ""),
            quarter=_quarter_label(
                r.get("CloseDate", ""), int(period["analysis_year"])
            ),
        )

    approval_models = (
        [
            _approval_model(r, f"Approved {period['analysis_year']}")
            for r in approved_2026
        ]
        + [_approval_model(r, "Approved (prior year)") for r in approved_prior]
        + [
            _approval_model(r, "Pending Approval", "Submit_for_Stage_20_Review_Date__c")
            for r in pending
        ]
        + [_approval_model(r, "Missing (Stage 3+)") for r in missing]
    )

    # ── 4. Renewals (open, reporting year, sorted by close date) ──
    print("  Renewals...", end=" ", flush=True)
    q = (
        f"SELECT Account.Name, Name, Owner.Name, StageName, CloseDate, "
        f"convertCurrency(Amount) Amount, Probability, NextStep FROM Opportunity "
        f"WHERE {where} AND IsClosed = false AND Type = 'Renewal' "
        f"{period['fy_close_filter']} {ACCOUNT_EXCLUDE} {OWNER_EXCLUDE} ORDER BY CloseDate ASC"
    )
    renewals = run_soql(session, instance_url, q, label=f"{territory}:renewals")
    print(f"{len(renewals)} renewals")

    renewal_models = [
        RenewalDeal(
            account=_val(r, "Account.Name"),
            opportunity=r.get("Name", ""),
            owner=_val(r, "Owner.Name"),
            stage=r.get("StageName", ""),
            close_date=r.get("CloseDate", ""),
            acv_unweighted=r.get("Amount") or 0,
            deal_type="Renewal",
            quarter=_quarter_label(
                r.get("CloseDate", ""), int(period["analysis_year"])
            ),
            probability=r.get("Probability") or 0,
            comments="",
        )
        for r in renewals
    ]

    # ── 5. Pipeline Inspection ──
    print("  PI view...", end=" ", flush=True)
    pi_raw = fetch_pi(session, instance_url, pi_lv, label=f"{territory}:pi")
    pi_rows = _build_pipeline_inspection_rows(
        pi_raw, analysis_year=int(period["analysis_year"]), corp_ccy=corp_ccy
    )
    print(f"{len(pi_rows)} open {period['fy_label']} deals")
    pi_models = [
        PIDeal(
            opportunity=row[0],
            owner=row[1],
            stage=row[2],
            forecast_category=row[3],
            arr_weighted=row[4] or 0,
            currency=row[5],
            close_date=row[6],
            push_count=row[7] or 0,
            score=row[8],
            priority=row[9] == "Yes",
        )
        for row in pi_rows
    ]

    # ── 5a. Forward-quarter Pipeline Inspection ──
    forward_pi_models: list[PIDeal] = []
    if forward_pi_source:
        print("  PI forward quarter...", end=" ", flush=True)
        forward_pi_raw = fetch_pi(
            session,
            instance_url,
            forward_pi_source["list_view_id"],
            label=f"{territory}:pi_forward:{forward_pi_source['quarter_label']}",
        )
        forward_pi_rows = _build_pipeline_inspection_rows(
            forward_pi_raw,
            analysis_year=int(period["analysis_year"]),
            close_start=forward_pi_source["start_date"],
            close_end=forward_pi_source["end_date"],
            corp_ccy=corp_ccy,
        )
        print(f"{len(forward_pi_rows)} open {forward_pi_source['quarter_title']} deals")
        forward_pi_models = [
            PIDeal(
                opportunity=row[0],
                owner=row[1],
                stage=row[2],
                forecast_category=row[3],
                arr_weighted=row[4] or 0,
                currency=row[5],
                close_date=row[6],
                push_count=row[7] or 0,
                score=row[8],
                priority=row[9] == "Yes",
            )
            for row in forward_pi_rows
        ]

    # ── 5b. Activity volume per open deal ──
    # Group Task + Event count per Opportunity for the 30/60/90-day windows.
    # Surfaces "silent" deals that LastActivityDate alone doesn't reveal
    # (e.g. deal with one email 65 days ago has LastActivityDate = 65d but
    # only shows up as "stale" in Deal Risk Scoring — activity count adds
    # whether there's ANY recent touch).
    print("  Activity volume...", end=" ", flush=True)
    open_opp_ids = [r.get("Id") for r in pipeline if r.get("Id")]
    activity_rows = []
    if open_opp_ids:
        # Salesforce caps IN () lists at ~10k ids; our director books are <100,
        # so we batch in 200s defensively.
        from datetime import timedelta as _td

        today = date.fromisoformat(snapshot_date)
        # Single 90-day window for now. If we add 30/60-day breakdowns later,
        # compute them the same way and COUNT_DISTINCT by WhatId over the
        # appropriate ActivityDate range.
        d90 = (today - _td(days=90)).isoformat()

        def _batch(seq, n):
            for i in range(0, len(seq), n):
                yield seq[i : i + n]

        # Per-opp activity aggregate. Separate Task and Event streams so we
        # can render a breakdown (calls/emails in Task, meetings in Event).
        # SOQL note: ActivityDate does not support MAX() — we rely on the
        # Opportunity's own LastActivityDate (already in PIPELINE_FIELDS) for
        # "last touch" timing, and use these queries purely for count.
        act_by_opp: dict[str, dict] = {}
        for batch in _batch(open_opp_ids, 200):
            ids_sql = ", ".join(f"'{i}'" for i in batch)
            q_task = (
                "SELECT WhatId, COUNT(Id) n "
                f"FROM Task WHERE WhatId IN ({ids_sql}) "
                f"AND ActivityDate >= {d90} GROUP BY WhatId"
            )
            q_event = (
                "SELECT WhatId, COUNT(Id) n "
                f"FROM Event WHERE WhatId IN ({ids_sql}) "
                f"AND ActivityDate >= {d90} GROUP BY WhatId"
            )
            try:
                tasks = run_soql(
                    session, instance_url, q_task, label=f"{territory}:tasks_90d"
                )
            except requests.HTTPError as exc:
                print(f"  [WARN] activity query failed: {exc}")
                tasks = []
            try:
                events = run_soql(
                    session, instance_url, q_event, label=f"{territory}:events_90d"
                )
            except requests.HTTPError as exc:
                print(f"  [WARN] activity query failed: {exc}")
                events = []
            for rec in tasks:
                wid = rec.get("WhatId")
                if not wid:
                    continue
                a = act_by_opp.setdefault(
                    wid, {"tasks": 0, "events": 0, "last_date": None}
                )
                a["tasks"] = int(rec.get("n") or 0)
                d = rec.get("last_date")
                if d and (a["last_date"] is None or d > a["last_date"]):
                    a["last_date"] = d
            for rec in events:
                wid = rec.get("WhatId")
                if not wid:
                    continue
                a = act_by_opp.setdefault(
                    wid, {"tasks": 0, "events": 0, "last_date": None}
                )
                a["events"] = int(rec.get("n") or 0)
                d = rec.get("last_date")
                if d and (a["last_date"] is None or d > a["last_date"]):
                    a["last_date"] = d

        # Compose activity rows: counts from the aggregate dict, last-touch
        # date from the Opportunity's own LastActivityDate (which SOQL already
        # computes across Tasks+Events+EmailMessage so is authoritative).
        for rec in pipeline:
            oid = rec.get("Id")
            agg = act_by_opp.get(str(oid or ""), {"tasks": 0, "events": 0})
            total_90 = int(agg.get("tasks", 0)) + int(agg.get("events", 0))
            activity_rows.append(
                ActivitySignal(
                    account=_val(rec, "Account.Name"),
                    opportunity=rec.get("Name", ""),
                    owner=_val(rec, "Owner.Name"),
                    tasks_90d=int(agg.get("tasks", 0)),
                    events_90d=int(agg.get("events", 0)),
                    total_touches_90d=total_90,
                    last_activity_date=rec.get("LastActivityDate") or None,
                    flag="No touch 90d" if total_90 == 0 else "",
                )
            )
    activity_models = activity_rows
    print(
        f"{sum(1 for a in activity_models if a.total_touches_90d == 0)} silent deals (90d)"
    )

    # ── 5c. Commit items ──
    print("  Commit items... ", end=" ", flush=True)
    commit_models = []
    for r in pipeline:
        cat = str(r.get("ForecastCategoryName") or "").strip()
        if not cat or cat == "Omitted":
            continue
        close = str(r.get("CloseDate") or "")[:10]
        commit_models.append(
            CommitItem(
                account=_val(r, "Account.Name"),
                opportunity=r.get("Name", ""),
                owner=_val(r, "Owner.Name"),
                forecast_category=cat,
                arr_weighted=r.get("APTS_Forecast_ARR__c") or 0,
                arr_unweighted=r.get("APTS_Opportunity_ARR__c") or 0,
                close_date=close,
                period=_quarter_label(close, int(period["analysis_year"])),
                stage=r.get("StageName", ""),
            )
        )
    print(f"{len(commit_models)} commit rows")

    # ── 6. Q1 Movement (field history analysis) ──
    print("  Q1 movement...", end=" ", flush=True)
    # Convert the Opportunity-root `where` clause into one that works from
    # OpportunityFieldHistory: every bare SimCorp field needs an "Opportunity." prefix.
    # Do a targeted replace on the known SimCorp relationships so it works for any territory.
    where_for_history = where
    for fld in (
        "Account_Unit_Group__c",
        "Sales_Region__c",
        "Account.Unit__c",
        "Account.Region__c",
        "Account.Industry",
        "Lead_Scope__c",
    ):
        where_for_history = where_for_history.replace(fld, f"Opportunity.{fld}")
    # Avoid double-prefixing (e.g. if `where` already had "Opportunity." before any of them)
    where_for_history = where_for_history.replace(
        "Opportunity.Opportunity.", "Opportunity."
    )
    # Extended capture: pull CloseDate + StageName + ForecastCategoryName
    # history in one query so Q1 slip classification, stage-at-loss, and
    # commit-drift analyses all share one source-of-truth event stream.
    close_history = run_soql(
        session,
        instance_url,
        f"""
        SELECT OpportunityId, Opportunity.Name, Opportunity.Account.Name,
               Opportunity.Owner.Name, Opportunity.StageName, Opportunity.CloseDate,
               convertCurrency(Opportunity.APTS_Opportunity_ARR__c) APTS_Opportunity_ARR__c,
               Opportunity.IsClosed, Opportunity.IsWon,
               Field, OldValue, NewValue, CreatedDate
        FROM OpportunityFieldHistory
        WHERE Field IN ('CloseDate', 'StageName', 'ForecastCategoryName')
          AND {where_for_history}
          AND (NOT Opportunity.Account.Name LIKE '%simcorp%')
          AND (NOT Opportunity.Account.Name LIKE '%test%')
        ORDER BY CreatedDate ASC
    """,
        label=f"{territory}:field_history",
    )
    # Filter to CloseDate events for the existing Q1 slip / post-Q1 push
    # classification below. StageName and ForecastCategoryName events are
    # written separately so downstream analytics can consume them.
    stage_history_events = [r for r in close_history if r.get("Field") == "StageName"]
    fcat_history_events = [
        r for r in close_history if r.get("Field") == "ForecastCategoryName"
    ]
    close_history = [r for r in close_history if r.get("Field") == "CloseDate"]

    # Classify: Q1 slipped, post-Q1 pushed
    q1_movement_models = []
    seen_slip = set()
    seen_push = set()
    for r in close_history:
        opp = r.get("Opportunity") or {}
        oid = r.get("OpportunityId", "")
        name = opp.get("Name", "")
        old_val = str(r.get("OldValue", ""))
        new_val = str(r.get("NewValue", ""))
        change_date = str(r.get("CreatedDate", ""))[:10]
        arr = opp.get("APTS_Opportunity_ARR__c") or 0
        account = (opp.get("Account") or {}).get("Name", "")
        owner = (opp.get("Owner") or {}).get("Name", "")
        stage = opp.get("StageName", "")
        is_closed = opp.get("IsClosed", False)

        if (
            old_val >= period["q1_start"]
            and old_val <= period["q1_end"]
            and new_val > period["q1_end"]
            and change_date >= period["q1_start"]
            and oid not in seen_slip
        ):
            seen_slip.add(oid)
            q1_movement_models.append(
                MovementEvent(
                    account=account,
                    opportunity=name,
                    owner=owner,
                    stage=stage,
                    movement_type="Q1 Slipped",
                    old_close=old_val,
                    new_close=new_val,
                    changed_on=change_date,
                    arr_unweighted=arr,
                )
            )

        if change_date >= period["q2_start"] and not is_closed and oid not in seen_push:
            seen_push.add(oid)
            q1_movement_models.append(
                MovementEvent(
                    account=account,
                    opportunity=name,
                    owner=owner,
                    stage=stage,
                    movement_type="Post-Q1 Push",
                    old_close=old_val,
                    new_close=new_val,
                    changed_on=change_date,
                    arr_unweighted=arr,
                )
            )

    print(
        f"{sum(1 for m in q1_movement_models if m.movement_type == 'Q1 Slipped')} Q1 slips, {sum(1 for m in q1_movement_models if m.movement_type == 'Post-Q1 Push')} post-Q1 pushes"
    )

    # ── 6a. Q2 Movement ──
    print("  Q2 movement...", end=" ", flush=True)
    q2_movement_models = []
    seen_q2_slip = set()
    seen_q2_push = set()
    for r_raw in close_history:
        opp = r_raw.get("Opportunity") or {}
        oid = r_raw.get("OpportunityId", "")
        old_val = str(r_raw.get("OldValue", ""))
        new_val = str(r_raw.get("NewValue", ""))
        change_date = str(r_raw.get("CreatedDate", ""))[:10]
        arr = opp.get("APTS_Opportunity_ARR__c") or 0
        account = (opp.get("Account") or {}).get("Name", "")
        owner = (opp.get("Owner") or {}).get("Name", "")
        stage = opp.get("StageName", "")
        is_closed = opp.get("IsClosed", False)

        if (
            old_val >= period["q2_start"]
            and old_val <= period["q2_end"]
            and new_val > period["q2_end"]
            and change_date >= period["q2_start"]
            and oid not in seen_q2_slip
        ):
            seen_q2_slip.add(oid)
            q2_movement_models.append(
                MovementEvent(
                    account=account,
                    opportunity=opp.get("Name", ""),
                    owner=owner,
                    stage=stage,
                    movement_type="Q2 Slipped",
                    old_close=old_val,
                    new_close=new_val,
                    changed_on=change_date,
                    arr_unweighted=arr,
                )
            )

        if (
            change_date >= period["q3_start"]
            and not is_closed
            and oid not in seen_q2_push
        ):
            seen_q2_push.add(oid)
            q2_movement_models.append(
                MovementEvent(
                    account=account,
                    opportunity=opp.get("Name", ""),
                    owner=owner,
                    stage=stage,
                    movement_type="Post-Q2 Push",
                    old_close=old_val,
                    new_close=new_val,
                    changed_on=change_date,
                    arr_unweighted=arr,
                )
            )

    print(
        f"{sum(1 for m in q2_movement_models if m.movement_type == 'Q2 Slipped')} Q2 slips, {sum(1 for m in q2_movement_models if m.movement_type == 'Post-Q2 Push')} post-Q2 pushes"
    )

    # ── 6b. Stage + Forecast Category + Close Date History ──
    def _event_fields(r):
        opp = r.get("Opportunity") or {}
        return {
            "opportunity_id": r.get("OpportunityId", ""),
            "opportunity": opp.get("Name", ""),
            "account": (opp.get("Account") or {}).get("Name", ""),
            "owner": (opp.get("Owner") or {}).get("Name", ""),
            "current_stage": opp.get("StageName", ""),
            "old_value": str(r.get("OldValue") or ""),
            "new_value": str(r.get("NewValue") or ""),
            "created_date": str(r.get("CreatedDate", ""))[:10],
            "arr_unweighted": opp.get("APTS_Opportunity_ARR__c") or 0,
        }

    stage_models = [
        StageEvent(
            **_event_fields(r),
            is_closed=(r.get("Opportunity") or {}).get("IsClosed", False),
            is_won=(r.get("Opportunity") or {}).get("IsWon", False),
        )
        for r in stage_history_events
    ]
    fcat_models = [ForecastEvent(**_event_fields(r)) for r in fcat_history_events]
    close_date_models = [
        CloseDateEvent(
            **_event_fields(r),
            is_closed=(r.get("Opportunity") or {}).get("IsClosed", False),
        )
        for r in close_history
    ]
    print(f"  Stage history: {len(stage_models)} events")
    print(f"  Forecast category history: {len(fcat_models)} events")

    # ── Assemble DirectorBundle ──
    source_contract = SourceContract(
        sf_org="simcorp.my.salesforce.com",
        api_version=SF_API_VERSION,
        territory_soql_where=where,
        extract_timestamp=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        sources={
            "pipeline_open": DatasetSource(
                "soql", None, f"{territory}:all_fy_deals", len(pipeline_models), 0
            ),
            "won_lost": DatasetSource(
                "soql", None, f"{territory}:all_fy_deals", len(won_lost_models), 0
            ),
            "renewals": DatasetSource(
                "soql", None, f"{territory}:renewals", len(renewal_models), 0
            ),
            "pi_current": DatasetSource(
                "list_view", str(pi_lv), f"{territory}:pi", len(pi_models), 0
            ),
            "pi_forward": DatasetSource(
                "list_view",
                str((forward_pi_source or {}).get("list_view_id", "")) or None,
                f"{territory}:pi_forward",
                len(forward_pi_models),
                0,
            )
            if forward_pi_source
            else DatasetSource("list_view", None, f"{territory}:pi_forward", 0, 0),
            "activity": DatasetSource(
                "soql", None, f"{territory}:activity", len(activity_models), 0
            ),
            "stage_events": DatasetSource(
                "field_history",
                None,
                f"{territory}:field_history",
                len(stage_models),
                0,
            ),
        },
    )

    dataset_counts = {
        "pipeline_open": len(pipeline_models),
        "won_lost": len(won_lost_models),
        "renewals": len(renewal_models),
        "approvals": len(approval_models),
        "pi_current": len(pi_models),
        "pi_forward": len(forward_pi_models),
        "activity": len(activity_models),
        "commit_items": len(commit_models),
        "stage_events": len(stage_models),
        "forecast_category_events": len(fcat_models),
        "close_date_events": len(close_date_models),
        "movement_prior": len(q1_movement_models),
        "movement_current": len(q2_movement_models),
        "snapshot_trend": 0,
    }

    bundle = DirectorBundle(
        schema_version="1",
        snapshot_date=snapshot_date,
        director=director,
        territory=territory,
        corp_ccy=corp_ccy,
        extract_timestamp=source_contract.extract_timestamp,
        source_contract=source_contract,
        dataset_counts=dataset_counts,
        datasets=Datasets(
            pipeline_open=pipeline_models,
            won_lost=won_lost_models,
            renewals=renewal_models,
            approvals=approval_models,
            pi_current=pi_models,
            pi_forward=forward_pi_models,
            activity=activity_models,
            commit_items=commit_models,
            stage_events=stage_models,
            forecast_category_events=fcat_models,
            close_date_events=close_date_models,
            movement_prior=q1_movement_models,
            movement_current=q2_movement_models,
            snapshot_trend=[],
        ),
    )

    # Write JSON bundle
    slug = re.sub(r"[^a-z0-9]+", "-", director.lower()).strip("-")
    bundle_dir = BUNDLE_OUTPUT_ROOT / snapshot_date
    bundle_dir.mkdir(parents=True, exist_ok=True)
    bundle_path = bundle_dir / f"{slug}.json"
    bundle_path.write_text(bundle.to_json() + "\n", encoding="utf-8")

    # Render Excel workbook from the bundle
    render_bundle_to_excel(bundle, output_path)

    print(f"\n  Saved: {output_path}")
    print(f"  Bundle: {bundle_path}")
    print(
        f"  Pipeline: {len(pipeline_models)} deals, {corp_ccy} {sum(d.arr_unweighted for d in pipeline_models):,.0f}"
    )

    # Return dict (backward compat for audit — Phase 1)
    won = [d for d in won_lost_models if "Won" in d.stage]
    lost = [d for d in won_lost_models if "Lost" in d.stage or "Opt Out" in d.stage]
    return {
        "territory": territory,
        "director": director,
        "snapshot_date": snapshot_date,
        "workbook_path": _display_path(output_path),
        "bundle_path": _display_path(bundle_path),
        "analysis_year": int(period["analysis_year"]),
        "fy_label": str(period["fy_label"]),
        "counts": {
            "pipeline_open": len(pipeline_models),
            "won_lost": len(won_lost_models),
            "won": len(won),
            "lost": len(lost),
            "commercial_approval_land": len(approvals),
            "commercial_approval_sheet_rows": len(approval_models),
            "approved_current_year": len(approved_2026),
            "approved_prior_year": len(approved_prior),
            "pending_approval": len(pending),
            "missing_approval": len(missing),
            "renewals": len(renewal_models),
            "pipeline_inspection": len(pi_models),
            "pipeline_inspection_forward": len(forward_pi_models),
            "activity_volume_rows": len(activity_models),
            "commit_items": len(commit_models),
            "q1_movement": len(q1_movement_models),
            "q2_movement": len(q2_movement_models),
            "stage_history_events": len(stage_models),
            "forecast_category_history_events": len(fcat_models),
        },
        "arr": {
            "pipeline_open_eur": sum(d.arr_unweighted for d in pipeline_models),
            "won_eur": sum(d.arr_unweighted for d in won),
            "lost_eur": sum(d.arr_unweighted for d in lost),
            "renewal_acv_eur": sum(d.acv_unweighted for d in renewal_models),
        },
        "pi_source": {
            "list_view_id": str(pi_lv),
            "scope": str(period["fy_label"]),
            "deal_count": len(pi_models),
        },
        "forward_quarter_pi": {
            "status": forward_pi_source_origin,
            "quarter_label": str(period["forward_quarter_label"]),
            "quarter_title": str(period["forward_quarter_title"]),
            "list_view_id": str((forward_pi_source or {}).get("list_view_id", ""))
            if isinstance(forward_pi_source, dict)
            else "",
            "list_view_label": str((forward_pi_source or {}).get("list_view_label", ""))
            if isinstance(forward_pi_source, dict)
            else "",
            "deal_count": len(forward_pi_models),
        },
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--territory", default=None, help="Territory name (e.g. APAC)")
    parser.add_argument("--all", action="store_true", help="Extract all territories")
    parser.add_argument("--snapshot-date", default=datetime.now().strftime("%Y-%m-%d"))
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()

    if args.all:
        territories = list(TERRITORIES.keys())
    elif args.territory:
        if args.territory not in TERRITORIES:
            print(f"Unknown territory: {args.territory}", file=sys.stderr)
            print(f"Available: {', '.join(TERRITORIES.keys())}", file=sys.stderr)
            sys.exit(1)
        territories = [args.territory]
    else:
        print("Specify --territory or --all", file=sys.stderr)
        sys.exit(1)

    # Auth once; reuse across every territory. Saves ~1s × N sf-CLI subprocess
    # calls and removes N/A token-rotation windows mid-run.
    _run_started_at = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    token, instance_url = get_auth()
    session = build_session(token)
    corp_ccy = get_corporate_currency(session, instance_url)

    _territory_durations: dict[str, float] = {}

    def _run_one(territory):
        config = TERRITORIES[territory]
        slug = re.sub(r"[^a-z0-9]+", "-", config["director"].lower()).strip("-")
        output_path = args.output_root / args.snapshot_date / f"{slug}.xlsx"
        return extract_territory(
            territory,
            args.snapshot_date,
            output_path,
            session=session,
            instance_url=instance_url,
            corp_ccy=corp_ccy,
        )

    def _run_one_timed(territory):
        import time

        t0 = time.monotonic()
        result = _run_one(territory)
        _territory_durations[territory] = round(time.monotonic() - t0, 1)
        return result

    processed: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []

    # Parallelize when multiple territories — each extraction is ~80% I/O
    # against SF, so threads scale well. Sequential for a single-territory
    # run to keep logs readable.
    if len(territories) > 1:
        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"\nExtracting {len(territories)} territories in parallel...\n")
        with ThreadPoolExecutor(max_workers=min(9, len(territories))) as pool:
            futures = {pool.submit(_run_one_timed, t): t for t in territories}
            for f in as_completed(futures):
                t = futures[f]
                try:
                    processed.append(f.result())
                except Exception as exc:
                    import traceback as _tb

                    failures.append(
                        {
                            "territory": t,
                            "error_type": exc.__class__.__name__,
                            "message": str(exc),
                            "traceback": _tb.format_exc(),
                        }
                    )
                    print(f"  [FAIL] {t}: {exc}\n{_tb.format_exc()}")
    else:
        for t in territories:
            try:
                processed.append(_run_one_timed(t))
            except Exception as exc:
                import traceback as _tb

                failures.append(
                    {
                        "territory": t,
                        "error_type": exc.__class__.__name__,
                        "message": str(exc),
                        "traceback": _tb.format_exc(),
                    }
                )
                print(f"  [FAIL] {t}: {exc}\n{_tb.format_exc()}")

    # Flush query telemetry to a run log so manifest builders can pick it up.
    tele_path = None
    if QUERY_TELEMETRY:
        from datetime import datetime as _dt

        tele_dir = REPO_ROOT / "output" / "pipeline_logs" / args.snapshot_date
        tele_dir.mkdir(parents=True, exist_ok=True)
        tele_path = tele_dir / "sf_query_telemetry.json"
        existing = []
        if tele_path.exists():
            try:
                existing = json.loads(tele_path.read_text())
            except (json.JSONDecodeError, ValueError):
                existing = []
        existing.append(
            {
                "stage": "extract_director_live",
                "run_at": _dt.now().isoformat(timespec="seconds"),
                "queries": QUERY_TELEMETRY,
                "totals": {
                    "queries": len(QUERY_TELEMETRY),
                    "rows": sum(q["rows"] for q in QUERY_TELEMETRY),
                    "duration_ms": sum(q["duration_ms"] for q in QUERY_TELEMETRY),
                },
            }
        )
        tele_path.write_text(json.dumps(existing, indent=2) + "\n")
        print(f"\nSF query telemetry: {tele_path.relative_to(REPO_ROOT)}")

    processed.sort(key=lambda item: str(item.get("territory") or ""))
    query_totals = {
        "queries": len(QUERY_TELEMETRY),
        "rows": sum(int(item.get("rows") or 0) for item in QUERY_TELEMETRY),
        "duration_ms": sum(
            int(item.get("duration_ms") or 0) for item in QUERY_TELEMETRY
        ),
    }
    audit_payload = {
        "run_date": args.snapshot_date,
        "status": "failed"
        if (failures and not processed)
        else "partial"
        if failures
        else "ok",
        "scope": "all" if args.all else "territory",
        "territories_requested": territories,
        "processed": processed,
        "failures": failures,
        "query_telemetry_totals": query_totals,
        "query_telemetry_path": _display_path(tele_path) if tele_path else "",
    }
    audit_dir = AUDIT_OUTPUT_ROOT / args.snapshot_date
    _write_run_audit(audit_dir, audit_payload)
    print(f"Director live extract audit: {_display_path(audit_dir)}")

    _write_run_manifest(
        BUNDLE_OUTPUT_ROOT / args.snapshot_date / "manifest.json",
        processed=processed,
        failures=failures,
        durations=_territory_durations,
        snapshot_date=args.snapshot_date,
        started_at=_run_started_at,
        finished_at=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        query_telemetry_totals=query_totals,
    )
    print(
        f"Run manifest: {_display_path(BUNDLE_OUTPUT_ROOT / args.snapshot_date / 'manifest.json')}"
    )

    if failures:
        print(f"\n{len(failures)} failure(s):")
        for item in failures:
            print(f"  {item['territory']}: {item['message']}")
        if not processed:
            sys.exit(1)  # total failure — no territories succeeded
        sys.exit(2)  # partial success — some territories succeeded


if __name__ == "__main__":
    main()
