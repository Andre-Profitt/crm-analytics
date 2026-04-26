from __future__ import annotations

from calendar import monthrange
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from typing import Any, Literal


QuarterPolicyName = Literal["calendar_quarter", "fiscal_quarter"]
BusinessCalendarName = Literal["calendar", "fiscal"]
FiscalYearNamingPolicy = Literal["start_year", "end_year"]


@dataclass(frozen=True)
class QuarterPolicy:
    name: str
    rule: str
    source: str
    fiscal_year_start_month: int
    note: str


@dataclass(frozen=True)
class QuarterWindow:
    label: str
    title: str
    year: int
    quarter: int
    start_date: str
    end_date: str
    month_start: str
    month_end: str
    range_label: str


@dataclass(frozen=True)
class BusinessPeriod:
    calendar: BusinessCalendarName
    fiscal_year_start_month: int
    fiscal_year_naming_policy: FiscalYearNamingPolicy
    current_quarter: QuarterWindow
    prior_quarter: QuarterWindow
    forward_quarter: QuarterWindow
    current_quarter_label: str
    prior_quarter_label: str
    forward_quarter_label: str


@dataclass(frozen=True)
class SourceRegistryPeriod:
    calendar: str
    quarter_policy_name: str
    quarter_label_style: str
    fiscal_year_start_month: int
    current_quarter: QuarterWindow
    prior_quarter: QuarterWindow
    forward_quarter: QuarterWindow
    current_quarter_label: str
    prior_quarter_label: str
    forward_quarter_label: str
    reason: str


@dataclass(frozen=True)
class DisplayPeriod:
    label_source: str
    current_quarter_label: str
    prior_quarter_label: str
    forward_quarter_label: str
    reason: str


@dataclass(frozen=True)
class QuarterMappingApproval:
    approved: bool
    approved_by: str
    approved_at: str
    reason: str
    business_current_quarter_label: str
    source_current_quarter_label: str
    display_current_quarter_label: str


@dataclass(frozen=True)
class PeriodContext:
    as_of_date: str
    reporting_month: str
    snapshot_date: str
    deck_date: str
    month_title: str
    fiscal_year: str
    quarter_policy: QuarterPolicy
    business_period: BusinessPeriod
    source_registry_period: SourceRegistryPeriod
    display_period: DisplayPeriod
    quarter_mapping: QuarterMappingApproval
    current_quarter: QuarterWindow
    prior_quarter: QuarterWindow
    forward_quarter: QuarterWindow
    reporting_window_start: str
    reporting_window_end: str

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


_MONTH_LABELS = {
    1: "Jan",
    2: "Feb",
    3: "Mar",
    4: "Apr",
    5: "May",
    6: "Jun",
    7: "Jul",
    8: "Aug",
    9: "Sep",
    10: "Oct",
    11: "Nov",
    12: "Dec",
}

_MONTH_TITLES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}


def _coerce_date(value: str | date | datetime | None) -> date:
    if value is None:
        return datetime.now().date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return date.fromisoformat(str(value)[:10])


def _last_day_of_previous_month(as_of: date) -> date:
    first_of_month = as_of.replace(day=1)
    return first_of_month - timedelta(days=1)


def _quarter_policy(
    name: QuarterPolicyName,
    fiscal_year_start_month: int,
) -> QuarterPolicy:
    if not 1 <= fiscal_year_start_month <= 12:
        raise ValueError("fiscal_year_start_month must be between 1 and 12")
    if name == "calendar_quarter":
        return QuarterPolicy(
            name=name,
            rule="Q1 Jan-Mar, Q2 Apr-Jun, Q3 Jul-Sep, Q4 Oct-Dec.",
            source="scripts/monthly_platform/period.py",
            fiscal_year_start_month=1,
            note=(
                "Current source-backed monthly report registry uses calendar quarter "
                "labels. This is explicit to prevent silent drift between Salesforce "
                "report labels, workbook sheets, and deck fallback behavior."
            ),
        )
    return QuarterPolicy(
        name=name,
        rule=(
            f"Q1 starts in month {fiscal_year_start_month}; each quarter spans "
            "three months from that fiscal-year anchor."
        ),
        source="scripts/monthly_platform/period.py",
        fiscal_year_start_month=fiscal_year_start_month,
        note=(
            "Use this only when the Salesforce source registry and deck labels are "
            "migrated to fiscal-quarter source IDs."
        ),
    )


def _add_months(year: int, month: int, delta: int) -> tuple[int, int]:
    absolute_month = year * 12 + (month - 1) + delta
    return absolute_month // 12, (absolute_month % 12) + 1


def _quarter_window(target: date, policy: QuarterPolicy) -> QuarterWindow:
    anchor_month = int(policy.fiscal_year_start_month)
    month_offset = (target.month - anchor_month) % 12
    quarter = (month_offset // 3) + 1
    policy_year = target.year if target.month >= anchor_month else target.year - 1
    start_year, start_month = _add_months(policy_year, anchor_month, (quarter - 1) * 3)
    end_year, end_month = _add_months(start_year, start_month, 2)
    start = date(start_year, start_month, 1)
    end = date(end_year, end_month, monthrange(end_year, end_month)[1])
    return QuarterWindow(
        label=f"Q{quarter}",
        title=f"Q{quarter} {policy_year}",
        year=policy_year,
        quarter=quarter,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        month_start=f"{start_year}-{start_month:02d}",
        month_end=f"{end_year}-{end_month:02d}",
        range_label=f"{_MONTH_LABELS[start_month]}-{_MONTH_LABELS[end_month]}",
    )


def _shift_quarter(window: QuarterWindow, delta: int, policy: QuarterPolicy) -> QuarterWindow:
    quarter = window.quarter + delta
    year = window.year
    while quarter < 1:
        quarter += 4
        year -= 1
    while quarter > 4:
        quarter -= 4
        year += 1
    start_year, start_month = _add_months(
        year,
        int(policy.fiscal_year_start_month),
        (quarter - 1) * 3,
    )
    return _quarter_window(date(start_year, start_month, 1), policy)


def _fiscal_year_number(window: QuarterWindow, naming_policy: FiscalYearNamingPolicy) -> int:
    if naming_policy == "end_year":
        return window.year + 1
    return window.year


def _business_quarter_label(
    window: QuarterWindow,
    *,
    naming_policy: FiscalYearNamingPolicy,
) -> str:
    fiscal_year = _fiscal_year_number(window, naming_policy)
    return f"FY{fiscal_year % 100:02d} Q{window.quarter}"


def resolve_period_context(
    *,
    as_of_date: str | date | datetime | None = None,
    snapshot_date: str | date | datetime | None = None,
    deck_date: str | date | datetime | None = None,
    quarter_policy_name: QuarterPolicyName = "calendar_quarter",
    fiscal_year_start_month: int = 2,
    business_calendar: BusinessCalendarName = "fiscal",
    business_fiscal_year_start_month: int = 2,
    fiscal_year_naming_policy: FiscalYearNamingPolicy = "start_year",
    quarter_mapping_approved: bool = True,
    quarter_mapping_approved_by: str = "repo_config",
    quarter_mapping_approved_at: str = "2026-04-24T00:00:00Z",
    quarter_mapping_reason: str = (
        "Salesforce source registry remains calendar-quarter labelled while "
        "business reporting uses an explicitly mapped fiscal period."
    ),
) -> PeriodContext:
    as_of = _coerce_date(as_of_date)
    snapshot = (
        _coerce_date(snapshot_date)
        if snapshot_date is not None
        else _last_day_of_previous_month(as_of)
    )
    deck = _coerce_date(deck_date) if deck_date is not None else snapshot

    policy = _quarter_policy(quarter_policy_name, fiscal_year_start_month)
    current = _quarter_window(snapshot, policy)
    prior = _shift_quarter(current, -1, policy)
    forward = _shift_quarter(current, 1, policy)
    business_policy_name: QuarterPolicyName = (
        "fiscal_quarter" if business_calendar == "fiscal" else "calendar_quarter"
    )
    business_policy = _quarter_policy(
        business_policy_name,
        business_fiscal_year_start_month if business_calendar == "fiscal" else 1,
    )
    business_current = _quarter_window(snapshot, business_policy)
    business_prior = _shift_quarter(business_current, -1, business_policy)
    business_forward = _shift_quarter(business_current, 1, business_policy)
    business_period = BusinessPeriod(
        calendar=business_calendar,
        fiscal_year_start_month=business_policy.fiscal_year_start_month,
        fiscal_year_naming_policy=fiscal_year_naming_policy,
        current_quarter=business_current,
        prior_quarter=business_prior,
        forward_quarter=business_forward,
        current_quarter_label=_business_quarter_label(
            business_current,
            naming_policy=fiscal_year_naming_policy,
        ),
        prior_quarter_label=_business_quarter_label(
            business_prior,
            naming_policy=fiscal_year_naming_policy,
        ),
        forward_quarter_label=_business_quarter_label(
            business_forward,
            naming_policy=fiscal_year_naming_policy,
        ),
    )
    source_registry_period = SourceRegistryPeriod(
        calendar="calendar" if policy.name == "calendar_quarter" else "fiscal",
        quarter_policy_name=policy.name,
        quarter_label_style=(
            "salesforce_calendar_label"
            if policy.name == "calendar_quarter"
            else "salesforce_fiscal_label"
        ),
        fiscal_year_start_month=policy.fiscal_year_start_month,
        current_quarter=current,
        prior_quarter=prior,
        forward_quarter=forward,
        current_quarter_label=current.title,
        prior_quarter_label=prior.title,
        forward_quarter_label=forward.title,
        reason=policy.note,
    )
    display_period = DisplayPeriod(
        label_source="source_registry_period",
        current_quarter_label=source_registry_period.current_quarter_label,
        prior_quarter_label=source_registry_period.prior_quarter_label,
        forward_quarter_label=source_registry_period.forward_quarter_label,
        reason=(
            "Display remains aligned to the Salesforce source registry until "
            "business-facing fiscal labels are approved for deck publishing."
        ),
    )
    quarter_mapping = QuarterMappingApproval(
        approved=quarter_mapping_approved,
        approved_by=quarter_mapping_approved_by,
        approved_at=quarter_mapping_approved_at,
        reason=quarter_mapping_reason,
        business_current_quarter_label=business_period.current_quarter_label,
        source_current_quarter_label=source_registry_period.current_quarter_label,
        display_current_quarter_label=display_period.current_quarter_label,
    )
    reporting_start_year, reporting_start_month = _add_months(
        current.year,
        int(policy.fiscal_year_start_month),
        0,
    )

    return PeriodContext(
        as_of_date=as_of.isoformat(),
        reporting_month=snapshot.strftime("%Y-%m"),
        snapshot_date=snapshot.isoformat(),
        deck_date=deck.isoformat(),
        month_title=f"{_MONTH_TITLES[snapshot.month]} {snapshot.year}",
        fiscal_year=f"FY{current.year % 100:02d}",
        quarter_policy=policy,
        business_period=business_period,
        source_registry_period=source_registry_period,
        display_period=display_period,
        quarter_mapping=quarter_mapping,
        current_quarter=current,
        prior_quarter=prior,
        forward_quarter=forward,
        reporting_window_start=date(reporting_start_year, reporting_start_month, 1).isoformat(),
        reporting_window_end=forward.end_date,
    )


def sheet_names(fy: str = "") -> dict[str, str]:
    """Canonical workbook sheet name contract.

    Returns a dict mapping semantic keys to the actual Excel sheet names.
    All consumers should use these keys instead of hardcoding sheet names.
    If fy is not provided, resolves from current date.
    """
    if not fy:
        fy = f"FY{datetime.now().year % 100:02d}"
    return {
        "pipeline_open": f"Pipeline Open {fy}",
        "won_lost": f"Won Lost {fy}",
        "renewals": f"Renewals {fy}",
        "commercial_approval": "Commercial Approval",
        "pipeline_inspection": "Pipeline Inspection",
        "q1_movement": "Q1 Movement",
        "q2_movement": "Q2 Movement",
        "activity_volume": "Activity Volume",
        "commit_items": "Commit Items",
        "stage_history": "Stage History",
        "forecast_cat_history": "Forecast Category History",
        "close_date_history": "Close Date History",
    }
