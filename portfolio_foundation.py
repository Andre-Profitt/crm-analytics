#!/usr/bin/env python3
"""Shared utilities for the redesigned CRM Analytics portfolio builders."""

from __future__ import annotations

import math
from datetime import datetime


def safe_float(value, default=0.0):
    """Convert a value to float, returning default on bad input."""
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def coerce_bool(value) -> bool:
    """Parse Salesforce-style truthy values."""
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() == "true"


def month_key(date_str: str) -> str:
    """Return YYYY-MM for an ISO date string, or empty string."""
    if not date_str or len(date_str) < 7:
        return ""
    return date_str[:7]


def current_month_key(now: datetime | None = None) -> str:
    """Return the current calendar month as YYYY-MM."""
    current = now or datetime.now()
    return current.strftime("%Y-%m")


def last_complete_month_key(now: datetime | None = None) -> str:
    """Return the prior calendar month as YYYY-MM."""
    current = now or datetime.now()
    year = current.year
    month = current.month - 1
    if month == 0:
        year -= 1
        month = 12
    return f"{year:04d}-{month:02d}"


def month_start(date_str: str) -> str:
    """Return YYYY-MM-01 for an ISO date string, or empty string."""
    key = month_key(date_str)
    return f"{key}-01" if key else ""


def quarter_label(date_str: str) -> str:
    """Derive Q1-Q4 from an ISO date string."""
    if not date_str or len(date_str) < 7:
        return ""
    try:
        month = int(date_str[5:7])
    except ValueError:
        return ""
    if month <= 3:
        return "Q1"
    if month <= 6:
        return "Q2"
    if month <= 9:
        return "Q3"
    return "Q4"


def fiscal_label(year_value) -> str:
    """Format a CRM Analytics FY label."""
    try:
        year = int(year_value)
    except (TypeError, ValueError):
        return ""
    return f"FY{year}"


def current_fiscal_year(now: datetime | None = None) -> int:
    """Return the current fiscal year.

    This org's FY labels align with the calendar year.
    """
    current = now or datetime.now()
    return current.year


def current_fy_label(now: datetime | None = None) -> str:
    """Return the current FY label."""
    return fiscal_label(current_fiscal_year(now))


def normalize_motion(opp_type: str) -> str:
    """Map opportunity type values to a smaller motion vocabulary."""
    value = (opp_type or "").strip()
    if value == "Renewal":
        return "Renewal"
    if value == "Expand":
        return "Expand"
    if value == "Land":
        return "Land"
    if value in {"PS", "Fast track PS", "Coric PS"}:
        return "Services"
    return "Other"


def forecast_weight(forecast_category: str, probability: float = 0.0) -> float:
    """Return an operational forecast weight."""
    value = (forecast_category or "").strip().lower().replace(" ", "")
    if value == "commit":
        return 0.90
    if value == "bestcase":
        return 0.50
    if value == "pipeline":
        return 0.20
    if value == "omitted":
        return 0.05
    if probability > 0:
        return min(1.0, max(0.05, probability / 100.0))
    return 0.20


def risk_level_to_score(risk_level: str) -> float:
    """Map a renewal/churn risk label to a numeric score."""
    value = (risk_level or "").strip().lower()
    if value in {"critical", "very high"}:
        return 95.0
    if value == "high":
        return 80.0
    if value == "medium":
        return 55.0
    if value == "low":
        return 25.0
    return 10.0


def month_sequence(start_key: str, end_key: str) -> list[str]:
    """Return an inclusive list of YYYY-MM keys."""
    if not start_key or not end_key:
        return []
    try:
        start = datetime.strptime(f"{start_key}-01", "%Y-%m-%d")
        end = datetime.strptime(f"{end_key}-01", "%Y-%m-%d")
    except ValueError:
        return []

    months: list[str] = []
    year = start.year
    month = start.month
    while (year, month) <= (end.year, end.month):
        months.append(f"{year:04d}-{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return months


def running_total(values: list[float]) -> list[float]:
    """Return a cumulative running total series."""
    total = 0.0
    result: list[float] = []
    for value in values:
        total += safe_float(value)
        result.append(round(total, 2))
    return result


def safe_pct(numerator: float, denominator: float) -> float:
    """Return a guarded percentage."""
    denom = safe_float(denominator)
    if denom == 0:
        return 0.0
    return round((safe_float(numerator) / denom) * 100.0, 2)


def least_squares(values: list[float]) -> dict[str, float]:
    """Compute a simple least-squares line and residual statistics."""
    n = len(values)
    if n < 2:
        return {
            "slope": 0.0,
            "intercept": values[0] if values else 0.0,
            "r_squared": 0.0,
            "residual_se": 0.0,
            "x_mean": 0.0,
            "ss_xx": 0.0,
            "n": float(n),
        }

    sum_x = n * (n - 1) / 2
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6
    sum_y = sum(values)
    sum_xy = sum(idx * value for idx, value in enumerate(values))
    denom = n * sum_x2 - sum_x * sum_x

    if denom == 0:
        return {
            "slope": 0.0,
            "intercept": sum_y / n,
            "r_squared": 0.0,
            "residual_se": 0.0,
            "x_mean": sum_x / n,
            "ss_xx": 0.0,
            "n": float(n),
        }

    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    y_mean = sum_y / n
    ss_tot = sum((value - y_mean) ** 2 for value in values)
    ss_res = sum((value - (intercept + slope * idx)) ** 2 for idx, value in enumerate(values))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0
    residual_se = math.sqrt(ss_res / (n - 2)) if n > 2 else 0.0
    x_mean = sum_x / n
    ss_xx = sum_x2 - n * x_mean * x_mean

    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "residual_se": residual_se,
        "x_mean": x_mean,
        "ss_xx": ss_xx,
        "n": float(n),
    }


def t_critical(df: int) -> float:
    """Approximate the two-sided 95% t critical value."""
    if df < 1:
        return 12.706
    if df >= 120:
        return 1.96

    table = {
        1: 12.706,
        2: 4.303,
        3: 3.182,
        4: 2.776,
        5: 2.571,
        6: 2.447,
        7: 2.365,
        8: 2.306,
        9: 2.262,
        10: 2.228,
        12: 2.179,
        15: 2.131,
        20: 2.086,
        25: 2.060,
        30: 2.042,
        40: 2.021,
        60: 2.000,
        80: 1.990,
        100: 1.984,
        120: 1.980,
    }
    keys = sorted(table)
    for index, key in enumerate(keys):
        if df <= key:
            if index == 0:
                return table[key]
            low = keys[index - 1]
            high = key
            return table[low] + (table[high] - table[low]) * (df - low) / (high - low)
    return 1.96


def prediction_interval(fit: dict[str, float], x_new: int) -> float:
    """Return a 95% prediction interval half-width."""
    n = int(fit.get("n", 0))
    residual_se = fit.get("residual_se", 0.0)
    if n < 3 or residual_se == 0:
        return 0.0

    df = n - 2
    t_value = t_critical(df)
    ss_xx = fit.get("ss_xx", 0.0)
    x_mean = fit.get("x_mean", 0.0)
    if ss_xx == 0:
        return t_value * residual_se

    return t_value * residual_se * math.sqrt(1 + 1 / n + (x_new - x_mean) ** 2 / ss_xx)


def mean_absolute_pct_error(actuals: list[float], forecasts: list[float]) -> float:
    """Return MAPE across comparable actual/forecast pairs."""
    errors: list[float] = []
    for actual, forecast in zip(actuals, forecasts, strict=False):
        actual_value = safe_float(actual)
        if actual_value <= 0:
            continue
        errors.append(abs((actual_value - safe_float(forecast)) / actual_value) * 100.0)
    if not errors:
        return 0.0
    return round(sum(errors) / len(errors), 2)


def forecast_bias_pct(actuals: list[float], forecasts: list[float]) -> float:
    """Return average signed forecast bias percentage."""
    biases: list[float] = []
    for actual, forecast in zip(actuals, forecasts, strict=False):
        actual_value = safe_float(actual)
        if actual_value <= 0:
            continue
        biases.append(((safe_float(forecast) - actual_value) / actual_value) * 100.0)
    if not biases:
        return 0.0
    return round(sum(biases) / len(biases), 2)
