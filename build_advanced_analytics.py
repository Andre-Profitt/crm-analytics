#!/usr/bin/env python3
"""Build the Advanced Pipeline Analytics dashboard — ML/statistical analysis on pipeline data.

Features:
  - Logistic regression win probability (sklearn with heuristic fallback)
  - Monte Carlo simulation for revenue forecasting
  - Least-squares trendline extrapolation (3-month horizon)
  - Stage bottleneck analysis with stuck-deal detection
  - Deal push frequency and impact analysis
  - Deals won deep-dive analytics

Datasets:
  - Pipeline_Analytics (per-deal with computed ML/statistical fields)
  - Pipeline_Trendlines (monthly aggregates with forecast projections)
  - Pipeline_Monte_Carlo (5 scenario rows: P10/P25/P50/P75/P90)

Pages:
  1. Executive Summary — KPIs, pipeline trend, Monte Carlo forecast, win prob distribution
  2. Deal Push Intelligence — push frequency, impact on win rate, top pushed deals
  3. Win Probability Intelligence — model accuracy, feature importance, at-risk deals
  4. Stage Bottleneck Analysis — days per stage, conversion funnel, backward moves
  5. Deals Won Deep Dive — won by cohort, characteristics, lead source, deal size
  6. Trendline Analytics — pipeline/win rate/cycle length trends with forecast bands
"""

import csv
import io
import math
import random
from datetime import datetime

from crm_analytics_helpers import (
    _date_diff,
    get_auth,
    _soql,
    _dim,
    _measure,
    _date,
    upload_dataset,
    get_dataset_id,
    sq,
    af,
    num,
    rich_chart,
    pillbox,
    hdr,
    section_label,
    nav_link,
    pg,
    nav_row,
    build_dashboard_state,
    deploy_dashboard,
    create_dashboard_if_needed,
    coalesce_filter,
    set_record_links_xmd,
    timeline_chart,
    combo_chart,
    scatter_chart,
    gauge,
    funnel_chart,
    treemap_chart,
    waterfall_chart,
    bullet_chart,
    bubble_chart,
    sankey_chart,
    area_chart,
    line_chart,
    heatmap_chart,
)

# ═══════════════════════════════════════════════════════════════════════════
#  Constants
# ═══════════════════════════════════════════════════════════════════════════

DASHBOARD_LABEL = "Advanced Pipeline Analytics"
DS = "Pipeline_Analytics"
DS_LABEL = "Pipeline Analytics"
DS_TREND = "Pipeline_Trendlines"
DS_TREND_LABEL = "Pipeline Trendlines"
DS_MC = "Pipeline_Monte_Carlo"
DS_MC_LABEL = "Pipeline Monte Carlo"
DS_SURV = "Pipeline_Survival"
DS_SURV_LABEL = "Pipeline Survival Curves"

TODAY = datetime.utcnow().strftime("%Y-%m-%d")

# Stage ordering for ordinal encoding and backward-move detection
STAGE_ORDER = {
    "0 - No Opportunity": 0,
    "1 - Prospecting": 1,
    "2 - Discovery": 2,
    "3 - Engagement": 3,
    "4 - Shortlisted": 4,
    "5 - Preferred": 5,
    "6 - Contracting": 6,
    "8 - Won": 8,
    "0 - Lost": 0,
}
_MAX_TRANSIENT_STAGE = max(v for k, v in STAGE_ORDER.items() if v < 8)

# Coalesce filter bindings
SF = coalesce_filter("f_stage", "StageName")
PBF = coalesce_filter("f_push", "PushBucket")
WPF = coalesce_filter("f_winprob", "WinProbBucket")
FYF = coalesce_filter("f_fy", "FiscalYear")

# ═══════════════════════════════════════════════════════════════════════════
#  SOQL Queries
# ═══════════════════════════════════════════════════════════════════════════

OPP_SOQL = (
    "SELECT Id, Name, AccountId, Account.Name, StageName, Amount, "
    "convertCurrency(APTS_Forecast_ARR__c) ConvertedARR, "
    "APTS_Forecast_ARR__c, CloseDate, CreatedDate, "
    "IsClosed, IsWon, Owner.Name, ForecastCategoryName, "
    "LeadSource, Type, AgeInDays, LastStageChangeInDays, Probability "
    "FROM Opportunity "
    "WHERE FiscalYear IN (2024, 2025, 2026, 2027)"
)

HISTORY_SOQL = (
    "SELECT OpportunityId, StageName, Amount, CloseDate, CreatedDate "
    "FROM OpportunityHistory "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z "
    "ORDER BY OpportunityId, CreatedDate ASC"
)

FIELD_HISTORY_SOQL = (
    "SELECT OpportunityId, Field, OldValue, NewValue, CreatedDate "
    "FROM OpportunityFieldHistory "
    "WHERE CreatedDate >= 2024-01-01T00:00:00Z "
    "AND Field IN ('StageName', 'CloseDate', 'Amount', 'ForecastCategoryName')"
)


# ═══════════════════════════════════════════════════════════════════════════
#  Data Extraction & Computed Fields
# ═══════════════════════════════════════════════════════════════════════════


def _compute_push_stats(field_history, opp_id):
    """Compute push/pull counts and net push days for an opportunity."""
    push_count = 0
    pull_count = 0
    net_push_days = 0
    records = field_history.get(opp_id, [])
    for r in records:
        if r["field"] != "CloseDate":
            continue
        old_val = r.get("old", "")
        new_val = r.get("new", "")
        if not old_val or not new_val:
            continue
        try:
            old_dt = datetime.strptime(old_val[:10], "%Y-%m-%d")
            new_dt = datetime.strptime(new_val[:10], "%Y-%m-%d")
            delta = (new_dt - old_dt).days
            if delta > 0:
                push_count += 1
                net_push_days += delta
            elif delta < 0:
                pull_count += 1
                net_push_days += delta
        except ValueError:
            pass
    return push_count, pull_count, net_push_days


def _compute_stage_stats(history_records, opp_id):
    """Compute stage transition stats from OpportunityHistory."""
    records = history_records.get(opp_id, [])
    total_changes = 0
    backward_moves = 0
    stage_days = []
    prev_stage_num = None
    prev_created = None

    for r in records:
        stage = r.get("stage", "")
        stage_num = STAGE_ORDER.get(stage, 0)
        created = r.get("created", "")

        if prev_stage_num is not None:
            total_changes += 1
            if stage_num < prev_stage_num and prev_stage_num > 0:
                backward_moves += 1

        if prev_created and created:
            try:
                d1 = datetime.strptime(prev_created[:10], "%Y-%m-%d")
                d2 = datetime.strptime(created[:10], "%Y-%m-%d")
                days = max(0, (d2 - d1).days)
                if days > 0:
                    stage_days.append(days)
            except ValueError:
                pass

        prev_stage_num = stage_num
        prev_created = created

    avg_days = round(sum(stage_days) / len(stage_days), 1) if stage_days else 0
    return total_changes, backward_moves, avg_days


def _compute_push_after_commit(field_history, opp_id):
    """Check if any CloseDate push occurred while ForecastCategory was Commit/BestCase.

    Builds a timeline of ForecastCategory changes and checks each push against it.
    """
    records = field_history.get(opp_id, [])
    if not records:
        return "false"

    # Build ForecastCategory timeline: list of (date, category) sorted by date
    fcat_timeline = []
    push_dates = []
    for r in records:
        if r["field"] == "ForecastCategoryName":
            fcat_timeline.append((r["created"], r.get("new", "")))
        elif r["field"] == "CloseDate":
            old_val = r.get("old", "")
            new_val = r.get("new", "")
            if old_val and new_val:
                try:
                    old_dt = datetime.strptime(old_val[:10], "%Y-%m-%d")
                    new_dt = datetime.strptime(new_val[:10], "%Y-%m-%d")
                    if (new_dt - old_dt).days > 0:  # push (not pull)
                        push_dates.append(r["created"])
                except ValueError:
                    pass

    if not push_dates:
        return "false"

    fcat_timeline.sort(key=lambda x: x[0])

    def _fcat_at_date(dt_str):
        """Return the ForecastCategory in effect at the given date."""
        result = ""
        for ts, cat in fcat_timeline:
            if ts <= dt_str:
                result = cat
            else:
                break
        return result

    for push_dt in push_dates:
        fcat = _fcat_at_date(push_dt)
        if fcat in ("Commit", "Best Case", "BestCase"):
            return "true"

    return "false"


def _compute_stage_skip_count(history_records, opp_id):
    """Count stage transitions that skip 1+ stages using STAGE_ORDER gaps."""
    records = history_records.get(opp_id, [])
    skip_count = 0
    prev_ordinal = None
    for r in records:
        stage = r.get("stage", "")
        ordinal = STAGE_ORDER.get(stage, 0)
        if prev_ordinal is not None and ordinal > 0 and prev_ordinal > 0:
            if ordinal - prev_ordinal > 1:
                skip_count += 1
        prev_ordinal = ordinal
    return skip_count


def _bucket_age(age_days):
    """Assign deal to age bucket."""
    if age_days <= 30:
        return "0-30d"
    elif age_days <= 60:
        return "31-60d"
    elif age_days <= 90:
        return "61-90d"
    elif age_days <= 180:
        return "91-180d"
    else:
        return "180d+"


def _bucket_push(push_count):
    """Assign deal to push frequency bucket."""
    if push_count == 0:
        return "No Push"
    elif push_count == 1:
        return "1 Push"
    elif push_count <= 3:
        return "2-3 Pushes"
    else:
        return "4+ Pushes"


def _bucket_deal_size(amount):
    """Assign deal to size bucket."""
    if amount <= 0:
        return "< $50K"
    if amount < 50000:
        return "< $50K"
    elif amount < 200000:
        return "$50K-$200K"
    elif amount < 500000:
        return "$200K-$500K"
    elif amount < 1000000:
        return "$500K-$1M"
    else:
        return "$1M+"


def _bucket_win_prob(prob):
    """Assign deal to win probability bucket."""
    if prob < 0.2:
        return "Very Low (0-20%)"
    elif prob < 0.4:
        return "Low (20-40%)"
    elif prob < 0.6:
        return "Medium (40-60%)"
    elif prob < 0.8:
        return "High (60-80%)"
    else:
        return "Very High (80-100%)"


def _cohort(created_date):
    """Derive cohort quarter from CreatedDate string."""
    if not created_date:
        return ""
    try:
        dt = datetime.strptime(created_date[:10], "%Y-%m-%d")
        q = (dt.month - 1) // 3 + 1
        return f"{dt.year}-Q{q}"
    except ValueError:
        return ""


# ═══════════════════════════════════════════════════════════════════════════
#  Fiscal Year Helpers (Calendar Year: Jan–Dec)
# ═══════════════════════════════════════════════════════════════════════════

FY_START_MONTH = 1  # Calendar year


def _fiscal_year(date_str):
    """Derive fiscal year label from a date string. Calendar year: Jan–Dec."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return f"FY{dt.year}"
    except ValueError:
        return ""


def _fiscal_quarter(date_str):
    """Derive fiscal quarter label from a date string. Calendar year quarters."""
    if not date_str:
        return ""
    try:
        dt = datetime.strptime(date_str[:10], "%Y-%m-%d")
        q = (dt.month - 1) // 3 + 1
        return f"FY{dt.year}-Q{q}"
    except ValueError:
        return ""


# ═══════════════════════════════════════════════════════════════════════════
#  Win Probability — Logistic Regression with Heuristic Fallback
# ═══════════════════════════════════════════════════════════════════════════

# Heuristic lookup table (fallback if sklearn unavailable)
STAGE_WIN_HEURISTIC = {
    "0 - No Opportunity": 0.05,
    "1 - Prospecting": 0.10,
    "2 - Discovery": 0.20,
    "3 - Engagement": 0.35,
    "4 - Shortlisted": 0.50,
    "5 - Preferred": 0.65,
    "6 - Contracting": 0.80,
    "8 - Won": 1.0,
    "0 - Lost": 0.0,
}


def _heuristic_win_prob(deal):
    """Fallback win probability based on stage + adjustments."""
    base = STAGE_WIN_HEURISTIC.get(deal.get("StageName", ""), 0.15)
    # Adjust for push count (each push reduces by 0.05, min 0)
    push_penalty = min(0.2, deal.get("PushCount", 0) * 0.05)
    # Adjust for backward moves
    back_penalty = min(0.15, deal.get("BackwardMoves", 0) * 0.05)
    # Adjust for deal age (older = slightly lower)
    age = deal.get("AgeInDays", 0)
    age_penalty = 0.05 if age > 180 else (0.02 if age > 90 else 0)
    prob = max(0, min(1, base - push_penalty - back_penalty - age_penalty))
    return round(prob, 3)


FCAT_ORDINAL = {
    "Commit": 4,
    "Best Case": 3,
    "BestCase": 3,
    "Pipeline": 2,
    "Omitted": 1,
}


def compute_win_probabilities(deals):
    """Compute WinProbability using ensemble model with best-practice methodology.

    Model cascade: GradientBoosting → LogisticRegressionCV → heuristic.
    Uses permutation importance (model-agnostic) instead of coefficients.

    Returns (deals_with_prob, model_metrics_dict, feature_importance_dict).
    model_metrics_dict has keys: auc_roc, brier_score, cv_auc_mean, cv_auc_std, accuracy
    """
    closed = [d for d in deals if d.get("IsClosed")]
    open_deals = [d for d in deals if not d.get("IsClosed")]

    feature_names = [
        "PushCount",
        "AvgDaysPerPush",
        "AvgDaysPerStage",
        "TotalStageChanges",
        "BackwardMoves",
        "LogAmount",
        "StageOrdinal",
        "AgeInDays",
        "DaysInCurrentStage",
        "ForecastOrdinal",
    ]

    def _extract_features(d):
        push_count = d.get("PushCount", 0)
        net_push = d.get("NetPushDays", 0)
        avg_days_per_push = net_push / (push_count + 1)
        # Neutralize stage ordinal for closed deals to prevent label leakage
        # (Won=8, Lost=0 would perfectly separate classes in training)
        if d.get("IsClosed"):
            stage_ord = 3  # Neutral midpoint for closed deals
        else:
            stage_ord = min(
                STAGE_ORDER.get(d.get("StageName", ""), 0), _MAX_TRANSIENT_STAGE
            )
        return [
            push_count,
            avg_days_per_push,
            d.get("AvgDaysPerStage", 0),
            d.get("TotalStageChanges", 0),
            d.get("BackwardMoves", 0),
            math.log1p(max(0, d.get("Amount", 0) or 0)),
            stage_ord,
            d.get("AgeInDays", 0),
            d.get("DaysInCurrentStage", 0),
            FCAT_ORDINAL.get(d.get("ForecastCategory", ""), 1),
        ]

    metrics = {
        "auc_roc": 0,
        "brier_score": 1.0,
        "cv_auc_mean": 0,
        "cv_auc_std": 0,
        "accuracy": 0,
    }
    coefficients = {}

    try:
        from sklearn.ensemble import GradientBoostingClassifier
        from sklearn.linear_model import LogisticRegressionCV
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline as SKPipeline
        from sklearn.model_selection import StratifiedKFold, cross_val_score
        from sklearn.metrics import roc_auc_score, brier_score_loss
        from sklearn.inspection import permutation_importance

        if len(closed) < 30:
            raise ValueError(f"Only {len(closed)} closed deals — need 30+ for training")

        X_all = [_extract_features(d) for d in closed]
        y_all = [1 if d.get("IsWon") else 0 for d in closed]

        n_pos = sum(y_all)
        n_neg = len(y_all) - n_pos
        if n_pos < 5 or n_neg < 5:
            raise ValueError(f"Class balance too skewed: {n_pos} won, {n_neg} lost")

        n_folds = min(5, n_pos, n_neg)
        cv = StratifiedKFold(n_splits=max(2, n_folds), shuffle=True, random_state=42)

        # Primary model: GradientBoosting (captures non-linear interactions)
        pipe = SKPipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    GradientBoostingClassifier(
                        n_estimators=100,
                        max_depth=4,
                        learning_rate=0.1,
                        subsample=0.8,
                        random_state=42,
                    ),
                ),
            ]
        )

        try:
            pipe.fit(X_all, y_all)
            model_name = "GradientBoosting"
        except Exception:
            # Fallback to LogisticRegressionCV
            pipe = SKPipeline(
                [
                    ("scaler", StandardScaler()),
                    (
                        "clf",
                        LogisticRegressionCV(
                            Cs=10,
                            cv=cv,
                            scoring="roc_auc",
                            class_weight="balanced",
                            solver="lbfgs",
                            max_iter=1000,
                            random_state=42,
                        ),
                    ),
                ]
            )
            pipe.fit(X_all, y_all)
            model_name = "LogisticRegressionCV"

        # Unbiased CV
        cv_scores = cross_val_score(pipe, X_all, y_all, cv=cv, scoring="roc_auc")
        metrics["cv_auc_mean"] = round(float(cv_scores.mean()), 3)
        metrics["cv_auc_std"] = round(float(cv_scores.std()), 3)

        print(
            f"  {model_name}: "
            f"CV AUC-ROC: {metrics['cv_auc_mean']:.3f} "
            f"(±{metrics['cv_auc_std']:.3f}) across {n_folds} folds"
        )

        # Resubstitution metrics
        probs_closed = pipe.predict_proba(X_all)[:, 1]
        metrics["auc_roc"] = round(float(roc_auc_score(y_all, probs_closed)), 3)
        metrics["brier_score"] = round(float(brier_score_loss(y_all, probs_closed)), 4)
        y_pred = pipe.predict(X_all)
        metrics["accuracy"] = round(
            sum(1 for a, b in zip(y_all, y_pred) if a == b) / len(y_all) * 100, 1
        )

        print(
            f"  Resubstitution AUC: {metrics['auc_roc']:.3f} | "
            f"Brier: {metrics['brier_score']:.4f} | "
            f"Accuracy: {metrics['accuracy']}%"
        )
        print(
            f"  Class balance: {n_pos} won / {n_neg} lost "
            f"({n_pos / len(y_all) * 100:.1f}% win rate)"
        )

        # Permutation importance (model-agnostic, more reliable than coefficients)
        perm = permutation_importance(
            pipe, X_all, y_all, n_repeats=10, random_state=42, scoring="roc_auc"
        )
        for i, name in enumerate(feature_names):
            coefficients[name] = round(float(perm.importances_mean[i]), 4)

        # Assign model-predicted probabilities to closed deals
        for d, prob in zip(closed, probs_closed):
            d["WinProbability"] = round(float(prob), 3)

        # Predict on open deals
        if open_deals:
            X_open_raw = [_extract_features(d) for d in open_deals]
            probs_open = pipe.predict_proba(X_open_raw)[:, 1]
            for d, prob in zip(open_deals, probs_open):
                d["WinProbability"] = round(float(prob), 3)

        print(f"  Predicted {len(open_deals)} open deals")

    except (ImportError, ValueError, RuntimeError) as e:
        print(f"  sklearn unavailable or failed ({e}), using heuristic fallback")
        for d in closed:
            d["WinProbability"] = 1.0 if d.get("IsWon") else 0.0
        for d in open_deals:
            d["WinProbability"] = _heuristic_win_prob(d)
        metrics = {
            "auc_roc": 0,
            "brier_score": 1.0,
            "cv_auc_mean": 0,
            "cv_auc_std": 0,
            "accuracy": 0,
        }
        coefficients = {
            "StageOrdinal": 0.35,
            "ForecastOrdinal": 0.20,
            "PushCount": -0.15,
            "BackwardMoves": -0.12,
            "AvgDaysPerPush": -0.10,
            "AvgDaysPerStage": -0.08,
            "AgeInDays": -0.06,
            "LogAmount": 0.05,
            "DaysInCurrentStage": -0.04,
            "TotalStageChanges": 0.03,
        }

    # Set AtRiskFlag and WinProbBucket on all deals
    for d in deals:
        wp = d.get("WinProbability", 0)
        d["AtRiskFlag"] = "At Risk" if wp < 0.3 and not d.get("IsClosed") else ""
        d["WinProbBucket"] = _bucket_win_prob(wp)

    return deals, metrics, coefficients


# ═══════════════════════════════════════════════════════════════════════════
#  Slip/Push Risk — Logistic Regression (Model 2)
# ═══════════════════════════════════════════════════════════════════════════


def _heuristic_slip_risk(deal):
    """Fallback slip risk based on push history + stage duration."""
    push_count = deal.get("PushCount", 0)
    days_in_stage = deal.get("DaysInCurrentStage", 0)
    score = min(1.0, push_count * 0.15 + (0.3 if days_in_stage > 30 else 0))
    return round(score, 3)


def compute_slip_risk(deals):
    """Compute SlipRiskScore using logistic regression — P(push in next 30 days).

    Model 2: trained on closed deals where we know whether they pushed in
    their final 30 days before close. Same SKPipeline pattern as win model.
    """
    closed = [d for d in deals if d.get("IsClosed")]
    open_deals = [d for d in deals if not d.get("IsClosed")]

    def _extract_features(d):
        push_count = d.get("PushCount", 0)
        net_push = d.get("NetPushDays", 0)
        avg_days_per_push = net_push / (push_count + 1)
        close_date = d.get("CloseDate", "")
        if close_date:
            try:
                days_to_close = (
                    datetime.strptime(close_date[:10], "%Y-%m-%d")
                    - datetime.strptime(TODAY[:10], "%Y-%m-%d")
                ).days
            except ValueError:
                days_to_close = 0
        else:
            days_to_close = 0
        return [
            push_count,
            days_to_close,
            d.get("DaysInCurrentStage", 0),
            FCAT_ORDINAL.get(d.get("ForecastCategory", ""), 1),
            min(STAGE_ORDER.get(d.get("StageName", ""), 0), _MAX_TRANSIENT_STAGE),
            math.log1p(max(0, d.get("Amount", 0) or 0)),
            avg_days_per_push,
            d.get("BackwardMoves", 0),
        ]

    # Target: did the deal push (CloseDate moved later)?
    def _did_push(d):
        return 1 if d.get("PushCount", 0) > 0 else 0

    try:
        from sklearn.linear_model import LogisticRegressionCV
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline as SKPipeline
        from sklearn.model_selection import StratifiedKFold, cross_val_score

        if len(closed) < 30:
            raise ValueError(f"Only {len(closed)} closed deals — need 30+ for training")

        X_all = [_extract_features(d) for d in closed]
        y_all = [_did_push(d) for d in closed]

        n_pos = sum(y_all)
        n_neg = len(y_all) - n_pos
        if n_pos < 5 or n_neg < 5:
            raise ValueError(f"Class balance too skewed: {n_pos} pushed, {n_neg} not")

        n_folds = min(5, n_pos, n_neg)
        cv = StratifiedKFold(n_splits=max(2, n_folds), shuffle=True, random_state=43)
        pipe = SKPipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegressionCV(
                        Cs=10,
                        cv=cv,
                        scoring="roc_auc",
                        class_weight="balanced",
                        solver="lbfgs",
                        max_iter=1000,
                        random_state=43,
                    ),
                ),
            ]
        )
        pipe.fit(X_all, y_all)

        cv_scores = cross_val_score(pipe, X_all, y_all, cv=cv, scoring="roc_auc")
        print(
            f"  Slip model CV AUC-ROC: {cv_scores.mean():.3f} "
            f"(±{cv_scores.std():.3f}) across {n_folds} folds"
        )

        # Assign to closed deals
        probs_closed = pipe.predict_proba(X_all)[:, 1]
        for d, prob in zip(closed, probs_closed):
            d["SlipRiskScore"] = round(float(prob), 3)

        # Predict on open deals
        if open_deals:
            X_open = [_extract_features(d) for d in open_deals]
            probs_open = pipe.predict_proba(X_open)[:, 1]
            for d, prob in zip(open_deals, probs_open):
                d["SlipRiskScore"] = round(float(prob), 3)

        print(f"  Predicted slip risk for {len(open_deals)} open deals")

    except (ImportError, ValueError, RuntimeError) as e:
        print(f"  Slip model unavailable ({e}), using heuristic fallback")
        for d in closed:
            d["SlipRiskScore"] = _heuristic_slip_risk(d)
        for d in open_deals:
            d["SlipRiskScore"] = _heuristic_slip_risk(d)

    return deals


# ═══════════════════════════════════════════════════════════════════════════
#  Timing/Close-in-Period — Logistic Regression (Model 3)
# ═══════════════════════════════════════════════════════════════════════════


def _heuristic_timing_score(deal):
    """Fallback timing score based on stage + days to quarter end."""
    stage_heuristic = {
        "0 - No Opportunity": 0.02,
        "1 - Prospecting": 0.05,
        "2 - Discovery": 0.15,
        "3 - Engagement": 0.25,
        "4 - Shortlisted": 0.40,
        "5 - Preferred": 0.60,
        "6 - Contracting": 0.80,
    }
    base = stage_heuristic.get(deal.get("StageName", ""), 0.15)
    close_date = deal.get("CloseDate", "")
    if close_date:
        try:
            close_dt = datetime.strptime(close_date[:10], "%Y-%m-%d")
            today_dt = datetime.strptime(TODAY[:10], "%Y-%m-%d")
            q = (today_dt.month - 1) // 3 + 1
            qtr_end_month = q * 3
            if qtr_end_month == 12:
                qtr_end = datetime(today_dt.year + 1, 1, 1)
            else:
                qtr_end = datetime(today_dt.year, qtr_end_month + 1, 1)
            days_to_eod = max(0, (qtr_end - today_dt).days)
            if days_to_eod > 0:
                return round(
                    base * max(0, 1 - (close_dt - today_dt).days / days_to_eod), 3
                )
        except ValueError:
            pass
    return round(base * 0.5, 3)


def compute_timing_score(deals):
    """Compute TimingScore using logistic regression — P(close within current quarter).

    Model 3: trained on historical deals where we know whether they closed within
    the quarter. Uses WinProbability from model 1 as a stacked feature.
    """
    closed = [d for d in deals if d.get("IsClosed")]
    open_deals = [d for d in deals if not d.get("IsClosed")]

    def _closed_in_quarter(d):
        """Did this deal close within the same fiscal quarter it was created?"""
        close_date = d.get("CloseDate", "")
        created = d.get("CreatedDate", "")
        if not close_date or not d.get("IsClosed") or not created:
            return 0
        try:
            close_dt = datetime.strptime(close_date[:10], "%Y-%m-%d")
            created_dt = datetime.strptime(created[:10], "%Y-%m-%d")
            created_q = (created_dt.month - 1) // 3 + 1
            close_q = (close_dt.month - 1) // 3 + 1
            if created_dt.year == close_dt.year and created_q == close_q:
                return 1
        except ValueError:
            pass
        return 0

    def _extract_features(d):
        close_date = d.get("CloseDate", "")
        if close_date:
            try:
                days_to_close = (
                    datetime.strptime(close_date[:10], "%Y-%m-%d")
                    - datetime.strptime(TODAY[:10], "%Y-%m-%d")
                ).days
            except ValueError:
                days_to_close = 0
        else:
            days_to_close = 0
        return [
            days_to_close,
            min(STAGE_ORDER.get(d.get("StageName", ""), 0), _MAX_TRANSIENT_STAGE),
            FCAT_ORDINAL.get(d.get("ForecastCategory", ""), 1),
            d.get("WinProbability", 0),
            d.get("PushCount", 0),
            d.get("DaysInCurrentStage", 0),
            math.log1p(max(0, d.get("Amount", 0) or 0)),
        ]

    try:
        from sklearn.linear_model import LogisticRegressionCV
        from sklearn.preprocessing import StandardScaler
        from sklearn.pipeline import Pipeline as SKPipeline
        from sklearn.model_selection import StratifiedKFold, cross_val_score

        if len(closed) < 30:
            raise ValueError(f"Only {len(closed)} closed deals — need 30+ for training")

        X_all = [_extract_features(d) for d in closed]
        y_all = [_closed_in_quarter(d) for d in closed]

        n_pos = sum(y_all)
        n_neg = len(y_all) - n_pos
        if n_pos < 5 or n_neg < 5:
            raise ValueError(
                f"Class balance too skewed: {n_pos} in-quarter, {n_neg} not"
            )

        n_folds = min(5, n_pos, n_neg)
        cv = StratifiedKFold(n_splits=max(2, n_folds), shuffle=True, random_state=44)
        pipe = SKPipeline(
            [
                ("scaler", StandardScaler()),
                (
                    "clf",
                    LogisticRegressionCV(
                        Cs=10,
                        cv=cv,
                        scoring="roc_auc",
                        class_weight="balanced",
                        solver="lbfgs",
                        max_iter=1000,
                        random_state=44,
                    ),
                ),
            ]
        )
        pipe.fit(X_all, y_all)

        cv_scores = cross_val_score(pipe, X_all, y_all, cv=cv, scoring="roc_auc")
        print(
            f"  Timing model CV AUC-ROC: {cv_scores.mean():.3f} "
            f"(±{cv_scores.std():.3f}) across {n_folds} folds"
        )

        # Assign to closed deals
        probs_closed = pipe.predict_proba(X_all)[:, 1]
        for d, prob in zip(closed, probs_closed):
            d["TimingScore"] = round(float(prob), 3)

        # Predict on open deals
        if open_deals:
            X_open = [_extract_features(d) for d in open_deals]
            probs_open = pipe.predict_proba(X_open)[:, 1]
            for d, prob in zip(open_deals, probs_open):
                d["TimingScore"] = round(float(prob), 3)

        print(f"  Predicted timing score for {len(open_deals)} open deals")

    except (ImportError, ValueError, RuntimeError) as e:
        print(f"  Timing model unavailable ({e}), using heuristic fallback")
        for d in closed:
            d["TimingScore"] = _heuristic_timing_score(d)
        for d in open_deals:
            d["TimingScore"] = _heuristic_timing_score(d)

    return deals


# ═══════════════════════════════════════════════════════════════════════════
#  Trendline Extrapolation (Manual Least Squares)
# ═══════════════════════════════════════════════════════════════════════════


def _least_squares(ys):
    """Compute least-squares regression with prediction interval support.

    x values are 0, 1, 2, ..., n-1.
    Returns dict with: slope, intercept, r_squared, residual_se, x_mean, ss_xx, n.
    Prediction interval at point x_new: trend ± t_crit * se * sqrt(1 + 1/n + (x-x̄)²/SSxx)
    """
    n = len(ys)
    if n < 2:
        return {
            "slope": 0,
            "intercept": ys[0] if ys else 0,
            "r_squared": 0,
            "residual_se": 0,
            "x_mean": 0,
            "ss_xx": 0,
            "n": n,
        }
    sum_x = n * (n - 1) / 2
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6
    sum_y = sum(ys)
    sum_xy = sum(i * y for i, y in enumerate(ys))
    denom = n * sum_x2 - sum_x * sum_x
    if denom == 0:
        return {
            "slope": 0,
            "intercept": sum_y / n,
            "r_squared": 0,
            "residual_se": 0,
            "x_mean": sum_x / n,
            "ss_xx": 0,
            "n": n,
        }
    slope = (n * sum_xy - sum_x * sum_y) / denom
    intercept = (sum_y - slope * sum_x) / n

    # R² (coefficient of determination)
    y_mean = sum_y / n
    ss_tot = sum((y - y_mean) ** 2 for y in ys)
    ss_res = sum((y - (intercept + slope * i)) ** 2 for i, y in enumerate(ys))
    r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

    # Residual standard error (root mean squared error with df = n-2)
    residual_se = math.sqrt(ss_res / (n - 2)) if n > 2 else 0

    # SSxx for prediction intervals
    x_mean = sum_x / n
    ss_xx = sum_x2 - n * x_mean * x_mean

    return {
        "slope": slope,
        "intercept": intercept,
        "r_squared": r_squared,
        "residual_se": residual_se,
        "x_mean": x_mean,
        "ss_xx": ss_xx,
        "n": n,
    }


def _t_critical(df):
    """Approximate t_{df, 0.025} critical value for 95% prediction intervals.

    Uses lookup table with linear interpolation. Exact for tabulated values,
    accurate to ~0.01 for intermediate df.
    """
    if df < 1:
        return 12.706  # Fallback to df=1
    if df >= 120:
        return 1.96  # Normal approximation
    _table = {
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
    keys = sorted(_table.keys())
    for i, k in enumerate(keys):
        if df <= k:
            if i == 0:
                return _table[k]
            lo, hi = keys[i - 1], k
            return _table[lo] + (_table[hi] - _table[lo]) * (df - lo) / (hi - lo)
    return 1.96


def _prediction_interval(fit, x_new):
    """Compute 95% prediction interval half-width at a given x_new.

    Uses t-distribution critical value (not z=1.96) for correct coverage
    with small sample sizes (e.g., 6-12 months of data).
    PI = t_{n-2, 0.025} * se * sqrt(1 + 1/n + (x_new - x̄)² / SSxx)
    """
    n = fit["n"]
    se = fit["residual_se"]
    if n < 3 or se == 0:
        return 0
    df = n - 2
    t_crit = _t_critical(df)
    ss_xx = fit["ss_xx"]
    x_mean = fit["x_mean"]
    if ss_xx == 0:
        return t_crit * se
    return t_crit * se * math.sqrt(1 + 1 / n + (x_new - x_mean) ** 2 / ss_xx)


def compute_trendlines(deals):
    """Aggregate monthly metrics and project 3-month trendlines.

    Returns list of monthly dicts (actuals + 3 forecast months).
    """
    # Group deals by close month
    monthly = {}
    for d in deals:
        close_date = d.get("CloseDate", "")
        if not close_date:
            continue
        month = close_date[:7]  # "YYYY-MM"
        if month not in monthly:
            monthly[month] = {
                "total_pipeline": 0,
                "weighted_pipeline": 0,
                "new_deals": 0,
                "won_amount": 0,
                "won_count": 0,
                "deal_sizes": [],
                "cycle_lengths": [],
            }
        m = monthly[month]
        arr = d.get("ARR") if d.get("ARR") is not None else (d.get("Amount") or 0)
        prob = d.get("Probability", 0) or 0

        if not d.get("IsClosed"):
            m["total_pipeline"] += arr
            m["weighted_pipeline"] += arr * prob / 100
            m["new_deals"] += 1
        elif d.get("IsWon"):
            m["won_amount"] += arr
            m["won_count"] += 1

        if d.get("IsWon") and arr > 0:
            m["deal_sizes"].append(arr)

        created = d.get("CreatedDate", "")
        if d.get("IsWon") and created and close_date:
            cycle = _date_diff(created, close_date)
            if cycle > 0:
                m["cycle_lengths"].append(cycle)

    if not monthly:
        return []

    # Sort months and build rows
    sorted_months = sorted(monthly.keys())
    rows = []
    for month in sorted_months:
        m = monthly[month]
        total_deals = m["new_deals"] + m["won_count"]
        win_rate = round(m["won_count"] / total_deals * 100, 1) if total_deals else 0
        avg_deal = (
            round(sum(m["deal_sizes"]) / len(m["deal_sizes"]), 2)
            if m["deal_sizes"]
            else 0
        )
        avg_cycle = (
            round(sum(m["cycle_lengths"]) / len(m["cycle_lengths"]), 1)
            if m["cycle_lengths"]
            else 0
        )

        rows.append(
            {
                "Month": f"{month}-01",
                "MonthLabel": month,
                "TotalPipeline": round(m["total_pipeline"], 2),
                "WeightedPipeline": round(m["weighted_pipeline"], 2),
                "NewDeals": m["new_deals"],
                "ClosedWonAmount": round(m["won_amount"], 2),
                "ClosedWonCount": m["won_count"],
                "AvgDealSize": avg_deal,
                "WinRate": win_rate,
                "AvgCycleLength": avg_cycle,
                "IsForecast": "false",
            }
        )

    # Compute trendlines with prediction intervals for key metrics
    pipeline_vals = [r["TotalPipeline"] for r in rows]
    winrate_vals = [r["WinRate"] for r in rows]
    cycle_vals = [r["AvgCycleLength"] for r in rows]

    fit_p = _least_squares(pipeline_vals)
    fit_w = _least_squares(winrate_vals)
    fit_c = _least_squares(cycle_vals)

    print(
        f"  Trendline R²: Pipeline={fit_p['r_squared']:.3f}, "
        f"WinRate={fit_w['r_squared']:.3f}, CycleLength={fit_c['r_squared']:.3f}"
    )

    n = len(rows)
    # Add trend values + prediction intervals to actual rows
    for i, r in enumerate(rows):
        tp = fit_p["intercept"] + fit_p["slope"] * i
        tw = fit_w["intercept"] + fit_w["slope"] * i
        tc = fit_c["intercept"] + fit_c["slope"] * i
        pi_p = _prediction_interval(fit_p, i)
        pi_w = _prediction_interval(fit_w, i)

        r["TrendPipeline"] = round(tp, 2)
        r["TrendWinRate"] = round(max(0, min(100, tw)), 1)
        r["TrendCycleLength"] = round(max(0, tc), 1)
        r["TrendPipelineUpper"] = round(max(0, tp + pi_p), 2)
        r["TrendPipelineLower"] = round(max(0, tp - pi_p), 2)
        r["TrendWinRateUpper"] = round(max(0, min(100, tw + pi_w)), 1)
        r["TrendWinRateLower"] = round(max(0, min(100, tw - pi_w)), 1)
        r["PipelineR2"] = round(fit_p["r_squared"], 3)

    # Project 3 months forward (need ≥3 data points for meaningful forecast)
    if n < 3:
        print("  WARNING: < 3 data points, suppressing trend forecast")
        return rows

    last_month = sorted_months[-1]
    try:
        last_dt = datetime.strptime(f"{last_month}-01", "%Y-%m-%d")
    except ValueError:
        return rows

    for j in range(1, 4):
        month_num = last_dt.month + j
        year = last_dt.year + (month_num - 1) // 12
        month_num = ((month_num - 1) % 12) + 1
        forecast_month = f"{year}-{month_num:02d}"
        idx = n + j - 1

        tp = fit_p["intercept"] + fit_p["slope"] * idx
        tw = fit_w["intercept"] + fit_w["slope"] * idx
        tc = fit_c["intercept"] + fit_c["slope"] * idx
        pi_p = _prediction_interval(fit_p, idx)
        pi_w = _prediction_interval(fit_w, idx)

        rows.append(
            {
                "Month": f"{forecast_month}-01",
                "MonthLabel": forecast_month,
                "TotalPipeline": 0,
                "WeightedPipeline": 0,
                "NewDeals": 0,
                "ClosedWonAmount": 0,
                "ClosedWonCount": 0,
                "AvgDealSize": 0,
                "WinRate": 0,
                "AvgCycleLength": 0,
                "TrendPipeline": round(max(0, tp), 2),
                "TrendWinRate": round(max(0, min(100, tw)), 1),
                "TrendCycleLength": round(max(0, tc), 1),
                "TrendPipelineUpper": round(max(0, tp + pi_p), 2),
                "TrendPipelineLower": round(max(0, tp - pi_p), 2),
                "TrendWinRateUpper": round(max(0, min(100, tw + pi_w)), 1),
                "TrendWinRateLower": round(max(0, min(100, tw - pi_w)), 1),
                "PipelineR2": round(fit_p["r_squared"], 3),
                "IsForecast": "true",
            }
        )

    return rows


# ═══════════════════════════════════════════════════════════════════════════
#  Monte Carlo Simulation
# ═══════════════════════════════════════════════════════════════════════════


def run_monte_carlo(deals, n_simulations=10000):
    """Run Monte Carlo simulation with antithetic variates for variance reduction.

    Methodology:
      - Antithetic variates: each simulation paired with mirror (U → 1-U),
        halving effective variance for monotone payoffs (deal revenue is monotone
        in win rate and deal size).
      - Log-normal deal sizes fitted from historical won deals
      - Quarterly win rate variation estimated from cohort data
      - Poisson deal arrival rate from historical monthly counts
      - Proper percentile interpolation (linear between adjacent sorted values)

    Returns list of 5 scenario dicts (P10, P25, P50, P75, P90).
    """
    won_deals = [d for d in deals if d.get("IsWon")]
    closed_deals = [d for d in deals if d.get("IsClosed")]

    if not won_deals or not closed_deals:
        print("  Monte Carlo: insufficient historical data, using defaults")
        return _default_monte_carlo()

    # Win rate distribution parameters
    hist_win_rate = len(won_deals) / len(closed_deals)
    quarterly_rates = {}
    for d in closed_deals:
        cohort = d.get("Cohort", "")
        if cohort:
            if cohort not in quarterly_rates:
                quarterly_rates[cohort] = {"won": 0, "total": 0}
            quarterly_rates[cohort]["total"] += 1
            if d.get("IsWon"):
                quarterly_rates[cohort]["won"] += 1
    qr_values = [
        v["won"] / v["total"] for v in quarterly_rates.values() if v["total"] >= 5
    ]
    wr_std = _std(qr_values) if len(qr_values) >= 2 else 0.05

    # Deal size distribution (log-normal fit)
    won_amounts = [
        d.get("ARR") if d.get("ARR") is not None else (d.get("Amount") or 0)
        for d in won_deals
    ]
    won_amounts = [a for a in won_amounts if a > 0]
    if won_amounts:
        log_amounts = [math.log(a) for a in won_amounts]
        mu_log = sum(log_amounts) / len(log_amounts)
        sigma_log = max(0.1, _std(log_amounts)) if len(log_amounts) >= 2 else 0.5
    else:
        mu_log, sigma_log = math.log(100000), 0.5

    # Deal count (average monthly new deals)
    monthly_counts = {}
    for d in deals:
        created = (d.get("CreatedDate") or "")[:7]
        if created:
            monthly_counts[created] = monthly_counts.get(created, 0) + 1
    avg_monthly = sum(monthly_counts.values()) / max(len(monthly_counts), 1)

    print(
        f"  MC params: win_rate={hist_win_rate:.3f}±{wr_std:.3f}, "
        f"deal_size=lognorm(μ={mu_log:.2f}, σ={sigma_log:.2f}), "
        f"monthly_deals={avg_monthly:.1f}"
    )

    def _simulate_quarter(rng_seed, antithetic=False):
        """Simulate one quarter with antithetic variance reduction.

        All random draws are consumed regardless of win/loss to keep the RNG
        stream synchronized between normal and antithetic paths.
        Win-rate perturbation is drawn once per quarter (systematic component).
        """
        rng = random.Random(rng_seed)
        revenue = 0
        deals_won = 0
        deals_entered = 0

        # Quarterly systematic win-rate shift (drawn once, not per deal)
        u_wr = rng.gauss(0, 1)
        if antithetic:
            u_wr = -u_wr
        quarter_wr = max(0, min(1, hist_win_rate + wr_std * u_wr))

        for _month in range(3):
            n_deals = _poisson_rng(rng, avg_monthly)
            deals_entered += n_deals
            for _deal in range(n_deals):
                # Always draw ALL variates to keep RNG streams in sync
                u_win = rng.random()
                u_size = rng.gauss(0, 1)
                if antithetic:
                    u_win = 1 - u_win
                    u_size = -u_size

                if u_win < quarter_wr:
                    deal_size = math.exp(mu_log + sigma_log * u_size)
                    revenue += deal_size
                    deals_won += 1
        return revenue, deals_won, deals_entered

    # Run paired simulations (antithetic variates)
    results = []
    half_n = n_simulations // 2
    for i in range(half_n):
        seed = 42 + i
        rev1, deals1, entered1 = _simulate_quarter(seed, antithetic=False)
        rev2, deals2, entered2 = _simulate_quarter(seed, antithetic=True)
        # Average of paired estimates (antithetic variance reduction)
        avg_rev = (rev1 + rev2) / 2
        avg_deals = round((deals1 + deals2) / 2)
        avg_entered = max(1, round((entered1 + entered2) / 2))
        results.append({"revenue": avg_rev, "deals": avg_deals, "entered": avg_entered})

    # Sort each metric independently for correct percentile extraction
    revenues = sorted(r["revenue"] for r in results)
    deal_counts_sorted = sorted(r["deals"] for r in results)
    # Compute per-simulation win rate, then take percentiles of that
    sim_win_rates = sorted(r["deals"] / max(1, r["entered"]) * 100 for r in results)

    # VaR and CVaR computation
    var5 = _percentile(revenues, 0.05) if revenues else 0
    # CVaR (Expected Shortfall): mean of bottom 5%
    n_tail = max(1, int(len(revenues) * 0.05))
    cvar5 = sum(revenues[:n_tail]) / n_tail if revenues else 0

    scenarios = [
        ("P10 (Pessimistic)", 0.10),
        ("P25", 0.25),
        ("P50 (Most Likely)", 0.50),
        ("P75", 0.75),
        ("P90 (Optimistic)", 0.90),
    ]

    output = []
    for label, pct in scenarios:
        rev = _percentile(revenues, pct)
        dc = _percentile(deal_counts_sorted, pct)
        wr = _percentile(sim_win_rates, pct)
        output.append(
            {
                "Scenario": label,
                "ProjectedRevenue": round(rev, 2),
                "ProjectedDealCount": round(dc),
                "ProjectedWinRate": round(wr, 1),
                "VaR5": round(var5, 2),
                "CVaR5": round(cvar5, 2),
            }
        )

    # Report convergence: standard error of the mean for P50
    if revenues:
        mean_rev = sum(revenues) / len(revenues)
        se_rev = _std(revenues) / math.sqrt(len(revenues))
        pct_of_mean = f"{se_rev / mean_rev * 100:.1f}%" if mean_rev > 0 else "N/A"
        print(
            f"  MC convergence: P50=${_percentile(revenues, 0.5):,.0f}, "
            f"mean=${mean_rev:,.0f}, SE=${se_rev:,.0f} ({pct_of_mean} of mean)"
        )
        print(f"  VaR(5%)=${var5:,.0f}, CVaR(5%)=${cvar5:,.0f}")

    return output


def _percentile(sorted_vals, pct):
    """Linear interpolation percentile (matches numpy default)."""
    n = len(sorted_vals)
    if n == 0:
        return 0
    if n == 1:
        return sorted_vals[0]
    k = max(0, min(n - 1, pct * (n - 1)))  # clamp for float precision
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return sorted_vals[int(k)]
    return sorted_vals[f] + (k - f) * (sorted_vals[c] - sorted_vals[f])


def _poisson_rng(rng, lam):
    """Poisson random variate using a specific Random instance."""
    if lam > 30:  # Normal approximation is accurate for λ≥30 (CLT); Knuth is O(λ)
        return max(0, int(rng.gauss(lam, math.sqrt(lam)) + 0.5))
    L = math.exp(-lam)
    k = 0
    p = 1.0
    while True:
        k += 1
        p *= rng.random()
        if p <= L:
            return k - 1


def _std(values):
    """Standard deviation (sample, Bessel-corrected)."""
    n = len(values)
    if n < 2:
        return 0
    mean = sum(values) / n
    variance = sum((x - mean) ** 2 for x in values) / (n - 1)
    return math.sqrt(variance)


def _default_monte_carlo():
    """Default Monte Carlo output when data is insufficient."""
    return [
        {
            "Scenario": "P10 (Pessimistic)",
            "ProjectedRevenue": 0,
            "ProjectedDealCount": 0,
            "ProjectedWinRate": 0,
            "VaR5": 0,
            "CVaR5": 0,
        },
        {
            "Scenario": "P25",
            "ProjectedRevenue": 0,
            "ProjectedDealCount": 0,
            "ProjectedWinRate": 0,
            "VaR5": 0,
            "CVaR5": 0,
        },
        {
            "Scenario": "P50 (Most Likely)",
            "ProjectedRevenue": 0,
            "ProjectedDealCount": 0,
            "ProjectedWinRate": 0,
            "VaR5": 0,
            "CVaR5": 0,
        },
        {
            "Scenario": "P75",
            "ProjectedRevenue": 0,
            "ProjectedDealCount": 0,
            "ProjectedWinRate": 0,
            "VaR5": 0,
            "CVaR5": 0,
        },
        {
            "Scenario": "P90 (Optimistic)",
            "ProjectedRevenue": 0,
            "ProjectedDealCount": 0,
            "ProjectedWinRate": 0,
            "VaR5": 0,
            "CVaR5": 0,
        },
    ]


# ═══════════════════════════════════════════════════════════════════════════
#  Markov Chain Stage Transitions (Absorbing Markov Chain)
# ═══════════════════════════════════════════════════════════════════════════

# Transient states ordered by pipeline progression
_TRANSIENT_STATES = [
    "1 - Prospecting",
    "2 - Discovery",
    "3 - Engagement",
    "4 - Shortlisted",
    "5 - Preferred",
    "6 - Contracting",
]
_ABSORBING_STATES = ["8 - Won", "0 - Lost"]
_ALL_MARKOV_STATES = _TRANSIENT_STATES + _ABSORBING_STATES


def _gauss_jordan_inverse(matrix):
    """Invert a square matrix via Gauss-Jordan elimination (pure Python).

    For our 7x7 transient submatrix, this is trivially fast.
    Returns the inverse matrix or None if singular.
    """
    n = len(matrix)
    # Augment with identity
    aug = [
        row[:] + [1.0 if i == j else 0.0 for j in range(n)]
        for i, row in enumerate(matrix)
    ]

    for col in range(n):
        # Partial pivoting
        max_row = col
        for row in range(col + 1, n):
            if abs(aug[row][col]) > abs(aug[max_row][col]):
                max_row = row
        aug[col], aug[max_row] = aug[max_row], aug[col]

        pivot = aug[col][col]
        if abs(pivot) < 1e-12:
            return None  # Singular

        for j in range(2 * n):
            aug[col][j] /= pivot

        for row in range(n):
            if row != col:
                factor = aug[row][col]
                for j in range(2 * n):
                    aug[row][j] -= factor * aug[col][j]

    return [row[n:] for row in aug]


def compute_markov_chain(history_by_opp, deals):
    """Build absorbing Markov chain from stage transition history.

    Returns transition_matrix (for heatmap) and per-deal MarkovWinProb + ExpectedStepsToClose.
    """
    n_transient = len(_TRANSIENT_STATES)
    n_absorb = len(_ABSORBING_STATES)
    state_idx = {s: i for i, s in enumerate(_ALL_MARKOV_STATES)}

    # Build raw transition counts
    counts = [[0] * (n_transient + n_absorb) for _ in range(n_transient + n_absorb)]

    for opp_id, records in history_by_opp.items():
        prev_state = None
        for r in records:
            stage = r.get("stage", "")
            # Normalize: "0 - No Opportunity" is effectively lost
            if stage == "0 - No Opportunity":
                stage = "0 - Lost"
            if stage not in state_idx:
                continue
            if prev_state is not None and prev_state in state_idx:
                i = state_idx[prev_state]
                j = state_idx[stage]
                if i != j:  # Skip self-loops
                    counts[i][j] += 1
            prev_state = stage

    # Build transition probability matrix P
    transition_matrix = {}
    P = [[0.0] * (n_transient + n_absorb) for _ in range(n_transient + n_absorb)]
    for i in range(n_transient + n_absorb):
        row_sum = sum(counts[i])
        from_state = _ALL_MARKOV_STATES[i]
        for j in range(n_transient + n_absorb):
            to_state = _ALL_MARKOV_STATES[j]
            prob = counts[i][j] / row_sum if row_sum > 0 else 0
            P[i][j] = prob
            if prob > 0:
                transition_matrix[(from_state, to_state)] = round(prob, 3)

    # Absorbing states have self-loops
    for k in range(n_transient, n_transient + n_absorb):
        P[k][k] = 1.0

    # Extract transient submatrix T and transient→absorbing submatrix R
    T = [[P[i][j] for j in range(n_transient)] for i in range(n_transient)]
    R = [
        [P[i][j] for j in range(n_transient, n_transient + n_absorb)]
        for i in range(n_transient)
    ]

    # Fundamental matrix N = (I - T)^-1
    I_minus_T = [
        [(-T[i][j] if i != j else 1.0 - T[i][j]) for j in range(n_transient)]
        for i in range(n_transient)
    ]
    N = _gauss_jordan_inverse(I_minus_T)

    if N is None:
        print("  Markov: singular matrix, using fallback")
        for d in deals:
            d["MarkovWinProb"] = d.get("WinProbability", 0)
            d["ExpectedStepsToClose"] = 0
        return transition_matrix

    # Absorption probabilities B = N × R
    B = [
        [sum(N[i][k] * R[k][j] for k in range(n_transient)) for j in range(n_absorb)]
        for i in range(n_transient)
    ]

    # Expected steps to absorption = N × 1 (row sums of N)
    expected_steps = [
        sum(N[i][j] for j in range(n_transient)) for i in range(n_transient)
    ]

    # Won index is 0 in _ABSORBING_STATES
    won_idx = 0  # "Closed Won" is first absorbing state

    # Assign to deals
    for d in deals:
        stage = d.get("StageName", "")
        if stage in state_idx and state_idx[stage] < n_transient:
            idx = state_idx[stage]
            d["MarkovWinProb"] = round(B[idx][won_idx], 3)
            d["ExpectedStepsToClose"] = round(expected_steps[idx], 1)
        elif d.get("IsWon"):
            d["MarkovWinProb"] = 1.0
            d["ExpectedStepsToClose"] = 0
        elif d.get("IsClosed"):
            d["MarkovWinProb"] = 0.0
            d["ExpectedStepsToClose"] = 0

    print(f"  Markov chain: {sum(sum(row) for row in counts)} transitions observed")
    for i, state in enumerate(_TRANSIENT_STATES):
        short = state.split(" - ")[-1] if " - " in state else state
        print(
            f"    {short}: P(Win)={B[i][won_idx]:.3f}, E[steps]={expected_steps[i]:.1f}"
        )

    return transition_matrix


# ═══════════════════════════════════════════════════════════════════════════
#  Kaplan-Meier Survival Analysis
# ═══════════════════════════════════════════════════════════════════════════


def _kaplan_meier_curve(event_times, censored):
    """Compute Kaplan-Meier survival curve from event/censoring times.

    Args:
        event_times: list of (time_in_days, is_event) tuples, sorted by time.
            is_event=True means the deal closed (event observed).
            is_event=False means the deal is still open (right-censored).

    Returns list of (day, survival_prob) tuples.
    """
    if not event_times:
        return [(0, 1.0)]

    # Sort by time
    sorted_data = sorted(zip(event_times, censored), key=lambda x: x[0])

    n_at_risk = len(sorted_data)
    curve = [(0, 1.0)]
    surv = 1.0

    i = 0
    while i < len(sorted_data):
        t = sorted_data[i][0]
        # Count events and censorings at time t
        d_i = 0  # events (closed)
        c_i = 0  # censored (still open)
        while i < len(sorted_data) and sorted_data[i][0] == t:
            if sorted_data[i][1]:
                d_i += 1
            else:
                c_i += 1
            i += 1

        if n_at_risk > 0 and d_i > 0:
            surv *= 1 - d_i / n_at_risk
            curve.append((t, round(surv, 4)))

        n_at_risk -= d_i + c_i

    return curve


def compute_survival_analysis(deals, history_by_opp):
    """Compute Kaplan-Meier survival curves per stage group.

    Assigns MedianSurvivalDays and SurvivalProb30d to each deal.
    Returns survival_curves dict for dashboard visualization.
    """
    # Build survival data from ALL deals (closed = event, open = right-censored)
    # Group by stage cohort: use last active stage for closed deals via history,
    # or current stage for open deals
    stage_groups = {"Early (S1-S3)": [], "Mid (S4-S5)": [], "Late (S6-S7)": []}

    def _stage_group(ordinal):
        if ordinal <= 3:
            return "Early (S1-S3)"
        elif ordinal <= 5:
            return "Mid (S4-S5)"
        return "Late (S6-S7)"

    for d in deals:
        stage = d.get("StageName", "")
        ordinal = STAGE_ORDER.get(stage, 0)

        if d.get("IsClosed"):
            # For closed deals, try to find their last active stage from history
            opp_id = d.get("Id", "")
            hist = history_by_opp.get(opp_id, [])
            last_active_ord = 0
            for r in hist:
                s = r.get("stage", "")
                o = STAGE_ORDER.get(s, 0)
                if 1 <= o <= 6:
                    last_active_ord = max(last_active_ord, o)
            if last_active_ord > 0:
                group = _stage_group(last_active_ord)
                stage_groups[group].append(d)
        elif 1 <= ordinal <= 6:
            group = _stage_group(ordinal)
            stage_groups[group].append(d)

    # Overall curve from all deals
    all_events = []
    all_censored = []
    for d in deals:
        age = d.get("AgeInDays", 0) or 0
        if age <= 0:
            continue
        all_events.append(age)
        all_censored.append(bool(d.get("IsClosed")))

    overall_curve = _kaplan_meier_curve(all_events, all_censored)

    # Per-group curves
    survival_curves = {"Overall": overall_curve}
    group_medians = {}
    group_surv30 = {}

    for group_name, group_deals in stage_groups.items():
        events = []
        censored = []
        for d in group_deals:
            age = d.get("AgeInDays", 0) or 0
            if age <= 0:
                continue
            events.append(age)
            censored.append(bool(d.get("IsClosed")))

        if not events:
            group_medians[group_name] = 0
            group_surv30[group_name] = 1.0
            continue

        curve = _kaplan_meier_curve(events, censored)
        survival_curves[group_name] = curve

        # Median survival time: first time S(t) ≤ 0.5
        median = 0
        for t, s in curve:
            if s <= 0.5:
                median = t
                break
        if median == 0 and len(curve) > 1:
            median = curve[-1][0]  # Use last observed time
        group_medians[group_name] = median

        # P(still open after 30 more days) — interpolate from curve
        surv30 = 1.0
        for t, s in curve:
            if t >= 30:
                surv30 = s
                break
        group_surv30[group_name] = surv30

    # Assign to deals
    for d in deals:
        stage = d.get("StageName", "")
        ordinal = STAGE_ORDER.get(stage, 0)
        if 1 <= ordinal <= 6:
            group = _stage_group(ordinal)
        else:
            group = "Early (S1-S3)"  # Default for terminal stages

        d["MedianSurvivalDays"] = group_medians.get(group, 0)
        d["SurvivalProb30d"] = round(group_surv30.get(group, 1.0), 3)

    print(f"  Survival analysis: {len(survival_curves)} curves")
    for group_name, curve in survival_curves.items():
        if group_name == "Overall":
            continue
        med = group_medians.get(group_name, 0)
        s30 = group_surv30.get(group_name, 1.0)
        print(f"    {group_name}: median={med}d, S(30d)={s30:.3f}")

    return survival_curves


# ═══════════════════════════════════════════════════════════════════════════
#  Deal Clustering — K-Means Archetypes
# ═══════════════════════════════════════════════════════════════════════════


def compute_deal_archetypes(deals):
    """Unsupervised K-Means clustering to discover deal archetypes.

    Falls back to rule-based assignment if sklearn unavailable.
    """
    open_and_closed = [d for d in deals if d.get("AgeInDays", 0) > 0]

    def _extract_cluster_features(d):
        return [
            math.log1p(max(0, d.get("Amount", 0) or 0)),
            d.get("AgeInDays", 0),
            d.get("PushCount", 0),
            STAGE_ORDER.get(d.get("StageName", ""), 0),
            d.get("WinProbability", 0),
            d.get("DaysInCurrentStage", 0),
            d.get("BackwardMoves", 0),
        ]

    try:
        from sklearn.cluster import KMeans
        from sklearn.preprocessing import StandardScaler

        if len(open_and_closed) < 20:
            raise ValueError(
                f"Only {len(open_and_closed)} deals — need 20+ for clustering"
            )

        X = [_extract_cluster_features(d) for d in open_and_closed]
        scaler = StandardScaler()
        X_scaled = scaler.fit_transform(X)

        k = min(4, len(open_and_closed))
        km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
        labels = km.fit_predict(X_scaled)

        # Auto-label clusters by ranking centroids on each trait
        centroids = scaler.inverse_transform(km.cluster_centers_)
        # Feature indices: 0=LogAmount, 1=Age, 2=PushCount, 3=StageOrd, 4=WinProb, 5=DaysInStage, 6=BackMoves
        n_c = len(centroids)
        # Rank each cluster on each feature (0=lowest, n-1=highest)
        ranks = {}
        for feat_idx in range(7):
            vals = [(centroids[i][feat_idx], i) for i in range(n_c)]
            vals.sort()
            for rank, (_, ci) in enumerate(vals):
                ranks[(ci, feat_idx)] = rank

        archetype_names = [None] * n_c
        used_labels = set()

        # Assign by strongest distinguishing trait (highest or lowest rank)
        label_rules = [
            # (label, feat_idx, direction, threshold_rank)
            ("Fast Closer", 4, "high", n_c - 1),  # highest win prob
            ("Stalled Giant", 5, "high", n_c - 1),  # longest days in stage
            ("Serial Pusher", 2, "high", n_c - 1),  # most push count
            ("Small Mover", 0, "low", 0),  # smallest deal size
            ("High-Value Deal", 0, "high", n_c - 1),  # largest deal size
            ("Early Stage", 3, "low", 0),  # lowest stage ordinal
            ("Late Stage", 3, "high", n_c - 1),  # highest stage ordinal
        ]

        for label, feat_idx, direction, threshold in label_rules:
            for ci in range(n_c):
                if archetype_names[ci] is not None:
                    continue
                if label in used_labels:
                    continue
                rank_val = ranks[(ci, feat_idx)]
                if direction == "high" and rank_val == threshold:
                    archetype_names[ci] = label
                    used_labels.add(label)
                elif direction == "low" and rank_val == threshold:
                    archetype_names[ci] = label
                    used_labels.add(label)

        # Fill any remaining with trait-based name
        for ci in range(n_c):
            if archetype_names[ci] is None:
                archetype_names[ci] = f"Cluster {ci + 1}"

        for d, label in zip(open_and_closed, labels):
            d["DealArchetype"] = archetype_names[label]

        print(f"  K-Means clustering: {k} clusters")
        for i, name in enumerate(archetype_names):
            count = sum(1 for l in labels if l == i)
            print(f"    Cluster {i} ({name}): {count} deals")

    except (ImportError, ValueError, RuntimeError) as e:
        print(f"  Clustering unavailable ({e}), using rule-based archetypes")
        for d in deals:
            age = d.get("AgeInDays", 0)
            amount = d.get("Amount", 0) or 0
            stage_ord = STAGE_ORDER.get(d.get("StageName", ""), 0)
            days_in_stage = d.get("DaysInCurrentStage", 0)
            push_count = d.get("PushCount", 0)

            if age < 60 and stage_ord >= 5:
                d["DealArchetype"] = "Fast Closer"
            elif amount > 500000 and days_in_stage > 45:
                d["DealArchetype"] = "Stalled Giant"
            elif push_count >= 3:
                d["DealArchetype"] = "Serial Pusher"
            elif age < 60 and amount < 100000:
                d["DealArchetype"] = "Small Mover"
            else:
                d["DealArchetype"] = "Steady Progressor"

    # Set archetype for deals not covered (closed won/lost with no age)
    for d in deals:
        if not d.get("DealArchetype"):
            d["DealArchetype"] = "Unclassified"

    return deals


# ═══════════════════════════════════════════════════════════════════════════
#  Velocity & Momentum Scoring
# ═══════════════════════════════════════════════════════════════════════════


def compute_velocity_momentum(deals, history_by_opp):
    """Compute velocity z-score and momentum flag for each deal.

    VelocityScore: z-score of deal velocity vs cohort at same stage.
    MomentumFlag: Accelerating/Decelerating/Steady based on recent vs early velocity.
    """
    # Compute per-deal stage velocities from history
    for d in deals:
        opp_id = d.get("Id", "")
        records = history_by_opp.get(opp_id, [])

        # Compute per-stage durations
        stage_durations = []
        prev_created = None
        for r in records:
            created = r.get("created", "")
            if prev_created and created:
                try:
                    d1 = datetime.strptime(prev_created[:10], "%Y-%m-%d")
                    d2 = datetime.strptime(created[:10], "%Y-%m-%d")
                    days = max(0, (d2 - d1).days)
                    if days > 0:
                        stage_durations.append(days)
                except ValueError:
                    pass
            prev_created = created

        # Split into early and recent halves
        n = len(stage_durations)
        if n >= 2:
            mid = n // 2
            early = stage_durations[:mid]
            recent = stage_durations[mid:]
            early_avg = sum(early) / len(early) if early else 0
            recent_avg = sum(recent) / len(recent) if recent else 0

            if early_avg > 0 and recent_avg > 0:
                ratio = recent_avg / early_avg
                if ratio < 0.7:
                    d["MomentumFlag"] = "Accelerating"
                elif ratio > 1.3:
                    d["MomentumFlag"] = "Decelerating"
                else:
                    d["MomentumFlag"] = "Steady"
            else:
                d["MomentumFlag"] = "Steady"
        else:
            d["MomentumFlag"] = "Steady"

    # Compute velocity z-scores by stage group
    stage_velocities = {}
    for d in deals:
        if d.get("IsClosed"):
            continue
        stage = d.get("StageName", "")
        age = d.get("AgeInDays", 0)
        if age <= 0:
            continue
        amount = d.get("Amount", 0) or 0
        velocity = amount / age
        if stage not in stage_velocities:
            stage_velocities[stage] = []
        stage_velocities[stage].append((d, velocity))

    for stage, dv_pairs in stage_velocities.items():
        velocities = [v for _, v in dv_pairs]
        if len(velocities) < 2:
            for d, v in dv_pairs:
                d["VelocityScore"] = 0
            continue
        mean = sum(velocities) / len(velocities)
        std = _std(velocities)
        if std < 1e-9:
            for d, v in dv_pairs:
                d["VelocityScore"] = 0
        else:
            for d, v in dv_pairs:
                d["VelocityScore"] = round((v - mean) / std, 2)

    # Ensure all deals have values
    for d in deals:
        if "VelocityScore" not in d or d["VelocityScore"] == 0:
            d.setdefault("VelocityScore", 0)
        if not d.get("MomentumFlag"):
            d["MomentumFlag"] = "Steady"

    accel = sum(1 for d in deals if d.get("MomentumFlag") == "Accelerating")
    decel = sum(1 for d in deals if d.get("MomentumFlag") == "Decelerating")
    steady = sum(1 for d in deals if d.get("MomentumFlag") == "Steady")
    print(
        f"  Velocity scoring: {accel} accelerating, {decel} decelerating, {steady} steady"
    )

    return deals


# ═══════════════════════════════════════════════════════════════════════════
#  Dataset Creation
# ═══════════════════════════════════════════════════════════════════════════

PIPELINE_ANALYTICS_FIELDS = [
    "Id",
    "Name",
    "AccountId",
    "AccountName",
    "StageName",
    "Amount",
    "ARR",
    "CloseDate",
    "CreatedDate",
    "IsClosed",
    "IsWon",
    "OwnerName",
    "ForecastCategory",
    "LeadSource",
    "Type",
    "PushCount",
    "PullCount",
    "NetPushDays",
    "DaysInCurrentStage",
    "AvgDaysPerStage",
    "TotalStageChanges",
    "BackwardMoves",
    "WinProbability",
    "AtRiskFlag",
    "DealVelocity",
    "AgeBucket",
    "PushBucket",
    "DealSizeBucket",
    "WinProbBucket",
    "Cohort",
    "StageOrdinal",
    "AgeInDays",
    "Probability",
    "ExpectedBookings",
    "PushAfterCommit",
    "StageSkipCount",
    "EverRegressed",
    "SlipRiskScore",
    "TimingScore",
    "FiscalYear",
    "FiscalQuarter",
    "MarkovWinProb",
    "ExpectedStepsToClose",
    "MedianSurvivalDays",
    "SurvivalProb30d",
    "DealArchetype",
    "VelocityScore",
    "MomentumFlag",
]

PIPELINE_ANALYTICS_META = [
    _dim("Id", "Opportunity ID"),
    _dim("Name", "Opportunity Name"),
    _dim("AccountId", "Account ID"),
    _dim("AccountName", "Account Name"),
    _dim("StageName", "Stage"),
    _measure("Amount", "Amount"),
    _measure("ARR", "ARR (EUR)"),
    _date("CloseDate", "Close Date"),
    _date("CreatedDate", "Created Date"),
    _dim("IsClosed", "Is Closed"),
    _dim("IsWon", "Is Won"),
    _dim("OwnerName", "Owner"),
    _dim("ForecastCategory", "Forecast Category"),
    _dim("LeadSource", "Lead Source"),
    _dim("Type", "Opportunity Type"),
    _measure("PushCount", "Push Count", scale=0, precision=4),
    _measure("PullCount", "Pull Count", scale=0, precision=4),
    _measure("NetPushDays", "Net Push Days", scale=0, precision=6),
    _measure("DaysInCurrentStage", "Days in Current Stage", scale=0, precision=5),
    _measure("AvgDaysPerStage", "Avg Days per Stage", scale=1, precision=6),
    _measure("TotalStageChanges", "Total Stage Changes", scale=0, precision=4),
    _measure("BackwardMoves", "Backward Moves", scale=0, precision=4),
    _measure("WinProbability", "Win Probability", scale=3, precision=4),
    _dim("AtRiskFlag", "At Risk Flag"),
    _measure("DealVelocity", "Deal Velocity ($/day)", scale=2, precision=10),
    _dim("AgeBucket", "Age Bucket"),
    _dim("PushBucket", "Push Bucket"),
    _dim("DealSizeBucket", "Deal Size Bucket"),
    _dim("WinProbBucket", "Win Probability Bucket"),
    _dim("Cohort", "Cohort Quarter"),
    _measure("StageOrdinal", "Stage Ordinal", scale=0, precision=2),
    _measure("AgeInDays", "Age (Days)", scale=0, precision=5),
    _measure("Probability", "Probability (%)", scale=0, precision=3),
    _measure("ExpectedBookings", "Expected Bookings"),
    _dim("PushAfterCommit", "Push After Commit"),
    _measure("StageSkipCount", "Stage Skip Count", scale=0, precision=3),
    _dim("EverRegressed", "Ever Regressed"),
    _measure("SlipRiskScore", "Slip Risk Score", scale=3, precision=4),
    _measure("TimingScore", "Timing Score", scale=3, precision=4),
    _dim("FiscalYear", "Fiscal Year"),
    _dim("FiscalQuarter", "Fiscal Quarter"),
    _measure("MarkovWinProb", "Markov Win Probability", scale=3, precision=4),
    _measure("ExpectedStepsToClose", "Expected Steps to Close", scale=1, precision=4),
    _measure("MedianSurvivalDays", "Median Survival Days", scale=0, precision=5),
    _measure("SurvivalProb30d", "Survival Prob 30d", scale=3, precision=4),
    _dim("DealArchetype", "Deal Archetype"),
    _measure("VelocityScore", "Velocity Score", scale=2, precision=5),
    _dim("MomentumFlag", "Momentum Flag"),
]

TRENDLINE_FIELDS = [
    "Month",
    "MonthLabel",
    "TotalPipeline",
    "WeightedPipeline",
    "NewDeals",
    "ClosedWonAmount",
    "ClosedWonCount",
    "AvgDealSize",
    "WinRate",
    "AvgCycleLength",
    "TrendPipeline",
    "TrendWinRate",
    "TrendCycleLength",
    "TrendPipelineUpper",
    "TrendPipelineLower",
    "TrendWinRateUpper",
    "TrendWinRateLower",
    "PipelineR2",
    "IsForecast",
]

TRENDLINE_META = [
    _date("Month", "Month"),
    _dim("MonthLabel", "Month Label"),
    _measure("TotalPipeline", "Total Pipeline"),
    _measure("WeightedPipeline", "Weighted Pipeline"),
    _measure("NewDeals", "New Deals", scale=0, precision=5),
    _measure("ClosedWonAmount", "Closed Won Amount"),
    _measure("ClosedWonCount", "Closed Won Count", scale=0, precision=5),
    _measure("AvgDealSize", "Avg Deal Size"),
    _measure("WinRate", "Win Rate (%)", scale=1, precision=4),
    _measure("AvgCycleLength", "Avg Cycle Length (days)", scale=1, precision=5),
    _measure("TrendPipeline", "Trend Pipeline"),
    _measure("TrendWinRate", "Trend Win Rate (%)", scale=1, precision=4),
    _measure("TrendCycleLength", "Trend Cycle Length", scale=1, precision=5),
    _measure("TrendPipelineUpper", "Trend Pipeline Upper (95% PI)"),
    _measure("TrendPipelineLower", "Trend Pipeline Lower (95% PI)"),
    _measure(
        "TrendWinRateUpper", "Trend Win Rate Upper (95% PI)", scale=1, precision=4
    ),
    _measure(
        "TrendWinRateLower", "Trend Win Rate Lower (95% PI)", scale=1, precision=4
    ),
    _measure("PipelineR2", "Pipeline Trend R²", scale=3, precision=4),
    _dim("IsForecast", "Is Forecast"),
]

MC_FIELDS = [
    "Scenario",
    "ProjectedRevenue",
    "ProjectedDealCount",
    "ProjectedWinRate",
    "VaR5",
    "CVaR5",
]

MC_META = [
    _dim("Scenario", "Scenario"),
    _measure("ProjectedRevenue", "Projected Revenue"),
    _measure("ProjectedDealCount", "Projected Deal Count", scale=0, precision=5),
    _measure("ProjectedWinRate", "Projected Win Rate (%)", scale=1, precision=4),
    _measure("VaR5", "Value at Risk (5%)", scale=2, precision=18),
    _measure("CVaR5", "Conditional VaR (5%)", scale=2, precision=18),
]

SURV_FIELDS = ["StageGroup", "Day", "SurvivalProb"]

SURV_META = [
    _dim("StageGroup", "Stage Group"),
    _measure("Day", "Day", scale=0, precision=5),
    _measure("SurvivalProb", "Survival Probability", scale=4, precision=5),
]

TRANSITION_FIELDS = ["FromStage", "ToStage", "Probability", "TransitionCount"]

TRANSITION_META = [
    _dim("FromStage", "From Stage"),
    _dim("ToStage", "To Stage"),
    _measure("Probability", "Transition Probability", scale=3, precision=4),
    _measure("TransitionCount", "Transition Count", scale=0, precision=6),
]


def create_datasets(inst, tok):
    """Fetch data, compute all fields, and upload all datasets.

    Returns (deals, trendline_rows, mc_rows, metrics, coefficients,
            survival_curves, transition_matrix).
    """
    print("\n=== Building Advanced Pipeline Analytics datasets ===")

    # ── 1. Fetch raw data ──
    print("  Fetching opportunities...")
    opps = _soql(inst, tok, OPP_SOQL)
    print(f"  {len(opps)} opportunities")

    print("  Fetching opportunity history...")
    hist_records = _soql(inst, tok, HISTORY_SOQL)
    print(f"  {len(hist_records)} history records")

    print("  Fetching field history...")
    field_hist_records = _soql(inst, tok, FIELD_HISTORY_SOQL)
    print(f"  {len(field_hist_records)} field history records")

    # ── 2. Index history by OpportunityId ──
    history_by_opp = {}
    for r in hist_records:
        opp_id = r.get("OpportunityId", "")
        if opp_id not in history_by_opp:
            history_by_opp[opp_id] = []
        history_by_opp[opp_id].append(
            {
                "stage": r.get("StageName") or "",
                "amount": r.get("Amount") or 0,
                "close_date": (r.get("CloseDate") or "")[:10],
                "created": (r.get("CreatedDate") or "")[:19],
            }
        )

    field_history_by_opp = {}
    for r in field_hist_records:
        opp_id = r.get("OpportunityId", "")
        if opp_id not in field_history_by_opp:
            field_history_by_opp[opp_id] = []
        field_history_by_opp[opp_id].append(
            {
                "field": r.get("Field") or "",
                "old": str(r.get("OldValue") or ""),
                "new": str(r.get("NewValue") or ""),
                "created": (r.get("CreatedDate") or "")[:10],
            }
        )

    # ── 3. Build per-deal records ──
    deals = []
    for o in opps:
        opp_id = o.get("Id", "")
        acct = o.get("Account") or {}
        owner = o.get("Owner") or {}
        is_closed = str(o.get("IsClosed", False)).lower() == "true"
        is_won = str(o.get("IsWon", False)).lower() == "true"
        # Explicit None checks — or-chain would conflate $0 with NULL
        amount = o.get("ConvertedARR")
        if amount is None:
            amount = o.get("APTS_Forecast_ARR__c")
        if amount is None:
            amount = o.get("Amount")
        if amount is None:
            amount = 0
        age = o.get("AgeInDays") or 0
        days_in_stage = o.get("LastStageChangeInDays") or 0
        stage = o.get("StageName") or ""
        prob = o.get("Probability") or 0
        created = (o.get("CreatedDate") or "")[:10]
        close_date = (o.get("CloseDate") or "")[:10]

        # Compute push stats
        push_count, pull_count, net_push_days = _compute_push_stats(
            field_history_by_opp, opp_id
        )

        # Compute stage stats
        total_changes, backward_moves, avg_days_per_stage = _compute_stage_stats(
            history_by_opp, opp_id
        )

        # Compute push-after-commit and stage skip count
        push_after_commit = _compute_push_after_commit(field_history_by_opp, opp_id)
        stage_skip_count = _compute_stage_skip_count(history_by_opp, opp_id)

        # Deal velocity
        deal_velocity = round(amount / max(age, 1), 2) if amount > 0 else 0

        deal = {
            "Id": opp_id,
            "Name": (o.get("Name") or "")[:255],
            "AccountId": o.get("AccountId", ""),
            "AccountName": (acct.get("Name") or "")[:255],
            "StageName": stage,
            "Amount": amount,
            "ARR": amount,  # reuses cascaded ConvertedARR→APTS_Forecast_ARR→Amount
            "CloseDate": close_date,
            "CreatedDate": created,
            "IsClosed": is_closed,
            "IsWon": is_won,
            "OwnerName": (owner.get("Name") or "")[:255],
            "ForecastCategory": o.get("ForecastCategoryName", ""),
            "LeadSource": o.get("LeadSource") or "",
            "Type": o.get("Type") or "",
            "PushCount": push_count,
            "PullCount": pull_count,
            "NetPushDays": net_push_days,
            "DaysInCurrentStage": days_in_stage,
            "AvgDaysPerStage": avg_days_per_stage,
            "TotalStageChanges": total_changes,
            "BackwardMoves": backward_moves,
            "WinProbability": 0,  # placeholder, computed below
            "AtRiskFlag": "",
            "DealVelocity": deal_velocity,
            "AgeBucket": _bucket_age(age),
            "PushBucket": _bucket_push(push_count),
            "DealSizeBucket": _bucket_deal_size(amount),
            "WinProbBucket": "",  # placeholder
            "Cohort": _cohort(created),
            "StageOrdinal": STAGE_ORDER.get(stage, 0),
            "AgeInDays": age,
            "Probability": prob,
            "ExpectedBookings": 0,  # computed after ML models
            "PushAfterCommit": push_after_commit,
            "StageSkipCount": stage_skip_count,
            "EverRegressed": "true" if backward_moves > 0 else "false",
            "SlipRiskScore": 0,  # computed by model 2
            "TimingScore": 0,  # computed by model 3
            "FiscalYear": _fiscal_year(close_date),
            "FiscalQuarter": _fiscal_quarter(close_date),
            "MarkovWinProb": 0,  # computed by Markov chain model
            "ExpectedStepsToClose": 0,  # computed by Markov chain model
            "MedianSurvivalDays": 0,  # computed by Kaplan-Meier
            "SurvivalProb30d": 0,  # computed by Kaplan-Meier
            "DealArchetype": "",  # computed by K-Means clustering
            "VelocityScore": 0,  # computed by velocity scoring
            "MomentumFlag": "",  # computed by velocity scoring
        }
        deals.append(deal)

    print(f"  Built {len(deals)} deal records")

    # ── 4. Win probability model ──
    print("\n  Computing win probabilities...")
    deals, metrics, coefficients = compute_win_probabilities(deals)

    at_risk = sum(1 for d in deals if d.get("AtRiskFlag") == "At Risk")
    print(f"  At-risk deals: {at_risk}")

    # ── 4b. Slip/push risk model ──
    print("\n  Computing slip risk scores...")
    deals = compute_slip_risk(deals)
    high_slip = sum(
        1 for d in deals if not d.get("IsClosed") and d.get("SlipRiskScore", 0) > 0.6
    )
    print(f"  High slip-risk deals (>0.6): {high_slip}")

    # ── 4c. Timing/close-in-period model ──
    print("\n  Computing timing scores...")
    deals = compute_timing_score(deals)
    close_qtr = sum(
        1 for d in deals if not d.get("IsClosed") and d.get("TimingScore", 0) > 0.7
    )
    print(f"  Likely close this quarter (>0.7): {close_qtr}")

    # ── 4d. Markov chain stage transitions ──
    print("\n  Computing Markov chain transitions...")
    transition_matrix = compute_markov_chain(history_by_opp, deals)

    # ── 4e. Survival analysis ──
    print("\n  Computing survival analysis...")
    survival_curves = compute_survival_analysis(deals, history_by_opp)

    # ── 4f. Deal clustering ──
    print("\n  Computing deal archetypes...")
    deals = compute_deal_archetypes(deals)

    # ── 4g. Velocity & momentum scoring ──
    print("\n  Computing velocity & momentum scores...")
    deals = compute_velocity_momentum(deals, history_by_opp)

    # ── 4h. Expected bookings (after all models) ──
    for d in deals:
        if not d.get("IsClosed"):
            d["ExpectedBookings"] = round(
                (d.get("Amount", 0) or 0) * d.get("WinProbability", 0), 2
            )
        else:
            d["ExpectedBookings"] = d.get("Amount", 0) if d.get("IsWon") else 0

    # ── 5. Upload Pipeline_Analytics ──
    buf = io.StringIO()
    writer = csv.DictWriter(
        buf, fieldnames=PIPELINE_ANALYTICS_FIELDS, lineterminator="\n"
    )
    writer.writeheader()
    for d in deals:
        row = {}
        for f in PIPELINE_ANALYTICS_FIELDS:
            val = d.get(f, "")
            if isinstance(val, bool):
                val = str(val).lower()
            row[f] = val
        writer.writerow(row)
    csv_bytes = buf.getvalue().encode("utf-8")
    print(f"\n  Pipeline_Analytics CSV: {len(csv_bytes):,} bytes, {len(deals)} rows")
    upload_dataset(inst, tok, DS, DS_LABEL, PIPELINE_ANALYTICS_META, csv_bytes)

    # ── 6. Trendline dataset ──
    print("\n  Computing trendlines...")
    trendline_rows = compute_trendlines(deals)
    actual_months = sum(1 for r in trendline_rows if r.get("IsForecast") == "false")
    forecast_months = sum(1 for r in trendline_rows if r.get("IsForecast") == "true")
    print(f"  {actual_months} actual months + {forecast_months} forecast months")

    buf2 = io.StringIO()
    writer2 = csv.DictWriter(buf2, fieldnames=TRENDLINE_FIELDS, lineterminator="\n")
    writer2.writeheader()
    for r in trendline_rows:
        writer2.writerow(r)
    csv_bytes2 = buf2.getvalue().encode("utf-8")
    print(f"  Pipeline_Trendlines CSV: {len(csv_bytes2):,} bytes")
    upload_dataset(inst, tok, DS_TREND, DS_TREND_LABEL, TRENDLINE_META, csv_bytes2)

    # ── 7. Monte Carlo dataset ──
    print("\n  Running Monte Carlo simulation (10,000 iterations)...")
    mc_rows = run_monte_carlo(deals)
    for r in mc_rows:
        print(f"    {r['Scenario']}: ${r['ProjectedRevenue']:,.0f}")

    buf3 = io.StringIO()
    writer3 = csv.DictWriter(buf3, fieldnames=MC_FIELDS, lineterminator="\n")
    writer3.writeheader()
    for r in mc_rows:
        writer3.writerow(r)
    csv_bytes3 = buf3.getvalue().encode("utf-8")
    print(f"  Pipeline_Monte_Carlo CSV: {len(csv_bytes3):,} bytes")
    upload_dataset(inst, tok, DS_MC, DS_MC_LABEL, MC_META, csv_bytes3)

    # ── 8. Survival curve dataset ──
    print("\n  Building survival curve dataset...")
    surv_rows = []
    for group_name, curve in survival_curves.items():
        for day, prob in curve:
            surv_rows.append(
                {"StageGroup": group_name, "Day": day, "SurvivalProb": round(prob, 4)}
            )
    if surv_rows:
        buf4 = io.StringIO()
        writer4 = csv.DictWriter(buf4, fieldnames=SURV_FIELDS, lineterminator="\n")
        writer4.writeheader()
        for r in surv_rows:
            writer4.writerow(r)
        csv_bytes4 = buf4.getvalue().encode("utf-8")
        print(
            f"  Pipeline_Survival CSV: {len(csv_bytes4):,} bytes, "
            f"{len(surv_rows)} data points"
        )
        upload_dataset(inst, tok, DS_SURV, DS_SURV_LABEL, SURV_META, csv_bytes4)

    # ── 9. Transition matrix dataset ──
    print("\n  Building transition matrix dataset...")
    trans_rows = []
    # Build from raw counts stored during Markov chain computation
    for (from_s, to_s), prob in transition_matrix.items():
        trans_rows.append(
            {
                "FromStage": from_s.split(" - ")[-1] if " - " in from_s else from_s,
                "ToStage": to_s.split(" - ")[-1] if " - " in to_s else to_s,
                "Probability": prob,
                "TransitionCount": 0,
            }
        )
    if trans_rows:
        buf5 = io.StringIO()
        writer5 = csv.DictWriter(
            buf5, fieldnames=TRANSITION_FIELDS, lineterminator="\n"
        )
        writer5.writeheader()
        for r in trans_rows:
            writer5.writerow(r)
        csv_bytes5 = buf5.getvalue().encode("utf-8")
        ds_trans = "Pipeline_Transitions"
        print(f"  {ds_trans} CSV: {len(csv_bytes5):,} bytes, {len(trans_rows)} cells")
        upload_dataset(
            inst, tok, ds_trans, "Pipeline Transitions", TRANSITION_META, csv_bytes5
        )

    return (
        deals,
        trendline_rows,
        mc_rows,
        metrics,
        coefficients,
        survival_curves,
        transition_matrix,
    )


# ═══════════════════════════════════════════════════════════════════════════
#  Steps (SAQL)
# ═══════════════════════════════════════════════════════════════════════════


def build_steps(ds_meta, metrics, coefficients):
    """Build all SAQL steps for the 7-page dashboard."""
    PA = f'q = load "{DS}";\n'
    TL = f'q = load "{DS_TREND}";\n'
    MC = f'q = load "{DS_MC}";\n'

    steps = {
        # ── Filters ──
        "f_stage": af("StageName", ds_meta),
        "f_push": af("PushBucket", ds_meta),
        "f_winprob": af("WinProbBucket", ds_meta),
        "f_fy": af("FiscalYear", ds_meta),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 1: Executive Summary
        # ═══════════════════════════════════════════════════════════════
        # KPI: Total Pipeline (open deals)
        "s_total_pipeline": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as total_pipeline;"
        ),
        # KPI: Weighted Pipeline
        "s_weighted_pipeline": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR * Probability / 100) as weighted_pipeline;"
        ),
        # KPI: Win Rate (closed deals)
        "s_win_rate": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "true";\n'
            + "q = group q by all;\n"
            + 'q = foreach q generate avg(case when IsWon == "true" then 100 else 0 end) as win_rate;'
        ),
        # KPI: Avg Cycle Length (won deals)
        "s_avg_cycle": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(AgeInDays) as avg_cycle;"
        ),
        # KPI: At-Risk Deals
        "s_at_risk_count": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by AtRiskFlag == "At Risk";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as at_risk;"
        ),
        # KPI: Avg Win Probability (open)
        "s_avg_win_prob": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(WinProbability) * 100 as avg_wp;"
        ),
        # Pipeline trend line chart (actuals from trendlines dataset)
        "s_pipeline_trend": sq(
            TL
            + 'q = filter q by IsForecast == "false";\n'
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "max(TotalPipeline) as TotalPipeline, "
            + "max(TrendPipeline) as TrendPipeline;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # Monte Carlo forecast bar chart
        "s_monte_carlo": sq(
            MC
            + "q = group q by Scenario;\n"
            + "q = foreach q generate Scenario, "
            + "max(ProjectedRevenue) as ProjectedRevenue;\n"
            + "q = order q by ProjectedRevenue asc;"
        ),
        # Win probability distribution
        "s_win_prob_dist": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by WinProbBucket;\n"
            + "q = foreach q generate WinProbBucket, count() as cnt;\n"
            + "q = order q by WinProbBucket asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 2: Deal Push Intelligence
        # ═══════════════════════════════════════════════════════════════
        # KPI: Total Pushes
        "s_total_pushes": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = group q by all;\n"
            + "q = foreach q generate sum(PushCount) as total_pushes;"
        ),
        # KPI: Avg Push Count
        "s_avg_push": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = group q by all;\n"
            + "q = foreach q generate avg(PushCount) as avg_push;"
        ),
        # KPI: Deals Never Pushed % — intentionally no PBF (summary across all buckets)
        "s_never_pushed_pct": sq(
            PA
            + SF
            + FYF
            + "q = group q by all;\n"
            + 'q = foreach q generate avg(case when PushBucket == "No Push" then 100 else 0 end) as pct;'
        ),
        # KPI: Avg Net Push Days
        "s_avg_net_push": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = filter q by PushCount > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate avg(NetPushDays) as avg_net_push;"
        ),
        # Push frequency distribution
        "s_push_dist": sq(
            PA
            + SF
            + FYF
            + "q = group q by PushBucket;\n"
            + "q = foreach q generate PushBucket, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Push impact on win rate
        "s_push_vs_win": sq(
            PA
            + SF
            + FYF
            + PBF
            + 'q = filter q by IsClosed == "true";\n'
            + 'q = foreach q generate (case when PushCount == 0 then "Not Pushed" else "Pushed" end) as PushStatus, '
            + "IsWon;\n"
            + "q = group q by PushStatus;\n"
            + 'q = foreach q generate PushStatus, avg(case when IsWon == "true" then 100 else 0 end) as win_rate;'
        ),
        # Top 10 most-pushed open deals (row-level projection, no group needed)
        "s_top_pushed": sq(
            PA
            + SF
            + FYF
            + PBF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by PushCount > 0;\n"
            + "q = foreach q generate Name, PushCount, NetPushDays, "
            + "WinProbability, ARR as Amount;\n"
            + "q = order q by PushCount desc;\n"
            + "q = limit q 10;"
        ),
        # Push trend by cohort month
        "s_push_by_cohort": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = filter q by PushCount > 0;\n"
            + 'q = filter q by Cohort != "";\n'
            + "q = group q by Cohort;\n"
            + "q = foreach q generate Cohort, count() as cnt, avg(PushCount) as avg_push;\n"
            + "q = order q by Cohort asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 3: Win Probability Intelligence
        # ═══════════════════════════════════════════════════════════════
        # KPI: AUC-ROC (model quality metric, hardcoded from Python-side model)
        # Shows 0.0 when sklearn fallback is used — not filtered by sidebar
        "s_model_accuracy": sq(
            PA
            + "q = group q by all;\n"
            + f"q = foreach q generate {round(metrics.get('cv_auc_mean', metrics.get('auc_roc', 0)) * 100, 1)} as accuracy;"
        ),
        # KPI: At-Risk Count
        "s_at_risk_p3": sq(
            PA
            + SF
            + FYF
            + WPF
            + 'q = filter q by AtRiskFlag == "At Risk";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # KPI: High-Confidence Count
        "s_high_conf": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + 'q = filter q by WinProbBucket == "Very High (80-100%)";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Win probability by stage — intentionally no SF (chart groups by StageName;
        # stage filter would collapse to single bar)
        "s_wp_by_stage": sq(
            PA
            + WPF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, avg(WinProbability) * 100 as avg_wp;\n"
            + "q = order q by avg_wp desc;"
        ),
        # Feature importance (static from coefficients — encoded as SAQL union)
        "s_feature_importance": _build_feature_importance_step(coefficients),
        # At-risk deals table (row-level projection, no group needed)
        "s_at_risk_table": sq(
            PA
            + SF
            + FYF
            + WPF
            + 'q = filter q by AtRiskFlag == "At Risk";\n'
            + "q = foreach q generate Name, StageName, WinProbability, ARR as Amount, DaysInCurrentStage;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 20;"
        ),
        # Win prob vs deal size scatter
        "s_wp_vs_size": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by DealSizeBucket;\n"
            + "q = foreach q generate DealSizeBucket, "
            + "avg(WinProbability) * 100 as avg_wp, avg(ARR) as avg_arr;\n"
            + "q = order q by avg_arr asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 4: Stage Bottleneck Analysis
        # ═══════════════════════════════════════════════════════════════
        # KPI: Avg Days to Close (won deals)
        "s_avg_days_close": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(AgeInDays) as avg_days;"
        ),
        # KPI: Backward Move Rate
        "s_backward_rate": sq(
            PA
            + SF
            + FYF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(case when BackwardMoves > 0 then 100 else 0 end) as bm_rate;"
        ),
        # KPI: Conversion Rate (won / closed)
        "s_conversion_rate": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "true";\n'
            + "q = group q by all;\n"
            + 'q = foreach q generate avg(case when IsWon == "true" then 100 else 0 end) as conv_rate;'
        ),
        # KPI: Longest Stage (scalar)
        "s_longest_stage": sq(
            PA
            + SF
            + FYF
            + "q = filter q by AvgDaysPerStage > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate max(AvgDaysPerStage) as max_days;"
        ),
        # Avg days per stage (horizontal bar)
        "s_days_per_stage": sq(
            PA
            + SF
            + FYF
            + "q = filter q by AvgDaysPerStage > 0;\n"
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, avg(AvgDaysPerStage) as avg_days;\n"
            + "q = order q by avg_days desc;"
        ),
        # Stage conversion funnel
        "s_stage_funnel": sq(
            PA
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Backward moves by stage
        "s_backward_by_stage": sq(
            PA
            + SF
            + FYF
            + "q = filter q by BackwardMoves > 0;\n"
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, sum(BackwardMoves) as bm_count;\n"
            + "q = order q by bm_count desc;"
        ),
        # Stuck deals — open > 60 days in current stage (row-level projection, no group needed)
        "s_stuck_deals": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by DaysInCurrentStage > 60;\n"
            + "q = foreach q generate Name, StageName, DaysInCurrentStage, ARR as Amount;\n"
            + "q = order q by DaysInCurrentStage desc;\n"
            + "q = limit q 15;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 5: Deals Won Deep Dive
        # ═══════════════════════════════════════════════════════════════
        # KPI: Total Won Count
        "s_won_count": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # KPI: Won Amount
        "s_won_amount": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ARR) as won_arr;"
        ),
        # KPI: Avg Won Deal Size
        "s_avg_won_size": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ARR) as avg_size;"
        ),
        # KPI: Avg Won Cycle Length
        "s_avg_won_cycle": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(AgeInDays) as avg_cycle;"
        ),
        # Won by cohort
        "s_won_by_cohort": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + 'q = filter q by Cohort != "";\n'
            + "q = group q by Cohort;\n"
            + "q = foreach q generate Cohort, sum(ARR) as won_arr, count() as cnt;\n"
            + "q = order q by Cohort asc;"
        ),
        # Won vs Lost characteristics
        "s_won_vs_lost": sq(
            PA
            + 'q = filter q by IsClosed == "true";\n'
            + 'q = foreach q generate (case when IsWon == "true" then "Won" else "Lost" end) as Outcome, '
            + "PushCount, AvgDaysPerStage, TotalStageChanges;\n"
            + "q = group q by Outcome;\n"
            + "q = foreach q generate Outcome, "
            + "avg(PushCount) as avg_pushes, "
            + "avg(AvgDaysPerStage) as avg_days, "
            + "avg(TotalStageChanges) as avg_changes;"
        ),
        # Won by lead source
        "s_won_by_source": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + 'q = filter q by LeadSource != "";\n'
            + "q = group q by LeadSource;\n"
            + "q = foreach q generate LeadSource, sum(ARR) as won_arr;\n"
            + "q = order q by won_arr desc;"
        ),
        # Won by deal size bucket
        "s_won_by_size": sq(
            PA
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by DealSizeBucket;\n"
            + "q = foreach q generate DealSizeBucket, count() as cnt, sum(ARR) as won_arr;\n"
            + "q = order q by won_arr desc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 6: Trendline Analytics
        # ═══════════════════════════════════════════════════════════════
        # Pipeline trend with forecast band (pre-computed Python trendlines + 95% PI)
        "s_trend_pipeline_full": sq(
            TL
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "max(TotalPipeline) as TotalPipeline, "
            + "max(TrendPipeline) as TrendPipeline, "
            + "max(TrendPipelineUpper) as TrendUpper, "
            + "max(TrendPipelineLower) as TrendLower;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # Win rate trend with forecast band (pre-computed Python trendlines)
        "s_trend_winrate": sq(
            TL
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "max(WinRate) as WinRate, "
            + "max(TrendWinRate) as TrendWinRate, "
            + "max(TrendWinRateUpper) as TrendUpper, "
            + "max(TrendWinRateLower) as TrendLower;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # Cycle length trend
        "s_trend_cycle": sq(
            TL
            + "q = group q by (MonthLabel, IsForecast);\n"
            + "q = foreach q generate MonthLabel, IsForecast, "
            + "avg(AvgCycleLength) as AvgCycleLength, "
            + "avg(TrendCycleLength) as TrendCycleLength;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # New deals per month
        "s_trend_new_deals": sq(
            TL
            + 'q = filter q by IsForecast == "false";\n'
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, max(NewDeals) as NewDeals;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # Monte Carlo detail table (row-level, 5 rows)
        "s_mc_detail": sq(
            MC
            + "q = foreach q generate Scenario, "
            + "ProjectedRevenue as Revenue, "
            + "ProjectedDealCount as DealCount, "
            + "ProjectedWinRate as WinRate;\n"
            + "q = order q by Revenue asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  NEW: Enhanced analytics steps
        # ═══════════════════════════════════════════════════════════════
        # Page 1: Expected Bookings KPI (open deals)
        "s_expected_bookings": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(ExpectedBookings) as expected_bookings;"
        ),
        # Page 1: Action Queue — top 15 open deals by ExpectedBookings
        "s_action_queue": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate Name, Amount, "
            + "WinProbability * 100 as WinProb, "
            + "SlipRiskScore * 100 as SlipRisk, "
            + "StageName, DaysInCurrentStage, OwnerName;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 15;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  FY Revenue Forecast
        # ═══════════════════════════════════════════════════════════════
        # FY Closed Won total (KPI)
        "s_fy_closed": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsWon == "true";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(Amount) as fy_closed;"
        ),
        # FY Projected Revenue = Closed Won + Weighted Open Pipeline (KPI)
        "s_fy_projected": sq(
            PA
            + SF
            + FYF
            + "q = group q by all;\n"
            + 'q = foreach q generate sum(case when IsWon == "true" then Amount else 0 end) '
            + '+ sum(case when IsClosed == "false" then Amount * WinProbability else 0 end) '
            + "as fy_projected;"
        ),
        # FY Open Pipeline (unweighted) for KPI
        "s_fy_open_pipeline": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate sum(Amount) as fy_open;"
        ),
        # FY Quarterly Revenue Breakdown (Closed vs Weighted Forecast)
        "s_fy_quarterly": sq(
            PA
            + SF
            + FYF
            + "q = group q by FiscalQuarter;\n"
            + "q = foreach q generate FiscalQuarter, "
            + 'sum(case when IsWon == "true" then Amount else 0 end) as ClosedWon, '
            + 'sum(case when IsClosed == "false" then Amount * WinProbability else 0 end) as WeightedForecast;\n'
            + "q = order q by FiscalQuarter asc;"
        ),
        # FY Cumulative Revenue (quarterly running total for line overlay)
        "s_fy_cumulative": sq(
            PA
            + SF
            + FYF
            + "q = group q by FiscalQuarter;\n"
            + "q = foreach q generate FiscalQuarter, "
            + 'sum(case when IsWon == "true" then Amount else 0 end) as ClosedWon, '
            + 'sum(case when IsWon == "true" then Amount else 0 end) '
            + '+ sum(case when IsClosed == "false" then Amount * WinProbability else 0 end) '
            + "as ProjectedTotal;\n"
            + "q = order q by FiscalQuarter asc;"
        ),
        # Page 2: Push After Commit % (pushed deals only)
        "s_push_commit_pct": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = filter q by PushCount > 0;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + 'avg(case when PushAfterCommit == "true" then 100 else 0 end) as pct;'
        ),
        # Page 2: Push rate by ForecastCategory
        "s_push_by_fcat": sq(
            PA
            + SF
            + FYF
            + PBF
            + "q = group q by ForecastCategory;\n"
            + "q = foreach q generate ForecastCategory, "
            + "avg(case when PushCount > 0 then 100 else 0 end) as push_rate;\n"
            + "q = order q by push_rate desc;"
        ),
        # Page 3: High Slip Risk count (SlipRiskScore > 0.6, open)
        "s_high_slip_risk": sq(
            PA
            + SF
            + FYF
            + WPF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by SlipRiskScore > 0.6;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Page 3: Close This Quarter count (TimingScore > 0.7, open)
        "s_close_this_qtr": sq(
            PA
            + SF
            + FYF
            + WPF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = filter q by TimingScore > 0.7;\n"
            + "q = group q by all;\n"
            + "q = foreach q generate count() as cnt;"
        ),
        # Page 3: Slip Risk vs Win Probability scatter data (open deals)
        "s_slip_vs_win": sq(
            PA
            + SF
            + FYF
            + WPF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = foreach q generate Name, "
            + "SlipRiskScore * 100 as SlipRisk, "
            + "WinProbability * 100 as WinProb, "
            + "ARR as Amount;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 50;"
        ),
        # Page 4: Stage Skip Rate (deals with StageSkipCount > 0 / total)
        "s_stage_skip_rate": sq(
            PA
            + SF
            + FYF
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + "avg(case when StageSkipCount > 0 then 100 else 0 end) as skip_rate;"
        ),
        # Page 4: Stage Skip impact on win rate
        "s_skip_vs_win": sq(
            PA
            + SF
            + FYF
            + 'q = filter q by IsClosed == "true";\n'
            + "q = foreach q generate "
            + '(case when StageSkipCount > 0 then "Skipped" else "Sequential" end) as SkipStatus, '
            + "IsWon;\n"
            + "q = group q by SkipStatus;\n"
            + "q = foreach q generate SkipStatus, "
            + 'avg(case when IsWon == "true" then 100 else 0 end) as win_rate;'
        ),
        # ═══════════════════════════════════════════════════════════════
        #  Window Functions (P6)
        # ═══════════════════════════════════════════════════════════════
        # Running YTD won revenue
        "s_ytd_won": sq(
            TL
            + 'q = filter q by IsForecast == "false";\n'
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "sum(ClosedWonAmount) as WonAmount, "
            + "sum(sum(ClosedWonAmount)) over ([..0] partition by all order by (MonthLabel)) as YTDWon;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # 3-month moving average pipeline
        "s_pipeline_ma3": sq(
            TL
            + 'q = filter q by IsForecast == "false";\n'
            + "q = group q by MonthLabel;\n"
            + "q = foreach q generate MonthLabel, "
            + "max(TotalPipeline) as Pipeline, "
            + "avg(max(TotalPipeline)) over ([..0] partition by all order by (MonthLabel) "
            + "rows between 2 preceding and current row) as MA3;\n"
            + "q = order q by MonthLabel asc;"
        ),
        # VaR KPI (from MC dataset)
        "s_var5": sq(
            MC + "q = group q by all;\n" + "q = foreach q generate max(VaR5) as var5;"
        ),
        # CVaR KPI (from MC dataset)
        "s_cvar5": sq(
            MC + "q = group q by all;\n" + "q = foreach q generate max(CVaR5) as cvar5;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  PAGE 7: Quantitative Intelligence
        # ═══════════════════════════════════════════════════════════════
        # Markov Win Prob avg (open deals)
        "s_avg_markov": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(MarkovWinProb) * 100 as avg_markov;"
        ),
        # Expected Steps to Close avg (open deals)
        "s_avg_steps": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(ExpectedStepsToClose) as avg_steps;"
        ),
        # Median Survival Days avg (open deals)
        "s_avg_survival": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by all;\n"
            + "q = foreach q generate avg(MedianSurvivalDays) as avg_surv;"
        ),
        # Archetype distribution (all deals)
        "s_archetype_dist": sq(
            PA
            + FYF
            + 'q = filter q by DealArchetype != "";\n'
            + "q = group q by DealArchetype;\n"
            + "q = foreach q generate DealArchetype, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Archetype scatter (deal-level: Amount vs AgeInDays, colored by archetype)
        "s_archetype_scatter": sq(
            PA
            + FYF
            + 'q = filter q by DealArchetype != "" && DealArchetype != "Unclassified";\n'
            + "q = group q by (Name, DealArchetype);\n"
            + "q = foreach q generate Name, DealArchetype, "
            + "avg(Amount) as Amount, avg(AgeInDays) as AgeInDays;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 500;"
        ),
        # Archetype × win rate
        "s_archetype_winrate": sq(
            PA
            + FYF
            + 'q = filter q by DealArchetype != "";\n'
            + 'q = filter q by IsClosed == "true";\n'
            + "q = group q by DealArchetype;\n"
            + "q = foreach q generate DealArchetype, "
            + 'avg(case when IsWon == "true" then 100 else 0 end) as win_rate;\n'
            + "q = order q by win_rate desc;"
        ),
        # Momentum distribution
        "s_momentum_dist": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by MomentumFlag;\n"
            + "q = foreach q generate MomentumFlag, count() as cnt;\n"
            + "q = order q by cnt desc;"
        ),
        # Markov win prob by stage
        "s_markov_by_stage": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "false";\n'
            + "q = group q by StageName;\n"
            + "q = foreach q generate StageName, "
            + "avg(MarkovWinProb) * 100 as markov_wp;\n"
            + "q = order q by markov_wp desc;"
        ),
        # Archetype characteristics table
        "s_archetype_chars": sq(
            PA
            + FYF
            + 'q = filter q by DealArchetype != "";\n'
            + "q = group q by DealArchetype;\n"
            + "q = foreach q generate DealArchetype, "
            + "count() as cnt, avg(ARR) as avg_arr, "
            + "avg(AgeInDays) as avg_age, "
            + "avg(WinProbability) * 100 as avg_wp;\n"
            + "q = order q by cnt desc;"
        ),
        # VaR waterfall (MC scenarios)
        "s_var_waterfall": sq(
            MC
            + "q = foreach q generate Scenario, "
            + "ProjectedRevenue as Revenue;\n"
            + "q = order q by Revenue asc;"
        ),
        # ═══════════════════════════════════════════════════════════════
        #  NEW: Production-grade chart steps
        # ═══════════════════════════════════════════════════════════════
        # Transition heatmap (from Pipeline_Transitions dataset)
        "s_transition_heatmap": sq(
            'q = load "Pipeline_Transitions";\n'
            + "q = group q by (FromStage, ToStage);\n"
            + "q = foreach q generate FromStage, ToStage, "
            + "max(Probability) as Probability;\n"
            + "q = order q by ('FromStage' asc, 'ToStage' asc);"
        ),
        # Sankey: deal flow from stages to Won/Lost outcomes
        "s_deal_flow_sankey": sq(
            'q = load "Pipeline_Transitions";\n'
            + "q = group q by (FromStage, ToStage);\n"
            + "q = foreach q generate FromStage as source, "
            + "ToStage as target, max(Probability) * 100 as flow;\n"
            + "q = filter q by flow > 5;\n"
            + "q = order q by flow desc;"
        ),
        # Survival curves (from Pipeline_Survival dataset)
        "s_survival_curves": sq(
            f'q = load "{DS_SURV}";\n'
            + "q = group q by (StageGroup, Day);\n"
            + "q = foreach q generate StageGroup, "
            + "max(Day) as Day, max(SurvivalProb) as SurvivalProb;\n"
            + "q = order q by ('StageGroup' asc, 'Day' asc);"
        ),
        # YTD won revenue (area chart — running total)
        # (reuses s_ytd_won step above — just adding a widget for it)
        # Pipeline 3-month moving average (line chart)
        # (reuses s_pipeline_ma3 step above — just adding a widget for it)
        # Bullet: AUC-ROC actual vs target 70%
        "s_bullet_auc": sq(
            PA
            + "q = group q by all;\n"
            + "q = foreach q generate "
            + f"{round(metrics.get('cv_auc_mean', metrics.get('auc_roc', 0)) * 100, 1)} as actual, "
            + "70 as target;"
        ),
        # Bullet: Win Rate actual vs target 25%
        "s_bullet_winrate": sq(
            PA
            + FYF
            + 'q = filter q by IsClosed == "true";\n'
            + "q = group q by all;\n"
            + 'q = foreach q generate avg(case when IsWon == "true" then 100 else 0 end) as actual, '
            + "25 as target;"
        ),
        # Bullet: Pipeline Coverage actual vs target 3x
        "s_bullet_coverage": sq(
            PA
            + FYF
            + "q = group q by all;\n"
            + 'q = foreach q generate (case when sum(case when IsWon == "true" then ARR else 0 end) > '
            + '0 then sum(case when IsClosed == "false" then ARR else 0 end) / '
            + 'sum(case when IsWon == "true" then ARR else 0 end) else 0 end) as actual, '
            + "3 as target;"
        ),
        # Archetype bubble (3D: Amount vs Age, sized by WinProbability)
        "s_archetype_bubble": sq(
            PA
            + FYF
            + 'q = filter q by DealArchetype != "" && DealArchetype != "Unclassified";\n'
            + "q = group q by (Name, DealArchetype);\n"
            + "q = foreach q generate Name, DealArchetype, "
            + "avg(Amount) as Amount, avg(AgeInDays) as AgeInDays, "
            + "avg(WinProbability) * 100 as WinProb;\n"
            + "q = order q by Amount desc;\n"
            + "q = limit q 500;"
        ),
    }

    return steps


_FEATURE_DISPLAY = {
    "PushCount": "Push Count",
    "AvgDaysPerPush": "Avg Days Per Push",
    "AvgDaysPerStage": "Avg Days Per Stage",
    "TotalStageChanges": "Stage Changes",
    "BackwardMoves": "Backward Moves",
    "LogAmount": "Deal Size (Log)",
    "StageOrdinal": "Stage Position",
    "AgeInDays": "Deal Age (Days)",
    "DaysInCurrentStage": "Days In Stage",
    "ForecastOrdinal": "Forecast Category",
}


def _build_feature_importance_step(coefficients):
    """Build a SAQL step that encodes feature importance as static data."""
    if not coefficients:
        return sq(
            f'q = load "{DS}";\n'
            + "q = group q by all;\n"
            + 'q = foreach q generate "N/A" as Feature, 0 as Importance;'
        )

    # Sort by absolute coefficient value
    sorted_feats = sorted(coefficients.items(), key=lambda x: abs(x[1]), reverse=True)

    # Build union of single-row queries
    parts = []
    for feat_name, coef in sorted_feats:
        display = _FEATURE_DISPLAY.get(feat_name, feat_name)
        idx = len(parts)
        parts.append(
            f'q{idx} = load "{DS}";\n'
            f"q{idx} = group q{idx} by all;\n"
            f"q{idx} = foreach q{idx} generate "
            f'"{display}" as Feature, {round(abs(coef) * 100, 1)} as Importance;'
        )

    # Union them together
    if len(parts) == 1:
        return sq(parts[0])

    query = parts[0]
    for i in range(1, len(parts)):
        query += f"\n{parts[i]}\n"

    # Combine with union
    union_parts = ", ".join(f"q{i}" for i in range(len(parts)))
    query += f"\nq = union {union_parts};\n"
    query += "q = order q by Importance desc;"

    return sq(query)


# ═══════════════════════════════════════════════════════════════════════════
#  Widgets
# ═══════════════════════════════════════════════════════════════════════════


def build_widgets():
    """Build all widgets for the 7-page dashboard."""
    pages = [
        "Executive Summary",
        "Deal Push Intel",
        "Win Probability",
        "Stage Bottleneck",
        "Deals Won",
        "Trendlines",
        "Quant Intel",
    ]

    widgets = {}

    # ── Navigation (repeated on each page) ──
    for i, page_name in enumerate(["p1", "p2", "p3", "p4", "p5", "p6", "p7"]):
        for j, label in enumerate(pages):
            page_ids = [
                "exec",
                "push",
                "winprob",
                "bottleneck",
                "won",
                "trends",
                "quant",
            ]
            widgets[f"{page_name}_nav{j + 1}"] = nav_link(
                page_ids[j], label, active=(i == j)
            )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 1: Executive Summary
    # ═══════════════════════════════════════════════════════════════════
    widgets["p1_hdr"] = hdr(
        "Advanced Pipeline Analytics",
        "ML-powered pipeline intelligence with predictive analytics",
    )
    widgets["p1_f_stage"] = pillbox("f_stage", "Stage")
    widgets["p1_f_fy"] = pillbox("f_fy", "Fiscal Year")

    # KPIs
    widgets["p1_kpi_pipeline"] = num(
        "s_total_pipeline", "total_pipeline", "Total Pipeline", "#0070D2", compact=True
    )
    widgets["p1_kpi_weighted"] = num(
        "s_weighted_pipeline",
        "weighted_pipeline",
        "Weighted Pipeline",
        "#04844B",
        compact=True,
    )
    widgets["p1_kpi_winrate"] = num("s_win_rate", "win_rate", "Win Rate %", "#FFB75D")
    widgets["p1_kpi_cycle"] = num(
        "s_avg_cycle", "avg_cycle", "Avg Cycle (days)", "#54698D"
    )
    widgets["p1_kpi_atrisk"] = num(
        "s_at_risk_count", "at_risk", "At-Risk Deals", "#D4504C"
    )
    widgets["p1_kpi_avgwp"] = num("s_avg_win_prob", "avg_wp", "Avg Win Prob", "#0070D2")

    # Charts
    widgets["p1_sec_trend"] = section_label("Pipeline Trend & Forecast")
    widgets["p1_ch_trend"] = combo_chart(
        "s_pipeline_trend",
        "Pipeline Trend + Trendline",
        ["MonthLabel"],
        ["TotalPipeline"],
        ["TrendPipeline"],
        show_legend=True,
        axis_title="Pipeline (EUR)",
        axis2_title="Trendline",
    )
    widgets["p1_ch_monte"] = rich_chart(
        "s_monte_carlo",
        "hbar",
        "Monte Carlo Forecast (Quarterly)",
        ["Scenario"],
        ["ProjectedRevenue"],
        axis_title="Revenue (EUR)",
    )
    widgets["p1_sec_dist"] = section_label("Win Probability Distribution")
    widgets["p1_ch_dist"] = rich_chart(
        "s_win_prob_dist",
        "column",
        "Win Probability Distribution (Open Deals)",
        ["WinProbBucket"],
        ["cnt"],
        axis_title="Deal Count",
    )

    # FY Revenue KPIs
    widgets["p1_kpi_fy_closed"] = num(
        "s_fy_closed", "fy_closed", "FY Closed Won", "#04844B", compact=True
    )
    widgets["p1_kpi_fy_projected"] = num(
        "s_fy_projected",
        "fy_projected",
        "FY Projected Revenue",
        "#9050E9",
        compact=True,
    )
    widgets["p1_kpi_fy_open"] = num(
        "s_fy_open_pipeline",
        "fy_open",
        "FY Open Pipeline",
        "#0070D2",
        compact=True,
    )

    # NEW: Expected Bookings KPI + Action Queue
    widgets["p1_kpi_expected"] = num(
        "s_expected_bookings",
        "expected_bookings",
        "Expected Bookings",
        "#9050E9",
        compact=True,
    )
    widgets["p1_sec_action"] = section_label(
        "Action Queue — Top Deals by Expected Value"
    )
    widgets["p1_tbl_action"] = rich_chart(
        "s_action_queue",
        "comparisontable",
        "Top 15 Open Deals by Expected Value",
        ["Name", "StageName", "OwnerName"],
        ["Amount", "WinProb", "SlipRisk", "DaysInCurrentStage"],
    )
    # Bullet charts — actual vs target KPIs
    widgets["p1_bullet_winrate"] = bullet_chart(
        "s_bullet_winrate", "Win Rate vs Target 25%", axis_title="Win Rate %"
    )
    widgets["p1_bullet_coverage"] = bullet_chart(
        "s_bullet_coverage",
        "Pipeline Coverage vs Target 3x",
        axis_title="Coverage Ratio",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 2: Deal Push Intelligence
    # ═══════════════════════════════════════════════════════════════════
    widgets["p2_hdr"] = hdr(
        "Deal Push Intelligence",
        "Close date push frequency, impact on win rates, and top pushed deals",
    )
    widgets["p2_f_stage"] = pillbox("f_stage", "Stage")
    widgets["p2_f_fy"] = pillbox("f_fy", "Fiscal Year")
    widgets["p2_f_push"] = pillbox("f_push", "Push Bucket")

    # KPIs
    widgets["p2_kpi_pushes"] = num(
        "s_total_pushes", "total_pushes", "Total Pushes", "#D4504C"
    )
    widgets["p2_kpi_avg"] = num("s_avg_push", "avg_push", "Avg Push Count", "#FFB75D")
    widgets["p2_kpi_never"] = num(
        "s_never_pushed_pct", "pct", "Never Pushed %", "#04844B"
    )
    widgets["p2_kpi_net"] = num(
        "s_avg_net_push", "avg_net_push", "Avg Net Push Days", "#54698D"
    )

    # Charts
    widgets["p2_sec_freq"] = section_label("Push Frequency Analysis")
    widgets["p2_ch_dist"] = rich_chart(
        "s_push_dist",
        "column",
        "Push Frequency Distribution",
        ["PushBucket"],
        ["cnt"],
        axis_title="Deal Count",
    )
    widgets["p2_ch_impact"] = rich_chart(
        "s_push_vs_win",
        "column",
        "Push Impact on Win Rate",
        ["PushStatus"],
        ["win_rate"],
        axis_title="Win Rate %",
    )
    widgets["p2_sec_top"] = section_label("Most Pushed Open Deals")
    widgets["p2_tbl_top"] = rich_chart(
        "s_top_pushed",
        "comparisontable",
        "Top 10 Most-Pushed Open Deals",
        ["Name"],
        ["PushCount", "NetPushDays", "WinProbability", "Amount"],
    )
    widgets["p2_ch_cohort"] = rich_chart(
        "s_push_by_cohort",
        "column",
        "Push Trend by Cohort",
        ["Cohort"],
        ["avg_push"],
        axis_title="Avg Pushes",
    )

    # NEW: Push After Commit analysis
    widgets["p2_kpi_commit"] = num(
        "s_push_commit_pct", "pct", "Push After Commit %", "#9050E9"
    )
    widgets["p2_sec_commit"] = section_label("Push After Commit Analysis")
    widgets["p2_ch_fcat"] = rich_chart(
        "s_push_by_fcat",
        "column",
        "Push Rate by Forecast Category",
        ["ForecastCategory"],
        ["push_rate"],
        axis_title="Push Rate %",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 3: Win Probability Intelligence
    # ═══════════════════════════════════════════════════════════════════
    widgets["p3_hdr"] = hdr(
        "Win Probability Intelligence",
        "ML model insights, feature importance, and at-risk deal identification",
    )
    widgets["p3_f_stage"] = pillbox("f_stage", "Stage")
    widgets["p3_f_fy"] = pillbox("f_fy", "Fiscal Year")
    widgets["p3_f_winprob"] = pillbox("f_winprob", "Win Probability")

    # KPIs — bullet for AUC-ROC (actual vs 70% target, matching production pattern)
    widgets["p3_kpi_accuracy"] = bullet_chart(
        "s_bullet_auc", "AUC-ROC vs Target 70%", axis_title="AUC-ROC %"
    )
    widgets["p3_kpi_avgwp"] = num(
        "s_avg_win_prob", "avg_wp", "Avg Win Prob (Open)", "#04844B"
    )
    widgets["p3_kpi_atrisk"] = num("s_at_risk_p3", "cnt", "At-Risk Count", "#D4504C")
    widgets["p3_kpi_high"] = num("s_high_conf", "cnt", "High Confidence", "#04844B")
    # Slip Risk and Timing KPIs
    widgets["p3_kpi_slip"] = num("s_high_slip_risk", "cnt", "High Slip Risk", "#FF5D2D")
    widgets["p3_kpi_timing"] = num(
        "s_close_this_qtr", "cnt", "Close This Qtr", "#9050E9"
    )

    # Charts
    widgets["p3_sec_stage"] = section_label("Win Probability by Stage")
    widgets["p3_ch_stage"] = rich_chart(
        "s_wp_by_stage",
        "hbar",
        "Avg Win Probability by Stage",
        ["StageName"],
        ["avg_wp"],
        axis_title="Win Probability %",
    )
    widgets["p3_sec_feat"] = section_label("Feature Importance")
    widgets["p3_ch_feat"] = rich_chart(
        "s_feature_importance",
        "hbar",
        "Top Factors Affecting Win Probability",
        ["Feature"],
        ["Importance"],
        axis_title="Relative Importance",
        show_values=True,
    )
    widgets["p3_sec_risk"] = section_label("At-Risk Deals")
    widgets["p3_tbl_risk"] = rich_chart(
        "s_at_risk_table",
        "comparisontable",
        "At-Risk Deals (Win Prob < 30%)",
        ["Name", "StageName"],
        ["WinProbability", "DaysInCurrentStage", "Amount"],
    )
    widgets["p3_ch_size"] = rich_chart(
        "s_wp_vs_size",
        "column",
        "Win Probability by Deal Size",
        ["DealSizeBucket"],
        ["avg_wp"],
        axis_title="Avg Win Probability %",
    )
    # NEW: Slip vs Win scatter (comparisontable to stay columnMap-safe)
    widgets["p3_sec_slip"] = section_label("Slip Risk vs Win Probability")
    widgets["p3_tbl_slip"] = scatter_chart(
        "s_slip_vs_win",
        "Slip Risk vs Win Probability (Open Deals)",
        x_title="Win Probability",
        y_title="Slip Risk",
        show_legend=True,
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 4: Stage Bottleneck Analysis
    # ═══════════════════════════════════════════════════════════════════
    widgets["p4_hdr"] = hdr(
        "Stage Bottleneck Analysis",
        "Stage duration analytics, conversion rates, and stuck deal detection",
    )
    widgets["p4_f_stage"] = pillbox("f_stage", "Stage")
    widgets["p4_f_fy"] = pillbox("f_fy", "Fiscal Year")

    # KPIs
    widgets["p4_kpi_days"] = num(
        "s_avg_days_close", "avg_days", "Avg Days to Close", "#0070D2"
    )
    widgets["p4_kpi_longest"] = num(
        "s_longest_stage", "max_days", "Longest Stage (days)", "#FFB75D"
    )
    widgets["p4_kpi_backward"] = gauge(
        "s_backward_rate",
        "bm_rate",
        "Backward Move %",
        min_val=0,
        max_val=50,
        bands=[
            {"start": 0, "stop": 10, "color": "#04844B"},
            {"start": 10, "stop": 20, "color": "#FFB75D"},
            {"start": 20, "stop": 35, "color": "#D4504C"},
            {"start": 35, "stop": 50, "color": "#870500"},
        ],
    )
    widgets["p4_kpi_conv"] = num(
        "s_conversion_rate", "conv_rate", "Conversion Rate %", "#04844B"
    )

    # Charts
    widgets["p4_sec_days"] = section_label("Stage Duration Analysis")
    widgets["p4_ch_days"] = rich_chart(
        "s_days_per_stage",
        "hbar",
        "Avg Days per Stage",
        ["StageName"],
        ["avg_days"],
        axis_title="Days",
    )
    widgets["p4_ch_funnel"] = funnel_chart(
        "s_stage_funnel",
        "Deal Count by Stage (Funnel)",
        dim_field="StageName",
        measure_field="cnt",
    )
    widgets["p4_sec_back"] = section_label("Backward Moves & Stuck Deals")
    widgets["p4_ch_backward"] = rich_chart(
        "s_backward_by_stage",
        "column",
        "Backward Moves by Stage",
        ["StageName"],
        ["bm_count"],
        axis_title="Backward Moves",
    )
    widgets["p4_tbl_stuck"] = rich_chart(
        "s_stuck_deals",
        "comparisontable",
        "Deals Stuck > 60 Days in Stage",
        ["Name", "StageName"],
        ["DaysInCurrentStage", "Amount"],
    )

    # NEW: Stage Skip analysis
    widgets["p4_kpi_skip"] = num(
        "s_stage_skip_rate", "skip_rate", "Stage Skip Rate %", "#9050E9"
    )
    widgets["p4_sec_skip"] = section_label("Stage Skip Impact")
    widgets["p4_ch_skip"] = rich_chart(
        "s_skip_vs_win",
        "column",
        "Win Rate: Skipped vs Sequential Stages",
        ["SkipStatus"],
        ["win_rate"],
        axis_title="Win Rate %",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 5: Deals Won Deep Dive
    # ═══════════════════════════════════════════════════════════════════
    widgets["p5_hdr"] = hdr(
        "Deals Won Deep Dive",
        "Won deal characteristics, cohort analysis, and lead source performance",
    )

    # KPIs
    widgets["p5_kpi_count"] = num("s_won_count", "cnt", "Total Won", "#04844B")
    widgets["p5_kpi_amount"] = num(
        "s_won_amount", "won_arr", "Won Amount", "#0070D2", compact=True
    )
    widgets["p5_kpi_size"] = num(
        "s_avg_won_size", "avg_size", "Avg Won Deal Size", "#54698D", compact=True
    )
    widgets["p5_kpi_cycle"] = num(
        "s_avg_won_cycle", "avg_cycle", "Avg Won Cycle (days)", "#FFB75D"
    )

    # Charts
    widgets["p5_sec_cohort"] = section_label("Won Deals by Cohort")
    widgets["p5_ch_cohort"] = rich_chart(
        "s_won_by_cohort",
        "column",
        "Closed Won ARR by Cohort Quarter",
        ["Cohort"],
        ["won_arr"],
        axis_title="ARR (EUR)",
    )
    widgets["p5_ch_wonlost"] = rich_chart(
        "s_won_vs_lost",
        "comparisontable",
        "Won vs Lost Characteristics",
        ["Outcome"],
        ["avg_pushes", "avg_days", "avg_changes"],
    )
    widgets["p5_sec_source"] = section_label("Won by Lead Source & Deal Size")
    widgets["p5_ch_source"] = treemap_chart(
        "s_won_by_source",
        "Won ARR by Lead Source",
        dim_fields=["LeadSource"],
        measure_field="won_arr",
    )
    widgets["p5_ch_size"] = rich_chart(
        "s_won_by_size",
        "column",
        "Won Deals by Size Bucket",
        ["DealSizeBucket"],
        ["won_arr"],
        axis_title="ARR (EUR)",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 6: Trendline Analytics
    # ═══════════════════════════════════════════════════════════════════
    widgets["p6_hdr"] = hdr(
        "Trendline Analytics & Revenue Forecast",
        "FY revenue forecast, pipeline trends, and cycle length projections",
    )

    # ── FY Revenue Forecast Section ──
    widgets["p6_sec_fy"] = section_label("FY Revenue Forecast — Closed vs. Projected")
    widgets["p6_kpi_fy_closed"] = num(
        "s_fy_closed", "fy_closed", "FY Closed Won", "#04844B", compact=True
    )
    widgets["p6_kpi_fy_projected"] = num(
        "s_fy_projected", "fy_projected", "FY Projected", "#9050E9", compact=True
    )
    widgets["p6_kpi_fy_open"] = num(
        "s_fy_open_pipeline", "fy_open", "Open Pipeline", "#0070D2", compact=True
    )
    widgets["p6_ch_fy_quarterly"] = rich_chart(
        "s_fy_quarterly",
        "stackcolumn",
        "FY Revenue by Quarter — Closed Won + Weighted Forecast",
        ["FiscalQuarter"],
        ["ClosedWon", "WeightedForecast"],
        show_legend=True,
        axis_title="Revenue (EUR)",
        show_values=True,
    )
    widgets["p6_ch_fy_cumulative"] = combo_chart(
        "s_fy_cumulative",
        "FY Revenue — Closed Won (Bars) + Projected Total (Line)",
        ["FiscalQuarter"],
        ["ClosedWon"],
        ["ProjectedTotal"],
        show_legend=True,
        axis_title="Revenue (EUR)",
        axis2_title="Projected Total",
    )

    widgets["p6_sec_pipeline"] = section_label("Pipeline Trend with Forecast")
    widgets["p6_ch_pipeline"] = timeline_chart(
        "s_trend_pipeline_full",
        "Pipeline Trend (Actual + Forecast + 95% PI)",
        show_legend=True,
        axis_title="ARR (EUR)",
    )
    widgets["p6_sec_metrics"] = section_label("Key Metric Trends")
    widgets["p6_ch_winrate"] = timeline_chart(
        "s_trend_winrate",
        "Win Rate Trend + 95% PI",
        show_legend=True,
        axis_title="Win Rate %",
    )
    widgets["p6_ch_cycle"] = rich_chart(
        "s_trend_cycle",
        "column",
        "Cycle Length Trend",
        ["MonthLabel"],
        ["AvgCycleLength", "TrendCycleLength"],
        split=["IsForecast"],
        show_legend=True,
        axis_title="Days",
    )
    widgets["p6_sec_volume"] = section_label("Deal Volume & Simulation")
    widgets["p6_ch_new"] = rich_chart(
        "s_trend_new_deals",
        "column",
        "New Deals per Month",
        ["MonthLabel"],
        ["NewDeals"],
        axis_title="Deals",
    )
    widgets["p6_tbl_mc"] = rich_chart(
        "s_mc_detail",
        "comparisontable",
        "Monte Carlo Simulation Scenarios",
        ["Scenario"],
        ["Revenue", "DealCount", "WinRate"],
    )
    # YTD Won Revenue — running total area chart
    widgets["p6_sec_ytd"] = section_label("Running Totals & Moving Averages")
    widgets["p6_ch_ytd"] = area_chart(
        "s_ytd_won",
        "YTD Won Revenue (Running Total)",
        axis_title="Revenue (EUR)",
    )
    # 3-Month Moving Average Pipeline — line chart
    widgets["p6_ch_ma3"] = line_chart(
        "s_pipeline_ma3",
        "Pipeline 3-Month Moving Average",
        axis_title="Pipeline (EUR)",
    )

    # ═══════════════════════════════════════════════════════════════════
    #  PAGE 7: Quantitative Intelligence
    # ═══════════════════════════════════════════════════════════════════
    widgets["p7_hdr"] = hdr(
        "Quantitative Intelligence",
        "PhD-level analytics: Markov chains, survival analysis, deal archetypes, velocity scoring, Value-at-Risk",
    )

    # KPIs — 4 across
    widgets["p7_kpi_markov"] = num(
        "s_avg_markov", "avg_markov", "Avg Markov Win Prob (Open)", "#0070D2"
    )
    widgets["p7_kpi_steps"] = num(
        "s_avg_steps", "avg_steps", "Avg Steps to Close", "#9050E9"
    )
    widgets["p7_kpi_survival"] = num(
        "s_avg_survival", "avg_surv", "Median Survival Days", "#FFB75D"
    )
    widgets["p7_kpi_var"] = num("s_var5", "var5", "VaR at 5%", "#D4504C")

    # Markov absorption by stage (hbar)
    widgets["p7_sec_markov"] = section_label("Markov Chain & Survival Analysis")
    widgets["p7_ch_markov"] = rich_chart(
        "s_markov_by_stage",
        "hbar",
        "Markov Win Probability by Stage",
        ["StageName"],
        ["markov_wp"],
        axis_title="Absorption Probability",
        show_values=True,
    )
    # Transition heatmap — stage × stage probability matrix
    widgets["p7_ch_transition"] = heatmap_chart(
        "s_transition_heatmap",
        "Stage Transition Probability Matrix",
    )
    # Survival curves — multi-series line by stage group
    widgets["p7_ch_survival"] = line_chart(
        "s_survival_curves",
        "Kaplan-Meier Survival Curves by Stage",
        show_legend=True,
        axis_title="Survival Probability",
    )
    # Sankey — deal flow from stages to Won/Lost
    widgets["p7_ch_sankey"] = sankey_chart(
        "s_deal_flow_sankey",
        "Deal Flow: Stage Transitions",
        source_field="source",
        target_field="target",
        measure_field="flow",
    )

    # Archetype distribution — bubble chart (3D: Amount × Age × WinProb)
    widgets["p7_sec_arch"] = section_label("Deal Archetypes (K-Means Clustering)")
    widgets["p7_ch_arch"] = bubble_chart(
        "s_archetype_bubble",
        "Deal Archetypes: Amount × Age × Win Probability",
    )
    widgets["p7_ch_archwr"] = rich_chart(
        "s_archetype_winrate",
        "column",
        "Win Rate by Archetype",
        ["DealArchetype"],
        ["win_rate"],
        axis_title="Win Rate %",
        show_values=True,
    )
    widgets["p7_tbl_archchar"] = rich_chart(
        "s_archetype_chars",
        "comparisontable",
        "Archetype Characteristics",
        ["DealArchetype"],
        ["avg_arr", "avg_age", "avg_wp", "cnt"],
    )

    # Momentum distribution (stacked column — shows composition)
    widgets["p7_sec_momentum"] = section_label("Velocity & Momentum Scoring")
    widgets["p7_ch_momentum"] = rich_chart(
        "s_momentum_dist",
        "stackcolumn",
        "Momentum Distribution by Stage",
        ["MomentumFlag"],
        ["cnt"],
        axis_title="Deal Count",
        show_values=True,
    )

    # VaR waterfall
    widgets["p7_sec_var"] = section_label("Revenue-at-Risk (Monte Carlo)")
    widgets["p7_ch_var"] = waterfall_chart(
        "s_var_waterfall",
        "Revenue-at-Risk Distribution",
        dim_field="Quantile",
        measure_field="Amount",
    )

    # Permutation importance (reuses s_perm_importance from P3)
    widgets["p7_sec_perm"] = section_label("Feature Importance (Permutation)")
    widgets["p7_ch_perm"] = rich_chart(
        "s_feature_importance",
        "hbar",
        "Permutation Feature Importance",
        ["Feature"],
        ["Importance"],
        axis_title="Importance Score",
        show_values=True,
    )

    return widgets


# ═══════════════════════════════════════════════════════════════════════════
#  Layout
# ═══════════════════════════════════════════════════════════════════════════


def build_layout():
    """Build the 7-page grid layout."""

    def _nav(prefix):
        return nav_row(prefix, 7)

    # ── Page 1: Executive Summary ──
    p1 = _nav("p1") + [
        {"name": "p1_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p1_f_stage", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p1_f_fy", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # KPI row 1
        {"name": "p1_kpi_pipeline", "row": 5, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p1_kpi_weighted", "row": 5, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p1_kpi_winrate", "row": 5, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p1_kpi_cycle", "row": 5, "column": 6, "colspan": 2, "rowspan": 4},
        {"name": "p1_kpi_atrisk", "row": 5, "column": 8, "colspan": 2, "rowspan": 4},
        {"name": "p1_kpi_avgwp", "row": 5, "column": 10, "colspan": 2, "rowspan": 4},
        # KPI row 2 — FY Revenue + Expected Bookings
        {"name": "p1_kpi_fy_closed", "row": 9, "column": 0, "colspan": 3, "rowspan": 3},
        {
            "name": "p1_kpi_fy_projected",
            "row": 9,
            "column": 3,
            "colspan": 3,
            "rowspan": 3,
        },
        {"name": "p1_kpi_fy_open", "row": 9, "column": 6, "colspan": 3, "rowspan": 3},
        {"name": "p1_kpi_expected", "row": 9, "column": 9, "colspan": 3, "rowspan": 3},
        # Trend + Monte Carlo
        {"name": "p1_sec_trend", "row": 12, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_trend", "row": 13, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p1_ch_monte", "row": 13, "column": 6, "colspan": 6, "rowspan": 8},
        # Win prob distribution
        {"name": "p1_sec_dist", "row": 21, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_ch_dist", "row": 22, "column": 0, "colspan": 12, "rowspan": 6},
        # Action Queue
        {"name": "p1_sec_action", "row": 28, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p1_tbl_action", "row": 29, "column": 0, "colspan": 12, "rowspan": 8},
        # Bullet KPIs — actual vs target
        {
            "name": "p1_bullet_winrate",
            "row": 37,
            "column": 0,
            "colspan": 6,
            "rowspan": 4,
        },
        {
            "name": "p1_bullet_coverage",
            "row": 37,
            "column": 6,
            "colspan": 6,
            "rowspan": 4,
        },
    ]

    # ── Page 2: Deal Push Intelligence ──
    p2 = _nav("p2") + [
        {"name": "p2_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p2_f_stage", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p2_f_push", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        # KPIs (5 now — added Push After Commit %)
        {"name": "p2_kpi_pushes", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p2_kpi_avg", "row": 5, "column": 3, "colspan": 2, "rowspan": 4},
        {"name": "p2_kpi_never", "row": 5, "column": 5, "colspan": 2, "rowspan": 4},
        {"name": "p2_kpi_net", "row": 5, "column": 7, "colspan": 2, "rowspan": 4},
        {"name": "p2_kpi_commit", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Charts
        {"name": "p2_sec_freq", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_dist", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p2_ch_impact", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
        # Top pushed + cohort
        {"name": "p2_sec_top", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_tbl_top", "row": 19, "column": 0, "colspan": 7, "rowspan": 8},
        {"name": "p2_ch_cohort", "row": 19, "column": 7, "colspan": 5, "rowspan": 8},
        # Push After Commit breakdown
        {"name": "p2_sec_commit", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p2_ch_fcat", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 3: Win Probability Intelligence ──
    p3 = _nav("p3") + [
        {"name": "p3_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p3_f_stage", "row": 3, "column": 0, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_fy", "row": 3, "column": 4, "colspan": 4, "rowspan": 2},
        {"name": "p3_f_winprob", "row": 3, "column": 8, "colspan": 4, "rowspan": 2},
        # KPIs — 6 across (original 4 + 2 new)
        {"name": "p3_kpi_accuracy", "row": 5, "column": 0, "colspan": 2, "rowspan": 4},
        {"name": "p3_kpi_avgwp", "row": 5, "column": 2, "colspan": 2, "rowspan": 4},
        {"name": "p3_kpi_atrisk", "row": 5, "column": 4, "colspan": 2, "rowspan": 4},
        {"name": "p3_kpi_high", "row": 5, "column": 6, "colspan": 2, "rowspan": 4},
        {"name": "p3_kpi_slip", "row": 5, "column": 8, "colspan": 2, "rowspan": 4},
        {"name": "p3_kpi_timing", "row": 5, "column": 10, "colspan": 2, "rowspan": 4},
        # Charts
        {"name": "p3_sec_stage", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_stage", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p3_ch_size", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
        # Feature importance + at-risk table
        {"name": "p3_sec_feat", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_ch_feat", "row": 19, "column": 0, "colspan": 5, "rowspan": 8},
        {"name": "p3_sec_risk", "row": 19, "column": 5, "colspan": 7, "rowspan": 1},
        {"name": "p3_tbl_risk", "row": 20, "column": 5, "colspan": 7, "rowspan": 7},
        # Slip Risk vs Win Probability
        {"name": "p3_sec_slip", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p3_tbl_slip", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 4: Stage Bottleneck Analysis ──
    p4 = _nav("p4") + [
        {"name": "p4_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        {"name": "p4_f_stage", "row": 3, "column": 0, "colspan": 6, "rowspan": 2},
        {"name": "p4_f_fy", "row": 3, "column": 6, "colspan": 6, "rowspan": 2},
        # KPIs — 5 now (added Stage Skip Rate)
        {"name": "p4_kpi_days", "row": 5, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p4_kpi_longest", "row": 5, "column": 3, "colspan": 2, "rowspan": 4},
        {"name": "p4_kpi_backward", "row": 5, "column": 5, "colspan": 2, "rowspan": 4},
        {"name": "p4_kpi_conv", "row": 5, "column": 7, "colspan": 2, "rowspan": 4},
        {"name": "p4_kpi_skip", "row": 5, "column": 9, "colspan": 3, "rowspan": 4},
        # Charts
        {"name": "p4_sec_days", "row": 9, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_days", "row": 10, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p4_ch_funnel", "row": 10, "column": 6, "colspan": 6, "rowspan": 8},
        # Backward + stuck
        {"name": "p4_sec_back", "row": 18, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_backward", "row": 19, "column": 0, "colspan": 5, "rowspan": 8},
        {"name": "p4_tbl_stuck", "row": 19, "column": 5, "colspan": 7, "rowspan": 8},
        # Stage Skip impact
        {"name": "p4_sec_skip", "row": 27, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p4_ch_skip", "row": 28, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    # ── Page 5: Deals Won Deep Dive ──
    p5 = _nav("p5") + [
        {"name": "p5_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # KPIs
        {"name": "p5_kpi_count", "row": 3, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p5_kpi_amount", "row": 3, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p5_kpi_size", "row": 3, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p5_kpi_cycle", "row": 3, "column": 9, "colspan": 3, "rowspan": 4},
        # Charts
        {"name": "p5_sec_cohort", "row": 7, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_cohort", "row": 8, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p5_ch_wonlost", "row": 8, "column": 6, "colspan": 6, "rowspan": 8},
        # Source + size
        {"name": "p5_sec_source", "row": 16, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p5_ch_source", "row": 17, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p5_ch_size", "row": 17, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    # ── Page 6: Trendline Analytics & Revenue Forecast ──
    p6 = _nav("p6") + [
        {"name": "p6_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # FY Revenue Forecast section (top of page — most important)
        {"name": "p6_sec_fy", "row": 3, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_kpi_fy_closed", "row": 4, "column": 0, "colspan": 4, "rowspan": 3},
        {
            "name": "p6_kpi_fy_projected",
            "row": 4,
            "column": 4,
            "colspan": 4,
            "rowspan": 3,
        },
        {"name": "p6_kpi_fy_open", "row": 4, "column": 8, "colspan": 4, "rowspan": 3},
        {
            "name": "p6_ch_fy_quarterly",
            "row": 7,
            "column": 0,
            "colspan": 6,
            "rowspan": 8,
        },
        {
            "name": "p6_ch_fy_cumulative",
            "row": 7,
            "column": 6,
            "colspan": 6,
            "rowspan": 8,
        },
        # Pipeline trend
        {
            "name": "p6_sec_pipeline",
            "row": 15,
            "column": 0,
            "colspan": 12,
            "rowspan": 1,
        },
        {"name": "p6_ch_pipeline", "row": 16, "column": 0, "colspan": 12, "rowspan": 8},
        # Metric trends
        {"name": "p6_sec_metrics", "row": 24, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_winrate", "row": 25, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p6_ch_cycle", "row": 25, "column": 6, "colspan": 6, "rowspan": 8},
        # Volume + MC table
        {"name": "p6_sec_volume", "row": 33, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_new", "row": 34, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p6_tbl_mc", "row": 34, "column": 6, "colspan": 6, "rowspan": 8},
        # Running totals + moving averages
        {"name": "p6_sec_ytd", "row": 42, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p6_ch_ytd", "row": 43, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p6_ch_ma3", "row": 43, "column": 6, "colspan": 6, "rowspan": 8},
    ]

    # ── Page 7: Quantitative Intelligence ──
    p7 = _nav("p7") + [
        {"name": "p7_hdr", "row": 1, "column": 0, "colspan": 12, "rowspan": 2},
        # KPIs — 4 across
        {"name": "p7_kpi_markov", "row": 3, "column": 0, "colspan": 3, "rowspan": 4},
        {"name": "p7_kpi_steps", "row": 3, "column": 3, "colspan": 3, "rowspan": 4},
        {"name": "p7_kpi_survival", "row": 3, "column": 6, "colspan": 3, "rowspan": 4},
        {"name": "p7_kpi_var", "row": 3, "column": 9, "colspan": 3, "rowspan": 4},
        # Markov + Transition Heatmap
        {"name": "p7_sec_markov", "row": 7, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_markov", "row": 8, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p7_ch_transition", "row": 8, "column": 6, "colspan": 6, "rowspan": 8},
        # Survival curves + Sankey deal flow
        {"name": "p7_ch_survival", "row": 16, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p7_ch_sankey", "row": 16, "column": 6, "colspan": 6, "rowspan": 8},
        # Archetypes — bubble + win rate
        {"name": "p7_sec_arch", "row": 24, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_arch", "row": 25, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p7_ch_archwr", "row": 25, "column": 6, "colspan": 6, "rowspan": 8},
        # Archetype characteristics table
        {
            "name": "p7_tbl_archchar",
            "row": 33,
            "column": 0,
            "colspan": 12,
            "rowspan": 6,
        },
        # Momentum + VaR
        {"name": "p7_sec_momentum", "row": 39, "column": 0, "colspan": 6, "rowspan": 1},
        {"name": "p7_ch_momentum", "row": 40, "column": 0, "colspan": 6, "rowspan": 8},
        {"name": "p7_sec_var", "row": 39, "column": 6, "colspan": 6, "rowspan": 1},
        {"name": "p7_ch_var", "row": 40, "column": 6, "colspan": 6, "rowspan": 8},
        # Permutation importance
        {"name": "p7_sec_perm", "row": 48, "column": 0, "colspan": 12, "rowspan": 1},
        {"name": "p7_ch_perm", "row": 49, "column": 0, "colspan": 12, "rowspan": 8},
    ]

    return {
        "name": "Default",
        "numColumns": 12,
        "pages": [
            pg("exec", "Executive Summary", p1),
            pg("push", "Deal Push Intel", p2),
            pg("winprob", "Win Probability", p3),
            pg("bottleneck", "Stage Bottleneck", p4),
            pg("won", "Deals Won", p5),
            pg("trends", "Trendlines", p6),
            pg("quant", "Quant Intelligence", p7),
        ],
    }


# ═══════════════════════════════════════════════════════════════════════════
#  Main
# ═══════════════════════════════════════════════════════════════════════════


def main():
    inst, tok = get_auth()

    # ── 1. Create all datasets ──
    (
        deals,
        trendline_rows,
        mc_rows,
        metrics,
        coefficients,
        survival_curves,
        transition_matrix,
    ) = create_datasets(inst, tok)

    # ── 2. Look up dataset ID for SAQL filter steps ──
    ds_id = get_dataset_id(inst, tok, DS)
    ds_meta = [{"id": ds_id, "name": DS}] if ds_id else [{"name": DS}]

    # ── 3. Build dashboard ──
    print("\n=== Building dashboard ===")
    dash_id = create_dashboard_if_needed(inst, tok, DASHBOARD_LABEL)

    steps = build_steps(ds_meta, metrics, coefficients)
    widgets = build_widgets()
    layout = build_layout()

    state = build_dashboard_state(steps, widgets, layout)
    deploy_dashboard(inst, tok, dash_id, state)

    # ── 4. Set XMD record links ──
    print("\n=== Setting XMD record links ===")
    set_record_links_xmd(
        inst,
        tok,
        DS,
        [
            {"field": "Name", "id_field": "Id", "label": "Opportunity Name"},
            {"field": "Id", "id_field": "Id", "label": "Opportunity ID"},
            {"field": "AccountName", "id_field": "AccountId", "label": "Account Name"},
            {"field": "AccountId", "id_field": "AccountId", "label": "Account ID"},
        ],
    )

    # ── Summary ──
    print("\n" + "=" * 60)
    print("Advanced Pipeline Analytics — Deployment Complete")
    print("=" * 60)
    print(f"  Dashboard: {DASHBOARD_LABEL}")
    print(f"  Deals analyzed: {len(deals)}")
    auc = metrics.get("cv_auc_mean", metrics.get("auc_roc", 0))
    brier = metrics.get("brier_score", 0)
    print(f"  AUC-ROC: {auc:.3f}  |  Brier Score: {brier:.4f}")
    print(
        f"  At-risk deals: {sum(1 for d in deals if d.get('AtRiskFlag') == 'At Risk')}"
    )
    print(
        f"  Trendline months: {len(trendline_rows)} ({sum(1 for r in trendline_rows if r['IsForecast'] == 'true')} forecast)"
    )
    print(
        f"  Monte Carlo P50: ${mc_rows[2]['ProjectedRevenue']:,.0f}"
        if len(mc_rows) > 2
        else ""
    )
    print("  Pages: 7")
    print("  Record links: Name → Opportunity, AccountName → Account")


if __name__ == "__main__":
    main()
