"""Track C — source-quality baseline loader, comparator, and calibrator.

Closes the GPT Pro v2/v3 review's "validators are partly theater" finding for
source-quality drift: each (requirement_id, territory, period_role) source can
carry a calibrated baseline (median row count, expected row range, per-field
null-rate baselines) that the extract audit step compares the live run against.

Track C scope (per ``docs/2026-04-25-gpt-pro-feedback-implementation-plan.md``):

* Read-only first. The comparator NEVER writes to disk and emits ``info``
  severity findings unless the source contract or baseline file explicitly
  opts up to ``warning``/``blocked``.
* Baselines live at ``config/source_quality_baselines/<baseline_key>.json`` and
  are only written by ``scripts/calibrate_source_quality_baselines.py`` when
  ``--promote-baselines`` is passed.
* Stage-mix and quarter-mix logic is intentionally NOT implemented here —
  Track D owns distribution audits and will consume the ``stage_mix_seed``
  scaffold this module exposes.
"""

from __future__ import annotations

import json
import re
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable, Literal

from pydantic import Field, field_validator

from scripts.monthly_platform.contracts import (
    ContractModel,
    Finding,
    FindingSeverity,
    utc_now_iso,
)
from scripts.monthly_platform.source_requirements import (
    BaselineDriftAction,
    PeriodRole,
    SourcePlanItem,
    baseline_drift_action_to_severity,
)


SCHEMA_VERSION = "monthly_platform.source_quality_baseline.v1"

# Default percentile envelope around the historical row-count median. These
# defaults assume small (n=1..3) sample sizes; the calibrator may widen them.
DEFAULT_LOW_FACTOR = 0.5
DEFAULT_HIGH_FACTOR = 2.0
DEFAULT_NULL_RATE_DELTA = 0.05  # absolute pct; baseline+0.05 → drift


class BaselineRowCount(ContractModel):
    """Calibrated row-count envelope for a baseline."""

    median: float
    p05: float
    p95: float
    min_observed: int
    max_observed: int
    expected_min: float | None = None
    expected_max: float | None = None
    sample_count: int
    allow_zero: bool = True

    @field_validator("sample_count")
    @classmethod
    def positive_sample_count(cls, value: int) -> int:
        if value < 1:
            raise ValueError("sample_count must be >= 1")
        return value


class BaselineNullRate(ContractModel):
    """Calibrated null-rate ceiling for a single required field."""

    baseline_pct: float
    max_pct: float
    samples: list[float] = Field(default_factory=list)

    @field_validator("baseline_pct", "max_pct")
    @classmethod
    def in_unit_interval(cls, value: float) -> float:
        if not 0.0 <= value <= 1.0:
            raise ValueError("null-rate values must be in [0, 1]")
        return value


class BaselinePolicy(ContractModel):
    """How the comparator should react when this baseline is breached.

    Track C default = ``info``: drift is surfaced in the audit but never blocks
    the release. Operators escalate to ``warning``/``blocked`` after the
    calibration period via ``--promote-baselines`` runs.
    """

    row_count_drift_action: BaselineDriftAction = "info"
    null_rate_drift_action: BaselineDriftAction = "info"
    row_count_low_factor: float = DEFAULT_LOW_FACTOR
    row_count_high_factor: float = DEFAULT_HIGH_FACTOR
    null_rate_abs_delta: float = DEFAULT_NULL_RATE_DELTA


class BaselineProvenance(ContractModel):
    """One observation that contributed to the calibrated baseline."""

    run_id: str
    snapshot_date: str
    quarter_label: str | None = None
    salesforce_id: str | None = None
    row_count: int


class SourceQualityBaseline(ContractModel):
    """Calibrated source-quality baseline for one (requirement, territory, period_role)."""

    schema_version: Literal["monthly_platform.source_quality_baseline.v1"] = (
        SCHEMA_VERSION
    )
    baseline_key: str
    requirement_id: str
    territory: str  # "global" for global-scope sources
    period_role: PeriodRole
    promoted_at: str
    promoted_from: list[BaselineProvenance]
    row_count: BaselineRowCount
    null_rates: dict[str, BaselineNullRate] = Field(default_factory=dict)
    policy: BaselinePolicy = Field(default_factory=BaselinePolicy)
    # Track D scaffolding: calibrator may emit a stage-mix seed without acting
    # on it. Comparator ignores this in Track C.
    stage_mix_seed: dict[str, float] | None = None
    notes: str = ""


# ---------------------------------------------------------------------------
# Key helpers
# ---------------------------------------------------------------------------


def _territory_slug(territory: str | None) -> str:
    """Normalize a territory name for use as a baseline-key path component.

    Lower-cases, replaces whitespace with ``_``, drops ``&``, and collapses
    repeated underscores. Conservative so the resulting key is filesystem-safe
    and stable across the live territory registry.
    """
    if not territory:
        return "global"
    cleaned = territory.strip().lower().replace("&", "and")
    parts = [chunk for chunk in re.split(r"[^a-z0-9]+", cleaned) if chunk]
    return "_".join(parts) if parts else "global"


def baseline_key_for_item(item: SourcePlanItem) -> str:
    """Stable baseline key from a SourcePlanItem.

    Drops ``quarter_label`` and ``salesforce_id`` so the key stays valid as the
    quarter rotates and as Salesforce report ids are reissued.
    """
    return f"{item.requirement_id}.{_territory_slug(item.territory)}.{item.period_role}"


def baseline_key_for_quality(quality: dict[str, Any]) -> str:
    """Stable baseline key from a source_extract_quality_audit ``sources[]`` entry."""
    return (
        f"{quality['requirement_id']}."
        f"{_territory_slug(quality.get('territory'))}."
        f"{quality['period_role']}"
    )


def baseline_filename(baseline_key: str) -> str:
    return f"{baseline_key}.json"


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def load_baselines(directory: Path) -> dict[str, SourceQualityBaseline]:
    """Load every ``*.json`` in ``directory`` as a baseline.

    Missing directory returns ``{}`` — the comparator treats absent baselines
    as a no-op (no findings emitted) so a fresh repo is not a release blocker.
    """
    if not directory.exists() or not directory.is_dir():
        return {}
    baselines: dict[str, SourceQualityBaseline] = {}
    for path in sorted(directory.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        baseline = SourceQualityBaseline.model_validate(payload)
        if baseline.baseline_key in baselines:
            raise ValueError(
                f"duplicate baseline_key={baseline.baseline_key} at {path}"
            )
        baselines[baseline.baseline_key] = baseline
    return baselines


# ---------------------------------------------------------------------------
# Comparator (read-only, never mutates inputs or files)
# ---------------------------------------------------------------------------


def _finding(
    *,
    severity: FindingSeverity,
    issue: str,
    quality: dict[str, Any],
    evidence: str,
) -> Finding:
    prefix = (
        f"{quality['requirement_id']} {quality.get('territory') or 'global'} "
        f"{quality['period_role']} {quality.get('source_type', '')} "
        f"{quality.get('salesforce_id', '')}"
    ).strip()
    return Finding(
        severity=severity,
        issue=issue,
        evidence=f"{prefix}: {evidence}",
    )


def _resolve_drift_action(
    *,
    baseline_action: BaselineDriftAction,
    contract_override: BaselineDriftAction | None,
) -> BaselineDriftAction:
    return contract_override if contract_override is not None else baseline_action


def _row_count_drift(
    *,
    quality: dict[str, Any],
    baseline: SourceQualityBaseline,
    contract_override: BaselineDriftAction | None,
) -> Finding | None:
    row_count = int(quality.get("row_count", 0))
    rc = baseline.row_count
    expected_min = rc.expected_min
    expected_max = rc.expected_max
    low_threshold = (
        expected_min * baseline.policy.row_count_low_factor
        if expected_min is not None
        else None
    )
    high_threshold = (
        expected_max * baseline.policy.row_count_high_factor
        if expected_max is not None
        else None
    )

    breach: str | None = None
    if not rc.allow_zero and row_count == 0:
        breach = "row_count_zero_disallowed"
    elif low_threshold is not None and row_count < low_threshold:
        breach = "row_count_below_low_threshold"
    elif high_threshold is not None and row_count > high_threshold:
        breach = "row_count_above_high_threshold"

    if breach is None:
        return None

    action = _resolve_drift_action(
        baseline_action=baseline.policy.row_count_drift_action,
        contract_override=contract_override,
    )
    severity = baseline_drift_action_to_severity(action)
    if severity is None:
        return None
    return _finding(
        severity=severity,
        issue=f"source_quality_baseline_{breach}",
        quality=quality,
        evidence=(
            f"row_count={row_count}; baseline_median={rc.median}; "
            f"expected_range=[{expected_min}, {expected_max}]; "
            f"thresholds=[{low_threshold}, {high_threshold}]; "
            f"action={action}"
        ),
    )


def _null_rate_drift(
    *,
    quality: dict[str, Any],
    baseline: SourceQualityBaseline,
) -> list[Finding]:
    findings: list[Finding] = []
    if not baseline.null_rates:
        return findings
    if int(quality.get("row_count", 0)) == 0:
        # No rows means null-rate is undefined; the row-count axis already
        # reports the underlying issue.
        return findings
    field_audits = quality.get("field_audits") or []
    audit_by_name = {audit["field_name"]: audit for audit in field_audits}
    delta = baseline.policy.null_rate_abs_delta
    action = baseline.policy.null_rate_drift_action
    severity = baseline_drift_action_to_severity(action)
    if severity is None:
        return findings
    for field_name, baseline_null in baseline.null_rates.items():
        audit = audit_by_name.get(field_name)
        if not audit or not audit.get("present"):
            continue
        observed = audit.get("null_pct")
        if observed is None:
            continue
        observed = float(observed)
        ceiling = max(baseline_null.max_pct, baseline_null.baseline_pct + delta)
        if observed <= ceiling:
            continue
        findings.append(
            _finding(
                severity=severity,
                issue="source_quality_baseline_null_rate_drift",
                quality=quality,
                evidence=(
                    f"field={field_name}; observed_null_pct={observed:.4f}; "
                    f"baseline_pct={baseline_null.baseline_pct:.4f}; "
                    f"max_pct={baseline_null.max_pct:.4f}; delta={delta:.4f}; "
                    f"action={action}"
                ),
            )
        )
    return findings


def compare_quality_to_baseline(
    *,
    quality: dict[str, Any],
    baseline: SourceQualityBaseline,
    contract_override: BaselineDriftAction | None = None,
) -> list[Finding]:
    """Compare one source's live quality payload to its baseline.

    Pure: never writes to disk, never mutates ``quality`` or ``baseline``.
    Returns the list of findings (possibly empty). Severity is decided by the
    baseline policy unless ``contract_override`` (from the source contract's
    ``RowCountPolicy.baseline_drift_action``) supersedes it.
    """
    findings: list[Finding] = []
    row_finding = _row_count_drift(
        quality=quality,
        baseline=baseline,
        contract_override=contract_override,
    )
    if row_finding is not None:
        findings.append(row_finding)
    findings.extend(_null_rate_drift(quality=quality, baseline=baseline))
    return findings


def compare_run_to_baselines(
    *,
    quality_audit: dict[str, Any],
    baselines: dict[str, SourceQualityBaseline],
    contract_overrides: dict[str, BaselineDriftAction | None] | None = None,
) -> tuple[list[Finding], dict[str, Any]]:
    """Run the comparator across an entire ``source_extract_quality_audit``.

    ``contract_overrides`` is keyed by ``baseline_key`` and resolved per-source
    from the corresponding ``SourcePlanItem.row_count_policy.baseline_drift_action``.
    Returns a list of findings and a per-source comparison report (used by the
    audit JSON and the calibrator CLI).
    """
    contract_overrides = contract_overrides or {}
    findings: list[Finding] = []
    comparisons: list[dict[str, Any]] = []
    matched = 0
    missing = 0
    for source in quality_audit.get("sources", []):
        key = baseline_key_for_quality(source)
        baseline = baselines.get(key)
        override = contract_overrides.get(key)
        if baseline is None:
            missing += 1
            comparisons.append(
                {
                    "baseline_key": key,
                    "status": "no_baseline",
                    "findings": [],
                }
            )
            continue
        matched += 1
        source_findings = compare_quality_to_baseline(
            quality=source,
            baseline=baseline,
            contract_override=override,
        )
        findings.extend(source_findings)
        comparisons.append(
            {
                "baseline_key": key,
                "status": "ok" if not source_findings else "drift",
                "findings": [f.model_dump(mode="json") for f in source_findings],
            }
        )
    summary = {
        "schema_version": "monthly_platform.source_quality_baseline_comparison.v1",
        "generated_at": utc_now_iso(),
        "baseline_dir_loaded_count": len(baselines),
        "matched_source_count": matched,
        "missing_baseline_source_count": missing,
        "drift_finding_count": len(findings),
        "info_finding_count": sum(1 for f in findings if f.severity == "info"),
        "medium_finding_count": sum(1 for f in findings if f.severity == "medium"),
        "high_finding_count": sum(1 for f in findings if f.severity == "high"),
        "comparisons": comparisons,
    }
    return findings, summary


# ---------------------------------------------------------------------------
# Calibrator: derive baseline candidates from one or more quality audits
# ---------------------------------------------------------------------------


def _percentile(values: list[float], pct: float) -> float:
    """Inclusive linear-interpolation percentile. ``pct`` in [0, 100]."""
    if not values:
        raise ValueError("percentile undefined for empty input")
    ordered = sorted(values)
    if len(ordered) == 1:
        return float(ordered[0])
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    fraction = rank - low
    return float(ordered[low] + (ordered[high] - ordered[low]) * fraction)


def _expected_envelope(
    values: list[int],
    *,
    low_factor: float = DEFAULT_LOW_FACTOR,
    high_factor: float = DEFAULT_HIGH_FACTOR,
) -> tuple[float | None, float | None]:
    """Conservative expected-min/expected-max derivation.

    Single-sample baselines deliberately leave the envelope ``None`` so the
    comparator does not flag drift on a one-shot calibration.
    """
    if len(values) < 2:
        return None, None
    floor = max(min(values) * low_factor, 0.0)
    ceil_ = max(values) * high_factor
    return floor, ceil_


def derive_baseline(
    *,
    baseline_key: str,
    requirement_id: str,
    territory: str | None,
    period_role: PeriodRole,
    observations: list[dict[str, Any]],
    promoted_at: str | None = None,
    policy: BaselinePolicy | None = None,
    notes: str = "",
) -> SourceQualityBaseline:
    """Compute a calibrated baseline from one or more quality observations.

    Each observation must be a dict with at least ``run_id``, ``snapshot_date``,
    ``row_count``, ``row_count_policy.allow_zero`` and ``field_audits``.
    """
    if not observations:
        raise ValueError("derive_baseline requires at least one observation")
    row_counts = [int(obs["row_count"]) for obs in observations]
    expected_min, expected_max = _expected_envelope(row_counts)
    allow_zero = bool(
        observations[-1].get("row_count_policy", {}).get("allow_zero", True)
    )
    rc = BaselineRowCount(
        median=float(statistics.median(row_counts)),
        p05=_percentile([float(v) for v in row_counts], 5.0),
        p95=_percentile([float(v) for v in row_counts], 95.0),
        min_observed=min(row_counts),
        max_observed=max(row_counts),
        expected_min=expected_min,
        expected_max=expected_max,
        sample_count=len(observations),
        allow_zero=allow_zero,
    )

    null_samples: dict[str, list[float]] = defaultdict(list)
    for obs in observations:
        for audit in obs.get("field_audits", []) or []:
            if not audit.get("present"):
                continue
            null_pct = audit.get("null_pct")
            if null_pct is None:
                continue
            null_samples[audit["field_name"]].append(float(null_pct))
    null_rates: dict[str, BaselineNullRate] = {}
    for field_name, samples in null_samples.items():
        baseline_pct = float(statistics.median(samples))
        # max_pct: observed max + delta cushion, capped at 1.0.
        cushion = max(samples) + DEFAULT_NULL_RATE_DELTA
        max_pct = min(1.0, max(baseline_pct, cushion))
        null_rates[field_name] = BaselineNullRate(
            baseline_pct=baseline_pct,
            max_pct=max_pct,
            samples=[round(v, 6) for v in samples],
        )

    provenance = [
        BaselineProvenance(
            run_id=str(obs.get("run_id") or ""),
            snapshot_date=str(obs.get("snapshot_date") or ""),
            quarter_label=obs.get("quarter_label"),
            salesforce_id=obs.get("salesforce_id"),
            row_count=int(obs["row_count"]),
        )
        for obs in observations
    ]

    return SourceQualityBaseline(
        baseline_key=baseline_key,
        requirement_id=requirement_id,
        territory=territory if territory else "global",
        period_role=period_role,
        promoted_at=promoted_at or utc_now_iso(),
        promoted_from=provenance,
        row_count=rc,
        null_rates=null_rates,
        policy=policy or BaselinePolicy(),
        notes=notes,
    )


def collect_observations(
    quality_audits: Iterable[dict[str, Any]],
    *,
    include_statuses: tuple[str, ...] = ("ok", "warning"),
) -> dict[str, list[dict[str, Any]]]:
    """Group ``sources[]`` entries from many audits by baseline key.

    Quality entries with ``status`` not in ``include_statuses`` are skipped so
    blocked extracts (zero-row, missing fields) do not poison the baseline.
    """
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for audit in quality_audits:
        run_id = audit.get("run_id")
        snapshot_date = audit.get("snapshot_date")
        for source in audit.get("sources", []):
            if source.get("status") not in include_statuses:
                continue
            key = baseline_key_for_quality(source)
            enriched = dict(source)
            enriched.setdefault("run_id", run_id)
            enriched.setdefault("snapshot_date", snapshot_date)
            grouped[key].append(enriched)
    return grouped


def write_baseline(directory: Path, baseline: SourceQualityBaseline) -> Path:
    """Write a baseline JSON to ``directory``. Caller controls promotion gating."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / baseline_filename(baseline.baseline_key)
    payload = baseline.model_dump(mode="json")
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return path
