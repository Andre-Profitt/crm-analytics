"""Track D — distribution audit for source extracts.

Closes the original GPT Pro v2 hidden-risk example: *"Stage 5 deals quietly
disappeared."* A Salesforce report can extract successfully, satisfy Track B
row-count policies, and even match the Track C row-count baseline while a
specific stage / quarter / territory / owner segment silently drops to zero
rows. Track D adds four checks per declared dimension:

1. **Required-category presence** — categories the contract names in
   ``DimensionPolicy.required_categories`` must produce at least one row.
2. **Disappeared-category** — categories observed in the baseline seed but
   absent from the current run. Catches accidental scope collapse without
   needing a hand-authored required list.
3. **Share drift** — any category whose share moved by more than
   ``max_abs_share_delta`` versus the seed.
4. **Concentration drift** — top-1 category share exceeds
   ``max_top_category_share`` (catches accidental filter collapse onto
   one owner / territory / director).

Plus a thin **slice sentinel** layer: a named (field, category) presence
guardrail so the original "Stage 5 disappeared" failure mode shows up as
``stage_5_presence sentinel failed`` in audit evidence.

The module is pure: ``audit_distribution`` and ``compare_run_distributions``
never mutate inputs and never touch the filesystem. Seeds are loaded by
``load_distribution_seeds`` from ``config/source_distribution_baselines/``;
absent seeds make the *required-category* and *concentration* checks still
fire (those don't need a seed) but skip the *disappeared* and *share-drift*
checks for that source.

Track D scope (per ``docs/2026-04-25-gpt-pro-feedback-implementation-plan.md``):

* No DuckDB / Parquet — that is Track H.
* No deck contract or template-first builder — Tracks E/F.
* No OpenLineage / waivers — Tracks J/K.
* Default severity for drift is ``info``; contracts opt up per dimension.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Literal

from pydantic import Field, field_validator

from scripts.monthly_platform.contracts import (
    ContractModel,
    Finding,
    FindingSeverity,
    utc_now_iso,
)
from scripts.monthly_platform.source_quality_baselines import (
    _territory_slug,
)
from scripts.monthly_platform.source_requirements import (
    DimensionPolicy,
    PeriodRole,
    SliceSentinel,
    SourcePlanItem,
    distribution_action_to_severity,
)


SCHEMA_SEED = "monthly_platform.source_distribution_seed.v1"
SCHEMA_COMPARISON = "monthly_platform.source_distribution_comparison.v1"


# ---------------------------------------------------------------------------
# Seed schema
# ---------------------------------------------------------------------------


class DimensionSeed(ContractModel):
    """Calibrated category share for one dimension on one baseline_key.

    ``share_by_category`` is the seed share each category held when the seed
    was promoted. The comparator computes current shares the same way and
    reports drift relative to these values.
    """

    field: str
    semantic_name: str | None = None
    sample_count: int = 0  # rows seen during seed derivation
    share_by_category: dict[str, float] = Field(default_factory=dict)

    @field_validator("share_by_category")
    @classmethod
    def shares_in_unit_interval(cls, value: dict[str, float]) -> dict[str, float]:
        for category, share in value.items():
            if not 0.0 <= share <= 1.0:
                raise ValueError(f"share_by_category[{category}]={share} not in [0, 1]")
        return value


class SourceDistributionSeed(ContractModel):
    """Seed values for one (requirement_id, territory, period_role) triple.

    Hand-promoted into ``config/source_distribution_baselines/<baseline_key>.json``.
    A missing seed file is not a release blocker — the comparator still runs
    required-category and concentration checks (which need no seed); the
    disappeared-category and share-drift checks are skipped for that source.
    """

    schema_version: Literal["monthly_platform.source_distribution_seed.v1"] = (
        SCHEMA_SEED
    )
    baseline_key: str
    requirement_id: str
    territory: str
    period_role: PeriodRole
    promoted_at: str
    promoted_from: list[dict[str, Any]] = Field(default_factory=list)
    dimensions: dict[str, DimensionSeed] = Field(default_factory=dict)
    notes: str = ""


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------


def baseline_key_for_item(item: SourcePlanItem) -> str:
    """Mirror of Track C's baseline-key shape so seeds stay collocatable."""
    return f"{item.requirement_id}.{_territory_slug(item.territory)}.{item.period_role}"


def load_distribution_seeds(directory: Path) -> dict[str, SourceDistributionSeed]:
    """Load every ``*.json`` in ``directory`` as a seed.

    Missing directory → ``{}``. The audit treats absent seeds as a no-op for
    seed-dependent axes (disappearance, share drift) so a fresh repo never
    blocks on the absence of distribution baselines.
    """
    if not directory.exists() or not directory.is_dir():
        return {}
    seeds: dict[str, SourceDistributionSeed] = {}
    for path in sorted(directory.glob("*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        seed = SourceDistributionSeed.model_validate(payload)
        if seed.baseline_key in seeds:
            raise ValueError(
                f"duplicate seed baseline_key={seed.baseline_key} at {path}"
            )
        seeds[seed.baseline_key] = seed
    return seeds


# ---------------------------------------------------------------------------
# Row-value extraction (handles dotted Salesforce paths like "Owner.Name")
# ---------------------------------------------------------------------------


_MISSING = object()


def _extract_value(row: dict[str, Any], field_name: str) -> Any:
    """Resolve a possibly dotted field path on a Salesforce row dict.

    Supports four Salesforce row shapes encountered in the wild:

    * Flat top-level key: ``row["StageName"]``.
    * ``__display`` / ``_display`` flat fallback for relations:
      ``row["Owner__display"]``.
    * Simple nested dict (test fixtures, JSON-flat extracts):
      ``row["Owner"]["Name"]``.
    * Salesforce list-view nested-object envelope:
      ``row["Owner"]["fields"]["Name"]["value"]`` (and the matching
      ``displayValue`` form).

    Returns ``_MISSING`` when the key is absent so callers can distinguish a
    missing field from an explicit ``None``.
    """
    if field_name in row:
        return row[field_name]
    if "." in field_name:
        head, _, tail = field_name.partition(".")
        nested = row.get(head)
        if isinstance(nested, dict):
            value = _extract_value_nested(nested, tail)
            if value is not _MISSING:
                return value
        flat_key = field_name.replace(".", "_")
        if flat_key in row:
            return row[flat_key]
        for suffix in ("__display", "_display"):
            if flat_key + suffix in row:
                return row[flat_key + suffix]
        # Salesforce list-view extracts often expose a relation as
        # ``Head__display`` instead of ``Head_Tail`` (one display string
        # per related record). Fall back to that when the dotted path
        # targets a relation's primary display field (Name / Label).
        if tail.lower() in {"name", "label"}:
            for suffix in ("__display", "_display"):
                if head + suffix in row:
                    return row[head + suffix]
        return _MISSING
    for suffix in ("__display", "_display"):
        if field_name + suffix in row:
            return row[field_name + suffix]
    return _MISSING


def _extract_value_nested(nested: dict[str, Any], tail: str) -> Any:
    """Resolve ``tail`` against a nested dict, handling Salesforce envelopes.

    ``tail`` may itself be a dotted path. Tries:
    1. Plain key lookup (test fixtures).
    2. Salesforce list-view envelope: ``nested["fields"][key]["value"]``
       (preferred), then ``["displayValue"]`` as fallback.
    3. Recursive descent for deeper dotted paths.
    """
    if "." in tail:
        head, _, rest = tail.partition(".")
        deeper = _extract_value_nested(nested, head)
        if deeper is _MISSING or not isinstance(deeper, dict):
            return _MISSING
        return _extract_value_nested(deeper, rest)
    if tail in nested:
        return nested[tail]
    fields = nested.get("fields")
    if isinstance(fields, dict):
        field = fields.get(tail)
        if isinstance(field, dict):
            if "value" in field:
                return field["value"]
            if "displayValue" in field:
                return field["displayValue"]
    return _MISSING


def _category_counts(rows: Iterable[dict[str, Any]], field_name: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for row in rows:
        value = _extract_value(row, field_name)
        if value is _MISSING or value is None or value == "":
            counts["__null__"] += 1
            continue
        counts[str(value)] += 1
    return counts


def _shares(counts: Counter[str], total: int) -> dict[str, float]:
    if total == 0:
        return {}
    return {category: round(count / total, 6) for category, count in counts.items()}


def _top_n(counts: Counter[str], n: int) -> list[tuple[str, int]]:
    return counts.most_common(n)


# ---------------------------------------------------------------------------
# Per-source audit (pure)
# ---------------------------------------------------------------------------


def _finding(
    *,
    severity: FindingSeverity,
    issue: str,
    item: SourcePlanItem,
    field: str,
    evidence: str,
) -> Finding:
    prefix = (
        f"{item.requirement_id} {item.territory or 'global'} "
        f"{item.period_role} {item.source_type} {item.source_id}"
    )
    return Finding(
        severity=severity,
        issue=issue,
        evidence=f"{prefix}: field={field}; {evidence}",
    )


def _audit_dimension(
    *,
    item: SourcePlanItem,
    rows: list[dict[str, Any]],
    dimension: DimensionPolicy,
    seed: DimensionSeed | None,
    seed_status: Literal["present", "missing", "no_source_seed"],
) -> tuple[dict[str, Any], list[Finding]]:
    findings: list[Finding] = []
    counts = _category_counts(rows, dimension.field)
    total = sum(counts.values())
    current_shares = _shares(counts, total)
    seed_shares = seed.share_by_category if seed else {}

    # 0. Partial seed coverage — the source has a seed file but this dimension
    # is not in it. Emit a finding so the gap is visible. ``no_source_seed``
    # is a separate, explicit state (no seed file at all) and is *not* a
    # finding here — the run-level summary already records that case.
    if seed_status == "missing":
        severity = distribution_action_to_severity(dimension.missing_seed_action)
        if severity is not None:
            findings.append(
                _finding(
                    severity=severity,
                    issue="source_distribution_dimension_seed_missing",
                    item=item,
                    field=dimension.field,
                    evidence=(
                        f"semantic={dimension.semantic_name}; "
                        f"seed_status={seed_status}; "
                        f"action={dimension.missing_seed_action}"
                    ),
                )
            )

    # 1. Required categories — contract-named presence checks.
    missing_required: list[str] = []
    for required in dimension.required_categories:
        if counts.get(required, 0) == 0:
            missing_required.append(required)
    severity = distribution_action_to_severity(dimension.missing_category_action)
    if missing_required and severity is not None:
        for category in missing_required:
            findings.append(
                _finding(
                    severity=severity,
                    issue="source_distribution_required_category_missing",
                    item=item,
                    field=dimension.field,
                    evidence=(
                        f"semantic={dimension.semantic_name}; "
                        f"required_category={category!r}; observed_rows=0; "
                        f"action={dimension.missing_category_action}"
                    ),
                )
            )

    # 2. Disappeared categories — present in seed, gone from current run.
    disappeared: list[str] = []
    if seed:
        for category, baseline_share in seed_shares.items():
            if baseline_share <= 0:
                continue
            if counts.get(category, 0) == 0:
                disappeared.append(category)
        severity = distribution_action_to_severity(
            dimension.disappeared_category_action
        )
        if disappeared and severity is not None:
            for category in disappeared:
                findings.append(
                    _finding(
                        severity=severity,
                        issue="source_distribution_category_disappeared",
                        item=item,
                        field=dimension.field,
                        evidence=(
                            f"semantic={dimension.semantic_name}; "
                            f"category={category!r}; "
                            f"baseline_share={seed_shares[category]:.4f}; "
                            f"current_share=0.0000; "
                            f"action={dimension.disappeared_category_action}"
                        ),
                    )
                )

    # 3. Share drift — any category moved more than the configured delta.
    share_drift: list[dict[str, Any]] = []
    if seed:
        all_categories = set(seed_shares) | set(current_shares)
        for category in all_categories:
            baseline_share = seed_shares.get(category, 0.0)
            current_share = current_shares.get(category, 0.0)
            delta = abs(current_share - baseline_share)
            if delta > dimension.max_abs_share_delta:
                share_drift.append(
                    {
                        "category": category,
                        "baseline_share": baseline_share,
                        "current_share": current_share,
                        "abs_delta": round(delta, 6),
                    }
                )
        severity = distribution_action_to_severity(dimension.share_drift_action)
        if share_drift and severity is not None:
            for entry in share_drift:
                findings.append(
                    _finding(
                        severity=severity,
                        issue="source_distribution_share_drift",
                        item=item,
                        field=dimension.field,
                        evidence=(
                            f"semantic={dimension.semantic_name}; "
                            f"category={entry['category']!r}; "
                            f"baseline_share={entry['baseline_share']:.4f}; "
                            f"current_share={entry['current_share']:.4f}; "
                            f"abs_delta={entry['abs_delta']:.4f}; "
                            f"max_abs_share_delta={dimension.max_abs_share_delta}; "
                            f"action={dimension.share_drift_action}"
                        ),
                    )
                )

    # 4. Concentration drift — top category dominates the source.
    concentration_finding: dict[str, Any] | None = None
    if dimension.max_top_category_share is not None and total > 0:
        top_category, top_count = _top_n(counts, 1)[0] if counts else ("", 0)
        top_share = top_count / total
        if top_share > dimension.max_top_category_share:
            severity = distribution_action_to_severity(dimension.concentration_action)
            if severity is not None:
                concentration_finding = {
                    "top_category": top_category,
                    "top_share": round(top_share, 6),
                    "max_top_category_share": dimension.max_top_category_share,
                }
                findings.append(
                    _finding(
                        severity=severity,
                        issue="source_distribution_concentration_drift",
                        item=item,
                        field=dimension.field,
                        evidence=(
                            f"semantic={dimension.semantic_name}; "
                            f"top_category={top_category!r}; "
                            f"top_share={top_share:.4f}; "
                            f"max_top_category_share={dimension.max_top_category_share}; "
                            f"action={dimension.concentration_action}"
                        ),
                    )
                )

    payload = {
        "field": dimension.field,
        "semantic_name": dimension.semantic_name,
        "row_count": total,
        "category_count": len(counts),
        "current_shares": current_shares,
        "baseline_shares": seed_shares,
        "top_n": [
            {
                "category": cat,
                "count": cnt,
                "share": round(cnt / total, 6) if total else 0.0,
            }
            for cat, cnt in _top_n(counts, dimension.top_n_for_evidence)
        ],
        "missing_required_categories": missing_required,
        "disappeared_categories": disappeared,
        "share_drift": share_drift,
        "concentration": concentration_finding,
        "seed_present": seed is not None,
        "seed_status": seed_status,
    }
    return payload, findings


def _audit_sentinels(
    *,
    item: SourcePlanItem,
    rows: list[dict[str, Any]],
    sentinels: list[SliceSentinel],
) -> tuple[list[dict[str, Any]], list[Finding]]:
    """Evaluate slice sentinels (named (field, category) presence guards)."""
    if not sentinels:
        return [], []
    counts_by_field: dict[str, Counter[str]] = {}
    findings: list[Finding] = []
    payloads: list[dict[str, Any]] = []
    for sentinel in sentinels:
        counts = counts_by_field.setdefault(
            sentinel.field, _category_counts(rows, sentinel.field)
        )
        observed_rows = counts.get(sentinel.category, 0)
        passed = observed_rows > 0
        payloads.append(
            {
                "id": sentinel.id,
                "field": sentinel.field,
                "category": sentinel.category,
                "observed_rows": observed_rows,
                "passed": passed,
                "action": sentinel.action,
                "reason": sentinel.reason,
            }
        )
        if passed:
            continue
        severity = distribution_action_to_severity(sentinel.action)
        if severity is None:
            continue
        findings.append(
            _finding(
                severity=severity,
                issue="source_distribution_sentinel_failed",
                item=item,
                field=sentinel.field,
                evidence=(
                    f"sentinel_id={sentinel.id}; category={sentinel.category!r}; "
                    f"observed_rows=0; reason={sentinel.reason or 'unspecified'}; "
                    f"action={sentinel.action}"
                ),
            )
        )
    return payloads, findings


def audit_distribution(
    *,
    item: SourcePlanItem,
    rows: list[dict[str, Any]],
    seed: SourceDistributionSeed | None,
) -> tuple[dict[str, Any], list[Finding]]:
    """Audit one source's distribution. Pure: never mutates inputs or files.

    Returns a per-source comparison payload (intended to be embedded in
    ``quality_audit['distribution_comparison']['comparisons']``) and the list
    of findings the audit emitted. ``policy=None`` produces an empty
    comparison payload with status ``no_policy`` and zero findings — the
    monthly contract is the one that opts each source into Track D.
    """
    policy = item.distribution_policy
    if policy is None or (not policy.dimensions and not policy.slice_sentinels):
        return (
            {
                "source_key": _source_key(item),
                "status": "no_policy",
                "dimensions": [],
                "slice_sentinels": [],
                "seed_present": seed is not None,
            },
            [],
        )

    dimension_payloads: list[dict[str, Any]] = []
    findings: list[Finding] = []
    for dimension in policy.dimensions:
        if seed is None:
            seed_dim = None
            seed_status: Literal["present", "missing", "no_source_seed"] = (
                "no_source_seed"
            )
        elif dimension.field not in seed.dimensions:
            seed_dim = None
            seed_status = "missing"
        else:
            seed_dim = seed.dimensions[dimension.field]
            seed_status = "present"
        payload, dim_findings = _audit_dimension(
            item=item,
            rows=rows,
            dimension=dimension,
            seed=seed_dim,
            seed_status=seed_status,
        )
        dimension_payloads.append(payload)
        findings.extend(dim_findings)

    sentinel_payloads, sentinel_findings = _audit_sentinels(
        item=item, rows=rows, sentinels=policy.slice_sentinels
    )
    findings.extend(sentinel_findings)

    return (
        {
            "source_key": _source_key(item),
            "status": "drift" if findings else "ok",
            "dimensions": dimension_payloads,
            "slice_sentinels": sentinel_payloads,
            "seed_present": seed is not None,
            "row_count": len(rows),
        },
        findings,
    )


def _source_key(item: SourcePlanItem) -> str:
    territory = (item.territory or "global").lower().replace(" ", "_")
    return (
        f"{item.requirement_id}.{territory}."
        f"{item.period_role}.{item.quarter_label}.{item.source_id}"
    )


# ---------------------------------------------------------------------------
# Run-level wrapper
# ---------------------------------------------------------------------------


def compare_run_distributions(
    *,
    per_source_payloads: list[dict[str, Any]],
    findings: list[Finding],
) -> dict[str, Any]:
    """Summarize per-source distribution payloads into one run-level block."""
    matched = [p for p in per_source_payloads if p["status"] != "no_policy"]
    no_policy = [p for p in per_source_payloads if p["status"] == "no_policy"]
    missing_seed = [p for p in matched if not p.get("seed_present")]
    # Track D activation: distinct gap from "no source seed" — the source has
    # a seed but a configured dimension is missing from it. Counted across
    # all matched sources for the run-level summary.
    missing_seed_dimension_count = sum(
        1
        for p in matched
        for dim in p.get("dimensions", [])
        if dim.get("seed_status") == "missing"
    )
    return {
        "schema_version": SCHEMA_COMPARISON,
        "generated_at": utc_now_iso(),
        "matched_source_count": len(matched),
        "no_policy_source_count": len(no_policy),
        "missing_seed_source_count": len(missing_seed),
        "missing_seed_dimension_count": missing_seed_dimension_count,
        "distribution_finding_count": len(findings),
        "info_finding_count": sum(1 for f in findings if f.severity == "info"),
        "medium_finding_count": sum(1 for f in findings if f.severity == "medium"),
        "high_finding_count": sum(1 for f in findings if f.severity == "high"),
        "comparisons": per_source_payloads,
    }


# ---------------------------------------------------------------------------
# Re-export utilities used by tests / external callers
# ---------------------------------------------------------------------------


__all__ = [
    "SCHEMA_SEED",
    "SCHEMA_COMPARISON",
    "DimensionSeed",
    "SourceDistributionSeed",
    "audit_distribution",
    "baseline_key_for_item",
    "compare_run_distributions",
    "load_distribution_seeds",
]
