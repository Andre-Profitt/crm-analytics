"""Declarative source requirement registry for monthly deck extraction.

This module separates *what the monthly deck platform needs* from *where a
particular territory stores its Salesforce report/list-view ids*. Adding a new
Salesforce report should usually mean editing a JSON requirement registry and
territory ids, not changing extraction/rendering code.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import Field, field_validator

from scripts.monthly_platform.contracts import ContractModel, Finding
from scripts.monthly_platform.period import PeriodContext


SourceType = Literal[
    "salesforce_report", "salesforce_list_view", "salesforce_soql_probe"
]
RequirementScope = Literal["territory", "global"]
PeriodRole = Literal["prior_quarter", "current_quarter", "forward_quarter"]


class FieldContract(ContractModel):
    name: str
    semantic_name: str | None = None
    required: bool = True
    data_type: str | None = None
    description: str = ""


class SourcePathRule(ContractModel):
    period_role: PeriodRole
    source_id_path: str = ""
    source_label_path: str | None = None
    source_label_template: str | None = None
    source_id: str | None = None
    source_label: str | None = None


QualityAction = Literal["ok", "warning", "fallback", "blocked"]
BaselineDriftAction = Literal["ok", "info", "warning", "blocked"]


def action_to_severity(action: QualityAction) -> Literal["high", "medium"] | None:
    """Map a per-axis quality action policy to a finding severity.

    Returns ``None`` for ``ok`` (caller should skip emitting a finding entirely).
    """
    if action == "blocked":
        return "high"
    if action in ("warning", "fallback"):
        return "medium"
    return None


def baseline_drift_action_to_severity(
    action: BaselineDriftAction,
) -> Literal["high", "medium", "info"] | None:
    """Map a baseline drift action policy to a finding severity.

    Track C: baseline drift is read-only first. ``info`` is the default so a
    drift finding surfaces in audits without blocking releases. Contracts must
    explicitly opt up to ``warning``/``blocked`` once thresholds are calibrated.
    """
    if action == "blocked":
        return "high"
    if action == "warning":
        return "medium"
    if action == "info":
        return "info"
    return None


class RowCountPolicy(ContractModel):
    """Per-source row-count and field-quality policy.

    Track B (2026-04-25): each axis carries its own action so a min-rows breach
    no longer inherits its severity from ``zero_row_action``. GPT Pro v2 review
    flagged this conflation as "partly theater." See
    docs/2026-04-25-gpt-pro-feedback-implementation-plan.md.
    """

    allow_zero: bool = True
    min_rows: int = 0
    max_rows: int | None = None
    max_required_field_null_pct: float | None = None
    required_field_null_action: Literal["ok", "warning", "blocked"] = "warning"
    zero_row_action: QualityAction = "ok"
    # Track B: per-axis actions, separate from zero_row_action.
    min_rows_action: QualityAction = "warning"
    max_rows_action: QualityAction = "blocked"
    max_records_action: QualityAction = "warning"
    distribution_action: QualityAction = "warning"
    # Free-form annotations describing predicates under which an empty
    # extraction is legitimate (e.g. "territory_has_no_forward_pipeline").
    # Propagated into finding evidence; not auto-evaluated yet (Track C/D).
    expected_empty_conditions: list[str] = Field(default_factory=list)
    # Track C: optional contract-level override for baseline drift severity.
    # When set, the baseline comparator uses this in place of the per-baseline
    # `policy.row_count_drift_action` declared in the baseline JSON. Default
    # ``None`` means "follow the baseline file's policy" (read-only / info).
    baseline_drift_action: BaselineDriftAction | None = None

    @field_validator("min_rows")
    @classmethod
    def non_negative_min_rows(cls, value: int) -> int:
        if value < 0:
            raise ValueError("min_rows must be non-negative")
        return value

    @field_validator("max_rows")
    @classmethod
    def positive_max_rows(cls, value: int | None) -> int | None:
        if value is not None and value < 1:
            raise ValueError("max_rows must be positive when supplied")
        return value

    @field_validator("max_required_field_null_pct")
    @classmethod
    def valid_null_pct(cls, value: float | None) -> float | None:
        if value is not None and not 0 <= value <= 1:
            raise ValueError("max_required_field_null_pct must be between 0 and 1")
        return value


class FallbackPolicy(ContractModel):
    trigger: str
    from_period_role: PeriodRole
    to_period_role: PeriodRole
    description: str = ""


class SourceRequirement(ContractModel):
    requirement_id: str
    enabled: bool = True
    description: str = ""
    owner: str = "sales_director_monthly"
    source_system: Literal["salesforce"] = "salesforce"
    source_type: SourceType
    salesforce_object: str = "Opportunity"
    dataset: str
    output_grain: str
    scope: RequirementScope = "territory"
    territories: list[str] | None = None
    period_roles: list[PeriodRole]
    source_path_rules: list[SourcePathRule]
    required_fields: list[FieldContract] = Field(default_factory=list)
    row_count_policy: RowCountPolicy = Field(default_factory=RowCountPolicy)
    fallback_policy: FallbackPolicy | None = None
    consumers: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)

    @field_validator("period_roles")
    @classmethod
    def require_period_roles(cls, value: list[PeriodRole]) -> list[PeriodRole]:
        if not value:
            raise ValueError("period_roles must not be empty")
        return value


class SourceRequirementsRegistry(ContractModel):
    schema_version: str
    description: str = ""
    requirements: list[SourceRequirement]

    @field_validator("requirements")
    @classmethod
    def requirement_ids_unique(
        cls, value: list[SourceRequirement]
    ) -> list[SourceRequirement]:
        seen: set[str] = set()
        for requirement in value:
            if requirement.requirement_id in seen:
                raise ValueError(
                    f"duplicate requirement_id: {requirement.requirement_id}"
                )
            seen.add(requirement.requirement_id)
        return value


class SourcePlanItem(ContractModel):
    requirement_id: str
    source_system: str
    source_type: SourceType
    salesforce_object: str = "Opportunity"
    dataset: str
    output_grain: str
    scope: RequirementScope
    territory: str | None = None
    director: str | None = None
    region: str | None = None
    period_role: PeriodRole
    quarter_label: str
    quarter_title: str
    source_id: str
    source_label: str
    status: Literal["configured", "missing_source_id", "disabled"]
    required_fields: list[FieldContract] = Field(default_factory=list)
    row_count_policy: RowCountPolicy = Field(default_factory=RowCountPolicy)
    fallback_policy: FallbackPolicy | None = None
    consumers: list[str] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)


class SourceRequirementPlan(ContractModel):
    snapshot_date: str
    status: Literal["ok", "blocked"]
    items: list[SourcePlanItem]
    findings: list[Finding] = Field(default_factory=list)


def load_source_requirements(path: Path) -> SourceRequirementsRegistry:
    payload = json.loads(path.read_text(encoding="utf-8"))
    return SourceRequirementsRegistry.model_validate(payload)


def build_source_requirement_plan(
    *,
    registry: SourceRequirementsRegistry,
    territories: dict[str, Any],
    period: PeriodContext,
) -> SourceRequirementPlan:
    items: list[SourcePlanItem] = []
    findings: list[Finding] = []
    for requirement in registry.requirements:
        if not requirement.enabled:
            continue
        if requirement.scope == "global":
            for period_role in requirement.period_roles:
                item, finding = _build_item(
                    requirement=requirement,
                    territory=None,
                    territory_config={},
                    period_role=period_role,
                    period=period,
                )
                items.append(item)
                if finding:
                    findings.append(finding)
            continue

        for territory, territory_config in territories.items():
            if requirement.territories and territory not in requirement.territories:
                continue
            for period_role in requirement.period_roles:
                item, finding = _build_item(
                    requirement=requirement,
                    territory=str(territory),
                    territory_config=territory_config,
                    period_role=period_role,
                    period=period,
                )
                items.append(item)
                if finding:
                    findings.append(finding)

    return SourceRequirementPlan(
        snapshot_date=period.snapshot_date,
        status="blocked" if any(f.severity == "high" for f in findings) else "ok",
        items=items,
        findings=findings,
    )


def requirement_summary(plan: SourceRequirementPlan) -> dict[str, Any]:
    configured = [item for item in plan.items if item.status == "configured"]
    missing = [item for item in plan.items if item.status == "missing_source_id"]
    by_requirement: dict[str, int] = {}
    by_source_type: dict[str, int] = {}
    for item in configured:
        by_requirement[item.requirement_id] = (
            by_requirement.get(item.requirement_id, 0) + 1
        )
        by_source_type[item.source_type] = by_source_type.get(item.source_type, 0) + 1
    return {
        "status": plan.status,
        "snapshot_date": plan.snapshot_date,
        "configured_count": len(configured),
        "missing_source_id_count": len(missing),
        "finding_count": len(plan.findings),
        "by_requirement": by_requirement,
        "by_source_type": by_source_type,
    }


def filter_plan_items(
    plan: SourceRequirementPlan,
    *,
    only_requirement: str | None = None,
    only_territory: str | None = None,
    max_sources: int | None = None,
) -> list[SourcePlanItem]:
    items = [
        item
        for item in plan.items
        if item.status == "configured"
        and (not only_requirement or item.requirement_id == only_requirement)
        and (
            not only_territory
            or item.territory == only_territory
            or item.scope == "global"
        )
    ]
    if max_sources is not None:
        return items[:max_sources]
    return items


def _build_item(
    *,
    requirement: SourceRequirement,
    territory: str | None,
    territory_config: dict[str, Any],
    period_role: PeriodRole,
    period: PeriodContext,
) -> tuple[SourcePlanItem, Finding | None]:
    quarter_label = _quarter_label_for_role(period_role, period)
    quarter_title = _quarter_title_for_role(period_role, period)
    rule = _rule_for_period_role(requirement, period_role)
    source_id = ""
    source_label = ""
    if rule:
        if rule.source_id:
            source_id = rule.source_id.strip()
        elif rule.source_id_path:
            source_id = str(
                _get_path_value(
                    territory_config,
                    _render_path(rule.source_id_path, quarter_label=quarter_label),
                )
                or ""
            ).strip()
        if rule.source_label:
            source_label = rule.source_label.strip()
        elif rule.source_label_path:
            source_label = str(
                _get_path_value(
                    territory_config,
                    _render_path(rule.source_label_path, quarter_label=quarter_label),
                )
                or ""
            ).strip()
        if not source_label and rule.source_label_template:
            source_label = rule.source_label_template.format(
                territory=territory or "Global",
                director=territory_config.get("director") or "",
                quarter_label=quarter_label,
                quarter_title=quarter_title,
                dataset=requirement.dataset,
            )

    status = "configured" if source_id else "missing_source_id"
    finding = None
    if status == "missing_source_id":
        finding = Finding(
            severity="high",
            issue="source_requirement_missing_source_id",
            evidence=(
                f"{requirement.requirement_id} {territory or 'global'} "
                f"{period_role} {quarter_label}"
            ),
            owner=requirement.owner,
        )
    item = SourcePlanItem(
        requirement_id=requirement.requirement_id,
        source_system=requirement.source_system,
        source_type=requirement.source_type,
        salesforce_object=requirement.salesforce_object,
        dataset=requirement.dataset,
        output_grain=requirement.output_grain,
        scope=requirement.scope,
        territory=territory,
        director=territory_config.get("director") if territory_config else None,
        region=territory_config.get("region") if territory_config else None,
        period_role=period_role,
        quarter_label=quarter_label,
        quarter_title=quarter_title,
        source_id=source_id,
        source_label=source_label,
        status=status,
        required_fields=requirement.required_fields,
        row_count_policy=requirement.row_count_policy,
        fallback_policy=requirement.fallback_policy,
        consumers=requirement.consumers,
        tags=requirement.tags,
    )
    return item, finding


def _rule_for_period_role(
    requirement: SourceRequirement,
    period_role: PeriodRole,
) -> SourcePathRule | None:
    for rule in requirement.source_path_rules:
        if rule.period_role == period_role:
            return rule
    return None


def _quarter_label_for_role(period_role: PeriodRole, period: PeriodContext) -> str:
    if period_role == "prior_quarter":
        return period.prior_quarter.label
    if period_role == "current_quarter":
        return period.current_quarter.label
    return period.forward_quarter.label


def _quarter_title_for_role(period_role: PeriodRole, period: PeriodContext) -> str:
    if period_role == "prior_quarter":
        return period.prior_quarter.title
    if period_role == "current_quarter":
        return period.current_quarter.title
    return period.forward_quarter.title


def _render_path(path: str, *, quarter_label: str) -> str:
    return path.format(quarter_label=quarter_label)


def _get_path_value(payload: dict[str, Any], dotted_path: str) -> Any:
    current: Any = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current
