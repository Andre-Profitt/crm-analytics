"""Typed contracts for the local monthly deck factory.

These contracts describe run state, source extracts, and artifacts. They are
deliberately separate from the existing row-level dataclasses in `models.py`:
`models.py` is business data; this module is platform/control-plane metadata.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


SCHEMA_VERSION = "monthly_platform.contracts.v1"

StageStatus = Literal["ok", "warning", "blocked", "failed", "skipped"]
FindingSeverity = Literal["high", "medium", "low", "info"]


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Finding(ContractModel):
    severity: FindingSeverity
    issue: str
    evidence: str = ""
    owner: str | None = None


class ArtifactRef(ContractModel):
    artifact_id: str
    artifact_type: str
    path: str
    format: str
    byte_count: int = 0
    row_count: int | None = None
    sha256: str
    schema_sha256: str | None = None
    created_at: str = Field(default_factory=utc_now_iso)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("byte_count")
    @classmethod
    def non_negative_byte_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("byte_count must be non-negative")
        return value

    @field_validator("row_count")
    @classmethod
    def non_negative_row_count(cls, value: int | None) -> int | None:
        if value is not None and value < 0:
            raise ValueError("row_count must be non-negative")
        return value


class SourceExtract(ContractModel):
    source_extract_id: str
    snapshot_date: str
    source_system: str = "salesforce"
    source_type: str
    source_id: str
    source_label: str
    territory: str | None = None
    director: str | None = None
    region: str | None = None
    period_role: str | None = None
    quarter_label: str | None = None
    status: StageStatus = "ok"
    row_count: int
    raw_artifact: ArtifactRef
    normalized_artifact: ArtifactRef | None = None
    schema_sha256: str
    rowset_sha256: str
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("row_count")
    @classmethod
    def non_negative_row_count(cls, value: int) -> int:
        if value < 0:
            raise ValueError("row_count must be non-negative")
        return value


class StageResult(ContractModel):
    stage_name: str
    status: StageStatus
    started_at: str
    finished_at: str
    duration_seconds: float
    inputs: list[ArtifactRef] = Field(default_factory=list)
    outputs: list[ArtifactRef] = Field(default_factory=list)
    source_extracts: list[SourceExtract] = Field(default_factory=list)
    findings: list[Finding] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("duration_seconds")
    @classmethod
    def non_negative_duration(cls, value: float) -> float:
        if value < 0:
            raise ValueError("duration_seconds must be non-negative")
        return value


class MonthlyRunManifest(ContractModel):
    schema_version: str = SCHEMA_VERSION
    run_id: str
    snapshot_date: str
    status: StageStatus
    created_at: str = Field(default_factory=utc_now_iso)
    updated_at: str = Field(default_factory=utc_now_iso)
    period_context: dict[str, Any] = Field(default_factory=dict)
    stages: list[StageResult] = Field(default_factory=list)
    artifacts: list[ArtifactRef] = Field(default_factory=list)
    source_extracts: list[SourceExtract] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)

    def with_stage(self, stage: StageResult) -> "MonthlyRunManifest":
        status = self.status
        if stage.status in {"blocked", "failed"}:
            status = stage.status
        elif status == "ok" and stage.status == "warning":
            status = "warning"
        return self.model_copy(
            update={
                "status": status,
                "updated_at": utc_now_iso(),
                "stages": [*self.stages, stage],
                "artifacts": [*self.artifacts, *stage.outputs],
                "source_extracts": [*self.source_extracts, *stage.source_extracts],
            }
        )
