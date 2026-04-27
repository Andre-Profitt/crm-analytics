"""Track K — release catalog.

Wraps the Track G-Lite release packet (which runs all 8 validators)
with a waiver-application pass that:

  1. Loads waivers from ``config/waivers/`` via ``waivers.load_waivers``.
  2. Walks every finding emitted by every validator inside the packet.
  3. For each finding whose ``code`` matches a waiver's ``gate`` AND
     whose finding_path matches the waiver's optional ``finding_path``
     filter AND that's allowed for the current run_id, the finding's
     severity is downgraded per the waiver's ``severity_after``.
  4. Aggregates the post-waiver totals and emits a final
     ``release_decision`` (publish_ready / blocked_with_warnings /
     blocked).

The release_catalog also captures the WAIVED list separately so
auditors can see exactly which findings were downgraded by which
waiver, and which waivers were unused (no matching findings).

Output schema: ``monthly_platform.release_catalog.v1``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.build_release_packet import build_release_packet
from scripts.monthly_platform.waivers import (
    ReleasePolicy,
    Waiver,
    load_policy,
    load_waivers,
)


CATALOG_SCHEMA_VERSION = "monthly_platform.release_catalog.v1"


@dataclass
class WaiverApplication:
    """Records a single (waiver, finding) match."""

    waiver_id: str
    waiver_owner: str
    waiver_approved_by: str
    gate: str
    validator: str
    severity_before: str
    severity_after: str
    finding_message: str
    finding_path: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "waiver_id": self.waiver_id,
            "waiver_owner": self.waiver_owner,
            "waiver_approved_by": self.waiver_approved_by,
            "gate": self.gate,
            "validator": self.validator,
            "severity_before": self.severity_before,
            "severity_after": self.severity_after,
            "finding_message": self.finding_message,
            "finding_path": self.finding_path,
        }


def _gate_for(validator_name: str, finding_code: str) -> str:
    return f"{validator_name}.{finding_code}"


def _findings_in(detail_report: dict[str, Any]) -> list[dict[str, Any]]:
    """Return the list of findings inside any validator detail report.

    Each validator has a ``findings`` list at the top level by
    convention. Returns an empty list if the report doesn't carry one.
    """
    return list(detail_report.get("findings") or [])


def _waiver_matches(
    waiver: Waiver,
    *,
    validator: str,
    finding: dict[str, Any],
    run_id: str | None,
) -> bool:
    if not waiver.applies_to_run(run_id):
        return False
    expected_gate = _gate_for(validator, str(finding.get("code", "")))
    if waiver.gate != expected_gate:
        return False
    if waiver.severity_before != finding.get("severity"):
        return False
    if waiver.finding_path:
        if str(finding.get("path", "")) != waiver.finding_path:
            return False
    return True


@dataclass
class CatalogResult:
    publish_decision: str
    pre_waiver_blocker_total: int
    pre_waiver_warning_total: int
    post_waiver_blocker_total: int
    post_waiver_warning_total: int
    applied_waivers: list[WaiverApplication] = field(default_factory=list)
    unused_waivers: list[Waiver] = field(default_factory=list)
    waiver_findings: list[dict[str, Any]] = field(default_factory=list)
    release_packet: dict[str, Any] = field(default_factory=dict)
    artifact_digests: dict[str, Any] = field(default_factory=dict)
    captured_at: str = ""
    run_id: str | None = None
    policy_path: str = ""
    waiver_dir: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": CATALOG_SCHEMA_VERSION,
            "captured_at": self.captured_at,
            "run_id": self.run_id,
            "policy_path": self.policy_path,
            "waiver_dir": self.waiver_dir,
            "publish_decision": self.publish_decision,
            "pre_waiver_blocker_total": self.pre_waiver_blocker_total,
            "pre_waiver_warning_total": self.pre_waiver_warning_total,
            "post_waiver_blocker_total": self.post_waiver_blocker_total,
            "post_waiver_warning_total": self.post_waiver_warning_total,
            "applied_waivers": [w.as_dict() for w in self.applied_waivers],
            "unused_waivers": [w.as_dict() for w in self.unused_waivers],
            "waiver_loader_findings": self.waiver_findings,
            "artifact_digests": self.artifact_digests,
            "release_packet_summaries": self.release_packet.get("summaries", []),
        }


def build_release_catalog(
    *,
    workbook: Path,
    pptx: Path,
    deck_contract_path: Path | None = None,
    workbook_contract_path: Path | None = None,
    policy_path: Path | None = None,
    waiver_dir: Path | None = None,
    run_id: str | None = None,
    skip_visual: bool = False,
) -> CatalogResult:
    # 1. Run all validators via Track G-Lite.
    packet = build_release_packet(
        workbook=workbook,
        pptx=pptx,
        deck_contract_path=deck_contract_path,
        workbook_contract_path=workbook_contract_path,
        skip_visual=skip_visual,
    )

    # 2. Load policy + waivers.
    policy: ReleasePolicy = load_policy(policy_path)
    waivers, waiver_findings = load_waivers(waiver_dir, policy=policy)

    # 3. Pre-waiver totals come straight from the packet.
    pre_blockers = int(packet["blocker_total"])
    pre_warnings = int(packet["warning_total"])

    # 4. Walk findings, apply matching waivers.
    applied: list[WaiverApplication] = []
    used_waiver_ids: set[str] = set()
    detail_reports = packet.get("detail_reports", {}) or {}

    for validator_name, detail in detail_reports.items():
        if not isinstance(detail, dict):
            continue
        for finding in _findings_in(detail):
            severity = finding.get("severity")
            if severity not in ("blocker", "warning"):
                continue
            for waiver in waivers:
                if not _waiver_matches(
                    waiver, validator=validator_name, finding=finding, run_id=run_id
                ):
                    continue
                applied.append(
                    WaiverApplication(
                        waiver_id=waiver.id,
                        waiver_owner=waiver.owner,
                        waiver_approved_by=waiver.approved_by,
                        gate=waiver.gate,
                        validator=validator_name,
                        severity_before=str(severity),
                        severity_after=waiver.severity_after,
                        finding_message=str(finding.get("message", ""))[:300],
                        finding_path=str(finding.get("path", "")),
                    )
                )
                used_waiver_ids.add(waiver.id)
                # Mark the finding as downgraded for total recalculation.
                finding["_post_waiver_severity"] = waiver.severity_after
                finding["_waiver_id"] = waiver.id
                break  # one waiver per finding

    # 5. Recompute totals using the post-waiver severity where set.
    post_blockers = 0
    post_warnings = 0
    for detail in detail_reports.values():
        if not isinstance(detail, dict):
            continue
        for finding in _findings_in(detail):
            sev = finding.get("_post_waiver_severity") or finding.get("severity")
            if sev == "blocker":
                post_blockers += 1
            elif sev == "warning":
                post_warnings += 1

    if post_blockers > 0:
        decision = "blocked"
    elif post_warnings > 0:
        decision = "blocked_with_warnings"
    else:
        decision = "publish_ready"

    # 6. Identify unused waivers.
    unused = [w for w in waivers if w.id not in used_waiver_ids]

    return CatalogResult(
        publish_decision=decision,
        pre_waiver_blocker_total=pre_blockers,
        pre_waiver_warning_total=pre_warnings,
        post_waiver_blocker_total=post_blockers,
        post_waiver_warning_total=post_warnings,
        applied_waivers=applied,
        unused_waivers=unused,
        waiver_findings=waiver_findings,
        release_packet=packet,
        artifact_digests=packet.get("artifact_digests", {}),
        captured_at=datetime.now(timezone.utc).isoformat(),
        run_id=run_id,
        policy_path=str(policy_path) if policy_path else "config/release_policy.yaml",
        waiver_dir=str(waiver_dir) if waiver_dir else "config/waivers/",
    )


def render_markdown(result: CatalogResult) -> str:
    lines: list[str] = []
    lines.append("# Release catalog\n")
    lines.append(f"- captured_at: {result.captured_at}")
    lines.append(f"- run_id: {result.run_id or '—'}")
    lines.append(f"- **publish_decision: {result.publish_decision}**")
    lines.append("")
    lines.append("## Pre vs post waiver\n")
    lines.append(
        f"| | Blockers | Warnings |\n| --- | ---: | ---: |\n"
        f"| Pre-waiver  | {result.pre_waiver_blocker_total} | {result.pre_waiver_warning_total} |\n"
        f"| Post-waiver | {result.post_waiver_blocker_total} | {result.post_waiver_warning_total} |"
    )
    lines.append("")
    if result.applied_waivers:
        lines.append("## Applied waivers\n")
        lines.append("| Waiver ID | Gate | Severity (before → after) | Owner | Path |")
        lines.append("| --- | --- | --- | --- | --- |")
        for a in result.applied_waivers:
            lines.append(
                f"| `{a.waiver_id}` | {a.gate} | "
                f"{a.severity_before} → {a.severity_after} | {a.waiver_owner} | "
                f"`{a.finding_path}` |"
            )
        lines.append("")
    if result.unused_waivers:
        lines.append("## Unused waivers (no matching findings)\n")
        for w in result.unused_waivers:
            lines.append(
                f"- `{w.id}` gate={w.gate} owner={w.owner!r} expires={w.expires_on}"
            )
        lines.append("")
    if result.waiver_findings:
        lines.append("## Waiver loader findings\n")
        for f in result.waiver_findings:
            lines.append(f"- **{f['severity']}** `{f['code']}` — {f['message']}")
        lines.append("")
    return "\n".join(lines) + "\n"


__all__ = [
    "CATALOG_SCHEMA_VERSION",
    "CatalogResult",
    "WaiverApplication",
    "build_release_catalog",
    "render_markdown",
]
