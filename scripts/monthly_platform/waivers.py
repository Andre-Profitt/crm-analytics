"""Track K — waiver loader + validator.

Reads waiver YAMLs from a directory (default ``config/waivers/``),
enforces the strict-waiver rules from GPT's release-train plan, and
returns a list of typed Waiver objects the release catalog uses to
downgrade matching findings.

Strict rules (any violation rejects the waiver at load time):

  - ``id`` matches ``^WV-\\d{4}-\\d{2}-\\d{3}$``
  - ``gate`` non-empty (format: ``<validator>.<finding_code>``)
  - ``owner`` non-empty
  - ``approved_by`` non-empty
  - ``reason`` >= 10 chars (no one-liners; require an explanation)
  - ``severity_before`` in {"blocker", "warning"}
  - ``severity_after`` in {"info", "warning", "waived"}
  - severity ranking: severity_after must be lower than severity_before
    (no upgrades, no no-ops). Ranking: blocker > warning > info > waived
  - ``expires_on`` is a valid ISO date AND in the future relative to now
  - ``allowed_runs`` is a list of strings (may be empty meaning all runs)
  - ``gate`` is NOT in ``release_policy.never_waivable``

The validator returns finding codes that the release catalog uses to
match waivers against findings.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_WAIVER_DIR = REPO_ROOT / "config" / "waivers"
DEFAULT_POLICY_PATH = REPO_ROOT / "config" / "release_policy.yaml"

WAIVER_ID_RE = re.compile(r"^WV-\d{4}-\d{2}-\d{3}$")
SEVERITY_RANK = {"blocker": 4, "warning": 3, "info": 2, "waived": 1}
ALLOWED_SEVERITY_BEFORE = {"blocker", "warning"}
ALLOWED_SEVERITY_AFTER = {"info", "warning", "waived"}
MIN_REASON_LEN = 10


@dataclass
class Waiver:
    id: str
    gate: str
    owner: str
    approved_by: str
    reason: str
    severity_before: str
    severity_after: str
    expires_on: date
    allowed_runs: list[str] = field(default_factory=list)
    finding_path: str | None = None
    source_path: Path | None = None

    def applies_to_run(self, run_id: str | None) -> bool:
        if not self.allowed_runs:
            return True
        return run_id is not None and run_id in self.allowed_runs

    def is_expired(self, *, now: date | None = None) -> bool:
        now = now or date.today()
        return self.expires_on < now

    def as_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "gate": self.gate,
            "owner": self.owner,
            "approved_by": self.approved_by,
            "reason": self.reason,
            "severity_before": self.severity_before,
            "severity_after": self.severity_after,
            "expires_on": self.expires_on.isoformat(),
            "allowed_runs": list(self.allowed_runs),
            "finding_path": self.finding_path,
            "source_path": str(self.source_path) if self.source_path else None,
        }


@dataclass
class ReleasePolicy:
    schema_version: str
    never_waivable: set[str]
    severity_overrides: dict[str, str]

    def is_never_waivable(self, gate: str) -> bool:
        return gate in self.never_waivable


def load_policy(path: Path | None = None) -> ReleasePolicy:
    p = path or DEFAULT_POLICY_PATH
    raw = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return ReleasePolicy(
        schema_version=str(raw.get("schema_version", "")),
        never_waivable=set(raw.get("never_waivable") or []),
        severity_overrides=dict(raw.get("severity_overrides") or {}),
    )


class WaiverError(ValueError):
    """Raised when a waiver violates the strict rules."""


def _parse_waiver(
    raw: dict[str, Any], *, source_path: Path, policy: ReleasePolicy
) -> Waiver:
    def get(field_name: str, *, required: bool = True) -> Any:
        val = raw.get(field_name)
        if required and (val is None or (isinstance(val, str) and not val.strip())):
            raise WaiverError(
                f"{source_path.name}: waiver missing required field {field_name!r}"
            )
        return val

    wid = str(get("id"))
    if not WAIVER_ID_RE.match(wid):
        raise WaiverError(
            f"{source_path.name}: id {wid!r} does not match WV-YYYY-MM-NNN"
        )

    gate = str(get("gate"))
    if "." not in gate:
        raise WaiverError(
            f"{source_path.name}: gate {gate!r} must use <validator>.<finding_code> format"
        )
    if policy.is_never_waivable(gate):
        raise WaiverError(
            f"{source_path.name}: gate {gate!r} is in release_policy.never_waivable; cannot waive"
        )

    owner = str(get("owner"))
    approved_by = str(get("approved_by"))
    reason = str(get("reason"))
    if len(reason.strip()) < MIN_REASON_LEN:
        raise WaiverError(
            f"{source_path.name}: reason must be at least {MIN_REASON_LEN} chars, got {len(reason.strip())}"
        )

    sev_before = str(get("severity_before"))
    sev_after = str(get("severity_after"))
    if sev_before not in ALLOWED_SEVERITY_BEFORE:
        raise WaiverError(
            f"{source_path.name}: severity_before {sev_before!r} not in {sorted(ALLOWED_SEVERITY_BEFORE)}"
        )
    if sev_after not in ALLOWED_SEVERITY_AFTER:
        raise WaiverError(
            f"{source_path.name}: severity_after {sev_after!r} not in {sorted(ALLOWED_SEVERITY_AFTER)}"
        )
    if SEVERITY_RANK[sev_after] >= SEVERITY_RANK[sev_before]:
        raise WaiverError(
            f"{source_path.name}: severity_after {sev_after!r} must be lower than "
            f"severity_before {sev_before!r}; waivers cannot upgrade or no-op"
        )

    expires_raw = get("expires_on")
    if isinstance(expires_raw, date):
        expires_on = expires_raw
    else:
        try:
            expires_on = date.fromisoformat(str(expires_raw))
        except ValueError as e:
            raise WaiverError(
                f"{source_path.name}: expires_on {expires_raw!r} is not ISO YYYY-MM-DD"
            ) from e

    allowed_runs = list(raw.get("allowed_runs") or [])
    finding_path = raw.get("finding_path")
    if finding_path is not None:
        finding_path = str(finding_path)

    return Waiver(
        id=wid,
        gate=gate,
        owner=owner,
        approved_by=approved_by,
        reason=reason.strip(),
        severity_before=sev_before,
        severity_after=sev_after,
        expires_on=expires_on,
        allowed_runs=allowed_runs,
        finding_path=finding_path,
        source_path=source_path,
    )


def load_waivers(
    waiver_dir: Path | None = None,
    *,
    policy: ReleasePolicy | None = None,
    now: date | None = None,
    skip_expired: bool = False,
    skip_examples: bool = False,
) -> tuple[list[Waiver], list[dict[str, Any]]]:
    """Load every waiver YAML in waiver_dir.

    Returns ``(waivers, findings)``. ``findings`` describes any waiver
    file that failed to parse — those waivers are excluded from the
    returned list. The release catalog surfaces these as warnings.

    skip_examples: skip files whose name starts with "EXAMPLE" (the
    repo ships an example demonstrative waiver; CI runs it but real
    operators may want to filter examples out).
    """
    waiver_dir = waiver_dir or DEFAULT_WAIVER_DIR
    policy = policy or load_policy()
    now = now or date.today()

    waivers: list[Waiver] = []
    findings: list[dict[str, Any]] = []

    if not waiver_dir.exists():
        return waivers, findings

    for path in sorted(waiver_dir.glob("*.yaml")):
        if skip_examples and path.name.startswith("EXAMPLE"):
            continue
        try:
            raw = yaml.safe_load(path.read_text(encoding="utf-8"))
            if not isinstance(raw, dict):
                raise WaiverError(f"{path.name}: must be a YAML mapping")
            waiver = _parse_waiver(raw, source_path=path, policy=policy)
            if waiver.is_expired(now=now):
                if skip_expired:
                    continue
                findings.append(
                    {
                        "severity": "warning",
                        "code": "waiver_expired",
                        "path": str(path),
                        "message": (
                            f"waiver {waiver.id} expired on {waiver.expires_on.isoformat()}; "
                            f"not applied"
                        ),
                    }
                )
                continue
            waivers.append(waiver)
        except WaiverError as e:
            findings.append(
                {
                    "severity": "blocker",
                    "code": "waiver_invalid",
                    "path": str(path),
                    "message": str(e),
                }
            )
        except Exception as e:
            findings.append(
                {
                    "severity": "blocker",
                    "code": "waiver_parse_error",
                    "path": str(path),
                    "message": f"could not parse: {e}",
                }
            )

    return waivers, findings


__all__ = [
    "DEFAULT_POLICY_PATH",
    "DEFAULT_WAIVER_DIR",
    "ReleasePolicy",
    "SEVERITY_RANK",
    "Waiver",
    "WaiverError",
    "load_policy",
    "load_waivers",
]
