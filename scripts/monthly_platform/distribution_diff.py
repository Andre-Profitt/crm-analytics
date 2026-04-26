"""Track D activation — run-over-run distribution diff.

Reads two ``source_extract_quality_audit.json`` payloads (a prior approved
run and the current run) and produces a side-by-side diff of every source's
``distribution_comparison`` block. The diff is **information only** — it
emits no findings and never gates a release. Its purpose is to give a
reviewer one artifact that answers "what changed since the last approved
run?" without opening raw extracts or two audit JSONs.

Output shape (per-source)::

    {
      "source_key": "sd_pipeline_open.apac.current_quarter.Q2.<id>",
      "baseline_key": "sd_pipeline_open.apac.current_quarter",
      "row_count_delta": 13 - 11,                         # ints, signed
      "dimensions": [
        {
          "field": "StageName",
          "semantic_name": "stage",
          "new_categories":     ["6 - Contracting"],      # in current, not prior
          "dropped_categories": ["1 - Prospecting"],      # in prior, not current
          "share_deltas": [
            {"category": "3 - Engagement",
             "prior_share": 0.30, "current_share": 0.45,
             "abs_delta": 0.15},
            ...
          ]
        },
        ...
      ]
    }

Pure: never mutates inputs or files. The caller (``extract_salesforce_sources``)
writes the artifact when the user passes ``--prior-quality-audit``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from scripts.monthly_platform.contracts import utc_now_iso


SCHEMA_VERSION = "monthly_platform.source_distribution_diff.v1"


def _comparisons_by_source_key(audit: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index per-source distribution payloads by source_key.

    ``status == "no_policy"`` entries are excluded: those sources are not
    opted into Track D, so including them would inflate
    ``compared_source_count`` / ``new_source_count`` / ``dropped_source_count``
    with sources that have no Track D semantics on either side. The diff is
    intended to surface like-for-like changes for opted-in sources only.
    """
    block = audit.get("distribution_comparison") or {}
    return {
        c["source_key"]: c
        for c in block.get("comparisons", [])
        if c.get("status") != "no_policy"
    }


def _baseline_key_from_source_key(source_key: str) -> str:
    """Strip ``.{quarter_label}.{salesforce_id}`` to recover the baseline key.

    The source_key is built as
    ``{requirement_id}.{territory_slug}.{period_role}.{quarter_label}.{salesforce_id}``;
    the baseline key is the first three components. We split from the right
    on `.` because territory slugs can contain `.`-free underscores but never
    a literal dot.
    """
    parts = source_key.split(".")
    if len(parts) < 5:
        return source_key
    # requirement_id may itself contain underscores but not dots, so the first
    # three dot-separated segments are unambiguously the baseline key.
    return ".".join(parts[:3])


def _diff_dimension(
    *,
    prior: dict[str, Any] | None,
    current: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Diff one dimension payload across two audits. Returns ``None`` when both sides are absent."""
    if prior is None and current is None:
        return None
    field = (current or prior or {}).get("field", "")
    semantic = (current or prior or {}).get("semantic_name")
    prior_shares: dict[str, float] = (prior or {}).get("current_shares") or {}
    current_shares: dict[str, float] = (current or {}).get("current_shares") or {}
    prior_keys = set(prior_shares.keys())
    current_keys = set(current_shares.keys())
    new_categories = sorted(current_keys - prior_keys)
    dropped_categories = sorted(prior_keys - current_keys)
    share_deltas: list[dict[str, Any]] = []
    for category in sorted(prior_keys | current_keys):
        prior_share = float(prior_shares.get(category, 0.0))
        current_share = float(current_shares.get(category, 0.0))
        share_deltas.append(
            {
                "category": category,
                "prior_share": round(prior_share, 6),
                "current_share": round(current_share, 6),
                "abs_delta": round(abs(current_share - prior_share), 6),
            }
        )
    seed_status_prior = (prior or {}).get("seed_status")
    seed_status_current = (current or {}).get("seed_status")
    # Like-for-like only: a transition counts only when BOTH prior and
    # current have a seed_status for this dimension AND they differ.
    # New sources, dropped sources, and dimensions added/removed mid-cycle
    # are surfaced via ``presence`` / ``new_categories`` / ``dropped_categories``
    # — not as seed-status transitions, which would otherwise inflate
    # ``seed_status_changed_count`` with non-transitions.
    seed_status_changed = (
        seed_status_prior is not None
        and seed_status_current is not None
        and seed_status_prior != seed_status_current
    )
    return {
        "field": field,
        "semantic_name": semantic,
        "new_categories": new_categories,
        "dropped_categories": dropped_categories,
        "share_deltas": share_deltas,
        "seed_status_prior": seed_status_prior,
        "seed_status_current": seed_status_current,
        "seed_status_changed": seed_status_changed,
    }


def _diff_source(
    *,
    prior_payload: dict[str, Any] | None,
    current_payload: dict[str, Any] | None,
) -> dict[str, Any]:
    payload = current_payload or prior_payload or {}
    source_key = payload.get("source_key", "")
    prior_dims = {
        d["field"]: d
        for d in (prior_payload or {}).get("dimensions", [])
        if d.get("field")
    }
    current_dims = {
        d["field"]: d
        for d in (current_payload or {}).get("dimensions", [])
        if d.get("field")
    }
    fields = sorted(prior_dims.keys() | current_dims.keys())
    dim_diffs = [
        d
        for d in (
            _diff_dimension(prior=prior_dims.get(f), current=current_dims.get(f))
            for f in fields
        )
        if d is not None
    ]
    prior_row_count = (prior_payload or {}).get("row_count")
    current_row_count = (current_payload or {}).get("row_count")
    if isinstance(prior_row_count, int) and isinstance(current_row_count, int):
        row_count_delta: int | None = current_row_count - prior_row_count
    else:
        row_count_delta = None
    presence: str
    if prior_payload is None and current_payload is not None:
        presence = "new_source"
    elif prior_payload is not None and current_payload is None:
        presence = "dropped_source"
    else:
        presence = "both"
    return {
        "source_key": source_key,
        "baseline_key": _baseline_key_from_source_key(source_key),
        "presence": presence,
        "prior_row_count": prior_row_count,
        "current_row_count": current_row_count,
        "row_count_delta": row_count_delta,
        "dimensions": dim_diffs,
    }


def diff_distribution_audits(
    *,
    prior: dict[str, Any],
    current: dict[str, Any],
) -> dict[str, Any]:
    """Build a run-over-run distribution diff payload.

    Both audits must be ``source_extract_quality_audit.v1`` payloads with the
    Track D ``distribution_comparison`` block. Sources present in only one of
    the two runs are still surfaced (``presence`` field on the per-source
    diff), so dropped or newly-added sources are visible.
    """
    prior_block = prior.get("distribution_comparison") or {}
    current_block = current.get("distribution_comparison") or {}
    prior_by_key = _comparisons_by_source_key(prior)
    current_by_key = _comparisons_by_source_key(current)
    all_keys = sorted(set(prior_by_key.keys()) | set(current_by_key.keys()))
    per_source = [
        _diff_source(
            prior_payload=prior_by_key.get(k),
            current_payload=current_by_key.get(k),
        )
        for k in all_keys
    ]
    new_source_count = sum(1 for d in per_source if d["presence"] == "new_source")
    dropped_source_count = sum(
        1 for d in per_source if d["presence"] == "dropped_source"
    )
    seed_status_changed_count = sum(
        1
        for src in per_source
        for dim in src["dimensions"]
        if dim["seed_status_changed"]
    )
    new_category_count = sum(
        len(dim["new_categories"]) for src in per_source for dim in src["dimensions"]
    )
    dropped_category_count = sum(
        len(dim["dropped_categories"])
        for src in per_source
        for dim in src["dimensions"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": utc_now_iso(),
        "prior_run": {
            "snapshot_date": prior.get("snapshot_date"),
            "run_id": prior.get("run_id"),
            "matched_source_count": prior_block.get("matched_source_count"),
        },
        "current_run": {
            "snapshot_date": current.get("snapshot_date"),
            "run_id": current.get("run_id"),
            "matched_source_count": current_block.get("matched_source_count"),
        },
        "summary": {
            "compared_source_count": len(per_source),
            "new_source_count": new_source_count,
            "dropped_source_count": dropped_source_count,
            "seed_status_changed_count": seed_status_changed_count,
            "new_category_count": new_category_count,
            "dropped_category_count": dropped_category_count,
        },
        "comparisons": per_source,
    }


def load_audit(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


__all__ = [
    "SCHEMA_VERSION",
    "diff_distribution_audits",
    "load_audit",
]
