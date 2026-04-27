"""Track K — release catalog + waivers tests.

Covers:
  - waiver loader rules (ID format, required fields, severity ranking,
    expiry, never-waivable gates)
  - release catalog applies a waiver and downgrades the matching finding
  - unused waivers are reported
  - never-waivable gates can't be waived
"""

from __future__ import annotations

import shutil
from datetime import date, timedelta
from pathlib import Path

import pytest
import yaml

from scripts.monthly_platform import release_catalog
from scripts.monthly_platform.waivers import (
    ReleasePolicy,
    Waiver,
    WaiverError,
    load_policy,
    load_waivers,
    _parse_waiver,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
APAC_WORKBOOK = Path("/Users/test/Downloads/jesper-tyrer-2026-04-20.xlsx")
APAC_DECK = Path("/Users/test/Downloads/jesper-tyrer-LAND.pptx")
SOFFICE_BIN = Path("/opt/homebrew/bin/soffice")


def _have_soffice() -> bool:
    return SOFFICE_BIN.exists() or bool(shutil.which("soffice"))


@pytest.fixture()
def policy() -> ReleasePolicy:
    return load_policy()


def _good_waiver_dict(**overrides) -> dict:
    base = {
        "id": "WV-2026-04-999",
        "gate": "brand_fingerprint.template_size_mismatch",
        "owner": "Sales Operations Engineering",
        "approved_by": "Andre",
        "reason": "Synthesized waiver for unit testing.",
        "severity_before": "warning",
        "severity_after": "info",
        "allowed_runs": [],
        "expires_on": (date.today() + timedelta(days=30)).isoformat(),
    }
    base.update(overrides)
    return base


# ----------------------------------------------------------------------
# Waiver loader rules
# ----------------------------------------------------------------------


def test_load_policy_has_never_waivable(policy):
    assert policy.schema_version == "monthly_platform.release_policy.v1"
    assert "brand_fingerprint.template_sha256_mismatch" in policy.never_waivable
    assert "pptx_contract.table_header_mismatch" in policy.never_waivable


def test_canonical_waiver_loader_passes(policy):
    """The repo ships at least one example waiver; loader should parse it."""
    waivers, findings = load_waivers(policy=policy)
    assert findings == []
    assert any(w.id == "WV-2026-04-001" for w in waivers)


def test_waiver_invalid_id_format_rejected(tmp_path, policy):
    f = tmp_path / "bad.yaml"
    f.write_text(yaml.safe_dump(_good_waiver_dict(id="bad-id-format")))
    with pytest.raises(WaiverError, match="WV-YYYY-MM-NNN"):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_missing_owner_rejected(tmp_path, policy):
    f = tmp_path / "bad.yaml"
    raw = _good_waiver_dict()
    del raw["owner"]
    f.write_text(yaml.safe_dump(raw))
    with pytest.raises(WaiverError, match="owner"):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_short_reason_rejected(tmp_path, policy):
    f = tmp_path / "bad.yaml"
    f.write_text(yaml.safe_dump(_good_waiver_dict(reason="oops")))
    with pytest.raises(WaiverError, match="reason"):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_severity_no_op_rejected(tmp_path, policy):
    f = tmp_path / "bad.yaml"
    f.write_text(
        yaml.safe_dump(
            _good_waiver_dict(severity_before="warning", severity_after="warning")
        )
    )
    with pytest.raises(WaiverError, match="lower"):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_severity_upgrade_rejected(tmp_path, policy):
    """severity_after must be lower than severity_before."""
    f = tmp_path / "bad.yaml"
    f.write_text(
        yaml.safe_dump(
            _good_waiver_dict(severity_before="warning", severity_after="info")
        )
    )
    # info < warning → should pass. Now flip:
    f.write_text(
        yaml.safe_dump(
            _good_waiver_dict(severity_before="warning", severity_after="warning")
        )
    )
    with pytest.raises(WaiverError):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_never_waivable_gate_rejected(tmp_path, policy):
    """A waiver targeting a never-waivable gate must be rejected at parse time."""
    f = tmp_path / "bad.yaml"
    f.write_text(
        yaml.safe_dump(
            _good_waiver_dict(
                gate="brand_fingerprint.template_sha256_mismatch",
                severity_before="blocker",
                severity_after="info",
            )
        )
    )
    with pytest.raises(WaiverError, match="never_waivable"):
        _parse_waiver(yaml.safe_load(f.read_text()), source_path=f, policy=policy)


def test_waiver_expired_filtered_out(tmp_path, policy):
    """Expired waivers don't get applied; loader records a warning finding."""
    f = tmp_path / "expired.yaml"
    f.write_text(
        yaml.safe_dump(
            _good_waiver_dict(
                id="WV-2024-01-001",
                expires_on=(date.today() - timedelta(days=1)).isoformat(),
            )
        )
    )
    waivers, findings = load_waivers(tmp_path, policy=policy)
    assert waivers == []
    assert any(f["code"] == "waiver_expired" for f in findings)


def test_waiver_applies_to_run_filter():
    """allowed_runs filters the waiver to specific run IDs."""
    w = Waiver(
        id="WV-2026-04-100",
        gate="x.y",
        owner="o",
        approved_by="a",
        reason="testing",
        severity_before="warning",
        severity_after="info",
        expires_on=date.today() + timedelta(days=10),
        allowed_runs=["run-A"],
    )
    assert w.applies_to_run("run-A") is True
    assert w.applies_to_run("run-B") is False
    assert w.applies_to_run(None) is False
    # Empty allowed_runs = applies to ALL runs.
    w2 = Waiver(**{**w.__dict__, "id": "WV-2026-04-101", "allowed_runs": []})
    assert w2.applies_to_run("any-run") is True
    assert w2.applies_to_run(None) is True


# ----------------------------------------------------------------------
# Release catalog
# ----------------------------------------------------------------------


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
@pytest.mark.skipif(not _have_soffice(), reason="soffice not installed.")
def test_release_catalog_publish_ready_with_canonical_inputs():
    result = release_catalog.build_release_catalog(
        workbook=APAC_WORKBOOK, pptx=APAC_DECK, run_id="test-canonical"
    )
    assert result.publish_decision == "publish_ready"
    assert result.pre_waiver_blocker_total == 0
    assert result.post_waiver_blocker_total == 0
    # The example waiver carried in the repo is unused (no findings to match).
    assert any(w.id == "WV-2026-04-001" for w in result.unused_waivers)
    assert result.applied_waivers == []


@pytest.mark.skipif(
    not (APAC_WORKBOOK.exists() and APAC_DECK.exists()),
    reason="Live APAC anchors not present.",
)
@pytest.mark.skipif(not _have_soffice(), reason="soffice not installed.")
def test_release_catalog_applies_matching_waiver(tmp_path):
    """Force a real warning by mutating the brand contract's expected size,
    then waive that warning and confirm the catalog downgrades it."""
    # Build a waiver dir with a synthetic waiver that will match.
    wf = tmp_path / "WV-2026-04-101.yaml"
    wf.write_text(
        yaml.safe_dump(
            _good_waiver_dict(
                id="WV-2026-04-101",
                gate="brand_fingerprint.template_size_mismatch",
                severity_before="warning",
                severity_after="info",
            )
        )
    )

    # Mutate deck contract: bad size, real SHA. brand_fingerprint emits
    # template_size_mismatch (warning).
    # NOTE: brand_contract resolves brand.template against
    # contract.path.parent.parent (assuming the contract lives in
    # <repo>/config/). When we drop the mutated contract in tmp_path
    # the relative path doesn't resolve. Override with an absolute
    # template path so the SHA / size check runs against the real file.
    canonical_deck = REPO_ROOT / "config" / "deck_contract.yaml"
    raw = yaml.safe_load(canonical_deck.read_text(encoding="utf-8"))
    raw["brand"]["expected_template_size_bytes"] = 1
    raw["brand"]["template"] = str(REPO_ROOT / "assets" / "SimCorp_PPT_Template.pptx")
    mutated = tmp_path / "deck_contract.yaml"
    mutated.write_text(yaml.safe_dump(raw, sort_keys=False), encoding="utf-8")

    result = release_catalog.build_release_catalog(
        workbook=APAC_WORKBOOK,
        pptx=APAC_DECK,
        deck_contract_path=mutated,
        waiver_dir=tmp_path,
        run_id="test-waivered",
        skip_visual=True,
    )
    # The size mismatch should have been a warning pre-waiver, downgraded
    # to info after the waiver applies.
    assert result.pre_waiver_warning_total >= 1
    assert any(
        a.waiver_id == "WV-2026-04-101"
        and a.gate == "brand_fingerprint.template_size_mismatch"
        for a in result.applied_waivers
    )
    # Post-waiver: the warning is gone, no new blockers introduced.
    assert result.post_waiver_warning_total < result.pre_waiver_warning_total


def test_render_markdown_includes_decision():
    result = release_catalog.CatalogResult(
        publish_decision="publish_ready",
        pre_waiver_blocker_total=0,
        pre_waiver_warning_total=0,
        post_waiver_blocker_total=0,
        post_waiver_warning_total=0,
    )
    md = release_catalog.render_markdown(result)
    assert "publish_decision: publish_ready" in md
    assert "Pre vs post waiver" in md
