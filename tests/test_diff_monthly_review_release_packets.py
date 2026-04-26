import sys
from pathlib import Path

from scripts import diff_monthly_review_release_packets as diff_script


def test_resolve_baseline_date_picks_latest_prior_packet(
    tmp_path: Path, monkeypatch
) -> None:
    packet_root = tmp_path / "output" / "monthly_review_release_packets"
    for run_date in ["2026-04-22", "2026-04-23", "2026-05-01"]:
        path = packet_root / run_date
        path.mkdir(parents=True, exist_ok=True)
        (path / "legacy_monthly_review_release_packet.json").write_text(
            "{}",
            encoding="utf-8",
        )

    monkeypatch.setattr(diff_script, "PACKET_ROOT", packet_root)

    assert diff_script._resolve_baseline_date("2026-05-01") == "2026-04-23"
    assert diff_script._resolve_baseline_date("2026-04-22") is None


def test_build_snapshot_diff_surfaces_gate_and_blocker_deltas() -> None:
    baseline_payload = {
        "run_date": "2026-04-22",
        "status": "blocked",
        "publish_ready": False,
        "pipeline_ok": False,
        "step_counts": {"ok": 10, "failed": 1, "blocked": 0, "other": 0, "total": 11},
        "output_counts": {"extracts": 9, "decks": 10, "reports": 2},
        "publish_blockers": ["Deck font audit is `warning` with `9` deck(s) showing issues."],
        "pipeline_blockers": ["Pipeline step failure: 4_validate_tie_out: failed."],
        "source_contract": {
            "active_lane_status": "ok",
            "candidate_forward_status": "ok",
        },
        "data_quality": {
            "gap_changes": 18,
            "baseline_changes": 2,
            "critical_backlog_after": 456,
            "important_backlog_after": 6061,
        },
        "deck_font_audit": {
            "status": "warning",
            "deck_count": 10,
            "decks_with_issues": 9,
            "failure_count": 0,
        },
        "tie_out": {
            "status": "failed",
            "checks": 90,
            "mismatches": 2,
            "directors_audited": 9,
        },
    }
    current_payload = {
        "run_date": "2026-04-23",
        "status": "ok",
        "publish_ready": True,
        "pipeline_ok": True,
        "step_counts": {"ok": 48, "failed": 0, "blocked": 0, "other": 0, "total": 48},
        "output_counts": {"extracts": 9, "decks": 10, "reports": 2},
        "publish_blockers": [],
        "pipeline_blockers": [],
        "source_contract": {
            "active_lane_status": "ok",
            "candidate_forward_status": "ok",
        },
        "data_quality": {
            "gap_changes": 12,
            "baseline_changes": 1,
            "critical_backlog_after": 460,
            "important_backlog_after": 6058,
        },
        "deck_font_audit": {
            "status": "ok",
            "deck_count": 10,
            "decks_with_issues": 0,
            "failure_count": 0,
        },
        "tie_out": {
            "status": "ok",
            "checks": 90,
            "mismatches": 0,
            "directors_audited": 9,
        },
    }

    payload = diff_script.build_snapshot_diff(baseline_payload, current_payload)

    assert payload["release_packet"]["status_before"] == "blocked"
    assert payload["release_packet"]["status_after"] == "ok"
    assert payload["release_packet"]["publish_ready_before"] is False
    assert payload["release_packet"]["publish_ready_after"] is True
    assert payload["release_packet"]["pipeline_blocker_changes"]["resolved"] == [
        "Pipeline step failure: 4_validate_tie_out: failed."
    ]
    assert payload["release_packet"]["publish_blocker_changes"]["resolved"] == [
        "Deck font audit is `warning` with `9` deck(s) showing issues."
    ]
    assert payload["release_packet"]["gate_changes"]["deck_font_audit"] == {
        "decks_with_issues": {"before": 9, "after": 0, "delta": -9.0},
        "status": {"before": "warning", "after": "ok"},
    }
    assert payload["release_packet"]["gate_changes"]["data_quality"] == {
        "baseline_changes": {"before": 2, "after": 1, "delta": -1.0},
        "critical_backlog_after": {"before": 456, "after": 460, "delta": 4.0},
        "gap_changes": {"before": 18, "after": 12, "delta": -6.0},
        "important_backlog_after": {"before": 6061, "after": 6058, "delta": -3.0},
    }


def test_main_writes_skipped_diff_when_no_baseline_exists(
    tmp_path: Path, monkeypatch
) -> None:
    packet_root = tmp_path / "output" / "monthly_review_release_packets"
    output_root = tmp_path / "output" / "monthly_review_release_packet_snapshot_diff"
    current_dir = packet_root / "2026-04-22"
    current_dir.mkdir(parents=True, exist_ok=True)
    (current_dir / "legacy_monthly_review_release_packet.json").write_text(
        '{"run_date":"2026-04-22","status":"ok","publish_ready":true,"pipeline_ok":true}',
        encoding="utf-8",
    )

    monkeypatch.setattr(diff_script, "PACKET_ROOT", packet_root)
    monkeypatch.setattr(diff_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "diff_monthly_review_release_packets.py",
            "--current-date",
            "2026-04-22",
        ],
    )

    assert diff_script.main() == 0
    summary = output_root / "2026-04-22" / "summary.md"
    payload = (
        output_root
        / "2026-04-22"
        / "monthly_review_release_packet_snapshot_diff.json"
    )

    assert summary.exists()
    assert payload.exists()
    assert '"status": "skipped"' in payload.read_text(encoding="utf-8")
