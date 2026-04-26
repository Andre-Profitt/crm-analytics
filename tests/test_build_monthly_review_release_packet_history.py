import json
import sys
from pathlib import Path

from scripts import build_monthly_review_release_packet_history as history_script


def test_build_release_packet_history_payload_summarizes_runs(tmp_path: Path) -> None:
    packet_root = tmp_path / "output" / "monthly_review_release_packets"
    packet_diff_root = tmp_path / "output" / "monthly_review_release_packet_snapshot_diff"
    for (
        run_date,
        status,
        publish_ready,
        pipeline_ok,
        critical,
        important,
        publish_blockers,
        pipeline_blockers,
    ) in [
        ("2026-04-22", "ok", True, True, 456, 6061, [], []),
        ("2026-04-23", "ok", True, True, 460, 6058, [], []),
        (
            "2026-04-24",
            "blocked",
            False,
            False,
            470,
            6070,
            ["Deck font audit is `failed` with `9` deck(s) showing issues."],
            ["Pipeline step failure: 4_validate_tie_out: failed."],
        ),
    ]:
        run_dir = packet_root / run_date
        run_dir.mkdir(parents=True, exist_ok=True)
        (run_dir / "legacy_monthly_review_release_packet.json").write_text(
            json.dumps(
                {
                    "run_date": run_date,
                    "status": status,
                    "publish_ready": publish_ready,
                    "pipeline_ok": pipeline_ok,
                    "step_counts": {"total": 48, "failed": 0},
                    "output_counts": {"extracts": 9, "decks": 10, "reports": 2},
                    "publish_blockers": publish_blockers,
                    "pipeline_blockers": pipeline_blockers,
                    "source_contract": {"active_lane_status": "ok"},
                    "data_quality": {
                        "gap_changes": 10,
                        "critical_backlog_after": critical,
                        "important_backlog_after": important,
                    },
                    "workbook_contract": {"status": "ok", "validated_count": 9},
                    "deck_font_audit": {"status": "ok", "decks_with_issues": 0},
                    "tie_out": {"status": "ok", "mismatches": 0},
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    diff_dir = packet_diff_root / "2026-04-23"
    diff_dir.mkdir(parents=True, exist_ok=True)
    (diff_dir / "monthly_review_release_packet_snapshot_diff.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "baseline_run_date": "2026-04-22",
                "current_run_date": "2026-04-23",
                "release_packet": {
                    "status_before": "ok",
                    "status_after": "ok",
                    "publish_ready_before": True,
                    "publish_ready_after": True,
                    "pipeline_ok_before": True,
                    "pipeline_ok_after": True,
                    "publish_blocker_changes": {"added": [], "resolved": []},
                    "pipeline_blocker_changes": {"added": [], "resolved": []},
                    "changed_gates": ["data_quality"],
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    diff_dir = packet_diff_root / "2026-04-24"
    diff_dir.mkdir(parents=True, exist_ok=True)
    (diff_dir / "monthly_review_release_packet_snapshot_diff.json").write_text(
        json.dumps(
            {
                "status": "ok",
                "baseline_run_date": "2026-04-23",
                "current_run_date": "2026-04-24",
                "release_packet": {
                    "status_before": "ok",
                    "status_after": "blocked",
                    "publish_ready_before": True,
                    "publish_ready_after": False,
                    "pipeline_ok_before": True,
                    "pipeline_ok_after": False,
                    "publish_blocker_changes": {
                        "added": [
                            "Deck font audit is `failed` with `9` deck(s) showing issues."
                        ],
                        "resolved": [],
                    },
                    "pipeline_blocker_changes": {
                        "added": ["Pipeline step failure: 4_validate_tie_out: failed."],
                        "resolved": [],
                    },
                    "changed_gates": ["data_quality"],
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    payload = history_script.build_release_packet_history_payload(
        packet_root=packet_root,
        packet_diff_root=packet_diff_root,
    )

    assert payload["run_count"] == 3
    assert payload["green_run_count"] == 2
    assert payload["blocked_run_count"] == 1
    assert payload["current_green_streak"] == 0
    assert payload["latest_run_date"] == "2026-04-24"
    assert payload["latest_status"] == "blocked"
    assert payload["latest_blocked_run_date"] == "2026-04-24"
    assert payload["latest_blocked_publish_blockers"] == [
        "Deck font audit is `failed` with `9` deck(s) showing issues."
    ]
    assert payload["latest_blocked_pipeline_blockers"] == [
        "Pipeline step failure: 4_validate_tie_out: failed."
    ]
    assert payload["latest_core_state_transition"] == {
        "baseline_run_date": "2026-04-23",
        "current_run_date": "2026-04-24",
        "status_before": "ok",
        "status_after": "blocked",
        "publish_ready_before": True,
        "publish_ready_after": False,
        "pipeline_ok_before": True,
        "pipeline_ok_after": False,
        "changed_gates": ["data_quality"],
        "changed_gate_count": 1,
        "publish_blockers_added": [
            "Deck font audit is `failed` with `9` deck(s) showing issues."
        ],
        "publish_blockers_resolved": [],
        "pipeline_blockers_added": [
            "Pipeline step failure: 4_validate_tie_out: failed."
        ],
        "pipeline_blockers_resolved": [],
        "gate_change_summaries": ["data_quality"],
        "core_state_changes": [
            "status `ok` -> `blocked`",
            "publish_ready `True` -> `False`",
        ],
    }
    assert payload["recurring_publish_blockers"] == [
        {
            "blocker": "Deck font audit is `failed` with `9` deck(s) showing issues.",
            "count": 1,
            "latest_run_date": "2026-04-24",
        }
    ]
    assert payload["recurring_pipeline_blockers"] == [
        {
            "blocker": "Pipeline step failure: 4_validate_tie_out: failed.",
            "count": 1,
            "latest_run_date": "2026-04-24",
        }
    ]
    assert payload["latest_packet_diff"] == {
        "baseline_run_date": "2026-04-23",
        "current_run_date": "2026-04-24",
        "status_before": "ok",
        "status_after": "blocked",
        "publish_ready_before": True,
        "publish_ready_after": False,
        "pipeline_ok_before": True,
        "pipeline_ok_after": False,
        "changed_gates": ["data_quality"],
        "changed_gate_count": 1,
        "publish_blockers_added": [
            "Deck font audit is `failed` with `9` deck(s) showing issues."
        ],
        "publish_blockers_resolved": [],
        "pipeline_blockers_added": [
            "Pipeline step failure: 4_validate_tie_out: failed."
        ],
        "pipeline_blockers_resolved": [],
        "gate_change_summaries": ["data_quality"],
    }
    assert payload["green_gate_drift"] == [
        {
            "baseline_run_date": "2026-04-22",
            "current_run_date": "2026-04-23",
            "status_before": "ok",
            "status_after": "ok",
            "publish_ready_before": True,
            "publish_ready_after": True,
            "pipeline_ok_before": True,
            "pipeline_ok_after": True,
            "changed_gates": ["data_quality"],
            "changed_gate_count": 1,
            "publish_blockers_added": [],
            "publish_blockers_resolved": [],
            "pipeline_blockers_added": [],
            "pipeline_blockers_resolved": [],
            "gate_change_summaries": ["data_quality"],
        }
    ]
    assert payload["blocked_runs"][0]["run_date"] == "2026-04-24"
    assert payload["entries"][0]["run_date"] == "2026-04-24"
    assert payload["entries"][1]["run_date"] == "2026-04-23"
    summary = history_script.build_release_packet_history_markdown(payload)
    assert (
        "Latest core state transition publish blockers added: "
        "`['Deck font audit is `failed` with `9` deck(s) showing issues.']`"
    ) in summary
    assert (
        "Latest core state transition pipeline blockers added: "
        "`['Pipeline step failure: 4_validate_tie_out: failed.']`"
    ) in summary
    assert "Drift details: `['data_quality']`" in summary


def test_main_writes_history_bundle(tmp_path: Path, monkeypatch) -> None:
    packet_root = tmp_path / "output" / "monthly_review_release_packets"
    output_root = tmp_path / "output" / "monthly_review_release_packet_history"
    run_dir = packet_root / "2026-04-23"
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "legacy_monthly_review_release_packet.json").write_text(
        json.dumps(
            {
                "run_date": "2026-04-23",
                "status": "ok",
                "publish_ready": True,
                "pipeline_ok": True,
                "step_counts": {"total": 48, "failed": 0},
                "output_counts": {"extracts": 9, "decks": 10, "reports": 2},
                "publish_blockers": [],
                "pipeline_blockers": [],
                "data_quality": {
                    "gap_changes": 12,
                    "critical_backlog_after": 460,
                    "important_backlog_after": 6058,
                },
                "deck_font_audit": {"status": "ok", "decks_with_issues": 0},
                "tie_out": {"status": "ok", "mismatches": 0},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(history_script, "PACKET_ROOT", packet_root)
    monkeypatch.setattr(history_script, "PACKET_DIFF_ROOT", tmp_path / "output" / "monthly_review_release_packet_snapshot_diff")
    monkeypatch.setattr(history_script, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(
        sys,
        "argv",
        ["build_monthly_review_release_packet_history.py"],
    )

    assert history_script.main() == 0
    assert (output_root / "history.json").exists()
    assert (output_root / "summary.md").exists()
    assert (output_root / "latest.json").exists()
    summary = (output_root / "summary.md").read_text(encoding="utf-8")
    assert "Runs tracked" in summary
    assert "## Active Exceptions" in summary
    assert "## Core State Transitions" in summary
    assert "## Recurring Blockers" in summary
    assert "## Recent Gate Drift" in summary
