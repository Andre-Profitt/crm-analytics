import json
from pathlib import Path

from scripts.refresh_monthly_review_release_packet import (
    attach_refresh_audit_to_manifest,
    build_refresh_audit_payload,
    refresh_release_packet_for_manifest,
    refresh_release_packets_for_all_manifests,
    write_refresh_audit_bundle,
)


def test_refresh_release_packet_for_manifest_updates_manifest_with_snapshot_diff(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    logs_root = repo_root / "output" / "pipeline_logs" / "2026-04-22"
    logs_root.mkdir(parents=True, exist_ok=True)
    manifest_path = logs_root / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "run_date": "2026-04-22",
                "started_at": "2026-04-22T10:00:00",
                "steps": [{"name": "0_source_contract_preflight", "status": "ok"}],
                "outputs": {"extracts": [], "decks": [], "reports": []},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    source_dir = repo_root / "output" / "source_contract_audit" / "2026-04-22"
    source_dir.mkdir(parents=True, exist_ok=True)
    (source_dir / "source_contract_audit.json").write_text(
        json.dumps(
            {"run_date": "2026-04-22", "active_lane": {"status": "ok"}},
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    packet_root = repo_root / "output" / "monthly_review_release_packets"
    baseline_packet_dir = packet_root / "2026-04-21"
    baseline_packet_dir.mkdir(parents=True, exist_ok=True)
    (baseline_packet_dir / "legacy_monthly_review_release_packet.json").write_text(
        json.dumps(
            {
                "run_date": "2026-04-21",
                "status": "blocked",
                "publish_ready": False,
                "pipeline_ok": False,
                "step_counts": {"ok": 10, "failed": 1, "blocked": 0, "other": 0, "total": 11},
                "output_counts": {"extracts": 0, "decks": 0, "reports": 0},
                "publish_blockers": ["old blocker"],
                "pipeline_blockers": ["old pipeline blocker"],
                "source_contract": {"active_lane_status": "ok", "candidate_forward_status": "ok"},
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    result = refresh_release_packet_for_manifest(
        manifest_path=manifest_path,
        repo_root=repo_root,
        packet_root=packet_root,
        packet_diff_root=repo_root / "output" / "monthly_review_release_packet_snapshot_diff",
    )

    assert result["packet_status"] == "ok"
    assert result["packet_diff_status"] == "ok"
    assert result["semantic_change"] is True
    assert result["semantic_changed_fields"] == ["publish_ready", "status"]

    updated_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    release_packet = updated_manifest["release_packet"]
    assert release_packet["status"] == "ok"
    assert release_packet["snapshot_diff_status"] == "ok"
    assert release_packet["history_generated_at"] is not None
    assert release_packet["history_run_count"] == 2
    assert release_packet["history_green_run_count"] == 1
    assert release_packet["history_blocked_run_count"] == 1
    assert release_packet["history_current_green_streak"] == 1
    assert release_packet["history_latest_core_state_transition_baseline_run_date"] == "2026-04-21"
    assert release_packet["history_latest_core_state_transition_run_date"] == "2026-04-22"
    assert release_packet["history_latest_core_state_transition_changes"] == [
        "status `blocked` -> `ok`",
        "publish_ready `False` -> `True`",
    ]
    assert release_packet[
        "history_latest_core_state_transition_publish_blockers_added"
    ] == []
    assert release_packet[
        "history_latest_core_state_transition_publish_blockers_resolved"
    ] == ["old blocker"]
    assert release_packet[
        "history_latest_core_state_transition_pipeline_blockers_added"
    ] == []
    assert release_packet[
        "history_latest_core_state_transition_pipeline_blockers_resolved"
    ] == ["old pipeline blocker"]
    assert release_packet["history_latest_blocked_run_date"] == "2026-04-21"
    assert release_packet["history_latest_blocked_publish_blockers"] == ["old blocker"]
    assert release_packet["history_latest_blocked_pipeline_blockers"] == [
        "old pipeline blocker"
    ]
    assert release_packet["history_latest_drift_baseline_run_date"] == "2026-04-21"
    assert release_packet["history_latest_drift_run_date"] == "2026-04-22"
    assert release_packet["history_latest_drift_changed_gates"] == ["source_contract"]
    assert release_packet["history_latest_drift_change_summaries"] == [
        "source_contract: candidate_forward_status `ok` -> `None`"
    ]
    assert (repo_root / release_packet["json_path"]).exists()
    assert (repo_root / release_packet["summary_path"]).exists()
    assert (repo_root / release_packet["snapshot_diff_json_path"]).exists()
    assert (repo_root / release_packet["snapshot_diff_summary_path"]).exists()
    assert (repo_root / release_packet["history_json_path"]).exists()
    assert (repo_root / release_packet["history_summary_path"]).exists()


def test_refresh_release_packets_for_all_manifests_sweeps_in_date_order(
    tmp_path: Path,
) -> None:
    repo_root = tmp_path
    logs_root = repo_root / "output" / "pipeline_logs"
    packet_root = repo_root / "output" / "monthly_review_release_packets"
    packet_diff_root = repo_root / "output" / "monthly_review_release_packet_snapshot_diff"

    for run_date in ["2026-04-22", "2026-04-23"]:
        log_dir = logs_root / run_date
        log_dir.mkdir(parents=True, exist_ok=True)
        (log_dir / "manifest.json").write_text(
            json.dumps(
                {
                    "run_date": run_date,
                    "started_at": f"{run_date}T10:00:00",
                    "steps": [{"name": "0_source_contract_preflight", "status": "ok"}],
                    "outputs": {"extracts": [], "decks": [], "reports": []},
                },
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )
        source_dir = repo_root / "output" / "source_contract_audit" / run_date
        source_dir.mkdir(parents=True, exist_ok=True)
        (source_dir / "source_contract_audit.json").write_text(
            json.dumps(
                {"run_date": run_date, "active_lane": {"status": "ok"}},
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

    payload = refresh_release_packets_for_all_manifests(
        logs_root=logs_root,
        repo_root=repo_root,
        packet_root=packet_root,
        packet_diff_root=packet_diff_root,
    )

    assert payload["status"] == "ok"
    assert payload["refreshed_count"] == 2
    assert payload["failure_count"] == 0
    assert [item["run_date"] for item in payload["results"]] == [
        "2026-04-22",
        "2026-04-23",
    ]

    manifest_22 = json.loads(
        (logs_root / "2026-04-22" / "manifest.json").read_text(encoding="utf-8")
    )
    manifest_23 = json.loads(
        (logs_root / "2026-04-23" / "manifest.json").read_text(encoding="utf-8")
    )
    assert manifest_22["release_packet"]["snapshot_diff_status"] == "skipped"
    assert manifest_23["release_packet"]["snapshot_diff_status"] == "ok"
    assert payload["results"][0]["semantic_change"] is True
    assert payload["results"][1]["semantic_change"] is True
    assert payload["results"][0]["semantic_changed_fields"] == [
        "publish_ready",
        "status",
    ]
    assert payload["results"][1]["semantic_changed_fields"] == [
        "publish_ready",
        "status",
    ]


def test_attach_refresh_audit_to_manifest_adds_refresh_paths(tmp_path: Path) -> None:
    manifest_path = tmp_path / "output" / "pipeline_logs" / "2026-04-23" / "manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps(
            {
                "run_date": "2026-04-23",
                "release_packet": {
                    "status": "ok",
                    "json_path": "output/monthly_review_release_packets/2026-04-23/legacy_monthly_review_release_packet.json",
                },
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    refresh_audit_dir = tmp_path / "output" / "monthly_review_release_packet_refresh" / "20260423-120000"
    refresh_audit_dir.mkdir(parents=True, exist_ok=True)
    (refresh_audit_dir / "refresh_audit.json").write_text(
        json.dumps(
            {
                "generated_at": "2026-04-23T12:00:00",
                "scope": "all",
                "status": "ok",
                "results": [
                    {
                        "manifest_path": str(manifest_path),
                        "semantic_change": False,
                        "semantic_changed_fields": [],
                    }
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    attach_refresh_audit_to_manifest(
        manifest_path=manifest_path,
        refresh_audit_dir=refresh_audit_dir,
        repo_root=tmp_path,
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    release_packet = payload["release_packet"]
    assert release_packet["refresh_audit_generated_at"] is not None
    assert release_packet["refresh_audit_scope"] is not None
    assert release_packet["refresh_audit_status"] is not None
    assert release_packet["refresh_semantic_change"] is False
    assert release_packet["refresh_semantic_changed_fields"] == []
    assert release_packet["refresh_audit_dir"] == "output/monthly_review_release_packet_refresh/20260423-120000"
    assert release_packet["refresh_audit_json_path"] == "output/monthly_review_release_packet_refresh/20260423-120000/refresh_audit.json"
    assert release_packet["refresh_audit_summary_path"] == "output/monthly_review_release_packet_refresh/20260423-120000/summary.md"


def test_write_refresh_audit_bundle_summarizes_bulk_refresh(tmp_path: Path) -> None:
    payload = build_refresh_audit_payload(
        scope="all",
        refresh_payload={
            "status": "ok",
            "manifest_count": 2,
            "refreshed_count": 2,
            "failure_count": 0,
            "results": [
                {
                    "run_date": "2026-04-22",
                    "packet_status": "ok",
                    "publish_ready": True,
                    "packet_diff_status": "skipped",
                    "history_run_count": 1,
                    "history_current_green_streak": 1,
                    "history_latest_blocked_run_date": "2026-04-21",
                    "history_latest_blocked_publish_blockers": ["old blocker"],
                    "history_latest_blocked_pipeline_blockers": ["old pipeline blocker"],
                    "history_latest_core_state_transition_baseline_run_date": "2026-04-20",
                    "history_latest_core_state_transition_run_date": "2026-04-21",
                    "history_latest_core_state_transition_changes": [
                        "status `ok` -> `blocked`"
                    ],
                    "history_latest_core_state_transition_publish_blockers_added": [],
                    "history_latest_core_state_transition_publish_blockers_resolved": [],
                    "history_latest_core_state_transition_pipeline_blockers_added": [
                        "Pipeline step failure: 4_validate_tie_out: failed."
                    ],
                    "history_latest_core_state_transition_pipeline_blockers_resolved": [],
                    "history_latest_drift_baseline_run_date": "2026-04-20",
                    "history_latest_drift_run_date": "2026-04-21",
                    "history_latest_drift_changed_gates": ["source_contract"],
                    "history_latest_drift_change_summaries": ["source_contract"],
                    "semantic_changed_fields": ["status"],
                    "manifest_path": "/tmp/2026-04-22/manifest.json",
                },
                {
                    "run_date": "2026-04-23",
                    "packet_status": "ok",
                    "publish_ready": True,
                    "packet_diff_status": "ok",
                    "history_run_count": 2,
                    "history_current_green_streak": 2,
                    "history_latest_blocked_run_date": "2026-04-22",
                    "history_latest_blocked_publish_blockers": [],
                    "history_latest_blocked_pipeline_blockers": [
                        "Pipeline step failure: 4_validate_tie_out: failed."
                    ],
                    "history_latest_core_state_transition_baseline_run_date": "2026-04-22",
                    "history_latest_core_state_transition_run_date": "2026-04-23",
                    "history_latest_core_state_transition_changes": [
                        "status `blocked` -> `ok`",
                        "publish_ready `False` -> `True`",
                    ],
                    "history_latest_core_state_transition_publish_blockers_added": [],
                    "history_latest_core_state_transition_publish_blockers_resolved": [],
                    "history_latest_core_state_transition_pipeline_blockers_added": [],
                    "history_latest_core_state_transition_pipeline_blockers_resolved": [
                        "Pipeline step failure: 4_validate_tie_out: failed."
                    ],
                    "history_latest_drift_baseline_run_date": "2026-04-22",
                    "history_latest_drift_run_date": "2026-04-23",
                    "history_latest_drift_changed_gates": ["data_quality"],
                    "history_latest_drift_change_summaries": ["data_quality"],
                    "semantic_changed_fields": [],
                    "manifest_path": "/tmp/2026-04-23/manifest.json",
                },
            ],
            "failures": [],
        },
    )

    run_dir = write_refresh_audit_bundle(
        output_root=tmp_path / "output" / "monthly_review_release_packet_refresh",
        payload=payload,
        run_token="20260423-120000",
    )

    assert payload["history_current_green_streak_after"] == 2
    assert payload["history_latest_blocked_run_date_after"] == "2026-04-22"
    assert payload["history_latest_blocked_publish_blockers_after"] == []
    assert payload["history_latest_blocked_pipeline_blockers_after"] == [
        "Pipeline step failure: 4_validate_tie_out: failed."
    ]
    assert payload["history_latest_core_state_transition_baseline_run_date_after"] == "2026-04-22"
    assert payload["history_latest_core_state_transition_run_date_after"] == "2026-04-23"
    assert payload["history_latest_core_state_transition_changes_after"] == [
        "status `blocked` -> `ok`",
        "publish_ready `False` -> `True`",
    ]
    assert payload[
        "history_latest_core_state_transition_publish_blockers_added_after"
    ] == []
    assert payload[
        "history_latest_core_state_transition_publish_blockers_resolved_after"
    ] == []
    assert payload[
        "history_latest_core_state_transition_pipeline_blockers_added_after"
    ] == []
    assert payload[
        "history_latest_core_state_transition_pipeline_blockers_resolved_after"
    ] == ["Pipeline step failure: 4_validate_tie_out: failed."]
    assert payload["history_latest_drift_baseline_run_date_after"] == "2026-04-22"
    assert payload["history_latest_drift_run_date_after"] == "2026-04-23"
    assert payload["history_latest_drift_changed_gates_after"] == ["data_quality"]
    assert payload["history_latest_drift_change_summaries_after"] == ["data_quality"]
    assert (run_dir / "refresh_audit.json").exists()
    assert (run_dir / "summary.md").exists()
    assert (run_dir.parent / "latest.json").exists()
    summary = (run_dir / "summary.md").read_text(encoding="utf-8")
    assert "Monthly Review Release Packet Refresh Audit" in summary
    assert "Refreshed: `2`" in summary
    assert "Current green streak after refresh: `2`" in summary
    assert "Latest blocked run after refresh: `2026-04-22`" in summary
    assert (
        "Latest blocked pipeline blockers after refresh: "
        "`['Pipeline step failure: 4_validate_tie_out: failed.']`"
    ) in summary
    assert "Semantic changes: `1`" in summary
    assert "Semantic no-op refreshes: `1`" in summary
    assert (
        "Latest core state transition after refresh: `2026-04-22` -> `2026-04-23` "
        "`['status `blocked` -> `ok`', 'publish_ready `False` -> `True`']`"
    ) in summary
    assert (
        "Latest core state transition pipeline blockers resolved after refresh: "
        "`['Pipeline step failure: 4_validate_tie_out: failed.']`"
    ) in summary
    assert (
        "Latest green drift after refresh: `2026-04-22` -> `2026-04-23` "
        "`['data_quality']`"
    ) in summary
    assert "Latest green drift details after refresh: `['data_quality']`" in summary
    assert "`ok`: `2`" in summary
    assert "`skipped`: `1`" in summary
    assert "`status`: `1`" in summary
    assert (
        "`2026-04-22`: packet `ok`, diff `skipped`, publish `True`, semantic `changed`, "
        "fields `['status']`"
    ) in summary
    assert "`2026-04-23`: packet `ok`, diff `ok`, publish `True`, semantic `no-op`" in summary
