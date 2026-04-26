from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "ai_os_browser_cli.py"

INDEX_BY_TOOL_FAMILY = {
    "builder_brain": "builder_brain_run_index.json",
    "intelligence_workflows": "intelligence_workflow_run_index.json",
    "salesforce_dashboard": "salesforce_dashboard_run_index.json",
    "salesforce_report": "salesforce_report_run_index.json",
    "wave_patch": "wave_patch_run_index.json",
}

LANDING_BY_TOOL_FAMILY = {
    "builder_brain": "README.md",
    "intelligence_workflows": "intelligence_workflow_overview.md",
    "salesforce_dashboard": "salesforce_dashboard_overview.md",
    "salesforce_report": "salesforce_report_overview.md",
    "wave_patch": "wave_patch_overview.md",
}


def run_cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )


def seed_collection(
    browser_root: Path,
    *,
    tool_family: str,
    collection_name: str,
    updated_at: str,
    runs: list[dict[str, str]],
) -> Path:
    collection_dir = browser_root / collection_name
    collection_dir.mkdir(parents=True, exist_ok=True)
    entries = []
    for item in runs:
        run_dir = collection_dir / item["run_name"]
        run_dir.mkdir(parents=True, exist_ok=True)
        landing_path = run_dir / "README.md"
        landing_path.write_text(f"# {item['label']}\n", encoding="utf-8")
        entries.append(
            {
                "command": item["command"],
                "status": item["status"],
                "label": item["label"],
                "run_dir": str(run_dir),
                "landing_artifact": str(landing_path),
                "updated_at": item["updated_at"],
            }
        )
    payload = {"updated_at": updated_at, "entries": entries}
    index_path = collection_dir / INDEX_BY_TOOL_FAMILY[tool_family]
    index_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    landing_path = collection_dir / LANDING_BY_TOOL_FAMILY[tool_family]
    landing_path.write_text(f"# {collection_name}\n", encoding="utf-8")
    return index_path


def test_overview_refreshes_ai_os_browser(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at="2026-03-30T16:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_bundle",
                "label": "Manager Dashboard",
                "command": "bundle",
                "status": "ok",
                "updated_at": "2026-03-30T16:00:00+00:00",
            }
        ],
    )
    seed_collection(
        browser_root,
        tool_family="wave_patch",
        collection_name="wave_runs",
        updated_at="2026-03-30T15:00:00+00:00",
        runs=[
            {
                "run_name": "wave_bundle",
                "label": "Forecast Dashboard",
                "command": "bundle",
                "status": "ok",
                "updated_at": "2026-03-30T15:00:00+00:00",
            }
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("overview", "--refresh", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["collection_count"] == 2
    assert Path(payload["browser_index_artifact"]).exists()
    assert Path(payload["browser_landing_artifact"]).exists()
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert (browser_root / "ai_os_health.json").exists()
    assert (browser_root / "ai_os_health.md").exists()
    browser_overview = Path(payload["browser_landing_artifact"]).read_text(encoding="utf-8")
    browser_index = json.loads(Path(payload["browser_index_artifact"]).read_text(encoding="utf-8"))
    assert str(browser_root / "ai_os_health.md") in browser_overview
    assert "## Health Snapshot" in browser_overview
    assert "Run recency:" in browser_overview
    assert "Stale collections:" in browser_overview
    assert payload["health_summary"]["collection_count"] == 2
    assert isinstance(payload["health_summary"]["run_recency_counts"], dict)
    assert browser_index["health_summary"]["collection_count"] == 2
    assert isinstance(browser_index["health_summary"]["stale_tool_families"], list)
    assert {item["tool_family"] for item in payload["collections"]} == {"salesforce_dashboard", "wave_patch"}
    assert any(message["code"] == "overview_ready" for message in payload["messages"])


def test_collections_filters_tool_family(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at="2026-03-30T16:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_apply",
                "label": "Manager Dashboard",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T16:00:00+00:00",
            }
        ],
    )
    seed_collection(
        browser_root,
        tool_family="salesforce_report",
        collection_name="report_runs",
        updated_at="2026-03-30T14:00:00+00:00",
        runs=[
            {
                "run_name": "report_apply",
                "label": "Manager Report",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T14:00:00+00:00",
            }
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("collections", "--refresh", "--tool-family", "salesforce_dashboard", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["collection_count"] == 1
    assert payload["collections"][0]["tool_family"] == "salesforce_dashboard"
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert any(message["code"] == "collections_ready" for message in payload["messages"])


def test_health_summarizes_statuses_and_bypasses(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    recent_apply = now.isoformat()
    recent_verify = (now - timedelta(hours=2)).isoformat()
    stale_wave = (now - timedelta(days=10)).isoformat()
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at=recent_apply,
        runs=[
            {
                "run_name": "dashboard_apply",
                "label": "Apply Dashboard",
                "command": "apply",
                "status": "ok",
                "updated_at": recent_apply,
            },
            {
                "run_name": "dashboard_verify",
                "label": "Verify Dashboard",
                "command": "verify",
                "status": "warn",
                "updated_at": recent_verify,
            },
        ],
    )
    bypass_path = browser_root / "dashboard_runs" / "dashboard_verify" / "evaluation_bypass_audit.json"
    bypass_path.write_text(json.dumps({"policy_exceptions": ["evaluation_bypass"]}), encoding="utf-8")
    seed_collection(
        browser_root,
        tool_family="wave_patch",
        collection_name="wave_runs",
        updated_at=stale_wave,
        runs=[
            {
                "run_name": "wave_deploy",
                "label": "Deploy Forecast",
                "command": "deploy",
                "status": "error",
                "updated_at": stale_wave,
            }
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("health", "--refresh", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["collection_count"] == 2
    assert payload["run_count"] == 3
    assert payload["evaluation_bypass_count"] == 1
    assert payload["risk_run_count"] == 2
    assert payload["attention_run_count"] == 2
    assert payload["latest_run_updated_at"] == recent_apply
    assert payload["latest_collection_updated_at"] == recent_apply
    assert payload["status_counts"] == {"ok": 1, "warn": 1, "error": 1}
    assert payload["attention_level_counts"]["critical"] == 1
    assert payload["run_recency_counts"]["last_24h"] == 2
    assert payload["run_recency_counts"]["older"] == 1
    assert payload["collection_recency_counts"]["last_24h"] == 1
    assert payload["collection_recency_counts"]["older"] == 1
    assert payload["stale_collection_count"] == 1
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    health_json = json.loads(Path(payload["health_index_artifact"]).read_text(encoding="utf-8"))
    assert health_json["evaluation_bypass_count"] == 1
    assert health_json["attention_run_count"] == 2
    assert health_json["stale_collection_count"] == 1
    health_markdown = Path(payload["health_landing_artifact"]).read_text(encoding="utf-8")
    assert "# AI OS Health" in health_markdown
    assert "## Freshness" in health_markdown
    assert "## Attention Levels" in health_markdown
    assert "## Attention Queue" in health_markdown
    assert "## Stale Tool Families" in health_markdown
    assert "Evaluation bypass runs" in health_markdown
    assert payload["tool_family_health"][0]["tool_family"] in {"salesforce_dashboard", "wave_patch"}
    assert any(item["attention_run_count"] >= 1 for item in payload["tool_family_health"])
    assert any(item["tool_family"] == "wave_patch" and item["recency_bucket"] == "older" for item in payload["stale_tool_families"])
    assert any(item["has_evaluation_bypass"] for item in payload["risky_runs"])
    assert payload["attention_runs"][0]["has_evaluation_bypass"] is True
    assert payload["attention_runs"][0]["attention_level"] == "critical"
    assert any(message["code"] == "health_ready" for message in payload["messages"])


def test_health_filters_tool_family(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_apply",
                "label": "Apply Dashboard",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            }
        ],
    )
    seed_collection(
        browser_root,
        tool_family="salesforce_report",
        collection_name="report_runs",
        updated_at="2026-03-30T17:00:00+00:00",
        runs=[
            {
                "run_name": "report_apply",
                "label": "Apply Report",
                "command": "apply",
                "status": "warn",
                "updated_at": "2026-03-30T17:00:00+00:00",
            }
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("health", "--refresh", "--tool-family", "salesforce_report", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["collection_count"] == 1
    assert payload["run_count"] == 1
    assert payload["status_counts"] == {"warn": 1}
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert len(payload["tool_family_health"]) == 1
    assert payload["tool_family_health"][0]["tool_family"] == "salesforce_report"
    assert payload["tool_family_health"][0]["run_count"] == 1
    assert payload["tool_family_health"][0]["status_counts"] == {"warn": 1}
    assert payload["tool_family_health"][0]["evaluation_verdict_counts"] == {}
    assert payload["tool_family_health"][0]["evaluation_bypass_count"] == 0
    assert payload["tool_family_health"][0]["latest_updated_at"] == "2026-03-30T17:00:00+00:00"
    assert isinstance(payload["tool_family_health"][0]["recency_bucket"], str)
    assert isinstance(payload["tool_family_health"][0]["is_stale"], bool)


def test_runs_selects_latest_collection_for_tool_family(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs_old",
        updated_at="2026-03-30T10:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_old",
                "label": "Old Dashboard",
                "command": "bundle",
                "status": "ok",
                "updated_at": "2026-03-30T10:00:00+00:00",
            }
        ],
    )
    latest_index_path = seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs_latest",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_apply",
                "label": "Latest Dashboard",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            },
            {
                "run_name": "dashboard_verify",
                "label": "Latest Verify",
                "command": "verify",
                "status": "warn",
                "updated_at": "2026-03-30T17:30:00+00:00",
            },
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("runs", "--refresh", "--tool-family", "salesforce_dashboard", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["selected_collection"]["collection_index_artifact"] == str(latest_index_path)
    assert payload["selected_collection"]["tool_family"] == "salesforce_dashboard"
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert len(payload["runs"]) == 2
    assert payload["runs"][0]["label"] == "Latest Dashboard"
    assert any(message["code"] == "runs_ready" for message in payload["messages"])


def test_runs_filters_by_status_and_command(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_apply",
                "label": "Apply Dashboard",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            },
            {
                "run_name": "dashboard_verify",
                "label": "Verify Dashboard",
                "command": "verify",
                "status": "warn",
                "updated_at": "2026-03-30T17:00:00+00:00",
            },
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli(
        "runs",
        "--refresh",
        "--tool-family",
        "salesforce_dashboard",
        "--status",
        "warn",
        "--run-command",
        "verify",
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["run_count"] == 1
    assert payload["runs"][0]["label"] == "Verify Dashboard"
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert payload["filters"]["status"] == "warn"
    assert payload["filters"]["run_command"] == "verify"


def test_run_returns_selected_run_detail_and_landing_markdown(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_report",
        collection_name="report_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "report_apply",
                "label": "Latest Report",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            },
            {
                "run_name": "report_verify",
                "label": "Older Verify",
                "command": "verify",
                "status": "warn",
                "updated_at": "2026-03-30T17:00:00+00:00",
            },
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("run", "--refresh", "--tool-family", "salesforce_report", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["selected_collection"]["tool_family"] == "salesforce_report"
    assert payload["selected_run"]["label"] == "Latest Report"
    assert payload["selected_run"]["command"] == "apply"
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert "# Latest Report" in payload["landing_markdown"]
    assert any(message["code"] == "run_ready" for message in payload["messages"])


def test_search_finds_warn_runs_across_collections(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "dashboard_warn",
                "label": "Manager Dashboard",
                "command": "verify",
                "status": "warn",
                "updated_at": "2026-03-30T18:00:00+00:00",
            }
        ],
    )
    seed_collection(
        browser_root,
        tool_family="wave_patch",
        collection_name="wave_runs",
        updated_at="2026-03-30T17:30:00+00:00",
        runs=[
            {
                "run_name": "wave_warn",
                "label": "Forecast Dashboard",
                "command": "deploy",
                "status": "warn",
                "updated_at": "2026-03-30T17:30:00+00:00",
            }
        ],
    )
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("search", "--refresh", "--status", "warn", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["run_count"] == 2
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert {item["tool_family"] for item in payload["runs"]} == {"salesforce_dashboard", "wave_patch"}
    assert all(item["status"] == "warn" for item in payload["runs"])
    assert any(message["code"] == "search_ready" for message in payload["messages"])


def test_search_detects_evaluation_bypass_artifact(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="salesforce_report",
        collection_name="report_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "report_apply",
                "label": "Latest Report",
                "command": "apply",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            }
        ],
    )
    bypass_path = browser_root / "report_runs" / "report_apply" / "evaluation_bypass_audit.json"
    bypass_path.write_text(json.dumps({"policy_exceptions": ["evaluation_bypass"]}), encoding="utf-8")
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("search", "--refresh", "--has-evaluation-bypass", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["run_count"] == 1
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
    assert payload["runs"][0]["has_evaluation_bypass"] is True
    assert payload["runs"][0]["evaluation_bypass_artifact"] == str(bypass_path.resolve())
    assert payload["runs"][0]["policy_exceptions"] == ["evaluation_bypass"]


def test_search_filters_and_prioritizes_attention_runs(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    now = datetime.now(timezone.utc).replace(microsecond=0)
    recent_warn = now.isoformat()
    stale_ok = (now - timedelta(days=9)).isoformat()
    recent_bypass = (now - timedelta(hours=1)).isoformat()
    seed_collection(
        browser_root,
        tool_family="salesforce_dashboard",
        collection_name="dashboard_runs",
        updated_at=recent_warn,
        runs=[
            {
                "run_name": "dashboard_warn",
                "label": "Warn Dashboard",
                "command": "verify",
                "status": "warn",
                "updated_at": recent_warn,
            },
            {
                "run_name": "dashboard_stale",
                "label": "Stale Dashboard",
                "command": "bundle",
                "status": "ok",
                "updated_at": stale_ok,
            },
        ],
    )
    seed_collection(
        browser_root,
        tool_family="salesforce_report",
        collection_name="report_runs",
        updated_at=recent_bypass,
        runs=[
            {
                "run_name": "report_apply",
                "label": "Bypassed Report",
                "command": "apply",
                "status": "ok",
                "updated_at": recent_bypass,
            }
        ],
    )
    bypass_path = browser_root / "report_runs" / "report_apply" / "evaluation_bypass_audit.json"
    bypass_path.write_text(json.dumps({"policy_exceptions": ["evaluation_bypass"]}), encoding="utf-8")
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli("search", "--refresh", "--needs-attention", "--json", env=env)
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["filters"]["needs_attention"] is True
    assert payload["run_count"] == 3
    assert payload["runs"][0]["label"] == "Bypassed Report"
    assert payload["runs"][0]["attention_level"] == "critical"
    assert payload["runs"][1]["label"] == "Warn Dashboard"
    assert payload["runs"][1]["attention_level"] in {"medium", "high"}
    assert payload["runs"][2]["label"] == "Stale Dashboard"
    assert payload["runs"][2]["attention_level"] == "low"
    assert all(item["attention_score"] > 0 for item in payload["runs"])
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()


def test_run_selects_explicit_run_dir(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="wave_patch",
        collection_name="wave_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "wave_apply",
                "label": "Apply Run",
                "command": "deploy",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            },
            {
                "run_name": "wave_validate",
                "label": "Validate Run",
                "command": "validate",
                "status": "ok",
                "updated_at": "2026-03-30T17:00:00+00:00",
            },
        ],
    )
    target_run_dir = browser_root / "wave_runs" / "wave_validate"
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli(
        "run",
        "--refresh",
        "--collection-dir",
        str(browser_root / "wave_runs"),
        "--run-dir",
        str(target_run_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["selected_run"]["run_dir"] == str(target_run_dir.resolve())
    assert payload["selected_run"]["label"] == "Validate Run"
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()


def test_run_filters_to_bypass_runs_before_position_selection(tmp_path: Path) -> None:
    browser_root = tmp_path / "output"
    seed_collection(
        browser_root,
        tool_family="wave_patch",
        collection_name="wave_runs",
        updated_at="2026-03-30T18:00:00+00:00",
        runs=[
            {
                "run_name": "wave_apply",
                "label": "Apply Run",
                "command": "deploy",
                "status": "ok",
                "updated_at": "2026-03-30T18:00:00+00:00",
            },
            {
                "run_name": "wave_bypass",
                "label": "Bypass Run",
                "command": "deploy",
                "status": "warn",
                "updated_at": "2026-03-30T17:00:00+00:00",
            },
        ],
    )
    bypass_path = browser_root / "wave_runs" / "wave_bypass" / "evaluation_bypass_audit.json"
    bypass_path.write_text(json.dumps({"policy_exceptions": ["evaluation_bypass"]}), encoding="utf-8")
    env = os.environ.copy()
    env["CRM_AI_BROWSER_ROOT"] = str(browser_root)
    result = run_cli(
        "run",
        "--refresh",
        "--collection-dir",
        str(browser_root / "wave_runs"),
        "--has-evaluation-bypass",
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["selected_run"]["label"] == "Bypass Run"
    assert payload["selected_run"]["evaluation_bypass_artifact"] == str(bypass_path.resolve())
    assert Path(payload["health_index_artifact"]).exists()
    assert Path(payload["health_landing_artifact"]).exists()
