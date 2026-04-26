from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "salesforce_dashboard_filter_automation.py"


def run_cli(*args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
        env=env,
    )


def build_plan(tmp_path: Path, *, dashboard_id: str | None = "01ZTESTDASHBOARDAAA") -> Path:
    plan_path = tmp_path / "salesforce_dashboard_filter_automation_plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_automation_plan",
                "executor": "playwright_cli",
                "target_org": "apro@simcorp.com",
                "target_dashboard_id": dashboard_id,
                "relative_edit_route": (
                    f"/lightning/r/Dashboard/{dashboard_id}/edit"
                    if dashboard_id
                    else "/lightning/r/Dashboard/__FILL_TARGET_DASHBOARD_ID__/edit"
                ),
                "relative_edit_route_template": "/lightning/r/Dashboard/{dashboard_id}/edit",
                "preflight_actions": [
                    {"order": 1, "action": "goto_edit_route"},
                    {"order": 2, "action": "snapshot"},
                    {"order": 3, "action": "assert_dashboard_editor", "success_signals": ["Add filter", "Save", "Done"]},
                ],
                "filter_actions": [
                    {
                        "order": 1,
                        "action": "author_dashboard_filter",
                        "source_label": "forecast_category",
                        "filter_name": "Forecast Category",
                        "field_picker_terms": ["Forecast Category"],
                        "options": [{"alias": "Pipeline", "operation": "equals", "value": "Pipeline"}],
                        "save_after": True,
                    }
                ],
                "post_actions": [
                    {"order": 1, "action": "save_dashboard"},
                    {"order": 2, "action": "exit_dashboard_editor"},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return plan_path


def build_multi_plan(tmp_path: Path, *, dashboard_id: str | None = "01ZTESTDASHBOARDAAA") -> Path:
    plan_path = tmp_path / "salesforce_dashboard_filter_automation_plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_automation_plan",
                "executor": "playwright_cli",
                "target_org": "apro@simcorp.com",
                "target_dashboard_id": dashboard_id,
                "relative_edit_route": (
                    f"/lightning/r/Dashboard/{dashboard_id}/edit"
                    if dashboard_id
                    else "/lightning/r/Dashboard/__FILL_TARGET_DASHBOARD_ID__/edit"
                ),
                "relative_edit_route_template": "/lightning/r/Dashboard/{dashboard_id}/edit",
                "preflight_actions": [
                    {"order": 1, "action": "goto_edit_route"},
                    {"order": 2, "action": "snapshot"},
                    {"order": 3, "action": "assert_dashboard_editor", "success_signals": ["Add filter", "Save", "Done"]},
                ],
                "filter_actions": [
                    {
                        "order": 1,
                        "action": "author_dashboard_filter",
                        "source_label": "forecast_category",
                        "filter_name": "Forecast Category",
                        "field_picker_terms": ["Forecast Category"],
                        "options": [{"alias": "Pipeline", "operation": "equals", "value": "Pipeline"}],
                        "save_after": True,
                    },
                    {
                        "order": 2,
                        "action": "author_dashboard_filter",
                        "source_label": "fiscal_period",
                        "filter_name": "Close Date",
                        "field_picker_terms": ["Close Date"],
                        "options": [{"alias": "Q1-2026", "operation": "between", "start_value": "01.02.2026", "end_value": "30.04.2026", "value": None}],
                        "save_after": True,
                    },
                ],
                "post_actions": [
                    {"order": 1, "action": "save_dashboard"},
                    {"order": 2, "action": "exit_dashboard_editor"},
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return plan_path


def build_fake_sf_bin(tmp_path: Path) -> Path:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import sys",
                "args = sys.argv[1:]",
                "if args[:3] == ['org', 'open', '--url-only']:",
                "    print('https://simcorp.lightning.force.com/secur/frontdoor.jsp?sid=TOKEN')",
                "    raise SystemExit(0)",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)
    return fake_bin


def build_fake_flow_wrapper(tmp_path: Path) -> Path:
    state_file = tmp_path / "flow_state.txt"
    wrapper = tmp_path / "playwright_cli.sh"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"STATE_FILE={state_file.as_posix()!r}",
                "if [[ ${1:-} == -s=* ]]; then",
                "  shift",
                "fi",
                "cmd=${1:-}",
                "if [[ \"$cmd\" == \"open\" || \"$cmd\" == \"goto\" || \"$cmd\" == \"resize\" || \"$cmd\" == \"click\" ]]; then",
                "  exit 0",
                "fi",
                "if [[ \"$cmd\" != \"snapshot\" ]]; then",
                "  exit 1",
                "fi",
                "count=0",
                "if [[ -f \"$STATE_FILE\" ]]; then",
                "  count=$(cat \"$STATE_FILE\")",
                "fi",
                "count=$((count + 1))",
                "printf '%s' \"$count\" > \"$STATE_FILE\"",
                "case \"$count\" in",
                "  1)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f6e1001] [cursor=pointer]:",
                "  - button \"Save\" [ref=f6e1002] [cursor=pointer]",
                "  - button \"Done\" [ref=f6e1003] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  2)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f6e2001] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e2002] [cursor=pointer]",
                "  - option \"Forecast Category\" [ref=f6e2003] [cursor=pointer]:",
                "  - generic \"Forecast Category\" [ref=f6e2004]",
                "EOF",
                "    ;;",
                "  3)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add Filter Value\" [ref=f6e3001] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e3002] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e3003]",
                "EOF",
                "    ;;",
                "  4)",
                "    cat <<'EOF'",
                "- generic:",
                "  - option \"Pipeline\" [ref=f6e4001] [cursor=pointer]:",
                "  - button \"Apply\" [ref=f6e4002] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e4003] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e4004]",
                "EOF",
                "    ;;",
                "  5)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Apply\" [ref=f6e5001] [cursor=pointer]",
                "  - button \"Add\" [disabled] [ref=f6e5002]",
                "  - button \"Cancel\" [ref=f6e5003] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e5004]",
                "  - generic \"Pipeline\" [ref=f6e5005]",
                "EOF",
                "    ;;",
                "  6)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f6e6001] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e6002] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e6003]",
                "  - generic \"Pipeline\" [ref=f6e6004]",
                "EOF",
                "    ;;",
                "  7)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f6e7001] [cursor=pointer]:",
                "  - button \"Save\" [ref=f6e7002] [cursor=pointer]",
                "  - button \"Done\" [ref=f6e7003] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e7004]",
                "  - generic \"Pipeline\" [ref=f6e7005]",
                "EOF",
                "    ;;",
                "  *)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Done\" [ref=f6e8001] [cursor=pointer]",
                "  - button \"Save\" [active] [ref=f6e8002] [cursor=pointer]",
                "  - generic \"Dashboard saved\" [ref=f6e8003]",
                "  - generic \"Forecast Category\" [ref=f6e8004]",
                "  - generic \"Pipeline\" [ref=f6e8005]",
                "EOF",
                "    ;;",
                "esac",
            ]
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


def build_fake_multi_flow_wrapper(tmp_path: Path) -> Path:
    state_file = tmp_path / "multi_flow_state.txt"
    wrapper = tmp_path / "playwright_cli_multi.sh"
    wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                f"STATE_FILE={state_file.as_posix()!r}",
                "if [[ ${1:-} == -s=* ]]; then",
                "  shift",
                "fi",
                "cmd=${1:-}",
                "if [[ \"$cmd\" == \"open\" || \"$cmd\" == \"goto\" || \"$cmd\" == \"resize\" || \"$cmd\" == \"click\" ]]; then",
                "  exit 0",
                "fi",
                "if [[ \"$cmd\" != \"snapshot\" ]]; then",
                "  exit 1",
                "fi",
                "count=0",
                "if [[ -f \"$STATE_FILE\" ]]; then",
                "  count=$(cat \"$STATE_FILE\")",
                "fi",
                "count=$((count + 1))",
                "printf '%s' \"$count\" > \"$STATE_FILE\"",
                "case \"$count\" in",
                "  1)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f7e1001] [cursor=pointer]:",
                "  - button \"Save\" [ref=f7e1002] [cursor=pointer]",
                "  - button \"Done\" [ref=f7e1003] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  2)",
                "    cat <<'EOF'",
                "- generic:",
                "  - option \"Forecast Category\" [ref=f7e2001] [cursor=pointer]:",
                "  - option \"Close Date\" [ref=f7e2002] [cursor=pointer]:",
                "  - button \"Add\" [ref=f7e2003] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  3)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add Filter Value\" [ref=f7e3001] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f7e3002]",
                "EOF",
                "    ;;",
                "  4)",
                "    cat <<'EOF'",
                "- generic:",
                "  - option \"Pipeline\" [ref=f7e4001] [cursor=pointer]:",
                "  - button \"Apply\" [ref=f7e4002] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  5)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Apply\" [ref=f7e5001] [cursor=pointer]",
                "  - button \"Add\" [disabled] [ref=f7e5002]",
                "  - generic \"Forecast Category\" [ref=f7e5003]",
                "  - generic \"Pipeline\" [ref=f7e5004]",
                "EOF",
                "    ;;",
                "  6)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f7e6001] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f7e6002]",
                "  - generic \"Pipeline\" [ref=f7e6003]",
                "EOF",
                "    ;;",
                "  7)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f7e7001] [cursor=pointer]:",
                "  - button \"Save\" [ref=f7e7002] [cursor=pointer]",
                "  - button \"Done\" [ref=f7e7003] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f7e7004]",
                "  - generic \"Pipeline\" [ref=f7e7005]",
                "EOF",
                "    ;;",
                "  8)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Done\" [ref=f7e8001] [cursor=pointer]",
                "  - button \"Save\" [active] [ref=f7e8002] [cursor=pointer]",
                "  - generic \"Dashboard saved\" [ref=f7e8003]",
                "  - generic \"Forecast Category\" [ref=f7e8004]",
                "  - generic \"Pipeline\" [ref=f7e8005]",
                "  - button \"Add filter Filter\" [ref=f7e8006] [cursor=pointer]:",
                "EOF",
                "    ;;",
                "  9)",
                "    cat <<'EOF'",
                "- generic:",
                "  - option \"Close Date\" [ref=f7e9001] [cursor=pointer]:",
                "  - button \"Add\" [ref=f7e9002] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  10)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add Filter Value\" [ref=f7e10001] [cursor=pointer]",
                "  - generic \"Close Date\" [ref=f7e10002]",
                "EOF",
                "    ;;",
                "  11)",
                "    cat <<'EOF'",
                "- generic:",
                "  - option \"Q1-2026\" [ref=f7e11001] [cursor=pointer]:",
                "  - button \"Apply\" [ref=f7e11002] [cursor=pointer]",
                "EOF",
                "    ;;",
                "  12)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Apply\" [ref=f7e12001] [cursor=pointer]",
                "  - button \"Add\" [disabled] [ref=f7e12002]",
                "  - generic \"Close Date\" [ref=f7e12003]",
                "  - generic \"Q1-2026\" [ref=f7e12004]",
                "EOF",
                "    ;;",
                "  13)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f7e13001] [cursor=pointer]",
                "  - generic \"Close Date\" [ref=f7e13002]",
                "  - generic \"Q1-2026\" [ref=f7e13003]",
                "EOF",
                "    ;;",
                "  14)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f7e14001] [cursor=pointer]:",
                "  - button \"Save\" [ref=f7e14002] [cursor=pointer]",
                "  - button \"Done\" [ref=f7e14003] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f7e14004]",
                "  - generic \"Pipeline\" [ref=f7e14005]",
                "  - generic \"Close Date\" [ref=f7e14006]",
                "  - generic \"Q1-2026\" [ref=f7e14007]",
                "EOF",
                "    ;;",
                "  *)",
                "    cat <<'EOF'",
                "- generic:",
                "  - button \"Done\" [ref=f7e15001] [cursor=pointer]",
                "  - button \"Save\" [active] [ref=f7e15002] [cursor=pointer]",
                "  - generic \"Dashboard saved\" [ref=f7e15003]",
                "  - generic \"Forecast Category\" [ref=f7e15004]",
                "  - generic \"Pipeline\" [ref=f7e15005]",
                "  - generic \"Close Date\" [ref=f7e15006]",
                "  - generic \"Q1-2026\" [ref=f7e15007]",
                "  - button \"Add filter Filter\" [ref=f7e15008] [cursor=pointer]:",
                "EOF",
                "    ;;",
                "esac",
            ]
        ),
        encoding="utf-8",
    )
    wrapper.chmod(0o755)
    return wrapper


def build_fake_dashboard_executor(tmp_path: Path) -> Path:
    script = tmp_path / "fake_salesforce_dashboard_executor.py"
    script.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import json",
                "import sys",
                "from pathlib import Path",
                "args = sys.argv[1:]",
                "if not args or args[0] != 'verify':",
                "    raise SystemExit(1)",
                "def value(flag):",
                "    if flag not in args:",
                "        return None",
                "    return args[args.index(flag) + 1]",
                "output_dir = value('--output-dir')",
                "manual_filter_json = value('--manual-filter-authoring-json')",
                "dashboard_id = value('--dashboard-id')",
                "target_org = value('--target-org')",
                "payload = json.loads(Path(manual_filter_json).read_text(encoding='utf-8'))",
                "filter_intents = payload['filter_intents']",
                "verify_payload = {",
                "    'status': 'ok',",
                "    'tool': 'salesforce_dashboard_executor',",
                "    'lane': 'native_surface_authoring',",
                "    'command_class': 'live_read',",
                "    'command': 'verify',",
                "    'messages': [{'level': 'info', 'code': 'verify_complete', 'text': 'Verified dashboard filter contract.'}],",
                "    'artifacts': [],",
                "    'summary': {'manual_filter_verified_count': len(filter_intents), 'finding_count': 1},",
                "    'manual_filter_verification': {",
                "        'source': 'manual_filter_authoring_artifact',",
                "        'verified_filters': [",
                "            {",
                "                'source_label': filter_intent.get('source_label'),",
                "                'expected_filter_name': (filter_intent.get('proposed_filter') or {}).get('name'),",
                "                'actual_filter_name': (filter_intent.get('proposed_filter') or {}).get('name'),",
                "            }",
                "            for filter_intent in filter_intents",
                "        ],",
                "        'missing_filters': [],",
                "        'mismatched_filters': [],",
                "        'unexpected_filters': [],",
                "    },",
                "    'target_dashboard_id': dashboard_id,",
                "    'target_org': target_org,",
                "}",
                "if output_dir:",
                "    output = Path(output_dir)",
                "    output.mkdir(parents=True, exist_ok=True)",
                "    verify_path = output / 'salesforce_dashboard_verify.json'",
                "    verify_path.write_text(json.dumps({'artifact_type': 'salesforce_dashboard_verify', 'summary': verify_payload.get('summary', {}), 'manual_filter_verification': verify_payload.get('manual_filter_verification', {})}, indent=2), encoding='utf-8')",
                "    verify_payload['artifacts'].append({'type': 'salesforce_dashboard_verify', 'path': str(verify_path)})",
                "print(json.dumps(verify_payload))",
            ]
        ),
        encoding="utf-8",
    )
    script.chmod(0o755)
    return script


def build_manual_filter_authoring(tmp_path: Path, *, multi: bool = False) -> Path:
    artifact_path = tmp_path / "salesforce_dashboard_manual_filter_authoring.json"
    filter_intents = [
        {
            "source_label": "forecast_category",
            "proposal_source": "repo_vocab.dashboard_filter:forecast_category->Forecast Category",
            "proposed_filter": {
                "name": "Forecast Category",
                "options": [
                    {"alias": "Pipeline", "operation": "equals", "startValue": None, "endValue": None, "value": "Pipeline"},
                    {"alias": "Commit", "operation": "equals", "startValue": None, "endValue": None, "value": "Commit"},
                ],
                "selectedOption": None,
            },
        }
    ]
    if multi:
        filter_intents.append(
            {
                "source_label": "fiscal_period",
                "proposal_source": "repo_vocab.dashboard_filter_template:fiscal_period->Close Date",
                "proposed_filter": {
                    "name": "Close Date",
                    "options": [
                        {"alias": "Q1-2026", "operation": "between", "startValue": "01.02.2026", "endValue": "30.04.2026", "value": None},
                        {"alias": "Q2-2026", "operation": "between", "startValue": "01.05.2026", "endValue": "31.07.2026", "value": None},
                    ],
                    "selectedOption": None,
                },
            }
        )
    artifact_path.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_manual_filter_authoring",
                "package_developer_name": "manager_dashboard",
                "suggested_dashboard_label": "Manager Dashboard",
                "target_org": "apro@simcorp.com",
                "filter_intents": filter_intents,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return artifact_path


def build_verify_package(tmp_path: Path) -> Path:
    package_path = tmp_path / "build_package.json"
    package_path.write_text(
        json.dumps(
            {
                "surface_contract": {
                    "surface_type": "salesforce_dashboard",
                    "developer_name": "manager_dashboard",
                }
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    return package_path


def test_validate_plan(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    result = run_cli("validate", "--plan", str(plan_path), "--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["filter_action_count"] == 1


def test_run_filter_flow_through_apply_filter_value(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    fake_bin = build_fake_sf_bin(tmp_path)
    fake_wrapper = build_fake_flow_wrapper(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    output_dir = tmp_path / "run_filter_flow_apply"
    result = run_cli(
        "run-filter-flow",
        "--plan",
        str(plan_path),
        "--through",
        "apply-filter-value",
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command"] == "run-filter-flow"
    assert payload["command_class"] == "live_read"
    assert payload["summary"]["through_stage"] == "apply-filter-value"
    assert payload["summary"]["executed_steps"] == [
        "prepare",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
    ]
    assert payload["summary"]["option_alias"] == "Pipeline"
    assert len(payload["step_results"]) == 6
    assert (output_dir / "06_apply_filter_value" / "salesforce_dashboard_filter_apply.json").exists()
    assert not (output_dir / "07_commit_dashboard_filter").exists()


def test_run_filter_flow_through_save_dashboard(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    fake_bin = build_fake_sf_bin(tmp_path)
    fake_wrapper = build_fake_flow_wrapper(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    output_dir = tmp_path / "run_filter_flow_save"
    result = run_cli(
        "run-filter-flow",
        "--plan",
        str(plan_path),
        "--through",
        "save-dashboard",
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["through_stage"] == "save-dashboard"
    assert payload["summary"]["done_ready"] is True
    assert payload["summary"]["executed_steps"] == [
        "prepare",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
        "commit-dashboard-filter",
        "save-dashboard",
    ]
    assert len(payload["step_results"]) == 8
    assert (output_dir / "08_save_dashboard" / "salesforce_dashboard_filter_save.json").exists()


def test_run_filter_flow_through_verify_dashboard(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    build_manual_filter_authoring(tmp_path)
    verify_package = build_verify_package(tmp_path)
    fake_bin = build_fake_sf_bin(tmp_path)
    fake_wrapper = build_fake_flow_wrapper(tmp_path)
    fake_dashboard_executor = build_fake_dashboard_executor(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    output_dir = tmp_path / "run_filter_flow_verify"
    result = run_cli(
        "run-filter-flow",
        "--plan",
        str(plan_path),
        "--through",
        "verify-dashboard",
        "--verify-package",
        str(verify_package),
        "--dashboard-executor-script",
        str(fake_dashboard_executor),
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["through_stage"] == "verify-dashboard"
    assert payload["summary"]["verify_status"] == "ok"
    assert payload["summary"]["manual_filter_verified_count"] == 1
    assert payload["summary"]["filter_name"] == "Forecast Category"
    assert payload["summary"]["option_alias"] == "Pipeline"
    assert payload["summary"]["executed_steps"] == [
        "prepare",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
        "commit-dashboard-filter",
        "save-dashboard",
        "verify",
    ]
    assert len(payload["step_results"]) == 9
    verification_contract_path = Path(payload["summary"]["verification_contract_path"])
    assert verification_contract_path.exists()
    verification_contract = json.loads(verification_contract_path.read_text(encoding="utf-8"))
    proposed_filter = verification_contract["filter_intents"][0]["proposed_filter"]
    assert proposed_filter["name"] == "Forecast Category"
    assert proposed_filter["selectedOption"] == "Pipeline"
    assert len(proposed_filter["options"]) == 1
    assert proposed_filter["options"][0]["alias"] == "Pipeline"
    assert (output_dir / "09_verify_dashboard" / "salesforce_dashboard_verify.json").exists()


def test_run_filter_flow_all_filters_through_verify_dashboard(tmp_path: Path) -> None:
    plan_path = build_multi_plan(tmp_path)
    build_manual_filter_authoring(tmp_path, multi=True)
    verify_package = build_verify_package(tmp_path)
    fake_bin = build_fake_sf_bin(tmp_path)
    fake_wrapper = build_fake_multi_flow_wrapper(tmp_path)
    fake_dashboard_executor = build_fake_dashboard_executor(tmp_path)
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    output_dir = tmp_path / "run_filter_flow_verify_all"
    result = run_cli(
        "run-filter-flow",
        "--plan",
        str(plan_path),
        "--all-filters",
        "--through",
        "verify-dashboard",
        "--verify-package",
        str(verify_package),
        "--dashboard-executor-script",
        str(fake_dashboard_executor),
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["command_class"] == "mutating"
    assert payload["summary"]["through_stage"] == "verify-dashboard"
    assert payload["summary"]["authored_filter_count"] == 2
    assert payload["summary"]["manual_filter_verified_count"] == 2
    assert payload["summary"]["authored_filters"] == [
        {"filter_name": "Forecast Category", "option_alias": "Pipeline"},
        {"filter_name": "Close Date", "option_alias": "Q1-2026"},
    ]
    assert payload["summary"]["executed_steps"] == [
        "prepare",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
        "commit-dashboard-filter",
        "save-dashboard",
        "open-filter",
        "open-filter-field",
        "open-filter-value",
        "select-filter-option",
        "apply-filter-value",
        "commit-dashboard-filter",
        "save-dashboard",
        "verify",
    ]
    verification_contract_path = Path(payload["summary"]["verification_contract_path"])
    verification_contract = json.loads(verification_contract_path.read_text(encoding="utf-8"))
    assert len(verification_contract["filter_intents"]) == 2
    assert verification_contract["filter_intents"][0]["proposed_filter"]["selectedOption"] == "Pipeline"
    assert verification_contract["filter_intents"][1]["proposed_filter"]["selectedOption"] == "Q1-2026"
    assert (output_dir / "09_verify_dashboard" / "salesforce_dashboard_verify.json").exists()


def test_prepare_plan_with_fake_sf_and_playwright(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import sys",
                "args = sys.argv[1:]",
                "if args[:3] == ['org', 'open', '--url-only']:",
                "    print('https://simcorp.lightning.force.com/secur/frontdoor.jsp?sid=TOKEN')",
                "    raise SystemExit(0)",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"open\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"goto\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"resize\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f6e3354] [cursor=pointer]:",
                "    - text: Filter",
                "  - button \"Save\" [ref=f6e3359] [cursor=pointer]",
                "  - button \"Done\" [ref=f6e3361] [cursor=pointer]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "prepare"
    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    result = run_cli(
        "prepare",
        "--plan",
        str(plan_path),
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["target_org"] == "apro@simcorp.com"
    assert payload["summary"]["target_dashboard_id"] == "01ZTESTDASHBOARDAAA"
    assert payload["prepare_artifact"]["relative_edit_route"] == "/lightning/r/Dashboard/01ZTESTDASHBOARDAAA/edit"
    assert payload["prepare_artifact"]["candidate_refs"]["Add filter"][0]["ref"] == "f6e3354"
    assert payload["prepare_artifact"]["candidate_refs"]["Add filter"][0]["disabled"] is False
    assert payload["prepare_artifact"]["candidate_refs"]["Save"][0]["ref"] == "f6e3359"
    assert payload["prepare_artifact"]["candidate_refs"]["Done"][0]["ref"] == "f6e3361"
    assert payload["summary"]["editor_state"] == "ready"
    assert payload["summary"]["blocking_signal_count"] == 0
    assert payload["summary"]["add_filter_disabled"] is False
    assert (output_dir / "salesforce_dashboard_filter_prepare.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_prepare_snapshot.yml").exists()


def test_prepare_plan_reports_blocking_editor_state(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_sf = fake_bin / "sf"
    fake_sf.write_text(
        "\n".join(
            [
                "#!/usr/bin/env python3",
                "import sys",
                "args = sys.argv[1:]",
                "if args[:3] == ['org', 'open', '--url-only']:",
                "    print('https://simcorp.lightning.force.com/secur/frontdoor.jsp?sid=TOKEN')",
                "    raise SystemExit(0)",
                "raise SystemExit(1)",
            ]
        ),
        encoding="utf-8",
    )
    fake_sf.chmod(0o755)

    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"open\" ] || [ \"$cmd\" = \"goto\" ] || [ \"$cmd\" = \"resize\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [disabled] [ref=f6e3354]:",
                "  - button \"Save\" [ref=f6e3359] [cursor=pointer]",
                "  - button \"Done\" [ref=f6e3361] [cursor=pointer]",
                "  - generic: entity is deleted",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    result = run_cli(
        "prepare",
        "--plan",
        str(plan_path),
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
        env=env,
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["editor_state"] == "blocked"
    assert payload["summary"]["blocking_signal_count"] == 1
    assert payload["summary"]["add_filter_disabled"] is True
    assert payload["prepare_artifact"]["blocking_signals"][0]["code"] == "entity_deleted"
    assert [item["code"] for item in payload["messages"][:2]] == ["editor_blocking_signal", "add_filter_disabled"]


def test_prepare_plan_requires_dashboard_id_when_missing(tmp_path: Path) -> None:
    plan_path = build_plan(tmp_path, dashboard_id=None)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)
    result = run_cli(
        "prepare",
        "--plan",
        str(plan_path),
        "--session",
        "dashfilter",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "prepare_failed"
    assert "dashboard_id" in payload["messages"][0]["text"]


def test_open_filter_from_prepare(tmp_path: Path) -> None:
    prepare_artifact = tmp_path / "salesforce_dashboard_filter_prepare.json"
    prepare_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_prepare",
                "session": "dashfilter",
                "candidate_refs": {
                    "Add filter": [{"ref": "f6e3354", "line": "- button \"Add filter Filter\" [ref=f6e3354]", "disabled": False}],
                    "Done": [{"ref": "f6e3361", "line": "- button \"Done\" [ref=f6e3361]", "disabled": False}],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f6e4001] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e4002] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e4003]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "open_filter"
    result = run_cli(
        "open-filter",
        "--prepare-artifact",
        str(prepare_artifact),
        "--plan",
        str(plan_path),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["open_filter_artifact"]["source_add_filter_ref"]["ref"] == "f6e3354"
    assert "Forecast Category" in payload["open_filter_artifact"]["candidate_refs"]
    assert (output_dir / "salesforce_dashboard_filter_open.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_open_snapshot.yml").exists()


def test_open_filter_from_prepare_warns_when_disabled(tmp_path: Path) -> None:
    prepare_artifact = tmp_path / "salesforce_dashboard_filter_prepare.json"
    prepare_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_prepare",
                "session": "dashfilter",
                "candidate_refs": {
                    "Add filter": [{"ref": "f6e3354", "line": "- button \"Add filter Filter\" [disabled] [ref=f6e3354]", "disabled": True}],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "open-filter",
        "--prepare-artifact",
        str(prepare_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["messages"][0]["code"] == "add_filter_disabled"


def test_open_filter_from_prepare_warns_when_disabled_is_only_in_line(tmp_path: Path) -> None:
    prepare_artifact = tmp_path / "salesforce_dashboard_filter_prepare.json"
    prepare_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_prepare",
                "session": "dashfilter",
                "candidate_refs": {
                    "Add filter": [{"ref": "f6e3354", "line": "- button \"Add filter Filter\" [disabled] [ref=f6e3354]"}],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "open-filter",
        "--prepare-artifact",
        str(prepare_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["messages"][0]["code"] == "add_filter_disabled"


def test_open_filter_from_prepare_surfaces_blocking_signals(tmp_path: Path) -> None:
    prepare_artifact = tmp_path / "salesforce_dashboard_filter_prepare.json"
    prepare_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_prepare",
                "session": "dashfilter",
                "blocking_signals": [
                    {"code": "entity_deleted", "text": "The dashboard editor reports that the target dashboard entity is deleted."}
                ],
                "candidate_refs": {
                    "Add filter": [{"ref": "f6e3354", "line": "- button \"Add filter Filter\" [disabled] [ref=f6e3354]", "disabled": True}],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "open-filter",
        "--prepare-artifact",
        str(prepare_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert [item["code"] for item in payload["messages"][:2]] == ["editor_blocking_signal", "add_filter_disabled"]
    assert payload["summary"]["blocking_signal_count"] == 1


def test_open_filter_field_from_artifact(tmp_path: Path) -> None:
    open_artifact = tmp_path / "salesforce_dashboard_filter_open.json"
    open_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_open",
                "session": "dashfilter",
                "candidate_refs": {
                    "Forecast Category": [
                        {
                            "ref": "f6e4000",
                            "line": "- button [ref=f6e4000]: Forecast Category Pipeline",
                            "disabled": False,
                        },
                        {
                            "ref": "f6e4003",
                            "line": "- option \"Forecast Category\" [ref=f6e4003] [cursor=pointer]:",
                            "disabled": False,
                        },
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add Filter Value\" [ref=f6e5001] [cursor=pointer]",
                "  - option \"Pipeline\" [ref=f6e5002] [cursor=pointer]:",
                "  - button \"Add\" [ref=f6e5003] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e5004] [cursor=pointer]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "open_filter_field"
    result = run_cli(
        "open-filter-field",
        "--open-filter-artifact",
        str(open_artifact),
        "--plan",
        str(plan_path),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["selected_field_term"] == "Forecast Category"
    assert payload["summary"]["selected_field_ref"] == "f6e4003"
    assert payload["open_filter_field_artifact"]["selected_field_ref"]["ref"] == "f6e4003"
    assert "Pipeline" in payload["open_filter_field_artifact"]["candidate_refs"]
    assert (output_dir / "salesforce_dashboard_filter_field.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_field_snapshot.yml").exists()


def test_open_filter_field_requires_matching_candidate(tmp_path: Path) -> None:
    open_artifact = tmp_path / "salesforce_dashboard_filter_open.json"
    open_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_open",
                "session": "dashfilter",
                "candidate_refs": {
                    "Opportunity Owner": [
                        {
                            "ref": "f6e4007",
                            "line": "- option \"Opportunity Owner\" [ref=f6e4007] [cursor=pointer]:",
                            "disabled": False,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "open-filter-field",
        "--open-filter-artifact",
        str(open_artifact),
        "--plan",
        str(plan_path),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "open_filter_field_failed"
    assert "Forecast Category" in payload["messages"][0]["text"]


def test_open_filter_value_from_field_artifact(tmp_path: Path) -> None:
    field_artifact = tmp_path / "salesforce_dashboard_filter_field.json"
    field_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_field",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e5001",
                            "line": "- button \"Add Filter Value\" [ref=f6e5001] [cursor=pointer]",
                            "disabled": False,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - option \"Pipeline\" [ref=f6e6001] [cursor=pointer]:",
                "  - option \"Won\" [ref=f6e6002] [cursor=pointer]:",
                "  - button \"Add\" [disabled] [ref=f6e6003]",
                "  - button \"Cancel\" [ref=f6e6004] [cursor=pointer]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "open_filter_value"
    result = run_cli(
        "open-filter-value",
        "--field-artifact",
        str(field_artifact),
        "--plan",
        str(plan_path),
        "--filter-name",
        "Forecast Category",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["selected_filter_name"] == "Forecast Category"
    assert payload["summary"]["source_add_filter_value_ref"] == "f6e5001"
    assert "Pipeline" in payload["open_filter_value_artifact"]["candidate_refs"]
    assert (output_dir / "salesforce_dashboard_filter_value.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_value_snapshot.yml").exists()


def test_open_filter_value_requires_add_filter_value_candidate(tmp_path: Path) -> None:
    field_artifact = tmp_path / "salesforce_dashboard_filter_field.json"
    field_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_field",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e5003",
                            "line": "- button \"Add\" [disabled] [ref=f6e5003]",
                            "disabled": True,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "open-filter-value",
        "--field-artifact",
        str(field_artifact),
        "--plan",
        str(plan_path),
        "--filter-name",
        "Forecast Category",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "open_filter_value_failed"
    assert "Add Filter Value" in payload["messages"][0]["text"]


def test_select_filter_option_from_value_artifact(tmp_path: Path) -> None:
    value_artifact = tmp_path / "salesforce_dashboard_filter_value.json"
    value_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_value",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "candidate_refs": {
                    "Pipeline": [
                        {
                            "ref": "f6e7000",
                            "line": "- button [ref=f6e7000]: Forecast Category Pipeline",
                            "disabled": False,
                        },
                        {
                            "ref": "f6e7001",
                            "line": "- option \"Pipeline\" [ref=f6e7001] [cursor=pointer]:",
                            "disabled": False,
                        },
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f6e7101] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e7102] [cursor=pointer]",
                "  - button \"Pipeline\" [ref=f6e7103] [cursor=pointer]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "select_filter_option"
    result = run_cli(
        "select-filter-option",
        "--value-artifact",
        str(value_artifact),
        "--plan",
        str(plan_path),
        "--filter-name",
        "Forecast Category",
        "--option-alias",
        "Pipeline",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["selected_option_alias"] == "Pipeline"
    assert payload["summary"]["selected_option_ref"] == "f6e7001"
    assert payload["select_filter_option_artifact"]["selected_option_ref"]["ref"] == "f6e7001"
    assert (output_dir / "salesforce_dashboard_filter_option.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_option_snapshot.yml").exists()


def test_select_filter_option_requires_matching_candidate(tmp_path: Path) -> None:
    value_artifact = tmp_path / "salesforce_dashboard_filter_value.json"
    value_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_value",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "candidate_refs": {
                    "Commit": [
                        {
                            "ref": "f6e7004",
                            "line": "- option \"Commit\" [ref=f6e7004] [cursor=pointer]:",
                            "disabled": False,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    plan_path = build_plan(tmp_path)
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "select-filter-option",
        "--value-artifact",
        str(value_artifact),
        "--plan",
        str(plan_path),
        "--filter-name",
        "Forecast Category",
        "--option-alias",
        "Pipeline",
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "select_filter_option_failed"
    assert "Pipeline" in payload["messages"][0]["text"]


def test_apply_filter_value_from_option_artifact(tmp_path: Path) -> None:
    option_artifact = tmp_path / "salesforce_dashboard_filter_option.json"
    option_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_option",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "selected_option_alias": "Pipeline",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e8001",
                            "line": "- button \"Add\" [ref=f6e8001] [cursor=pointer]",
                            "disabled": False,
                        }
                    ],
                    "Apply": [
                        {
                            "ref": "f6e8002",
                            "line": "- button \"Apply\" [ref=f6e8002] [cursor=pointer]",
                            "disabled": False,
                        }
                    ],
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add\" [ref=f6e8101] [cursor=pointer]",
                "  - button \"Cancel\" [ref=f6e8102] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e8103]",
                "  - generic \"Pipeline\" [ref=f6e8104]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "apply_filter_value"
    result = run_cli(
        "apply-filter-value",
        "--option-artifact",
        str(option_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["source_apply_ref"] == "f6e8002"
    assert payload["summary"]["add_ready"] is True
    assert payload["apply_filter_value_artifact"]["source_apply_ref"]["ref"] == "f6e8002"
    assert (output_dir / "salesforce_dashboard_filter_apply.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_apply_snapshot.yml").exists()


def test_apply_filter_value_requires_apply_candidate(tmp_path: Path) -> None:
    option_artifact = tmp_path / "salesforce_dashboard_filter_option.json"
    option_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_option",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "selected_option_alias": "Pipeline",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e8001",
                            "line": "- button \"Add favorite\" [ref=f6e8001] [cursor=pointer]",
                            "disabled": True,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "apply-filter-value",
        "--option-artifact",
        str(option_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "apply_filter_value_failed"
    assert "Apply" in payload["messages"][0]["text"]


def test_commit_dashboard_filter_from_apply_artifact(tmp_path: Path) -> None:
    apply_artifact = tmp_path / "salesforce_dashboard_filter_apply.json"
    apply_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_apply",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "selected_option_alias": "Pipeline",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e9001",
                            "line": "- button \"Add\" [ref=f6e9001] [cursor=pointer]",
                            "disabled": False,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Add filter Filter\" [ref=f6e9101] [cursor=pointer]:",
                "  - button \"Save\" [ref=f6e9102] [cursor=pointer]",
                "  - button \"Done\" [ref=f6e9103] [cursor=pointer]",
                "  - generic \"Forecast Category\" [ref=f6e9104]",
                "  - generic \"Pipeline\" [ref=f6e9105]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "commit_dashboard_filter"
    result = run_cli(
        "commit-dashboard-filter",
        "--apply-artifact",
        str(apply_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["source_add_ref"] == "f6e9001"
    assert payload["summary"]["save_ready"] is True
    assert payload["commit_dashboard_filter_artifact"]["save_ref"]["ref"] == "f6e9102"
    assert (output_dir / "salesforce_dashboard_filter_commit.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_commit_snapshot.yml").exists()


def test_commit_dashboard_filter_requires_enabled_add(tmp_path: Path) -> None:
    apply_artifact = tmp_path / "salesforce_dashboard_filter_apply.json"
    apply_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_apply",
                "session": "dashfilter",
                "candidate_refs": {
                    "Add": [
                        {
                            "ref": "f6e9006",
                            "line": "- button \"Add\" [disabled] [ref=f6e9006]",
                            "disabled": True,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "commit-dashboard-filter",
        "--apply-artifact",
        str(apply_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "commit_dashboard_filter_failed"
    assert "selectable candidate for button: Add" in payload["messages"][0]["text"]


def test_save_dashboard_from_commit_artifact(tmp_path: Path) -> None:
    commit_artifact = tmp_path / "salesforce_dashboard_filter_commit.json"
    commit_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_commit",
                "session": "dashfilter",
                "selected_filter_name": "Forecast Category",
                "selected_option_alias": "Pipeline",
                "candidate_refs": {
                    "Save": [
                        {
                            "ref": "f6e9201",
                            "line": "- button \"Save\" [ref=f6e9201] [cursor=pointer]",
                            "disabled": False,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text(
        "\n".join(
            [
                "#!/usr/bin/env bash",
                "set -euo pipefail",
                "shift",
                "cmd=\"$1\"",
                "if [ \"$cmd\" = \"click\" ]; then",
                "  exit 0",
                "fi",
                "if [ \"$cmd\" = \"snapshot\" ]; then",
                "  cat <<'EOF'",
                "- generic:",
                "  - button \"Done\" [ref=f6e9301] [cursor=pointer]",
                "  - button \"Save\" [disabled] [ref=f6e9302]",
                "  - generic \"Forecast Category\" [ref=f6e9303]",
                "  - generic \"Pipeline\" [ref=f6e9304]",
                "EOF",
                "  exit 0",
                "fi",
                "exit 1",
            ]
        ),
        encoding="utf-8",
    )
    fake_wrapper.chmod(0o755)

    output_dir = tmp_path / "save_dashboard"
    result = run_cli(
        "save-dashboard",
        "--commit-artifact",
        str(commit_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["source_save_ref"] == "f6e9201"
    assert payload["summary"]["done_ready"] is True
    assert payload["save_dashboard_artifact"]["done_ref"]["ref"] == "f6e9301"
    assert (output_dir / "salesforce_dashboard_filter_save.json").exists()
    assert (output_dir / "salesforce_dashboard_filter_save_snapshot.yml").exists()


def test_save_dashboard_requires_enabled_save(tmp_path: Path) -> None:
    commit_artifact = tmp_path / "salesforce_dashboard_filter_commit.json"
    commit_artifact.write_text(
        json.dumps(
            {
                "artifact_type": "salesforce_dashboard_filter_commit",
                "session": "dashfilter",
                "candidate_refs": {
                    "Save": [
                        {
                            "ref": "f6e9205",
                            "line": "- button \"Save\" [disabled] [ref=f6e9205]",
                            "disabled": True,
                        }
                    ]
                },
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    fake_wrapper = tmp_path / "playwright_cli.sh"
    fake_wrapper.write_text("#!/usr/bin/env bash\nexit 0\n", encoding="utf-8")
    fake_wrapper.chmod(0o755)

    result = run_cli(
        "save-dashboard",
        "--commit-artifact",
        str(commit_artifact),
        "--playwright-wrapper",
        str(fake_wrapper),
        "--json",
    )
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "error"
    assert payload["messages"][0]["code"] == "save_dashboard_failed"
    assert "selectable candidate for button: Save" in payload["messages"][0]["text"]
