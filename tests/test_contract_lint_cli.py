from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "contract_lint.py"


def run_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def test_contract_lint_self_test_json() -> None:
    result = run_cli("--json")
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["self_test"] is True


def test_contract_lint_warns_on_invalid_state_json(tmp_path: Path) -> None:
    path = tmp_path / "dashboard.json"
    path.write_text(
        json.dumps(
            {
                "state": {
                    "gridLayouts": [{"pages": [{"name": "overview", "widgets": []}]}],
                    "steps": {
                        "agg_filter": {
                            "type": "aggregateflex",
                            "isFacet": True,
                            "datasets": [{"name": "Dataset", "label": "Bad"}],
                        }
                    },
                    "widgets": {},
                }
            }
        ),
        encoding="utf-8",
    )

    result = run_cli(str(path), "--json")
    assert result.returncode == 1
    payload = json.loads(result.stdout)
    assert payload["status"] == "warn"
    assert payload["summary"]["total_violations"] >= 1
    assert payload["results"][0]["status"] == "warn"
