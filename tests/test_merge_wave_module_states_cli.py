from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "merge_wave_module_states.py"


def run_merge(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )


def write_state(path: Path, state: dict) -> Path:
    path.write_text(json.dumps(state), encoding="utf-8")
    return path


def test_merge_wave_module_states_merges_pages_and_shared_selectors(tmp_path: Path) -> None:
    state_one = write_state(
        tmp_path / "page2.json",
        {
            "filters": [],
            "gridLayouts": [{"pages": [{"name": "crm_data_quality", "widgets": [{"name": "w1", "row": 0, "column": 0, "rowspan": 2, "colspan": 2}]}]}],
            "widgets": {"w1": {"type": "number", "parameters": {"step": "s1", "measureField": "Value"}}},
            "steps": {
                "s1": {"type": "saql", "query": 'q = load "Dataset";\nq = group q by all;\nq = foreach q generate count() as Value;'},
                "f_region": {
                    "type": "aggregateflex",
                    "datasets": [{"name": "Dataset"}],
                    "query": {"query": '{"measures":[["count","*"]],"groups":["SalesRegion"]}'},
                },
            },
            "widgetStyle": {"backgroundColor": "#fff"},
        },
    )
    state_two = write_state(
        tmp_path / "page3.json",
        {
            "filters": [],
            "gridLayouts": [{"pages": [{"name": "process_compliance", "widgets": [{"name": "w2", "row": 0, "column": 0, "rowspan": 2, "colspan": 2}]}]}],
            "widgets": {"w2": {"type": "number", "parameters": {"step": "s2", "measureField": "Value"}}},
            "steps": {
                "s2": {
                    "type": "saql",
                    "query": (
                        'q = load "Dataset";\n'
                        'q = filter q by {{coalesce(column(f_region.selection, ["SalesRegion"]), '
                        'column(f_region.result, ["SalesRegion"])).asEquality(\'SalesRegion\')}};\n'
                        "q = group q by all;\n"
                        "q = foreach q generate count() as Value;"
                    ),
                },
                "f_region": {
                    "type": "aggregateflex",
                    "datasets": [{"name": "Dataset"}],
                    "query": {"query": '{"measures":[["count","*"]],"groups":["SalesRegion"]}'},
                },
            },
            "widgetStyle": {"backgroundColor": "#fff"},
        },
    )

    output_dir = tmp_path / "merged"
    result = run_merge(
        "--state",
        str(state_one),
        "--state",
        str(state_two),
        "--output-dir",
        str(output_dir),
        "--json",
    )
    assert result.returncode == 0, result.stderr or result.stdout
    payload = json.loads(result.stdout)
    assert payload["status"] == "ok"
    assert payload["summary"]["page_count"] == 2
    assert payload["summary"]["widget_count"] == 2
    assert payload["summary"]["step_count"] == 3
    assert payload["summary"]["contract_violation_count"] == 0
    merged_state = json.loads((output_dir / "dashboard_state.merged.json").read_text(encoding="utf-8"))
    assert [page["name"] for page in merged_state["gridLayouts"][0]["pages"]] == [
        "crm_data_quality",
        "process_compliance",
    ]
    assert sorted(merged_state["steps"].keys()) == ["f_region", "s1", "s2"]


def test_merge_wave_module_states_rejects_conflicting_step_definitions(tmp_path: Path) -> None:
    state_one = write_state(
        tmp_path / "page_a.json",
        {
            "filters": [],
            "gridLayouts": [{"pages": [{"name": "page_a", "widgets": []}]}],
            "widgets": {},
            "steps": {"shared_step": {"type": "saql", "query": 'q = load "A";'}},
            "widgetStyle": {},
        },
    )
    state_two = write_state(
        tmp_path / "page_b.json",
        {
            "filters": [],
            "gridLayouts": [{"pages": [{"name": "page_b", "widgets": []}]}],
            "widgets": {},
            "steps": {"shared_step": {"type": "saql", "query": 'q = load "B";'}},
            "widgetStyle": {},
        },
    )

    result = run_merge(
        "--state",
        str(state_one),
        "--state",
        str(state_two),
        "--output-dir",
        str(tmp_path / "merged_conflict"),
        "--json",
    )
    assert result.returncode != 0
    assert "conflicting step definition" in (result.stderr or result.stdout)
