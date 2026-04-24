from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import compile_monthly_source_contract_config as compiler  # noqa: E402


def _write_authoring(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "monthly_platform.source_contract_authoring.v1",
                "compiled_targets": {
                    "example": {
                        "path": "config/example.json",
                        "payload": payload,
                    }
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )


def test_compile_check_passes_when_runtime_json_matches_yaml(tmp_path: Path) -> None:
    authoring_path = tmp_path / "contract.yaml"
    runtime_path = tmp_path / "config" / "example.json"
    payload = {"schema_version": "example.v1", "items": [{"name": "pipeline"}]}
    _write_authoring(authoring_path, payload)
    runtime_path.parent.mkdir(parents=True)
    runtime_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")

    result = compiler.compile_monthly_source_contract_config(
        authoring_path=authoring_path,
        output_root=tmp_path,
        check=True,
    )

    assert result["status"] == "ok"
    assert result["target_count"] == 1
    assert result["drift_count"] == 0
    assert result["finding_count"] == 0


def test_compile_check_blocks_when_runtime_json_drifts(tmp_path: Path) -> None:
    authoring_path = tmp_path / "contract.yaml"
    runtime_path = tmp_path / "config" / "example.json"
    _write_authoring(authoring_path, {"schema_version": "example.v1", "enabled": True})
    runtime_path.parent.mkdir(parents=True)
    runtime_path.write_text(
        json.dumps({"schema_version": "example.v1", "enabled": False}) + "\n",
        encoding="utf-8",
    )

    result = compiler.compile_monthly_source_contract_config(
        authoring_path=authoring_path,
        output_root=tmp_path,
        check=True,
    )

    assert result["status"] == "blocked"
    assert result["drift_count"] == 1
    assert result["findings"][0]["issue"] == "compiled_json_drift"


def test_compile_write_regenerates_runtime_json(tmp_path: Path) -> None:
    authoring_path = tmp_path / "contract.yaml"
    runtime_path = tmp_path / "config" / "example.json"
    payload = {"schema_version": "example.v1", "enabled": True}
    _write_authoring(authoring_path, payload)
    runtime_path.parent.mkdir(parents=True)
    runtime_path.write_text(
        json.dumps({"schema_version": "example.v1", "enabled": False}) + "\n",
        encoding="utf-8",
    )

    result = compiler.compile_monthly_source_contract_config(
        authoring_path=authoring_path,
        output_root=tmp_path,
        check=True,
        write=True,
    )

    assert result["status"] == "ok"
    assert result["drift_count"] == 0
    assert json.loads(runtime_path.read_text(encoding="utf-8")) == payload
