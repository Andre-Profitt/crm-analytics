from __future__ import annotations

import json
from pathlib import Path

from scripts.lint_monthly_source_contract import lint_monthly_source_contract


def test_current_monthly_source_contract_lint_passes() -> None:
    result = lint_monthly_source_contract(
        requirements_path=Path("config/monthly_source_requirements.json"),
        bundle_contract_path=Path("config/monthly_director_bundle_contract.json"),
    )

    assert result["status"] == "ok"
    assert result["high_finding_count"] == 0
    assert result["publish_required_source_backed_dataset_count"] == 4


def test_publish_required_dataset_blocks_when_requirement_disabled(
    tmp_path: Path,
) -> None:
    requirements = json.loads(Path("config/monthly_source_requirements.json").read_text())
    for requirement in requirements["requirements"]:
        if requirement["requirement_id"] == "sd_pipeline_inspection":
            requirement["enabled"] = False
    requirements_path = tmp_path / "monthly_source_requirements.json"
    requirements_path.write_text(json.dumps(requirements), encoding="utf-8")

    result = lint_monthly_source_contract(
        requirements_path=requirements_path,
        bundle_contract_path=Path("config/monthly_director_bundle_contract.json"),
    )

    issues = {finding["issue"] for finding in result["findings"]}
    assert result["status"] == "blocked"
    assert "source_requirement_id_disabled" in issues
    assert "publish_required_dataset_has_no_enabled_requirement" in issues
