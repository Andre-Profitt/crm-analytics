from __future__ import annotations

import json
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.run_sales_region_canonical_shell_builder as module


def test_promote_canonical_shell_writes_stable_and_dated_copies(tmp_path: Path) -> None:
    working = tmp_path / "working.pptx"
    working.write_bytes(b"pptx")

    result = module.promote_canonical_shell(
        working_deck_path=working,
        canonical_root=tmp_path / "canonical",
        region_name="EMEA",
        snapshot_date="2026-04-10",
    )

    stable = Path(result["stable_path"])
    dated = Path(result["dated_path"])
    assert stable.exists()
    assert dated.exists()
    assert stable.read_bytes() == b"pptx"
    assert dated.read_bytes() == b"pptx"


def test_main_skip_mode_writes_authoring_bundle(monkeypatch, tmp_path: Path) -> None:
    template = tmp_path / "SimCorp_PPT_Template.pptx"
    template.write_bytes(b"pptx")
    contract = tmp_path / "shell.json"
    contract.write_text(json.dumps({"slides": []}), encoding="utf-8")
    design = tmp_path / "design.md"
    design.write_text("# design\n", encoding="utf-8")

    argv = [
        "run_sales_region_canonical_shell_builder.py",
        "--region-name",
        "EMEA",
        "--snapshot-date",
        "2026-04-10",
        "--master-template-path",
        str(template),
        "--shell-contract-path",
        str(contract),
        "--design-system-path",
        str(design),
        "--run-root",
        str(tmp_path / "runs"),
        "--canonical-root",
        str(tmp_path / "canonical"),
        "--powerpoint-mode",
        "skip",
    ]
    monkeypatch.setattr(sys, "argv", argv)
    module.main()

    manifests = list((tmp_path / "runs" / "2026-04-10").glob("*/emea/manifest.json"))
    assert manifests
    manifest = json.loads(manifests[0].read_text())
    assert manifest["powerpoint_shell_authoring"]["status"] == "skipped"
    assert Path(manifest["working_deck_path"]).exists()
    assert Path(manifest["shell_author_prompt_path"]).exists()
