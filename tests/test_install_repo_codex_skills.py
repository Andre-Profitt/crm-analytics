from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "install_repo_codex_skills.py"


def test_install_repo_codex_skills_copies_skill_folder(tmp_path: Path) -> None:
    source_root = tmp_path / "source"
    skill_dir = source_root / "demo-skill"
    skill_dir.mkdir(parents=True)
    (skill_dir / "SKILL.md").write_text(
        "---\nname: demo-skill\ndescription: Demo skill.\n---\n\n# Demo\n",
        encoding="utf-8",
    )

    target_root = tmp_path / "target"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--source-root",
            str(source_root),
            "--target-root",
            str(target_root),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert payload["install_count"] == 1
    assert (target_root / "demo-skill" / "SKILL.md").exists()
