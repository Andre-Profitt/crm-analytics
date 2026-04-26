from __future__ import annotations

import json
import zipfile
from pathlib import Path
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "package_claude_skills.py"


def test_package_claude_skills_creates_expected_zip_structure(tmp_path: Path) -> None:
    skills_root = tmp_path / "skills"
    skill_dir = skills_root / "demo-skill"
    resource_dir = skill_dir / "resources"
    resource_dir.mkdir(parents=True)
    (skill_dir / "Skill.md").write_text(
        "---\nname: Demo Skill\ndescription: Demo.\n---\n\n# Demo\n",
        encoding="utf-8",
    )
    (resource_dir / "note.md").write_text("demo", encoding="utf-8")

    output_root = tmp_path / "packages"
    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT),
            "--skills-root",
            str(skills_root),
            "--output-root",
            str(output_root),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout

    payload = json.loads(result.stdout)
    assert payload["package_count"] == 1
    archive_path = Path(payload["packages"][0]["archive_path"])
    assert archive_path.exists()

    with zipfile.ZipFile(archive_path) as archive:
        names = set(archive.namelist())
    assert "demo-skill/Skill.md" in names
    assert "demo-skill/resources/note.md" in names
