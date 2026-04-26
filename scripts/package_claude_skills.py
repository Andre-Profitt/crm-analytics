#!/usr/bin/env python3
"""Package local Claude custom skills into uploadable ZIP files."""

from __future__ import annotations

import argparse
import json
import shutil
from datetime import UTC, datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SKILLS_ROOT = REPO_ROOT / "claude_skills"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "output" / "claude_skill_packages"


def skill_dirs(root: Path) -> list[Path]:
    return sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and (path / "Skill.md").exists()
    )


def package_skill(skill_dir: Path, output_root: Path) -> dict[str, str]:
    output_root.mkdir(parents=True, exist_ok=True)
    archive_base = output_root / skill_dir.name
    archive_path = shutil.make_archive(str(archive_base), "zip", root_dir=skill_dir.parent, base_dir=skill_dir.name)
    return {
        "skill_name": skill_dir.name,
        "skill_dir": str(skill_dir),
        "archive_path": archive_path,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skills-root", type=Path, default=SKILLS_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--skill", help="Optional single skill directory name to package.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    directories = skill_dirs(args.skills_root)
    if args.skill:
        directories = [path for path in directories if path.name == args.skill]
    if not directories:
        raise SystemExit("No Claude skill directories matched the requested scope.")

    packages = [package_skill(skill_dir, args.output_root) for skill_dir in directories]
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "skills_root": str(args.skills_root),
        "output_root": str(args.output_root),
        "package_count": len(packages),
        "packages": packages,
    }
    manifest_path = args.output_root / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
