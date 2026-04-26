#!/usr/bin/env python3
"""Install repo-hosted Codex skills into the local Codex skills directory."""

from __future__ import annotations

import argparse
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOT = REPO_ROOT / "codex_skills"
DEFAULT_TARGET_ROOT = Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))) / "skills"


def skill_dirs(root: Path) -> list[Path]:
    return sorted(path for path in root.iterdir() if path.is_dir() and (path / "SKILL.md").exists())


def install_skill(source_dir: Path, target_root: Path, *, link: bool) -> dict[str, str]:
    target_root.mkdir(parents=True, exist_ok=True)
    destination = target_root / source_dir.name
    if destination.exists() or destination.is_symlink():
        if destination.is_symlink() or destination.is_file():
            destination.unlink()
        else:
            shutil.rmtree(destination)
    if link:
        destination.symlink_to(source_dir, target_is_directory=True)
        mode = "symlink"
    else:
        shutil.copytree(source_dir, destination)
        mode = "copy"
    return {
        "skill_name": source_dir.name,
        "source_dir": str(source_dir),
        "destination": str(destination),
        "mode": mode,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-root", type=Path, default=DEFAULT_SOURCE_ROOT)
    parser.add_argument("--target-root", type=Path, default=DEFAULT_TARGET_ROOT)
    parser.add_argument("--skill", help="Optional single skill folder name to install.")
    parser.add_argument("--link", action="store_true", help="Install as symlinks instead of copies.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    directories = skill_dirs(args.source_root)
    if args.skill:
        directories = [path for path in directories if path.name == args.skill]
    if not directories:
        raise SystemExit("No repo-hosted Codex skill directories matched the requested scope.")

    installs = [install_skill(path, args.target_root, link=args.link) for path in directories]
    manifest = {
        "generated_at": datetime.now(UTC).isoformat(),
        "source_root": str(args.source_root),
        "target_root": str(args.target_root),
        "install_count": len(installs),
        "installs": installs,
    }
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
