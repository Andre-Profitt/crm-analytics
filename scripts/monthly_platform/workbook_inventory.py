from __future__ import annotations

from datetime import date
from pathlib import Path


def available_snapshot_dates(workbook_root: Path) -> list[str]:
    if not workbook_root.exists():
        return []

    snapshot_dates: list[str] = []
    for child in sorted(workbook_root.iterdir()):
        if not child.is_dir():
            continue
        try:
            date.fromisoformat(child.name)
        except ValueError:
            continue
        snapshot_dates.append(child.name)
    return snapshot_dates
