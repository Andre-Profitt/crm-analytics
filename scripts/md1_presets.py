#!/usr/bin/env python3
"""Shared loader for MD-1 preset config data."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class MD1Preset:
    name: str
    territory: str
    filters: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class MD1PresetConfig:
    path: Path
    dashboard_id: str
    generated: str
    notes: tuple[str, ...]
    presets: tuple[MD1Preset, ...]


def load_md1_preset_config(config_path: str | Path) -> MD1PresetConfig:
    path = Path(config_path)
    payload = json.loads(path.read_text(encoding="utf-8"))
    presets = tuple(
        MD1Preset(
            name=str(item["name"]),
            territory=str(item["territory"]),
            filters=tuple(dict(filter_item) for filter_item in (item.get("filters") or [])),
        )
        for item in (payload.get("presets") or [])
    )
    return MD1PresetConfig(
        path=path,
        dashboard_id=str(payload.get("dashboard_id") or ""),
        generated=str(payload.get("generated") or ""),
        notes=tuple(str(item) for item in (payload.get("notes") or []) if item is not None),
        presets=presets,
    )


def find_md1_preset(config: MD1PresetConfig, preset_name: str) -> MD1Preset | None:
    for preset in config.presets:
        if preset.name == preset_name:
            return preset
    return None


def md1_preset_config_summary(config: MD1PresetConfig) -> dict[str, Any]:
    return {
        "config_path": str(config.path),
        "dashboard_id": config.dashboard_id,
        "generated": config.generated,
        "notes": list(config.notes),
        "preset_count": len(config.presets),
        "preset_names": [preset.name for preset in config.presets],
    }


def md1_preset_summary(preset: MD1Preset) -> dict[str, Any]:
    return {
        "name": preset.name,
        "territory": preset.territory,
        "filter_count": len(preset.filters),
        "filters": [dict(item) for item in preset.filters],
    }
