"""Track F / F4 — brand fingerprint validator.

Verifies that the SimCorp PowerPoint template the deck builder loads
matches the brand block declared in ``config/deck_contract.yaml``:

    brand:
      template: assets/SimCorp_PPT_Template.pptx
      expected_template_sha256: <hex>
      expected_template_size_bytes: <int>          # optional
      expected_slide_master_count: 1               # optional, default 1
      required_layouts:                            # optional
        - Title 1
        - Title and Content
        - ...
      theme:
        fonts:  { heading: <str>, body: <str> }   # optional
        colors: { name: "#RRGGBB", ... }          # optional

Checks (all blockers unless noted):

  - template file exists at brand.template (relative to repo root)
  - file SHA-256 matches expected_template_sha256
  - file size matches expected_template_size_bytes (when declared)
  - slide_master count matches expected_slide_master_count (default 1)
  - every name in required_layouts exists on the template's layouts
  - theme color hex syntax is valid (warning, not blocker)

Read-only — never modifies the template or any deck file.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pptx import Presentation

from scripts.monthly_platform import deck_contract


REPORT_SCHEMA_VERSION = "monthly_platform.brand_fingerprint_report.v1"
HEX_COLOR_RE = re.compile(r"^#[0-9A-Fa-f]{6}$")


@dataclass
class BrandFinding:
    severity: str
    code: str
    path: str
    message: str

    def as_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "code": self.code,
            "path": self.path,
            "message": self.message,
        }


@dataclass
class BrandReport:
    status: str
    blocker_count: int
    warning_count: int
    template_path: str
    template_sha256: str
    expected_sha256: str
    template_size_bytes: int
    expected_size_bytes: int | None
    slide_master_count: int
    expected_slide_master_count: int
    layouts_present: list[str]
    layouts_missing: list[str]
    theme_color_count: int
    theme_color_invalid: list[str]
    findings: list[BrandFinding] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema_version": REPORT_SCHEMA_VERSION,
            "validated_at": datetime.now(timezone.utc).isoformat(),
            "status": self.status,
            "blocker_count": self.blocker_count,
            "warning_count": self.warning_count,
            "template_path": self.template_path,
            "template_sha256": self.template_sha256,
            "expected_sha256": self.expected_sha256,
            "template_size_bytes": self.template_size_bytes,
            "expected_size_bytes": self.expected_size_bytes,
            "slide_master_count": self.slide_master_count,
            "expected_slide_master_count": self.expected_slide_master_count,
            "layouts_present": self.layouts_present,
            "layouts_missing": self.layouts_missing,
            "theme_color_count": self.theme_color_count,
            "theme_color_invalid": self.theme_color_invalid,
            "findings": [f.as_dict() for f in self.findings],
        }


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def validate_brand(
    contract: deck_contract.DeckContract | None = None,
    *,
    repo_root: Path | None = None,
) -> BrandReport:
    if contract is None:
        contract = deck_contract.load()
    assert contract is not None

    repo_root = repo_root or contract.path.parent.parent  # config/ -> repo
    brand = contract.raw.get("brand", {}) or {}

    findings: list[BrandFinding] = []
    template_rel = str(brand.get("template", "") or "")
    expected_sha = str(brand.get("expected_template_sha256", "") or "")
    expected_size = brand.get("expected_template_size_bytes")
    expected_master = int(brand.get("expected_slide_master_count", 1) or 1)
    required_layouts = list(brand.get("required_layouts") or [])
    theme = brand.get("theme") or {}
    theme_colors = theme.get("colors") or {}

    template_path = (repo_root / template_rel) if template_rel else None
    template_sha = ""
    template_size = 0
    layouts_present: list[str] = []
    layouts_missing: list[str] = []
    slide_master_count = 0
    theme_color_invalid: list[str] = []

    # 1. template file exists
    if template_path is None or not template_path.exists():
        findings.append(
            BrandFinding(
                severity="blocker",
                code="template_missing",
                path="brand.template",
                message=f"template file not found: {template_path}",
            )
        )
    else:
        template_sha = _sha256(template_path)
        template_size = template_path.stat().st_size

        # 2. SHA-256 match
        if not expected_sha:
            findings.append(
                BrandFinding(
                    severity="blocker",
                    code="missing_expected_sha256",
                    path="brand.expected_template_sha256",
                    message="brand.expected_template_sha256 is required",
                )
            )
        elif template_sha != expected_sha:
            findings.append(
                BrandFinding(
                    severity="blocker",
                    code="template_sha256_mismatch",
                    path="brand.expected_template_sha256",
                    message=(
                        f"template SHA-256 mismatch: actual={template_sha} "
                        f"expected={expected_sha}"
                    ),
                )
            )

        # 3. size (optional)
        if expected_size is not None and template_size != int(expected_size):
            findings.append(
                BrandFinding(
                    severity="warning",
                    code="template_size_mismatch",
                    path="brand.expected_template_size_bytes",
                    message=(
                        f"template size mismatch: actual={template_size} "
                        f"expected={expected_size} (sha mismatch is the blocker; "
                        f"size is informational)"
                    ),
                )
            )

        # 4. slide masters + layouts
        try:
            prs = Presentation(str(template_path))
        except Exception as e:
            findings.append(
                BrandFinding(
                    severity="blocker",
                    code="template_parse_error",
                    path="brand.template",
                    message=f"could not parse template: {e}",
                )
            )
        else:
            slide_master_count = len(prs.slide_masters)
            if slide_master_count != expected_master:
                findings.append(
                    BrandFinding(
                        severity="blocker",
                        code="slide_master_count_mismatch",
                        path="brand.expected_slide_master_count",
                        message=(
                            f"slide_master count mismatch: actual={slide_master_count} "
                            f"expected={expected_master}"
                        ),
                    )
                )

            actual_layout_names = {layout.name for layout in prs.slide_layouts}
            layouts_present = sorted(actual_layout_names)
            for required in required_layouts:
                if required not in actual_layout_names:
                    layouts_missing.append(required)
                    findings.append(
                        BrandFinding(
                            severity="blocker",
                            code="required_layout_missing",
                            path=f"brand.required_layouts[{required!r}]",
                            message=(
                                f"required layout {required!r} not found on template; "
                                f"available layouts: {sorted(actual_layout_names)[:5]}..."
                            ),
                        )
                    )

    # 5. theme color hex validity
    for color_name, color_value in theme_colors.items():
        if not HEX_COLOR_RE.match(str(color_value)):
            theme_color_invalid.append(color_name)
            findings.append(
                BrandFinding(
                    severity="warning",
                    code="theme_color_invalid_hex",
                    path=f"brand.theme.colors.{color_name}",
                    message=f"invalid hex color {color_value!r} (expected #RRGGBB)",
                )
            )

    blockers = [f for f in findings if f.severity == "blocker"]
    warnings = [f for f in findings if f.severity == "warning"]

    return BrandReport(
        status="pass" if not blockers else "fail",
        blocker_count=len(blockers),
        warning_count=len(warnings),
        template_path=str(template_path) if template_path else "",
        template_sha256=template_sha,
        expected_sha256=expected_sha,
        template_size_bytes=template_size,
        expected_size_bytes=int(expected_size) if expected_size is not None else None,
        slide_master_count=slide_master_count,
        expected_slide_master_count=expected_master,
        layouts_present=layouts_present,
        layouts_missing=layouts_missing,
        theme_color_count=len(theme_colors),
        theme_color_invalid=theme_color_invalid,
        findings=findings,
    )


__all__ = [
    "BrandFinding",
    "BrandReport",
    "REPORT_SCHEMA_VERSION",
    "validate_brand",
]
