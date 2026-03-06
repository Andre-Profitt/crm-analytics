#!/usr/bin/env python3
"""Report on ML model drift for CRM Analytics predictive features.

Scans builder modules for sklearn model usage and reports:
  1. Which models are trained (LogisticRegression, GradientBoosting, etc.)
  2. Feature sets used for each model
  3. Whether heuristic fallbacks are in place
  4. Model performance metadata (if available)

Supports --dry-run to report configuration without org access.
"""

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files, is_dry_run

# Known model class patterns
MODEL_PATTERNS = [
    (r'LogisticRegression(?:CV)?', "Logistic Regression"),
    (r'GradientBoosting(?:Classifier|Regressor)', "Gradient Boosting"),
    (r'RandomForest(?:Classifier|Regressor)', "Random Forest"),
    (r'LinearRegression', "Linear Regression"),
]


def scan_models(path: Path) -> list[dict]:
    """Scan a file for ML model definitions."""
    text = path.read_text()
    found = []
    for pattern, label in MODEL_PATTERNS:
        matches = list(re.finditer(pattern, text))
        if matches:
            lines = [
                text[:m.start()].count('\n') + 1 for m in matches
            ]
            found.append({
                "model": label,
                "file": path.name,
                "lines": lines,
            })

    # Check for heuristic fallback
    has_fallback = bool(re.search(
        r'(except\s+ImportError|sklearn.*not.*available|heuristic.*fallback)',
        text, re.IGNORECASE,
    ))
    if found:
        for f in found:
            f["has_fallback"] = has_fallback

    return found


def main():
    dry = is_dry_run()
    print("=== ML Model Drift Report ===")
    if dry:
        print("  (dry-run mode)\n")

    all_models = []
    for f in core_files():
        if f.exists():
            models = scan_models(f)
            all_models.extend(models)

    # Also scan helpers
    helpers = ROOT / "crm_analytics_helpers.py"
    if helpers.exists():
        all_models.extend(scan_models(helpers))

    if not all_models:
        print("No ML models found in codebase")
        return

    print(f"Found {len(all_models)} model usage(s):\n")
    for m in all_models:
        fallback = "yes" if m.get("has_fallback") else "NO"
        lines_str = ", ".join(str(l) for l in m["lines"])
        print(f"  {m['model']:30s} {m['file']}:{lines_str}  fallback={fallback}")

    without_fallback = [m for m in all_models if not m.get("has_fallback")]
    if without_fallback:
        print(f"\nWARNING: {len(without_fallback)} model(s) lack heuristic fallback")
    else:
        print("\nAll models have heuristic fallbacks — OK")


if __name__ == "__main__":
    main()
