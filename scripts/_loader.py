#!/usr/bin/env python3
"""Shared loader utilities for CRM Analytics scripts.

Provides common helpers used across the scripts/ directory:
  - project root resolution
  - dynamic import of crm_analytics_helpers
  - standard CLI argument parsing
"""

import importlib.util
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
"""Absolute path to the repository root (one level above scripts/)."""


def load_helpers():
    """Dynamically import crm_analytics_helpers from the project root."""
    helpers_path = ROOT / "crm_analytics_helpers.py"
    if not helpers_path.exists():
        print(f"ERROR: {helpers_path} not found", file=sys.stderr)
        sys.exit(1)
    spec = importlib.util.spec_from_file_location("crm_analytics_helpers", helpers_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def core_files():
    """Return a list of Path objects for all core builder modules."""
    names = [
        "crm_analytics_helpers.py",
        "build_dashboard.py",
        "build_revenue_motions.py",
        "build_sales_compliance.py",
        "build_customer_intelligence.py",
        "build_account_intelligence.py",
        "build_lead_management.py",
        "build_contract_operations.py",
        "build_forecasting.py",
        "build_pipeline_history.py",
        "build_arr_bridge.py",
    ]
    return [ROOT / n for n in names]


def script_files():
    """Return a list of Path objects for all scripts in this directory."""
    return sorted(
        p for p in (ROOT / "scripts").glob("*.py") if not p.name.startswith("__")
    )


def is_dry_run():
    """Return True if --dry-run was passed on the command line."""
    return "--dry-run" in sys.argv
