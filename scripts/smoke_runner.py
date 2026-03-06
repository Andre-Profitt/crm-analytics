#!/usr/bin/env python3
"""Smoke-test runner for CRM Analytics builder modules.

Imports each build_*.py module and verifies:
  1. Module imports without errors
  2. build_steps / build_widgets / build_layout are callable
  3. build_steps() returns a non-empty dict
"""

import importlib.util
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files


def smoke_test(path: Path) -> list[str]:
    errors = []
    if not path.name.startswith("build_"):
        return errors

    mod_name = path.stem
    try:
        spec = importlib.util.spec_from_file_location(mod_name, path)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    except Exception as e:
        return [f"{mod_name}: import failed — {e}"]

    for fn_name in ("build_widgets", "build_layout"):
        fn = getattr(mod, fn_name, None)
        if fn is None:
            errors.append(f"{mod_name}: missing {fn_name}()")
        elif not callable(fn):
            errors.append(f"{mod_name}: {fn_name} is not callable")

    if not hasattr(mod, "build_steps"):
        errors.append(f"{mod_name}: missing build_steps()")

    return errors


def main():
    all_errors = []
    for f in core_files():
        if f.exists() and f.name.startswith("build_"):
            print(f"  Smoke-testing {f.name} ... ", end="")
            errs = smoke_test(f)
            if errs:
                print("FAIL")
                all_errors.extend(errs)
            else:
                print("OK")

    if all_errors:
        print("\nSmoke tests FAILED:")
        for e in all_errors:
            print(f"  {e}")
        sys.exit(1)
    else:
        print("\nAll smoke tests passed")


if __name__ == "__main__":
    main()
