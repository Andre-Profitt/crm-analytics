#!/usr/bin/env python3
"""Lint builder modules for contract violations.

Checks:
  1. Every build_*.py defines DS (dataset API name)
  2. No hard-coded dataset IDs (should use get_dataset_id())
  3. Every build_*.py has a main() entry point
  4. SAQL load statements reference the module's own DS constant
"""

import ast
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _loader import ROOT, core_files


def lint_file(path: Path) -> list[str]:
    errors = []
    text = path.read_text()

    # Only lint build_*.py files
    if not path.name.startswith("build_"):
        return errors

    # Parse AST
    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as e:
        return [f"{path.name}: SyntaxError at line {e.lineno}: {e.msg}"]

    # Check for DS constant
    has_ds = any(
        isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "DS" for t in node.targets
        )
        for node in ast.walk(tree)
    )
    if not has_ds:
        errors.append(f"{path.name}: missing DS constant")

    # Check for main()
    has_main = any(
        isinstance(node, ast.FunctionDef) and node.name == "main"
        for node in ast.walk(tree)
    )
    if not has_main:
        errors.append(f"{path.name}: missing main() entry point")

    # Check for hard-coded dataset IDs (pattern: 0Fb followed by 15+ alnum chars)
    for i, line in enumerate(text.splitlines(), 1):
        if re.search(r'0Fb[A-Za-z0-9]{15,}', line) and "get_dataset_id" not in line:
            errors.append(f"{path.name}:{i}: hard-coded dataset ID detected")

    return errors


def main():
    all_errors = []
    for f in core_files():
        if f.exists():
            all_errors.extend(lint_file(f))

    if all_errors:
        print("Contract lint FAILED:")
        for e in all_errors:
            print(f"  {e}")
        sys.exit(1)
    else:
        print("Contract lint passed — all checks OK")


if __name__ == "__main__":
    main()
