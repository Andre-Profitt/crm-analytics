"""Pytest collection config for the source-backed lane.

The default ``pytest tests/`` invocation runs only the source-backed suite —
tests that exercise the live monthly pipeline, source contracts, Track
A/B/C/D behavior, and the warehouse skeleton. Legacy / quarantined tests
live under ``tests/legacy/`` and are skipped by default.

Three ways to run the legacy lane:

* ``pytest --legacy``                              (run the whole legacy lane)
* ``pytest tests/legacy/ --legacy``                (target the directory)
* ``pytest tests/legacy/test_<name>.py --legacy``  (per-file)

Without ``--legacy``, the legacy directory is ignored at collection time so
files with ``ModuleNotFoundError`` / ``ImportError`` from missing legacy
scripts cannot break the source-backed CI lane.

See ``tests/legacy/README.md`` for the rationale and the per-file failure shape.
"""

from __future__ import annotations

from pathlib import Path


def pytest_addoption(parser) -> None:
    parser.addoption(
        "--legacy",
        action="store_true",
        default=False,
        help=(
            "Include quarantined legacy tests under tests/legacy/. "
            "Default invocations only run the source-backed lane."
        ),
    )


def pytest_ignore_collect(collection_path: Path, config) -> bool | None:
    """Skip the legacy directory at collection time unless ``--legacy`` is set.

    Returning ``True`` here prevents pytest from importing the file, so a
    legacy test that imports a missing module never breaks the
    source-backed lane. ``--legacy`` opts the lane back in: collection
    proceeds (and import errors surface, which is the expected behavior
    when the operator explicitly targets the legacy lane).
    """
    if config.getoption("--legacy"):
        return None  # pytest's default behavior — collect everything
    parts = collection_path.parts
    if "legacy" in parts and "tests" in parts:
        # Confirm the "legacy" segment is the immediate child of "tests".
        try:
            tests_idx = parts.index("tests")
        except ValueError:
            return None
        if len(parts) > tests_idx + 1 and parts[tests_idx + 1] == "legacy":
            return True
    return None
