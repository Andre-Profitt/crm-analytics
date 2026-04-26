"""Pytest collection config for the source-backed lane.

The default ``pytest tests/`` invocation runs only the source-backed suite —
tests that exercise the live monthly pipeline, source contracts, Track
A/B/C/D behavior, and the warehouse skeleton. Legacy / quarantined tests
live under ``tests/legacy/`` and are skipped by default unless EITHER:

* the user passes ``--legacy``, OR
* the user explicitly targets a path inside ``tests/legacy/`` on the
  command line (per-file or per-dir targeting is itself an opt-in).

So all of these run the legacy lane:

    pytest --legacy
    pytest tests/legacy/ --legacy
    pytest tests/legacy/                           (directory target = opt-in)
    pytest tests/legacy/test_<name>.py             (file target = opt-in)

And ``pytest tests/`` (the recursive default) skips ``tests/legacy/``
entirely — the directory match is ignored at collection time so files
that ``ImportError`` on collection (missing legacy modules) cannot break
the source-backed lane.

See ``tests/legacy/README.md`` for the rationale and per-file failure shape.
"""

from __future__ import annotations

from pathlib import Path


LEGACY_DIR_NAME = "legacy"

# Anchor the legacy directory to *this* conftest's location, not pytest's
# ``rootpath``. The repo's parent directory hosts a global pytest.ini, so
# rootpath resolves outside the repo and would let an unrelated absolute
# path containing "tests/" confuse the filter.
TESTS_DIR = Path(__file__).resolve().parent
LEGACY_ROOT = (TESTS_DIR / LEGACY_DIR_NAME).resolve()


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


def _user_explicitly_targeted_legacy(config) -> bool:
    """True if any pytest CLI arg points at a path inside ``tests/legacy/``.

    Per-file (``pytest tests/legacy/test_x.py``) and per-dir
    (``pytest tests/legacy/``) targeting both count as explicit opt-in —
    typing the path is itself the signal that the user wants the legacy
    lane for that scope, even without ``--legacy``.
    """
    for arg in config.args or []:
        # CLI args may include ``::test_name`` selectors; strip them.
        path_str = str(arg).split("::", 1)[0]
        if not path_str:
            continue
        try:
            arg_path = Path(path_str).resolve()
        except (OSError, ValueError):
            continue
        try:
            arg_path.relative_to(LEGACY_ROOT)
        except ValueError:
            continue
        return True
    return False


def pytest_ignore_collect(collection_path: Path, config) -> bool | None:
    """Skip ``tests/legacy/`` at collection time unless opted in.

    Returning ``True`` here prevents pytest from importing the file, so a
    legacy test that imports a missing module never breaks the
    source-backed lane. ``--legacy`` and explicit per-path targeting both
    opt the lane back in: collection proceeds, and any import errors
    surface — which is the expected behavior when the operator targets
    the legacy lane on purpose.
    """
    if config.getoption("--legacy"):
        return None
    try:
        collection_path.resolve().relative_to(LEGACY_ROOT)
    except (ValueError, OSError):
        return None  # outside tests/legacy/, default behavior
    if _user_explicitly_targeted_legacy(config):
        return None  # explicit per-path targeting is its own opt-in
    return True
