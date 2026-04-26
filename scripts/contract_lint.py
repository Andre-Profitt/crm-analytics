#!/usr/bin/env python3
"""Static Wave PATCH contract lint for dashboard states."""

from __future__ import annotations

import argparse
from collections import Counter
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_contract_helpers():
    from crm_analytics_helpers import (
        find_dashboard_patch_contract_violations,
        normalize_dashboard_state_for_patch,
    )

    return find_dashboard_patch_contract_violations, normalize_dashboard_state_for_patch


def _load_state(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("state"), dict):
        return payload["state"]
    if isinstance(payload, dict):
        return payload
    raise ValueError(f"{path}: expected dashboard JSON object or state object")


def _expand_input_paths(raw_paths: list[str]) -> list[Path]:
    expanded: list[Path] = []
    for raw_path in raw_paths:
        path = Path(raw_path)
        if path.is_dir():
            expanded.extend(sorted(path.glob("**/dashboard.json")))
        else:
            expanded.append(path)
    return expanded


def _print_violations(path: Path, violations: list[dict[str, str]]) -> None:
    print(f"{path}: {len(violations)} violation(s)")
    for violation in violations:
        print(
            f"  - {violation['code']}: {violation['path']} :: {violation['message']}"
        )


def _print_summary(results: list[tuple[Path, list[dict[str, str]]]]) -> None:
    total_files = len(results)
    files_with_violations = sum(1 for _, violations in results if violations)
    total_violations = sum(len(violations) for _, violations in results)

    print(
        "contract_lint:"
        f" {files_with_violations}/{total_files} file(s) with violations,"
        f" {total_violations} total violation(s)"
    )

    if not total_violations:
        return

    violation_counts = Counter(
        violation["code"]
        for _, violations in results
        for violation in violations
    )
    file_counts = Counter(
        code
        for _, violations in results
        for code in {item["code"] for item in violations}
    )

    print("By code:")
    for code, count in violation_counts.most_common():
        print(f"  - {code}: {count} across {file_counts[code]} file(s)")

    print("Top files:")
    top_files = sorted(
        (
            (len(violations), path)
            for path, violations in results
            if violations
        ),
        reverse=True,
    )[:10]
    for count, path in top_files:
        print(f"  - {count}: {path}")


def _make_message(level: str, code: str, text: str) -> dict[str, str]:
    return {"level": level, "code": code, "text": text}


def _make_result(
    *,
    status: str,
    messages: list[dict[str, str]],
    **extra: Any,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "status": status,
        "tool": "contract_lint",
        "lane": "patch_guardrails",
        "command_class": "read_only",
        "messages": messages,
        "artifacts": [],
    }
    payload.update(extra)
    return payload


def _sample_state() -> dict[str, Any]:
    return {
        "layouts": {"legacy": True},
        "gridLayouts": [
            {
                "selectors": [],
                "numColumns": 12,
                "pages": [
                    {
                        "name": "overview",
                        "label": "Overview",
                        "navigationHidden": False,
                        "widgets": [],
                    }
                ],
            }
        ],
        "steps": {
            "agg_filter": {
                "type": "aggregateflex",
                "isFacet": True,
                "datasets": [
                    {
                        "name": "Dataset",
                        "label": "Dataset Label",
                        "url": "/services/data/v66.0/wave/datasets/1",
                    }
                ],
                "query": {
                    "query": '{"groups": [&quot;Owner&quot;], "measures": [["count", "*"]]}',
                    "datasets": [
                        {
                            "name": "Dataset",
                            "label": "Inner Dataset Label",
                            "url": "/services/data/v66.0/wave/datasets/1",
                        }
                    ],
                },
            },
            "saql_step": {"type": "saql", "query": 'q = load &quot;Dataset&quot;;'},
        },
        "widgets": {
            "bad_chart": {
                "type": "chart",
                "parameters": {
                    "visualizationType": "hbar",
                    "columnMap": {
                        "dimensionAxis": [],
                        "plots": [],
                        "trellis": [],
                    },
                },
            },
            "bad_funnel": {
                "type": "chart",
                "parameters": {
                    "visualizationType": "funnel",
                    "columnMap": {
                        "dimensionAxis": [],
                        "plots": [],
                        "trellis": [],
                        "split": [],
                    },
                },
            },
            "bad_number": {
                "type": "number",
                "parameters": {
                    "title": "ARR",
                    "compact": True,
                    "numberFormat": "$#,##0",
                },
            },
            "bad_link": {
                "type": "link",
                "parameters": {
                    "destinationLink": {"name": "missing_page"},
                },
            },
        },
    }


def _run_self_test(*, emit_text: bool = True) -> int:
    (
        find_dashboard_patch_contract_violations,
        normalize_dashboard_state_for_patch,
    ) = _load_contract_helpers()
    sample = _sample_state()
    violations = find_dashboard_patch_contract_violations(sample)
    codes = {item["code"] for item in violations}
    expected_codes = {
        "aggregateflex_isfacet",
        "dataset_readonly_fields",
        "columnmap_missing_keys",
        "columnmap_must_be_null",
        "number_widget_banned_fields",
        "destination_link_name_mismatch",
    }
    missing_codes = sorted(expected_codes - codes)
    if missing_codes:
        raise AssertionError(
            "contract lint self-test missed expected codes: "
            + ", ".join(missing_codes)
        )

    normalized = normalize_dashboard_state_for_patch(
        sample,
        strip_page_labels=True,
        strip_number_widget_patch_fields=True,
    )

    page = normalized["gridLayouts"][0]["pages"][0]
    agg_filter = normalized["steps"]["agg_filter"]
    agg_dataset = agg_filter["datasets"][0]
    inner_dataset = agg_filter["query"]["datasets"][0]
    bad_number = normalized["widgets"]["bad_number"]["parameters"]

    if "layouts" in normalized:
        raise AssertionError("normalize_dashboard_state_for_patch should drop layouts")
    if "selectors" in normalized["gridLayouts"][0] or "numColumns" in normalized["gridLayouts"][0]:
        raise AssertionError("gridLayouts metadata was not stripped")
    if "label" in page or "navigationHidden" in page:
        raise AssertionError("page metadata was not stripped")
    if "isFacet" in agg_filter:
        raise AssertionError("aggregateflex isFacet should be stripped")
    if any(key in agg_dataset for key in ("label", "url")):
        raise AssertionError("step dataset read-only fields should be stripped")
    if any(key in inner_dataset for key in ("label", "url")):
        raise AssertionError("query dataset read-only fields should be stripped")
    if "&quot;" in normalized["steps"]["saql_step"]["query"]:
        raise AssertionError("SAQL query should be fully unescaped")
    if "&quot;" in agg_filter["query"]["query"]:
        raise AssertionError("aggregateflex query JSON should be fully unescaped")
    if any(key in bad_number for key in ("compact", "numberFormat", "title")):
        raise AssertionError("number widget banned fields should be stripped when requested")

    if emit_text:
        print("contract_lint: self-test passed")
    return 0


def _run_self_test_json() -> tuple[dict[str, Any], int]:
    _run_self_test(emit_text=False)
    return (
        _make_result(
            status="ok",
            messages=[
                _make_message(
                    "info",
                    "self_test_passed",
                    "contract_lint self-test passed.",
                )
            ],
            summary={
                "self_test": True,
                "files_checked": 1,
                "files_with_violations": 0,
                "total_violations": 0,
            },
            results=[],
        ),
        0,
    )


def _build_json_payload(
    *,
    raw_paths: list[str],
    normalized: bool,
) -> tuple[dict[str, Any], int]:
    expanded_paths = _expand_input_paths(raw_paths)
    if not expanded_paths:
        return (
            _make_result(
                status="error",
                messages=[
                    _make_message(
                        "error",
                        "no_dashboard_json",
                        "No dashboard JSON files found.",
                    )
                ],
                summary={
                    "self_test": False,
                    "normalized": normalized,
                    "files_checked": 0,
                    "files_with_violations": 0,
                    "total_violations": 0,
                },
                results=[],
            ),
            1,
        )

    (
        find_dashboard_patch_contract_violations,
        normalize_dashboard_state_for_patch,
    ) = _load_contract_helpers()
    results: list[dict[str, Any]] = []
    total_violations = 0
    error_count = 0

    for path in expanded_paths:
        entry: dict[str, Any] = {
            "path": str(path),
            "normalized": normalized,
            "violation_count": 0,
            "violations": [],
        }
        if not path.exists():
            entry["status"] = "error"
            entry["error"] = "file not found"
            results.append(entry)
            error_count += 1
            continue
        try:
            state = _load_state(path)
            if normalized:
                state = normalize_dashboard_state_for_patch(
                    state,
                    strip_page_labels=True,
                    strip_number_widget_patch_fields=True,
                )
            violations = find_dashboard_patch_contract_violations(state)
        except Exception as exc:
            entry["status"] = "error"
            entry["error"] = str(exc)
            results.append(entry)
            error_count += 1
            continue

        entry["status"] = "warn" if violations else "ok"
        entry["violation_count"] = len(violations)
        entry["violations"] = violations
        results.append(entry)
        total_violations += len(violations)

    files_with_violations = sum(
        1 for item in results if item["status"] == "warn"
    )
    status = "error" if error_count else ("warn" if total_violations else "ok")
    messages: list[dict[str, str]] = []
    if status == "error":
        messages.append(
            _make_message(
                "error",
                "lint_failed",
                f"contract_lint encountered {error_count} file error(s).",
            )
        )
    elif status == "warn":
        messages.append(
            _make_message(
                "warn",
                "violations_found",
                f"Found {total_violations} violation(s) across {files_with_violations} file(s).",
            )
        )
    else:
        messages.append(
            _make_message(
                "info",
                "lint_clean",
                f"Checked {len(results)} file(s) with 0 violations.",
            )
        )

    return (
        _make_result(
            status=status,
            messages=messages,
            summary={
                "self_test": False,
                "normalized": normalized,
                "files_checked": len(results),
                "files_with_violations": files_with_violations,
                "total_violations": total_violations,
                "file_errors": error_count,
            },
            results=results,
        ),
        1 if status in {"warn", "error"} else 0,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "paths",
        nargs="*",
        help="Dashboard JSON files or directories containing dashboard.json exports.",
    )
    parser.add_argument(
        "--normalized",
        action="store_true",
        help=(
            "Lint normalized PATCH state instead of raw exports. "
            "This also strips PATCH-banned number widget fields."
        ),
    )
    parser.add_argument(
        "--summary",
        action="store_true",
        help="Print aggregate counts instead of every individual violation.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print machine-readable result output.",
    )
    args = parser.parse_args()

    if not args.paths:
        if args.json:
            payload, exit_code = _run_self_test_json()
            print(json.dumps(payload, indent=2))
            return exit_code
        return _run_self_test()

    if args.json:
        payload, exit_code = _build_json_payload(
            raw_paths=args.paths,
            normalized=args.normalized,
        )
        print(json.dumps(payload, indent=2))
        return exit_code

    expanded_paths = _expand_input_paths(args.paths)
    if not expanded_paths:
        print("contract_lint: no dashboard JSON files found", file=sys.stderr)
        return 1

    (
        find_dashboard_patch_contract_violations,
        normalize_dashboard_state_for_patch,
    ) = _load_contract_helpers()
    total_violations = 0
    results: list[tuple[Path, list[dict[str, str]]]] = []
    for path in expanded_paths:
        if not path.exists():
            print(f"{path}: file not found", file=sys.stderr)
            total_violations += 1
            continue
        state = _load_state(path)
        if args.normalized:
            state = normalize_dashboard_state_for_patch(
                state,
                strip_page_labels=True,
                strip_number_widget_patch_fields=True,
            )
        violations = find_dashboard_patch_contract_violations(state)
        results.append((path, violations))
        if violations:
            total_violations += len(violations)
            if not args.summary:
                _print_violations(path, violations)
        elif not args.summary:
            print(f"{path}: OK")

    if args.summary:
        _print_summary(results)

    return 1 if total_violations else 0


if __name__ == "__main__":
    raise SystemExit(main())
