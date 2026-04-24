#!/usr/bin/env python3
"""Compile/check YAML-authored monthly source-contract config into runtime JSON."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_AUTHORING_PATH = ROOT / "config" / "source_contracts" / "sales_director_monthly.yaml"
SCHEMA_VERSION = "monthly_platform.source_contract_authoring_compile.v1"


def load_yaml_payload(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - dependency failure path
        raise RuntimeError(
            "PyYAML is required for source-contract YAML authoring. "
            "Install with `python3 -m pip install PyYAML`."
        ) from exc
    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"authoring YAML must be an object: {path}")
    return payload


def compile_monthly_source_contract_config(
    *,
    authoring_path: Path = DEFAULT_AUTHORING_PATH,
    output_root: Path = ROOT,
    check: bool = True,
    write: bool = False,
) -> dict[str, Any]:
    authoring = load_yaml_payload(authoring_path)
    compiled_targets = authoring.get("compiled_targets")
    if not isinstance(compiled_targets, dict) or not compiled_targets:
        raise ValueError("authoring YAML must define non-empty compiled_targets")

    findings: list[dict[str, str]] = []
    targets: list[dict[str, Any]] = []
    for key, target in compiled_targets.items():
        if not isinstance(target, dict):
            findings.append(
                _finding(
                    issue="target_not_object",
                    evidence=str(key),
                )
            )
            continue
        target_path_value = target.get("path")
        target_payload = target.get("payload")
        if not isinstance(target_path_value, str) or not target_path_value:
            findings.append(_finding(issue="target_missing_path", evidence=str(key)))
            continue
        if target_payload is None:
            findings.append(_finding(issue="target_missing_payload", evidence=str(key)))
            continue

        target_path = _resolve_target_path(output_root, target_path_value)
        existing_payload = _load_json_payload(target_path)
        exists = existing_payload is not None
        drifted = existing_payload != target_payload

        if write:
            target_path.parent.mkdir(parents=True, exist_ok=True)
            target_path.write_text(_stable_json(target_payload), encoding="utf-8")
            existing_payload = target_payload
            exists = True
            drifted = False

        if check and not exists:
            findings.append(
                _finding(
                    issue="compiled_json_missing",
                    evidence=str(target_path),
                )
            )
        elif check and drifted:
            findings.append(
                _finding(
                    issue="compiled_json_drift",
                    evidence=str(target_path),
                )
            )

        targets.append(
            {
                "key": str(key),
                "path": str(target_path),
                "exists": exists,
                "drifted": drifted,
                "payload_sha256": _payload_sha256(target_payload),
                "existing_sha256": _payload_sha256(existing_payload)
                if existing_payload is not None
                else None,
            }
        )

    high_findings = [finding for finding in findings if finding["severity"] == "high"]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "blocked" if high_findings else "ok",
        "authoring_schema_version": authoring.get("schema_version"),
        "authoring_path": str(authoring_path),
        "output_root": str(output_root),
        "check": check,
        "write": write,
        "target_count": len(targets),
        "drift_count": sum(1 for target in targets if target["drifted"]),
        "missing_count": sum(1 for target in targets if not target["exists"]),
        "finding_count": len(findings),
        "high_finding_count": len(high_findings),
        "targets": targets,
        "findings": findings,
    }


def _resolve_target_path(output_root: Path, target_path_value: str) -> Path:
    target_path = Path(target_path_value)
    if target_path.is_absolute():
        return target_path
    return output_root / target_path


def _load_json_payload(path: Path) -> Any | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return None
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid compiled JSON target: {path}") from exc


def _stable_json(payload: Any) -> str:
    return json.dumps(payload, indent=2, ensure_ascii=False) + "\n"


def _payload_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _finding(
    *,
    issue: str,
    evidence: str,
    severity: str = "high",
) -> dict[str, str]:
    return {
        "severity": severity,
        "issue": issue,
        "evidence": evidence,
    }


def _write_output(path: Path | None, payload: dict[str, Any]) -> None:
    if not path:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authoring", type=Path, default=DEFAULT_AUTHORING_PATH)
    parser.add_argument("--output-root", type=Path, default=ROOT)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--output-path", type=Path)
    parser.add_argument("--json", action="store_true", dest="json_output")
    args = parser.parse_args(argv)

    check = args.check or not args.write
    payload = compile_monthly_source_contract_config(
        authoring_path=args.authoring,
        output_root=args.output_root,
        check=check,
        write=args.write,
    )
    _write_output(args.output_path, payload)
    if args.json_output:
        print(json.dumps(payload, indent=2, ensure_ascii=False))
    else:
        print(
            f"{payload['status']}: {payload['target_count']} targets, "
            f"{payload['drift_count']} drifted, {payload['missing_count']} missing"
        )
    return 0 if payload["status"] == "ok" else 2


if __name__ == "__main__":
    raise SystemExit(main())
