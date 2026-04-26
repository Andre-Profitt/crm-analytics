#!/usr/bin/env python3
"""Shared IO helpers for native Salesforce dashboard/report executors."""

from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any
from urllib import error as urllib_error
from urllib import request as urllib_request


_ORG_SESSION_CACHE: dict[str, dict[str, str]] = {}


def load_json_file(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path}: expected JSON object")
    return payload


def format_sf_error(stdout: str, stderr: str, *, path: str) -> str:
    if stdout.strip():
        try:
            payload = json.loads(stdout)
            if isinstance(payload, list):
                messages = []
                for item in payload:
                    if isinstance(item, dict):
                        message = item.get("message")
                        error_code = item.get("errorCode")
                        if message and error_code:
                            messages.append(f"{error_code}: {message}")
                        elif message:
                            messages.append(str(message))
                if messages:
                    return " | ".join(messages)
            elif isinstance(payload, dict):
                message = payload.get("message")
                error_code = payload.get("errorCode")
                if message and error_code:
                    return f"{error_code}: {message}"
                if message:
                    return str(message)
        except json.JSONDecodeError:
            return stdout.strip()
    if stderr.strip():
        return stderr.strip()
    return f"sf api request failed for {path}"


def get_org_session(target_org: str, *, root: Path) -> dict[str, str] | None:
    cached = _ORG_SESSION_CACHE.get(target_org)
    if cached:
        return cached
    result = subprocess.run(
        ["sf", "org", "display", "--target-org", target_org, "--verbose", "--json"],
        cwd=root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        return None
    try:
        payload = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    result_payload = payload.get("result")
    if not isinstance(result_payload, dict):
        return None
    access_token = result_payload.get("accessToken")
    instance_url = result_payload.get("instanceUrl")
    if not isinstance(access_token, str) or not access_token:
        return None
    if not isinstance(instance_url, str) or not instance_url:
        return None
    session = {"access_token": access_token, "instance_url": instance_url.rstrip("/")}
    _ORG_SESSION_CACHE[target_org] = session
    return session


def run_direct_rest_request(
    path: str,
    *,
    org_session: dict[str, str],
    method: str = "GET",
    body: Any | None = None,
) -> Any:
    url = f"{org_session['instance_url']}{path}"
    request_body: bytes | None = None
    headers = {
        "Authorization": f"Bearer {org_session['access_token']}",
        "Accept": "application/json",
    }
    if body is not None:
        request_body = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib_request.Request(url, data=request_body, headers=headers, method=method)
    try:
        with urllib_request.urlopen(request) as response:
            response_body = response.read().decode("utf-8")
    except urllib_error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(format_sf_error(error_body, "", path=path)) from exc
    except urllib_error.URLError:
        raise
    if not response_body.strip():
        return {}
    return json.loads(response_body)


def run_rest_request(
    path: str,
    *,
    root: Path,
    target_org: str | None,
    method: str = "GET",
    body: Any | None = None,
    expect_dict: bool = True,
) -> Any:
    if target_org:
        org_session = get_org_session(target_org, root=root)
        if org_session:
            payload = run_direct_rest_request(
                path,
                org_session=org_session,
                method=method,
                body=body,
            )
            if expect_dict and not isinstance(payload, dict):
                raise RuntimeError(f"unexpected non-object payload returned for {path}")
            return payload

    command = ["sf", "api", "request", "rest", path]
    if target_org:
        command.extend(["--target-org", target_org])
    if method != "GET":
        command.extend(["--method", method])
    if body is not None:
        command.extend(["--body", json.dumps(body)])

    result = subprocess.run(
        command,
        cwd=root,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        message = format_sf_error(result.stdout, result.stderr, path=path)
        raise RuntimeError(message)
    if not result.stdout.strip():
        return {}
    payload = json.loads(result.stdout)
    if expect_dict and not isinstance(payload, dict):
        raise RuntimeError(f"unexpected non-object payload returned for {path}")
    return payload


def fetch_rest_json(path: str, *, root: Path, target_org: str | None) -> dict[str, Any]:
    payload = run_rest_request(path, root=root, target_org=target_org, method="GET", expect_dict=True)
    if not isinstance(payload, dict):
        raise RuntimeError(f"unexpected non-object payload returned for {path}")
    return payload
