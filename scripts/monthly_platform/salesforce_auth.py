"""Shared Salesforce auth/session helpers for monthly deck extraction."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass

import requests


DEFAULT_TARGET_ORG = "apro@simcorp.com"
DEFAULT_API_VERSION = "v66.0"


@dataclass(frozen=True)
class SalesforceAuth:
    access_token: str
    instance_url: str
    target_org: str
    api_version: str = DEFAULT_API_VERSION


def get_salesforce_auth(
    *,
    target_org: str = DEFAULT_TARGET_ORG,
    api_version: str = DEFAULT_API_VERSION,
) -> SalesforceAuth:
    try:
        result = subprocess.run(
            ["sf", "org", "display", "--target-org", target_org, "--json"],
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip() or "(no stderr)"
        raise RuntimeError(f"Salesforce auth failed for {target_org}: {stderr}") from exc
    payload = json.loads(result.stdout)
    data = payload.get("result") or {}
    access_token = str(data.get("accessToken") or "")
    instance_url = str(data.get("instanceUrl") or "")
    if not access_token or not instance_url:
        raise RuntimeError(f"Salesforce auth missing token/instance for {target_org}")
    return SalesforceAuth(
        access_token=access_token,
        instance_url=instance_url.rstrip("/"),
        target_org=target_org,
        api_version=api_version,
    )


def build_salesforce_session(auth: SalesforceAuth) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "Authorization": f"Bearer {auth.access_token}",
            "Accept": "application/json",
        }
    )
    return session
