"""Salesforce Reports/List View client for monthly source extraction."""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Any

import requests

from scripts.monthly_platform.salesforce_auth import SalesforceAuth


@dataclass(frozen=True)
class SalesforceSourceResult:
    source_type: str
    source_id: str
    source_label: str
    rows: list[dict[str, Any]]
    raw_payload: dict[str, Any]
    duration_ms: int
    status_code: int
    metadata: dict[str, Any]


class SalesforceSourceClient:
    def __init__(
        self,
        *,
        auth: SalesforceAuth,
        session: requests.Session,
        timeout_seconds: int = 120,
    ) -> None:
        self.auth = auth
        self.session = session
        self.timeout_seconds = timeout_seconds

    def run_report(
        self,
        *,
        report_id: str,
        source_label: str,
        include_details: bool = True,
    ) -> SalesforceSourceResult:
        started = time.monotonic()
        url = (
            f"{self.auth.instance_url}/services/data/{self.auth.api_version}"
            f"/analytics/reports/{report_id}"
        )
        response = self.session.get(
            url,
            params={"includeDetails": str(include_details).lower()},
            timeout=self.timeout_seconds,
        )
        payload = _json_response(response)
        response.raise_for_status()
        rows = normalize_report_rows(payload)
        return SalesforceSourceResult(
            source_type="salesforce_report",
            source_id=report_id,
            source_label=source_label,
            rows=rows,
            raw_payload=payload,
            duration_ms=int((time.monotonic() - started) * 1000),
            status_code=response.status_code,
            metadata=report_metadata_summary(payload),
        )

    def run_list_view(
        self,
        *,
        list_view_id: str,
        source_label: str,
        page_size: int = 200,
        max_records: int = 5000,
    ) -> SalesforceSourceResult:
        started = time.monotonic()
        url = (
            f"{self.auth.instance_url}/services/data/{self.auth.api_version}"
            f"/ui-api/list-records/{list_view_id}"
        )
        params: dict[str, Any] | None = {"pageSize": page_size}
        records: list[dict[str, Any]] = []
        pages: list[dict[str, Any]] = []
        status_code = 200
        while url and len(records) < max_records:
            response = self.session.get(
                url,
                params=params,
                timeout=min(self.timeout_seconds, 60),
            )
            payload = _json_response(response)
            response.raise_for_status()
            status_code = response.status_code
            page_records = payload.get("records") or []
            if isinstance(page_records, list):
                records.extend(page_records)
            pages.append(payload)
            next_url = payload.get("nextPageUrl")
            url = f"{self.auth.instance_url}{next_url}" if next_url else ""
            params = None
        rows = [normalize_list_view_record(record) for record in records[:max_records]]
        return SalesforceSourceResult(
            source_type="salesforce_list_view",
            source_id=list_view_id,
            source_label=source_label,
            rows=rows,
            raw_payload={"pages": pages},
            duration_ms=int((time.monotonic() - started) * 1000),
            status_code=status_code,
            metadata={
                "page_count": len(pages),
                "record_count": len(rows),
                "max_records": max_records,
            },
        )


def normalize_report_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    metadata = payload.get("reportMetadata") or {}
    detail_columns = [str(column) for column in (metadata.get("detailColumns") or [])]
    column_info = (payload.get("reportExtendedMetadata") or {}).get(
        "detailColumnInfo"
    ) or {}
    headers = [
        str((column_info.get(column) or {}).get("label") or column)
        for column in detail_columns
    ]
    rows: list[dict[str, Any]] = []
    for fact in (payload.get("factMap") or {}).values():
        for row in fact.get("rows") or []:
            cells = row.get("dataCells") or []
            if not isinstance(cells, list):
                continue
            row_payload: dict[str, Any] = {}
            for index, cell in enumerate(cells):
                header = headers[index] if index < len(headers) else f"column_{index + 1}"
                row_payload[header] = _cell_value(cell)
            if row_payload:
                rows.append(row_payload)
    return rows


def normalize_list_view_record(record: dict[str, Any]) -> dict[str, Any]:
    fields = record.get("fields") or {}
    row: dict[str, Any] = {
        "id": record.get("id"),
        "apiName": record.get("apiName"),
    }
    if isinstance(fields, dict):
        for field_name, field_payload in fields.items():
            if isinstance(field_payload, dict):
                row[str(field_name)] = field_payload.get("value")
                display_value = field_payload.get("displayValue")
                if display_value not in (None, row[str(field_name)]):
                    row[f"{field_name}__display"] = display_value
            else:
                row[str(field_name)] = field_payload
    return row


def report_metadata_summary(payload: dict[str, Any]) -> dict[str, Any]:
    metadata = payload.get("reportMetadata") or {}
    extended = payload.get("reportExtendedMetadata") or {}
    return {
        "name": metadata.get("name"),
        "report_type": (metadata.get("reportType") or {}).get("type"),
        "report_format": metadata.get("reportFormat"),
        "detail_columns": metadata.get("detailColumns") or [],
        "historical_snapshot_dates": metadata.get("historicalSnapshotDates") or [],
        "detail_column_info_keys": sorted(
            ((extended.get("detailColumnInfo") or {}).keys())
        ),
    }


def _cell_value(cell: Any) -> Any:
    if not isinstance(cell, dict):
        return cell
    value = cell.get("value")
    label = cell.get("label")
    if value not in (None, ""):
        if isinstance(value, dict | list):
            return label if not isinstance(label, dict | list) else str(label)
        return value
    if isinstance(label, dict | list):
        return str(label)
    return label


def _json_response(response: requests.Response) -> dict[str, Any]:
    payload = response.json()
    if not isinstance(payload, dict):
        raise ValueError("Salesforce response was not a JSON object")
    return payload
