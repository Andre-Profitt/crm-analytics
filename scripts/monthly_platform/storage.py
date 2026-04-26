"""Local storage substrate for monthly deck runs.

The storage layer is intentionally boring:
- raw extract JSON for replay/debugging
- Parquet tables for local analytics
- SQLite ledger for artifact/stage lookup
- optional DuckDB registration when `duckdb` is installed

No Salesforce, Excel, PowerPoint, or AI calls belong here.
"""

from __future__ import annotations

import hashlib
import json
import re
import sqlite3
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

import pandas as pd

from scripts.monthly_platform.contracts import (
    ArtifactRef,
    MonthlyRunManifest,
    SourceExtract,
    StageResult,
    utc_now_iso,
)


DEFAULT_STORAGE_ROOT = Path("output") / "monthly_platform_storage"


def stable_json_bytes(payload: Any) -> bytes:
    return json.dumps(
        payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def slugify(value: str) -> str:
    token = re.sub(r"[^a-zA-Z0-9]+", "-", str(value).strip().lower())
    return token.strip("-") or "unknown"


def parquet_safe_value(value: Any) -> Any:
    if value is None or isinstance(value, str | int | float | bool):
        return value
    if isinstance(value, dict | list | tuple):
        return json.dumps(value, ensure_ascii=True, sort_keys=True)
    return str(value)


def parquet_safe_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): parquet_safe_value(value) for key, value in row.items()}


def schema_fingerprint(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, str]], str]:
    fields: dict[str, set[str]] = {}
    for row in rows:
        for key, value in row.items():
            fields.setdefault(str(key), set()).add(type(value).__name__)
    schema = [
        {"name": name, "types": "|".join(sorted(types))}
        for name, types in sorted(fields.items())
    ]
    return schema, sha256_bytes(stable_json_bytes(schema))


class MonthlyStorage:
    def __init__(
        self,
        *,
        root: Path | str = DEFAULT_STORAGE_ROOT,
        snapshot_date: str,
        run_id: str | None = None,
    ) -> None:
        self.root = Path(root)
        self.snapshot_date = snapshot_date
        self.run_id = run_id or f"monthly-{snapshot_date}"
        self.run_dir = self.root / snapshot_date / self.run_id
        self.raw_dir = self.run_dir / "raw"
        self.table_dir = self.run_dir / "tables"
        self.ledger_path = self.run_dir / "ledger.sqlite"
        self.manifest_path = self.run_dir / "run_manifest.json"
        self.duckdb_path = self.run_dir / "warehouse.duckdb"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.table_dir.mkdir(parents=True, exist_ok=True)
        self._init_ledger()

    def create_run(
        self,
        *,
        period_context: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> MonthlyRunManifest:
        manifest = MonthlyRunManifest(
            run_id=self.run_id,
            snapshot_date=self.snapshot_date,
            status="ok",
            period_context=period_context or {},
            metadata=metadata or {},
        )
        self._upsert_run(manifest)
        self._write_manifest(manifest)
        return manifest

    def load_manifest(self) -> MonthlyRunManifest | None:
        if not self.manifest_path.exists():
            return None
        return MonthlyRunManifest.model_validate_json(
            self.manifest_path.read_text(encoding="utf-8")
        )

    def register_source_extract(
        self,
        *,
        source_type: str,
        source_id: str,
        source_label: str,
        rows: list[dict[str, Any]],
        raw_payload: Any | None = None,
        stage_name: str = "source_extract",
        territory: str | None = None,
        director: str | None = None,
        region: str | None = None,
        period_role: str | None = None,
        quarter_label: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> SourceExtract:
        rowset_hash = sha256_bytes(stable_json_bytes(rows))
        schema, schema_hash = schema_fingerprint(rows)
        extract_id = self._source_extract_id(
            source_type=source_type,
            source_id=source_id,
            territory=territory,
            director=director,
            period_role=period_role,
            quarter_label=quarter_label,
            rowset_hash=rowset_hash,
        )

        raw_artifact_payload = {
            "source_extract_id": extract_id,
            "snapshot_date": self.snapshot_date,
            "source_type": source_type,
            "source_id": source_id,
            "source_label": source_label,
            "territory": territory,
            "director": director,
            "region": region,
            "period_role": period_role,
            "quarter_label": quarter_label,
            "schema": schema,
            "row_count": len(rows),
            "raw_payload": raw_payload,
            "rows": rows,
            "metadata": metadata or {},
        }
        raw_path = self.raw_dir / f"{extract_id}.json"
        raw_path.write_bytes(stable_json_bytes(raw_artifact_payload))
        raw_artifact = self._artifact_ref(
            artifact_id=f"{extract_id}.raw",
            artifact_type="raw_source_extract",
            path=raw_path,
            format="json",
            row_count=len(rows),
            schema_sha256=schema_hash,
            metadata={
                "source_extract_id": extract_id,
                "source_type": source_type,
                "source_id": source_id,
            },
        )

        parquet_path = self.table_dir / f"{extract_id}.parquet"
        pd.DataFrame([parquet_safe_row(row) for row in rows]).to_parquet(
            parquet_path,
            index=False,
        )
        table_artifact = self._artifact_ref(
            artifact_id=f"{extract_id}.table",
            artifact_type="normalized_source_table",
            path=parquet_path,
            format="parquet",
            row_count=len(rows),
            schema_sha256=schema_hash,
            metadata={
                "source_extract_id": extract_id,
                "source_type": source_type,
                "source_id": source_id,
                "duckdb_relation": self._register_duckdb_table(
                    extract_id, parquet_path
                ),
            },
        )

        extract = SourceExtract(
            source_extract_id=extract_id,
            snapshot_date=self.snapshot_date,
            source_type=source_type,
            source_id=source_id,
            source_label=source_label,
            territory=territory,
            director=director,
            region=region,
            period_role=period_role,
            quarter_label=quarter_label,
            status="ok",
            row_count=len(rows),
            raw_artifact=raw_artifact,
            normalized_artifact=table_artifact,
            schema_sha256=schema_hash,
            rowset_sha256=rowset_hash,
            metadata=metadata or {},
        )

        self._insert_artifact(
            raw_artifact, stage_name=stage_name, source_extract_id=extract_id
        )
        self._insert_artifact(
            table_artifact, stage_name=stage_name, source_extract_id=extract_id
        )
        self._insert_source_extract(extract, stage_name=stage_name)
        return extract

    def register_json_artifact(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        payload: Any,
        relative_path: str,
        stage_name: str,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRef:
        path = self.run_dir / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(stable_json_bytes(payload))
        artifact = self._artifact_ref(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            path=path,
            format="json",
            row_count=len(payload) if isinstance(payload, list) else None,
            schema_sha256=None,
            metadata=metadata or {},
        )
        self._insert_artifact(artifact, stage_name=stage_name)
        return artifact

    def record_stage_result(self, stage: StageResult) -> MonthlyRunManifest:
        manifest = self.load_manifest() or self.create_run()
        manifest = manifest.with_stage(stage)
        self._insert_stage(stage)
        for artifact in stage.outputs:
            self._insert_artifact(artifact, stage_name=stage.stage_name)
        for extract in stage.source_extracts:
            self._insert_source_extract(extract, stage_name=stage.stage_name)
        self._upsert_run(manifest)
        self._write_manifest(manifest)
        return manifest

    def get_artifact(self, artifact_id: str) -> ArtifactRef | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM artifacts WHERE artifact_id = ?",
                (artifact_id,),
            ).fetchone()
        if row is None:
            return None
        return ArtifactRef.model_validate_json(row[0])

    def get_source_extract(self, source_extract_id: str) -> SourceExtract | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT payload_json FROM source_extracts WHERE source_extract_id = ?",
                (source_extract_id,),
            ).fetchone()
        if row is None:
            return None
        return SourceExtract.model_validate_json(row[0])

    def artifacts_for_stage(self, stage_name: str) -> list[ArtifactRef]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT payload_json FROM artifacts WHERE stage_name = ? ORDER BY artifact_id",
                (stage_name,),
            ).fetchall()
        return [ArtifactRef.model_validate_json(row[0]) for row in rows]

    def _source_extract_id(
        self,
        *,
        source_type: str,
        source_id: str,
        territory: str | None,
        director: str | None,
        period_role: str | None,
        quarter_label: str | None,
        rowset_hash: str,
    ) -> str:
        parts = [
            "src",
            self.snapshot_date,
            territory or director or "global",
            period_role or "period",
            quarter_label or "q",
            source_type,
            source_id,
            rowset_hash[:12],
        ]
        return slugify("-".join(parts))

    def _artifact_ref(
        self,
        *,
        artifact_id: str,
        artifact_type: str,
        path: Path,
        format: str,
        row_count: int | None,
        schema_sha256: str | None,
        metadata: dict[str, Any] | None = None,
    ) -> ArtifactRef:
        return ArtifactRef(
            artifact_id=artifact_id,
            artifact_type=artifact_type,
            path=str(path),
            format=format,
            byte_count=path.stat().st_size if path.exists() else 0,
            row_count=row_count,
            sha256=sha256_bytes(path.read_bytes()) if path.exists() else "",
            schema_sha256=schema_sha256,
            metadata=metadata or {},
        )

    def _register_duckdb_table(self, extract_id: str, parquet_path: Path) -> str:
        relation = slugify(extract_id).replace("-", "_")
        try:
            import duckdb  # type: ignore
        except ModuleNotFoundError:
            return "duckdb_not_installed"

        # DuckDB >= 1.5 disallows prepared parameters in DDL like
        # CREATE VIEW; inline the path with single-quote escaping. The
        # ``parquet_path`` is constructed by ``MonthlyStorage`` from a slugged
        # extract_id, so it carries no caller-controlled SQL.
        escaped_path = str(parquet_path).replace("'", "''")
        with duckdb.connect(str(self.duckdb_path)) as conn:
            conn.execute(
                f'CREATE OR REPLACE VIEW "{relation}" AS '
                f"SELECT * FROM read_parquet('{escaped_path}')"
            )
        return relation

    def _init_ledger(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    snapshot_date TEXT NOT NULL,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS artifacts (
                    artifact_id TEXT PRIMARY KEY,
                    stage_name TEXT,
                    source_extract_id TEXT,
                    artifact_type TEXT NOT NULL,
                    path TEXT NOT NULL,
                    format TEXT NOT NULL,
                    row_count INTEGER,
                    sha256 TEXT NOT NULL,
                    schema_sha256 TEXT,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS source_extracts (
                    source_extract_id TEXT PRIMARY KEY,
                    stage_name TEXT,
                    source_type TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    source_label TEXT NOT NULL,
                    territory TEXT,
                    director TEXT,
                    period_role TEXT,
                    quarter_label TEXT,
                    row_count INTEGER NOT NULL,
                    schema_sha256 TEXT NOT NULL,
                    rowset_sha256 TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stages (
                    stage_name TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    finished_at TEXT NOT NULL
                );
                """
            )

    def _connect(self) -> sqlite3.Connection:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(self.ledger_path)

    def _write_manifest(self, manifest: MonthlyRunManifest) -> None:
        self.manifest_path.write_text(
            manifest.model_dump_json(indent=2) + "\n", encoding="utf-8"
        )

    def _upsert_run(self, manifest: MonthlyRunManifest) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs (run_id, snapshot_date, status, payload_json, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    snapshot_date = excluded.snapshot_date,
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    updated_at = excluded.updated_at
                """,
                (
                    manifest.run_id,
                    manifest.snapshot_date,
                    manifest.status,
                    manifest.model_dump_json(),
                    manifest.updated_at,
                ),
            )

    def _insert_artifact(
        self,
        artifact: ArtifactRef,
        *,
        stage_name: str | None = None,
        source_extract_id: str | None = None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO artifacts (
                    artifact_id, stage_name, source_extract_id, artifact_type, path,
                    format, row_count, sha256, schema_sha256, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(artifact_id) DO UPDATE SET
                    stage_name = excluded.stage_name,
                    source_extract_id = excluded.source_extract_id,
                    artifact_type = excluded.artifact_type,
                    path = excluded.path,
                    format = excluded.format,
                    row_count = excluded.row_count,
                    sha256 = excluded.sha256,
                    schema_sha256 = excluded.schema_sha256,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (
                    artifact.artifact_id,
                    stage_name,
                    source_extract_id,
                    artifact.artifact_type,
                    artifact.path,
                    artifact.format,
                    artifact.row_count,
                    artifact.sha256,
                    artifact.schema_sha256,
                    artifact.model_dump_json(),
                    artifact.created_at,
                ),
            )

    def _insert_source_extract(
        self,
        extract: SourceExtract,
        *,
        stage_name: str,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO source_extracts (
                    source_extract_id, stage_name, source_type, source_id, source_label,
                    territory, director, period_role, quarter_label, row_count,
                    schema_sha256, rowset_sha256, payload_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_extract_id) DO UPDATE SET
                    stage_name = excluded.stage_name,
                    source_type = excluded.source_type,
                    source_id = excluded.source_id,
                    source_label = excluded.source_label,
                    territory = excluded.territory,
                    director = excluded.director,
                    period_role = excluded.period_role,
                    quarter_label = excluded.quarter_label,
                    row_count = excluded.row_count,
                    schema_sha256 = excluded.schema_sha256,
                    rowset_sha256 = excluded.rowset_sha256,
                    payload_json = excluded.payload_json,
                    created_at = excluded.created_at
                """,
                (
                    extract.source_extract_id,
                    stage_name,
                    extract.source_type,
                    extract.source_id,
                    extract.source_label,
                    extract.territory,
                    extract.director,
                    extract.period_role,
                    extract.quarter_label,
                    extract.row_count,
                    extract.schema_sha256,
                    extract.rowset_sha256,
                    extract.model_dump_json(),
                    utc_now_iso(),
                ),
            )

    def _insert_stage(self, stage: StageResult) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO stages (stage_name, status, payload_json, finished_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(stage_name) DO UPDATE SET
                    status = excluded.status,
                    payload_json = excluded.payload_json,
                    finished_at = excluded.finished_at
                """,
                (
                    stage.stage_name,
                    stage.status,
                    stage.model_dump_json(),
                    stage.finished_at,
                ),
            )
