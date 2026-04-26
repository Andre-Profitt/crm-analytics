"""Track H — deterministic Parquet writer + warehouse manifest.

The writer takes the per-table row lists from :mod:`marts` and persists them
under :class:`WarehousePaths`. Output is deterministic: column order is fixed
by ``TABLE_BUILDERS``, row order is set by each builder, and each table's
sha256 is recorded alongside row count + byte count in
``warehouse_manifest.json``.

DuckDB is used to write Parquet because (a) it is already a project
dependency for the v3 work and (b) ``COPY ... TO ... (FORMAT 'parquet')``
emits a stable column order without pandas index churn. The manifest
records both row counts and content hashes so downstream parity work can
prove the warehouse is byte-exact for a given input.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import duckdb

from scripts.monthly_platform.contracts import utc_now_iso
from scripts.monthly_platform.warehouse.marts import TABLE_BUILDERS
from scripts.monthly_platform.warehouse.paths import (
    MANIFEST_FILENAME,
    WarehousePaths,
)


SCHEMA_VERSION = "monthly_platform.source_backed_warehouse_manifest.v1"


@dataclass(frozen=True)
class WarehouseTable:
    table_id: str
    relative_path: str
    columns: list[str]
    row_count: int
    byte_count: int
    sha256: str


@dataclass
class WarehouseManifest:
    schema_version: str = SCHEMA_VERSION
    generated_at: str = field(default_factory=utc_now_iso)
    snapshot_date: str = ""
    run_id: str = ""
    warehouse_root: str = ""
    tables: list[WarehouseTable] = field(default_factory=list)

    def model_dump(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at,
            "snapshot_date": self.snapshot_date,
            "run_id": self.run_id,
            "warehouse_root": self.warehouse_root,
            "tables": [
                {
                    "table_id": t.table_id,
                    "relative_path": t.relative_path,
                    "columns": list(t.columns),
                    "row_count": t.row_count,
                    "byte_count": t.byte_count,
                    "sha256": t.sha256,
                }
                for t in self.tables
            ],
        }


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def _write_parquet(
    *,
    rows: list[dict[str, Any]],
    columns: list[str],
    path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Always materialize even an empty table so downstream readers don't have
    # to special-case missing files. DuckDB will infer column types from the
    # provided rows; for an empty input we register a typed VARCHAR table so
    # COPY succeeds with the documented column order.
    con = duckdb.connect()
    try:
        if rows:
            # Normalize each row to dict aligned to ``columns`` so DuckDB sees a
            # uniform schema, then register as a polars-free relation.
            normalized = [{col: row.get(col) for col in columns} for row in rows]
            relation = con.from_arrow(_records_to_arrow(normalized, columns))
            relation.to_parquet(str(path))
        else:
            # Build a typed-but-empty table by selecting CAST(NULL ...) and
            # filtering it out. This works on DuckDB without literal-list
            # parsing surprises.
            select_cols = ", ".join(
                f'CAST(NULL AS VARCHAR) AS "{col}"' for col in columns
            )
            con.execute(
                f"CREATE TEMP TABLE __empty AS SELECT {select_cols} WHERE FALSE;"
            )
            con.execute(f"COPY __empty TO '{path}' (FORMAT 'parquet');")
    finally:
        con.close()


def _records_to_arrow(records: list[dict[str, Any]], columns: list[str]):
    """Convert ``records`` to a pyarrow Table with the documented column order.

    Lazy-imports pyarrow so the import error surfaces at call time rather than
    at module import (the project's other warehouse callers may stub this).
    """
    import pyarrow as pa

    cols = {col: [r.get(col) for r in records] for col in columns}
    return pa.table(cols)


def write_table(
    *,
    rows: list[dict[str, Any]],
    columns: list[str],
    path: Path,
) -> WarehouseTable:
    """Write one table to Parquet and return its manifest entry."""
    _write_parquet(rows=rows, columns=columns, path=path)
    return WarehouseTable(
        table_id="",  # caller sets this
        relative_path="",
        columns=list(columns),
        row_count=len(rows),
        byte_count=path.stat().st_size,
        sha256=_sha256(path),
    )


def build_warehouse(
    *,
    paths: WarehousePaths,
    plan: dict[str, Any],
    audit: dict[str, Any],
    registry: dict[str, Any],
) -> WarehouseManifest:
    """Materialize every warehouse table for one (snapshot, run) and write the manifest.

    ``plan``, ``audit``, ``registry`` are the JSON evidence the upstream
    pipeline already wrote (``source_requirement_plan.json``,
    ``source_extract_quality_audit.json``, ``monthly_source_requirements.json``).
    The warehouse never re-runs extraction — it materializes what the
    pipeline already knows into Parquet so downstream tools (and Track I/E/F)
    can read without re-parsing JSON.
    """
    paths.root.mkdir(parents=True, exist_ok=True)
    manifest = WarehouseManifest(
        snapshot_date=paths.snapshot_date,
        run_id=paths.run_id,
        warehouse_root=str(paths.root),
    )
    for table_id, (builder, columns) in TABLE_BUILDERS.items():
        if table_id == "raw_salesforce_extract_plan":
            rows = builder(plan=plan, run_id=paths.run_id)
        elif table_id == "staged_source_requirements":
            rows = builder(registry)
        else:
            rows = builder(audit)
        path = paths.table_path(table_id)
        entry = write_table(rows=rows, columns=columns, path=path)
        manifest.tables.append(
            WarehouseTable(
                table_id=table_id,
                relative_path=str(path.relative_to(paths.root)),
                columns=entry.columns,
                row_count=entry.row_count,
                byte_count=entry.byte_count,
                sha256=entry.sha256,
            )
        )
    paths.manifest_path.write_text(
        json.dumps(manifest.model_dump(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def load_manifest(paths: WarehousePaths) -> dict[str, Any]:
    return json.loads((paths.root / MANIFEST_FILENAME).read_text(encoding="utf-8"))
