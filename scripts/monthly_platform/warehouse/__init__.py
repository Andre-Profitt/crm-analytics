"""Track H — DuckDB / Parquet warehouse skeleton.

The warehouse materializes what the monthly extract pipeline already knows:
extraction plan, quality audit, contract registry, and findings (Track B/C/D)
into a deterministic Parquet layout that downstream tools (and Track I/E/F
work) can read without re-parsing JSON evidence.

Concrete-first: the warehouse is read-only of the existing pipeline outputs
and writes only to ``output/source_backed_warehouse/<snapshot>/<run_id>/``.
It does NOT alter extraction, deck generation, or release logic.

Public surface (re-exports):

* :class:`WarehousePaths` — filesystem layout helper.
* :func:`build_warehouse` — orchestrator (build all marts, write manifest).
* :func:`compute_parity` — input-vs-warehouse row-count parity report.
"""

from __future__ import annotations

from scripts.monthly_platform.warehouse.parity import (
    compute_parity,
)
from scripts.monthly_platform.warehouse.paths import (
    WarehousePaths,
)
from scripts.monthly_platform.warehouse.writer import (
    WarehouseManifest,
    WarehouseTable,
    build_warehouse,
)

__all__ = [
    "WarehouseManifest",
    "WarehousePaths",
    "WarehouseTable",
    "build_warehouse",
    "compute_parity",
]
