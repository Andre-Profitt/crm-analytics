"""Per-run audit trail for CRM Analytics builders.

Every builder writes one JSON file per run to runs/<Dataset>/<ts>.json
capturing what ran, how long, what got uploaded, and any errors.

Part of Builder Modernization 1A — see
docs/superpowers/specs/2026-04-06-builder-modernization-1a-plumbing-design.md
"""

from __future__ import annotations

import hashlib
import json
import logging
import socket
import time
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

logger = logging.getLogger(__name__)

RUNS_ROOT = Path(__file__).parent / "runs"


@dataclass
class RunSummary:
    dataset_name: str
    builder_path: str
    started_at: str  # ISO 8601 UTC with Z suffix
    summary_schema_version: int = 1
    external_id: str = ""  # populated in __post_init__
    finished_at: str | None = None
    runtime_s: float | None = None
    row_count: int | None = None
    byte_count: int | None = None
    dataset_id: str | None = None
    dataset_version_id: str | None = None
    status: str = "running"  # "running" | "ok" | "failed"
    errors: list[str] = field(default_factory=list)
    host: str = field(default_factory=socket.gethostname)

    def __post_init__(self) -> None:
        if not self.external_id:
            key = f"{self.dataset_name}|{self.started_at}".encode()
            self.external_id = hashlib.sha256(key).hexdigest()[:18]

    def to_json_path(self) -> Path:
        ts = self.started_at.replace(":", "").replace("-", "")
        return RUNS_ROOT / self.dataset_name / f"{ts}.json"

    def write(self) -> Path:
        path = self.to_json_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(asdict(self), indent=2, sort_keys=True))
        return path


@contextmanager
def builder_run(dataset_name: str, builder_path: str) -> Iterator[RunSummary]:
    """Wrap a builder's main() body so the RunSummary is written on
    both success and failure paths.

    Contracts (from spec Error Handling section):
    1. RunSummary write failures must never suppress the body exception.
    2. If the body raised AND the write failed, the body exception is
       the one that propagates.
    3. If the body succeeded but the write failed, the write failure is
       logged at ERROR and the process still exits 0 — a local disk
       issue must not convert a successful data upload into a failure.
    """
    started = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    summary = RunSummary(
        dataset_name=dataset_name,
        builder_path=builder_path,
        started_at=started,
    )
    t0 = time.monotonic()
    body_exc: BaseException | None = None
    try:
        yield summary
        summary.status = "ok"
    except BaseException as exc:
        body_exc = exc
        summary.status = "failed"
        summary.errors.append(f"{type(exc).__name__}: {exc}")
        summary.errors.append(traceback.format_exc())
    finally:
        summary.finished_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        summary.runtime_s = round(time.monotonic() - t0, 2)
        try:
            path = summary.write()
            logger.info(
                "RunSummary written: %s status=%s runtime=%.1fs rows=%s",
                path,
                summary.status,
                summary.runtime_s,
                summary.row_count,
            )
        except OSError as write_exc:
            logger.error("RunSummary write failed: %s", write_exc)
            # Intentionally swallow: must not mask the body exception,
            # and a write failure on a successful body should not turn
            # a successful run into a failure.
        if body_exc is not None:
            raise body_exc
