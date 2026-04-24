"""Run log and JSON summary writers for the CEX gap-fill stage."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


def make_run_id() -> str:
    """Return a sortable run identifier in ``YYYYMMDD_HHMMSS`` format."""
    return datetime.now(UTC).strftime("%Y%m%d_%H%M%S")


@dataclass
class RunSummary:
    """Accumulated statistics for one ``fill_gaps_from_cex`` invocation.

    :param run_id: Identifier returned by :func:`make_run_id`.
    :param started_at: ISO-8601 start timestamp.
    :param finished_at: ISO-8601 finish timestamp; filled in at the end.
    """

    run_id: str
    started_at: str
    finished_at: str = ""
    symbols_processed: int = 0
    symbols_skipped_no_cex: list[str] = field(default_factory=list)
    totals: dict[str, int] = field(
        default_factory=lambda: {"full_replaced": 0, "volume_replaced": 0, "kept": 0}
    )
    errors: list[str] = field(default_factory=list)


def write_summary_json(summary: RunSummary, path: Path) -> None:
    """Write a :class:`RunSummary` as a JSON file.

    :param summary: Summary to serialise.
    :param path: Destination file path. Parent dirs are created if needed.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary.__dict__, indent=2, default=str) + "\n")


def configure_run_logger(log_path: Path, level: str = "INFO") -> logging.Logger:
    """Configure the ``cex_gap_fill`` logger tree to write to ``log_path``.

    Idempotent — removes stale handlers from previous runs before adding new one.

    :param log_path: Destination log file. Parent dirs are created if needed.
    :param level: Python logging level name, e.g. ``"INFO"``, ``"DEBUG"``.
    :returns: Configured logger instance.
    """
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("gmx_historical_data.cex_gap_fill")
    logger.setLevel(level)
    for h in list(logger.handlers):
        if getattr(h, "_cex_gap_fill", False):
            logger.removeHandler(h)
    fh = logging.FileHandler(log_path, mode="w")
    fh.setFormatter(
        logging.Formatter(
            "[%(asctime)sZ] %(name)s %(levelname)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )
    )
    fh._cex_gap_fill = True  # type: ignore[attr-defined]
    logger.addHandler(fh)
    return logger
