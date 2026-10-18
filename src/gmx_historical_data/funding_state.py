"""The raw funding state the daily release carries, and how it is seeded.

``extract_unified_funding.py --resume`` is incremental only when it finds its
checkpoints; with none it walks from ``GMX_V2_GENESIS_BLOCK``, which takes over
an hour -- more than the whole release job is allowed. The state it needs
(raw events, rebuilt rates, direction and the two checkpoints) therefore has to
travel from release to release, in its own ``gmx-funding-state.tar.gz`` asset
so it does not eat ``gmx-full``'s 2 GiB asset budget.

The one-off seed for that state is the ``data/full-history`` branch, which
holds ``raw/``, ``rates/`` and ``direction/`` but **no checkpoints** (they were
never persisted before #75). :func:`seed_funding_state` copies that tree into
place and derives the checkpoints from the raw events themselves.

Layout under ``{gmx_root}/funding/arbitrum`` (the ``--output-dir`` the daily
extraction uses)::

    raw/funding/{SYM}/partition=N/data.parquet
    raw/fee_per_size/{SYM}/data.parquet
    rates/{SYM}/1h.parquet            canonical hourly rates
    direction/{SYM}/1h.parquet
    checkpoints/funding_factor_checkpoint.json
    checkpoints/fee_per_size_checkpoint.json
    SEED_MANIFEST.json                 only when seeded
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any

import polars as pl

NETWORK = "arbitrum"
FACTOR_CHECKPOINT = "funding_factor_checkpoint.json"
FEE_CHECKPOINT = "fee_per_size_checkpoint.json"
SEED_MANIFEST = "SEED_MANIFEST.json"

#: Blocks re-scanned after a seed (~7 h on Arbitrum). The raw stores dedupe on
#: ``(block_number, log_index)``, so overlap is harmless and covers any events
#: the branch's last run saw only partially.
SEED_OVERLAP_BLOCKS = 100_000

#: ``(manifest key, raw sub-directory, checkpoint file, checkpoint ``symbol``)``.
#: ``symbol`` and the key set below mirror ``save_checkpoint`` in
#: ``extract_funding_factor.py`` / ``extract_funding_fee_per_size.py``; a
#: contract test pins them against those scripts.
_SOURCES: tuple[tuple[str, str, str, str], ...] = (
    ("funding_factor", "funding", FACTOR_CHECKPOINT, "funding_factor_all"),
    ("fee_per_size", "fee_per_size", FEE_CHECKPOINT, "fee_per_size_all"),
)


class FundingState(Enum):
    """Whether the daily extraction can run incrementally.

    :cvar READY: Raw events and both checkpoints are present.
    :cvar ABSENT: No raw events at all (never seeded, or the restore failed).
    :cvar INCOMPLETE: Raw events without both checkpoints; ``--resume`` would
        restart from genesis, so the extraction must not run.
    """

    READY = "ready"
    ABSENT = "absent"
    INCOMPLETE = "incomplete"


class SeedError(Exception):
    """The seed source is unusable, or the destination must not be replaced."""


@dataclass(frozen=True, slots=True)
class SeedResult:
    """Outcome of :func:`seed_funding_state`.

    :param status: ``"seeded"`` or ``"skipped_existing"``.
    :param manifest: The written ``SEED_MANIFEST.json`` content, if seeded.
    """

    status: str
    manifest: dict[str, Any] | None = None


def funding_network_dir(gmx_root: Path) -> Path:
    """Return ``{gmx_root}/funding/arbitrum``."""
    return Path(gmx_root) / "funding" / NETWORK


def _parquet_files(directory: Path) -> list[Path]:
    if not directory.is_dir():
        return []
    return sorted(p for p in directory.rglob("*.parquet") if not p.name.startswith("."))


def assess_funding_state(gmx_root: Path) -> FundingState:
    """Classify the restored funding state.

    :param gmx_root: The ``user_data/data/gmx`` directory.
    :returns: :class:`FundingState`.
    """
    net = funding_network_dir(gmx_root)
    if not any(_parquet_files(net / "raw" / sub) for _, sub, _, _ in _SOURCES):
        return FundingState.ABSENT
    for _, _, name, _ in _SOURCES:
        if _read_checkpoint(net / "checkpoints" / name) is None:
            return FundingState.INCOMPLETE
    return FundingState.READY


def _read_checkpoint(path: Path) -> dict[str, Any] | None:
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    if not isinstance(data, dict) or not isinstance(data.get("last_block"), int):
        return None
    return data


def _raw_stats(files: list[Path], what: str) -> dict[str, int]:
    """Row count, block/time extents and market count of a raw event store."""
    if not files:
        raise SeedError(f"seed source has no raw {what} parquet files")
    try:
        row = (
            pl.scan_parquet(files)
            .select(
                pl.len().alias("rows"),
                pl.col("block_number").min().alias("min_block"),
                pl.col("block_number").max().alias("max_block"),
                pl.col("block_timestamp").min().alias("min_ts"),
                pl.col("symbol").n_unique().alias("markets"),
            )
            .collect()
            .row(0, named=True)
        )
    except Exception as exc:  # noqa: BLE001 - any unreadable parquet is a bad seed
        raise SeedError(f"seed source raw {what} is unreadable: {exc}") from exc
    if not row["rows"]:
        raise SeedError(f"seed source raw {what} is empty")
    return {k: int(v) for k, v in row.items()}


def _timestamp_at(files: list[Path], block: int, fallback: int) -> int:
    """Newest ``block_timestamp`` at or before ``block`` (``fallback`` if none)."""
    value = (
        pl.scan_parquet(files)
        .filter(pl.col("block_number") <= block)
        .select(pl.col("block_timestamp").max())
        .collect()
        .item()
    )
    return int(value) if value is not None else fallback


def _checkpoint_payload(
    symbol: str, last_block: int, last_timestamp: int, events: int, markets: int
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "last_block": last_block,
        "last_timestamp": last_timestamp,
        "total_events": events,
        "last_updated": datetime.now(tz=UTC).isoformat(),
        "metadata": {"markets_seen": markets},
    }


def seed_funding_state(
    source_dir: Path,
    gmx_root: Path,
    *,
    source_info: Mapping[str, Any] | None = None,
    overlap_blocks: int = SEED_OVERLAP_BLOCKS,
) -> SeedResult:
    """Seed ``{gmx_root}/funding/arbitrum`` from a ``data/full-history``-shaped tree.

    Never replaces existing state: raw events already carried forward by a
    previous release are newer than any seed, so they win. Everything is
    validated and assembled in a scratch directory next to ``gmx_root`` and
    moved into place last, so a failure leaves no half-seeded state.

    :param source_dir: Directory holding ``raw/``, ``rates/`` and ``direction/``
        (``historical/funding/arbitrum`` on the branch).
    :param gmx_root: The ``user_data/data/gmx`` directory.
    :param source_info: Provenance recorded in ``SEED_MANIFEST.json``.
    :param overlap_blocks: Blocks re-scanned past the synthesized checkpoint.
    :returns: :class:`SeedResult`.
    :raises SeedError: Source unusable, or a partial state is in the way.
    """
    source_dir = Path(source_dir)
    gmx_root = Path(gmx_root)
    target = funding_network_dir(gmx_root)

    if any(_parquet_files(target / "raw" / sub) for _, sub, _, _ in _SOURCES):
        return SeedResult("skipped_existing")
    if target.exists() and any(target.iterdir()):
        raise SeedError(
            f"{target} holds a partial funding state without raw events; refusing to "
            "replace it -- inspect or remove it deliberately"
        )
    if not source_dir.is_dir():
        raise SeedError(f"seed source {source_dir} does not exist")

    raw_files = {sub: _parquet_files(source_dir / "raw" / sub) for _, sub, _, _ in _SOURCES}
    stats = {key: _raw_stats(raw_files[sub], f"{sub} events") for key, sub, _, _ in _SOURCES}
    rate_files = [p for p in _parquet_files(source_dir / "rates") if p.name == "1h.parquet"]
    if not rate_files:
        raise SeedError("seed source has no rates/*/1h.parquet files")

    scratch = gmx_root.parent / ".funding-seed.tmp"
    if scratch.exists():
        shutil.rmtree(scratch)
    try:
        staged = scratch / NETWORK
        shutil.copytree(
            source_dir,
            staged,
            ignore=shutil.ignore_patterns("checkpoints", ".git*", "._*", SEED_MANIFEST),
        )

        checkpoints: dict[str, dict[str, Any]] = {}
        cp_dir = staged / "checkpoints"
        cp_dir.mkdir(parents=True, exist_ok=True)
        for key, sub, name, symbol in _SOURCES:
            existing = _read_checkpoint(source_dir / "checkpoints" / name)
            if existing is not None:
                shutil.copy2(source_dir / "checkpoints" / name, cp_dir / name)
                origin, last_block = "source", existing["last_block"]
            else:
                st = stats[key]
                last_block = max(st["min_block"] - 1, st["max_block"] - overlap_blocks)
                payload = _checkpoint_payload(
                    symbol,
                    last_block,
                    _timestamp_at(raw_files[sub], last_block, st["min_ts"]),
                    st["rows"],
                    st["markets"],
                )
                (cp_dir / name).write_text(json.dumps(payload, indent=2))
                origin = "synthesized"
            checkpoints[key] = {
                "origin": origin,
                "last_block": last_block,
                "raw_max_block": stats[key]["max_block"],
                "raw_rows": stats[key]["rows"],
            }

        manifest: dict[str, Any] = {
            "seeded_at": datetime.now(tz=UTC).isoformat(),
            "source": dict(source_info or {}),
            "overlap_blocks": overlap_blocks,
            "checkpoints": checkpoints,
            "symbols": len([d for d in (staged / "rates").iterdir() if d.is_dir()]),
            "files": sum(1 for p in staged.rglob("*") if p.is_file()),
        }
        (staged / SEED_MANIFEST).write_text(json.dumps(manifest, indent=2))

        target.parent.mkdir(parents=True, exist_ok=True)
        if target.exists():
            target.rmdir()  # verified empty above
        os.rename(staged, target)
    except SeedError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise SeedError(f"seeding failed: {exc}") from exc
    finally:
        shutil.rmtree(scratch, ignore_errors=True)
        if target.parent.is_dir() and not any(target.parent.iterdir()):
            target.parent.rmdir()

    return SeedResult("seeded", manifest)
