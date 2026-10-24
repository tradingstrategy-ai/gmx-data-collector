"""Audit or repair the zero-volume contract of GMX oracle candle exports."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

from gmx_historical_data.atomic_parquet import atomic_write_ipc_pandas


def _sync_directory(path: Path) -> None:
    """Persist directory entries before a destructive candle replacement."""
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _create_durable_backup_directory(path: Path) -> None:
    """Persist the new backup root and every newly created ancestor."""
    missing = []
    ancestor = path
    while not ancestor.exists():
        missing.append(ancestor)
        ancestor = ancestor.parent
    path.mkdir(parents=True, exist_ok=False)
    for directory in missing:
        _sync_directory(directory)
        _sync_directory(directory.parent)


def _write_durable_json(path: Path, value: dict) -> None:
    """Commit a journal with durable contents and directory entry."""
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w") as stream:
        stream.write(json.dumps(value, indent=2) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, path)
    _sync_directory(path.parent)


def _all_zero(volume: pd.Series) -> bool:
    """Reject nulls as well as nonzero values, including nullable dtypes."""
    return bool(volume.notna().all() and volume.eq(0.0).fillna(False).all())


def enforce_zero_volume(futures_dir: Path, backup_dir: Path | None = None) -> dict:
    """Audit canonical price files; a backup directory explicitly enables repair.

    :param futures_dir: GMX futures export directory.
    :param backup_dir: Empty/new backup root for original timestamp/volume columns and hashes.
    :returns: Counts, changed files, preimage hashes and structural failures.
    :raises ValueError: If the input directory is absent or backup overlaps inputs.
    """
    root = futures_dir.resolve()
    if not root.is_dir():
        raise ValueError("GMX futures directory is absent")
    if backup_dir is not None:
        backup_dir = backup_dir.resolve()
        if backup_dir.is_relative_to(root) or root.is_relative_to(backup_dir):
            raise ValueError("Backups must be outside the candle directory")
        _create_durable_backup_directory(backup_dir)
    result = {
        "contract": "gmx-oracle-zero-volume-v1",
        "checked": 0,
        "nonzero_files": [],
        "repaired": [],
        "failures": [],
        "preimages": {},
        "created_at": datetime.now(UTC).isoformat(),
    }
    paths = sorted(
        {p for suffix in ("futures", "mark", "index") for p in root.glob(f"*-{suffix}.feather")}
    )
    if not paths:
        raise ValueError("No canonical GMX price files found")
    for path in paths:
        try:
            before = pd.read_feather(path)
            if not {"date", "open", "high", "low", "close", "volume"} <= set(before.columns):
                raise ValueError("Missing candle columns")
            result["checked"] += 1
            if _all_zero(before["volume"]):
                continue
            result["nonzero_files"].append(path.name)
            if backup_dir is None:
                continue
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            saved = backup_dir / (path.name + ".original-volume.feather")
            original_volume = before[["date", "volume"]]
            atomic_write_ipc_pandas(original_volume, saved, compression="zstd")
            restored_volume = pd.read_feather(saved)
            if not restored_volume.equals(original_volume):
                raise ValueError("Volume preimage verification failed")
            preimage = {
                "file_sha256": digest,
                "volume_preimage": saved.name,
                "volume_preimage_sha256": hashlib.sha256(saved.read_bytes()).hexdigest(),
                "rows": len(before),
                "logical_preimage_reconstructed": False,
            }
            result["preimages"][path.name] = preimage
            journal = backup_dir / (path.name + ".json")
            _write_durable_json(journal, preimage)
            after = before.assign(volume=0.0)
            if not after.drop(columns="volume").equals(before.drop(columns="volume")):
                raise ValueError("Price or timestamp columns changed before repair")
            atomic_write_ipc_pandas(after, path, compression="zstd", compression_level=3)
            written = pd.read_feather(path)
            if not _all_zero(written["volume"]):
                raise ValueError("Stored candle volume is still nonzero")
            if not written.drop(columns="volume").equals(before.drop(columns="volume")):
                raise ValueError("Price or timestamp columns changed during repair")
            reconstructed = written.copy()
            reconstructed["volume"] = restored_volume["volume"]
            if not reconstructed.equals(before):
                raise ValueError("Logical preimage could not be reconstructed")
            preimage["logical_preimage_reconstructed"] = True
            _write_durable_json(journal, preimage)
            result["repaired"].append(path.name)
        except (OSError, ValueError, TypeError, AssertionError) as exc:
            result["failures"].append({"file": path.name, "error": str(exc)})
    if backup_dir is not None:
        _write_durable_json(backup_dir / "repair.json", result)
    return result


def main() -> int:
    """Fail a scheduled collector if any oracle-candle volume is nonzero."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--futures-dir", type=Path, required=True)
    parser.add_argument(
        "--backup-dir",
        type=Path,
        help="Enable repair and retain original timestamp/volume columns here",
    )
    args = parser.parse_args()
    result = enforce_zero_volume(args.futures_dir, args.backup_dir)
    print(json.dumps(result))
    return int(bool(result["failures"] or (result["nonzero_files"] and args.backup_dir is None)))


if __name__ == "__main__":
    raise SystemExit(main())
