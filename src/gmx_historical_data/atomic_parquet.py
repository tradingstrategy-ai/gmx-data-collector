"""Shared atomic-Parquet-write primitives.

Every Parquet write in this repo should route through one of the two
functions here instead of calling ``DataFrame.write_parquet()`` /
``to_parquet()`` directly. A write interrupted mid-flight (HyperSync
``429``, timeout, OOM-kill, process kill) can otherwise leave a truncated
file with no footer magic bytes -- the exact root cause of the 2026-08-25
incident, where an interrupted collection write left
``candles/arbitrum/GMX/1m.parquet`` and ``candles/arbitrum/OP/1h.parquet``
corrupt, which then aborted the entire Freqtrade export.

Both helpers write to ``<name>.parquet.tmp`` in the same directory, fsync
the file (and, after the rename, the containing directory), then
``os.replace()`` onto the final target. ``os.replace`` is atomic within a
filesystem, so an interruption at any point before the rename leaves only a
stray ``.tmp`` file and an intact previous target -- never a partial write
visible at the real path. ``ParquetStorage.sweep_orphaned_tmp_files()``
clears any such stragglers at collection startup.
"""

import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl

logger = logging.getLogger(__name__)


def _commit_tmp_file(tmp_path: Path, output_path: Path) -> None:
    """fsync a completed temp file, then atomically rename it onto its target.

    :param tmp_path: The fully-written temporary file (``<name>.parquet.tmp``).
    :param output_path: Final destination path.
    :raises OSError: If the temp file cannot be fsynced or renamed onto the target.
    """
    # fsync the temp file's contents before the rename so a crash between
    # write and rename cannot leave the target pointing at unflushed data.
    fd = os.open(str(tmp_path), os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

    os.replace(str(tmp_path), str(output_path))

    # fsync the containing directory so the rename itself is durable across
    # a crash, not just the file content.
    dir_fd = os.open(str(output_path.parent), os.O_RDONLY)
    try:
        os.fsync(dir_fd)
    finally:
        os.close(dir_fd)


def atomic_write_parquet(
    df: pl.DataFrame,
    output_path: Path,
    *,
    compression: str = "zstd",
    compression_level: int = 3,
) -> None:
    """Write a Polars DataFrame to Parquet atomically.

    :param df: Polars DataFrame to write.
    :param output_path: Final destination path for the Parquet file.
    :param compression: Parquet compression codec.
    :param compression_level: Compression level for the chosen codec.
    :raises OSError: If the temporary file cannot be written, fsynced, or
        renamed onto the target.
    """
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    df.write_parquet(str(tmp_path), compression=compression, compression_level=compression_level)
    _commit_tmp_file(tmp_path, output_path)


def atomic_write_parquet_pandas(
    df: pd.DataFrame,
    output_path: Path,
    *,
    index: bool = False,
    **to_parquet_kwargs: Any,
) -> None:
    """Write a pandas DataFrame to Parquet atomically.

    Same contract as :func:`atomic_write_parquet`, for the handful of call
    sites (e.g. the block-timestamp cache) that use pandas' ``to_parquet``
    instead of Polars.

    :param df: pandas DataFrame to write.
    :param output_path: Final destination path for the Parquet file.
    :param index: Forwarded to ``DataFrame.to_parquet`` (default ``False``,
        matching this repo's existing pandas Parquet call sites).
    :param to_parquet_kwargs: Additional keyword arguments forwarded to
        ``DataFrame.to_parquet``.
    :raises OSError: If the temporary file cannot be written, fsynced, or
        renamed onto the target.
    """
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    df.to_parquet(tmp_path, index=index, **to_parquet_kwargs)
    _commit_tmp_file(tmp_path, output_path)
