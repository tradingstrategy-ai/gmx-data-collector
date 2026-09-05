"""Shared atomic-write primitives for Parquet and Feather (IPC) files.

Every Parquet or Feather write in this repo should route through one of the
functions here instead of calling ``DataFrame.write_parquet()`` /
``to_parquet()`` / ``write_ipc()`` directly. A write interrupted mid-flight
(HyperSync ``429``, timeout, OOM-kill, process kill) can otherwise leave a
truncated file with no footer magic bytes -- the exact root cause of the
2026-08-25 incident, where an interrupted collection write left
``candles/arbitrum/GMX/1m.parquet`` and ``candles/arbitrum/OP/1h.parquet``
corrupt, which then aborted the entire Freqtrade export. The exported
feathers are themselves production artifacts (``user_data/data/gmx/futures/
*.feather`` feeds the downstream regime/drawdown panel that gates live
trading), so they get the same guarantee as the source Parquet store.

Every writer here writes to ``<name>.<ext>.tmp`` in the same directory,
fsyncs the file (and, after the rename, the containing directory), then
``os.replace()`` onto the final target. ``os.replace`` is atomic within a
filesystem, so an interruption at any point before the rename leaves only a
stray ``.tmp`` file and an intact previous target -- never a partial write
visible at the real path. ``ParquetStorage.sweep_orphaned_tmp_files()``
clears any such stragglers at collection startup.

Also exports :data:`CORRUPT_PARQUET_ERRORS`, the shared exception tuple for
"this file is corrupt" across both read engines this repo uses: pandas +
pyarrow (``ArrowInvalid``) and Polars directly (``pl.exceptions.ComputeError``,
covering both its Parquet and Feather/IPC readers). :data:`DATA_DEFECT_ERRORS`
(also in this module) supersedes it for new export-guard call sites.
"""

import errno
import logging
import os
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
from pyarrow.lib import ArrowInvalid

from gmx_historical_data.ohlcv_validation import ExportValidationError

logger = logging.getLogger(__name__)

#: Exception types raised by a truncated/corrupt Parquet or Feather file,
#: across both read engines this repo uses: pandas + pyarrow
#: (``storage.read_candles()``, via ``ArrowInvalid``) and Polars directly
#: (``pl.read_parquet``/``pl.read_ipc``, via ``pl.exceptions.ComputeError``).
#: ``OSError`` covers filesystem-level failures (permissions, disk full)
#: surfacing at the same call sites. Deprecated: kept only as a back-compat
#: alias for importers that predate :data:`DATA_DEFECT_ERRORS` --
#: ``FreqtradeExporter.export_candles()``/``export_funding()``'s per-symbol
#: guard no longer uses this tuple. See :data:`DATA_DEFECT_ERRORS` for
#: current usage.
CORRUPT_PARQUET_ERRORS: tuple[type[Exception], ...] = (
    ArrowInvalid,
    pl.exceptions.ComputeError,
    OSError,
)

#: Fatal filesystem/OS-resource conditions where the correct behaviour is to
#: abort the whole export run rather than treat one symbol as corrupt --
#: continuing would mark every remaining symbol "failed" one at a time for a
#: condition that has nothing to do with any of their data. See
#: :func:`is_fatal_environment_error`.
FATAL_ENVIRONMENT_ERRNOS: frozenset[int] = frozenset(
    {errno.ENOSPC, errno.EROFS, errno.EDQUOT, errno.EMFILE, errno.ENFILE}
)


def is_fatal_environment_error(exc: BaseException) -> bool:
    """Return ``True`` if ``exc`` is an ``OSError`` from a fatal environment condition.

    Distinguishes "the disk is full" (``ENOSPC``), a read-only remount
    (``EROFS``), a quota hit (``EDQUOT``), or exhausted file descriptors
    (``EMFILE``/``ENFILE``) from an ordinary "this file is corrupt"
    ``OSError``. Callers must check this *before* treating an ``OSError`` as
    a per-symbol data defect, and re-raise instead of skipping when it is
    ``True`` -- see ``FreqtradeExporter.export_candles``/``export_funding``.

    :param exc: The caught exception.
    :returns: ``True`` iff ``exc`` is an ``OSError`` whose ``errno`` is in
        :data:`FATAL_ENVIRONMENT_ERRNOS`.
    """
    return isinstance(exc, OSError) and exc.errno in FATAL_ENVIRONMENT_ERRNOS


#: Exception types that mean "this symbol/timeframe's data is defective --
#: skip it and keep going", classified by what the operator should do
#: rather than by which library raised it. Supersedes
#: :data:`CORRUPT_PARQUET_ERRORS` for new call sites: adds
#: :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError` so a
#: ``validate_ohlcv``/``assert_export_parity``/history-guard failure is
#: caught by the same per-symbol guard as a truncated Parquet file, instead
#: of propagating and aborting the whole export. ``OSError`` here still
#: needs an :func:`is_fatal_environment_error` check first -- an ``OSError``
#: matching that check must be re-raised, not treated as a data defect.
DATA_DEFECT_ERRORS: tuple[type[Exception], ...] = (
    ArrowInvalid,
    pl.exceptions.ComputeError,
    ExportValidationError,
    OSError,
)


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


def atomic_write_ipc(
    df: pl.DataFrame,
    output_path: Path,
    *,
    compression: str = "zstd",
) -> None:
    """Write a Polars DataFrame to Feather/IPC atomically.

    Same contract as :func:`atomic_write_parquet`: writes to
    ``<name>.tmp`` in the same directory, fsyncs, then ``os.replace()``s
    onto ``output_path``, so an interrupted write can only ever strand the
    ``.tmp`` file -- never leave a truncated feather at the real path.

    :param df: Polars DataFrame to write.
    :param output_path: Final destination path for the Feather file.
    :param compression: IPC compression codec.
    :raises OSError: If the temporary file cannot be written, fsynced, or
        renamed onto the target.
    """
    tmp_path = output_path.with_name(output_path.name + ".tmp")
    df.write_ipc(tmp_path, compression=compression)
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
