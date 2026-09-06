"""Export GMX data to Freqtrade-compatible format.

Freqtrade expects OHLCV data with columns: date, open, high, low, close, volume

Exported file types:

- **OHLCV candles**: ``{SYMBOL}_USDC_USDC-{tf}-futures.feather``
- **Funding rate**: ``{SYMBOL}_USDC_USDC-{tf}-funding_rate.feather``
  (``open`` = hourly funding rate, other OHLCV columns = 0)
- **Mark price**: ``{SYMBOL}_USDC_USDC-{tf}-mark.feather``
  (OHLCV data used as mark price proxy)
- **Index price**: ``{SYMBOL}_USDC_USDC-{tf}-index.feather``
  (same as mark — GMX uses Chainlink oracle as index price)

Funding rate parquet files are read from
``{data_dir}/funding/arbitrum/rates/{SYMBOL}/{tf}.parquet``.
"""

import logging
import re
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import polars as pl

from gmx_historical_data.atomic_parquet import (
    DATA_DEFECT_ERRORS,
    atomic_write_ipc,
    atomic_write_parquet,
    is_fatal_environment_error,
)
from gmx_historical_data.ohlcv_validation import (
    ExportValidationError,
    assert_export_parity,
    validate_ohlcv,
)
from gmx_historical_data.storage import (
    ParquetStorage,
    _assert_history_preserved,
    _coverage_stats,
)

logger = logging.getLogger(__name__)

# Matches genuine timeframe-shaped filename stems only (e.g. "1h", "4h",
# "15m", "1d") -- the unified-funding pipeline also writes companion data
# products into the same directory (e.g. "1h_datastore", "1h_factor",
# "1h_short_borrow", "1h_borrow_rate"), none of which are funding-rate data
# and all of which fail this pattern because of their trailing suffix.
_FUNDING_TIMEFRAME_PATTERN = re.compile(r"^\d+[mhd]$")


@dataclass(frozen=True, slots=True)
class ExportFailure:
    """One skipped ``(symbol, timeframe)`` during export.

    Carries enough structure for the CLI's failure panel and any downstream
    alerting to show *what kind* of failure occurred, not just that one did
    -- the whole point of this taxonomy (see the 2026-09-05 design doc).

    :param symbol: Token symbol that failed.
    :param timeframe: Timeframe that failed.
    :param reason: Short greppable slug -- an
        :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`'s
        own ``.reason``, or the caught exception's class name (e.g.
        ``"ArrowInvalid"``, ``"ComputeError"``, ``"OSError"``) when it isn't
        one.
    :param message: Full exception message, for logs/failure panels.
    """

    symbol: str
    timeframe: str
    reason: str
    message: str


def _classify_export_failure(symbol: str, timeframe: str, exc: Exception) -> ExportFailure:
    """Build an :class:`ExportFailure` from a caught data-defect exception.

    :param symbol: The symbol being exported when ``exc`` was raised.
    :param timeframe: The timeframe being exported when ``exc`` was raised.
    :param exc: The caught exception (a member of
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS`).
    :returns: An :class:`ExportFailure` with ``reason`` taken from
        ``exc.reason`` when ``exc`` is an
        :class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`,
        else the exception's class name.
    """
    reason = exc.reason if isinstance(exc, ExportValidationError) else type(exc).__name__
    return ExportFailure(symbol=symbol, timeframe=timeframe, reason=reason, message=str(exc))


class FreqtradeExporter:
    """Export GMX candle and funding rate data to Freqtrade format.

    :param data_dir: Source directory with GMX data (candles + funding).
    :param output_dir: Output directory for Freqtrade files.
    """

    def __init__(self, data_dir: Path, output_dir: Path):
        """Initialize exporter.

        :param data_dir: Source GMX data directory.
        :param output_dir: Target directory for Freqtrade files.
        """
        self.data_dir = Path(data_dir)
        self.storage = ParquetStorage(self.data_dir)
        self.output_dir = Path(output_dir)
        self.funding_dir = self.data_dir / "funding" / "arbitrum" / "rates"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export_candles(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        keep_parquet: bool = True,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
        """Export OHLCV (candles + mark + index) feathers only.

        Reads only from ``{data_dir}/candles/`` and writes only ``-futures``,
        ``-mark``, and ``-index`` feathers.  Never touches funding files.

        The per-symbol, per-timeframe guard catches
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS` --
        both a corrupt source/destination Parquet or Feather file
        (``ArrowInvalid``, ``pl.exceptions.ComputeError``) and a validation
        failure from this module's own transform
        (:class:`~gmx_historical_data.ohlcv_validation.ExportValidationError`,
        covering ``validate_ohlcv``, ``assert_export_parity``, and the
        history-preservation guard). A fatal environment ``OSError`` (see
        :func:`~gmx_historical_data.atomic_parquet.is_fatal_environment_error`
        -- e.g. disk full) is re-raised immediately rather than treated as a
        per-symbol defect, since it says nothing about any one symbol's data.

        The guard wraps each *timeframe* individually, not the whole symbol:
        a symbol with 3 healthy timeframes and 1 failing one still gets the
        3 healthy timeframes counted in ``results`` and appears in
        ``failed_symbols`` for the one that failed -- both can be true for
        the same symbol at once.

        :param symbols: Specific symbols (default: all candle symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :param keep_parquet: Default ``True``.  If ``False`` the source candle
            parquet is deleted after a successful feather export.
        :returns: Tuple of (dict mapping symbol to export stats, sorted list
            of symbols with at least one failed timeframe, list of
            :class:`ExportFailure` detailing each failed timeframe).
        :raises OSError: If a fatal environment condition (disk full,
            read-only filesystem, quota, or file-descriptor exhaustion) is
            hit -- see
            :func:`~gmx_historical_data.atomic_parquet.is_fatal_environment_error`.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        candle_symbols = set(self.storage.list_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in candle_symbols) if symbols else sorted(candle_symbols)
        )

        results: dict[str, dict] = {}
        failed_symbols: list[str] = []
        failures: list[ExportFailure] = []
        for symbol in export_symbols:
            ohlcv_files = mark_files = index_files = total_candles = 0
            candle_tfs = set(self.storage.list_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in candle_tfs] if timeframes else sorted(candle_tfs)
            )
            symbol_failed = False

            for tf in export_tfs:
                try:
                    raw = self.storage.read_candles(tf, symbol)
                    if raw.empty:
                        continue
                    df = pl.from_pandas(raw)

                    ft_df = validate_ohlcv(
                        self._transform_dataframe(df),
                        timestamp_column="date",
                        location=f"export_candles({symbol}/{tf})",
                    )
                    self._write(
                        ft_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    ohlcv_files += 2 if output_format == "both" else 1
                    total_candles += len(ft_df)

                    mark_df = validate_ohlcv(
                        self._transform_mark_price(df),
                        timestamp_column="date",
                        location=f"export_candles({symbol}/{tf}) mark",
                    )
                    self._write(
                        mark_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="mark",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    mark_files += 2 if output_format == "both" else 1

                    self._write(
                        mark_df,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="index",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                    )
                    index_files += 2 if output_format == "both" else 1

                    if not keep_parquet and output_format in {"feather", "both"}:
                        self._cleanup_candle_source(symbol, tf)
                except DATA_DEFECT_ERRORS as e:
                    if is_fatal_environment_error(e):
                        raise
                    logger.error(
                        "export_candles(%s/%s): skipping timeframe after read/write failure: %s",
                        symbol,
                        tf,
                        e,
                    )
                    failures.append(_classify_export_failure(symbol, tf, e))
                    symbol_failed = True
                    continue

            if ohlcv_files or mark_files or index_files:
                results[symbol] = {
                    "files": ohlcv_files + mark_files + index_files,
                    "candles": total_candles,
                    "ohlcv_files": ohlcv_files,
                    "funding_files": 0,
                    "mark_files": mark_files,
                    "index_files": index_files,
                }
            if symbol_failed:
                failed_symbols.append(symbol)

        return results, failed_symbols, failures

    def export_funding(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
        """Export funding_rate feathers only.

        Reads only from ``{data_dir}/funding/`` and writes only
        ``-funding_rate`` feathers.  Never touches OHLCV files.  The funding
        parquet source is owned by the unified-funding pipeline — this
        method never deletes it.

        Shares :meth:`export_candles`'s per-symbol, per-timeframe guard over
        :data:`~gmx_historical_data.atomic_parquet.DATA_DEFECT_ERRORS` --
        see that method's docstring for the full contract, including the
        fatal-environment-error re-raise and the "a symbol can be in both
        ``results`` and ``failed_symbols``" semantics.

        :param symbols: Specific symbols (default: all funding symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :returns: Tuple of (dict mapping symbol to export stats, sorted list
            of symbols with at least one failed timeframe, list of
            :class:`ExportFailure` detailing each failed timeframe).
        :raises OSError: On a fatal environment condition -- see
            :meth:`export_candles`.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        funding_symbols = set(self.list_funding_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in funding_symbols)
            if symbols
            else sorted(funding_symbols)
        )

        results: dict[str, dict] = {}
        failed_symbols: list[str] = []
        failures: list[ExportFailure] = []
        for symbol in export_symbols:
            funding_files = 0
            funding_tfs = set(self.list_funding_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in funding_tfs]
                if timeframes
                else sorted(funding_tfs)
            )
            symbol_failed = False

            for tf in export_tfs:
                try:
                    funding_df = self._read_funding_rate(symbol, tf)
                    if funding_df is None or funding_df.is_empty():
                        continue
                    ft_funding = validate_ohlcv(
                        self._transform_funding_rate(funding_df),
                        timestamp_column="date",
                        location=f"export_funding({symbol}/{tf})",
                        allow_nonpositive_prices=True,
                    )
                    self._write(
                        ft_funding,
                        gmx_dir
                        / self._get_freqtrade_filename(
                            symbol,
                            tf,
                            "feather" if output_format == "both" else output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="funding_rate",
                        ),
                        output_format,
                        overwrite,
                        unsafe_overwrite,
                        allow_nonpositive_prices=True,
                    )
                    funding_files += 2 if output_format == "both" else 1
                except DATA_DEFECT_ERRORS as e:
                    if is_fatal_environment_error(e):
                        raise
                    logger.error(
                        "export_funding(%s/%s): skipping timeframe after read/write failure: %s",
                        symbol,
                        tf,
                        e,
                    )
                    failures.append(_classify_export_failure(symbol, tf, e))
                    symbol_failed = True
                    continue

            if funding_files:
                results[symbol] = {
                    "files": funding_files,
                    "candles": 0,
                    "ohlcv_files": 0,
                    "funding_files": funding_files,
                    "mark_files": 0,
                    "index_files": 0,
                }
            if symbol_failed:
                failed_symbols.append(symbol)

        return results, failed_symbols, failures

    def export(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        keep_parquet: bool = True,
    ) -> tuple[dict[str, dict], list[str], list[ExportFailure]]:
        """Backward-compat wrapper: run candle export then funding export.

        Prefer :meth:`export_candles` and :meth:`export_funding` directly so
        callers can isolate the two pipelines.  This wrapper exists for the
        ``gmx_historical_data export-freqtrade`` CLI command and any external
        callers that relied on the combined behaviour.

        Parameters identical to :meth:`export_candles` plus the funding
        rate output.

        :returns: Tuple of (merged per-symbol stats, sorted union of symbols
            that failed candle export and/or funding export and were
            skipped, sorted union of both pipelines' :class:`ExportFailure`
            lists -- see :meth:`export_candles` and :meth:`export_funding`).
        """
        candle_kwargs = dict(
            symbols=symbols,
            timeframes=timeframes,
            output_format=output_format,
            trading_mode=trading_mode,
            quote_currency=quote_currency,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
            keep_parquet=keep_parquet,
        )
        candle_results, candle_failed_symbols, candle_failures = self.export_candles(
            **candle_kwargs
        )
        funding_results, funding_failed_symbols, funding_failures = self.export_funding(
            symbols=symbols,
            timeframes=timeframes,
            output_format=output_format,
            trading_mode=trading_mode,
            quote_currency=quote_currency,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
        )
        failed_symbols = sorted(set(candle_failed_symbols) | set(funding_failed_symbols))
        failures = sorted(
            [*candle_failures, *funding_failures], key=lambda f: (f.symbol, f.timeframe)
        )

        merged: dict[str, dict] = {}
        for symbol in sorted(set(candle_results) | set(funding_results)):
            c = candle_results.get(symbol, {})
            f = funding_results.get(symbol, {})
            merged[symbol] = {
                "files": c.get("files", 0) + f.get("files", 0),
                "candles": c.get("candles", 0),
                "ohlcv_files": c.get("ohlcv_files", 0),
                "funding_files": f.get("funding_files", 0),
                "mark_files": c.get("mark_files", 0),
                "index_files": c.get("index_files", 0),
            }
        return merged, failed_symbols, failures

    def _make_gmx_dir(self, trading_mode: str) -> Path:
        """Resolve and create the per-trading-mode output directory."""
        gmx_dir = (
            self.output_dir / "gmx" / "futures"
            if trading_mode == "futures"
            else self.output_dir / "gmx"
        )
        gmx_dir.mkdir(parents=True, exist_ok=True)
        return gmx_dir

    # ------------------------------------------------------------------
    # Funding rate helpers
    # ------------------------------------------------------------------

    def list_funding_symbols(self) -> list[str]:
        """List symbols that have funding rate data.

        :returns: Sorted list of symbol names.
        """
        if not self.funding_dir.exists():
            return []
        # Skip macOS AppleDouble sidecars (._*.parquet) and other dotfiles.
        return sorted(
            d.name
            for d in self.funding_dir.iterdir()
            if d.is_dir()
            and not d.name.startswith(".")
            and any(not p.name.startswith(".") for p in d.glob("*.parquet"))
        )

    def list_funding_timeframes(self, symbol: str) -> list[str]:
        """List available funding rate timeframes for a symbol.

        Only matches genuine timeframe files (e.g. ``1h``, ``4h``) -- the same
        directory also holds companion data products from the unified-funding
        pipeline (``{tf}_datastore``, ``{tf}_factor``, ``{tf}_short_borrow``,
        ``{tf}_borrow_rate``, ...), which must never be treated as an
        exportable funding-rate timeframe.

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :returns: Sorted list of timeframe strings.
        """
        symbol_dir = self.funding_dir / symbol
        if not symbol_dir.exists():
            return []
        return sorted(
            f.stem
            for f in symbol_dir.glob("*.parquet")
            if not f.name.startswith(".") and _FUNDING_TIMEFRAME_PATTERN.match(f.stem)
        )

    # ------------------------------------------------------------------
    # Data readers
    # ------------------------------------------------------------------

    def _read_funding_rate(self, symbol: str, timeframe: str) -> pl.DataFrame | None:
        """Read funding rate parquet for a symbol/timeframe.

        :param symbol: Token symbol.
        :param timeframe: Timeframe (e.g., ``'1h'``).
        :returns: Polars DataFrame with funding columns, or ``None`` if missing.
        """
        path = self.funding_dir / symbol / f"{timeframe}.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if df.is_empty():
            return None
        return df

    # ------------------------------------------------------------------
    # Transformers
    # ------------------------------------------------------------------

    def _transform_dataframe(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform GMX OHLCV dataframe to Freqtrade format.

        :param df: GMX candle dataframe.
        :returns: Freqtrade-compatible Polars dataframe.
        """
        df = df.rename({"timestamp": "date"})
        df = df.with_columns(
            [
                pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
                pl.lit(0).cast(pl.Float64).alias("volume"),
            ]
        )
        df = df.select(["date", "open", "high", "low", "close", "volume"])
        return df.sort("date")

    def _transform_funding_rate(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform GMX funding rate dataframe to Freqtrade format.

        FreqTrade stores funding rate in the ``open`` column with other
        OHLCV columns set to 0.  Uses ``funding_rate_hourly`` as the
        rate value (falls back to ``funding_rate`` if hourly is missing).

        The output is restricted to the canonical Freqtrade schema
        (``date, open, high, low, close, volume``).  Earlier versions of
        this method passed source columns through, which caused width
        drift as the upstream funding parquet schema evolved (added
        ``is_gap_filled`` / ``source``).  Pinning the output schema
        keeps merges into older feathers compatible.

        :param df: Funding rate Polars dataframe from parquet.
        :returns: Freqtrade-compatible Polars dataframe with exactly
            six columns: ``date, open, high, low, close, volume``.
        """
        col = "funding_rate_hourly" if "funding_rate_hourly" in df.columns else "funding_rate"
        df = df.rename({"timestamp": "date", col: "open"})
        df = df.with_columns(
            [
                pl.col("open").cast(pl.Float64),
                pl.lit(0).cast(pl.Float64).alias("high"),
                pl.lit(0).cast(pl.Float64).alias("low"),
                pl.lit(0).cast(pl.Float64).alias("close"),
                pl.lit(0).cast(pl.Float64).alias("volume"),
                pl.col("date").dt.convert_time_zone("UTC").dt.cast_time_unit("ns"),
            ]
        )
        df = df.drop_nulls(subset=["open"])
        df = df.select(["date", "open", "high", "low", "close", "volume"])
        return (
            df.sort("date").unique(subset=["date"], keep="first", maintain_order=False).sort("date")
        )

    def _transform_mark_price(self, df: pl.DataFrame) -> pl.DataFrame:
        """Generate mark price feather from OHLCV candle data.

        Uses OHLCV as a mark price proxy (GMX doesn't provide a separate
        mark price feed).

        :param df: GMX candle Polars dataframe.
        :returns: Freqtrade-compatible mark price Polars dataframe.
        """
        return (
            self._transform_dataframe(df)
            .unique(subset=["date"], keep="first", maintain_order=False)
            .sort("date")
        )

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def _cleanup_candle_source(self, symbol: str, timeframe: str) -> None:
        """Delete the source *candle* parquet for a symbol/timeframe.

        Only ever touches ``{data_dir}/candles/...``.  By design, no analogous
        method exists for funding — the funding parquet is owned by the
        unified-funding pipeline and never deleted by this exporter.

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :param timeframe: Timeframe string (e.g., ``'1h'``).
        """
        candle_path = self.data_dir / "candles" / "arbitrum" / symbol / f"{timeframe}.parquet"
        if candle_path.exists():
            candle_path.unlink()
            logger.debug("Removed source candle parquet: %s", candle_path)

    def _read_existing_export_frame(
        self,
        path: Path,
        fmt: str,
        *,
        allow_nonpositive_prices: bool,
    ) -> pl.DataFrame:
        """Read and validate an existing export destination."""
        existing = (
            pl.read_ipc(path, memory_map=False) if fmt == "feather" else pl.read_parquet(path)
        )
        validate_ohlcv(
            existing,
            timestamp_column="date",
            location=str(path),
            allow_nonpositive_prices=allow_nonpositive_prices,
        )
        return existing

    def _merge_export_frames(
        self,
        incoming: pl.DataFrame,
        existing: pl.DataFrame,
        path: Path,
        *,
        file_size: int,
        allow_nonpositive_prices: bool,
    ) -> pl.DataFrame:
        """Merge validated export frames while preserving history."""
        if set(incoming.columns) != set(existing.columns):
            extra_in_existing = set(existing.columns) - set(incoming.columns)
            extra_in_incoming = set(incoming.columns) - set(existing.columns)
            if extra_in_incoming:
                raise ExportValidationError(
                    str(path),
                    "schema_regression",
                    f"Schema regression while merging {path.name}: incoming "
                    f"dataframe has columns the existing file lacks: "
                    f"{sorted(extra_in_incoming)}.  Refusing to fill nulls — "
                    "regenerate the file with --unsafe-overwrite if this is intentional.",
                )
            logger.info(
                "Schema realignment on %s: dropping legacy columns %s "
                "(existing width %d -> incoming width %d)",
                path.name,
                sorted(extra_in_existing),
                len(existing.columns),
                len(incoming.columns),
            )
            existing = existing.select(incoming.columns)

        # Timestamp-precision alignment.  ``_transform_*`` always emits ``date``
        # at nanosecond precision, but a destination feather on disk may carry a
        # different time unit -- written by an older exporter, by a different
        # tool, or by a partially-completed run.  ``pl.concat`` rejects that
        # outright ("failed to vstack column 'date'"), so a single odd-precision
        # file wedges the whole export.  Normalise the existing frame onto the
        # incoming dtype, mirroring the canonicalisation
        # ``ParquetStorage.save_candles`` already applies to the raw candle
        # store.  Widening (us -> ns) is lossless; the narrowing direction is
        # safe here because OHLCV timestamps land on whole bar boundaries.
        incoming_date_dtype = incoming.schema["date"]
        if existing.schema["date"] != incoming_date_dtype:
            logger.info(
                "Timestamp precision realignment on %s: %s -> %s",
                path.name,
                existing.schema["date"],
                incoming_date_dtype,
            )
            existing = existing.with_columns(pl.col("date").cast(incoming_date_dtype))

        existing_stats = _coverage_stats(existing, ts_col="date")
        incoming_stats = _coverage_stats(incoming, ts_col="date")
        merged = (
            pl.concat([existing, incoming])
            .unique(subset=["date"], keep="last", maintain_order=False)
            .sort("date")
        )
        validate_ohlcv(
            merged,
            timestamp_column="date",
            location=str(path),
            allow_nonpositive_prices=allow_nonpositive_prices,
        )
        merged_stats = _coverage_stats(merged, ts_col="date")
        _assert_history_preserved(
            existing_stats, incoming_stats, merged_stats, ts_label="date", location=str(path)
        )
        logger.debug(
            "Merged %s: existing=%d rows (%.1f KB), new=%d rows, merged=%d rows",
            path,
            existing_stats["rows"],
            file_size / 1024,
            incoming_stats["rows"],
            merged_stats["rows"],
        )
        return merged

    def _prepare_export_frame(
        self,
        df: pl.DataFrame,
        path: Path,
        *,
        fmt: str,
        unsafe_overwrite: bool,
        allow_nonpositive_prices: bool,
    ) -> pl.DataFrame:
        """Merge incoming export data with any existing destination file.

        ``unsafe_overwrite`` bypasses reading the destination entirely so a
        corrupt existing file can be regenerated — validation of the existing
        frame must not run before that escape hatch.
        """
        if unsafe_overwrite or not path.exists():
            return df

        existing = self._read_existing_export_frame(
            path,
            fmt,
            allow_nonpositive_prices=allow_nonpositive_prices,
        )
        return self._merge_export_frames(
            df,
            existing,
            path,
            file_size=path.stat().st_size,
            allow_nonpositive_prices=allow_nonpositive_prices,
        )

    def _write_single_frame(self, df: pl.DataFrame, path: Path, fmt: str) -> None:
        """Write a single export file in the requested format.

        Both branches write atomically (see ``atomic_parquet.py``) so an
        interrupted write (HyperSync ``429``, kill, timeout) can never leave a
        truncated ``-futures``/``-funding_rate`` feather or parquet -- these
        exported feathers are themselves production artifacts
        (``user_data/data/gmx/futures/*.feather`` feeds the downstream
        regime/drawdown panel that gates live trading), so they get the same
        guarantee as the source Parquet store. This is the direct
        single-format counterpart of ``_write_both``'s tmp+backup dance below.
        """
        if fmt == "feather":
            atomic_write_ipc(df, path)
        elif fmt == "parquet":
            atomic_write_parquet(df, path)
        else:
            raise ValueError(f"Unsupported export format: {fmt}")

    def _write_both(
        self,
        df: pl.DataFrame,
        feather_path: Path,
        parquet_path: Path,
        unsafe_overwrite: bool,
        allow_nonpositive_prices: bool,
    ) -> None:
        """Publish matching Feather and Parquet files from one canonical frame.

        ``unsafe_overwrite`` bypasses reading and merging both destinations so a
        corrupt pre-existing file can be regenerated from ``df`` alone.
        """
        if not unsafe_overwrite:
            existing_frames: list[pl.DataFrame] = []
            if feather_path.exists():
                existing_frames.append(
                    self._read_existing_export_frame(
                        feather_path,
                        "feather",
                        allow_nonpositive_prices=allow_nonpositive_prices,
                    )
                )
            if parquet_path.exists():
                existing_frames.append(
                    self._read_existing_export_frame(
                        parquet_path,
                        "parquet",
                        allow_nonpositive_prices=allow_nonpositive_prices,
                    )
                )

            if len(existing_frames) == 2:
                assert_export_parity(
                    existing_frames[0], existing_frames[1], location=str(feather_path)
                )

            existing = existing_frames[0] if existing_frames else None
            if existing is not None:
                df = self._merge_export_frames(
                    df,
                    existing,
                    feather_path,
                    file_size=feather_path.stat().st_size if feather_path.exists() else 0,
                    allow_nonpositive_prices=allow_nonpositive_prices,
                )

        feather_tmp = feather_path.with_name(f".{feather_path.name}.{uuid4().hex}.tmp")
        parquet_tmp = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.tmp")
        feather_backup = feather_path.with_name(f".{feather_path.name}.{uuid4().hex}.bak")
        parquet_backup = parquet_path.with_name(f".{parquet_path.name}.{uuid4().hex}.bak")
        feather_had_original = feather_path.exists()
        parquet_had_original = parquet_path.exists()
        try:
            self._write_single_frame(df, feather_tmp, "feather")
            self._write_single_frame(df, parquet_tmp, "parquet")
            assert_export_parity(
                pl.read_ipc(feather_tmp, memory_map=False),
                pl.read_parquet(parquet_tmp),
                location=str(feather_path),
            )
            if feather_path.exists():
                feather_path.replace(feather_backup)
            if parquet_path.exists():
                parquet_path.replace(parquet_backup)
            feather_tmp.replace(feather_path)
            parquet_tmp.replace(parquet_path)
        except Exception:
            for destination, backup, had_original in (
                (feather_path, feather_backup, feather_had_original),
                (parquet_path, parquet_backup, parquet_had_original),
            ):
                if backup.exists():
                    if destination.exists():
                        destination.unlink()
                    backup.replace(destination)
                elif not had_original and destination.exists():
                    # No original file existed, so a partially published new
                    # destination must not survive a failed paired publish.
                    destination.unlink()
            for temp_path in (feather_tmp, parquet_tmp):
                if temp_path.exists():
                    temp_path.unlink()
            raise
        finally:
            for backup_path in (feather_backup, parquet_backup):
                if backup_path.exists():
                    backup_path.unlink()

    def _write(
        self,
        df: pl.DataFrame,
        path: Path,
        fmt: str,
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
        allow_nonpositive_prices: bool = False,
    ) -> None:
        """Merge-write dataframe into an existing file or create it.

        Behaviour matrix:

        +---------------------+----------------+----------------------------------+
        | Flags               | Existing file? | Effect                           |
        +=====================+================+==================================+
        | default             | yes            | merge, history guard ON          |
        +---------------------+----------------+----------------------------------+
        | overwrite=True      | yes            | merge, history guard ON          |
        +---------------------+----------------+----------------------------------+
        | unsafe_overwrite    | yes            | replace entirely, NO guard       |
        +---------------------+----------------+----------------------------------+
        | any                 | no             | write new file                   |
        +---------------------+----------------+----------------------------------+

        ``overwrite`` is kept as a backward-compatible flag but now still
        merges with the history guard.  Use ``unsafe_overwrite=True`` only
        for schema migrations where you intentionally discard old rows.
        This change was made after the 2026-05-11 incident, where
        ``--overwrite`` (set for a funding schema migration) silently
        truncated multi-year OHLCV feathers to a 6-month window.

        :param df: New Polars dataframe to merge in.
        :param path: Output file path (created if missing).
        :param fmt: ``'feather'`` or ``'parquet'``.
        :param overwrite: Reserved for CLI compatibility; still merges with
            the history guard.
        :param unsafe_overwrite: If ``True``, bypass the history guard and
            replace the file entirely.  For schema migrations only.
        :raises ValueError: If a merge would shrink existing history and
            ``unsafe_overwrite`` is not set.
        """
        if fmt == "both":
            feather_path = path if path.suffix == ".feather" else path.with_suffix(".feather")
            parquet_path = feather_path.with_suffix(".parquet")
            self._write_both(
                df,
                feather_path,
                parquet_path,
                unsafe_overwrite,
                allow_nonpositive_prices,
            )
            return

        if fmt not in {"feather", "parquet"}:
            raise ValueError(f"Unsupported export format: {fmt}")

        merged = self._prepare_export_frame(
            df,
            path,
            fmt=fmt,
            unsafe_overwrite=unsafe_overwrite,
            allow_nonpositive_prices=allow_nonpositive_prices,
        )
        self._write_single_frame(merged, path, fmt)

    # ------------------------------------------------------------------
    # Filename generation
    # ------------------------------------------------------------------

    def _get_freqtrade_filename(
        self,
        symbol: str,
        timeframe: str,
        fmt: str,
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        candle_type: str | None = None,
    ) -> str:
        """Generate Freqtrade-compatible filename.

        For futures OHLCV: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-futures.{ext}``
        For funding rate: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-funding_rate.{ext}``
        For mark price:   ``{BASE}_{QUOTE}_{SETTLE}-{tf}-mark.{ext}``

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :param timeframe: Timeframe (e.g., ``'1h'``).
        :param fmt: File format (``'feather'`` or ``'parquet'``).
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency.
        :param candle_type: Optional candle type suffix
            (``'funding_rate'``, ``'mark'``). ``None`` = standard OHLCV.
        :returns: Filename string.
        """
        base = f"{symbol}_{quote_currency}_{quote_currency}"

        if candle_type:
            # Funding rate / mark price files
            return f"{base}-{timeframe}-{candle_type}.{fmt}"

        if trading_mode == "futures":
            return f"{base}-{timeframe}-futures.{fmt}"
        else:
            return f"{symbol}_{quote_currency}-{timeframe}.{fmt}"
