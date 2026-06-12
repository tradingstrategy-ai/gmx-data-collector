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
from pathlib import Path

import polars as pl

from gmx_historical_data.storage import (
    ParquetStorage,
    _assert_history_preserved,
    _coverage_stats,
)

logger = logging.getLogger(__name__)


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
    ) -> dict[str, dict]:
        """Export OHLCV (candles + mark + index) feathers only.

        Reads only from ``{data_dir}/candles/`` and writes only ``-futures``,
        ``-mark``, and ``-index`` feathers.  Never touches funding files.

        :param symbols: Specific symbols (default: all candle symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :param keep_parquet: Default ``True``.  If ``False`` the source candle
            parquet is deleted after a successful feather export.
        :returns: Dict mapping symbol to export stats.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        candle_symbols = set(self.storage.list_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in candle_symbols) if symbols else sorted(candle_symbols)
        )

        results: dict[str, dict] = {}
        for symbol in export_symbols:
            ohlcv_files = mark_files = index_files = total_candles = 0
            candle_tfs = set(self.storage.list_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in candle_tfs] if timeframes else sorted(candle_tfs)
            )

            for tf in export_tfs:
                raw = self.storage.read_candles(tf, symbol)
                if raw.empty:
                    continue
                df = pl.from_pandas(raw)

                ft_df = self._transform_dataframe(df)
                self._write(
                    ft_df,
                    gmx_dir
                    / self._get_freqtrade_filename(
                        symbol, tf, output_format, trading_mode, quote_currency
                    ),
                    output_format,
                    overwrite,
                    unsafe_overwrite,
                )
                ohlcv_files += 1
                total_candles += len(ft_df)

                mark_df = self._transform_mark_price(df)
                self._write(
                    mark_df,
                    gmx_dir
                    / self._get_freqtrade_filename(
                        symbol, tf, output_format, trading_mode, quote_currency, candle_type="mark"
                    ),
                    output_format,
                    overwrite,
                    unsafe_overwrite,
                )
                mark_files += 1

                self._write(
                    mark_df,
                    gmx_dir
                    / self._get_freqtrade_filename(
                        symbol, tf, output_format, trading_mode, quote_currency, candle_type="index"
                    ),
                    output_format,
                    overwrite,
                    unsafe_overwrite,
                )
                index_files += 1

                if not keep_parquet and output_format == "feather":
                    self._cleanup_candle_source(symbol, tf)

            results[symbol] = {
                "files": ohlcv_files + mark_files + index_files,
                "candles": total_candles,
                "ohlcv_files": ohlcv_files,
                "funding_files": 0,
                "mark_files": mark_files,
                "index_files": index_files,
            }

        return results

    def export_funding(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
    ) -> dict[str, dict]:
        """Export funding_rate feathers only.

        Reads only from ``{data_dir}/funding/`` and writes only
        ``-funding_rate`` feathers.  Never touches OHLCV files.  The funding
        parquet source is owned by the unified-funding pipeline — this
        method never deletes it.

        :param symbols: Specific symbols (default: all funding symbols).
        :param timeframes: Specific timeframes (default: all available).
        :param output_format: ``'feather'`` or ``'parquet'``.
        :param trading_mode: ``'futures'`` or ``'spot'``.
        :param quote_currency: Quote/settlement currency (default ``'USDC'``).
        :param overwrite: Backward-compat alias; merges with history guard.
        :param unsafe_overwrite: Bypass the history guard.  Schema migrations only.
        :returns: Dict mapping symbol to export stats.
        """
        gmx_dir = self._make_gmx_dir(trading_mode)

        funding_symbols = set(self.list_funding_symbols())
        export_symbols = (
            sorted(s for s in symbols if s in funding_symbols)
            if symbols
            else sorted(funding_symbols)
        )

        results: dict[str, dict] = {}
        for symbol in export_symbols:
            funding_files = 0
            funding_tfs = set(self.list_funding_timeframes(symbol))
            export_tfs = (
                [tf for tf in timeframes if tf in funding_tfs]
                if timeframes
                else sorted(funding_tfs)
            )

            for tf in export_tfs:
                funding_df = self._read_funding_rate(symbol, tf)
                if funding_df is None or funding_df.is_empty():
                    continue
                ft_funding = self._transform_funding_rate(funding_df)
                self._write(
                    ft_funding,
                    gmx_dir
                    / self._get_freqtrade_filename(
                        symbol,
                        tf,
                        output_format,
                        trading_mode,
                        quote_currency,
                        candle_type="funding_rate",
                    ),
                    output_format,
                    overwrite,
                    unsafe_overwrite,
                )
                funding_files += 1

            results[symbol] = {
                "files": funding_files,
                "candles": 0,
                "ohlcv_files": 0,
                "funding_files": funding_files,
                "mark_files": 0,
                "index_files": 0,
            }

        return results

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
    ) -> dict[str, dict]:
        """Backward-compat wrapper: run candle export then funding export.

        Prefer :meth:`export_candles` and :meth:`export_funding` directly so
        callers can isolate the two pipelines.  This wrapper exists for the
        ``gmx_historical_data export-freqtrade`` CLI command and any external
        callers that relied on the combined behaviour.

        Parameters identical to :meth:`export_candles` plus the funding
        rate output.  Returns merged per-symbol stats.
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
        candle_results = self.export_candles(**candle_kwargs)
        funding_results = self.export_funding(
            symbols=symbols,
            timeframes=timeframes,
            output_format=output_format,
            trading_mode=trading_mode,
            quote_currency=quote_currency,
            overwrite=overwrite,
            unsafe_overwrite=unsafe_overwrite,
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
        return merged

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

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :returns: Sorted list of timeframe strings.
        """
        symbol_dir = self.funding_dir / symbol
        if not symbol_dir.exists():
            return []
        return sorted(f.stem for f in symbol_dir.glob("*.parquet") if not f.name.startswith("."))

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

    def _write(
        self,
        df: pl.DataFrame,
        path: Path,
        fmt: str,
        overwrite: bool = False,
        unsafe_overwrite: bool = False,
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
        if unsafe_overwrite or not path.exists():
            if fmt == "feather":
                df.write_ipc(path, compression="zstd")
            else:
                df.write_parquet(str(path))
            return

        # Both default and overwrite=True paths run the merge + history guard.
        file_size = path.stat().st_size
        # memory_map=False: polars cannot memory-map zstd-compressed IPC files
        existing = (
            pl.read_ipc(path, memory_map=False) if fmt == "feather" else pl.read_parquet(path)
        )

        # Schema-tolerant alignment.  If the existing feather has columns the
        # incoming dataframe does not (legacy schema with extra source-side
        # columns), project it down to the incoming column set so polars'
        # ``concat`` accepts the merge.  Extra columns are dropped — the
        # canonical Freqtrade schema is whatever the current transform
        # produces.  If incoming has columns the existing file lacks, that
        # is a genuine schema regression and we surface it instead of
        # silently filling nulls.
        if set(df.columns) != set(existing.columns):
            extra_in_existing = set(existing.columns) - set(df.columns)
            extra_in_incoming = set(df.columns) - set(existing.columns)
            if extra_in_incoming:
                raise ValueError(
                    f"Schema regression while merging {path.name}: incoming "
                    f"dataframe has columns the existing file lacks: "
                    f"{sorted(extra_in_incoming)}.  Refusing to fill nulls — "
                    "regenerate the file with --unsafe-overwrite if this is "
                    "intentional."
                )
            logger.info(
                "Schema realignment on %s: dropping legacy columns %s "
                "(existing width %d -> incoming width %d)",
                path.name,
                sorted(extra_in_existing),
                len(existing.columns),
                len(df.columns),
            )
            existing = existing.select(df.columns)

        existing_stats = _coverage_stats(existing, ts_col="date")
        incoming_stats = _coverage_stats(df, ts_col="date")
        merged = (
            pl.concat([existing, df])
            .unique(subset=["date"], keep="last", maintain_order=False)
            .sort("date")
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

        if fmt == "feather":
            merged.write_ipc(path, compression="zstd")
        else:
            merged.write_parquet(str(path))

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
