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

    def export(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
        trading_mode: str = "futures",
        quote_currency: str = "USDC",
        overwrite: bool = False,
        keep_parquet: bool = False,
    ) -> dict[str, dict]:
        """Export GMX data to Freqtrade format.

        Exports OHLCV candles, funding rates, and mark price files for
        each symbol/timeframe combination.

        By default existing files are merged incrementally — no history is
        ever deleted.  Pass ``overwrite=True`` to replace files entirely.

        :param symbols: Specific symbols to export (default: all).
        :param timeframes: Specific timeframes to export (default: all).
        :param output_format: Output format (``'feather'`` or ``'parquet'``).
        :param trading_mode: ``'futures'`` or ``'spot'`` (default: ``'futures'``).
        :param quote_currency: Quote/settlement currency (default: ``'USDC'``).
        :param overwrite: If ``True``, replace existing files instead of merging.
        :param keep_parquet: If ``False`` (default), delete the source candle
            parquet file after a successful feather export.  Pass ``True`` to
            retain the source file.
        :returns: Dict mapping symbol to export stats.
        """
        # Create output directory
        if trading_mode == "futures":
            gmx_dir = self.output_dir / "gmx" / "futures"
        else:
            gmx_dir = self.output_dir / "gmx"
        gmx_dir.mkdir(parents=True, exist_ok=True)

        # Merge symbols from both candle and funding data
        candle_symbols = set(self.storage.list_symbols())
        funding_symbols = set(self.list_funding_symbols())
        all_symbols = sorted(candle_symbols | funding_symbols)

        if symbols:
            export_symbols = [s for s in symbols if s in all_symbols]
        else:
            export_symbols = all_symbols

        results = {}

        for symbol in export_symbols:
            ohlcv_files = 0
            funding_files = 0
            mark_files = 0
            index_files = 0
            total_candles = 0

            # Determine timeframes from candle + funding data
            candle_tfs = (
                set(self.storage.list_timeframes(symbol)) if symbol in candle_symbols else set()
            )
            funding_tfs = (
                set(self.list_funding_timeframes(symbol)) if symbol in funding_symbols else set()
            )
            available_tfs = sorted(candle_tfs | funding_tfs)

            if timeframes:
                export_tfs = [tf for tf in timeframes if tf in available_tfs]
            else:
                export_tfs = available_tfs

            for tf in export_tfs:
                # --- OHLCV candles ---
                if tf in candle_tfs:
                    raw = self.storage.read_candles(tf, symbol)
                    if not raw.empty:
                        df = pl.from_pandas(raw)
                        ft_df = self._transform_dataframe(df)
                        filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                        )
                        self._write(ft_df, gmx_dir / filename, output_format, overwrite)
                        ohlcv_files += 1
                        total_candles += len(ft_df)

                        # --- Mark price (OHLCV proxy) ---
                        mark_df = self._transform_mark_price(df)
                        mark_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="mark",
                        )
                        self._write(mark_df, gmx_dir / mark_filename, output_format, overwrite)
                        mark_files += 1

                        # --- Index price (same as mark for GMX/Chainlink) ---
                        index_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="index",
                        )
                        self._write(mark_df, gmx_dir / index_filename, output_format, overwrite)
                        index_files += 1

                # --- Funding rate ---
                if tf in funding_tfs:
                    funding_df = self._read_funding_rate(symbol, tf)
                    if funding_df is not None and not funding_df.is_empty():
                        ft_funding = self._transform_funding_rate(funding_df)
                        funding_filename = self._get_freqtrade_filename(
                            symbol,
                            tf,
                            output_format,
                            trading_mode,
                            quote_currency,
                            candle_type="funding_rate",
                        )
                        self._write(
                            ft_funding, gmx_dir / funding_filename, output_format, overwrite
                        )
                        funding_files += 1

                # --- Cleanup source parquet if requested ---
                # Deferred until after all writes (OHLCV, mark, index, funding) succeed.
                if not keep_parquet and output_format == "feather":
                    self._cleanup_source_parquet(symbol, tf)

            results[symbol] = {
                "files": ohlcv_files + funding_files + mark_files + index_files,
                "candles": total_candles,
                "ohlcv_files": ohlcv_files,
                "funding_files": funding_files,
                "mark_files": mark_files,
                "index_files": index_files,
            }

        return results

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

        :param df: Funding rate Polars dataframe from parquet.
        :returns: Freqtrade-compatible Polars dataframe.
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

    def _cleanup_source_parquet(self, symbol: str, timeframe: str) -> None:
        """Delete the source candle parquet for a symbol/timeframe.

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :param timeframe: Timeframe string (e.g., ``'1h'``).
        """
        candle_path = self.data_dir / "candles" / "arbitrum" / symbol / f"{timeframe}.parquet"
        if candle_path.exists():
            candle_path.unlink()
            logger.debug("Removed source parquet: %s", candle_path)

    def _write(self, df: pl.DataFrame, path: Path, fmt: str, overwrite: bool = False) -> None:
        """Merge-write dataframe into an existing file or create it.

        By default (``overwrite=False``) existing rows are never deleted.
        New rows are appended and overlapping timestamps are resolved by keeping
        the newer value (``keep="last"`` after concatenating ``[existing, new]``).
        This mirrors the ``_merge_feather`` logic in ``collect_daily_snapshot.py``
        and prevents ``export-freqtrade`` from truncating history built by the
        daily snapshot pipeline.

        Pass ``overwrite=True`` to skip the merge and replace the file entirely.

        Read errors propagate immediately — there is no fallback to writing
        incoming-only data.

        :param df: New Polars dataframe to merge in.
        :param path: Output file path (created if missing).
        :param fmt: ``'feather'`` or ``'parquet'``.
        :param overwrite: If ``True``, replace the existing file instead of merging.
        :raises: Any exception raised by ``pl.read_ipc`` / ``pl.read_parquet``
            propagates unchanged.
        """
        if not overwrite and path.exists():
            file_size = path.stat().st_size
            existing = pl.read_ipc(path) if fmt == "feather" else pl.read_parquet(path)
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
            df = merged

        if fmt == "feather":
            df.write_ipc(path)
        else:
            df.write_parquet(str(path))

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
