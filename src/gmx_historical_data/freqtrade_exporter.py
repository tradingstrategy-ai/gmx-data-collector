"""Export GMX data to Freqtrade-compatible format.

Freqtrade expects OHLCV data with columns: date, open, high, low, close, volume

Exported file types:

- **OHLCV candles**: ``{SYMBOL}_USDC_USDC-{tf}-futures.feather``
- **Funding rate**: ``{SYMBOL}_USDC_USDC-{tf}-funding_rate.feather``
  (``open`` = hourly funding rate, other OHLCV columns = 0)
- **Mark price**: ``{SYMBOL}_USDC_USDC-{tf}-mark.feather``
  (OHLCV data used as mark price proxy)

Funding rate parquet files are read from
``{data_dir}/funding/arbitrum/rates/{SYMBOL}/{tf}.parquet``.
"""

import logging
from pathlib import Path

import pandas as pd
import pyarrow.feather as feather

from gmx_historical_data.storage import ParquetStorage

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
    ) -> dict[str, dict]:
        """Export GMX data to Freqtrade format.

        Exports OHLCV candles, funding rates, and mark price files for
        each symbol/timeframe combination.

        :param symbols: Specific symbols to export (default: all).
        :param timeframes: Specific timeframes to export (default: all).
        :param output_format: Output format (``'feather'`` or ``'parquet'``).
        :param trading_mode: ``'futures'`` or ``'spot'`` (default: ``'futures'``).
        :param quote_currency: Quote/settlement currency (default: ``'USDC'``).
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
            total_candles = 0

            # Determine timeframes from candle + funding data
            candle_tfs = set(self.storage.list_timeframes(symbol)) if symbol in candle_symbols else set()
            funding_tfs = set(self.list_funding_timeframes(symbol)) if symbol in funding_symbols else set()
            available_tfs = sorted(candle_tfs | funding_tfs)

            if timeframes:
                export_tfs = [tf for tf in timeframes if tf in available_tfs]
            else:
                export_tfs = available_tfs

            for tf in export_tfs:
                # --- OHLCV candles ---
                if tf in candle_tfs:
                    df = self.storage.read_candles(tf, symbol)
                    if not df.empty:
                        ft_df = self._transform_dataframe(df)
                        filename = self._get_freqtrade_filename(
                            symbol, tf, output_format, trading_mode, quote_currency,
                        )
                        self._write(ft_df, gmx_dir / filename, output_format)
                        ohlcv_files += 1
                        total_candles += len(ft_df)

                        # --- Mark price (OHLCV proxy) ---
                        mark_df = self._transform_mark_price(df)
                        mark_filename = self._get_freqtrade_filename(
                            symbol, tf, output_format, trading_mode, quote_currency,
                            candle_type="mark",
                        )
                        self._write(mark_df, gmx_dir / mark_filename, output_format)
                        mark_files += 1

                # --- Funding rate ---
                if tf in funding_tfs:
                    funding_df = self._read_funding_rate(symbol, tf)
                    if funding_df is not None and not funding_df.empty:
                        ft_funding = self._transform_funding_rate(funding_df)
                        funding_filename = self._get_freqtrade_filename(
                            symbol, tf, output_format, trading_mode, quote_currency,
                            candle_type="funding_rate",
                        )
                        self._write(ft_funding, gmx_dir / funding_filename, output_format)
                        funding_files += 1

            results[symbol] = {
                "files": ohlcv_files + funding_files + mark_files,
                "candles": total_candles,
                "ohlcv_files": ohlcv_files,
                "funding_files": funding_files,
                "mark_files": mark_files,
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
        return sorted(
            d.name for d in self.funding_dir.iterdir()
            if d.is_dir() and list(d.glob("*.parquet"))
        )

    def list_funding_timeframes(self, symbol: str) -> list[str]:
        """List available funding rate timeframes for a symbol.

        :param symbol: Token symbol (e.g., ``'ETH'``).
        :returns: Sorted list of timeframe strings.
        """
        symbol_dir = self.funding_dir / symbol
        if not symbol_dir.exists():
            return []
        return sorted(f.stem for f in symbol_dir.glob("*.parquet"))

    # ------------------------------------------------------------------
    # Data readers
    # ------------------------------------------------------------------

    def _read_funding_rate(self, symbol: str, timeframe: str) -> pd.DataFrame | None:
        """Read funding rate parquet for a symbol/timeframe.

        :param symbol: Token symbol.
        :param timeframe: Timeframe (e.g., ``'1h'``).
        :returns: DataFrame with funding columns, or ``None`` if missing.
        """
        path = self.funding_dir / symbol / f"{timeframe}.parquet"
        if not path.exists():
            return None
        df = pd.read_parquet(path)
        if df.empty:
            return None
        return df

    # ------------------------------------------------------------------
    # Transformers
    # ------------------------------------------------------------------

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform GMX OHLCV dataframe to Freqtrade format.

        :param df: GMX candle dataframe.
        :returns: Freqtrade-compatible dataframe.
        """
        result = df.copy()
        result = result.rename(columns={"timestamp": "date"})
        result["date"] = result["date"].dt.as_unit("ns")
        result["volume"] = 0.0
        return result[["date", "open", "high", "low", "close", "volume"]]

    def _transform_funding_rate(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform GMX funding rate dataframe to Freqtrade format.

        FreqTrade stores funding rate in the ``open`` column with other
        OHLCV columns set to 0.  Uses ``funding_rate_hourly`` as the
        rate value (falls back to ``funding_rate`` if hourly is missing).

        :param df: Funding rate dataframe from parquet.
        :returns: Freqtrade-compatible dataframe.
        """
        result = pd.DataFrame()
        result["date"] = df["timestamp"].dt.as_unit("ns")

        # Prefer hourly rate; fall back to raw rate
        if "funding_rate_hourly" in df.columns:
            result["open"] = df["funding_rate_hourly"].astype(float)
        else:
            result["open"] = df["funding_rate"].astype(float)

        result["high"] = 0.0
        result["low"] = 0.0
        result["close"] = 0.0
        result["volume"] = 0.0

        result = result.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)
        result = result.dropna(subset=["open"])
        return result

    def _transform_mark_price(self, df: pd.DataFrame) -> pd.DataFrame:
        """Generate mark price feather from OHLCV candle data.

        Uses OHLCV as a mark price proxy (GMX doesn't provide a separate
        mark price feed).

        :param df: GMX candle dataframe.
        :returns: Freqtrade-compatible mark price dataframe.
        """
        result = df.copy()
        result = result.rename(columns={"timestamp": "date"})
        result["date"] = result["date"].dt.as_unit("ns")
        result["volume"] = 0.0
        result = result[["date", "open", "high", "low", "close", "volume"]]
        return result.sort_values("date").drop_duplicates(subset=["date"]).reset_index(drop=True)

    # ------------------------------------------------------------------
    # File I/O
    # ------------------------------------------------------------------

    def _write(self, df: pd.DataFrame, path: Path, fmt: str) -> None:
        """Write dataframe in the requested format.

        :param df: Dataframe to write.
        :param path: Output file path.
        :param fmt: ``'feather'`` or ``'parquet'``.
        """
        if fmt == "feather":
            feather.write_feather(df, path)
        else:
            df.to_parquet(path, index=False)

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
