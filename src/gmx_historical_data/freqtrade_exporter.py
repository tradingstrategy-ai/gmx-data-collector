"""Export GMX data to Freqtrade-compatible format.

Freqtrade expects OHLCV data with columns: date, open, high, low, close, volume
Files should be named: {SYMBOL}_USD-{timeframe}.feather
"""

from pathlib import Path

import pandas as pd
import pyarrow.feather as feather

from gmx_historical_data.storage import ParquetStorage


class FreqtradeExporter:
    """Export GMX candle data to Freqtrade format.

    :param data_dir: Source directory with GMX candle data
    :param output_dir: Output directory for Freqtrade files
    """

    def __init__(self, data_dir: Path, output_dir: Path):
        """Initialize exporter.

        :param data_dir: Source GMX data directory
        :param output_dir: Target directory for Freqtrade files
        """
        self.storage = ParquetStorage(data_dir)
        self.output_dir = Path(output_dir)

    def export(
        self,
        symbols: list[str] | None = None,
        timeframes: list[str] | None = None,
        output_format: str = "feather",
    ) -> dict[str, dict]:
        """Export GMX data to Freqtrade format.

        :param symbols: Specific symbols to export (default: all)
        :param timeframes: Specific timeframes to export (default: all)
        :param output_format: Output format ('feather' or 'parquet')
        :return: Dict mapping symbol to export stats
        """
        # Create output directory
        gmx_dir = self.output_dir / "gmx"
        gmx_dir.mkdir(parents=True, exist_ok=True)

        # Get symbols to export
        available_symbols = self.storage.list_symbols()
        if symbols:
            export_symbols = [s for s in symbols if s in available_symbols]
        else:
            export_symbols = available_symbols

        results = {}

        for symbol in export_symbols:
            # Get timeframes for this symbol
            available_tfs = self.storage.list_timeframes(symbol)
            if timeframes:
                export_tfs = [tf for tf in timeframes if tf in available_tfs]
            else:
                export_tfs = available_tfs

            files_exported = 0
            total_candles = 0

            for tf in export_tfs:
                # Read GMX data
                df = self.storage.read_candles(tf, symbol)
                if df.empty:
                    continue

                # Transform to Freqtrade format
                ft_df = self._transform_dataframe(df)

                # Generate filename
                filename = self._get_freqtrade_filename(symbol, tf, output_format)
                output_path = gmx_dir / filename

                # Save in requested format
                if output_format == "feather":
                    feather.write_feather(ft_df, output_path)
                else:
                    ft_df.to_parquet(output_path, index=False)

                files_exported += 1
                total_candles += len(ft_df)

            results[symbol] = {
                "files": files_exported,
                "candles": total_candles,
            }

        return results

    def _transform_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform GMX dataframe to Freqtrade format.

        Changes:
        - Rename 'timestamp' to 'date'
        - Add 'volume' column with 0.0 (GMX API doesn't provide volume)
        - Ensure column order: date, open, high, low, close, volume
        - Remove 'symbol' column

        :param df: GMX candle dataframe
        :return: Freqtrade-compatible dataframe
        """
        result = df.copy()

        # Rename timestamp to date
        result = result.rename(columns={"timestamp": "date"})

        # Add volume column (GMX doesn't provide volume data)
        result["volume"] = 0.0

        # Select and order columns for Freqtrade
        return result[["date", "open", "high", "low", "close", "volume"]]

    def _get_freqtrade_filename(
        self,
        symbol: str,
        timeframe: str,
        fmt: str,
    ) -> str:
        """Generate Freqtrade-compatible filename.

        Format: {SYMBOL}_USD-{timeframe}.{format}
        Example: ETH_USD-1h.feather

        :param symbol: Token symbol (e.g., 'ETH')
        :param timeframe: Timeframe (e.g., '1h')
        :param fmt: File format ('feather' or 'parquet')
        :return: Filename string
        """
        return f"{symbol}_USD-{timeframe}.{fmt}"
