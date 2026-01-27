#!/usr/bin/env python3
"""Plot historical data to verify collection worked correctly."""

from pathlib import Path
from typing import Optional
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn
from rich.panel import Panel
from rich import box

from gmx_historical_data.storage import ParquetStorage
from gmx_historical_data.config import TIMEFRAMES, TIMEFRAME_TO_FILENAME


def normalize_timeframe(tf: str) -> str:
    """Normalize timeframe input to internal format.

    Accepts various formats and converts to internal pandas-compatible format.

    :param tf: Timeframe string (e.g., '1m', '1min', '1D', '1d')
    :return: Normalized timeframe (e.g., '1min', '1d')
    """
    # Map common user inputs to internal format
    mapping = {
        # Short format to pandas format
        "1m": "1min",
        "5m": "5min",
        "15m": "15min",
        # Uppercase D to lowercase
        "1D": "1d",
    }
    return mapping.get(tf, tf)

console = Console()


def get_available_symbols(storage: ParquetStorage) -> list[str]:
    """Get list of symbols with data in storage directory.

    Scans the candles directory to find which symbols have been collected.

    :param storage: ParquetStorage instance
    :return: List of symbol names
    """
    symbols = []
    candles_dir = storage.candles_dir

    if candles_dir.exists():
        # Each symbol has its own directory
        for symbol_dir in candles_dir.iterdir():
            if symbol_dir.is_dir():
                # Check if directory has any parquet files
                parquet_files = list(symbol_dir.glob("*.parquet"))
                if parquet_files:
                    symbols.append(symbol_dir.name)

    return sorted(symbols)


def plot_raw_events(
    storage: ParquetStorage,
    symbol: str,
    output_dir: Path,
) -> None:
    """Plot raw event data (tick data).

    :param storage: ParquetStorage instance
    :param symbol: Token symbol
    :param output_dir: Output directory for plots
    """
    console.print(f"  [dim]Loading raw events for {symbol}...[/dim]")
    df = storage.read_raw_events(symbol)

    if df.empty:
        console.print(f"  [yellow]No data found for {symbol}[/yellow]")
        return

    # Convert timestamp to datetime
    df["datetime"] = pd.to_datetime(df["timestamp"], unit="s")

    # Scale prices
    df["price_scaled"] = df["price"] / 1e8

    console.print(f"  [cyan]Found {len(df):,} events[/cyan]")
    console.print(
        f"  [dim]Date range: {df['datetime'].min()} to {df['datetime'].max()}[/dim]"
    )
    console.print(
        f"  [dim]Price range: ${df['price_scaled'].min():.2f} to ${df['price_scaled'].max():.2f}[/dim]"
    )

    # Plot
    fig, axes = plt.subplots(2, 1, figsize=(14, 10))

    # Price over time
    ax1 = axes[0]
    ax1.plot(df["datetime"], df["price_scaled"], "b-", linewidth=0.5, alpha=0.7)
    ax1.set_title(
        f"{symbol}/USD - Raw Event Data (Tick Data)", fontsize=14, fontweight="bold"
    )
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Price (USD)")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Price distribution
    ax2 = axes[1]
    ax2.hist(df["price_scaled"], bins=50, color="blue", alpha=0.7, edgecolor="black")
    ax2.set_title(f"{symbol}/USD - Price Distribution", fontsize=14, fontweight="bold")
    ax2.set_xlabel("Price (USD)")
    ax2.set_ylabel("Frequency")
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    output_path = output_dir / f"{symbol}_raw_events.png"
    plt.savefig(output_path, dpi=150)
    console.print(f"  [green]✓[/green] Saved: [dim]{output_path}[/dim]")
    plt.close()


def plot_candles(
    storage: ParquetStorage,
    symbol: str,
    timeframe: str,
    output_dir: Path,
) -> None:
    """Plot OHLCV candle data.

    :param storage: ParquetStorage instance
    :param symbol: Token symbol
    :param timeframe: Timeframe (e.g., '1h', '1D')
    :param output_dir: Output directory for plots
    """
    console.print(f"  [dim]Loading {timeframe} candles for {symbol}...[/dim]")
    df = storage.read_candles(timeframe, symbol)

    if df.empty:
        console.print(
            f"  [yellow]No candle data found for {symbol} at {timeframe}[/yellow]"
        )
        return

    console.print(f"  [cyan]Found {len(df):,} candles[/cyan]")
    console.print(
        f"  [dim]Date range: {df['timestamp'].min()} to {df['timestamp'].max()}[/dim]"
    )
    console.print(
        f"  [dim]Price range: ${df['low'].min():.2f} to ${df['high'].max():.2f}[/dim]"
    )

    # Plot candlestick-style (using OHLC bars)
    fig, axes = plt.subplots(3, 1, figsize=(14, 12))

    # OHLC price
    ax1 = axes[0]
    ax1.plot(df["timestamp"], df["open"], "g-", linewidth=1, alpha=0.5, label="Open")
    ax1.plot(df["timestamp"], df["high"], "b-", linewidth=1, alpha=0.5, label="High")
    ax1.plot(df["timestamp"], df["low"], "r-", linewidth=1, alpha=0.5, label="Low")
    ax1.plot(df["timestamp"], df["close"], "k-", linewidth=1.5, label="Close")
    ax1.set_title(f"{symbol}/USD - {timeframe} Candles", fontsize=14, fontweight="bold")
    ax1.set_ylabel("Price (USD)")
    ax1.legend(loc="best")
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Close price only (cleaner view)
    ax2 = axes[1]
    ax2.plot(df["timestamp"], df["close"], "b-", linewidth=1.5)
    ax2.fill_between(
        df["timestamp"],
        df["low"],
        df["high"],
        alpha=0.2,
        color="blue",
        label="High-Low Range",
    )
    ax2.set_title(
        f"{symbol}/USD - Close Price with High-Low Range",
        fontsize=14,
        fontweight="bold",
    )
    ax2.set_ylabel("Price (USD)")
    ax2.legend(loc="best")
    ax2.grid(True, alpha=0.3)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax2.xaxis.get_majorticklabels(), rotation=45, ha="right")

    # Returns (percentage change)
    ax3 = axes[2]
    returns = df["close"].pct_change() * 100
    ax3.plot(df["timestamp"], returns, "purple", linewidth=1)
    ax3.axhline(y=0, color="black", linestyle="-", linewidth=0.5)
    ax3.set_title(f"{symbol}/USD - Returns (%)", fontsize=14, fontweight="bold")
    ax3.set_xlabel("Date")
    ax3.set_ylabel("Return (%)")
    ax3.grid(True, alpha=0.3)
    ax3.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    output_path = output_dir / f"{symbol}_{timeframe}_candles.png"
    plt.savefig(output_path, dpi=150)
    console.print(f"  [green]✓[/green] Saved: [dim]{output_path}[/dim]")
    plt.close()


def plot_multiple_timeframes(
    storage: ParquetStorage,
    symbol: str,
    output_dir: Path,
) -> None:
    """Plot multiple timeframes on one chart.

    :param storage: ParquetStorage instance
    :param symbol: Token symbol
    :param output_dir: Output directory for plots
    """
    console.print(f"  [dim]Plotting multiple timeframes for {symbol}...[/dim]")

    fig, axes = plt.subplots(len(TIMEFRAMES), 1, figsize=(14, 3 * len(TIMEFRAMES)))

    for i, timeframe in enumerate(TIMEFRAMES):
        df = storage.read_candles(timeframe, symbol)

        if df.empty:
            continue

        ax = axes[i] if len(TIMEFRAMES) > 1 else axes
        ax.plot(df["timestamp"], df["close"], "b-", linewidth=1.5)
        ax.set_title(f"{symbol}/USD - {timeframe}", fontsize=12, fontweight="bold")
        ax.set_ylabel("Price (USD)")
        ax.grid(True, alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
        plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha="right")

    plt.tight_layout()
    output_path = output_dir / f"{symbol}_all_timeframes.png"
    plt.savefig(output_path, dpi=150)
    console.print(f"  [green]✓[/green] Saved: [dim]{output_path}[/dim]")
    plt.close()


def plot_symbol(
    storage: ParquetStorage,
    symbol: str,
    output_dir: Path,
    timeframe: Optional[str] = None,
    raw: bool = False,
) -> None:
    """Plot data for a single symbol.

    :param storage: ParquetStorage instance
    :param symbol: Token symbol
    :param output_dir: Output directory for plots
    :param timeframe: Specific timeframe to plot
    :param raw: Plot raw event data
    """
    console.print(f"\n[bold cyan]Plotting {symbol}...[/bold cyan]")

    # Plot raw events
    if raw:
        plot_raw_events(storage, symbol, output_dir)

    # Plot candles
    if timeframe:
        plot_candles(storage, symbol, timeframe, output_dir)
    else:
        # Plot all timeframes overview
        plot_multiple_timeframes(storage, symbol, output_dir)

        # Plot detailed candles for all timeframes
        for tf in TIMEFRAMES:
            plot_candles(storage, symbol, tf, output_dir)


def cli(
    symbol: Optional[str] = typer.Argument(
        None,
        help="Token symbol to plot (e.g., ETH, BTC). Omit to use --all.",
    ),
    data_dir: Path = typer.Option(
        Path("./data"),
        "--data-dir",
        help="Data directory",
    ),
    output_dir: Path = typer.Option(
        Path("./plots"),
        "--output-dir",
        help="Output directory for plots",
    ),
    timeframe: Optional[str] = typer.Option(
        None,
        "--timeframe",
        help="Specific timeframe to plot (default: all)",
    ),
    raw: bool = typer.Option(
        False,
        "--raw",
        help="Plot raw event data",
    ),
    all_symbols: bool = typer.Option(
        False,
        "--all",
        help="Plot all symbols found in data directory",
    ),
) -> None:
    """Plot GMX historical data from Parquet files.

    Examples:
        # Plot ETH with all timeframes
        plot-gmx-data ETH

        # Plot BTC 1h candles only
        plot-gmx-data BTC --timeframe 1h

        # Plot ETH raw tick data
        plot-gmx-data ETH --raw

        # Plot all collected symbols
        plot-gmx-data --all
    """
    # Validate arguments
    if not symbol and not all_symbols:
        console.print("[red]Error: Must specify either a symbol or use --all[/red]")
        raise typer.Exit(1)

    if symbol and all_symbols:
        console.print("[red]Error: Cannot specify both a symbol and --all[/red]")
        raise typer.Exit(1)

    # Normalize and validate timeframe
    if timeframe:
        timeframe = normalize_timeframe(timeframe)
        if timeframe not in TIMEFRAMES:
            # Show both internal and user-friendly formats
            user_formats = [TIMEFRAME_TO_FILENAME.get(tf, tf) for tf in TIMEFRAMES]
            console.print(
                f"[red]Error: Invalid timeframe '{timeframe}'. Must be one of: {', '.join(user_formats)}[/red]"
            )
            raise typer.Exit(1)

    # Create output directory
    output_dir.mkdir(parents=True, exist_ok=True)

    storage = ParquetStorage(data_dir)

    console.print()
    console.print(
        Panel(
            f"[bold]GMX Historical Data Plotter[/bold]\n"
            f"[dim]Data directory:[/dim] {data_dir}\n"
            f"[dim]Output directory:[/dim] {output_dir}",
            box=box.ROUNDED,
        )
    )

    if all_symbols:
        # Get all symbols that have been collected (scan data directory)
        symbols = get_available_symbols(storage)

        if not symbols:
            console.print("[yellow]No symbols found in data directory.[/yellow]")
            console.print(f"[dim]Looking in: {data_dir / 'candles' / 'arbitrum'}[/dim]")
            console.print("\n[bold]Run data collection first:[/bold]")
            console.print("  [cyan]poetry run gmx_historical_data --full[/cyan]")
            raise typer.Exit(1)

        console.print(f"\n[bold]Found [cyan]{len(symbols)}[/cyan] symbols with collected data[/bold]")
        console.print(f"[dim]Symbols: {', '.join(symbols[:10])}{' ...' if len(symbols) > 10 else ''}[/dim]")

        successful = 0
        failed = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Generating plots...", total=len(symbols))

            for sym in symbols:
                try:
                    plot_symbol(storage, sym, output_dir, timeframe, raw)
                    successful += 1
                except Exception as e:
                    console.print(f"[red]✗ Error plotting {sym}: {e}[/red]")
                    failed += 1

                progress.update(task, advance=1)

        console.print(
            f"\n[green]✓ Successfully plotted: {successful}/{len(symbols)}[/green]"
        )
        if failed > 0:
            console.print(f"[red]✗ Failed: {failed}/{len(symbols)}[/red]")

    else:
        # Plot single symbol
        symbol_upper = symbol.upper()
        plot_symbol(storage, symbol_upper, output_dir, timeframe, raw)

    console.print(f"\n[bold green]✓ Plotting complete![/bold green]")
    console.print(f"[dim]Plots saved to: {output_dir.absolute()}[/dim]")


def main() -> None:
    """Main entry point."""
    typer.run(cli)


if __name__ == "__main__":
    main()
