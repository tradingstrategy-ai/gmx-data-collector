"""Tests for the fill-gaps-cex typer command."""

from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from gmx_historical_data.cli import app


def test_fill_gaps_cex_invokes_orchestrator(tmp_path: Path):
    runner = CliRunner()
    with patch("gmx_historical_data.cli.fill_gaps_from_cex") as mock:
        mock.return_value = None
        result = runner.invoke(
            app,
            [
                "fill-gaps-cex",
                "--data-dir",
                str(tmp_path),
                "--symbol",
                "BTC,ETH",
                "--timeframe",
                "1h",
                "--gap-threshold",
                "0.25",
                "--merge-gap-bars",
                "3",
                "--exchanges",
                "binance,bybit",
                "--routing-file",
                str(tmp_path / "r.json"),
                "--log-dir",
                str(tmp_path / "logs"),
                "--dry-run",
            ],
        )
        assert result.exit_code == 0, result.output
        assert mock.called
        kwargs = mock.call_args.kwargs
        assert kwargs["symbols"] == ["BTC", "ETH"]
        assert kwargs["timeframes"] == ["1h"]
        assert kwargs["gap_threshold"] == 0.25
        assert kwargs["merge_gap_bars"] == 3
        assert kwargs["exchanges"] == ["binance", "bybit"]
        assert kwargs["dry_run"] is True


def test_fill_gaps_cex_empty_symbol_passes_none(tmp_path: Path):
    runner = CliRunner()
    with patch("gmx_historical_data.cli.fill_gaps_from_cex") as mock:
        mock.return_value = None
        result = runner.invoke(
            app,
            [
                "fill-gaps-cex",
                "--data-dir",
                str(tmp_path),
                "--routing-file",
                str(tmp_path / "r.json"),
                "--log-dir",
                str(tmp_path / "logs"),
            ],
        )
        assert result.exit_code == 0, result.output
        kwargs = mock.call_args.kwargs
        assert kwargs["symbols"] is None
        assert kwargs["timeframes"] is None
