"""The Freqtrade funding export carries the funding direction as the rate's sign.

GMX funding rates are stored as magnitudes (``funding_rate_hourly`` is never
negative) with the direction in ``longs_pay_shorts``. Freqtrade reads only the
exported ``open`` column and treats a positive rate as longs paying shorts, so
an unsigned export charges longs and pays shorts in every hour, including the
hours where shorts actually pay. These tests pin the signing in
:meth:`FreqtradeExporter._transform_funding_rate`:

- known direction: ``+|rate|`` when longs pay, ``-|rate|`` when shorts pay;
- unknown direction (``null``, before the first observation): value unchanged;
- no ``longs_pay_shorts`` column (already-signed sources): value unchanged;
- the ``funding_rate`` fallback column is signed the same way.
"""

from datetime import UTC, datetime

import polars as pl

from gmx_historical_data.freqtrade_exporter import FreqtradeExporter


def _hours(n: int) -> pl.Series:
    """Return ``n`` consecutive hourly UTC timestamps starting 2026-01-01."""
    return pl.datetime_range(
        datetime(2026, 1, 1, tzinfo=UTC),
        datetime(2026, 1, 1, n, tzinfo=UTC),
        interval="1h",
        time_zone="UTC",
        eager=True,
        closed="left",
    )


def test_rate_is_signed_by_direction(tmp_path):
    """Shorts-pay hours export negative, longs-pay hours positive."""
    df = pl.DataFrame(
        {
            "timestamp": _hours(4),
            "funding_rate_hourly": [1e-6, 2e-6, 3e-6, 4e-6],
            "longs_pay_shorts": [True, False, True, False],
        }
    )

    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(df)

    assert out["open"].to_list() == [1e-6, -2e-6, 3e-6, -4e-6]
    assert out.columns == ["date", "open", "high", "low", "close", "volume"]


def test_unknown_direction_keeps_the_value(tmp_path):
    """Hours before the first direction observation are left as stored."""
    df = pl.DataFrame(
        {
            "timestamp": _hours(3),
            "funding_rate_hourly": [1e-6, 2e-6, 3e-6],
            "longs_pay_shorts": pl.Series([None, None, False], dtype=pl.Boolean),
        }
    )

    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(df)

    assert out["open"].to_list() == [1e-6, 2e-6, -3e-6]


def test_without_direction_column_values_pass_through(tmp_path):
    """Sources that are already signed (no direction column) are not changed."""
    df = pl.DataFrame(
        {
            "timestamp": _hours(3),
            "funding_rate_hourly": [1e-6, -2e-6, 3e-6],
        }
    )

    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(df)

    assert out["open"].to_list() == [1e-6, -2e-6, 3e-6]


def test_direction_decides_the_sign_even_if_the_rate_is_signed(tmp_path):
    """The magnitude is signed by the flag, so a signed input is never flipped twice."""
    df = pl.DataFrame(
        {
            "timestamp": _hours(2),
            "funding_rate_hourly": [-2e-6, -3e-6],
            "longs_pay_shorts": [False, True],
        }
    )

    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(df)

    assert out["open"].to_list() == [-2e-6, 3e-6]


def test_funding_rate_fallback_is_signed(tmp_path):
    """When ``funding_rate_hourly`` is absent, ``funding_rate`` is signed the same way."""
    df = pl.DataFrame(
        {
            "timestamp": _hours(2),
            "funding_rate": [2.8e-10, 5.6e-10],
            "longs_pay_shorts": [True, False],
        }
    )

    out = FreqtradeExporter(tmp_path, tmp_path)._transform_funding_rate(df)

    assert out["open"].to_list() == [2.8e-10, -5.6e-10]


def test_export_funding_writes_signed_rates(tmp_path):
    """End to end: the written ``-1h-funding_rate`` feather carries the sign."""
    data_dir = tmp_path / "data"
    rates_dir = data_dir / "funding" / "arbitrum" / "rates" / "ADA"
    rates_dir.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": _hours(3),
            "funding_rate": [1e-10, 2e-10, 3e-10],
            "funding_rate_hourly": [3.6e-7, 7.2e-7, 1.08e-6],
            "longs_pay_shorts": [True, False, False],
        }
    ).write_parquet(rates_dir / "1h.parquet")

    FreqtradeExporter(data_dir, tmp_path / "output").export_funding(
        symbols=["ADA"], timeframes=["1h"]
    )

    written = pl.read_ipc(
        tmp_path / "output" / "gmx" / "futures" / "ADA_USDC_USDC-1h-funding_rate.feather"
    )
    assert written["open"].to_list() == [3.6e-7, -7.2e-7, -1.08e-6]
