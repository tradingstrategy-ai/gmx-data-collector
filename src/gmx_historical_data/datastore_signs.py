"""Sign repair for DataStore hourly funding rows.

``rates/<SYM>/1h_datastore.parquet`` files written before #69 carry an inverted
``funding_fee_long`` on every negative-rate row (a negative rate multiplied by another
negative sign). ``funding_rate_hourly`` was always correctly signed (positive = longs
pay), so the fee and direction columns are re-derived from it and the stored values
are never trusted:

- ``funding_fee_long = rate``
- ``funding_fee_short = -rate``
- ``longs_pay_shorts = rate > 0`` (``False`` for exactly zero, as the extractor writes it)

A null rate leaves all three null.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from gmx_historical_data.atomic_parquet import atomic_write_parquet

#: Columns re-derived from the signed per-hour rate.
DERIVED_COLUMNS = ("longs_pay_shorts", "funding_fee_long", "funding_fee_short")


def resign_from_rate(df: pl.DataFrame) -> pl.DataFrame:
    """Recompute the fee and direction columns of DataStore rows from the signed rate.

    :param df: DataStore frame with ``funding_rate_hourly`` (or, failing that, the
        per-second ``funding_rate``). A frame with neither is returned unchanged.
    :returns: ``df`` with ``longs_pay_shorts``, ``funding_fee_long`` and
        ``funding_fee_short`` derived from the rate; other columns and column order
        are preserved (missing derived columns are appended).
    """
    if "funding_rate_hourly" in df.columns:
        rate = pl.col("funding_rate_hourly").cast(pl.Float64)
    elif "funding_rate" in df.columns:
        rate = pl.col("funding_rate").cast(pl.Float64) * 3600.0
    else:
        return df
    return df.with_columns(
        (rate > 0).alias("longs_pay_shorts"),
        rate.alias("funding_fee_long"),
        (-rate).alias("funding_fee_short"),
    )


def repair_datastore_file(path: Path) -> bool:
    """Rewrite a legacy ``1h_datastore.parquet`` in place when its derived columns are stale.

    The rewrite is atomic and happens only when a derived column differs, so a repaired
    file is never touched again.

    :param path: ``rates/<SYM>/1h_datastore.parquet``.
    :returns: ``True`` when the file was rewritten.
    """
    if not path.exists():
        return False
    df = pl.read_parquet(path)
    fixed = resign_from_rate(df)
    stale = any(c not in df.columns or not fixed[c].equals(df[c]) for c in DERIVED_COLUMNS)
    if not stale:
        return False
    atomic_write_parquet(fixed, path)
    return True
