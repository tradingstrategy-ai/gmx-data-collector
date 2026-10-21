from datetime import UTC, datetime, timedelta

import pandas as pd

from gmx_historical_data.recent_candle_audit import audit_recent_candles


def test_audit_recent_candles_reports_only_breaks_inside_requested_window(tmp_path):
    path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
    pd.DataFrame(
        {
            "date": [
                datetime(2026, 10, 21, 0, tzinfo=UTC),
                datetime(2026, 10, 21, 1, tzinfo=UTC),
                datetime(2026, 10, 21, 3, tzinfo=UTC),
                datetime(2026, 10, 21, 4, tzinfo=UTC),
            ]
        }
    ).to_feather(path)

    breaks = audit_recent_candles(
        tmp_path,
        datetime(2026, 10, 21, 0, 30, tzinfo=UTC),
        datetime(2026, 10, 21, 4, tzinfo=UTC),
    )

    assert len(breaks) == 1
    assert breaks[0].before == datetime(2026, 10, 21, 1, tzinfo=UTC)
    assert breaks[0].after - breaks[0].before == timedelta(hours=2)


def test_audit_recent_candles_includes_gap_crossing_window_start(tmp_path):
    path = tmp_path / "BTC_USDC_USDC-1h-futures.feather"
    pd.DataFrame(
        {"date": [datetime(2026, 10, 21, 0, tzinfo=UTC), datetime(2026, 10, 21, 3, tzinfo=UTC)]}
    ).to_feather(path)

    breaks = audit_recent_candles(
        tmp_path,
        datetime(2026, 10, 21, 1, tzinfo=UTC),
        datetime(2026, 10, 21, 4, tzinfo=UTC),
    )

    assert len(breaks) == 1
    assert breaks[0].before == datetime(2026, 10, 21, 0, tzinfo=UTC)


def test_ohlcv_cli_default_audit_window_covers_missed_twice_daily_runs(tmp_path):
    from scripts.collect_gmx_ohlcv import build_parser

    args = build_parser().parse_args(["--output-dir", str(tmp_path)])

    assert args.window_hours == 160
