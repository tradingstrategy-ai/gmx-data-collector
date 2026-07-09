"""Subprocess wrapper for ``./freqtrade-gmx download-data``.

This is the only network boundary in the gap-fill stage. Freqtrade (inside its
own venv) handles ccxt, rate limits, and retries.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


class CEXDownloadError(RuntimeError):
    """Raised when ``freqtrade download-data`` fails for an exchange call."""


#: GMX/pandas-style timeframe tokens -> freqtrade/ccxt tokens.
#: Hour/day tokens are identical in both styles and pass through unchanged.
_GMX_TF_TO_FREQTRADE: dict[str, str] = {
    "1min": "1m",
    "5min": "5m",
    "15min": "15m",
}


def to_freqtrade_timeframe(tf: str) -> str:
    """Translate a GMX-style timeframe (``"1min"``) to freqtrade style (``"1m"``).

    Tokens already valid for freqtrade (``"1h"``, ``"4h"``, ``"1d"``) pass through.
    Freqtrade exits 0 even on invalid-timeframe configuration errors, so this
    translation is the only guard against silently downloading nothing.
    """
    return _GMX_TF_TO_FREQTRADE.get(tf, tf)


def build_download_argv(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    timerange_start: str,
    datadir: Path | None,
    trading_mode: str = "futures",
) -> list[str]:
    """Construct the subprocess argv for one ``./freqtrade-gmx download-data`` call.

    :param exchange: Exchange name, e.g. ``"binance"``.
    :param pairs: Freqtrade pair strings, e.g. ``["BTC/USDT:USDT"]``.
    :param timeframes: GMX-style timeframe strings, e.g. ``["1h", "4h"]``;
        translated to freqtrade/ccxt tokens before being passed on.
    :param timerange_start: Start date as ``"YYYYMMDD"``. Becomes ``--timerange
        YYYYMMDD-`` so freqtrade fetches forward to the present.
    :param datadir: Override freqtrade data dir. If ``None``, the flag is omitted
        and freqtrade uses its own default.
    :param trading_mode: ``"futures"`` or ``"spot"``.
    :returns: Argv list ready for :func:`subprocess.run`.
    """
    argv: list[str] = [
        "./freqtrade-gmx",
        "download-data",
        "--exchange",
        exchange,
        "--pairs",
        *pairs,
        "--timeframes",
        *[to_freqtrade_timeframe(tf) for tf in timeframes],
        "--timerange",
        f"{timerange_start}-",
        "--data-format-ohlcv",
        "feather",
        "--trading-mode",
        trading_mode,
    ]
    if datadir is not None:
        argv += ["--datadir", str(datadir)]
    return argv


def run_download(
    exchange: str,
    pairs: list[str],
    timeframes: list[str],
    timerange_start: str,
    datadir: Path | None,
    cwd: Path,
    timeout: int = 1800,
) -> subprocess.CompletedProcess[bytes]:
    """Invoke ``./freqtrade-gmx download-data`` for one exchange.

    :param cwd: Working directory where ``./freqtrade-gmx`` lives.
    :param timeout: Seconds before the subprocess is killed.
    :raises CEXDownloadError: On non-zero exit or timeout.
    """
    argv = build_download_argv(
        exchange=exchange,
        pairs=pairs,
        timeframes=timeframes,
        timerange_start=timerange_start,
        datadir=datadir,
    )
    try:
        result = subprocess.run(argv, cwd=cwd, timeout=timeout, capture_output=True)
    except subprocess.TimeoutExpired as err:
        raise CEXDownloadError(
            f"freqtrade download-data timed out after {timeout}s for {exchange}"
        ) from err
    if result.returncode != 0:
        tail = result.stderr.decode(errors="replace").strip().splitlines()[-10:]
        raise CEXDownloadError(
            f"freqtrade download-data exit={result.returncode} for {exchange}: " + "\n".join(tail)
        )
    return result


def resolve_feather_path(datadir: Path, exchange: str, pair: str, timeframe: str) -> Path:
    """Build the expected freqtrade feather path for a given ``(exchange, pair, tf)``.

    Freqtrade naming: ``{BASE}_{QUOTE}_{SETTLE}-{tf}-futures.feather``.

    :param datadir: Freqtrade data directory root.
    :param exchange: Exchange name, e.g. ``"binance"``.
    :param pair: Freqtrade pair, e.g. ``"BTC/USDT:USDT"``.
    :param timeframe: GMX-style timeframe string, e.g. ``"1h"`` or ``"1min"``;
        translated to the freqtrade/ccxt token freqtrade actually names files with.
    :returns: Absolute path to the expected feather file.
    """
    base, rest = pair.split("/", 1)
    quote, settle = rest.split(":", 1)
    fname = f"{base}_{quote}_{settle}-{to_freqtrade_timeframe(timeframe)}-futures.feather"
    return datadir / exchange / "futures" / fname
