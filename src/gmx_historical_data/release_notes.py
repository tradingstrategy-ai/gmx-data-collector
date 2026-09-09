"""Build the per-asset OHLCV coverage table for release notes.

The GitHub release body previously only stated a file count. Consumers
deciding whether to pull a release need the same thing the Apex sibling
project publishes: a per-asset, per-timeframe ``From``/``To``/``Candles``
table (see ``Ankvik-Tech-Labs/Apex-Historical-Data`` release notes). This
module builds that table from the futures feathers about to ship, so the
release workflow can append it to the notes it already writes.
"""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

EXIT_OK = 0
EXIT_ERROR = 1

_TIMEFRAMES = ("1m", "5m", "15m", "1h", "4h", "1d")
_CANDLE_FILE = re.compile(
    r"^(?P<asset>.+)-(?P<timeframe>" + "|".join(_TIMEFRAMES) + r")-futures\.feather$"
)


@dataclass(frozen=True, slots=True)
class CoverageRow:
    """One asset/timeframe line in the coverage table.

    :param asset: Feather stem before ``-{timeframe}-futures``, e.g. ``BTC_USDC_USDC``.
    :param timeframe: One of ``1m``, ``5m``, ``15m``, ``1h``, ``4h``, ``1d``.
    :param first: Earliest candle date, ``YYYY-MM-DD``.
    :param last: Latest candle date, ``YYYY-MM-DD``.
    :param candles: Row count in the feather file.
    """

    asset: str
    timeframe: str
    first: str
    last: str
    candles: int


def collect_coverage_rows(futures_dir: Path) -> list[CoverageRow]:
    """Read every candle feather in ``futures_dir`` into a :class:`CoverageRow`.

    :param futures_dir: Directory holding ``{asset}-{timeframe}-futures.feather``
        files (and non-candle siblings like ``_cadence_manifest.json``, which
        are ignored).
    :returns: One row per non-empty candle file, unsorted.
    """
    if not futures_dir.is_dir():
        return []

    rows: list[CoverageRow] = []
    for path in sorted(futures_dir.iterdir()):
        match = _CANDLE_FILE.match(path.name)
        if not match:
            continue
        try:
            dates = pd.read_feather(path, columns=["date"])["date"]
        except Exception:
            continue
        if dates.empty:
            continue
        dates = pd.to_datetime(dates, utc=True)
        rows.append(
            CoverageRow(
                asset=match.group("asset"),
                timeframe=match.group("timeframe"),
                first=dates.min().strftime("%Y-%m-%d"),
                last=dates.max().strftime("%Y-%m-%d"),
                candles=int(len(dates)),
            )
        )
    return rows


def render_coverage_table(rows: Sequence[CoverageRow]) -> str:
    """Render a ``## Coverage`` markdown section, Apex-release style.

    :param rows: Rows from :func:`collect_coverage_rows`.
    :returns: Markdown block; a placeholder line when ``rows`` is empty
        rather than a header over an empty table.
    """
    if not rows:
        return "## Coverage\n\n(no futures candle data in this release)\n"

    lines = [
        "## Coverage",
        "",
        "| Asset | Timeframe | From | To | Candles |",
        "|---|---|---|---|---:|",
    ]
    for row in sorted(rows, key=lambda r: (r.asset, r.timeframe)):
        lines.append(
            f"| {row.asset} | {row.timeframe} | {row.first} | {row.last} | {row.candles:,} |"
        )
    lines.append("")
    return "\n".join(lines)


def _command_build(args: argparse.Namespace) -> int:
    rows = collect_coverage_rows(args.futures_dir)
    table = render_coverage_table(rows)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(table, encoding="utf-8")
    print(f"Coverage table: {len(rows)} asset/timeframe rows -> {args.out}")
    return EXIT_OK


def main(argv: Sequence[str] | None = None) -> int:
    """Entry point for ``python -m gmx_historical_data.release_notes``.

    :param argv: Argument vector; defaults to ``sys.argv[1:]``.
    :returns: Process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    build = sub.add_parser("build", help="build the coverage table from futures feathers")
    build.add_argument("--futures-dir", type=Path, required=True)
    build.add_argument("--out", type=Path, required=True)
    build.set_defaults(func=_command_build)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":  # pragma: no cover - exercised via the workflow
    raise SystemExit(main())
