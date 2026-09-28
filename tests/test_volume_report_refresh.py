"""The shipped report must never print a made-up volume of zero.

``generate_report`` runs before the workflow's separate "Collect 24h volume
snapshot" step, so ``main()`` always calls it with ``volume_count=0,
volume_data={}`` (see the comment at its call site) -- every shipped
``data_report.txt`` said "Volume entries: 0 markets" / "Total 24h Volume:
N/A" even on days ~101 markets' worth of volume was saved to
``volumes/{date}.parquet`` moments later.

``refresh_volume_report_section`` is called once that later step has run, to
patch the already-written report with what was actually collected.
"""

from decimal import Decimal

import pandas as pd

from scripts.collect_daily_snapshot import (
    load_volume_snapshot,
    refresh_volume_report_section,
)


def _write_volumes(volumes_dir, date_str: str, rows: dict[str, str]) -> None:
    volumes_dir.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(
        [
            {"date": date_str, "market_address": addr, "volume_usd": vol}
            for addr, vol in rows.items()
        ]
    )
    df.to_parquet(volumes_dir / f"{date_str}.parquet", index=False)


class TestLoadVolumeSnapshot:
    def test_missing_file_is_not_found(self, tmp_path):
        snapshot = load_volume_snapshot(tmp_path / "volumes", "2026-09-27")

        assert snapshot.found is False
        assert snapshot.count == 0
        assert snapshot.by_market == {}

    def test_reads_real_file(self, tmp_path):
        volumes_dir = tmp_path / "volumes"
        _write_volumes(volumes_dir, "2026-09-27", {"0xAAA": "1000.50", "0xBBB": "2500"})

        snapshot = load_volume_snapshot(volumes_dir, "2026-09-27")

        assert snapshot.found is True
        assert snapshot.count == 2
        assert snapshot.by_market["0xaaa"] == Decimal("1000.50")
        assert snapshot.by_market["0xbbb"] == Decimal("2500")

    def test_corrupt_file_is_not_found(self, tmp_path):
        volumes_dir = tmp_path / "volumes"
        volumes_dir.mkdir(parents=True)
        (volumes_dir / "2026-09-27.parquet").write_bytes(b"not a parquet file")

        snapshot = load_volume_snapshot(volumes_dir, "2026-09-27")

        assert snapshot.found is False


_REPORT_TEMPLATE = """# GMX Data Report — {date}
Generated: 2026-09-27 02:00:00 UTC

## Collection Summary
- Snapshot date: {date}
- Total markets from API: 2
- Volume entries: 0 markets
- Total 24h Volume: N/A

## Date Range Summary
- Volumes: No data available
- OHLCV 1h: 2026-01-01 to {date} (2 symbols)

## Data Files
- Snapshot parquet files: 1 days
- Volume parquet files: 0 days

## Top 10 Markets by Open Interest
   1. BTC/USD                                  $      1,000,000

## OHLCV Coverage — 1h (API slice vs Combined feather)
  symbol      status       API slice                         Combined                          hist depth
"""


class TestRefreshVolumeReportSection:
    def test_missing_report_does_not_crash(self, tmp_path):
        refresh_volume_report_section(
            report_path=tmp_path / "nope.txt",
            volumes_dir=tmp_path / "volumes",
            snapshots_dir=tmp_path / "snapshots",
            date_str="2026-09-27",
        )
        assert not (tmp_path / "nope.txt").exists()

    def test_volume_absent_reports_explicit_missing_text_not_a_fabricated_zero(self, tmp_path):
        date_str = "2026-09-27"
        report_path = tmp_path / "data_report.txt"
        report_path.write_text(_REPORT_TEMPLATE.format(date=date_str), encoding="utf-8")

        refresh_volume_report_section(
            report_path=report_path,
            volumes_dir=tmp_path / "volumes",
            snapshots_dir=tmp_path / "snapshots",
            date_str=date_str,
        )

        content = report_path.read_text()
        assert "- Volume entries: 0 markets" not in content
        assert f"not collected (volumes/{date_str}.parquet missing)" in content

    def test_volume_present_replaces_fabricated_fields_with_real_numbers(self, tmp_path):
        date_str = "2026-09-27"
        report_path = tmp_path / "data_report.txt"
        report_path.write_text(_REPORT_TEMPLATE.format(date=date_str), encoding="utf-8")

        volumes_dir = tmp_path / "volumes"
        _write_volumes(volumes_dir, date_str, {"0xAAA": "1500000", "0xBBB": "2500000"})

        snapshots_dir = tmp_path / "snapshots"
        snapshots_dir.mkdir(parents=True)
        pd.DataFrame(
            {
                "market_token": ["0xAAA", "0xBBB"],
                "name": ["FOO/USD", "BAR/USD"],
            }
        ).to_parquet(snapshots_dir / f"{date_str}.parquet", index=False)

        refresh_volume_report_section(
            report_path=report_path,
            volumes_dir=volumes_dir,
            snapshots_dir=snapshots_dir,
            date_str=date_str,
        )

        content = report_path.read_text()
        assert "- Volume entries: 2 markets" in content
        assert "- Total 24h Volume: $4,000,000" in content
        assert "- Volume parquet files: 1 days" in content
        assert f"- Volumes: {date_str} to {date_str} (1 days)" in content
        assert "## Top 10 Markets by 24h Volume" in content
        assert "BAR/USD" in content and "FOO/USD" in content
        # The row order must be by descending volume.
        assert content.index("BAR/USD") < content.index("FOO/USD")
        # The rest of the report is untouched.
        assert "## Top 10 Markets by Open Interest" in content
        assert "## OHLCV Coverage — 1h" in content

    def test_missing_snapshot_file_falls_back_to_raw_address(self, tmp_path):
        date_str = "2026-09-27"
        report_path = tmp_path / "data_report.txt"
        report_path.write_text(_REPORT_TEMPLATE.format(date=date_str), encoding="utf-8")

        volumes_dir = tmp_path / "volumes"
        _write_volumes(volumes_dir, date_str, {"0xAAAAAAAAAAAAAAAA": "1000"})

        refresh_volume_report_section(
            report_path=report_path,
            volumes_dir=volumes_dir,
            snapshots_dir=tmp_path / "snapshots",
            date_str=date_str,
        )

        content = report_path.read_text()
        assert "## Top 10 Markets by 24h Volume" in content
        assert "0xaaaaaaaa..." in content

    def test_zero_market_volume_file_reports_zero_not_missing(self, tmp_path):
        """An empty-but-present file is a real "$0 collected today", distinct
        from "not collected at all"."""
        date_str = "2026-09-27"
        report_path = tmp_path / "data_report.txt"
        report_path.write_text(_REPORT_TEMPLATE.format(date=date_str), encoding="utf-8")

        volumes_dir = tmp_path / "volumes"
        _write_volumes(volumes_dir, date_str, {})

        refresh_volume_report_section(
            report_path=report_path,
            volumes_dir=volumes_dir,
            snapshots_dir=tmp_path / "snapshots",
            date_str=date_str,
        )

        content = report_path.read_text()
        assert "not collected" not in content
        assert "- Volume entries: 0 markets" in content
        assert "- Total 24h Volume: $0" in content
