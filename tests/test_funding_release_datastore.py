"""The daily gate with DataStore-sourced hours: tails, per-source counts, the variant file (#64, #47)."""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from gmx_historical_data import datastore_tail, funding_release
from tests._funding_release_fixtures import (
    HOUR,
    hours,
    write_checkpoint,
    write_companion,
    write_futures,
)

NOW = datetime(2026, 10, 18, 2, 20, tzinfo=UTC)
CUTOFF = datetime(2026, 10, 18, 1, 0, tzinfo=UTC)
EPOCH = datetime(2025, 8, 19, 12, tzinfo=UTC)
LAST_EVENT = CUTOFF - timedelta(hours=100)  # a quiet market: 100h behind the cutoff


def _canonical(symbol_dir: Path, *, datastore_tail_hours: int, gap_filled: int = 0) -> None:
    """Canonical 1h.parquet: event hours to LAST_EVENT, then ``datastore_tail_hours`` of reads."""
    event_stamps = hours(EPOCH, int((LAST_EVENT - EPOCH) / HOUR) + 1)
    ds_stamps = hours(LAST_EVENT + HOUR, datastore_tail_hours)
    n_e, n_d = len(event_stamps), len(ds_stamps)
    gap_flags = [i >= n_e - gap_filled for i in range(n_e)]
    frame = pl.DataFrame(
        {
            "timestamp": event_stamps + ds_stamps,
            "funding_rate_hourly": [1e-6] * n_e + [-2e-6] * n_d,
            "longs_pay_shorts": [True] * n_e + [False] * n_d,
            "source": ["hypersync"] * n_e + ["datastore"] * n_d,
            "is_gap_filled": gap_flags + [False] * n_d,
            "market": ["0xm"] * (n_e + n_d),
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    )
    symbol_dir.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(symbol_dir / "1h.parquet")
    if n_d:
        frame.filter(pl.col("source") == "datastore").select(
            "timestamp",
            "funding_rate_hourly",
            "longs_pay_shorts",
            "source",
            "is_gap_filled",
            "market",
        ).write_parquet(symbol_dir / "1h_datastore.parquet")


def _lake(tmp_path: Path, *, datastore_tail_hours: int, gap_filled: int = 0) -> Path:
    gmx_root = tmp_path / "gmx"
    futures = gmx_root / "futures"
    write_futures(
        futures,
        "MOO",
        datetime(2026, 1, 1, tzinfo=UTC),
        int((CUTOFF - datetime(2026, 1, 1, tzinfo=UTC)) / HOUR) + 1,
    )
    _canonical(
        gmx_root / "funding" / "arbitrum" / "rates" / "MOO",
        datastore_tail_hours=datastore_tail_hours,
        gap_filled=gap_filled,
    )
    # A liquid market, current on events alone: keeps the set-level head check (B4) satisfied
    # so these tests isolate what the quiet market's tail-fill does.
    n_btc = int((CUTOFF - EPOCH) / HOUR) + 1
    write_futures(
        futures,
        "BTC",
        datetime(2026, 1, 1, tzinfo=UTC),
        int((CUTOFF - datetime(2026, 1, 1, tzinfo=UTC)) / HOUR) + 1,
    )
    btc = gmx_root / "funding" / "arbitrum" / "rates" / "BTC"
    btc.mkdir(parents=True)
    pl.DataFrame(
        {
            "timestamp": hours(EPOCH, n_btc),
            "funding_rate_hourly": [1e-6] * n_btc,
            "longs_pay_shorts": [True] * n_btc,
            "source": ["hypersync"] * n_btc,
            "is_gap_filled": [False] * n_btc,
            "market": ["0xbtc"] * n_btc,
        },
        schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
    ).write_parquet(btc / "1h.parquet")
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    write_checkpoint(cp, "funding_factor_checkpoint.json", "funding_factor_all", NOW)
    write_checkpoint(cp, "fee_per_size_checkpoint.json", "fee_per_size_all", NOW)
    return gmx_root


def _gate(gmx_root: Path, stage: Path, report: Path | None = None):
    return funding_release.run_gate(
        gmx_root, stage, state="ready", extract_outcome="success", now=NOW, report_path=report
    )


def test_a_datastore_extended_tail_is_current(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=100)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    pair = coverage["pairs"]["MOO"]
    assert coverage["status"] == "PASS"
    assert pair["tail"] == "CURRENT"
    assert pair["last_bar"] == CUTOFF.isoformat()
    assert pair["tail_age_hours"] == 0
    assert coverage["summary"]["current"] == 2


def test_the_same_market_without_the_tail_fill_is_still_stale(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=0)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["pairs"]["MOO"]["tail"] == "STALE"
    assert coverage["pairs"]["MOO"]["tail_age_hours"] == 100


def test_the_tail_age_follows_the_last_bar_whatever_its_source(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=70)  # reads reach 30h short of the cutoff
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    pair = _gate(gmx_root, tmp_path / "stage")["pairs"]["MOO"]

    assert pair["tail"] == "LAGGING" and pair["tail_age_hours"] == 30


def test_hours_by_source_are_counted_per_pair(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=100, gap_filled=7)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    pair = _gate(gmx_root, tmp_path / "stage")["pairs"]["MOO"]

    events = int((LAST_EVENT - EPOCH) / HOUR) + 1
    assert pair["source_hours"] == {
        "hypersync": events - 7,
        "datastore": 100,
        "gap_filled": 7,
        "other": 0,
    }


def test_the_report_shows_the_per_source_counts_and_the_tail_status(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=100, gap_filled=7)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    datastore_tail.write_tail_status(cp, status="ok", hours_requested=100, hours_read=100, now=NOW)
    report = tmp_path / "data_report.txt"

    coverage = _gate(gmx_root, tmp_path / "stage", report)

    assert coverage["datastore_tail"] == "ok (100 hour(s) read)"
    text = report.read_text()
    assert "datastore_tail: ok (100 hour(s) read)" in text
    assert "Hours by source per pair (hypersync / datastore / gap_filled):" in text
    events = int((LAST_EVENT - EPOCH) / HOUR) + 1
    assert f"MOO: {events - 7} / 100 / 7" in text
    notes = funding_release.format_release_notes(coverage)
    assert "DataStore tail-fill: ok (100 hour(s) read)" in notes
    assert "100 chain-state reads (datastore)" in notes


def test_an_unavailable_archive_is_recorded_in_coverage_and_report_and_does_not_block(
    tmp_path: Path,
) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=0)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    datastore_tail.write_tail_status(
        cp, status="unavailable", reason="missing trie node 0xabc", hours_requested=100, now=NOW
    )
    report = tmp_path / "data_report.txt"

    coverage = _gate(gmx_root, tmp_path / "stage", report)

    assert coverage["datastore_tail"] == "unavailable (missing trie node 0xabc)"
    assert "datastore_tail: unavailable (missing trie node 0xabc)" in report.read_text()
    assert coverage["status"] == "PASS"  # the release continues
    assert coverage["pairs"]["MOO"]["tail"] == "STALE"  # flagged exactly as before
    on_disk = json.loads((gmx_root / "funding_coverage.json").read_text())
    assert on_disk["datastore_tail"] == "unavailable (missing trie node 0xabc)"


def test_a_withheld_verdict_carries_the_tail_status_too(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=0)
    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    datastore_tail.write_tail_status(cp, status="failed", reason="boom", now=NOW)

    coverage = funding_release.write_withheld_verdict(
        gmx_root, "gate crashed", state="ready", extract_outcome="success", now=NOW
    )

    assert coverage["datastore_tail"] == "failed (boom)"


def test_no_status_file_reads_as_not_run(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=0)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    assert _gate(gmx_root, tmp_path / "stage")["datastore_tail"] == "not run"


# ---------------------------------------------------------------------------
# The 1h_datastore variant through stage -> gate -> promote
# ---------------------------------------------------------------------------

VARIANT = "MOO_USDC_USDC-1h_datastore-funding_rate.feather"


def test_the_variant_is_staged_gated_and_promoted_with_signs_preserved(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=100)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    staged = pl.read_ipc(tmp_path / "stage" / "gmx" / "futures" / VARIANT)
    assert staged["open"].to_list() == [-2e-6] * 100
    assert staged["date"].min() == LAST_EVENT + HOUR

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "PASS"
    shipped = pl.read_ipc(gmx_root / "futures" / VARIANT)
    assert shipped["open"].to_list() == [-2e-6] * 100
    pair = coverage["pairs"]["MOO"]
    assert pair["datastore_rows"] == 100
    assert pair["datastore_last_bar"] == CUTOFF.isoformat()


def test_a_previously_published_variant_is_kept_when_the_source_has_less(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=10)
    older = hours(datetime(2024, 1, 1, tzinfo=UTC), 24)
    write_companion(
        gmx_root / "futures", "MOO", "funding_rate", older, value=-0.5, tf="1h_datastore"
    )

    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    staged = pl.read_ipc(tmp_path / "stage" / "gmx" / "futures" / VARIANT)
    assert staged["date"].min() == older[0]
    assert staged["date"].max() == LAST_EVENT + 10 * HOUR
    assert len(staged) == 24 + 10


def test_no_datastore_source_means_no_variant_and_no_blocker(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=0)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "PASS"
    assert not (gmx_root / "futures" / VARIANT).exists()
    assert coverage["pairs"]["MOO"]["datastore_rows"] == 0


def test_a_non_finite_value_in_the_variant_blocks_the_companions(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=10)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    staged_path = tmp_path / "stage" / "gmx" / "futures" / VARIANT
    frame = pl.read_ipc(staged_path, memory_map=False)
    frame.with_columns(
        pl.when(pl.col("date") == LAST_EVENT + HOUR)
        .then(float("nan"))
        .otherwise(pl.col("open"))
        .alias("open")
    ).write_ipc(staged_path)

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "WITHHELD"
    assert any("B8" in r and "non-finite" in r for r in coverage["reasons"])
    assert not (gmx_root / "futures" / VARIANT).exists()


def test_a_corrupt_variant_blocks_instead_of_shipping(tmp_path: Path) -> None:
    gmx_root = _lake(tmp_path, datastore_tail_hours=10)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")
    (tmp_path / "stage" / "gmx" / "futures" / VARIANT).write_bytes(b"not a feather file")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "WITHHELD"
    assert any("B8" in r for r in coverage["reasons"])


def test_the_variant_has_no_hole_or_tail_rule_because_it_is_sparse_by_design(
    tmp_path: Path,
) -> None:
    """Backfilled history + recent reads leave a middle the canonical 1h covers; not a defect."""
    gmx_root = _lake(tmp_path, datastore_tail_hours=10)
    ds_path = gmx_root / "funding" / "arbitrum" / "rates" / "MOO" / "1h_datastore.parquet"
    old = hours(datetime(2024, 3, 1, tzinfo=UTC), 24)
    pl.concat(
        [
            pl.read_parquet(ds_path),
            pl.DataFrame(
                {
                    "timestamp": old,
                    "funding_rate_hourly": [3e-6] * 24,
                    "longs_pay_shorts": [True] * 24,
                    "source": ["datastore"] * 24,
                    "is_gap_filled": [False] * 24,
                    "market": ["0xm"] * 24,
                },
                schema_overrides={"timestamp": pl.Datetime("ns", "UTC")},
            ),
        ]
    ).sort("timestamp").write_parquet(ds_path)
    funding_release.stage_companions(gmx_root, tmp_path / "stage")

    coverage = _gate(gmx_root, tmp_path / "stage")

    assert coverage["status"] == "PASS"
    assert coverage["pairs"]["MOO"]["datastore_rows"] == 34
