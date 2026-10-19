"""Seeding the funding state from the ``data/full-history`` branch layout."""

import json
import re
from pathlib import Path

import pytest

from gmx_historical_data.funding_state import (
    FACTOR_CHECKPOINT,
    FEE_CHECKPOINT,
    FundingState,
    SeedError,
    assess_funding_state,
    seed_funding_state,
)
from tests._funding_release_fixtures import write_seed_source

REPO = Path(__file__).resolve().parent.parent


def test_seed_copies_tree_and_synthesizes_checkpoints_from_raw(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    gmx_root = tmp_path / "gmx"

    result = seed_funding_state(source, gmx_root, source_info={"ref": "data/full-history"})

    net = gmx_root / "funding" / "arbitrum"
    assert result.status == "seeded"
    assert (net / "raw" / "funding" / "BTC" / "partition=0" / "data.parquet").exists()
    assert (net / "rates" / "ETH" / "1h.parquet").exists()
    assert (net / "direction" / "BTC" / "1h.parquet").exists()

    factor = json.loads((net / "checkpoints" / FACTOR_CHECKPOINT).read_text())
    fee = json.loads((net / "checkpoints" / FEE_CHECKPOINT).read_text())
    # raw max block minus the overlap, never the raw max itself (rerun dedupes).
    assert factor["last_block"] == 5_000_000 - 100_000
    assert fee["last_block"] == 5_000_010 - 100_000
    assert factor["symbol"] == "funding_factor_all"
    assert fee["symbol"] == "fee_per_size_all"
    assert factor["total_events"] == 6
    assert factor["metadata"] == {"markets_seen": 2}
    assert isinstance(factor["last_timestamp"], int)

    manifest = json.loads((net / "SEED_MANIFEST.json").read_text())
    assert manifest["source"] == {"ref": "data/full-history"}
    assert manifest["checkpoints"]["funding_factor"]["origin"] == "synthesized"
    assert assess_funding_state(gmx_root) is FundingState.READY


def test_seed_prefers_authoritative_checkpoints_present_in_the_source(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src", with_checkpoints=True)
    gmx_root = tmp_path / "gmx"

    seed_funding_state(source, gmx_root)

    cp = gmx_root / "funding" / "arbitrum" / "checkpoints"
    assert json.loads((cp / FACTOR_CHECKPOINT).read_text())["last_block"] == 4_000_000
    manifest = json.loads((gmx_root / "funding" / "arbitrum" / "SEED_MANIFEST.json").read_text())
    assert manifest["checkpoints"]["funding_factor"]["origin"] == "source"


def test_seed_clamps_overlap_to_start_of_a_short_history(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src", funding_blocks=(1_000, 5_000))
    gmx_root = tmp_path / "gmx"

    seed_funding_state(source, gmx_root)

    factor = json.loads(
        (gmx_root / "funding" / "arbitrum" / "checkpoints" / FACTOR_CHECKPOINT).read_text()
    )
    assert factor["last_block"] == 1_000 - 1


def test_seed_never_overwrites_existing_raw_state(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    gmx_root = tmp_path / "gmx"
    existing = gmx_root / "funding" / "arbitrum" / "raw" / "funding" / "BTC"
    existing.mkdir(parents=True)
    (existing / "newer.parquet").write_bytes(b"newer carried-forward state")

    result = seed_funding_state(source, gmx_root)

    assert result.status == "skipped_existing"
    assert (existing / "newer.parquet").read_bytes() == b"newer carried-forward state"
    assert not (gmx_root / "funding" / "arbitrum" / "rates").exists()


def test_seed_refuses_a_partial_state_without_raw(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    gmx_root = tmp_path / "gmx"
    stray = gmx_root / "funding" / "arbitrum" / "rates" / "BTC"
    stray.mkdir(parents=True)
    (stray / "1h.parquet").write_bytes(b"x")

    with pytest.raises(SeedError, match="partial"):
        seed_funding_state(source, gmx_root)
    assert (stray / "1h.parquet").read_bytes() == b"x"


def test_invalid_source_leaves_nothing_behind(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    # A truncated raw file: readable tree, unreadable parquet.
    (source / "raw" / "funding" / "BTC" / "partition=0" / "data.parquet").write_bytes(b"junk")
    gmx_root = tmp_path / "gmx"

    with pytest.raises(SeedError):
        seed_funding_state(source, gmx_root)

    assert not (gmx_root / "funding").exists()
    assert not any(p.name.startswith(".funding-seed") for p in tmp_path.rglob("*"))


def test_source_missing_a_required_part_is_rejected(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    for p in sorted((source / "raw" / "fee_per_size").rglob("*.parquet")):
        p.unlink()

    with pytest.raises(SeedError, match="fee_per_size"):
        seed_funding_state(source, tmp_path / "gmx")


def test_state_assessment(tmp_path: Path) -> None:
    gmx_root = tmp_path / "gmx"
    assert assess_funding_state(gmx_root) is FundingState.ABSENT

    source = write_seed_source(tmp_path / "src")
    seed_funding_state(source, gmx_root)
    assert assess_funding_state(gmx_root) is FundingState.READY

    # Raw without a checkpoint would make --resume walk from genesis.
    (gmx_root / "funding" / "arbitrum" / "checkpoints" / FEE_CHECKPOINT).unlink()
    assert assess_funding_state(gmx_root) is FundingState.INCOMPLETE


def test_checkpoint_contract_matches_the_extractors() -> None:
    """The synthesized checkpoint must be exactly what #75's extractors read."""
    factor_src = (REPO / "scripts" / "extract_funding_factor.py").read_text()
    fee_src = (REPO / "scripts" / "extract_funding_fee_per_size.py").read_text()
    unified_src = (REPO / "scripts" / "extract_unified_funding.py").read_text()

    for src, symbol, name in (
        (factor_src, "funding_factor_all", FACTOR_CHECKPOINT),
        (fee_src, "fee_per_size_all", FEE_CHECKPOINT),
    ):
        assert f'"symbol": "{symbol}"' in src
        assert name in src or name in unified_src
        for key in ("last_block", "last_timestamp", "total_events", "last_updated", "metadata"):
            assert f'"{key}"' in src
        assert '"markets_seen"' in src
    # The resume path reads <output-dir>/<network>/checkpoints.
    assert re.search(r'output_dir / "checkpoints"', factor_src)
    assert re.search(r'output_dir / "checkpoints"', fee_src)
