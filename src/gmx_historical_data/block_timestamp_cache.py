"""Block-timestamp cache for efficient timestamp↔block conversions.

This module provides the BlockTimestampCache class which samples blocks
at regular intervals and uses linear interpolation to convert between
timestamps and block numbers without excessive RPC calls.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.progress import BarColumn, Progress, SpinnerColumn, TaskProgressColumn, TextColumn
from web3 import Web3

from gmx_historical_data.atomic_parquet import atomic_write_parquet_pandas
from gmx_historical_data.config import (
    BLOCK_SAMPLE_INTERVAL,
    CACHE_STALE_THRESHOLD,
    GMX_V2_GENESIS_BLOCK,
)

logger = logging.getLogger(__name__)
console = Console()


class BlockTimestampCache:
    """Cache for efficient block↔timestamp conversions using sampled blocks.

    Samples blocks at regular intervals and provides linear interpolation
    for timestamp↔block conversions. Automatically loads, builds, and updates
    cache as needed.

    :param cache_path: Path to parquet cache file
    :param web3: Web3 instance for RPC calls
    :param genesis_block: Starting block number (default: GMX_V2_GENESIS_BLOCK)
    :param sample_interval: Blocks between samples (default: BLOCK_SAMPLE_INTERVAL)
    """

    def __init__(
        self,
        cache_path: Path,
        web3: Web3,
        genesis_block: int = GMX_V2_GENESIS_BLOCK,
        sample_interval: int = BLOCK_SAMPLE_INTERVAL,
        fetch_workers: int = 32,
    ):
        """Initialize BlockTimestampCache.

        :param cache_path: Path to cache file
        :param web3: Web3 instance
        :param genesis_block: Genesis block number
        :param sample_interval: Sample interval in blocks
        :param fetch_workers: Number of parallel RPC workers for cache build/update
        """
        self.cache_path = Path(cache_path)
        self.web3 = web3
        self.genesis_block = genesis_block
        self.sample_interval = sample_interval
        self.fetch_workers = fetch_workers
        self.cache_df: pd.DataFrame | None = None

        logger.info(
            f"Initialized BlockTimestampCache: path={cache_path}, "
            f"genesis={genesis_block}, interval={sample_interval}, workers={fetch_workers}"
        )

    def get_block_for_timestamp(self, timestamp: int) -> int:
        """Convert timestamp to block number using linear interpolation.

        Automatically loads and updates cache as needed.

        :param timestamp: Unix timestamp in seconds
        :return: Estimated block number
        """
        self._ensure_cache_loaded()

        # Use pandas interpolation
        # Find the two surrounding samples and interpolate
        df = self.cache_df

        # If timestamp is before first sample, use first sample
        if timestamp <= df["timestamp"].iloc[0]:
            return int(df["block"].iloc[0])

        # If timestamp is after last sample, extrapolate
        if timestamp >= df["timestamp"].iloc[-1]:
            # Linear extrapolation from last two points
            last_block = df["block"].iloc[-1]
            last_timestamp = df["timestamp"].iloc[-1]
            prev_block = df["block"].iloc[-2]
            prev_timestamp = df["timestamp"].iloc[-2]

            blocks_per_second = (last_block - prev_block) / (last_timestamp - prev_timestamp)
            estimated_block = last_block + int((timestamp - last_timestamp) * blocks_per_second)
            return estimated_block

        # Interpolate between samples
        # Find surrounding samples
        idx = df["timestamp"].searchsorted(timestamp)
        if idx == 0:
            idx = 1

        lower_timestamp = df["timestamp"].iloc[idx - 1]
        upper_timestamp = df["timestamp"].iloc[idx]
        lower_block = df["block"].iloc[idx - 1]
        upper_block = df["block"].iloc[idx]

        # Linear interpolation
        ratio = (timestamp - lower_timestamp) / (upper_timestamp - lower_timestamp)
        estimated_block = lower_block + int(ratio * (upper_block - lower_block))

        return estimated_block

    def get_timestamp_for_block(self, block: int) -> int:
        """Convert block number to timestamp using linear interpolation.

        Automatically loads and updates cache as needed.

        :param block: Block number
        :return: Estimated unix timestamp in seconds
        """
        self._ensure_cache_loaded()

        df = self.cache_df

        # If block is before first sample, use first sample
        if block <= df["block"].iloc[0]:
            return int(df["timestamp"].iloc[0])

        # If block is after last sample, extrapolate
        if block >= df["block"].iloc[-1]:
            # Linear extrapolation from last two points
            last_block = df["block"].iloc[-1]
            last_timestamp = df["timestamp"].iloc[-1]
            prev_block = df["block"].iloc[-2]
            prev_timestamp = df["timestamp"].iloc[-2]

            seconds_per_block = (last_timestamp - prev_timestamp) / (last_block - prev_block)
            estimated_timestamp = last_timestamp + int((block - last_block) * seconds_per_block)
            return estimated_timestamp

        # Interpolate between samples
        # Find surrounding samples
        idx = df["block"].searchsorted(block)
        if idx == 0:
            idx = 1

        lower_block = df["block"].iloc[idx - 1]
        upper_block = df["block"].iloc[idx]
        lower_timestamp = df["timestamp"].iloc[idx - 1]
        upper_timestamp = df["timestamp"].iloc[idx]

        # Linear interpolation
        ratio = (block - lower_block) / (upper_block - lower_block)
        estimated_timestamp = lower_timestamp + int(ratio * (upper_timestamp - lower_timestamp))

        return estimated_timestamp

    def _fetch_block_timestamp(self, block_num: int) -> tuple[int, int] | None:
        """Fetch timestamp for a single block via RPC.

        :param block_num: Block number to fetch
        :return: ``(block_num, timestamp)`` or ``None`` on failure
        """
        try:
            block = self.web3.eth.get_block(block_num)
            return block_num, block.timestamp
        except Exception as e:
            logger.warning(f"Failed to fetch block {block_num}: {e}. Skipping.")
            return None

    def build_cache(self, end_block: int | None = None) -> None:
        """Build cache from scratch by sampling blocks.

        Samples blocks from genesis_block to end_block at sample_interval intervals.
        Saves cache to disk as parquet file.

        :param end_block: End block number (None = current block)
        """
        if end_block is None:
            end_block = self.web3.eth.block_number
            logger.info(f"Using current block as end_block: {end_block}")

        logger.info(f"Building block-timestamp cache from {self.genesis_block} to {end_block}")

        # Calculate sample points
        sample_blocks = list(range(self.genesis_block, end_block + 1, self.sample_interval))

        # Ensure end_block is included
        if sample_blocks[-1] != end_block:
            sample_blocks.append(end_block)

        console.print(
            f"[cyan]Sampling {len(sample_blocks)} blocks at {self.sample_interval}-block intervals "
            f"({self.fetch_workers} workers)...[/cyan]"
        )

        # Fetch timestamps in parallel with progress bar
        blocks_data = []

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching block timestamps...", total=len(sample_blocks))

            with ThreadPoolExecutor(max_workers=self.fetch_workers) as executor:
                futures = {
                    executor.submit(self._fetch_block_timestamp, bn): bn for bn in sample_blocks
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        blocks_data.append({"block": result[0], "timestamp": result[1]})
                    progress.update(task, advance=1)

        # Sort by block number (as_completed returns in completion order)
        blocks_data.sort(key=lambda x: x["block"])

        # Create DataFrame
        df = pd.DataFrame(blocks_data)
        df["block"] = df["block"].astype("uint64")
        df["timestamp"] = df["timestamp"].astype("uint64")

        # Save to parquet (atomic — see atomic_parquet.py)
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        atomic_write_parquet_pandas(df, self.cache_path)
        self.cache_df = df

        console.print(
            f"[green]✓[/green] Cache built with {len(df)} samples and saved to {self.cache_path}"
        )
        logger.info(
            f"Cache built: {len(df)} samples from block {df['block'].iloc[0]} to {df['block'].iloc[-1]}"
        )

    def update_cache(self, end_block: int | None = None) -> None:
        """Update cache with new blocks since last sample.

        Loads existing cache, appends new samples, and saves.

        :param end_block: End block number (None = current block)
        """
        # Load existing cache
        if not self.cache_path.exists():
            logger.warning("Cache file does not exist. Building from scratch.")
            self.build_cache(end_block=end_block)
            return

        self._load_cache()

        if end_block is None:
            end_block = self.web3.eth.block_number

        last_cached_block = int(self.cache_df["block"].iloc[-1])

        if end_block <= last_cached_block:
            logger.info(
                f"Cache is already up to date (cached={last_cached_block}, requested={end_block})"
            )
            return

        logger.info(f"Updating cache from block {last_cached_block} to {end_block}")

        # Calculate new sample points
        start_block = last_cached_block + self.sample_interval
        sample_blocks = list(range(start_block, end_block + 1, self.sample_interval))

        # Ensure end_block is included
        if sample_blocks and sample_blocks[-1] != end_block:
            sample_blocks.append(end_block)
        elif not sample_blocks:
            sample_blocks = [end_block]

        console.print(
            f"[cyan]Updating cache with {len(sample_blocks)} new samples "
            f"({self.fetch_workers} workers)...[/cyan]"
        )

        # Fetch new timestamps in parallel
        new_blocks_data = []
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("Fetching new block timestamps...", total=len(sample_blocks))

            with ThreadPoolExecutor(max_workers=self.fetch_workers) as executor:
                futures = {
                    executor.submit(self._fetch_block_timestamp, bn): bn for bn in sample_blocks
                }
                for future in as_completed(futures):
                    result = future.result()
                    if result is not None:
                        new_blocks_data.append({"block": result[0], "timestamp": result[1]})
                    progress.update(task, advance=1)

        # Sort by block number (as_completed returns in completion order)
        new_blocks_data.sort(key=lambda x: x["block"])

        # Append to existing cache
        new_df = pd.DataFrame(new_blocks_data)
        new_df["block"] = new_df["block"].astype("uint64")
        new_df["timestamp"] = new_df["timestamp"].astype("uint64")

        self.cache_df = pd.concat([self.cache_df, new_df], ignore_index=True)

        # Save updated cache (atomic — see atomic_parquet.py)
        atomic_write_parquet_pandas(self.cache_df, self.cache_path)

        console.print(
            f"[green]✓[/green] Cache updated with {len(new_df)} new samples (total: {len(self.cache_df)})"
        )
        logger.info(
            f"Cache updated: now contains {len(self.cache_df)} samples up to block {self.cache_df['block'].iloc[-1]}"
        )

    def _ensure_cache_loaded(self) -> None:
        """Ensure cache is loaded and up to date.

        Loads cache from disk if not loaded, builds if missing,
        and updates if stale.
        """
        # Load cache if not loaded
        if self.cache_df is None:
            if self.cache_path.exists():
                self._load_cache()
            else:
                logger.info("Cache file not found. Building from scratch.")
                self.build_cache()
                return

        # Check if cache is stale
        current_block = self.web3.eth.block_number
        last_cached_block = int(self.cache_df["block"].iloc[-1])
        blocks_behind = current_block - last_cached_block

        if blocks_behind > CACHE_STALE_THRESHOLD:
            logger.info(
                f"Cache is stale (behind by {blocks_behind} blocks > threshold {CACHE_STALE_THRESHOLD}). Updating..."
            )
            self.update_cache()

    def _load_cache(self) -> None:
        """Load cache from parquet file.

        :raises FileNotFoundError: If cache file doesn't exist
        """
        if not self.cache_path.exists():
            raise FileNotFoundError(f"Cache file not found: {self.cache_path}")

        self.cache_df = pd.read_parquet(self.cache_path)
        logger.info(
            f"Cache loaded: {len(self.cache_df)} samples from block "
            f"{self.cache_df['block'].iloc[0]} to {self.cache_df['block'].iloc[-1]}"
        )
