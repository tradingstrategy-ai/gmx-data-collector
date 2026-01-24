"""Checkpoint management for incremental data collection.

Tracks collection progress to enable resuming from failures and
incremental updates.
"""

import json
from pathlib import Path
from typing import Any
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class Checkpoint:
    """Collection checkpoint data.

    :param symbol: Token symbol
    :param last_block: Last block number processed
    :param last_timestamp: Last event timestamp processed
    :param total_events: Total events collected so far
    :param last_updated: Timestamp of last checkpoint update
    :param metadata: Additional metadata
    """

    symbol: str
    last_block: int
    last_timestamp: int
    total_events: int
    last_updated: str
    metadata: dict[str, Any] = None

    def __post_init__(self):
        """Initialize metadata if None."""
        if self.metadata is None:
            self.metadata = {}


class CheckpointManager:
    """Manage collection checkpoints for resumable data collection.

    :param checkpoint_dir: Directory to store checkpoint files
    """

    def __init__(self, checkpoint_dir: Path):
        """Initialize checkpoint manager.

        :param checkpoint_dir: Directory for checkpoint files
        """
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)

    def _get_checkpoint_path(self, symbol: str) -> Path:
        """Get checkpoint file path for a symbol.

        :param symbol: Token symbol
        :return: Path to checkpoint file
        """
        return self.checkpoint_dir / f"{symbol.lower()}_checkpoint.json"

    def save_checkpoint(self, checkpoint: Checkpoint):
        """Save checkpoint to disk.

        :param checkpoint: Checkpoint data to save
        """
        checkpoint_path = self._get_checkpoint_path(checkpoint.symbol)
        with open(checkpoint_path, "w") as f:
            json.dump(asdict(checkpoint), f, indent=2)

    def load_checkpoint(self, symbol: str) -> Checkpoint | None:
        """Load checkpoint from disk.

        :param symbol: Token symbol
        :return: Checkpoint data or None if not found
        """
        checkpoint_path = self._get_checkpoint_path(symbol)
        if not checkpoint_path.exists():
            return None

        try:
            with open(checkpoint_path, "r") as f:
                data = json.load(f)
                return Checkpoint(**data)
        except (json.JSONDecodeError, TypeError, KeyError) as e:
            print(f"Warning: Failed to load checkpoint for {symbol}: {e}")
            return None

    def update_checkpoint(
        self,
        symbol: str,
        last_block: int,
        last_timestamp: int,
        events_added: int,
    ) -> Checkpoint:
        """Update checkpoint with new data.

        :param symbol: Token symbol
        :param last_block: Latest block processed
        :param last_timestamp: Latest timestamp processed
        :param events_added: Number of events added in this update
        :return: Updated checkpoint
        """
        # Load existing checkpoint or create new
        existing = self.load_checkpoint(symbol)

        if existing:
            total_events = existing.total_events + events_added
        else:
            total_events = events_added

        checkpoint = Checkpoint(
            symbol=symbol,
            last_block=last_block,
            last_timestamp=last_timestamp,
            total_events=total_events,
            last_updated=datetime.utcnow().isoformat(),
        )

        self.save_checkpoint(checkpoint)
        return checkpoint

    def delete_checkpoint(self, symbol: str):
        """Delete checkpoint for a symbol.

        :param symbol: Token symbol
        """
        checkpoint_path = self._get_checkpoint_path(symbol)
        if checkpoint_path.exists():
            checkpoint_path.unlink()

    def list_checkpoints(self) -> list[Checkpoint]:
        """List all checkpoints.

        :return: List of all checkpoints
        """
        checkpoints = []
        for checkpoint_file in self.checkpoint_dir.glob("*_checkpoint.json"):
            try:
                with open(checkpoint_file, "r") as f:
                    data = json.load(f)
                    checkpoints.append(Checkpoint(**data))
            except (json.JSONDecodeError, TypeError, KeyError) as e:
                print(f"Warning: Failed to load {checkpoint_file}: {e}")
                continue
        return checkpoints

    def get_resume_block(self, symbol: str, default: int = 0) -> int:
        """Get block number to resume from.

        :param symbol: Token symbol
        :param default: Default block if no checkpoint exists
        :return: Block number to resume from
        """
        checkpoint = self.load_checkpoint(symbol)
        if checkpoint:
            # Resume from next block after last processed
            return checkpoint.last_block + 1
        return default
