"""Validation and run-level reporting for legacy raw parquet schemas."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Sequence
from pathlib import Path

from rich.console import Console

_legacy_files: dict[Path, set[str]] = defaultdict(set)
_console = Console()


def project_raw_columns(
    columns: Sequence[str], filepath: Path, expected_columns: Sequence[str]
) -> list[str]:
    """Validate raw columns, record legacy extras, and return the projection.

    :param columns: Columns present in the raw file or frame.
    :param filepath: Source parquet path, included in validation errors.
    :param expected_columns: Current raw record schema, in projection order.
    :returns: The expected columns in their declared order.
    :raises ValueError: If a required column is absent.
    """
    missing = sorted(set(expected_columns) - set(columns))
    if missing:
        raise ValueError(
            f"Raw funding file {filepath} is missing required fields: {', '.join(missing)}"
        )

    extras = sorted(set(columns) - set(expected_columns))
    if extras:
        _legacy_files[filepath].update(extras)
    return list(expected_columns)


def report_legacy_raw_columns(console: Console | None = None) -> None:
    """Print one summary of legacy raw columns seen during this run.

    :param console: Rich console for the summary, or ``None`` to use the default.
    """
    if not _legacy_files:
        return
    columns = sorted({column for extras in _legacy_files.values() for column in extras})
    (console or _console).print(
        f"[yellow]Ignoring legacy raw columns in {len(_legacy_files)} file(s): "
        f"{', '.join(columns)}[/yellow]"
    )
    _legacy_files.clear()
