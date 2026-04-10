"""Quickstart seeding from the data/daily-collection branch.

Shared helpers used by both ``scripts/collect_daily_snapshot.py`` and
``gmx_historical_data collect --quickstart``. Shallow-clones the
``data/daily-collection`` branch of the origin remote and merge-copies
its ``user_data/`` tree into a local destination. Merge-only means
files that already exist locally are skipped — never overwritten — so
the operation is idempotent and reversible (``rm -rf user_data/``).
"""

import shutil
import subprocess
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

#: Public HTTPS fallback when no ``origin`` remote is resolvable.
DEFAULT_REMOTE_URL = "https://github.com/tradingstrategy-ai/gmx-data-collector.git"

#: Default branch seeded by the quickstart flow.
DEFAULT_BRANCH = "data/daily-collection"


def resolve_origin_url(search_from: Path | None = None) -> str:
    """Return the git ``origin`` URL of the repo containing ``search_from``.

    Prefers the user's configured remote so SSH vs HTTPS matches their
    existing clone. Falls back to the public HTTPS URL when the caller
    is outside a git working tree.

    :param search_from: Directory to search upward from. Defaults to the
        directory containing this module.
    :return: Remote URL suitable for ``git clone``.
    """
    if search_from is None:
        search_from = Path(__file__).resolve().parent
    try:
        result = subprocess.run(
            ["git", "-C", str(search_from), "remote", "get-url", "origin"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip() or DEFAULT_REMOTE_URL
    except (subprocess.CalledProcessError, FileNotFoundError):
        return DEFAULT_REMOTE_URL


def seed_from_branch(output_dir: Path, ref: str, console: Console) -> dict:
    """Seed ``output_dir`` from the remote data branch.

    Shallow-clones ``ref`` into a temp directory, then merge-copies
    ``user_data/`` into ``output_dir``. Existing local files are never
    overwritten; only missing files are copied.

    :param output_dir: Local destination root (e.g. ``./user_data``).
    :param ref: Remote branch to seed from.
    :param console: Rich console used for progress output.
    :return: Summary dict with keys ``copied``, ``skipped``, ``bytes``.
        On failure the dict also contains ``error``.
    """
    url = resolve_origin_url()
    tmp = Path(tempfile.mkdtemp(prefix="gmx-quickstart-"))
    try:
        console.print(f"  Cloning [cyan]{ref}[/cyan] from {url} (shallow)...")
        try:
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    ref,
                    "--single-branch",
                    url,
                    str(tmp),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            console.print(f"  [yellow]Warning:[/yellow] git clone failed: {stderr}")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": stderr}

        src_root = tmp / "user_data"
        if not src_root.is_dir():
            console.print("  [yellow]Warning:[/yellow] branch has no user_data/ directory")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": "no user_data"}

        copied = 0
        skipped = 0
        total_bytes = 0
        for src_file in src_root.rglob("*"):
            if not src_file.is_file():
                continue
            rel = src_file.relative_to(src_root)
            dest = output_dir / rel
            if dest.exists():
                skipped += 1
                continue
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_file, dest)
            copied += 1
            total_bytes += src_file.stat().st_size
        return {"copied": copied, "skipped": skipped, "bytes": total_bytes}
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


def print_coverage_summary(output_dir: Path, console: Console) -> None:
    """Print a short coverage summary of the seeded ``user_data`` tree.

    Reads nothing heavier than feather metadata for one representative
    OHLCV file; everything else is inferred from parquet filenames
    (``YYYY-MM-DD.parquet``).

    :param output_dir: Local root, e.g. ``./user_data``.
    :param console: Rich console for output.
    """
    gmx_root = output_dir / "data" / "gmx"
    if not gmx_root.is_dir():
        console.print("  [yellow]No data/gmx/ tree found under output dir.[/yellow]")
        return

    today = datetime.now(UTC).date()
    console.print("\n[bold]Seeded coverage[/bold]")

    for label, subdir in (("Snapshots", "snapshots"), ("Tickers", "tickers"), ("APY", "apy")):
        files = (
            sorted((gmx_root / subdir).glob("*.parquet")) if (gmx_root / subdir).is_dir() else []
        )
        if not files:
            console.print(f"  {label:10s} (none)")
            continue
        dates = [f.stem for f in files]
        console.print(f"  {label:10s} {len(files)} files — {dates[0]} → {dates[-1]}")
        if label == "Snapshots":
            try:
                latest = datetime.strptime(dates[-1], "%Y-%m-%d").date()
                gap = (today - latest).days
                colour = "red" if gap > 1 else "green"
                console.print(
                    f"  {'Gap':10s} [bold {colour}]{gap} day(s) to today ({today})[/bold {colour}]"
                )
            except ValueError:
                pass

    futures_dir = gmx_root / "futures"
    if futures_dir.is_dir():
        feather_files = sorted(futures_dir.glob("*.feather"))
        console.print(f"  {'Futures':10s} {len(feather_files)} feather files")
        sample = futures_dir / "ETH_USDC_USDC-1d-futures.feather"
        if sample.exists():
            try:
                df = pd.read_feather(sample)
                if not df.empty and "date" in df.columns:
                    console.print(
                        f"  {'  ETH 1d':10s} {len(df)} rows — "
                        f"{df['date'].min()} → {df['date'].max()}"
                    )
            except Exception as exc:  # noqa: BLE001
                console.print(f"  [yellow]Could not read sample: {exc}[/yellow]")
    else:
        console.print(f"  {'Futures':10s} (none)")
