"""Quickstart seeding from the latest GitHub Release.

Shared helpers used by both ``scripts/collect_daily_snapshot.py`` and
``gmx_historical_data collect --quickstart``. Downloads ``gmx-full.tar.gz``
from a GitHub Release of this repo and merge-copies its ``user_data/`` tree
into a local destination. Merge-only means files that already exist locally
are skipped — never overwritten — so the operation is idempotent and
reversible (``rm -rf user_data/``).
"""

import shutil
import subprocess
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from rich.console import Console

#: GitHub repository serving the releases.
DEFAULT_REPO = "tradingstrategy-ai/gmx-data-collector"

#: Sentinel meaning "the latest release" — passed to ``gh release download``
#: as no ``--tag`` argument.
DEFAULT_RELEASE_TAG = "latest"

#: Asset name to download. ``full`` includes ``futures/`` feathers; ``light``
#: omits them. The seed flow uses full so freqtrade backtests work end-to-end.
DEFAULT_ASSET = "gmx-full.tar.gz"


def seed_from_release(
    output_dir: Path,
    tag: str,
    console: Console,
    asset: str = DEFAULT_ASSET,
    repo: str = DEFAULT_REPO,
) -> dict:
    """Seed ``output_dir`` from a GitHub Release asset.

    Downloads ``asset`` from ``repo``'s release ``tag`` (or the latest release
    if ``tag == DEFAULT_RELEASE_TAG``), extracts it to a temp directory, then
    merge-copies the contained ``user_data/`` tree into ``output_dir``.
    Existing local files are never overwritten; only missing files are copied.

    :param output_dir: Local destination root (e.g. ``./user_data``).
    :param tag: Release tag, or ``DEFAULT_RELEASE_TAG`` for the latest release.
    :param console: Rich console used for progress output.
    :param asset: Release asset filename (``gmx-full.tar.gz`` or
        ``gmx-light.tar.gz``).
    :param repo: GitHub ``owner/name`` to pull from.
    :return: Summary dict with keys ``copied``, ``skipped``, ``bytes``.
        On failure the dict also contains ``error``.
    """
    tmp = Path(tempfile.mkdtemp(prefix="gmx-quickstart-"))
    try:
        cmd = ["gh", "release", "download"]
        if tag != DEFAULT_RELEASE_TAG:
            cmd.append(tag)
        cmd += ["--repo", repo, "--pattern", asset, "--dir", str(tmp), "--clobber"]

        console.print(f"  Downloading [cyan]{asset}[/cyan] from {repo}@{tag}...")
        try:
            subprocess.run(cmd, check=True, capture_output=True, text=True)
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            console.print(f"  [yellow]Warning:[/yellow] gh release download failed: {stderr}")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": stderr}

        tarball = tmp / asset
        if not tarball.is_file():
            console.print(f"  [yellow]Warning:[/yellow] asset {asset} not found in release")
            return {"copied": 0, "skipped": 0, "bytes": 0, "error": "asset missing"}

        extract_root = tmp / "extracted"
        extract_root.mkdir()
        with tarfile.open(tarball, "r:gz") as tf:
            tf.extractall(extract_root)  # noqa: S202 — trusted source

        src_root = extract_root / "user_data"
        if not src_root.is_dir():
            console.print("  [yellow]Warning:[/yellow] tarball has no user_data/ root")
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
