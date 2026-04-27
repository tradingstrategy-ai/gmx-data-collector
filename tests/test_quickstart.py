"""Tests for quickstart release-based seeding."""

from __future__ import annotations

import shutil
import subprocess
import tarfile
from pathlib import Path
from unittest.mock import patch

import pytest
from rich.console import Console

from gmx_historical_data.quickstart import (
    DEFAULT_RELEASE_TAG,
    seed_from_release,
)


@pytest.fixture
def fake_release_tarball(tmp_path: Path) -> Path:
    """Build a fake gmx-light.tar.gz under tmp_path/dl/."""
    src = tmp_path / "src" / "user_data" / "data" / "gmx"
    (src / "snapshots").mkdir(parents=True)
    (src / "snapshots" / "2026-04-26.parquet").write_bytes(b"snap")
    (src / "tickers").mkdir()
    (src / "tickers" / "2026-04-26.parquet").write_bytes(b"tick")

    dl_dir = tmp_path / "dl"
    dl_dir.mkdir()
    tarball = dl_dir / "gmx-light.tar.gz"
    with tarfile.open(tarball, "w:gz") as tf:
        tf.add(tmp_path / "src" / "user_data", arcname="user_data")
    return tarball


def _fake_gh_download(dl_dir: Path, src_tarball: Path):
    """Replace gh release download with a copy from src_tarball.

    Copies ``src_tarball`` into the ``--dir`` argument supplied in the
    ``gh release download`` command, so the production code finds the
    asset exactly where it expects it.
    """

    def runner(cmd, *args, **kwargs):
        if cmd[:3] == ["gh", "release", "download"]:
            # Extract the --dir argument from the command
            dest_dir = dl_dir  # fallback
            if "--dir" in cmd:
                dest_dir = Path(cmd[cmd.index("--dir") + 1])
            shutil.copy2(src_tarball, dest_dir / src_tarball.name)
            return subprocess.CompletedProcess(cmd, 0, "", "")
        raise AssertionError(f"unexpected command: {cmd}")

    return runner


def test_seed_from_release_copies_missing_files(tmp_path, fake_release_tarball):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    with patch(
        "gmx_historical_data.quickstart.subprocess.run",
        side_effect=_fake_gh_download(fake_release_tarball.parent, fake_release_tarball),
    ):
        result = seed_from_release(
            output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True), asset="gmx-light.tar.gz"
        )

    assert result["copied"] == 2
    assert result["skipped"] == 0
    assert (output_dir / "data" / "gmx" / "snapshots" / "2026-04-26.parquet").exists()


def test_seed_from_release_skips_existing(tmp_path, fake_release_tarball):
    output_dir = tmp_path / "out"
    existing = output_dir / "data" / "gmx" / "snapshots" / "2026-04-26.parquet"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"original")

    with patch(
        "gmx_historical_data.quickstart.subprocess.run",
        side_effect=_fake_gh_download(fake_release_tarball.parent, fake_release_tarball),
    ):
        result = seed_from_release(
            output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True), asset="gmx-light.tar.gz"
        )

    assert result["skipped"] == 1
    assert result["copied"] == 1  # tickers file copied, snapshots file skipped
    assert existing.read_bytes() == b"original"  # not overwritten


def test_seed_from_release_handles_gh_failure(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    def failing_runner(cmd, *args, **kwargs):
        raise subprocess.CalledProcessError(1, cmd, stderr="release not found")

    with patch("gmx_historical_data.quickstart.subprocess.run", side_effect=failing_runner):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert "error" in result
    assert result["copied"] == 0


def test_seed_from_release_asset_missing_after_download(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    def success_but_no_file(cmd, *args, **kwargs):
        return subprocess.CompletedProcess(cmd, 0, "", "")

    with patch("gmx_historical_data.quickstart.subprocess.run", side_effect=success_but_no_file):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert "error" in result
    assert result["error"] == "asset missing"
    assert result["copied"] == 0


def test_seed_from_release_tarball_missing_user_data_root(tmp_path):
    output_dir = tmp_path / "out"
    output_dir.mkdir()

    tarball_path = tmp_path / "gmx-full.tar.gz"
    with tarfile.open(tarball_path, "w:gz") as tf:
        dummy = tmp_path / "dummy.txt"
        dummy.write_bytes(b"no user_data here")
        tf.add(dummy, arcname="dummy.txt")

    def download_bad_tarball(cmd, *args, **kwargs):
        if "--dir" in cmd:
            dest = Path(cmd[cmd.index("--dir") + 1])
            shutil.copy2(tarball_path, dest / "gmx-full.tar.gz")
        return subprocess.CompletedProcess(cmd, 0, "", "")

    with patch("gmx_historical_data.quickstart.subprocess.run", side_effect=download_bad_tarball):
        result = seed_from_release(output_dir, DEFAULT_RELEASE_TAG, Console(quiet=True))

    assert "error" in result
    assert result["error"] == "no user_data"
    assert result["copied"] == 0
