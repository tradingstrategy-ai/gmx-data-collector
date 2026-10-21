import os
import subprocess
from pathlib import Path


def test_prepare_gmx_data_root_links_collector_paths_to_existing_data(tmp_path):
    root = tmp_path / "gmx"
    expected = [
        "futures",
        "snapshots",
        "tickers",
        "apy",
        "volumes",
        "ticks",
        "tick_volume",
        "checkpoints",
    ]
    for name in expected:
        (root / name).mkdir(parents=True)
        (root / name / "sentinel").write_text(name)

    script = Path("deploy/server/prepare-gmx-data-root.sh")
    result = subprocess.run(
        ["bash", str(script)],
        env={**os.environ, "GMX_ROOT": str(root)},
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    for name in expected:
        link = root / "user_data" / "data" / "gmx" / name
        assert link.is_symlink()
        assert link.resolve() == (root / name).resolve()
        assert (link / "sentinel").read_text() == name


def test_prepare_gmx_data_root_refuses_nonempty_destination(tmp_path):
    root = tmp_path / "gmx"
    (root / "futures").mkdir(parents=True)
    nested = root / "user_data" / "data" / "gmx" / "futures"
    nested.mkdir(parents=True)
    (nested / "do-not-overwrite").write_text("preserve")

    result = subprocess.run(
        ["bash", "deploy/server/prepare-gmx-data-root.sh"],
        env={**os.environ, "GMX_ROOT": str(root)},
        capture_output=True,
        text=True,
    )

    assert result.returncode != 0
    assert (nested / "do-not-overwrite").read_text() == "preserve"
