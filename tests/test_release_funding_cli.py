"""The ``scripts/release_funding_companions.py`` entry point the workflow calls."""

import json
from pathlib import Path

from scripts import release_funding_companions as cli
from tests._funding_release_fixtures import write_seed_source


def test_seed_cli_seeds_from_a_directory(tmp_path: Path) -> None:
    source = write_seed_source(tmp_path / "src")
    gmx_root = tmp_path / "gmx"

    rc = cli.main(["seed", "--gmx-root", str(gmx_root), "--source-dir", str(source)])

    assert rc == 0
    assert (gmx_root / "funding" / "arbitrum" / "SEED_MANIFEST.json").exists()


def test_seed_cli_failure_is_fail_soft(tmp_path: Path) -> None:
    rc = cli.main(
        ["seed", "--gmx-root", str(tmp_path / "gmx"), "--source-dir", str(tmp_path / "missing")]
    )
    assert rc == 0  # the release continues unseeded; the gate then withholds
    assert not (tmp_path / "gmx" / "funding").exists()


def test_state_cli_exports_the_state_for_later_steps(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    gmx_root = tmp_path / "gmx"

    assert cli.main(["state", "--gmx-root", str(gmx_root)]) == 0
    assert "FUNDING_STATE=absent" in env_file.read_text()

    cli.main(
        [
            "seed",
            "--gmx-root",
            str(gmx_root),
            "--source-dir",
            str(write_seed_source(tmp_path / "s")),
        ]
    )
    cli.main(["state", "--gmx-root", str(gmx_root)])
    assert env_file.read_text().strip().splitlines()[-1] == "FUNDING_STATE=ready"


def test_gate_cli_exit_codes_and_env(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    gmx_root = tmp_path / "gmx"
    (gmx_root / "futures").mkdir(parents=True)

    rc = cli.main(
        [
            "gate",
            "--gmx-root",
            str(gmx_root),
            "--stage-dir",
            str(tmp_path / "stage"),
            "--state",
            "absent",
            "--extract-outcome",
            "skipped",
        ]
    )

    assert rc == 1
    assert "FUNDING_GATE=fail" in env_file.read_text()
    assert json.loads((gmx_root / "funding_coverage.json").read_text())["status"] == "WITHHELD"


def test_notes_cli_prints_markdown(tmp_path: Path, capsys) -> None:
    coverage = tmp_path / "funding_coverage.json"
    coverage.write_text(
        json.dumps(
            {
                "status": "WITHHELD",
                "refreshed": False,
                "reasons": ["B4: extraction failure"],
                "pairs": {},
            }
        )
    )

    assert cli.main(["notes", "--coverage", str(coverage)]) == 0

    assert "NOT refreshed" in capsys.readouterr().out
    assert cli.main(["notes", "--coverage", str(tmp_path / "absent.json")]) == 0
