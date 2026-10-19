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


def _yesterdays_pass(gmx_root: Path) -> None:
    (gmx_root / "futures").mkdir(parents=True, exist_ok=True)
    (gmx_root / "funding_coverage.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "refreshed": True,
                "generated_at": "2026-10-17T02:20:00+00:00",
                "reasons": [],
                "pairs": {"BTC": {"tail": "CURRENT"}},
            }
        )
    )


def test_gate_crash_replaces_a_stale_pass_with_a_withheld_verdict(
    tmp_path: Path, monkeypatch
) -> None:
    from gmx_historical_data import funding_release

    env_file = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    gmx_root = tmp_path / "gmx"
    _yesterdays_pass(gmx_root)
    companion = gmx_root / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather"
    companion.write_bytes(b"previous release's file")
    report = tmp_path / "data_report.txt"

    def boom(*_a, **_kw):
        raise RuntimeError("kaput")

    monkeypatch.setattr(funding_release, "run_gate", boom)
    rc = cli.main(
        [
            "gate",
            "--gmx-root",
            str(gmx_root),
            "--stage-dir",
            str(tmp_path / "stage"),
            "--state",
            "ready",
            "--extract-outcome",
            "success",
            "--run-start",
            "2026-10-18T02:20:00+00:00",
            "--report",
            str(report),
        ]
    )

    assert rc != 0
    verdict = json.loads((gmx_root / "funding_coverage.json").read_text())
    assert verdict["status"] == "WITHHELD" and verdict["refreshed"] is False
    assert verdict["generated_at"] == "2026-10-18T02:20:00+00:00"
    assert any("kaput" in r for r in verdict["reasons"])
    assert "NOT refreshed" in report.read_text()
    assert companion.read_bytes() == b"previous release's file"
    assert "FUNDING_GATE=fail" in env_file.read_text()


def test_ensure_verdict_replaces_a_missing_or_stale_verdict(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    gmx_root = tmp_path / "gmx"
    _yesterdays_pass(gmx_root)
    report = tmp_path / "data_report.txt"
    args = [
        "ensure-verdict",
        "--gmx-root",
        str(gmx_root),
        "--state",
        "ready",
        "--extract-outcome",
        "success",
        "--run-start",
        "2026-10-18T02:20:00+00:00",
        "--report",
        str(report),
    ]

    # The gate exited 1 without producing a verdict (an uncaught exception).
    assert cli.main([*args, "--exit-code", "1"]) == 0  # a current WITHHELD verdict is in place
    verdict = json.loads((gmx_root / "funding_coverage.json").read_text())
    assert verdict["status"] == "WITHHELD"
    assert verdict["generated_at"] == "2026-10-18T02:20:00+00:00"
    assert "exit 1" in " ".join(verdict["reasons"])
    assert "FUNDING_GATE=fail" in env_file.read_text()
    assert "NOT refreshed" in report.read_text()


def test_ensure_verdict_trusts_a_verdict_this_run_wrote(tmp_path: Path, monkeypatch) -> None:
    env_file = tmp_path / "github_env"
    monkeypatch.setenv("GITHUB_ENV", str(env_file))
    gmx_root = tmp_path / "gmx"
    gmx_root.mkdir()
    run_start = "2026-10-18T02:20:00+00:00"
    (gmx_root / "funding_coverage.json").write_text(
        json.dumps({"status": "PASS", "refreshed": True, "generated_at": run_start, "reasons": []})
    )

    rc = cli.main(
        [
            "ensure-verdict",
            "--gmx-root",
            str(gmx_root),
            "--state",
            "ready",
            "--extract-outcome",
            "success",
            "--run-start",
            run_start,
            "--exit-code",
            "0",
        ]
    )

    assert rc == 0
    assert json.loads((gmx_root / "funding_coverage.json").read_text())["status"] == "PASS"
    assert "FUNDING_GATE=pass" in env_file.read_text()


def test_notes_without_any_verdict_say_funding_was_not_refreshed(tmp_path: Path, capsys) -> None:
    assert cli.main(["notes", "--coverage", str(tmp_path / "absent.json")]) == 0
    assert "NOT refreshed" in capsys.readouterr().out
