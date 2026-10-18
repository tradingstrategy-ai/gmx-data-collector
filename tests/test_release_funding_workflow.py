"""``release-data.yml``: funding state, staged companions, packaging contract."""

import os
import stat
import subprocess
import tarfile
from pathlib import Path

import pytest
import yaml

WORKFLOW = Path(".github/workflows/release-data.yml")


def _job() -> dict:
    return yaml.safe_load(WORKFLOW.read_text())["jobs"]["release"]


def _steps() -> list[dict]:
    return _job()["steps"]


def _step(name: str) -> dict:
    matches = [s for s in _steps() if s.get("name") == name]
    assert len(matches) == 1, f"expected exactly one step named {name!r}"
    return matches[0]


def _index(name: str) -> int:
    return [s.get("name") for s in _steps()].index(name)


def test_seed_is_a_dispatch_only_input_empty_by_default() -> None:
    triggers = yaml.safe_load(WORKFLOW.read_text())[True]  # PyYAML parses `on` as True
    spec = triggers["workflow_dispatch"]["inputs"]["seed_funding_from"]
    assert spec["default"] == ""
    seed = _step("Seed funding state")
    assert "seed_funding_from != ''" in seed["if"]
    assert "release_funding_companions.py seed" in seed["run"]


def test_funding_steps_run_in_contract_order() -> None:
    order = [
        "Restore previous release data",
        "Restore funding state",
        "Seed funding state",
        "Preflight funding state",
        "Collect daily snapshot",
        "Extract funding (incremental)",
        "Stage funding and mark companions",
        "Gate and promote funding companions",
        "Validate data type coverage",
        "Package tarballs",
        "Create GitHub Release",
        "Fail job if funding companions were withheld",
        "Fail job if a new cadence regression was not resolved by retry",
    ]
    indexes = [_index(name) for name in order]
    assert indexes == sorted(indexes)
    assert _steps()[-1]["name"].startswith("Fail job if a new cadence regression")


def test_extraction_is_incremental_fail_soft_and_never_walks_from_genesis() -> None:
    step = _step("Extract funding (incremental)")
    assert step["continue-on-error"] is True
    assert "FUNDING_STATE == 'ready'" in step["if"]
    assert "--resume" in step["run"]
    assert "--output-dir user_data/data/gmx/funding" in step["run"]
    assert "--include-datastore" not in step["run"]
    assert set(step["env"]) == {"JSON_RPC_ARBITRUM", "HYPERSYNC_API_TOKEN"}
    # 15 min on the cron, 30 on a seed dispatch (catch-up from the seed).
    assert "30" in str(step["timeout-minutes"]) and "15" in str(step["timeout-minutes"])


def test_staging_is_gated_on_extraction_and_fail_soft() -> None:
    step = _step("Stage funding and mark companions")
    assert step["continue-on-error"] is True
    assert "steps.funding_extract.outcome == 'success'" in step["if"]
    assert "--stage-dir user_data/.companion-stage" in step["run"]


def test_gate_always_runs_and_fails_closed() -> None:
    gate = _step("Gate and promote funding companions")
    assert gate["if"] == "always()"
    assert "release_funding_companions.py gate" in gate["run"]
    assert "--extract-outcome" in gate["run"] and "--report data_report.txt" in gate["run"]
    assert "FUNDING_GATE=fail" in gate["run"]  # gate crash -> withheld
    # Nothing in the funding steps deletes the restored state.
    for name in (
        "Restore funding state",
        "Extract funding (incremental)",
        "Stage funding and mark companions",
        "Gate and promote funding companions",
    ):
        run = _step(name)["run"]
        assert "rm -rf user_data/data/gmx/funding" not in run
        assert "rm -rf user_data/data/gmx/futures" not in run


def test_withheld_runs_publish_but_turn_the_job_red() -> None:
    step = _step("Fail job if funding companions were withheld")
    assert step["if"] == "always()"
    assert 'FUNDING_GATE:-fail}" != "pass"' in step["run"]
    assert "exit 1" in step["run"]
    release = _step("Create GitHub Release")["run"]
    assert "gmx-funding-state.tar.gz" in release
    assert "release_funding_companions.py notes" in release
    assert "funding_coverage.json" in release


def test_size_guard_covers_every_asset_at_1_8_gib() -> None:
    run = _step("Package tarballs")["run"]
    assert "1800 * 1024 * 1024" in run
    for asset in ("gmx-full", "gmx-light", "gmx-funding-state"):
        assert f"/tmp/{asset}.tar.gz" in run


# ---------------------------------------------------------------------------
# Execute the real shell of the packaging and restore steps against a fake tree.
# ---------------------------------------------------------------------------


def _run_step(
    name: str, cwd: Path, env_extra: dict[str, str] | None = None, extra_path: Path | None = None
) -> subprocess.CompletedProcess:
    script = _step(name)["run"].replace("/tmp/", f"{cwd}/out/")
    script = script.replace("${{ github.repository }}", "owner/repo")
    script = script.replace("sleep $((attempt * 10))", "sleep 0")
    (cwd / "out").mkdir(exist_ok=True)
    env = {**os.environ, **(env_extra or {})}
    if extra_path:
        env["PATH"] = f"{extra_path}:{env['PATH']}"
    return subprocess.run(
        ["bash", "-e", "-c", script], cwd=cwd, env=env, capture_output=True, text=True
    )


def _touch(path: Path, data: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def _gmx_tree(root: Path, *, companions: bool, state: bool) -> None:
    gmx = root / "user_data" / "data" / "gmx"
    for d in ("apy", "snapshots", "tickers", "volumes", "tick_volume", "ticks"):
        _touch(gmx / d / "f.parquet")
    _touch(gmx / "futures" / "BTC_USDC_USDC-1h-futures.feather")
    _touch(gmx / "futures" / "BTC_USDC_USDC-1m-futures.feather")
    if companions:
        _touch(gmx / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather")
        _touch(gmx / "futures" / "BTC_USDC_USDC-1h-mark.feather")
    _touch(gmx / "funding_coverage.json", b"{}")
    if state:
        _touch(gmx / "funding" / "arbitrum" / "raw" / "funding" / "BTC" / "data.parquet")
        _touch(gmx / "funding" / "arbitrum" / "checkpoints" / "funding_factor_checkpoint.json")


def _members(tarball: Path) -> list[str]:
    with tarfile.open(tarball) as tar:
        return tar.getnames()


@pytest.mark.parametrize("companions", [True, False])
def test_packaging_splits_state_out_and_keeps_light_to_funding_only(
    tmp_path: Path, companions: bool
) -> None:
    _gmx_tree(tmp_path, companions=companions, state=True)

    result = _run_step("Package tarballs", tmp_path)

    assert result.returncode == 0, result.stderr
    full = _members(tmp_path / "out" / "gmx-full.tar.gz")
    assert not any("/funding/" in m or m.endswith("/funding") for m in full)
    assert "user_data/data/gmx/funding_coverage.json" in full
    assert "user_data/data/gmx/futures/BTC_USDC_USDC-1m-futures.feather" in full
    assert ("user_data/data/gmx/futures/BTC_USDC_USDC-1h-mark.feather" in full) is companions

    state = _members(tmp_path / "out" / "gmx-funding-state.tar.gz")
    assert "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet" in state
    assert not any("futures" in m for m in state)

    light = _members(tmp_path / "out" / "gmx-light.tar.gz")
    funding = "user_data/data/gmx/futures/BTC_USDC_USDC-1h-funding_rate.feather"
    assert (funding in light) is companions
    assert "user_data/data/gmx/funding_coverage.json" in light
    assert "user_data/data/gmx/volumes/f.parquet" in light
    assert not any("mark" in m or "futures.feather" in m for m in light)
    assert not any("/funding/" in m for m in light)


def test_packaging_without_state_publishes_no_state_asset(tmp_path: Path) -> None:
    _gmx_tree(tmp_path, companions=True, state=False)

    result = _run_step("Package tarballs", tmp_path)

    assert result.returncode == 0, result.stderr
    assert (tmp_path / "out" / "gmx-full.tar.gz").exists()
    assert not (tmp_path / "out" / "gmx-funding-state.tar.gz").exists()
    assert "funding state" in result.stdout


_FAKE_GH = """#!/usr/bin/env bash
# Minimal `gh` stand-in driven by files under $FAKE_GH.
case "$1 $2" in
  "release list") cat "$FAKE_GH/tags.txt" ;;
  "release view") cat "$FAKE_GH/assets_$3.txt" 2>/dev/null || true ;;
  "release download")
    tag="$3"; dest=""; pattern=""
    shift 3
    while [ $# -gt 0 ]; do
      case "$1" in --dir) dest="$2"; shift;; --pattern) pattern="$2"; shift;; esac
      shift
    done
    [ -e "$FAKE_GH/fail_download" ] && exit 1
    cp "$FAKE_GH/$tag/$pattern" "$dest/$pattern" ;;
esac
"""


def _fake_gh(tmp_path: Path, *, tags: list[str], with_state: dict[str, bool]) -> Path:
    fake = tmp_path / "fake_gh"
    (fake / "bin").mkdir(parents=True)
    gh = fake / "bin" / "gh"
    gh.write_text(_FAKE_GH)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
    (fake / "tags.txt").write_text("\n".join(tags) + "\n")
    for tag, has in with_state.items():
        if not has:
            continue
        (fake / f"assets_{tag}.txt").write_text("gmx-full.tar.gz\ngmx-funding-state.tar.gz\n")
        build = tmp_path / f"build_{tag}"
        _touch(
            build / "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet", tag.encode()
        )
        (fake / tag).mkdir()
        with tarfile.open(fake / tag / "gmx-funding-state.tar.gz", "w:gz") as tar:
            tar.add(build / "user_data", arcname="user_data")
    return fake


def _restore(tmp_path: Path, fake: Path, previous: str = "data-2026-10-17"):
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    return work, _run_step(
        "Restore funding state",
        work,
        env_extra={"PREVIOUS_TAG": previous, "FAKE_GH": str(fake), "GH_TOKEN": "x"},
        extra_path=fake / "bin",
    )


def test_restore_uses_the_newest_release_that_carries_the_state(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path,
        tags=["data-2026-10-17", "data-2026-10-16", "data-2026-10-15"],
        with_state={"data-2026-10-16": True, "data-2026-10-15": True},
    )

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0, result.stderr
    restored = work / "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet"
    assert restored.read_bytes() == b"data-2026-10-16"
    assert "has no gmx-funding-state.tar.gz" in result.stdout


def test_restore_is_fail_soft_when_no_release_has_state(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=["data-2026-10-17"], with_state={})

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0
    assert not (work / "user_data/data/gmx/funding").exists()
    assert "::warning::" in result.stdout


def test_restore_failure_leaves_no_partial_state(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=["data-2026-10-17"], with_state={"data-2026-10-17": True})
    (fake / "fail_download").write_text("")

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0
    assert not (work / "user_data/data/gmx/funding").exists()
    assert "::error::could not restore" in result.stdout


def test_restore_does_nothing_without_a_previous_release(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[], with_state={})

    work, result = _restore(tmp_path, fake, previous="")

    assert result.returncode == 0
    assert not (work / "user_data").exists()


def test_restore_never_overwrites_an_existing_funding_dir(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=["data-2026-10-17"], with_state={"data-2026-10-17": True})
    work = tmp_path / "work"
    _touch(work / "user_data/data/gmx/funding/keep.txt", b"keep")

    work, result = _restore(tmp_path, fake)

    assert (work / "user_data/data/gmx/funding/keep.txt").read_bytes() == b"keep"
    assert result.returncode == 0
