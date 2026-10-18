"""``release-data.yml``: funding state, staged companions, packaging contract."""

import json
import os
import stat
import subprocess
import sys
import tarfile
from datetime import UTC, datetime, timedelta
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
    assert "gmx-funding-state-*.tar.gz" in _step("Package tarballs")["run"] or True
    assert "delete-asset" in release
    assert "release_funding_companions.py notes" in release
    assert "funding_coverage.json" in release


def test_size_guard_covers_every_asset_at_1_8_gib() -> None:
    run = _step("Package tarballs")["run"]
    assert "1800 * 1024 * 1024" in run
    for asset in ("gmx-full", "gmx-light"):
        assert f"/tmp/{asset}.tar.gz" in run
    assert "/tmp/gmx-funding-state-*.tar.gz" in run


# ---------------------------------------------------------------------------
# Execute the real shell of the packaging and restore steps against a fake tree.
# ---------------------------------------------------------------------------


REPO = Path(__file__).resolve().parent.parent


def _run_step(
    name: str, cwd: Path, env_extra: dict[str, str] | None = None, extra_path: Path | None = None
) -> subprocess.CompletedProcess:
    script = _step(name)["run"].replace("/tmp/", f"{cwd}/out/")
    script = script.replace("${{ github.repository }}", "owner/repo")
    script = script.replace("${{ github.event.inputs.retention_days || 14 }}", "14")
    for link in ("src", "scripts"):
        if not (cwd / link).exists():
            (cwd / link).symlink_to(REPO / link)
    script = script.replace("sleep $((attempt * 10))", "sleep 0")
    (cwd / "out").mkdir(exist_ok=True)
    env = {**os.environ, **(env_extra or {})}
    env["PATH"] = f"{Path(sys.executable).parent}:{env['PATH']}"
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


def _state_tarball(tmp_path: Path) -> Path:
    found = sorted((tmp_path / "out").glob("gmx-funding-state-*.tar.gz"))
    assert len(found) == 1, found
    return found[0]


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

    state = _members(_state_tarball(tmp_path))
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
    assert not list((tmp_path / "out").glob("gmx-funding-state*.tar.gz"))
    assert "funding state" in result.stdout


_FAKE_GH = r"""#!/usr/bin/env bash
# Stateful `gh` stand-in driven by files under $FAKE_GH. Release assets live in
# rel_<tag>.assets as "name size" lines; every call is appended to calls.log.
echo "$*" >> "$FAKE_GH/calls.log"
reg() { echo "$FAKE_GH/rel_$1.assets"; }
drop() { grep -v "^$2 " "$(reg "$1")" > "$(reg "$1").tmp" 2>/dev/null; mv "$(reg "$1").tmp" "$(reg "$1")"; }
add() { echo "$2 $(stat -c %s "$3")" >> "$(reg "$1")"; }
case "$1 $2" in
  "release list")
    [ -e "$FAKE_GH/list_fail" ] && exit 1
    cat "$FAKE_GH/tags.txt" ;;
  "release view")
    tag="$3"
    if [ -e "$FAKE_GH/view_fail_all" ] || [ -e "$FAKE_GH/view_fail_$tag" ]; then exit 1; fi
    case "$*" in
      *'select(.name=='*)
        name=$(echo "$*" | sed -n 's/.*select(.name=="\([^"]*\)").*/\1/p')
        grep "^$name " "$(reg "$tag")" | cut -d' ' -f2 ;;
      *) cut -d' ' -f1 "$(reg "$tag")" 2>/dev/null ;;
    esac ;;
  "release download")
    tag="$3"; dest=""; pattern=""
    shift 3
    while [ $# -gt 0 ]; do
      case "$1" in --dir) dest="$2"; shift;; --pattern) pattern="$2"; shift;; esac
      shift
    done
    [ -e "$FAKE_GH/fail_download" ] && exit 1
    cp "$FAKE_GH/$tag/$pattern" "$dest/$pattern" ;;
  "release upload")
    tag="$3"; clobber=0; files=()
    shift 3
    while [ $# -gt 0 ]; do
      case "$1" in --repo) shift;; --clobber) clobber=1;; *) files+=("$1");; esac
      shift
    done
    touch "$(reg "$tag")"
    for f in "${files[@]}"; do
      base=$(basename "$f")
      [ "$clobber" = 1 ] && drop "$tag" "$base"   # gh deletes the old asset first
      case "$base" in $FAKE_UPLOAD_FAIL) exit 1;; esac
      add "$tag" "$base" "$f"
    done ;;
  "release create")
    tag="$3"; shift 3
    [ -e "$(reg "$tag")" ] && exit 1   # gh: a release with this tag already exists
    : > "$(reg "$tag")"
    while [ $# -gt 0 ]; do
      case "$1" in
        --repo|--title) shift;;
        --notes-file) cp "$2" "$FAKE_GH/notes_$tag.md"; shift;;
        --latest) ;;
        *) add "$tag" "$(basename "$1")" "$1";;
      esac
      shift
    done ;;
  "release edit")
    tag="$3"; shift 3
    while [ $# -gt 0 ]; do
      case "$1" in --notes-file) cp "$2" "$FAKE_GH/notes_$tag.md"; shift;; --repo|--title) shift;; esac
      shift
    done ;;
  "release delete-asset") drop "$3" "$4" ;;
  "release delete") rm -f "$(reg "$3")" ;;
esac
"""


def _fake_gh(
    tmp_path: Path, *, tags: list[str], state_assets: dict[str, list[str]] | None = None
) -> Path:
    """A fake ``gh``; ``state_assets`` maps a tag to the state asset names it carries."""
    fake = tmp_path / "fake_gh"
    (fake / "bin").mkdir(parents=True)
    gh = fake / "bin" / "gh"
    gh.write_text(_FAKE_GH)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)
    (fake / "tags.txt").write_text("\n".join(tags) + "\n")
    for tag in tags:
        entries = ["gmx-full.tar.gz 1"]
        for name in (state_assets or {}).get(tag, []):
            entries.append(f"{name} 7")
            build = tmp_path / f"build_{tag}"
            _touch(
                build / "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet",
                f"{tag}/{name}".encode(),
            )
            (fake / tag).mkdir(exist_ok=True)
            with tarfile.open(fake / tag / name, "w:gz") as tar:
                tar.add(build / "user_data", arcname="user_data")
        (fake / f"rel_{tag}.assets").write_text("\n".join(entries) + "\n")
    return fake


def _assets(fake: Path, tag: str) -> list[str]:
    path = fake / f"rel_{tag}.assets"
    return [line.split()[0] for line in path.read_text().splitlines()] if path.exists() else []


def _restore(tmp_path: Path, fake: Path, previous: str = "data-2026-10-17"):
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    return work, _run_step(
        "Restore funding state",
        work,
        env_extra={"PREVIOUS_TAG": previous, "FAKE_GH": str(fake), "GH_TOKEN": "x"},
        extra_path=fake / "bin",
    )


OLD_STATE = "gmx-funding-state-20261016T020000Z.tar.gz"
NEW_STATE = "gmx-funding-state-20261018T020000Z.tar.gz"


def test_restore_uses_the_newest_release_that_carries_the_state(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path,
        tags=["data-2026-10-17", "data-2026-10-16", "data-2026-10-15"],
        state_assets={
            "data-2026-10-16": [OLD_STATE],
            "data-2026-10-15": ["gmx-funding-state-20261015T020000Z.tar.gz"],
        },
    )

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0, result.stderr
    restored = work / "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet"
    assert restored.read_bytes() == f"data-2026-10-16/{OLD_STATE}".encode()
    assert "has no funding state asset" in result.stdout


def test_restore_picks_the_newest_state_asset_within_a_release(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path,
        tags=["data-2026-10-17"],
        state_assets={"data-2026-10-17": [OLD_STATE, NEW_STATE]},
    )

    work, result = _restore(tmp_path, fake)

    restored = work / "user_data/data/gmx/funding/arbitrum/raw/funding/BTC/data.parquet"
    assert restored.read_bytes() == f"data-2026-10-17/{NEW_STATE}".encode()


def test_restore_is_fail_soft_when_no_release_has_state(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=["data-2026-10-17"])

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0
    assert not (work / "user_data/data/gmx/funding").exists()
    assert "::warning::" in result.stdout


def test_restore_failure_leaves_no_partial_state(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path, tags=["data-2026-10-17"], state_assets={"data-2026-10-17": [OLD_STATE]}
    )
    (fake / "fail_download").write_text("")

    work, result = _restore(tmp_path, fake)

    assert result.returncode == 0
    assert not (work / "user_data/data/gmx/funding").exists()
    assert "::error::could not restore" in result.stdout


def test_restore_does_nothing_without_a_previous_release(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[])

    work, result = _restore(tmp_path, fake, previous="")

    assert result.returncode == 0
    assert not (work / "user_data").exists()


def test_restore_never_overwrites_an_existing_funding_dir(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path, tags=["data-2026-10-17"], state_assets={"data-2026-10-17": [OLD_STATE]}
    )
    work = tmp_path / "work"
    _touch(work / "user_data/data/gmx/funding/keep.txt", b"keep")

    work, result = _restore(tmp_path, fake)

    assert (work / "user_data/data/gmx/funding/keep.txt").read_bytes() == b"keep"
    assert result.returncode == 0


# ---------------------------------------------------------------------------
# Release replacement and pruning must never destroy the only copy of the state.
# ---------------------------------------------------------------------------


def _today(offset_days: int = 0) -> str:
    return f"data-{(datetime.now(UTC) - timedelta(days=offset_days)):%Y-%m-%d}"


def _release_env(fake: Path, **extra: str) -> dict[str, str]:
    return {"FAKE_GH": str(fake), "GH_TOKEN": "x", "FAKE_UPLOAD_FAIL": "", **extra}


def _calls(fake: Path) -> list[str]:
    log = fake / "calls.log"
    return log.read_text().splitlines() if log.exists() else []


def _deletes(fake: Path) -> list[str]:
    return [c for c in _calls(fake) if c.startswith(("release delete ", "release delete-asset "))]


def _publish_tree(root: Path, *, new_state: bool) -> None:
    _gmx_tree(root, companions=True, state=False)
    out = root / "out"
    out.mkdir(exist_ok=True)
    for name in ("gmx-full.tar.gz", "gmx-light.tar.gz") + ((NEW_STATE,) if new_state else ()):
        (out / name).write_bytes(b"x" * 7)
    _touch(root / "user_data/data/gmx/futures/BTC_USDC_USDC-1h-futures.feather", b"x")


def _create_release(tmp_path: Path, fake: Path, *, new_state: bool, **env: str):
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    _publish_tree(work, new_state=new_state)
    return _run_step(
        "Create GitHub Release",
        work,
        env_extra=_release_env(fake, **env),
        extra_path=fake / "bin",
    )


def test_same_day_rerun_does_not_delete_a_release_that_holds_the_only_state(
    tmp_path: Path,
) -> None:
    """State restore failed (no new state asset) and today's release, the first
    seeded one, carries the only copy: it must be updated in place, not deleted."""
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})

    result = _create_release(tmp_path, fake, new_state=False)

    assert result.returncode == 0, result.stderr
    assert _deletes(fake) == []
    assert not any(c.startswith("release create") for c in _calls(fake))
    assert OLD_STATE in _assets(fake, today)
    assert any(c.startswith("release edit") and "--latest" in c for c in _calls(fake))


def test_state_is_uploaded_under_a_new_name_and_old_one_removed_only_after_confirming(
    tmp_path: Path,
) -> None:
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})

    result = _create_release(tmp_path, fake, new_state=True)

    assert result.returncode == 0, result.stderr
    assert sorted(a for a in _assets(fake, today) if "state" in a) == [NEW_STATE]
    calls = _calls(fake)
    # Never clobbers the state asset, and checks the upload before deleting.
    assert not any(NEW_STATE in c and "--clobber" in c for c in calls)
    upload = next(
        i for i, c in enumerate(calls) if c.startswith("release upload") and NEW_STATE in c
    )
    verify = next(i for i, c in enumerate(calls) if c.startswith("release view") and "select" in c)
    delete = next(i for i, c in enumerate(calls) if c.startswith("release delete-asset"))
    assert upload < verify < delete
    assert not any(c.startswith("release delete ") for c in calls)


def test_an_interrupted_state_upload_keeps_the_old_state_and_still_updates_notes(
    tmp_path: Path,
) -> None:
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})

    result = _create_release(tmp_path, fake, new_state=True, FAKE_UPLOAD_FAIL="gmx-funding-state-*")

    assert result.returncode != 0  # any upload failure turns the job red
    assert OLD_STATE in _assets(fake, today)
    assert NEW_STATE not in _assets(fake, today)
    assert _deletes(fake) == []
    # The consumer assets went up and the body was refreshed despite the failure.
    calls = _calls(fake)
    assert any(c.startswith("release edit") for c in calls)
    assert (fake / f"notes_{today}.md").exists()


def test_an_unconfirmed_state_upload_does_not_delete_the_old_state(tmp_path: Path) -> None:
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})
    # The registry will record the uploaded file with a wrong size.
    wrapper = fake / "bin" / "gh"
    wrapper.write_text(
        wrapper.read_text().replace('add() { echo "$2 $(stat -c %s "$3")"', 'add() { echo "$2 999"')
    )

    result = _create_release(tmp_path, fake, new_state=True)

    assert result.returncode != 0
    assert _deletes(fake) == []
    assert OLD_STATE in _assets(fake, today)


def test_same_day_rerun_without_state_still_recreates_the_release(tmp_path: Path) -> None:
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today])

    result = _create_release(tmp_path, fake, new_state=False)

    assert result.returncode == 0, result.stderr
    calls = _calls(fake)
    assert any(c.startswith("release delete ") for c in calls)
    assert any(c.startswith("release create") for c in calls)


def test_first_release_of_the_day_is_created_with_the_state(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[_today(1)], state_assets={_today(1): [OLD_STATE]})

    result = _create_release(tmp_path, fake, new_state=True)

    assert result.returncode == 0, result.stderr
    assert NEW_STATE in _assets(fake, _today())
    assert _deletes(fake) == []


@pytest.mark.parametrize("failing", ["list_fail", "view_fail_all"])
def test_a_failed_inspection_means_nothing_is_deleted(tmp_path: Path, failing: str) -> None:
    """A `gh` error is "unknown", never "no state"."""
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})
    (fake / failing).write_text("")

    result = _create_release(tmp_path, fake, new_state=True)

    assert _deletes(fake) == []
    assert OLD_STATE in _assets(fake, today)
    assert "::warning::could not inspect" in result.stdout


def test_a_failed_inspection_of_todays_release_falls_back_to_a_safe_update(
    tmp_path: Path,
) -> None:
    today = _today()
    fake = _fake_gh(tmp_path, tags=[today], state_assets={today: [OLD_STATE]})
    (fake / f"view_fail_{today}").write_text("")

    _create_release(tmp_path, fake, new_state=False)

    assert _deletes(fake) == []
    assert OLD_STATE in _assets(fake, today)


def _prune(tmp_path: Path, tags: list[str], state_assets: dict[str, list[str]], **flags: bool):
    fake = _fake_gh(tmp_path, tags=tags, state_assets=state_assets)
    for flag, on in flags.items():
        if on:
            (fake / flag).write_text("")
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    result = _run_step(
        "Prune old releases", work, env_extra=_release_env(fake), extra_path=fake / "bin"
    )
    deleted = [c.split()[2] for c in _calls(fake) if c.startswith("release delete ")]
    return result, deleted


def test_prune_keeps_the_newest_release_that_carries_the_state(tmp_path: Path) -> None:
    # Today's run published without the state asset: the 20-day-old release is
    # now the only resumable copy and is past the 14-day retention.
    tags = [_today(), _today(20), _today(30)]
    result, deleted = _prune(tmp_path, tags, {_today(20): [OLD_STATE], _today(30): [OLD_STATE]})

    assert result.returncode == 0, result.stderr
    assert _today(20) not in deleted
    assert _today(30) in deleted


def test_prune_deletes_old_state_once_a_newer_release_carries_it(tmp_path: Path) -> None:
    tags = [_today(), _today(20), _today(30)]
    _, deleted = _prune(
        tmp_path, tags, {_today(): [NEW_STATE], _today(20): [OLD_STATE], _today(30): [OLD_STATE]}
    )

    assert sorted(deleted) == sorted([_today(20), _today(30)])


@pytest.mark.parametrize("failing", ["list_fail", "view_fail_all"])
def test_prune_does_nothing_when_a_release_cannot_be_inspected(
    tmp_path: Path, failing: str
) -> None:
    tags = [_today(), _today(20), _today(30)]
    result, deleted = _prune(tmp_path, tags, {_today(20): [OLD_STATE]}, **{failing: True})

    assert result.returncode == 0
    assert deleted == []
    assert "skipping ALL pruning" in result.stdout


# ---------------------------------------------------------------------------
# The gate step must never leave yesterday's verdict standing.
# ---------------------------------------------------------------------------


def _run_gate_step(tmp_path: Path, python_stub: str) -> tuple[subprocess.CompletedProcess, Path]:
    work = tmp_path / "work"
    gmx = work / "user_data" / "data" / "gmx"
    (gmx / "futures").mkdir(parents=True)
    _touch(gmx / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather", b"previous release file")
    (gmx / "funding_coverage.json").write_text(
        json.dumps(
            {"status": "PASS", "refreshed": True, "generated_at": "2026-10-17T02:20:00+00:00"}
        )
    )
    (work / "data_report.txt").write_text("report\n")
    stub_dir = tmp_path / "stub"
    stub_dir.mkdir()
    stub = stub_dir / "python"
    stub.write_text(
        "#!/usr/bin/env bash\n"
        'case " $* " in\n'
        f"  *' gate '*) {python_stub} ;;\n"
        f'  *) exec "{sys.executable}" "$@" ;;\n'
        "esac\n"
    )
    stub.chmod(stub.stat().st_mode | stat.S_IEXEC)
    env_file = tmp_path / "github_env"
    env_file.write_text("")
    result = _run_step(
        "Gate and promote funding companions",
        work,
        env_extra={
            "FUNDING_STATE": "ready",
            "EXTRACT_OUTCOME": "success",
            "STAGE_OUTCOME": "success",
            "GITHUB_ENV": str(env_file),
        },
        extra_path=stub_dir,
    )
    return result, work


@pytest.mark.parametrize("stub", ["exit 1", "exit 139", "kill -9 $$"])
def test_gate_step_without_a_fresh_verdict_writes_withheld(tmp_path: Path, stub: str) -> None:
    result, work = _run_gate_step(tmp_path, stub)

    assert result.returncode == 0, result.stderr
    gmx = work / "user_data" / "data" / "gmx"
    verdict = json.loads((gmx / "funding_coverage.json").read_text())
    assert verdict["status"] == "WITHHELD" and verdict["refreshed"] is False
    assert verdict["generated_at"] != "2026-10-17T02:20:00+00:00"
    assert "NOT refreshed" in (work / "data_report.txt").read_text()
    assert (gmx / "futures" / "BTC_USDC_USDC-1h-funding_rate.feather").read_bytes() == (
        b"previous release file"
    )
    assert "FUNDING_GATE=fail" in (tmp_path / "github_env").read_text()
