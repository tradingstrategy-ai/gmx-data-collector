"""``funding-catchup.yml``: dispatch-only catch-up that persists progress as a state asset."""

import json
import os
import re
import subprocess
import sys
from pathlib import Path

import yaml

from tests.test_release_funding_workflow import (
    NEW_STATE,
    OLD_STATE,
    REPO,
    _assets,
    _calls,
    _deletes,
    _fake_gh,
    _touch,
)
from tests.test_release_funding_workflow import (
    WORKFLOW as RELEASE_WORKFLOW,
)

CATCHUP = Path(".github/workflows/funding-catchup.yml")
FUNDING = "user_data/data/gmx/funding/arbitrum"
OLDER_TAG = "data-2026-10-16"
LATEST_TAG = "data-2026-10-17"


def _doc(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


def _steps() -> list[dict]:
    return _doc(CATCHUP)["jobs"]["catchup"]["steps"]


def _step(name: str) -> dict:
    matches = [s for s in _steps() if s.get("name") == name]
    assert len(matches) == 1, f"expected exactly one step named {name!r}"
    return matches[0]


def _index(name: str) -> int:
    return [s.get("name") for s in _steps()].index(name)


def _normalised(run: str) -> list[str]:
    return [re.sub(r"\s+", " ", line.strip()) for line in run.strip().splitlines()]


# ---------------------------------------------------------------------------
# Workflow shape
# ---------------------------------------------------------------------------


def test_both_workflows_share_one_non_cancelling_concurrency_group() -> None:
    groups = []
    for path in (CATCHUP, RELEASE_WORKFLOW):
        concurrency = _doc(path)["concurrency"]
        assert concurrency["cancel-in-progress"] is False, path
        groups.append(concurrency["group"])
    assert groups[0] == groups[1] == "gmx-funding-state"


def test_catchup_is_dispatch_only_with_a_six_hour_job() -> None:
    doc = _doc(CATCHUP)
    assert list(doc[True]) == ["workflow_dispatch"]  # PyYAML parses `on` as True
    job = doc["jobs"]["catchup"]
    assert job["timeout-minutes"] == 360
    assert doc["permissions"] == {"contents": "write"}


def test_extraction_has_a_330_minute_cap_inside_the_job_cap() -> None:
    step = _step("Extract funding to chain head")
    assert step["timeout-minutes"] == 330 < _doc(CATCHUP)["jobs"]["catchup"]["timeout-minutes"]
    # A timeout must not stop the steps that save the progress.
    assert step["continue-on-error"] is True
    assert step["id"] == "funding_extract"
    assert set(step["env"]) == {"JSON_RPC_ARBITRUM", "HYPERSYNC_API_TOKEN"}


def test_extract_command_is_the_one_the_daily_release_runs() -> None:
    release = next(
        s
        for s in _doc(RELEASE_WORKFLOW)["jobs"]["release"]["steps"]
        if s.get("name") == "Extract funding (incremental)"
    )
    assert _normalised(_step("Extract funding to chain head")["run"]) == _normalised(release["run"])


def test_steps_run_in_contract_order() -> None:
    order = [
        "Set up uv",
        "Install Python dependencies",
        "Resolve newest data release",
        "Restore funding state",
        "Preflight funding state",
        "Require a restored funding state",
        "Record checkpoints before",
        "Extract funding to chain head",
        "Report catch-up",
        "Upload advanced funding state",
        "Fail job if catch-up did not finish",
    ]
    indexes = [_index(name) for name in order]
    assert indexes == sorted(indexes)


def test_progress_is_saved_whatever_happened_to_the_extraction() -> None:
    report = _step("Report catch-up")
    upload = _step("Upload advanced funding state")
    assert report["if"] == "always()"
    assert report["id"] == "catchup_report"
    assert "always()" in upload["if"]
    assert "steps.catchup_report.outputs.advanced == 'true'" in upload["if"]
    # Not conditioned on the extraction outcome: a timeout is the expected case.
    assert "funding_extract" not in upload["if"]


def test_catchup_leaves_consumer_bundles_and_notes_alone() -> None:
    text = CATCHUP.read_text()
    for forbidden in (
        "gmx-full",
        "gmx-light",
        "release create",
        "release edit",
        "release delete ",
        "--latest",
        "--notes",
        "release_funding_companions.py gate",
        "release_funding_companions.py export",
    ):
        assert forbidden not in text, forbidden


def test_shared_shell_lives_in_one_script_not_copied() -> None:
    script = Path("scripts/funding_state_assets.sh").read_text()
    assert "delete-asset" in script
    for path in (CATCHUP, RELEASE_WORKFLOW):
        text = path.read_text()
        assert "scripts/funding_state_assets.sh restore" in text
        assert "delete-asset" not in text
    assert "--clobber" in script.split("cmd_publish()")[0]  # restore re-downloads over a temp dir
    assert "--clobber" not in script.split("cmd_publish()")[1]  # never replaced in place


# ---------------------------------------------------------------------------
# The shared shell, run for real against the stateful fake gh
# ---------------------------------------------------------------------------


def _script(args: str, cwd: Path, fake: Path, **env: str) -> subprocess.CompletedProcess:
    for link in ("src", "scripts"):
        if not (cwd / link).exists():
            (cwd / link).symlink_to(REPO / link)
    full_env = {
        **os.environ,
        "GITHUB_REPOSITORY": "owner/repo",
        "FAKE_GH": str(fake),
        "FAKE_UPLOAD_FAIL": "",
        "GH_TOKEN": "x",
        "RESTORE_RETRY_SECONDS": "0",
        "GITHUB_ENV": str(cwd.parent / "github_env"),
        **env,
    }
    full_env["PATH"] = f"{Path(sys.executable).parent}:{fake / 'bin'}:{full_env['PATH']}"
    return subprocess.run(
        ["bash", "-e", "-c", f"bash scripts/funding_state_assets.sh {args}"],
        cwd=cwd,
        env=full_env,
        capture_output=True,
        text=True,
    )


def _work(tmp_path: Path) -> Path:
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    return work


def _write_checkpoints(work: Path, factor: int, fee: int) -> None:
    for name, symbol, block in (
        ("funding_factor_checkpoint.json", "funding_factor_all", factor),
        ("fee_per_size_checkpoint.json", "fee_per_size_all", fee),
    ):
        _touch(
            work / FUNDING / "checkpoints" / name,
            json.dumps({"symbol": symbol, "last_block": block, "last_timestamp": block}).encode(),
        )


def test_restore_records_the_release_the_state_came_from(tmp_path: Path) -> None:
    fake = _fake_gh(
        tmp_path,
        tags=[LATEST_TAG, OLDER_TAG],
        state_assets={OLDER_TAG: [OLD_STATE]},
    )
    work = _work(tmp_path)

    result = _script("restore", work, fake, PREVIOUS_TAG=LATEST_TAG)

    assert result.returncode == 0, result.stderr
    env_file = (tmp_path / "github_env").read_text()
    assert f"FUNDING_STATE_RELEASE={OLDER_TAG}" in env_file
    assert f"FUNDING_STATE_ASSET={OLD_STATE}" in env_file


def test_latest_tag_picks_the_newest_data_release(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[LATEST_TAG, OLDER_TAG])

    result = _script("latest-tag", _work(tmp_path), fake)

    assert result.stdout.strip() == LATEST_TAG


def test_latest_tag_fails_when_releases_cannot_be_listed(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[LATEST_TAG])
    (fake / "list_fail").write_text("")

    assert _script("latest-tag", _work(tmp_path), fake).returncode != 0


def _stamped(work: Path, name: str = NEW_STATE) -> Path:
    _touch(work / FUNDING / "raw" / "funding" / "BTC" / "data.parquet", b"advanced")
    result = subprocess.run(
        ["tar", "-czf", name, "user_data/data/gmx/funding/"], cwd=work, capture_output=True
    )
    assert result.returncode == 0, result.stderr
    return work / name


def test_publish_uploads_then_confirms_size_then_deletes_older_state(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[LATEST_TAG, OLDER_TAG], state_assets={OLDER_TAG: [OLD_STATE]})
    work = _work(tmp_path)
    archive = _stamped(work)

    result = _script(f"publish {OLDER_TAG} {archive}", work, fake)

    assert result.returncode == 0, result.stderr
    assert sorted(a for a in _assets(fake, OLDER_TAG) if "state" in a) == [NEW_STATE]
    calls = _calls(fake)
    assert not any(NEW_STATE in c and "--clobber" in c for c in calls)
    upload = next(i for i, c in enumerate(calls) if c.startswith("release upload"))
    verify = next(i for i, c in enumerate(calls) if c.startswith("release view") and "select" in c)
    delete = next(i for i, c in enumerate(calls) if c.startswith("release delete-asset"))
    assert upload < verify < delete
    assert not any(c.startswith("release delete ") for c in calls)


def test_publish_with_a_failed_name_inspection_deletes_nothing(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[OLDER_TAG], state_assets={OLDER_TAG: [OLD_STATE]})
    work = _work(tmp_path)
    archive = _stamped(work)
    (fake / "view_fail_names").write_text("")

    result = _script(f"publish {OLDER_TAG} {archive}", work, fake)

    assert _deletes(fake) == []
    assert {OLD_STATE, NEW_STATE} <= set(_assets(fake, OLDER_TAG))
    assert "::warning::" in result.stdout


def test_publish_with_an_unconfirmable_upload_deletes_nothing_and_fails(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[OLDER_TAG], state_assets={OLDER_TAG: [OLD_STATE]})
    work = _work(tmp_path)
    archive = _stamped(work)
    (fake / "view_fail_all").write_text("")

    result = _script(f"publish {OLDER_TAG} {archive}", work, fake)

    assert result.returncode != 0
    assert _deletes(fake) == []
    assert OLD_STATE in _assets(fake, OLDER_TAG)


def test_publish_with_a_failed_upload_keeps_the_old_state_and_fails(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[OLDER_TAG], state_assets={OLDER_TAG: [OLD_STATE]})
    work = _work(tmp_path)
    archive = _stamped(work)

    result = _script(
        f"publish {OLDER_TAG} {archive}", work, fake, FAKE_UPLOAD_FAIL="gmx-funding-state-*"
    )

    assert result.returncode != 0
    assert _deletes(fake) == []
    assert _assets(fake, OLDER_TAG).count(OLD_STATE) == 1 and NEW_STATE not in _assets(
        fake, OLDER_TAG
    )


def test_publish_no_delete_flag_keeps_every_older_asset(tmp_path: Path) -> None:
    fake = _fake_gh(tmp_path, tags=[OLDER_TAG], state_assets={OLDER_TAG: [OLD_STATE]})
    work = _work(tmp_path)
    archive = _stamped(work)

    result = _script(f"publish {OLDER_TAG} {archive} --no-delete", work, fake)

    assert result.returncode == 0, result.stderr
    assert _deletes(fake) == []
    assert {OLD_STATE, NEW_STATE} <= set(_assets(fake, OLDER_TAG))


# ---------------------------------------------------------------------------
# The whole timeout path: killed extraction -> report -> upload to the source release
# ---------------------------------------------------------------------------


def _run_py(args: list[str], work: Path, **env: str) -> subprocess.CompletedProcess:
    for link in ("src", "scripts"):
        if not (work / link).exists():
            (work / link).symlink_to(REPO / link)
    full_env = {**os.environ, "PYTHONPATH": "src", **env}
    return subprocess.run(
        [sys.executable, "scripts/release_funding_companions.py", *args],
        cwd=work,
        env=full_env,
        capture_output=True,
        text=True,
    )


def _outputs(path: Path) -> dict[str, str]:
    return dict(line.split("=", 1) for line in path.read_text().splitlines() if "=" in line)


def test_a_timed_out_extraction_that_advanced_is_uploaded_to_its_source_release(
    tmp_path: Path,
) -> None:
    fake = _fake_gh(
        tmp_path,
        tags=[LATEST_TAG, OLDER_TAG],
        state_assets={OLDER_TAG: [OLD_STATE]},
    )
    work = _work(tmp_path)
    gmx = "user_data/data/gmx"
    outputs = tmp_path / "github_output"
    summary = tmp_path / "summary.md"
    gh_env = tmp_path / "github_env"
    outputs.write_text("")
    summary.write_text("")
    gh_env.write_text("")

    assert _script("restore", work, fake, PREVIOUS_TAG=LATEST_TAG).returncode == 0
    _write_checkpoints(work, 1_000, 2_000)
    before = tmp_path / "before.json"
    assert _run_py(["checkpoints", "--gmx-root", gmx, "--out", str(before)], work).returncode == 0

    # The extraction made progress and was then cancelled by the step timeout.
    _write_checkpoints(work, 5_000, 6_000)

    report = _run_py(
        [
            "catchup-report",
            "--gmx-root",
            gmx,
            "--before",
            str(before),
            "--extract-outcome",
            "cancelled",
        ],
        work,
        GITHUB_OUTPUT=str(outputs),
        GITHUB_STEP_SUMMARY=str(summary),
    )
    assert report.returncode == 0, report.stderr
    assert _outputs(outputs)["advanced"] == "true"
    assert _outputs(outputs)["head_reached"] == "false"
    assert "cancelled" in summary.read_text()

    env = {}
    funding_release = _outputs(gh_env)["FUNDING_STATE_RELEASE"]
    assert funding_release == OLDER_TAG
    out = tmp_path / "out"
    out.mkdir()
    pack = _script(f"pack {out}", work, fake, **env)
    assert pack.returncode == 0, pack.stderr
    stamped = sorted(out.glob("gmx-funding-state-*.tar.gz"))[-1]
    upload = _script(f"publish {funding_release} {stamped}", work, fake, **env)
    assert upload.returncode == 0, upload.stderr

    state_assets = [a for a in _assets(fake, OLDER_TAG) if "state" in a]
    assert len(state_assets) == 1 and state_assets[0] != OLD_STATE
    assert not any("state" in a for a in _assets(fake, LATEST_TAG))
    assert not any(c.startswith("release create") for c in _calls(fake))


# ---------------------------------------------------------------------------
# The report: before/after checkpoints and the gate-B4 head check
# ---------------------------------------------------------------------------


def _checkpoint_json(path: Path, blocks: tuple[int, int], stamps: tuple[int, int]) -> None:
    for name, block, stamp in zip(
        ("funding_factor_checkpoint.json", "fee_per_size_checkpoint.json"),
        blocks,
        stamps,
        strict=True,
    ):
        _touch(
            path / FUNDING / "checkpoints" / name,
            json.dumps({"last_block": block, "last_timestamp": stamp}).encode(),
        )


def _report(tmp_path: Path, before_blocks, after_blocks, after_stamps, outcome="success", now=None):
    from datetime import UTC, datetime

    from gmx_historical_data import funding_catchup

    _checkpoint_json(tmp_path / "a", before_blocks, (1_700_000_000, 1_700_000_000))
    _checkpoint_json(tmp_path / "b", after_blocks, after_stamps)
    before = funding_catchup.snapshot(tmp_path / "a" / "user_data/data/gmx")
    return funding_catchup.build_report(
        before,
        tmp_path / "b" / "user_data/data/gmx",
        now=now or datetime.fromtimestamp(1_800_000_000, tz=UTC),
        extract_outcome=outcome,
    )


def test_report_flags_advance_and_head_reached(tmp_path: Path) -> None:
    result = _report(tmp_path, (10, 20), (50, 60), (1_799_999_000, 1_799_999_500))
    assert result.advanced is True and result.head_reached is True
    assert "Head reached: **yes**" in result.markdown


def test_report_head_not_reached_when_a_checkpoint_lags_beyond_the_b4_limit(
    tmp_path: Path,
) -> None:
    # 7h behind > the 6h head_lag_hours gate B4 uses.
    result = _report(tmp_path, (10, 20), (50, 60), (1_800_000_000 - 7 * 3600, 1_799_999_500))
    assert result.advanced is True and result.head_reached is False
    assert "Head reached: **no**" in result.markdown


def test_report_head_reached_needs_a_successful_extraction(tmp_path: Path) -> None:
    result = _report(
        tmp_path, (10, 20), (50, 60), (1_799_999_000, 1_799_999_500), outcome="failure"
    )
    assert result.head_reached is False


def test_report_no_progress_is_not_an_advance(tmp_path: Path) -> None:
    result = _report(tmp_path, (10, 20), (10, 20), (1_700_000_000, 1_700_000_000))
    assert result.advanced is False


def test_report_never_uploads_a_regressed_or_unreadable_state(tmp_path: Path) -> None:
    assert _report(tmp_path, (10, 20), (50, 5), (1_700_000_100, 1_700_000_100)).advanced is False
    from gmx_historical_data import funding_catchup

    after = {"funding_factor": {"last_block": 50, "last_timestamp": 1}, "fee_per_size": None}
    before = {
        "funding_factor": {"last_block": 10, "last_timestamp": 1},
        "fee_per_size": {"last_block": 20, "last_timestamp": 1},
    }
    assert funding_catchup.has_advanced(before, after) is False


def test_catchup_report_cli_without_a_before_file_reports_no_advance(tmp_path: Path) -> None:
    outputs = tmp_path / "out"
    outputs.write_text("")
    result = _run_py(
        [
            "catchup-report",
            "--gmx-root",
            "user_data/data/gmx",
            "--before",
            str(tmp_path / "missing.json"),
            "--extract-outcome",
            "skipped",
        ],
        _work(tmp_path),
        GITHUB_OUTPUT=str(outputs),
    )
    assert result.returncode == 0, result.stderr
    assert _outputs(outputs) == {"advanced": "false", "head_reached": "false"}
