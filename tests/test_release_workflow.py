from pathlib import Path

WORKFLOW = Path(".github/workflows/release-data.yml")


def test_release_workflow_requires_previous_release_unless_explicit_seed() -> None:
    text = WORKFLOW.read_text()

    assert "allow_fresh_seed" in text
    assert "ALLOW_FRESH_SEED" in text
    assert "No previous release found" not in text
    assert "exit 1" in text


def test_release_workflow_restores_explicit_data_release_tag() -> None:
    text = WORKFLOW.read_text()

    assert "PREVIOUS_TAG" in text
    assert '--pattern "gmx-full.tar.gz"' in text
    assert '"${PREVIOUS_TAG}"' in text
    assert "gh release download \\" in text


def test_release_workflow_validates_history_and_partial_fetches() -> None:
    text = WORKFLOW.read_text()

    assert "Snapshot restored candle history" in text
    assert "/tmp/gmx-restore-manifest.json" in text
    assert "Validate candle history integrity" in text
    assert "collector reported failed OHLCV fetches" in text


def test_release_workflow_snapshots_and_validates_cadence() -> None:
    text = WORKFLOW.read_text()

    assert "Snapshot restored cadence manifest" in text
    assert "/tmp/gmx-cadence-baseline.json" in text
    assert "Generate cadence manifest" in text
    assert "Validate candle cadence" in text
    assert "_cadence_manifest.json" in text
    assert "new cadence break" in text


def test_cadence_gate_runs_after_the_manifest_it_reads() -> None:
    """The gate must read the manifest this run just wrote, so it has to sit
    after the step that generates it -- and after the baseline snapshot that
    captured the previous release's copy before collection overwrote it."""
    text = WORKFLOW.read_text()

    assert text.index("Snapshot restored cadence manifest") < text.index(
        "Generate cadence manifest"
    )
    assert text.index("Generate cadence manifest") < text.index("Validate candle cadence")


def test_cadence_gate_retries_before_declaring_a_regression() -> None:
    """A transient read failure must not be reported as a data regression;
    the gate retries up to 3 attempts, matching this repo's existing
    hand-rolled bash retry convention (collect-gmx-data.yml)."""
    text = WORKFLOW.read_text()

    assert "for attempt in 1 2 3; do" in text
    assert "Cadence check attempt $attempt" in text


def test_cadence_regression_ships_and_annotates_rather_than_blocking() -> None:
    """A new break must not starve consumers of the whole release: the data
    still publishes, the manifest records which file regressed, and the job
    only goes red at the very end."""
    text = WORKFLOW.read_text()

    assert "regressed_from" in text
    assert "CADENCE_REGRESSION_DETECTED=true" in text
    assert "Fail job if a new cadence regression was not resolved by retry" in text
    assert text.index("Validate candle cadence") < text.index("Package tarballs")
    assert text.index("Create GitHub Release") < text.index(
        "Fail job if a new cadence regression was not resolved by retry"
    )


def test_release_workflow_checks_out_the_triggering_ref() -> None:
    """Hardcoding ``ref: master`` on checkout made ``workflow_dispatch --ref
    <branch>`` still run the old collector. Scheduled runs on master
    already have ``github.ref = refs/heads/master``.
    """
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    checkout = next(
        s
        for s in workflow["jobs"]["release"]["steps"]
        if str(s.get("uses", "")).startswith("actions/checkout")
    )
    assert "ref" not in (checkout.get("with") or {})


def test_release_workflow_can_opt_in_to_ohlcv_repair() -> None:
    """Interior 1m holes (2026-09-08 02:17) survive an incremental run.
    ``workflow_dispatch`` must be able to pass ``--repair-ohlcv``; the
    02:00 UTC cron must not, because a 10000-bar re-fetch of every
    timeframe every night is wasted work once the hole is gone.
    """
    text = WORKFLOW.read_text()

    assert "repair_ohlcv:" in text
    assert "default: 'false'" in text
    assert "--repair-ohlcv" in text
    assert "github.event.inputs.repair_ohlcv" in text

    import yaml

    workflow = yaml.safe_load(text)
    collect = next(
        s["run"]
        for s in workflow["jobs"]["release"]["steps"]
        if s.get("name") == "Collect daily snapshot"
    )
    assert "collect_daily_snapshot.py" in collect
    assert "--repair-ohlcv" in collect
    # Scheduled runs have empty inputs; the flag is added only when the input is true.
    assert '[ "${{ github.event.inputs.repair_ohlcv }}" = "true" ]' in collect


def test_release_workflow_can_limit_repair_to_timeframes() -> None:
    """#58: the 1d/4h backfill must not re-fetch 10000 bars of every timeframe.

    ``repair_timeframes`` (default empty = all) is passed as
    ``--repair-timeframes`` only on a repair dispatch.
    """
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    inputs = workflow[True]["workflow_dispatch"]["inputs"]
    assert inputs["repair_timeframes"]["default"] == ""
    assert inputs["repair_timeframes"]["required"] is False

    step = next(
        s for s in workflow["jobs"]["release"]["steps"] if s.get("name") == "Collect daily snapshot"
    )
    collect = step["run"]
    assert "--repair-timeframes" in collect
    # Free-text input goes through env, never interpolated into the script.
    assert step["env"]["REPAIR_TIMEFRAMES"] == "${{ github.event.inputs.repair_timeframes }}"
    assert "github.event.inputs.repair_timeframes" not in collect
    # Only meaningful together with a repair: appended after --repair-ohlcv is set.
    assert collect.index("--repair-ohlcv") < collect.index("--repair-timeframes")
    assert collect.index("--repair-timeframes") < collect.index("collect_daily_snapshot.py")


def test_cadence_regression_files_a_deduped_issue() -> None:
    text = WORKFLOW.read_text()

    assert "issues: write" in text
    assert "gh issue comment" in text
    assert "gh issue create" in text
    assert "--assignee Aviksaikat" in text


def test_cadence_issue_dedupe_matches_a_marker_not_a_fuzzy_search() -> None:
    """`gh issue list --search` tokenises, so a shared token (`4h`,
    `futures`, `feather`, `usdc`) could rank another symbol's ticket first
    and we would comment on the wrong one. The gate lists open issues once
    and matches the exact hidden marker instead."""
    text = WORKFLOW.read_text()

    assert "in:title cadence regression" not in text
    assert "--json number,body" in text
    assert "cadence_issue find-issue" in text


def test_cadence_regression_files_one_issue_per_incident() -> None:
    """A fleet-wide outage is one incident. Cap-of-10 alphabetical slices
    (#33–#42) buried the actual per-file signal in 10 duplicate bodies."""
    text = WORKFLOW.read_text()
    script = _cadence_step()

    assert 'FILED" -ge 10' not in text
    assert "while IFS= read -r FILE" not in script
    assert "cadence-regression:${FILE}" not in script
    assert "cadence_issue format" in script
    assert "cadence-regression-incident:" in script
    assert script.count("gh issue create") == 1


def test_cadence_gate_logic_is_importable_not_inlined() -> None:
    """The embedded-heredoc version of this gate shipped a bug that no
    YAML-substring test could see. The logic must stay in a module that
    tests/test_cadence_gate.py can exercise directly."""
    text = WORKFLOW.read_text()

    for command in ("build", "check", "annotate"):
        assert f"gmx_historical_data.cadence_gate {command}" in text


def _cadence_step() -> str:
    """:returns: The `Validate candle cadence` step's shell script."""
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    steps = workflow["jobs"]["release"]["steps"]
    return next(s for s in steps if s.get("name") == "Validate candle cadence")["run"]


def test_regression_flag_is_set_before_any_best_effort_reporting() -> None:
    """The reporting tail runs under `set -e` and *before* packaging, so an
    unguarded failure in it would block the release -- the fail-closed
    behaviour this design exists to avoid. The flag must land first, and
    every call after it must be `||`-guarded."""
    script = _cadence_step()
    flag = script.index('CADENCE_REGRESSION_DETECTED=true" >> "$GITHUB_ENV"')

    for call in (
        "cadence_gate annotate",
        "cadence_issue format",
        "cadence_issue find-issue",
        "gh issue comment",
        "gh issue create",
    ):
        assert flag < script.index(call), f"{call} runs before the flag is set"

    tail = script[flag:]
    assert tail.count("||") >= 5


def test_release_notes_include_the_per_asset_coverage_table() -> None:
    """The release body must carry the same Asset/Timeframe/From/To/Candles
    table the Apex-Historical-Data sibling repo publishes, not just a file
    count -- consumers decide whether to pull a release from it."""
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    create_release = next(
        s["run"]
        for s in workflow["jobs"]["release"]["steps"]
        if s.get("name") == "Create GitHub Release"
    )

    assert "gmx_historical_data.release_notes build" in create_release
    assert "--futures-dir ./user_data/data/gmx/futures" in create_release
    assert create_release.index("gmx_historical_data.release_notes build") < create_release.index(
        "gh release create"
    )
    assert 'cat /tmp/gmx-coverage-table.md >> "$NOTES_FILE"' in create_release


def test_cadence_gate_exempts_delisted_and_just_relisted_markets() -> None:
    """A relisting seam is a legitimate break (MEGA 1h, 213 bars). The gate
    reads the same roster the futures-integrity step does, plus the roster
    as it stood before this run rewrote it -- a market that relists drops
    off the live roster before collection, which is exactly the run whose
    seam needs exempting."""
    text = WORKFLOW.read_text()

    assert "--delisted-roster ./user_data/data/gmx/delisted_markets.json" in text
    assert "--delisted-roster /tmp/gmx-delisted-baseline.json" in text
    assert "/tmp/gmx-delisted-baseline.json" in text
    assert text.index("Record delisted markets") < text.index("Validate candle cadence")


def test_release_packages_tick_and_volume_directories() -> None:
    """The trade-tick phase is skippable, so its output directories may not
    exist -- and `tar` fails on a missing path, which would take the whole
    release down with it."""
    text = WORKFLOW.read_text()

    assert "mkdir -p user_data/data/gmx/tick_volume" in text
    assert "mkdir -p user_data/data/gmx/ticks" in text
    assert text.index("mkdir -p user_data/data/gmx/ticks") < text.index("tar -czf /tmp/gmx-full")
    # Per-symbol volume is small and belongs in the light bundle; the raw
    # per-fill tape does not.
    assert "user_data/data/gmx/tick_volume/ \\\n" in text or "tick_volume/" in text


def test_collect_step_has_the_credentials_the_tick_phase_needs() -> None:
    """Volume comes from HyperSync plus an RPC. Without both, the phase
    degrades to no-volume rather than failing -- but then no volume ever
    ships, which is a silent loss."""
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    collect = next(
        s for s in workflow["jobs"]["release"]["steps"] if s.get("name") == "Collect daily snapshot"
    )

    assert "HYPERSYNC_API_TOKEN" in collect["env"]
    assert "JSON_RPC_ARBITRUM" in collect["env"]


def test_registry_unreachable_surfaces_as_a_github_actions_warning() -> None:
    """A plain ``print("WARNING: ...")`` never shows up in the Actions UI --
    only the ``::warning::`` prefix does, and this failure mode (registry
    flakiness reusing the previous roster) is exactly the kind of thing that
    should be visible without opening the raw step log."""
    text = WORKFLOW.read_text()

    assert "::warning::registry unreachable" in text
    assert 'print(f"WARNING: registry unreachable' not in text


def test_report_is_refreshed_with_real_volume_data_before_it_ships() -> None:
    """`generate_report` (inside "Collect daily snapshot") runs before this
    job's own volume fetch, so data_report.txt always ships with a
    placeholder "Volume entries: 0 markets" unless something patches it
    afterwards with what "Collect 24h volume snapshot" just saved."""
    text = WORKFLOW.read_text()

    assert "Refresh report with 24h volume data" in text
    assert "refresh_volume_report_section" in text
    assert text.index("Collect 24h volume snapshot") < text.index(
        "Refresh report with 24h volume data"
    )
    assert text.index("Refresh report with 24h volume data") < text.index(
        "Show report in job summary"
    )
    assert text.index("Refresh report with 24h volume data") < text.index("Package tarballs")


def test_volume_refresh_step_runs_even_if_volume_collection_fails() -> None:
    """ "Collect 24h volume snapshot" can raise (network error, bad response)
    rather than exiting 0 with no rows. Without `if: always()`, GitHub's
    default `success()` condition would skip the refresh step entirely and
    the report would still ship the original fabricated "0 markets / N/A"
    placeholder -- exactly the bug this whole fix exists to close."""
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    refresh = next(
        s
        for s in workflow["jobs"]["release"]["steps"]
        if s.get("name") == "Refresh report with 24h volume data"
    )

    assert refresh.get("if") == "always()"


def _step(name: str) -> dict:
    import yaml

    workflow = yaml.safe_load(WORKFLOW.read_text())
    return next(s for s in workflow["jobs"]["release"]["steps"] if s.get("name") == name)


def _run_collect_step(tmp_path, repair_timeframes: str):
    """Run the "Collect daily snapshot" script with ``python`` stubbed to echo argv."""
    import os
    import subprocess

    bin_dir = tmp_path / "bin"
    bin_dir.mkdir(exist_ok=True)
    stub = bin_dir / "python"
    stub.write_text('#!/bin/bash\nprintf "ARG:%s\\n" "$@"\n')
    stub.chmod(0o755)
    script = tmp_path / "collect.sh"
    script.write_text(
        _step("Collect daily snapshot")["run"].replace(
            "${{ github.event.inputs.repair_ohlcv }}", "true"
        )
    )
    env = {
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "REPAIR_TIMEFRAMES": repair_timeframes,
    }
    return subprocess.run(
        ["bash", "-e", str(script)], env=env, cwd=tmp_path, capture_output=True, text=True
    )


def test_repair_timeframes_is_passed_as_one_normalised_argument(tmp_path) -> None:
    proc = _run_collect_step(tmp_path, "1d, 4h")
    assert proc.returncode == 0, proc.stderr
    args = [line[4:] for line in proc.stdout.splitlines() if line.startswith("ARG:")]
    assert "--repair-ohlcv" in args
    assert args[args.index("--repair-timeframes") + 1] == "1d,4h"


def test_repair_timeframes_rejects_option_injection(tmp_path) -> None:
    proc = _run_collect_step(tmp_path, "1d --output-dir /tmp/x")
    assert proc.returncode != 0
    assert "ARG:" not in proc.stdout


def test_repair_timeframes_empty_adds_no_flag(tmp_path) -> None:
    proc = _run_collect_step(tmp_path, "")
    assert proc.returncode == 0, proc.stderr
    assert "--repair-timeframes" not in proc.stdout
    assert "ARG:--repair-ohlcv" in proc.stdout


def _run_history_validation(tmp_path, baseline_rows: int, after_rows: int, pruned: list):
    """Run the "Validate candle history integrity" script on a one-file fixture.

    ``pruned`` is the prune evidence the collector recorded for the file.
    """
    import json
    import subprocess
    import sys

    import pandas as pd

    futures = tmp_path / "user_data" / "data" / "gmx" / "futures"
    futures.mkdir(parents=True, exist_ok=True)
    name = "ETH_USDC_USDC-4h-futures.feather"
    dates = pd.date_range("2026-10-01", periods=after_rows, freq="4h", tz="UTC")
    pd.DataFrame({"date": dates}).to_feather(futures / name)
    baseline_path = tmp_path / "restore.json"
    baseline_path.write_text(
        json.dumps(
            {
                name: {
                    "rows": baseline_rows,
                    "min_date": dates.min().isoformat(),
                    "max_date": dates.max().isoformat(),
                }
            }
        )
    )
    prune_path = tmp_path / "prune.json"
    if pruned:
        prune_path.write_text(json.dumps({name: pruned}))
    code = _step("Validate candle history integrity")["run"]
    code = code.split("<<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
    code = code.replace("/tmp/gmx-restore-manifest.json", str(baseline_path))
    code = code.replace("/tmp/gmx-prune-report.json", str(prune_path))
    src = str(Path(__file__).resolve().parents[1] / "src")
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=tmp_path,
        env={"PYTHONPATH": src, "PATH": "/usr/bin"},
        capture_output=True,
        text=True,
    )


def test_history_guard_tolerates_exactly_the_recorded_prunes(tmp_path) -> None:
    # A 03:59 repair pruned the 00:00 4h bar; validation at 04:01 must still pass
    # because the collector recorded it, whatever the wall clock says.
    proc = _run_history_validation(tmp_path, 10, 9, ["2026-10-02T00:00:00+00:00"])
    assert proc.returncode == 0, proc.stderr


def test_history_guard_rejects_shrinkage_without_or_beyond_prune_evidence(tmp_path) -> None:
    assert _run_history_validation(tmp_path, 10, 9, []).returncode != 0
    one = ["2026-10-02T00:00:00+00:00"]
    assert _run_history_validation(tmp_path, 10, 8, one).returncode != 0


def test_collect_step_writes_the_prune_report_outside_the_packaged_tree(tmp_path) -> None:
    proc = _run_collect_step(tmp_path, "")
    args = [line[4:] for line in proc.stdout.splitlines() if line.startswith("ARG:")]
    report = args[args.index("--prune-report") + 1]
    assert report == "/tmp/gmx-prune-report.json"
    assert "user_data" not in report  # the release tarballs pack user_data/data/gmx/
    assert report in _step("Validate candle history integrity")["run"]
