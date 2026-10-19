"""Release trust boundary, preservation, recovery and immutable publishing."""

import io
import json
import os
import tarfile
from pathlib import Path

import pandas as pd
import pytest

from gmx_historical_data import tick_release as release
from gmx_historical_data.candle_volume import write_tick_tapes
from gmx_historical_data.gmx_trade_ticks import TradeTick


def tick(number=1, day="2026-10-15"):
    return TradeTick(
        timestamp=int(pd.Timestamp(day, tz="UTC").timestamp()) + number,
        block_number=100 + number,
        transaction_hash="0x" + f"{number:064x}",
        log_index=number,
        event_name="PositionIncrease",
        kind="perp",
        symbol="BTC",
        market="0x" + "1" * 40,
        price_usd=100.0,
        size_tokens=2.0,
        size_usd=200.0,
        is_long=True,
        price_impact_usd=0.0,
    )


def bundle(tmp, ticks=None):
    source = tmp / "source"
    write_tick_tapes(ticks or [tick()], source / "ticks")
    release.dump(source / "checkpoints/trade_ticks.json", {"last_scanned_block": 999})
    release.apply_volume_from_tapes(
        source / "ticks", source / "tick_volume", source / "none", release.CANDLE_TIMEFRAMES
    )
    manifest = release.build(source, tmp / "assets")
    data = json.loads(manifest.read_text())
    return tmp / "assets/gmx-ticks.tar.gz", data


def test_deterministic_bundle_coverage_and_contents(tmp_path):
    archive, manifest = bundle(tmp_path)
    first = release.digest(archive)
    release.build(tmp_path / "source", tmp_path / "assets")
    assert release.digest(archive) == first
    assert manifest["last_scanned_block"] == 999
    assert manifest["max_tape_block"] == 101
    assert manifest["checkpoint_ahead_of_tape"]
    assert manifest["decode_gaps"] == "unknown"
    assert manifest["finality"] == "unknown"
    assert not manifest["history_complete"]
    assert manifest["chain_id"] == 42161
    with tarfile.open(archive) as tar:
        assert set(tar.getnames()) == {release.PREFIX + n for n in manifest["files"]}
    assert manifest["files"]["ticks/2026-10-15.parquet"]["rows"] == 1


def test_union_rerun_longer_history_and_no_candle_writes(tmp_path):
    archive, manifest = bundle(tmp_path, [tick(2)])
    root = tmp_path / "canonical"
    write_tick_tapes([tick(1), tick(3, "2026-10-14")], root / "ticks")
    (root / "futures").mkdir()
    candle = root / "futures/keep.feather"
    candle.write_bytes(b"existing longer candles")
    release.dump(root / "checkpoints/trade_ticks.json", {"last_scanned_block": 1000})
    before = candle.read_bytes()
    for _ in range(2):
        result = release.intake(root, archive, manifest["archive"]["sha256"], manifest)
    frame = pd.read_parquet(root / "ticks/2026-10-15.parquet")
    assert len(frame) == 2
    assert len(pd.read_parquet(root / "ticks/2026-10-14.parquet")) == 1
    volume = pd.read_parquet(root / "tick_volume/2026-10-15.parquet")
    assert volume[volume.timeframe.eq("1d")].volume.sum() == 4
    assert candle.read_bytes() == before
    assert (
        json.loads((root / "checkpoints/trade_ticks.json").read_text())["last_scanned_block"]
        == 1000
    )
    assert not result["history_complete"]
    assert result["last_intake"]["source_coverage"]["checkpoint_ahead_of_tape"]


@pytest.mark.parametrize(
    "name,kind",
    [
        ("../escape", "file"),
        ("/absolute", "file"),
        (release.PREFIX + "ticks/link", "sym"),
        (release.PREFIX + "ticks/link", "hard"),
        (release.PREFIX + "ticks/fifo", "fifo"),
    ],
)
def test_reject_archive_escape_links_devices(tmp_path, name, kind):
    path = tmp_path / "evil.tar.gz"
    with tarfile.open(path, "w:gz") as tar:
        info = tarfile.TarInfo(name)
        if kind == "sym":
            info.type = tarfile.SYMTYPE
            info.linkname = "/etc/passwd"
        elif kind == "hard":
            info.type = tarfile.LNKTYPE
            info.linkname = "../outside"
        elif kind == "fifo":
            info.type = tarfile.FIFOTYPE
        tar.addfile(info, io.BytesIO(b""))
    with pytest.raises(ValueError, match="Unsafe"):
        release.intake(tmp_path / "canonical", path, release.digest(path))
    assert not (tmp_path / "canonical/checkpoints/tick_intake.json").exists()


def test_hash_and_manifest_failure_preserve_canonical(tmp_path):
    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    write_tick_tapes([tick(2)], root / "ticks")
    before = (root / "ticks/2026-10-15.parquet").read_bytes()
    with pytest.raises(ValueError, match="SHA256"):
        release.intake(root, archive, "0" * 64, manifest)
    manifest["files"]["ticks/2026-10-15.parquet"]["rows"] = 999
    with pytest.raises(ValueError, match="Manifest"):
        release.intake(root, archive, release.digest(archive), manifest)
    assert (root / "ticks/2026-10-15.parquet").read_bytes() == before


def test_invalid_schema_before_any_write(tmp_path):
    archive, _ = bundle(tmp_path)
    source = tmp_path / "source"
    bad = source / "ticks/2026-10-15.parquet"
    pd.read_parquet(bad).drop(columns=["transaction_hash"]).to_parquet(bad)
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(bad, arcname=release.PREFIX + "ticks/2026-10-15.parquet")
    with pytest.raises(ValueError, match="schema"):
        release.intake(tmp_path / "canonical", archive, release.digest(archive))
    assert not (tmp_path / "canonical/checkpoints/tick_intake.json").exists()


def test_mid_commit_failure_rolls_back_and_reruns(tmp_path, monkeypatch):
    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    write_tick_tapes([tick(2)], root / "ticks")
    before = (root / "ticks/2026-10-15.parquet").read_bytes()
    replace = os.replace
    calls = []

    def fail_second(src, dest):
        if "/ready/" in str(src):
            calls.append(dest)
        if "/ready/" in str(src) and len(calls) == 2:
            raise OSError("simulated disk failure")
        return replace(src, dest)

    monkeypatch.setattr(release.os, "replace", fail_second)
    with pytest.raises(OSError, match="disk failure"):
        release.intake(root, archive, release.digest(archive), manifest)
    assert (root / "ticks/2026-10-15.parquet").read_bytes() == before
    assert not (root / "checkpoints/tick_intake.json").exists()
    monkeypatch.setattr(release.os, "replace", replace)
    release.intake(root, archive, release.digest(archive), manifest)
    assert len(pd.read_parquet(root / "ticks/2026-10-15.parquet")) == 2


def test_process_crash_journal_recovered_on_status(tmp_path):
    root = tmp_path / "canonical"
    name = "ticks/2026-10-15.parquet"
    write_tick_tapes([tick(1)], root / "ticks")
    original = (root / name).read_bytes()
    backup = root / ".tick-intake-transaction/backup" / name
    backup.parent.mkdir(parents=True)
    backup.write_bytes(original)
    (root / name).write_bytes(b"crash after first replacement")
    release.dump(
        root / ".tick-intake-journal.json",
        {"targets": {name: True, "checkpoints/tick_intake.json": False}},
    )
    with release.locked(root):
        result = release.status_unlocked(root)
    assert result["files"][name]["rows"] == 1
    assert (root / name).read_bytes() == original
    assert not (root / ".tick-intake-journal.json").exists()


def test_symlink_canonical_directory_rejected(tmp_path):
    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    root.mkdir()
    (root / "ticks").symlink_to(tmp_path / "source/ticks")
    with pytest.raises(ValueError, match="Symlink"):
        release.intake(root, archive, release.digest(archive), manifest)


def test_legacy_full_ignores_other_data(tmp_path):
    archive, _ = bundle(tmp_path)
    full = tmp_path / "full.tar.gz"
    with tarfile.open(archive) as source, tarfile.open(full, "w:gz") as tar:
        for member in source:
            tar.addfile(member, source.extractfile(member))
        info = tarfile.TarInfo(release.PREFIX + "futures/precious.feather")
        info.size = 4
        tar.addfile(info, io.BytesIO(b"nope"))
    release.intake(tmp_path / "canonical", full, release.digest(full))
    assert not (tmp_path / "canonical/futures").exists()


def test_publish_reuses_hash_or_appends_revision_never_clobbers(tmp_path, monkeypatch):
    archive, _ = bundle(tmp_path)
    manifest = tmp_path / "assets/gmx-ticks.manifest.json"
    assets = {}
    calls = []

    def fake_gh(*args):
        calls.append(args)
        assert "--clobber" not in args
        if args[1] == "view":
            return json.dumps({"assets": [{"name": n} for n in assets]})
        if args[1] == "upload":
            for file in args[5:]:
                path = Path(file)
                assets[path.name] = path.read_bytes()
        if args[1] == "download":
            stage = Path(args[-1])
            for n in [args[6], args[8]]:
                (stage / n).write_bytes(assets[n])
        return ""

    monkeypatch.setattr(release, "gh", fake_gh)
    release.publish("owner/repo", "data-day", archive, manifest)
    assert set(assets) == {"gmx-ticks.tar.gz", "gmx-ticks.manifest.json"}
    count = sum(c[1] == "upload" for c in calls)
    release.publish("owner/repo", "data-day", archive, manifest)
    assert sum(c[1] == "upload" for c in calls) == count
    assets["gmx-ticks.tar.gz"] = b"previous different archive"
    release.publish("owner/repo", "data-day", archive, manifest)
    assert "gmx-ticks.r1.tar.gz" in assets
    assert assets["gmx-ticks.tar.gz"] == b"previous different archive"


def test_workflow_preserves_existing_release_and_builds_ticks():
    text = Path(".github/workflows/release-data.yml").read_text()
    create = text[
        text.index("      - name: Create GitHub Release") : text.index(
            "      - name: Prune old releases"
        )
    ]
    assert "gh release delete" not in create
    assert "git push origin --delete" not in create
    assert "tick_release publish" in create
    assert create.index("gh release list") < create.index("gh release create")
    assert 'MODE=in_place' in create
    assert 'CONSUMER=()' in create
    assert "--clobber" not in create
    assert "funding_state_assets.sh publish" in create
    assert "gmx-ticks.tar.gz /tmp/gmx-funding-state-*.tar.gz" in text
    assert "queue: max" in text
    assert "tick_release build" in text
    assert "--root user_data/data/gmx --output /tmp" in text


def test_latest_download_selects_pair_and_reuses_cached_asset(tmp_path, monkeypatch):
    archive, manifest = bundle(tmp_path)
    calls = []

    def fake_gh(*args):
        calls.append(args)
        if args[0] == "api":
            assert args[1] == "repos/owner/repo/releases/latest"
            return json.dumps(
                {
                    "tag_name": "data-2026-10-16",
                    "assets": [
                        {"name": "gmx-ticks.tar.gz"},
                        {"name": "gmx-ticks.manifest.json"},
                        {"name": "gmx-ticks.r2.tar.gz"},  # interrupted pair: ignored
                    ],
                }
            )
        path = Path(args[-1]) / args[6]
        if path.suffix == ".json":
            path.write_text(json.dumps(manifest))
        else:
            path.write_bytes(archive.read_bytes())
        return ""

    monkeypatch.setattr(release, "gh", fake_gh)
    for _ in range(2):
        downloaded, sha, meta, tag = release.download("owner/repo", "latest", tmp_path / "download")
        assert release.digest(downloaded) == sha
        assert tag == "data-2026-10-16"
        assert meta == manifest
    assert sum(args[1] == "download" for args in calls) == 2


def test_real_process_exit_during_commit_recovers_then_reruns(tmp_path):
    import subprocess
    import sys

    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    write_tick_tapes([tick(2)], root / "ticks")
    original = (root / "ticks/2026-10-15.parquet").read_bytes()
    code = """
import os, sys
from pathlib import Path
from gmx_historical_data import tick_release as r
replace = os.replace
calls = 0
def crash(src, target):
    global calls
    if "/ready/" in str(src):
        calls += 1
    if "/ready/" in str(src) and calls == 2:
        os._exit(91)
    return replace(src, target)
r.os.replace = crash
r.intake(Path(sys.argv[1]), Path(sys.argv[2]), sys.argv[3])
"""
    proc = subprocess.run(
        [sys.executable, "-c", code, str(root), str(archive), manifest["archive"]["sha256"]]
    )
    assert proc.returncode == 91
    assert (root / ".tick-intake-journal.json").exists()
    with release.locked(root):
        assert (root / "ticks/2026-10-15.parquet").read_bytes() == original
        assert not (root / "checkpoints/tick_intake.json").exists()
    release.intake(root, archive, release.digest(archive), manifest)
    assert len(pd.read_parquet(root / "ticks/2026-10-15.parquet")) == 2


def test_legacy_fallback_requires_and_uses_rest_digest(tmp_path, monkeypatch):
    archive, _ = bundle(tmp_path)
    sha = release.digest(archive)
    api_digest = "sha256:" + sha

    def fake_gh(*args):
        if args[0] == "api":
            return json.dumps(
                {
                    "tag_name": "data-2026-10-16",
                    "assets": [{"name": "gmx-full.tar.gz", "digest": api_digest}],
                }
            )
        (Path(args[-1]) / "gmx-full.tar.gz").write_bytes(archive.read_bytes())
        return ""

    monkeypatch.setattr(release, "gh", fake_gh)
    path, expected, manifest, tag = release.download("owner/repo", "latest", tmp_path / "cache")
    assert expected == sha and manifest is None
    assert release.digest(path) == expected
    api_digest = None
    with pytest.raises(ValueError, match="no GitHub SHA256"):
        release.download("owner/repo", "latest", tmp_path / "other-cache")


def test_larger_local_aggregate_without_matching_tape_is_not_overwritten(tmp_path):
    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    (root / "tick_volume").mkdir(parents=True)
    volume = pd.read_parquet(tmp_path / "source/tick_volume/2026-10-15.parquet")
    volume["volume"] *= 2
    path = root / "tick_volume/2026-10-15.parquet"
    volume.to_parquet(path, index=False)
    original = path.read_bytes()
    with pytest.raises(ValueError, match="aggregate exceeds"):
        release.intake(root, archive, release.digest(archive), manifest)
    assert path.read_bytes() == original
    assert not (root / "ticks").exists()
    assert not (root / "checkpoints/tick_intake.json").exists()


def test_duplicate_archive_member_rejected(tmp_path):
    archive, _ = bundle(tmp_path)
    dup = tmp_path / "duplicate.tar.gz"
    with tarfile.open(archive) as source, tarfile.open(dup, "w:gz") as target:
        member = source.next()
        content = source.extractfile(member).read()
        target.addfile(member, io.BytesIO(content))
        target.addfile(member, io.BytesIO(content))
    with pytest.raises(ValueError, match="Duplicate archive"):
        release.intake(tmp_path / "canonical", dup, release.digest(dup))


@pytest.mark.parametrize("boundary", ["partial_write", "journal_replace"])
def test_journal_publication_process_exit_is_recoverable(tmp_path, boundary):
    import subprocess
    import sys

    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    write_tick_tapes([tick(2)], root / "ticks")
    original = (root / "ticks/2026-10-15.parquet").read_bytes()
    code = """
import os, sys
from pathlib import Path
from gmx_historical_data import tick_release as r
root = Path(sys.argv[1])
boundary = sys.argv[4]
original_open = Path.open
original_temp = r.tempfile.NamedTemporaryFile
original_replace = os.replace
class PartialWriter:
    def __init__(self, stream): self.stream = stream
    def __getattr__(self, name): return getattr(self.stream, name)
    def __enter__(self): return self
    def __exit__(self, *args): return self.stream.__exit__(*args)
    def write(self, text):
        self.stream.write(text[:8])
        self.stream.flush()
        os._exit(91)
def crash_open(path, *args, **kwargs):
    stream = original_open(path, *args, **kwargs)
    if boundary == 'partial_write' and path == root / '.tick-intake-journal.json' and 'w' in (args[0] if args else kwargs.get('mode', 'r')):
        return PartialWriter(stream)
    return stream
def crash_temp(*args, **kwargs):
    stream = original_temp(*args, **kwargs)
    if boundary == 'partial_write' and str(kwargs.get('prefix', '')).startswith('.tick-intake-journal.json'):
        return PartialWriter(stream)
    return stream
def crash_replace(src, dest):
    result = original_replace(src, dest)
    if boundary == 'journal_replace' and Path(dest) == root / '.tick-intake-journal.json':
        os._exit(91)
    return result
Path.open = crash_open
r.tempfile.NamedTemporaryFile = crash_temp
r.os.replace = crash_replace
r.intake(root, Path(sys.argv[2]), sys.argv[3])
"""
    proc = subprocess.run(
        [
            sys.executable,
            "-c",
            code,
            str(root),
            str(archive),
            manifest["archive"]["sha256"],
            boundary,
        ]
    )
    assert proc.returncode == 91
    assert (root / "ticks/2026-10-15.parquet").read_bytes() == original
    assert not (root / "checkpoints/tick_intake.json").exists()
    journal = root / ".tick-intake-journal.json"
    if boundary == "partial_write":
        assert not journal.exists()
    else:
        assert json.loads(journal.read_text())["targets"]
    for _ in range(2):
        with release.locked(root):
            assert release.status_unlocked(root)["files"]["ticks/2026-10-15.parquet"]["rows"] == 1
    release.intake(root, archive, release.digest(archive), manifest)
    assert len(pd.read_parquet(root / "ticks/2026-10-15.parquet")) == 2


def test_legacy_partial_journal_is_preserved_with_explicit_recovery_error(tmp_path):
    root = tmp_path / "canonical"
    root.mkdir()
    journal = root / ".tick-intake-journal.json"
    journal.write_text('{"targets":')
    transaction = root / ".tick-intake-transaction"
    transaction.mkdir()
    (transaction / "evidence").write_text("possibly needed for rollback")
    for _ in range(2):
        with pytest.raises(ValueError, match="manual recovery required"):
            with release.locked(root):
                pass
        assert journal.read_text() == '{"targets":'
        assert (transaction / "evidence").read_text() == "possibly needed for rollback"


@pytest.mark.parametrize("failure", ["raises", "short_count"])
def test_failed_journal_write_leaves_no_active_journal_or_canonical_change(
    tmp_path, monkeypatch, failure
):
    archive, manifest = bundle(tmp_path)
    root = tmp_path / "canonical"
    write_tick_tapes([tick(2)], root / "ticks")
    original = (root / "ticks/2026-10-15.parquet").read_bytes()
    original_temp = release.tempfile.NamedTemporaryFile

    class FailingWriter:
        def __init__(self, stream):
            self.stream = stream

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, text):
            self.stream.write(text[:8])
            self.stream.flush()
            if failure == "raises":
                raise OSError("simulated short write")
            return 8

    def fail_journal(*args, **kwargs):
        stream = original_temp(*args, **kwargs)
        return (
            FailingWriter(stream)
            if kwargs.get("prefix") == ".tick-intake-journal.json."
            else stream
        )

    monkeypatch.setattr(release.tempfile, "NamedTemporaryFile", fail_journal)
    with pytest.raises(OSError, match="short write"):
        release.intake(root, archive, release.digest(archive), manifest)
    assert (root / "ticks/2026-10-15.parquet").read_bytes() == original
    assert not (root / ".tick-intake-journal.json").exists()
    assert not list(root.glob(".tick-intake-journal.json.*.tmp"))
    monkeypatch.setattr(release.tempfile, "NamedTemporaryFile", original_temp)
    release.intake(root, archive, release.digest(archive), manifest)
    assert len(pd.read_parquet(root / "ticks/2026-10-15.parquet")) == 2


def test_dump_flushes_and_syncs_file_before_replace_then_syncs_directory(tmp_path, monkeypatch):
    import stat

    events = []
    fsync = os.fsync
    replace = os.replace

    def sync(fd):
        events.append("directory_sync" if stat.S_ISDIR(os.fstat(fd).st_mode) else "file_sync")
        return fsync(fd)

    def rename(src, dest):
        assert json.loads(Path(src).read_text()) == {"complete": True}
        events.append("replace")
        return replace(src, dest)

    monkeypatch.setattr(release.os, "fsync", sync)
    monkeypatch.setattr(release.os, "replace", rename)
    release.dump(tmp_path / "state.json", {"complete": True})
    assert events == ["file_sync", "replace", "directory_sync"]
