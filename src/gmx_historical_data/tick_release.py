"""Bounded Arbitrum tick releases and conservative canonical intake.

No completeness/finality guarantee is inferred from a scan checkpoint. This
module never writes candles, funding, OI, pool data or collector checkpoints.
"""

from __future__ import annotations

import argparse
import contextlib
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath

import pandas as pd

from gmx_historical_data.candle_volume import CANDLE_TIMEFRAMES, apply_volume_from_tapes
from gmx_historical_data.gmx_trade_ticks import _TICK_COLUMNS

PREFIX = "user_data/data/gmx/"
KEY = ["transaction_hash", "log_index", "symbol", "kind"]
VKEY = ["symbol", "date", "timeframe", "kind"]
LIMITS = {
    "chain": "arbitrum",
    "chain_id": 42161,
    "history_complete": False,
    "finality": "unknown",
    "reorg_handling": "none",
    "decode_gaps": "unknown",
    "unmapped_market_gaps": "unknown",
    "initial_scan": "bounded recent window, not genesis",
    "prices": "perp execution prices; swap-side oracle valuations, not a CLOB",
    "swap_notional": "two token-side rows; do not sum both as independent trades",
    "identity_limit": "existing key can collapse same-symbol swap sides",
    "price_impact": "optional: pendingPriceImpactUsd for new increases, priceImpactUsd otherwise; absent fields are NaN; legacy zero may mean missing",
    "numeric_encoding": "existing float64 human units; not raw fixed-point integers",
}


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def dump(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def selected(name: str) -> bool:
    return (
        bool(re.fullmatch(r"(ticks|tick_volume)/\d{4}-\d{2}-\d{2}\.parquet", name))
        or name == "checkpoints/trade_ticks.json"
    )


def validate(frame: pd.DataFrame, name: str) -> None:
    required = (
        _TICK_COLUMNS if name.startswith("ticks/") else [*VKEY, "volume", "volume_usd", "trades"]
    )
    if set(frame.columns) != set(required):
        raise ValueError(f"{name}: unexpected schema: {list(frame.columns)}")
    if not pd.api.types.is_datetime64_any_dtype(frame["date"]):
        raise ValueError(f"{name}: date must retain the timestamp schema")
    if not frame["symbol"].map(lambda value: isinstance(value, str) and bool(value)).all():
        raise ValueError(f"{name}: invalid symbol")
    dates = pd.to_datetime(frame["date"], utc=True, errors="raise")
    if dates.isna().any() or not dates.dt.strftime("%Y-%m-%d").eq(Path(name).stem).all():
        raise ValueError(f"{name}: invalid UTC partition")
    key = KEY if name.startswith("ticks/") else VKEY
    if frame[key].isna().any().any() or frame.duplicated(key).any():
        raise ValueError(f"{name}: null or duplicate identity")
    columns = (
        ["price_usd", "size_tokens", "size_usd", "block_number", "log_index"]
        if name.startswith("ticks/")
        else ["volume", "volume_usd", "trades"]
    )
    for col in columns:
        if not pd.api.types.is_numeric_dtype(frame[col]):
            raise ValueError(f"{name}: nonnumeric {col}")
        values = pd.to_numeric(frame[col], errors="raise")
        if (
            values.isna().any()
            or (~values.map(lambda v: float("-inf") < v < float("inf"))).any()
            or (values < 0).any()
        ):
            raise ValueError(f"{name}: invalid {col}")
    if not frame["kind"].isin(["perp", "swap"]).all():
        raise ValueError(f"{name}: invalid kind")
    if not name.startswith("ticks/") and not frame["timeframe"].isin(CANDLE_TIMEFRAMES).all():
        raise ValueError(f"{name}: invalid timeframe")
    if name.startswith("ticks/"):
        if not pd.api.types.is_numeric_dtype(frame["price_impact_usd"]):
            raise ValueError(f"{name}: nonnumeric optional impact")
        impact = frame["price_impact_usd"].dropna()
        if not impact.map(lambda value: float("-inf") < value < float("inf")).all():
            raise ValueError(f"{name}: nonfinite optional impact")
        for col, pattern in [
            ("transaction_hash", r"0x[0-9a-fA-F]{64}"),
            ("market", r"0x[0-9a-fA-F]{40}"),
        ]:
            if not frame[col].str.fullmatch(pattern).fillna(False).all():
                raise ValueError(f"{name}: invalid {col}")
        expected = frame["event_name"].map(
            {"PositionIncrease": "perp", "PositionDecrease": "perp", "SwapInfo": "swap"}
        )
        if not expected.eq(frame["kind"]).all():
            raise ValueError(f"{name}: event/kind mismatch")
        for col in ["block_number", "log_index"]:
            if not (frame[col] % 1 == 0).all():
                raise ValueError(f"{name}: non-integral {col}")


def inventory(root: Path) -> dict:
    files = {}
    for directory in ["ticks", "tick_volume"]:
        for path in sorted((root / directory).glob("*.parquet")):
            name = path.relative_to(root).as_posix()
            if path.is_symlink() or (root / directory).is_symlink() or not selected(name):
                raise ValueError(f"Invalid source: {path}")
            frame = pd.read_parquet(path)
            validate(frame, name)
            entry = {
                "sha256": digest(path),
                "bytes": path.stat().st_size,
                "rows": len(frame),
                "schema": {col: str(dtype) for col, dtype in frame.dtypes.items()},
            }
            if not frame.empty:
                entry.update(
                    min_date=pd.to_datetime(frame.date, utc=True).min().isoformat(),
                    max_date=pd.to_datetime(frame.date, utc=True).max().isoformat(),
                )
                if directory == "ticks":
                    entry.update(
                        min_block=int(frame.block_number.min()),
                        max_block=int(frame.block_number.max()),
                        kinds=frame.kind.value_counts().to_dict(),
                        symbols=sorted(frame.symbol.unique().tolist()),
                    )
            files[name] = entry
    checkpoint = root / "checkpoints/trade_ticks.json"
    scanned = None
    if checkpoint.exists():
        scanned = json.loads(checkpoint.read_text())["last_scanned_block"]
        if type(scanned) is not int or scanned < 0 or checkpoint.is_symlink():
            raise ValueError("Invalid source checkpoint")
        files["checkpoints/trade_ticks.json"] = {
            "sha256": digest(checkpoint),
            "bytes": checkpoint.stat().st_size,
        }
    maximum = max((v.get("max_block", -1) for v in files.values()), default=-1)
    return {
        "version": 1,
        **LIMITS,
        "files": files,
        "last_scanned_block": scanned,
        "max_tape_block": maximum if maximum >= 0 else None,
        "checkpoint_ahead_of_tape": scanned is not None and scanned > maximum,
    }


def build(root: Path, output: Path) -> Path:
    output.mkdir(parents=True, exist_ok=True)
    manifest = inventory(root)
    archive = output / "gmx-ticks.tar.gz"
    # Fixed metadata makes a repeat of the same source bytes hash-identical.
    import gzip

    with (
        archive.open("wb") as raw,
        gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as gz,
        tarfile.open(fileobj=gz, mode="w") as tar,
    ):
        for name in manifest["files"]:
            path = root / name
            info = tar.gettarinfo(str(path), arcname=PREFIX + name)
            info.uid = info.gid = info.mtime = 0
            info.uname = info.gname = ""
            info.mode = 0o644
            with path.open("rb") as stream:
                tar.addfile(info, stream)
    manifest["archive"] = {
        "name": archive.name,
        "sha256": digest(archive),
        "bytes": archive.stat().st_size,
    }
    path = output / "gmx-ticks.manifest.json"
    dump(path, manifest)
    return path


def extract(archive: Path, stage: Path) -> None:
    """Never call tar.extract: reject links/traversal even in ignored legacy data."""
    seen = set()
    total = 0
    with tarfile.open(archive, "r:gz") as tar:
        for member in tar:
            name = member.name.rstrip("/")
            parts = PurePosixPath(name).parts
            if (
                name.startswith("/")
                or ".." in parts
                or "\\" in name
                or not (member.isdir() or member.isfile())
            ):
                raise ValueError(f"Unsafe archive member: {name}")
            if name in seen:
                raise ValueError(f"Duplicate archive member: {name}")
            seen.add(name)
            if member.isdir():
                continue
            total += member.size
            if total > 16 * 1024**3:
                raise ValueError("Archive exceeds uncompressed limit")
            if not name.startswith(PREFIX):
                raise ValueError(f"Unexpected archive root: {name}")
            relative = name[len(PREFIX) :]
            if not selected(relative):
                # Full legacy archives contain other data. It is never extracted.
                continue
            target = stage / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            with tar.extractfile(member) as source, target.open("wb") as dest:
                shutil.copyfileobj(source, dest)


def gh(*args: str) -> str:
    return subprocess.check_output(["gh", *args], text=True)


def download(repo: str, tag: str, stage: Path) -> tuple[Path, str, dict | None, str]:
    from urllib.parse import quote

    endpoint = "latest" if tag == "latest" else "tags/" + quote(tag, safe="")
    # gh release view's JSON model drops digest on older clients. REST preserves it.
    release = json.loads(gh("api", f"repos/{repo}/releases/{endpoint}"))
    tag_name = release["tag_name"]
    if not re.fullmatch(r"data-\d{4}-\d{2}-\d{2}", tag_name):
        raise ValueError("Expected a daily data release tag")
    stage = stage / tag_name
    stage.mkdir(parents=True, exist_ok=True)
    assets = {a["name"]: a for a in release["assets"]}
    candidates = sorted(
        (
            n
            for n in assets
            if re.fullmatch(r"gmx-ticks(?:\.r\d+)?\.tar\.gz", n)
            and n.replace(".tar.gz", ".manifest.json") in assets
        ),
        key=lambda n: int(re.search(r"\.r(\d+)", n)[1]) if ".r" in n else 0,
    )
    name = candidates[-1] if candidates else "gmx-full.tar.gz"

    def fetch(asset: str) -> Path:
        path = stage / asset
        if path.is_symlink():
            raise ValueError("Symlink download cache")
        if not path.exists():
            gh(
                "release",
                "download",
                tag_name,
                "--repo",
                repo,
                "--pattern",
                asset,
                "--dir",
                str(stage),
            )
        expected = assets[asset].get("digest")
        if (
            expected
            and expected.startswith("sha256:")
            and digest(path) != expected.removeprefix("sha256:")
        ):
            raise ValueError(f"GitHub asset SHA256 mismatch: {asset}")
        return path

    manifest = None
    if candidates:
        manifest = json.loads(fetch(name.replace(".tar.gz", ".manifest.json")).read_text())
        if manifest["archive"]["name"] != name:
            raise ValueError("Archive name mismatch")
        sha = manifest["archive"]["sha256"]
    else:
        sha = assets[name].get("digest", "")
        if not sha or not sha.startswith("sha256:"):
            raise ValueError(
                "Legacy release has no GitHub SHA256 digest; supply audited --archive --sha256 instead"
            )
        sha = sha.removeprefix("sha256:")
    return fetch(name), sha, manifest, tag_name


def recover(root: Path) -> None:
    journal = root / ".tick-intake-journal.json"
    if not journal.exists():
        return
    state = json.loads(journal.read_text())
    transaction = root / ".tick-intake-transaction"
    for name, existed in state["targets"].items():
        if not selected(name) and name != "checkpoints/tick_intake.json":
            raise ValueError("Invalid recovery journal")
        target = root / name
        if existed:
            shutil.copy2(transaction / "backup" / name, target)
        else:
            target.unlink(missing_ok=True)
    journal.unlink()
    shutil.rmtree(transaction)


@contextlib.contextmanager
def locked(root: Path):
    root.mkdir(parents=True, exist_ok=True)
    for path in [
        root / ".tick-intake.lock",
        root / ".tick-intake-journal.json",
        root / ".tick-intake-transaction",
        root / "ticks",
        root / "tick_volume",
        root / "checkpoints",
    ]:
        if path.is_symlink():
            raise ValueError(f"Symlink in canonical intake path: {path}")
    with (root / ".tick-intake.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        recover(root)
        yield


def intake(
    root: Path, archive: Path, sha: str, manifest: dict | None = None, source: str = "local"
) -> dict:
    if not re.fullmatch("[0-9a-f]{64}", sha) or digest(archive) != sha:
        raise ValueError("Archive SHA256 mismatch")
    with locked(root), tempfile.TemporaryDirectory(prefix="gmx-tick-stage-") as temp:
        stage = Path(temp)
        extract(archive, stage)
        actual = inventory(stage)
        if manifest is not None:
            if (
                manifest.get("version") != 1
                or manifest.get("chain_id") != 42161
                or manifest["archive"]["sha256"] != sha
                or actual["files"] != manifest["files"]
            ):
                raise ValueError("Manifest/source mismatch")
        if not any(n.startswith("ticks/") for n in actual["files"]):
            raise ValueError("No tick tape; refusing to advance intake")
        # Validate ALL source and local files before preparing canonical changes.
        inventory(root)
        changed = []
        for name in actual["files"]:
            if not name.startswith("ticks/"):
                continue
            incoming = pd.read_parquet(stage / name)
            if (root / name).exists():
                incoming = pd.concat([pd.read_parquet(root / name), incoming], ignore_index=True)
            incoming = (
                incoming.drop_duplicates(KEY, keep="last")
                .sort_values(["date", "block_number", "log_index"])
                .reset_index(drop=True)
            )
            validate(incoming, name)
            incoming.to_parquet(stage / name, index=False)
            changed.append(name)
        # Rebuild aggregate values from the union, never add overlapping sums.
        shutil.rmtree(stage / "tick_volume", ignore_errors=True)
        apply_volume_from_tapes(
            stage / "ticks", stage / "tick_volume", stage / "no-candles", CANDLE_TIMEFRAMES
        )
        for path in sorted((stage / "tick_volume").glob("*.parquet")):
            name = path.relative_to(stage).as_posix()
            volume = pd.read_parquet(path)
            if (root / name).exists():
                prior = pd.read_parquet(root / name)
                overlap = prior.merge(volume, on=VKEY, suffixes=("_old", "_new"))
                for col in ["volume", "volume_usd", "trades"]:
                    if (overlap[f"{col}_old"] > overlap[f"{col}_new"] + 1e-8).any():
                        raise ValueError(
                            f"{name}: local aggregate exceeds union tape; reconcile manually"
                        )
                volume = pd.concat([prior, volume], ignore_index=True).drop_duplicates(
                    VKEY, keep="last"
                )
            validate(volume, name)
            volume.to_parquet(path, index=False)
            changed.append(name)
        # Preserve source scan metadata separately; NEVER drive the local collector with it.
        report = {
            "source": source,
            "archive_sha256": sha,
            "source_coverage": actual,
            "coverage": inventory(stage),
            **LIMITS,
        }
        dump(stage / "checkpoints/tick_intake.json", report)
        changed.append("checkpoints/tick_intake.json")
        if (root / "checkpoints/tick_intake.json").is_symlink():
            raise ValueError("Symlink intake checkpoint")
        transaction = root / ".tick-intake-transaction"
        shutil.rmtree(transaction, ignore_errors=True)
        transaction.mkdir()
        targets = {}
        for name in changed:
            target = root / name
            target.parent.mkdir(parents=True, exist_ok=True)
            targets[name] = target.exists()
            if target.exists():
                backup = transaction / "backup" / name
                backup.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(target, backup)
            ready = transaction / "ready" / name
            ready.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(stage / name, ready)
        # A rollback journal survives process interruption. Recovery runs
        # under the same lock before the next status/intake, checkpoint commits last.
        dump(root / ".tick-intake-journal.json", {"targets": targets})
        try:
            for name in changed:
                os.replace(transaction / "ready" / name, root / name)
        except BaseException:
            recover(root)
            raise
        (root / ".tick-intake-journal.json").unlink()
        shutil.rmtree(transaction)
        return status_unlocked(root)


def status_unlocked(root: Path) -> dict:
    result = inventory(root)
    checkpoint = root / "checkpoints/tick_intake.json"
    result["last_intake"] = json.loads(checkpoint.read_text()) if checkpoint.exists() else None
    result["bytes"] = sum(v["bytes"] for v in result["files"].values())
    # Source scan metadata is not a certification of the local union.
    result["history_complete"] = False
    return result


def publish(repo: str, tag: str, archive: Path, manifest_path: Path) -> None:
    manifest = json.loads(manifest_path.read_text())
    sha = digest(archive)
    if manifest["archive"]["sha256"] != sha:
        raise ValueError("Publish archive SHA256 mismatch")
    # Serialize through workflow concurrency; never --clobber, including races.
    assets = json.loads(gh("release", "view", tag, "--repo", repo, "--json", "assets"))["assets"]
    names = {a["name"] for a in assets}
    with tempfile.TemporaryDirectory(prefix="gmx-tick-publish-") as tmp:
        stage = Path(tmp)
        revision = 0
        while True:
            stem = "gmx-ticks" + (f".r{revision}" if revision else "")
            name = stem + ".tar.gz"
            meta = stem + ".manifest.json"
            if name not in names and meta not in names:
                shutil.copy2(archive, stage / name)
                manifest["archive"]["name"] = name
                dump(stage / meta, manifest)
                # Archive first: incomplete pair is skipped on repair via a new revision.
                gh("release", "upload", tag, "--repo", repo, str(stage / name), str(stage / meta))
                return
            if name in names and meta in names:
                gh(
                    "release",
                    "download",
                    tag,
                    "--repo",
                    repo,
                    "--pattern",
                    name,
                    "--pattern",
                    meta,
                    "--dir",
                    str(stage),
                )
                existing = json.loads((stage / meta).read_text())
                if digest(stage / name) == sha and existing == {
                    **manifest,
                    "archive": {**manifest["archive"], "name": name},
                }:
                    return
                (stage / name).unlink()
                (stage / meta).unlink()
            revision += 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["build", "intake", "status", "publish"])
    parser.add_argument("--root", type=Path, default=Path("user_data/data/gmx"))
    parser.add_argument("--output", type=Path, default=Path("/tmp"))
    parser.add_argument("--repo", default="tradingstrategy-ai/gmx-data-collector")
    parser.add_argument("--release", default="latest")
    parser.add_argument("--archive", type=Path)
    parser.add_argument("--sha256")
    parser.add_argument("--download-dir", type=Path, help="Retain downloads for audited seed/reuse")
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    if args.command == "build":
        print(build(args.root, args.output))
    elif args.command == "status":
        with locked(args.root):
            print(json.dumps(status_unlocked(args.root), indent=2))
    elif args.command == "publish":
        if not args.archive or not args.manifest or args.release == "latest":
            parser.error("publish requires --archive, --manifest and an exact --release")
        publish(args.repo, args.release, args.archive, args.manifest)
    elif args.archive:
        if not args.sha256:
            parser.error("--archive requires --sha256")
        manifest = json.loads(args.manifest.read_text()) if args.manifest else None
        print(json.dumps(intake(args.root, args.archive, args.sha256, manifest), indent=2))
    elif args.download_dir:
        args.download_dir.mkdir(parents=True, exist_ok=True)
        archive, sha, manifest, tag = download(args.repo, args.release, args.download_dir)
        print(json.dumps(intake(args.root, archive, sha, manifest, f"{args.repo}@{tag}"), indent=2))
    else:
        with tempfile.TemporaryDirectory(prefix="gmx-tick-download-") as tmp:
            archive, sha, manifest, tag = download(args.repo, args.release, Path(tmp))
            print(
                json.dumps(
                    intake(args.root, archive, sha, manifest, f"{args.repo}@{tag}"), indent=2
                )
            )


if __name__ == "__main__":
    main()
