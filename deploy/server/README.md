# GMX server collection on vitalik8

The server deployment writes beneath `/srv/gmx`; the collector's data root is
`/srv/gmx/user_data`, so candles resolve to
`/srv/gmx/user_data/data/gmx/futures`. The daily snapshot and recent-candle job
share `/srv/gmx/collection.lock` and do not restore state from GitHub Releases.

## Install and seed

Use the existing `avik:data` service identity and `/srv/gmx/{repo,venv,user_data,incidents}`.
Install the pinned repository revision at `/srv/gmx/repo` and its locked
runtime at `/srv/gmx/venv`. Run `deploy/server/prepare-gmx-data-root.sh` once
before collection. It creates symlinks from the collector's expected
`/srv/gmx/user_data/data/gmx/{futures,snapshots,tickers,apy,volumes,ticks,tick_volume,checkpoints}`
paths to the existing top-level directories without moving data. Existing
non-empty target directories cause the script to stop. Install the user systemd
units from `deploy/systemd/user/` into `~/.config/systemd/user/`; the `avik`
user manager has lingering enabled so schedules continue without an interactive
login. The optional `/srv/gmx/collector.env` may contain
`ARBITRUM_RPC_URL` and `HYPERSYNC_API_TOKEN`; keep it mode `0600`, owned by
`avik:data`, and never put credentials in unit files or the repository.

Before the first collection, compare file counts and coverage through the
symlinked paths against the existing directories. Inventory existing candle
gaps; repair those still inside the API window and write out-of-window gaps to
`/srv/gmx/incidents`.

Run a bounded manual daily snapshot and OHLCV refresh. Confirm their outputs,
reports, and checkpoints remain beneath `/srv/gmx`. The recent-candle job
runs every 12 hours with a 160-hour continuity audit window, just under the
shortest observed 1-minute API history window. Live probes found at least
6 days 22 hours of 1-minute history (10,000-row endpoint limit), with no 429s.
Three full runs refreshed 515 symbol/timeframe files in 259.3s, 237.4s, and
236.1s, with zero failed pairs and zero recent gaps. A 12-hour interval keeps about 13.8x
margin against the shortest observed history window while requiring only two
refreshes per day. Both runs finished far below half the interval. Recheck
these values if GMX changes endpoint limits or response behavior.

## Timers and logs

Install and review units before enabling:

```sh
install -m 0644 /srv/gmx/repo/deploy/systemd/user/gmx-* "$HOME/.config/systemd/user/"
systemctl --user daemon-reload
systemctl --user start gmx-daily-collection.service
systemctl --user start gmx-ohlcv-collection.service
journalctl --user -u gmx-daily-collection.service -u gmx-ohlcv-collection.service
```

After confirming output/state advancement and a clean recent continuity audit,
enable both schedules:

```sh
systemctl --user enable --now gmx-daily-collection.timer gmx-ohlcv-collection.timer
systemctl --user list-timers 'gmx-*'
```

A nonzero run fails its oneshot unit and is visible in the journal. Unresolved
candle gaps and failed symbol/timeframe fetches also create durable JSON files
in `/srv/gmx/incidents`.

## Stop and rollback

```sh
systemctl --user disable --now gmx-daily-collection.timer gmx-ohlcv-collection.timer
systemctl --user stop gmx-daily-collection.service gmx-ohlcv-collection.service
```

Preserve `/srv/gmx/user_data` and `/srv/gmx/incidents` during rollback. Do not
remove historical GitHub Release assets. Do not disable the old scheduled
collection path until both server jobs have been verified and the seeded state
is confirmed complete.
