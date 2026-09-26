# Landing Zone - Raw Events (`output/raw/`)

This directory serves as the landing zone for raw listening event partitions.

## Structure
```
output/raw/
  dt=2026-09-16/
    events.json
    _manifest.json (TODO: checksum, row counts, timestamp)
  dt=2026-09-17/
    events.json
  ...
```
`dt=` is the UTC date of `played_at`. The `dt=*` folders are generated locally and git-ignored.

## Event format
Each `events.json` is one JSON array of events:

| Field | Type | Notes |
|---|---|---|
| `event_id` | string | `evt_syn_…` (generator), `evt_real_…` (export), `evt_api_…` (live API) |
| `user_id` | string | `user_real_NN` (pseudonymised real person / persona) or `user_synth_NNN` |
| `track_id` | string | Spotify track URI |
| `played_at` | string | UTC, `YYYY-MM-DDTHH:MM:SSZ` |
| `ms_played` | int or null | `null` for `real_api` (the endpoint doesn't report it) |
| `skipped` | bool or null | `null` for `real_api` |
| `device_type` | string or null | `mobile`, `desktop`, `web_player`, `smart_speaker`; `null` for `real_api` |
| `source` | string | `synthetic`, `synthetic_persona_matched`, `real`, `real_api` |

`sample_events.json` is one generated day (2026-09-16, default seed) trimmed to the three personas and one filler user from each behavioural cluster, so the format can be inspected without running the generator. It contains synthetic rows only; real rows are never committed.

## Producers
1. **Synthetic Generator (`scripts/generate_data_v2.py`)**: Bulk historical baseline (`full`), single-day partitions (`incremental`) or re-delivered ranges (`backfill`).
2. **Real Data Sanitizer (`scripts/sanitize_real_data.py`)**: Merges sanitized Extended Streaming History export data.
3. **Live API Pull (`scripts/fetch_recently_played.py`)**: Merges live OAuth recently-played API pulls.
