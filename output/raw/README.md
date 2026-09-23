# Landing Zone - Raw Events (`output/raw/`)

This directory serves as the landing zone for raw listening event partitions.

## Structure
```
output/raw/
  dt=2026-09-16/
    events.json
    _manifest.json (checksum, row counts, timestamp)
  dt=2026-09-17/
    events.json
  ...
```

## Producers
1. **Synthetic Generator (`generate_data_v2.py`)**: Bulk historical baseline (`full`) or single-day partitions (`incremental`).
2. **Real Data Sanitizer (`scripts/sanitize_real_data.py`)**: Merges sanitized Extended Streaming History export data.
3. **Live API Pull (`scripts/fetch_recently_played.py`)**: Merges live OAuth recently-played API pulls.
