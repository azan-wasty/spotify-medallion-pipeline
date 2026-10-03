# Spotify Medallion Pipeline

An end-to-end lakehouse pipeline that turns music-listening data into a "Spotify Wrapped"-style dashboard plus business analytics. Built on Apache Spark and Delta Lake (Databricks), using the Medallion architecture: Landing → Bronze → Silver → Gold → Power BI.

Semester project for the Data Analysis & Visualization course (team of 2). Phase 2 is due Oct 10, 2026 and Phase 3 Oct 24, 2026.

---

## Architecture

```
generate_data_v2.py ------+
sanitize_real_data.py ----+--->  LANDING   output/raw/dt=*/events.json, output/dims/*.json
fetch_recently_played.py -+        |
                                   v
                            BRONZE  append-only Delta, every delivery kept + ingestion metadata
                                   v
                            SILVER  cleaned, deduplicated, bad rows quarantined, MERGE
                                   v
                            GOLD    star schema (fact_listening + dims), ML features, business marts
                                   v
                            Power BI (Import mode)
```

Details: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). The full design (layer contracts, table catalog, demo scenarios, course-concept mapping) is in [AGENTS.md](AGENTS.md).

---

## Data sources

Spotify's developer API no longer offers audio features, extended quota or more than 5 test users for new apps, so the pipeline runs mainly on generated data, calibrated against real listening history:

1. **Synthetic** (`scripts/generate_data_v2.py`): 50 users in behavioural clusters (power free, casual free, power premium), with realistic time-of-day, device, timezone, skip and repeat/discovery patterns. The bulk of the volume.
2. **Real exports** (`scripts/sanitize_real_data.py`): team members' and friends' Spotify Extended Streaming History, pseudonymised and stripped of personal data before landing.
3. **Live API** (`scripts/fetch_recently_played.py`): ongoing recently-played pulls for onboarded accounts.

Every event carries a `source` tag, so the pipeline can tell the feeds apart. The event format is described in [output/raw/README.md](output/raw/README.md), with a sample in [output/raw/sample_events.json](output/raw/sample_events.json).

---

## Repository layout

```
scripts/        producers (generator, sanitizer, API fetcher) and catalog tools
notebooks/      Databricks notebooks: 00_config, 01_setup, 10 Bronze, 20-21 Silver, 30-33 Gold, 40 serving, 90 demos, run_pipeline
output/         landing zone (dt= partitions are generated locally and git-ignored)
docs/           architecture, PII handling, Phase 1 proposal
dashboards/     Power BI specifications
tests/          test plan
```

---

## Quickstart

The scripts need only Python 3.9+ (standard library, no installs).

```bash
# 1. Synthetic history: 450 days of day-partitions plus the user/catalog snapshots
python scripts/generate_data_v2.py full

# 2. Real export for one person (raw files stay local; only sanitized events land)
python scripts/sanitize_real_data.py --input-dir "./dav data/user_real_01" --user-id user_real_01

# 3. Live API pulls (needs Spotify credentials in .env; --login once per account)
python scripts/fetch_recently_played.py --user-id user_real_01 --login

# 4. Daily incremental, or a backfill of a past range
python scripts/generate_data_v2.py incremental
python scripts/generate_data_v2.py backfill --start 2026-09-01 --end 2026-09-05
```

All generator modes take `--seed N` (default 42); the same seed gives byte-identical output.

Then upload `output/` to the Databricks landing location (`LANDING_ROOT` in `00_config`), run `01_setup` once, and run `run_pipeline`.

### Catalog genres

Genres come from the curated map in `scripts/artist_genres.json`. To relabel the catalog after editing it and see the top still-unclassified artists:

```bash
python scripts/build_full_catalog.py --reclassify
```

`scripts/enrich_genres_lastfm.py` resolves unclassified artists from Last.fm tags (needs `LASTFM_API_KEY` in `.env`). It writes the catalog files but not the map, so add the artists it resolves to `artist_genres.json`; otherwise the next `--reclassify` reverts them to `unclassified`.

---

## Running the Pipeline — Backfill vs. Incremental

All three pipeline notebooks (`10_bronze_ingest`, `20_silver_dims`, `21_silver_events`) are fully parameterized via Databricks widgets. There are **no hardcoded dates or file paths**.

### Parameters (widgets in `00_config`)

| Widget | Default | Description |
|--------|---------|-------------|
| `landing_root` | cluster-default LANDING_ROOT | Path to the `output/raw` landing zone on DBFS/Volume |
| `start_date` | *(auto: latest landing partition)* | First date to process (`YYYY-MM-DD`) |
| `end_date` | *(auto: equals `start_date`)* | Last date to process (`YYYY-MM-DD`) |
| `load_type` | `incremental` | `full` / `incremental` / `backfill` |
| `batch_id` | *(auto-generated UUID)* | Delivery identifier written into every row |

### Standard incremental run (today's data)

Leave all widgets empty and run `run_pipeline`. The pipeline resolves `start_date = end_date = latest_landing_partition()` automatically and processes only the newest date in the landing zone.

```
run_pipeline  →  10_bronze_ingest  →  20_silver_dims  →  21_silver_events
                 (latest dt= only)     (upsert users/tracks)  (MERGE events)
```

### Backfill (historical date range)

Set `start_date`, `end_date`, and `load_type = backfill` in the widgets before running `run_pipeline`. The orchestrator iterates over every date in the range and calls each notebook in order.

```
# Example: backfill Sep 27 – Oct 2
start_date = 2026-09-27
end_date   = 2026-10-02
load_type  = backfill
```

You can also trigger a single notebook directly for a specific range, e.g. to re-process Bronze for one bad day without re-running Silver:

```python
# In a Databricks cell or via dbutils.notebook.run():
dbutils.notebook.run("10_bronze_ingest", timeout_seconds=0, arguments={
    "start_date": "2026-09-29",
    "end_date":   "2026-09-29",
    "load_type":  "backfill"
})
```

**Idempotency guarantee**: running the same date range twice produces zero duplicate rows. Bronze skips files whose SHA-256 checksum hasn't changed (`ops.ingestion_log`). Silver's `MERGE INTO` on business keys + `_row_hash` guard skips rows whose content hasn't changed. Both runs log `SKIPPED_UNCHANGED` in `ops.pipeline_execution_logs`.

---

## Data Models — Bronze & Silver

> **Note**: Delta Lake does not enforce primary keys at the engine level. Uniqueness is guaranteed by the MERGE key in Silver and checked by post-load assertions in the notebooks. `PK` below denotes the logical primary/business key.

Full column-level documentation is also in [`docs/DATA_DICTIONARY.md`](docs/DATA_DICTIONARY.md).

---

### Bronze Layer

#### `bronze.events` — append-only; PK per delivery = `event_id` + `_batch_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `event_id` | STRING | N | Unique play ID (`evt_syn_*` for synthetic events, Spotify URI-derived for real) |
| `user_id` | STRING | N | Pseudonymous user identifier (`user_real_0N`, `user_synth_NNN`) |
| `track_id` | STRING | N | Spotify track URI |
| `played_at` | STRING | N | UTC ISO-8601 timestamp with `Z` suffix, kept as delivered (cast in Silver) |
| `ms_played` | LONG | Y | Milliseconds played; NULL for live-API rows |
| `skipped` | BOOLEAN | Y | Whether the track was skipped; NULL for live-API rows |
| `device_type` | STRING | Y | Raw device label as delivered by the source |
| `source` | STRING | N | `synthetic` / `synthetic_persona_matched` / `real` / `real_api` |
| `_source_file` | STRING | N | Landing file path this row came from |
| `_partition_dt` | DATE | N | `dt=YYYY-MM-DD` partition value parsed from the file path |
| `_batch_id` | STRING | N | Pipeline run / delivery ID (UUID) |
| `_rescued_data` | STRING | Y | Fields that did not match the declared schema (schema drift capture) |
| `load_timestamp` | TIMESTAMP | N | UTC timestamp when the row was written to Bronze |

#### `bronze.users` — snapshot per run; PK `user_id` + `_batch_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | STRING | N | Pseudonymous user identifier |
| `country` | STRING | Y | ISO-2 country code |
| `timezone` | STRING | Y | IANA timezone string |
| `is_premium` | BOOLEAN | Y | Spotify Premium flag |
| `_source_file` | STRING | N | Landing file path |
| `_batch_id` | STRING | N | Delivery ID |
| `_rescued_data` | STRING | Y | Schema drift capture |
| `load_timestamp` | TIMESTAMP | N | Ingestion timestamp |

#### `bronze.catalog` — snapshot per run; PK `track_id` + `_batch_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `track_id` | STRING | N | Spotify track URI (primary key) |
| `track_name` | STRING | Y | Track title |
| `artist` | STRING | Y | Primary artist name |
| `genre` | STRING | Y | Genre from curated `artist_genres.json`; `unclassified` if unknown |
| `release_year` | INT | Y | NULL for real-export tracks (not in the export format) |
| `duration_sec` | INT | Y | Track duration in seconds |
| `popularity` | INT | Y | 30–100 popularity score (rescaled from play counts for real tracks) |
| `danceability` | DOUBLE | Y | Deterministic pseudo audio feature (0–1) |
| `energy` | DOUBLE | Y | Deterministic pseudo audio feature (0–1) |
| `valence` | DOUBLE | Y | Deterministic pseudo audio feature (0–1) |
| `tempo_bpm` | DOUBLE | Y | Deterministic pseudo audio feature |
| `_source_file` | STRING | N | Landing file path |
| `_batch_id` | STRING | N | Delivery ID |
| `_rescued_data` | STRING | Y | Schema drift capture |
| `load_timestamp` | TIMESTAMP | N | Ingestion timestamp |

#### `bronze.events_quarantine` — append-only

| Column | Type | Description |
|--------|------|-------------|
| `raw_record` | STRING | Full raw JSON of the rejected record |
| `_reason` | STRING | `TYPE_MISMATCH` / `UNPARSEABLE` / `MISSING_REQUIRED` |
| `_source_file` | STRING | Landing file path |
| `_partition_dt` | DATE | Partition date |
| `_batch_id` | STRING | Delivery ID |
| `load_timestamp` | TIMESTAMP | When the row was quarantined |

---

### Silver Layer

#### `silver.events` — current state; PK `event_id`; MERGE key = `event_id`

| Column | Type | Nullable | Description / Cast |
|--------|------|----------|--------------------|
| `event_id` | STRING | N | **PK** — carried from Bronze |
| `user_id` | STRING | N | Must exist in `silver.users` |
| `track_id` | STRING | N | May be absent from `silver.tracks` — see `is_catalog_matched` |
| `played_at` | TIMESTAMP | N | **Cast** from STRING (`try_to_timestamp`), UTC |
| `ms_played` | INT | Y | **Cast** from LONG; must be `>= 0` |
| `skipped` | BOOLEAN | Y | Carried from Bronze |
| `device_type` | STRING | N | Mapped to controlled vocabulary (`mobile`, `desktop`, `web`, `smart_speaker`, `other`) |
| `data_source` | STRING | N | `source` renamed; `real` becomes `real_export` |
| `is_catalog_matched` | BOOLEAN | N | `true` if `track_id` found in `silver.tracks` |
| `completion_rate` | DOUBLE | Y | `ms_played / (duration_sec * 1000)`; NULL when `ms_played` is NULL |
| `hour_of_day` | TINYINT | N | 0–23, extracted from `played_at` UTC |
| `day_of_week` | TINYINT | N | 1 = Monday … 7 = Sunday |
| `is_weekend` | BOOLEAN | N | `day_of_week IN (6, 7)` |
| `session_id` | STRING | N | 30-minute gap session ID (user-scoped) |
| `play_sequence_num` | INT | N | Position within the session (1-indexed) |
| `dq_flags` | ARRAY\<STRING\> | Y | Non-fatal data-quality warnings (e.g. `HIGH_COMPLETION`) |
| `_row_hash` | STRING | N | `sha2` of all business columns — MERGE skips rows where this is unchanged |
| `_partition_dt` | DATE | N | UTC date of the play |
| `_batch_id` | STRING | N | Batch that last wrote or updated the row |
| `load_timestamp` | TIMESTAMP | N | Set on INSERT and on UPDATE; unchanged rows keep their original value |

#### `silver.users` — current state; PK `user_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `user_id` | STRING | N | **PK** |
| `country` | STRING | Y | Upper-case ISO-2 country code |
| `timezone` | STRING | Y | IANA timezone string |
| `is_premium` | BOOLEAN | Y | Spotify Premium flag |
| `_row_hash` | STRING | N | Hash of business columns for MERGE guard |
| `_batch_id` | STRING | N | Last batch to write this row |
| `load_timestamp` | TIMESTAMP | N | Last insert or update time |

#### `silver.tracks` — current state; PK `track_id`

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `track_id` | STRING | N | **PK** — Spotify track URI |
| `track_name` | STRING | Y | Track title |
| `artist` | STRING | Y | Primary artist name |
| `genre` | STRING | Y | Genre label |
| `release_year` | INT | Y | NULL for real-export tracks |
| `duration_sec` | INT | Y | Duration in seconds |
| `popularity` | INT | Y | 30–100 |
| `danceability` | DOUBLE | Y | 0–1 |
| `energy` | DOUBLE | Y | 0–1 |
| `valence` | DOUBLE | Y | 0–1 |
| `tempo_bpm` | DOUBLE | Y | Beats per minute |
| `_row_hash` | STRING | N | Hash of business columns for MERGE guard |
| `_batch_id` | STRING | N | Last batch to write this row |
| `load_timestamp` | TIMESTAMP | N | Last insert or update time |

#### `silver.events_quarantine` — append-only; natural key `event_id` + `_batch_id` + `_dq_rule`

All business columns from `bronze.events`, plus:

| Column | Type | Description |
|--------|------|-------------|
| `_dq_rule` | STRING | Rule that failed (e.g. `INVALID_PLAYED_AT`, `NEGATIVE_MS_PLAYED`) |
| `_dq_reason` | STRING | Human-readable description |
| `_quarantined_at` | TIMESTAMP | When the row was quarantined |
| `_batch_id` | STRING | Delivery ID |
| `load_timestamp` | TIMESTAMP | Ingestion timestamp |

---

### Ops Layer

#### `ops.pipeline_execution_logs` — audit log; PK `log_id`

| Column | Type | Description |
|--------|------|-------------|
| `log_id` | STRING | UUID per log row |
| `run_id` | STRING | One ID per `run_pipeline` execution |
| `batch_id` | STRING | Delivery ID written into the data |
| `layer` | STRING | `RAW_TO_BRONZE` / `BRONZE_TO_SILVER` |
| `target_table` | STRING | e.g. `bronze.events`, `silver.events` |
| `load_type` | STRING | `FULL` / `INCREMENTAL` / `BACKFILL` |
| `parameter` | STRING | File path (Raw→Bronze) or `start_date..end_date` (Bronze→Silver) |
| `start_time` | TIMESTAMP | Execution start |
| `end_time` | TIMESTAMP | Execution end |
| `duration_sec` | DOUBLE | Wall-clock seconds |
| `status` | STRING | `SUCCESS` / `FAILURE` / `SKIPPED_UNCHANGED` |
| `rows_read` | LONG | Rows read from source |
| `rows_inserted` | LONG | Rows appended or MERGE-inserted |
| `rows_updated` | LONG | Rows MERGE-updated |
| `rows_deleted` | LONG | Rows MERGE-deleted |
| `rows_quarantined` | LONG | Rows sent to quarantine |
| `error_message` | STRING | NULL on success; contains `SCHEMA_DRIFT` notes for rescued columns |
| `load_timestamp` | TIMESTAMP | When the log row was written |

#### `ops.ingestion_log` — per-file checksum control; PK `_source_file` + `_batch_id`

| Column | Type | Description |
|--------|------|-------------|
| `_source_file` | STRING | Landing file path |
| `_partition_dt` | DATE | Partition date |
| `sha256` | STRING | SHA-256 of the file contents |
| `row_count` | LONG | Rows in the file |
| `file_bytes` | LONG | File size in bytes |
| `status` | STRING | `LOADED` / `SKIPPED_UNCHANGED` / `REJECTED` |
| `reason` | STRING | Reason for `REJECTED` or NULL |
| `bronze_version` | LONG | Delta table version at time of write |
| `load_timestamp` | TIMESTAMP | When this control row was written |

---

## Privacy

Raw Spotify exports are never committed and never enter the lakehouse. Real people appear only as pseudonyms (`user_real_01`, ...). IP addresses, device model strings and connection country are dropped before landing. See [docs/PII_NOTES.md](docs/PII_NOTES.md).

---

## Status

**Phase 2** (due Oct 10, 2026):
- ✅ Bronze ingest (`10_bronze_ingest`) — checksum-idempotent, schema drift → quarantine
- ✅ Silver dimensions (`20_silver_dims`) — SCD1 MERGE for users and tracks
- ✅ Silver events (`21_silver_events`) — DQ rules, sessionisation, MERGE upsert
- ✅ Audit logging (`ops.pipeline_execution_logs`, `ops.ingestion_log`)
- ✅ Parameterized backfills via Databricks widgets

**Phase 3** (due Oct 24, 2026): Gold layer (star schema, ML features, business marts), demo scenarios, Power BI dashboard — in progress.

The full design checklist is at the end of [AGENTS.md](AGENTS.md).
