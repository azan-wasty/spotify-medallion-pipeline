# Spotify Medallion Pipeline — Data Dictionary (Bronze & Silver)

This document provides column-level documentation, data types, nullability, and primary key definitions for all Bronze, Silver, quarantine, and operations tables implemented in Phase 2.

> **Delta Lake Note on Primary Keys:** Delta Lake does not natively enforce uniqueness constraints on Primary Keys. Primary key integrity in this pipeline is guaranteed via idempotent hash-guarded `MERGE INTO` operations on Silver and checksum-based deduplication on Bronze.

---

## 1. Bronze Layer Tables

The Bronze layer is an append-only landing archive storing raw data with full ingestion provenance.

### `bronze.events`
- **Description:** Append-only log of raw listening events. Preserves every delivery verbatim.
- **Delivery Primary Key:** `event_id` + `_batch_id`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `event_id` | `StringType` | No | Unique play event identifier (`evt_syn_*`, `evt_real_*`, `evt_api_*`) |
| `user_id` | `StringType` | No | Pseudonymous listener identifier (`user_real_0N`, `user_synth_NNN`) |
| `track_id` | `StringType` | No | Spotify track URI (`spotify:track:...`) |
| `played_at` | `StringType` | No | Raw ISO-8601 UTC timestamp string with `Z` suffix |
| `ms_played` | `LongType` | Yes | Milliseconds played (null for `real_api` pulls) |
| `skipped` | `BooleanType` | Yes | Track skip flag (null for `real_api` pulls) |
| `device_type` | `StringType` | Yes | Raw device label reported by producer |
| `source` | `StringType` | No | Origin: `synthetic`, `synthetic_persona_matched`, `real`, or `real_api` |
| `_source_file` | `StringType` | No | Fully-qualified landing file path |
| `_partition_dt` | `DateType` | No | Date partition key extracted from folder structure |
| `_batch_id` | `StringType` | No | Ingestion delivery / batch UUID |
| `_rescued_data` | `StringType` | Yes | Captured JSON payload for schema drift / undeclared columns |
| `load_timestamp` | `TimestampType` | No | UTC timestamp when record was appended to Bronze |

---

### `bronze.users`
- **Description:** Append-only dimension delivery archive for listeners.
- **Delivery Primary Key:** `user_id` + `_batch_id`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `user_id` | `StringType` | No | Pseudonymous listener identifier |
| `country` | `StringType` | Yes | 2-letter ISO country code |
| `timezone` | `StringType` | Yes | IANA timezone string |
| `is_premium` | `BooleanType` | Yes | Subscription status flag at time of delivery |
| `_source_file` | `StringType` | No | Source landing file path |
| `_batch_id` | `StringType` | No | Ingestion batch UUID |
| `_rescued_data` | `StringType` | Yes | Captured payload for unexpected schema additions |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of Bronze ingestion |

---

### `bronze.catalog`
- **Description:** Append-only dimension delivery archive for tracks and audio features.
- **Delivery Primary Key:** `track_id` + `_batch_id`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `track_id` | `StringType` | No | Spotify track URI |
| `track_name` | `StringType` | Yes | Track title |
| `artist` | `StringType` | Yes | Primary artist name |
| `genre` | `StringType` | Yes | Curated or inferred musical genre |
| `release_year` | `IntegerType` | Yes | Track release year (null when unavailable) |
| `duration_sec` | `IntegerType` | Yes | Track duration in seconds |
| `popularity` | `IntegerType` | Yes | Rescaled popularity metric (0-100) |
| `danceability` | `DoubleType` | Yes | Musical danceability score (0.0 - 1.0) |
| `energy` | `DoubleType` | Yes | Musical energy score (0.0 - 1.0) |
| `valence` | `DoubleType` | Yes | Musical positiveness / valence score (0.0 - 1.0) |
| `tempo_bpm` | `DoubleType` | Yes | Tempo in beats per minute |
| `_source_file` | `StringType` | No | Source landing file path |
| `_batch_id` | `StringType` | No | Ingestion batch UUID |
| `_rescued_data` | `StringType` | Yes | Rescued drift columns |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of Bronze ingestion |

---

### `bronze.events_quarantine`
- **Description:** Dead-letter store for unparseable raw JSON records or fatal type mismatches during Landing → Bronze ingest.
- **Natural Key:** `_source_file` + `load_timestamp` + row index

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `raw_record` | `StringType` | Yes | Unparsed raw JSON payload |
| `_reason` | `StringType` | No | Failure cause: `TYPE_MISMATCH`, `UNPARSEABLE`, or `MISSING_REQUIRED` |
| `_source_file` | `StringType` | No | Source landing file path |
| `_partition_dt` | `DateType` | Yes | Target partition date if discernable |
| `_batch_id` | `StringType` | No | Ingestion batch UUID |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of quarantine logging |

---

## 2. Silver Layer Tables

The Silver layer enforces strict schemas, cleans data, derives analytical features, isolates bad records into quarantine, and maintains current state via `MERGE INTO`.

### `silver.events`
- **Description:** Conformed listening stream with sessionisation, temporal attributes, and data quality flags.
- **Business Primary Key:** `event_id` (enforced via MERGE)

| Column Name | PySpark Type | Nullable | Description / Transformation |
|---|---|---|---|
| `event_id` | `StringType` | No | Unique play event identifier |
| `user_id` | `StringType` | No | Foreign Key to `silver.users` (validated against orphan rule) |
| `track_id` | `StringType` | No | Foreign Key to `silver.tracks` |
| `played_at` | `TimestampType` | No | UTC Timestamp parsed from ISO-8601 string |
| `ms_played` | `IntegerType` | Yes | Non-negative integer (constraint: `ms_played >= 0`) |
| `skipped` | `BooleanType` | Yes | Boolean skip indicator |
| `device_type` | `StringType` | No | Normalized device category: `mobile`, `desktop`, `web_player`, `smart_speaker`, `other` |
| `data_source` | `StringType` | No | Standardized source: `synthetic`, `synthetic_persona_matched`, `real_export`, `real_api` |
| `is_catalog_matched` | `BooleanType` | No | True if track exists in `silver.tracks` |
| `completion_rate` | `DoubleType` | Yes | Ratio: `ms_played / (duration_sec * 1000)` clamped to [0.0, 1.0] |
| `hour_of_day` | `ByteType` (tinyint) | No | Hour of play in UTC (0 - 23) |
| `day_of_week` | `ByteType` (tinyint) | No | Day of week in UTC (1 = Sunday, 7 = Saturday) |
| `is_weekend` | `BooleanType` | No | True if play occurred on Saturday or Sunday |
| `session_id` | `StringType` | No | Session identifier partitioned by user with 30-minute inactivity boundary |
| `play_sequence_num` | `IntegerType` | No | 1-based sequential play index within `session_id` |
| `dq_flags` | `ArrayType(StringType)` | Yes | Non-fatal data quality warnings (e.g. `UNMATCHED_CATALOG`) |
| `_row_hash` | `StringType` | No | SHA-256 hash of business attributes; skips identical rows on MERGE |
| `_partition_dt` | `DateType` | No | Date partition key |
| `_batch_id` | `StringType` | No | Batch UUID that last updated or inserted the record |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of last Silver MERGE write |

---

### `silver.users`
- **Description:** Deduplicated listener dimension updated via SCD Type 1 `MERGE`.
- **Business Primary Key:** `user_id`

| Column Name | PySpark Type | Nullable | Description / Transformation |
|---|---|---|---|
| `user_id` | `StringType` | No | Primary key identifier |
| `country` | `StringType` | Yes | Uppercase 2-letter ISO country code |
| `timezone` | `StringType` | Yes | Validated IANA timezone string |
| `is_premium` | `BooleanType` | No | Current subscription tier indicator |
| `_row_hash` | `StringType` | No | SHA-256 hash of user attributes for change detection |
| `_batch_id` | `StringType` | No | Batch UUID of most recent update |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of last Silver MERGE write |

---

### `silver.tracks`
- **Description:** Deduplicated music catalog dimension updated via SCD Type 1 `MERGE`.
- **Business Primary Key:** `track_id`

| Column Name | PySpark Type | Nullable | Description / Transformation |
|---|---|---|---|
| `track_id` | `StringType` | No | Spotify URI primary key |
| `track_name` | `StringType` | No | Cleaned track title (defaulted to 'Unknown Track' if null) |
| `artist` | `StringType` | No | Cleaned artist name (defaulted to 'Unknown Artist' if null) |
| `genre` | `StringType` | No | Music genre classification |
| `release_year` | `IntegerType` | Yes | Year of release |
| `duration_sec` | `IntegerType` | No | Track duration in seconds |
| `popularity` | `IntegerType` | No | Relative popularity metric (0 - 100) |
| `danceability` | `DoubleType` | No | Pseudo audio feature (0.0 - 1.0) |
| `energy` | `DoubleType` | No | Pseudo audio feature (0.0 - 1.0) |
| `valence` | `DoubleType` | No | Pseudo audio feature (0.0 - 1.0) |
| `tempo_bpm` | `DoubleType` | No | Track tempo in BPM |
| `_row_hash` | `StringType` | No | SHA-256 hash of track attributes |
| `_batch_id` | `StringType` | No | Batch UUID of most recent update |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of last Silver MERGE write |

---

### `silver.events_quarantine`
- **Description:** Quarantined event records that violated fatal data quality rules during Bronze → Silver processing.
- **Natural Key:** `event_id` + `_batch_id` + `_dq_rule`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `event_id` | `StringType` | No | Originating event ID |
| `user_id` | `StringType` | Yes | Originating user ID |
| `track_id` | `StringType` | Yes | Originating track URI |
| `played_at` | `StringType` | Yes | Originating raw played_at string |
| `ms_played` | `LongType` | Yes | Originating ms_played |
| `skipped` | `BooleanType` | Yes | Originating skipped value |
| `device_type` | `StringType` | Yes | Originating device type |
| `source` | `StringType` | Yes | Originating source label |
| `_source_file` | `StringType` | Yes | Originating file path |
| `_partition_dt` | `DateType` | Yes | Originating partition date |
| `_dq_rule` | `StringType` | No | Violated rule: `INVALID_PLAYED_AT`, `NEGATIVE_MS_PLAYED`, `ORPHAN_USER`, `INVALID_DATA_SOURCE` |
| `_dq_reason` | `StringType` | No | Detailed descriptive diagnostic |
| `_quarantined_at`| `TimestampType` | No | Timestamp when the validation check failed |
| `_batch_id` | `StringType` | No | Ingestion batch UUID |
| `load_timestamp` | `TimestampType` | No | UTC timestamp of quarantine insert |

---

## 3. Operations & Governance Tables (`ops`)

The `ops` schema tracks pipeline health, runtime metrics, and file-level idempotency control.

### `ops.pipeline_execution_logs`
- **Description:** Central audit log for every step execution across full, incremental, and backfill loads.
- **Primary Key:** `log_id`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `log_id` | `StringType` | No | Unique UUID per execution log entry |
| `run_id` | `StringType` | No | Pipeline driver invocation run identifier |
| `batch_id` | `StringType` | No | Processing batch UUID |
| `layer` | `StringType` | No | Step identifier: `RAW_TO_BRONZE` or `BRONZE_TO_SILVER` |
| `target_table` | `StringType` | No | Target table (e.g. `bronze.events`, `silver.events`) |
| `load_type` | `StringType` | No | Execution mode: `FULL`, `INCREMENTAL`, or `BACKFILL` |
| `parameter` | `StringType` | No | Processed parameter (e.g., file path or `start_date..end_date`) |
| `start_time` | `TimestampType` | No | Step start UTC timestamp |
| `end_time` | `TimestampType` | No | Step completion UTC timestamp |
| `duration_sec` | `DoubleType` | No | Elapsed execution duration in seconds |
| `status` | `StringType` | No | Final outcome: `SUCCESS`, `FAILURE`, or `SKIPPED_UNCHANGED` |
| `rows_read` | `LongType` | No | Number of records scanned |
| `rows_inserted` | `LongType` | No | Number of new records inserted |
| `rows_updated` | `LongType` | No | Number of records updated via MERGE |
| `rows_deleted` | `LongType` | No | Number of records deleted (if applicable) |
| `rows_quarantined` | `LongType` | No | Number of records diverted to quarantine |
| `error_message` | `StringType` | Yes | Null on clean success; records error details or schema drift notices |
| `load_timestamp` | `TimestampType` | No | UTC timestamp when the log row was committed |

---

### `ops.ingestion_log`
- **Description:** File-level checksum ledger that guarantees Bronze ingestion idempotency.
- **Primary Key:** `_source_file` + `_batch_id`

| Column Name | PySpark Type | Nullable | Description |
|---|---|---|---|
| `_source_file` | `StringType` | No | Landing file URI |
| `_partition_dt` | `DateType` | No | Partition date associated with the landing file |
| `sha256` | `StringType` | No | SHA-256 hex digest of the landing file contents |
| `row_count` | `LongType` | No | Record count parsed from the landing file |
| `file_bytes` | `LongType` | No | File size in bytes |
| `status` | `StringType` | No | Ingestion state: `LOADED`, `SKIPPED_UNCHANGED`, or `REJECTED` |
| `reason` | `StringType` | Yes | Rejection reason or null |
| `bronze_version`| `LongType` | Yes | Delta table version resulting from the write |
| `load_timestamp` | `TimestampType` | No | UTC timestamp when this control row was committed |
