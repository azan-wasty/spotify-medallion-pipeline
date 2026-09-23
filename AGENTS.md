# AGENTS.md — Spotify Medallion Pipeline

This file orients any AI coding agent (or new contributor) working in this repo. Read this before making changes.

> **Course alignment.** The design follows the two course chapters this project is graded against: **Ch. 1, Delta Lake lakehouse format** (transaction log, ACID, time travel, schema enforcement, DML) and **Ch. 3, Medallion architecture** (Landing → Bronze → Silver → Gold). Where the project deliberately departs from the textbook pattern, the section says so and why. The [Course Concept Coverage](#course-concept-coverage) section maps every examinable concept to where it is implemented or demonstrated.

---

## Project Overview

**Course**: Data Analytics & Visualization / Data Engineering semester project
**Team size**: 2
**Phases**: Phase 1 (proposal) → Phase 2 (due Oct 10, 2026) → Phase 3 (due Oct 24, 2026)

**Goal**: build an end-to-end, automated data pipeline using Apache Spark (Databricks) and Delta Lake tables that ingests personalized music-listening data, processes it through Landing → Bronze → Silver → Gold layers (Medallion Architecture), and culminates in a Power BI / Tableau dashboard in the style of "Spotify Wrapped."

---

## Why This Project Doesn't Use the Spotify API as Its Primary Source

Spotify significantly restricted developer access between late 2024 and early 2026:
- `audio-features` / `audio-analysis` endpoints deprecated for all new apps since Nov 27, 2024.
- Extended API quota restricted to businesses with 250k+ MAU since May 2025.
- Since Feb 11, 2026: creating a Developer app requires an active Premium subscription; Development Mode test-user cap is 5.

**Decision**: the pipeline is built primarily on a **synthetic listener-behavior generator**, with a small set of **real, sanitized listener data** (from team members' and friends' Extended Streaming History exports) layered in for validation/demo realism — not as the primary volume source. This is a deliberate engineering trade-off, not a shortcut; document it as such if asked.

---

## Data Sources

Three-way split, confirmed:
1. **Synthetic** — the bulk of full + incremental volume, fully controlled.
2. **Real, export-based** — full-load history from Extended Streaming History exports (multiple people, sanitized via `sanitize_real_data.py`).
3. **Real, live API-based** — ongoing incremental pulls via OAuth for accounts within the dev app's 5-test-user cap, using `fetch_recently_played.py`.

How each source maps onto the course's **full vs. incremental load** vocabulary is in [Landing Zone](#landing-zone--detailed-design).

**Incremental strategy per persona, made explicit**: not every real persona will have live API access (either because they weren't set up before the 5-user cap, or haven't done the one-time login yet). For those, incremental data is **synthetic but behaviorally calibrated** to that persona's real derived profile (`favorite_genres`, `intensity`, `skip_rate`) — not generic filler. This means every real persona gets:
- **Full load**: always real (from their export).
- **Incremental load**: real (live API) where onboarded, otherwise persona-matched synthetic.

Use `data_source = synthetic_persona_matched` (not plain `synthetic`) for these calibrated-but-generated rows in `silver.events`, so it's distinguishable from both genuine live pulls and pure filler-user synthetic data.

### Real personas derived so far (3 of an expected 6-7)
Analyzed from real Extended Streaming History exports, pseudonymized, and used to calibrate `PERSONAS` in `generate_data_v2.py`:

| Pseudonym | Genre cluster | Events/active day (real) | Skip rate | Notes |
|---|---|---|---|---|
| `user_real_01` | Indie/alt-rock + hip-hop + pop (Arctic Monkeys, Joji, 21 Savage, The Weeknd) | 68.0 | 37.9% | Heavy PlayStation + Windows use |
| `user_real_02` | Bollywood/Sufi/Punjabi (Atif Aslam, Nusrat Fateh Ali Khan, AP Dhillon, Arijit Singh) | 57.5 | 42.3% (highest) | Mobile-dominant |
| `user_real_03` | J-pop/J-rock/anime (YOASOBI, Ado, Yorushika, BABYMETAL) | 58.8 | 17.9% (lowest — near-completionist) | Mixed tablet/mobile/windows |

**Key calibration finding**: real listeners average 57-68 events per *active* day — far higher than the original placeholder personas' 6-10 assumption. `PERSONAS` intensity values were set to 40-45 (a deliberate slight scale-down for demo legibility) rather than the literal real average; bump toward the real figures for the actual Phase 2 full-load run if a more faithful volume is wanted.

**Filler population uses a bell curve, not a flat range**: `EXTRA_RANDOM_USERS`' intensity is drawn from a truncated normal (`INTENSITY_MEAN=15`, `INTENSITY_STD=15`, floored at 1) rather than the old flat `randint(3,9)`. This puts the three real personas (40-45) at roughly **1.7-2.0 standard deviations above the filler population's mean** — i.e., they're realistically rare, very-heavy listeners relative to a normal population, not just "one option among many similar users."

**3-4 more real personas expected.** Repeat the same analysis pass per new export (top artists by play count/ms_played, skip rate, active-day intensity, device mix, `conn_country`) and check whether their genre cluster is genuinely new (as with the two clusters below) or overlaps with existing personas.

**Catalog clusters added to support real personas** (previously catalog was Western-genre-only): `indie-alt` (for user_real_01's Joji/The Neighbourhood/Chase Atlantic taste), `bollywood`/`punjabi`/`sufi-pop`/`qawwali`/`pakistani-pop`/`indie-pop-desi` (for user_real_02), `jpop`/`jrock`/`jmetal` (for user_real_03). Add a new cluster whenever a new persona's real genre isn't yet represented.

### 1. Synthetic data (primary — `generate_data_v2.py`)
- A catalog dimension with deterministic pseudo audio-features (danceability, energy, valence, tempo) standing in for Spotify's now-blocked audio-feature fields, now spanning Western genres plus the real-persona-derived clusters above.
- `PERSONAS`: named synthetic users calibrated to real personas' actual listening taste (genres, intensity) as their exports get analyzed — see table above. Additional generic filler users (`user_synth_NNN`) round out the population for volume.
- Three run modes:
  - `python3 generate_data_v2.py full` — bulk-generates historical day-partitions (default 450 days back from today).
  - `python3 generate_data_v2.py incremental [--date YYYY-MM-DD]` — generates exactly one day's partition; this is the job meant to be re-run daily/periodically.
  - `python3 generate_data_v2.py backfill --start YYYY-MM-DD --end YYYY-MM-DD` — regenerates a past date range in place, for testing Spark merge/backfill logic (also useful for quick demo samples over a short window).

### 2. Real data (secondary — validation/demo only — `sanitize_real_data.py`)
- Sourced from Spotify's **Extended Streaming History** export (self-requested via account privacy settings; can take up to 30 days to arrive). Multiple files per year are normal for heavy listeners — the export splits large years into `_1`, `_2` suffixed files; `sanitize_real_data.py`'s glob pattern already picks up all of them per person.
- Each real person assigned a pseudonym (`user_real_01`, `02`, ...) — **never** their real name, including in conversation/file names outside this repo.
- `sanitize_real_data.py` strips PII, transforms each export into the same event schema as the synthetic data, and writes into the same `output/raw/dt=YYYY-MM-DD/events.json` partitioned structure. Run once per person:
  ```
  python3 sanitize_real_data.py --input-dir ./raw_exports/personX --user-id user_real_0N --output-dir output/raw
  ```
- Real and synthetic events for the same date land in the same partition file (the script merges rather than overwrites).
- Note: exports do not include a `username` field (contrary to some older documentation) — one less PII field to strip than originally assumed. The field is `ip_addr`, not `ip_addr_decrypted`.

### 3. Real catalog extraction (replaces the hand-picked catalog — `real_catalog_extract.json`)
Instead of a small hand-picked track list, the catalog is now **extracted from actual real listening data**:
- Pulled the top 200 tracks by real play count across all analyzed real users' exports, keeping only tracks whose artist could be confidently genre-classified (`ARTIST_GENRE_MAP`, currently ~100 artists spanning Western/South Asian/Japanese clusters).
- These 200 tracks cover **90,265 of ~197,735 total real plays analyzed so far (46%)** — the rest is a long tail of ~3,800 mostly one-off artists, deliberately excluded rather than genre-guessed. (Consequence for the pipeline: real events referencing long-tail tracks have no catalog row. Silver flags them and Gold handles them as early-arriving facts; see those sections.)
- Real Spotify URIs are used directly as `track_id` (not synthetic `trk_XXXX` placeholders). `release_year` is honestly left `null` since exports don't include it.
- `popularity` is rescaled from real play counts (30-100 range), not randomly assigned.
- `generate_data_v2.py`'s `load_real_catalog_override()` automatically uses `real_catalog_extract.json` if present in the working directory, falling back to the small hand-picked `CATALOG_SEED` only if that file is missing.
- **Regenerate this file whenever new real personas are added** — re-run the same extraction/classification pass (top tracks by play count from confidently-classified artists) across *all* real users combined, expanding `ARTIST_GENRE_MAP` for any new artists that appear.

### Recommended build order (once more real users' data is collected)
Thanks to a merge-safety fix (see Landing Zone notes below), scripts no longer have a strict destructive-overwrite ordering requirement — but this is still the logical sequence:
1. Refresh `real_catalog_extract.json` across all real users' exports combined (expand `ARTIST_GENRE_MAP` for new artists).
2. Update `PERSONAS` in `generate_data_v2.py` with each new real user's derived genre/intensity/skip profile.
3. `python3 generate_data_v2.py full` — lay down the synthetic historical base using the refreshed catalog/personas.
4. `python3 sanitize_real_data.py --input-dir ... --user-id user_real_0N` — once per real person, merges their real events on top.
5. `python3 fetch_recently_played.py --user-id user_real_0N --login` — once per live-API account (one-time OAuth), then the same command without `--login` on an ongoing schedule.
6. Spot-check a few day partitions to confirm real events for each persona actually landed (didn't get silently dropped).
7. Upload `output/` to the Databricks landing location and run the `run_pipeline` notebook (see [Orchestration](#orchestration--pipeline-run-order)).

---

## PII Rules — Read Before Touching Any Real Data

These are non-negotiable for this repo:

1. **Never commit raw Spotify export files** (`Streaming_History_Audio_*.json`) to version control, not even as "samples." They contain real IP addresses (`ip_addr`) for every play.
2. **Always drop `ip_addr`** (and `ip_addr_decrypted` / `user_agent_decrypted` if present) before any real data reaches a committed file or the landing zone. `sanitize_real_data.py` already does this — don't bypass it.
3. **Real user identities are pseudonyms only** (`user_real_01`, etc.) in every file that touches the repo. The real-name-to-pseudonym mapping lives only in a local, uncommitted note — never in the codebase, commit messages, or docs.
4. If any export contains podcast/audiobook rows (`episode_name` or `audiobook_title` populated), `sanitize_real_data.py` currently **skips** those rows — extend `transform_event()` deliberately if a persona's real data needs them, don't silently include them.
5. Synthetic data requires no sanitization (no real PII by construction) but should still flow through the same Silver-layer transformation code path as real data, for consistency.
6. See `output/PII_NOTES.md` (generated by `generate_data_v2.py`) for the full write-up used in the Phase 1 proposal.
7. **Right to be forgotten.** If a real person withdraws consent, delete their pseudonym from every layer (Bronze, Silver, quarantine, Gold) *and* their rows from the landing files. A Delta `DELETE` is only a **soft delete**: the old Parquet files are tombstoned but still reachable through time travel. Physically removing the data takes `REORG TABLE ... APPLY (PURGE)` (needed when deletion vectors are on) followed by `VACUUM`. See demo scenario 10.

---

## Why a Lakehouse (Delta Lake) — Course Ch. 1

All three architectures target OLAP (analytical querying of large data). The project's fit:

| | Data warehouse | Data lake (plain files) | **Lakehouse (Delta Lake) — chosen** |
|---|---|---|---|
| Data types | Structured only; our sources are semi-structured JSON from 3 producers | Any format | Any format |
| Scaling | Scale up, one machine | Scale out, cheap storage | Scale out, cheap storage |
| Guarantees | ACID | BASE (no ACID: eventually consistent) | ACID via the transaction log |
| Failure mode that matters here | — | Failed/partial writes leave orphaned files → duplicates, wrong counts ("data swamp") | A failed job never commits, so readers never see it |

The project already hit the plain-lake failure mode in miniature: `generate_data_v2.py`'s old `write_partition()` silently overwrote a day's JSON file and destroyed real rows merged in earlier. Plain files have no transactions, so nothing prevented or recorded it. **From Bronze onward, every write is a Delta commit.**

Delta Lake features this project relies on:
- **ACID transactions** — each Bronze/Silver/Gold write is one atomic commit. A failed job never reaches the log, so a half-written load is invisible (the course's *partial-file failure* scenario: without Delta, a rerun plus the leftover partial file double-counts; with Delta, the partial file is never referenced).
- **Transaction log (`_delta_log/`)** — the single source of truth. Each commit is a numbered JSON file (`00000000000000000000.json`, `...01.json`, ...) recording files added/removed and the schema; a Parquet **checkpoint** is written every 10 commits by default so readers don't replay the whole log.
- **MVCC + time travel** — writes create new Parquet files instead of editing old ones, so older versions stay queryable: `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE`.
- **Soft delete + `VACUUM`** — DELETE/UPDATE/MERGE mark old files as removed (tombstones); `VACUUM` physically deletes them after the retention period (7 days by default).
- **Schema enforcement / evolution** — writes with wrong columns/types are rejected; `mergeSchema` allows additive changes.
- **DML** — `MERGE` (upsert), `UPDATE`, `DELETE`, used by Silver and Gold.
- **Change Data Feed (CDF)** — row-level change tracking; Gold reads only what changed in Silver.
- **Audit history** — `DESCRIBE HISTORY` shows who changed what, when, with which operation.
- **Table features** — `SHOW TBLPROPERTIES` shows `minReaderVersion` / `minWriterVersion` and enabled features (deletion vectors, CDF).
- **Unified batch/streaming** — same API for both; available as an option for Bronze ingestion (see Bronze), not the primary path.

Not used: **Delta Kernel** and **UniForm**. Only Spark reads these tables, so there's no cross-engine or cross-format (Iceberg/Hudi) need. Cover them as theory in the report.

**Rule**: never read a Delta table's folder with `spark.read.parquet(...)`. The folder still holds tombstoned files, so you'd get duplicates. Always read through the log: `spark.read.table(...)` or `format("delta")`.

---

## Medallion Architecture Plan

Principles taken from the course:
- **Layers are logical, not physical.** Physically this project has landing files plus four Databricks schemas: `bronze`, `silver`, `gold`, `ops` (pipeline metadata/control tables). Gold is split into three sublayers.
- **There is no strict industry definition of each layer**, so this project defines its own contract (table below). Changing what a layer is responsible for means updating this table first.
- **The landing zone is a separate pre-Bronze zone** (the course leaves this as a design choice). Landing = files, schema-on-read. Bronze starts where data becomes a queryable Delta table.

```
generate_data_v2.py -------+
sanitize_real_data.py -----+-->  LANDING   output/raw/dt=*/events.json, output/dims/*.json
fetch_recently_played.py --+        |      JSON files, schema-on-read, PII already removed
                                    |      checksum + file-level validation (intrusive: bad file halts run)
                                    v
                             BRONZE  bronze.events, bronze.users, bronze.catalog
                                    |      Delta, append-only, every delivery kept + ingestion metadata
                                    |      latest delivery -> clean -> conform -> DQ (nonintrusive: bad rows quarantined)
                                    v
                             SILVER  silver.events (+ silver.events_quarantine), silver.users, silver.tracks
                                    |      current state, source-oriented, row-level features only
                                    |      dims before facts, surrogate keys, SCD2, business rules, aggregation
                                    v
                             GOLD    G1 star schema:   gold.dim_* + gold.fact_listening
                                     G2 ML features:   gold.ml_*
                                     G3 business marts: gold.peak_usage_patterns ... gold.wrapped_user_year
                                    |      export -> Power BI Import mode (VertiPaq)
                                    v
                             SERVING Power BI semantic model + dashboard
```

### Layer contract (this project's standards)

| | Landing | Bronze | Silver | Gold |
|---|---|---|---|---|
| **Purpose** | Raw drop zone for the 3 producers | Validated raw data in Delta; historical record and single source of truth | Clean, standardized, read-optimized; feeds Gold, ML and operational queries | Business-ready: star schema, ML features, business marts |
| **Format** | JSON files in `dt=` folders | Delta, partitioned by `_partition_dt` | Delta | Delta (+ export for Power BI) |
| **Data model** | Source schema as the scripts produce it | Source schema + ingestion metadata columns | Mirrors Bronze 1:1, friendly names, smallest types | Kimball star schema + flat marts |
| **Schema** | Schema-on-read | Schema-on-write (explicit `StructType`); `mergeSchema` for additive changes | Enforced + `CHECK` constraints | Enforced |
| **Transformation** | None (PII removed upstream) | Minimal: add metadata columns, convert to Delta | Light: latest delivery, dedup, cast, standardize, flag, row-level features | Harmonize, surrogate keys, SCD2, aggregate, business rules |
| **Write mode** | File rewrite (merge-safe) | `append` only | `MERGE` (upsert) per affected day | `MERGE` (dims, fact) / overwrite (marts) |
| **History** | Latest file per day | Every delivery kept | Current state only (SCD1) | SCD2 on `dim_user` |
| **Quality gate** | File-level, **intrusive**: bad file → rejected, run halts | Load logged in `ops.ingestion_log` | Row-level, **nonintrusive**: bad rows → `silver.events_quarantine` | Row-count reconciliation vs Silver |
| **Who reads it** | Pipeline only | Pipeline + debugging | ML notebooks, analysts | Power BI, report |
| **Owner** | TBD | TBD | TBD | TBD |

### Table catalog

Every table also carries a table comment and column comments in Databricks (`COMMENT`), so this list and the catalog stay in sync.

| Table | Layer | Grain | Write pattern | Earlier draft name |
|---|---|---|---|---|
| `bronze.events` | Bronze | one row per event per delivery | append | — |
| `bronze.users` | Bronze | one row per user per snapshot | append | — |
| `bronze.catalog` | Bronze | one row per track per snapshot | append | — |
| `ops.ingestion_log` | ops | one row per landing file per run | append | — |
| `ops.pipeline_state` | ops | one row per downstream table (last processed version) | MERGE | — |
| `silver.events` | Silver | one row per unique play (`event_id`) | MERGE | `silver_events` |
| `silver.events_quarantine` | Silver | one row per rejected event row | append | — |
| `silver.users` | Silver | one row per user (current) | MERGE (SCD1) | — |
| `silver.tracks` | Silver | one row per track (current) | MERGE (SCD1) | — |
| `gold.dim_user` | Gold G1 | one row per user *version* | MERGE (SCD2) | `dim_user` |
| `gold.dim_track` | Gold G1 | one row per track | MERGE (SCD1 + inferred members) | `dim_track` |
| `gold.dim_date` | Gold G1 | one row per calendar day | overwrite (generated) | `dim_time` |
| `gold.dim_time` | Gold G1 | one row per hour of day (24 rows) | overwrite (generated) | `dim_time` |
| `gold.dim_device` | Gold G1 | one row per device type | MERGE (SCD1) | — |
| `gold.fact_listening` | Gold G1 | one row per play | MERGE via CDF | `fact_listening_summary` |
| `gold.ml_user_track_interactions` | Gold G2 | user × track | overwrite | `silver_user_track_interactions` |
| `gold.ml_track_features` | Gold G2 | track | overwrite | `silver_track_features` |
| `gold.ml_user_sessions` | Gold G2 | session | overwrite | `silver_user_sessions` |
| `gold.peak_usage_patterns` | Gold G3 | hour × weekday × device category × country | overwrite | `gold_peak_usage_patterns` |
| `gold.subscription_propensity_segments` | Gold G3 | user | overwrite | `gold_subscription_propensity_segments` |
| `gold.content_performance_trends` | Gold G3 | month × genre / artist | overwrite | `gold_content_performance_trends` |
| `gold.wrapped_user_year` | Gold G3 | user × year | overwrite | part of `fact_listening_summary` |

---

## Landing Zone — Detailed Design

Purpose: the drop zone where the three producers write. Files only, schema-on-read, nothing is a table yet.

### Layout (Raw Data Output Structure)

```
output/                     <- LANDING ZONE
  raw/
    dt=2026-09-16/
      events.json           <- merged real + synthetic events for this day
      _manifest.json        <- (TODO) row_count, sha256, per-source row counts, written_at, writer script
    dt=2026-09-17/
      events.json
    ...
  dims/
    catalog.json            <- track/artist/genre dimension (full snapshot)
    users.json              <- synthetic + named persona user dimension (full snapshot)
  PII_NOTES.md
```

This day-partitioned layout is a **hard requirement from the TA** — it's what enables Phase 2's automated pipeline merges and backfills to be demonstrated meaningfully. Do not collapse this back into single flat CSV files.

**Target change for dims (TODO)**: write each snapshot to `dims/snapshot_dt=YYYY-MM-DD/{catalog,users}.json` instead of overwriting. The course's full-load pattern keeps every past delivery in dated folders. Until this is done, the append-only `bronze.users` / `bronze.catalog` tables are the only history of the dims.

For the pipeline, `output/` is uploaded as-is to the Databricks landing location (`LANDING_ROOT` in `00_config`).

### Landing-zone steps (course: decompress → checksum → generate metadata)
- **Decompress**: the Spotify export arrives as a `.zip`; it's unzipped manually before `sanitize_real_data.py`, outside the lakehouse. Nothing in the landing zone is compressed.
- **Checksum + metadata (TODO)**: every producer writes `_manifest.json` next to the `events.json` it touched: row count, SHA-256 of the file, per-source row counts, write time, writer script. Stdlib only (`hashlib`, `json`). The Bronze notebook verifies files against their manifest before loading.
- **PII classification**: done *before* landing (see scoping note below), not inside Bronze.

### Full vs. incremental loads (course terminology mapped to producers)

| Producer / run mode | Load type | What it delivers |
|---|---|---|
| `generate_data_v2.py full` | Full (initial) | 450 days of `dt=` partitions + dims snapshot |
| `sanitize_real_data.py` | Full (one-off per person) | A real person's whole export history, merged into the `dt=` partitions |
| `generate_data_v2.py incremental` | Incremental | Exactly one new `dt=` partition |
| `fetch_recently_played.py` | Incremental | Recently played events for onboarded accounts, merged into the matching `dt=` partition |
| `generate_data_v2.py backfill` | Re-delivery | Regenerates past `dt=` partitions (real rows preserved) |
| `dims/catalog.json`, `dims/users.json` | Full snapshot every run | Small, so always delivered whole |

Course requirements for incremental loads, and how they're met:
- *A unique ID or an `updated_at` column*: every event has `event_id` and `played_at`.
- *Source must not modify records older than the last increment (otherwise CDC is needed)*: backfills and API top-ups **do** rewrite older day files. So this project does not use a watermark. Instead each landing file's checksum is tracked in `ops.ingestion_log`, and any new or changed file is loaded as a new delivery (see Bronze).

### Merge-safety of the landing files
`generate_data_v2.py`'s `write_partition()` is **merge-safe** (previously a bug: it silently overwrote a day's file, which could destroy real data already merged in by `sanitize_real_data.py` or `fetch_recently_played.py` for that same date). It now preserves any existing rows tagged `source: "real"` or `source: "real_api"` before writing its own synthetic rows, so re-running `full`/`backfill` can no longer wipe out real data regardless of script run order.

**Event `source` tagging** (added by each script, used by the merge-safety logic above, by Bronze's `_source_type` and by Silver's `data_source` column): `synthetic` (generic filler users), `synthetic_persona_matched` (named real-taste personas' generated events), `real` (from `sanitize_real_data.py`), `real_api` (from `fetch_recently_played.py`).

**Important scoping note (deliberate deviation from the textbook)**: raw, unsanitized Spotify export files (the ones containing real `ip_addr` values) never enter the landing zone or Bronze. The course's Bronze processing flow includes an optional "classify/encrypt PII" step (e.g., encrypting with Fernet). This project instead **drops** the PII before landing, via `sanitize_real_data.py`. Encryption would only make sense if the IPs had some later analytical use, and they have none. Normally Bronze holds truly untouched raw data; here there's no reason for raw IP addresses to persist anywhere in the lakehouse, even in a restricted raw zone.

---

## Bronze Layer — Detailed Design

Purpose (course): store source data in its **original state**, queryable, as an **immutable historical record and single source of truth**. No cleaning, no joins, no dedup — that's Silver's job. Bronze exists so that any later bug in Silver logic can always be recovered from by reprocessing, without re-running ingestion. Business users and Power BI never query Bronze.

### Tables
- `bronze.events` — every delivery of every `dt` partition, appended. Partitioned by `_partition_dt` (mirrors the landing layout).
- `bronze.users`, `bronze.catalog` — every dims snapshot, appended. Silver picks the latest.

### Ingestion metadata columns
Added when loading into Delta, not written into the JSON files:
- `_ingested_at` — when the row was loaded into Bronze (distinct from `played_at`, which is when the track was actually played).
- `_source_type` — the event's `source` tag carried through verbatim: `synthetic` / `synthetic_persona_matched` / `real` / `real_api`. (Silver renames `real` → `real_export`.)
- `_source_file` — the landing file path (`_metadata.file_path`), for traceability during debugging/backfill verification.
- `_partition_dt` — the `dt=` value from the path.
- `_batch_id` — one ID per pipeline run; identifies a *delivery*.

### Processing steps (course's Bronze processing flow, as applied)
1. **Discover** — list landing files and compare each file's SHA-256 with the last `LOADED` entry in `ops.ingestion_log` (the course's *metadata control table* for tracking the last load). New or changed → load. Unchanged → `SKIPPED_UNCHANGED`.
2. **Validate the file** (intrusive, see policy below) — plain Python (stdlib `json` + `hashlib`): checksum matches the manifest, the file parses, row count matches the manifest, required fields are present. New unknown fields are logged as `SCHEMA_DRIFT`, a warning rather than a failure.
3. **Convert to Delta** — read the validated files with the explicit schema, add the metadata columns, `append`.
4. **Log** — one `ops.ingestion_log` row per file: `_batch_id`, file path, `_partition_dt`, checksum, row count, file bytes, arrival time (file mtime), loaded time, status (`LOADED` / `SKIPPED_UNCHANGED` / `REJECTED`), reason, resulting Bronze table version.

```python
events = (spark.read
          .schema(BRONZE_EVENT_SCHEMA)       # explicit StructType from 00_config: schema-on-write starts here
          .option("mode", "FAILFAST")        # files were validated in step 2; any malformed row aborts the whole commit
          # .option("multiLine", "true")     # only if events.json is one JSON array instead of JSON Lines — check the producers
          .json(validated_files)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_partition_dt", F.to_date(F.regexp_extract("_source_file", r"dt=(\d{4}-\d{2}-\d{2})", 1)))
          .withColumn("_source_type", F.col("source"))
          .withColumn("_batch_id", F.lit(BATCH_ID))
          .withColumn("_ingested_at", F.current_timestamp()))

(events.write.format("delta")
       .mode("append")                       # Bronze is append-only
       .option("mergeSchema", "true")        # additive schema evolution (see Schema management)
       .partitionBy("_partition_dt")
       .saveAsTable("bronze.events"))
```

### Append, not merge — and why backfills stay in Bronze
- The course offers two ways to integrate incremental data: **append** (new rows only; suits logs and events) and **merge** (upsert on a key). Bronze always **appends**. Merge is used in Silver and Gold.
- A backfill or an API top-up re-delivers a whole `dt` file. Bronze appends it as a new delivery with a newer `_batch_id`; the older delivery stays. This is the course's **historization in Bronze**: Bronze is an audit archive that can recreate "what did we know on day X", and it lets Silver be rebuilt after a bad Silver change.
- Silver then applies one rule: **for each `_partition_dt`, only the latest delivery counts.**
- **Immutability rule**: never `overwrite`, `UPDATE` or `DELETE` Bronze. The only exception is the right-to-be-forgotten procedure (PII rule 7).

Alternative considered: Auto Loader / Structured Streaming with `trigger(availableNow=True)` would show the course's unified batch/streaming idea. It isn't the primary path because Auto Loader tracks *new* files, and this project rewrites existing day files on backfill/API top-up (Auto Loader ignores overwritten files unless `cloudFiles.allowOverwrites` is set). A checksum-based control table handles that cleanly. Optional demo only.

### Schema management
- **Schema-on-read** in Landing (JSON, drift detected during validation); **schema-on-write** from Bronze on, with the explicit `BRONZE_EVENT_SCHEMA` / `BRONZE_USER_SCHEMA` / `BRONZE_CATALOG_SCHEMA` StructTypes in `00_config`. Never use `inferSchema` in pipeline code, because inferred types can change silently between runs.
- **Additive change** (a producer adds a field): add it to the StructType. `mergeSchema` adds the column to the table; old rows get `null`. A column missing from new files stays in the table and new rows get `null`.
- **Type change**: never silent. Widening within the same type family (int → long) via Delta type widening where supported; otherwise a deliberate overwrite with `overwriteSchema=true`, recorded in this doc.
- **Disruptive change** (rename, incompatible type): new pipeline version with a new target table (e.g., `bronze.events_v2`).
- **Policy** (course: restrict disruptive changes; metadata-driven): producer scripts may only make backward-compatible, additive changes. Any schema change updates the StructType in `00_config` and this doc in the same commit.
- **Schema enforcement** does the rest: a write whose columns/types don't match the table is rejected and the transaction is cancelled.

### Validation policy: intrusive vs. nonintrusive

| Problem | Level | Handling |
|---|---|---|
| Checksum mismatch, unparseable JSON, row count ≠ manifest, required field missing | File | **Intrusive**: file marked `REJECTED` in `ops.ingestion_log`, copied to `_rejected/`, run halts before Silver. Fix the producer and rerun. |
| Unknown new field | File | Warning (`SCHEMA_DRIFT`), load continues; someone decides whether to add it to the schema |
| `ms_played` null from `real_api`, negative values, unknown `track_id`/`user_id`, bad country code | Row | **Nonintrusive**: lands in Bronze as-is; Silver flags or quarantines it |

Why hybrid: a corrupt day file would make that day's Gold numbers wrong, so it must stop the run. Row-level issues are normal in real data and shouldn't block the other few thousand rows of that day.

### Historization vs. time travel
- Long-term history lives in **Bronze rows** (every delivery kept), not in Delta time travel. Time travel is for recovery and audit over days; `VACUUM` removes old files after the retention period, so it is **not** a long-term archive.
- Recovery from a bad load: `DESCRIBE HISTORY bronze.events` → inspect with `VERSION AS OF n` → `RESTORE TABLE bronze.events TO VERSION AS OF n` → fix → rerun.

### Governance
- **Access**: Bronze is read by the pipeline and for debugging only. Power BI gets the `gold` schema only.
- **Ingestion log**: `ops.ingestion_log` (above) records time, source and outcome of every file.
- **Anomaly checks** at the end of each Bronze run, printed and failing the run when breached: a day's row count outside ±50% of the trailing 7-day median; a missing `dt` in the expected date range; a file arriving more than a day after its `dt`.
- **Incident response** (short runbook): stop before Gold runs on bad data → `DESCRIBE HISTORY` to find the bad commit → `RESTORE` the affected tables → fix the producer/rule → rerun from Landing. After restoring a Silver table, rebuild Gold fully instead of relying on CDF across the restore.
- **Catalog**: table and column comments on every table, plus the table catalog above.
- Run `OPTIMIZE` on `bronze.events` occasionally: daily partitions produce many small files.

---

## Silver Layer — Detailed Design

Purpose (course): **cleanse, standardize and slightly enrich**, while staying **source-oriented** (one Silver table per Bronze entity, no cross-source integration). Silver is queryable and feeds Gold, ML and operational queries directly. Only minimal business rules live here: data types, reference data, naming, simple calculated values.

### Tables

| Table | From | Grain | History |
|---|---|---|---|
| `silver.users` | latest snapshot in `bronze.users` | one row per user | current state (SCD1) |
| `silver.tracks` | latest snapshot in `bronze.catalog` | one row per track | current state (SCD1) |
| `silver.events` | latest delivery per day in `bronze.events` | one row per unique `event_id` | current state |
| `silver.events_quarantine` | rows of `bronze.events` that failed a rule | one row per rejected row | append-only |

Load `silver.users` and `silver.tracks` **before** `silver.events`, because the orphan check needs them.

**Harmonization note**: `silver.events` unions the four event feeds (synthetic, persona-matched, real export, real API). This isn't the cross-source integration the course places in Gold: the producers already conform to one event schema before landing, and every feed describes the same entity (a play). `data_source` keeps them distinguishable.

### Step 1 — Take the latest delivery
For each `_partition_dt` loaded in this run, keep only rows from the newest `_batch_id`. A backfill or an API top-up therefore *replaces* that day in Silver instead of adding to it. Recompute sessions for the affected days ±1 day, because sessions can cross midnight.

### Step 2 — Clean (course's cleaning activities, mapped)

| Course activity | Rule in this project | On failure |
|---|---|---|
| Duplicates | One row per `event_id` (overlapping live-API pulls produce repeats) | Keep newest, drop the rest |
| Correct data types | `played_at` → TIMESTAMP (UTC), `ms_played` → INT, `skipped` → BOOLEAN | Failed cast → quarantine |
| Standard formats | Timestamps in UTC, dates `yyyy-MM-dd`, snake_case column names | — |
| Trimming | Trim every string column; empty string → NULL | — |
| Consistency | `device_type` mapped to the controlled list in `00_config` (built from the values the producers actually emit, e.g. "PS4"/"PlayStation" → `playstation`); `country` upper-case ISO-2; source tag `real` → `real_export` | Unknown value → `other` + entry in `dq_flags` |
| Ranges | `ms_played >= 0`; `played_at` not in the future and not before 2008 (Spotify launch) | Quarantine |
| Missing values | `ms_played` / `skipped` are NULL for `real_api`: **kept as NULL, no imputation**, and `data_source` explains why | NULL from any other source → quarantine |
| Constraints / no orphans | `user_id` must exist in `silver.users` | Quarantine |
| | `track_id` missing from `silver.tracks` is *expected* (real long tail, new tracks from the API) | Flag `is_catalog_matched = false`; Gold treats it as an early-arriving fact |
| Anomalies / outliers | `ms_played` > 3 × track duration | Flag only |
| PII masking | Already done before landing; assert no `ip_addr` / `ip_addr_decrypted` / `user_agent_decrypted` column exists | Fail the run |

**Quarantine, not delete** (course): failed rows go to the sibling table `silver.events_quarantine` with all original columns plus `_dq_rule`, `_dq_reason`, `_quarantined_at`. Fix the cause at the source (the producer script) where possible, then reprocess. Cleaning is iterative.

**Constraints as a last guard** (schema enforcement): a violating write fails as a whole, atomically.
```sql
ALTER TABLE silver.events ADD CONSTRAINT ms_played_non_negative CHECK (ms_played IS NULL OR ms_played >= 0);
ALTER TABLE silver.events ADD CONSTRAINT valid_data_source
  CHECK (data_source IN ('synthetic', 'synthetic_persona_matched', 'real_export', 'real_api'));
```

### Step 3 — Conform the data model
- 1:1 with Bronze, but with **friendly, consistent snake_case names** and **column comments** (e.g., `source` → `data_source`). Metadata columns `_partition_dt` and `_batch_id` are kept for traceability.
- **Smallest suitable types**: `hour_of_day`, `day_of_week` → TINYINT; `ms_played` → INT; `is_weekend`, `skipped`, `is_catalog_matched` → BOOLEAN.
- **No surrogate keys** in Silver; they belong in Gold. Silver uses business keys: `event_id`, `user_id`, `track_id`.
- **No SCD2** in Silver. The course discusses both placements and recommends Gold, since SCD2 is expensive to build and store. Silver holds current state; history stays recoverable because Bronze keeps every users/catalog snapshot.
- Not used: **3NF / data vault**. That modelling suits large enterprises integrating many sources. Here there is one domain and a few conformed feeds, and data-vault joins would be slow in Spark. Bronze reloads plus time travel already give the resilience a vault would add.

### Step 4 — Light, row-level enrichment
Computed once here and reused everywhere. These are simple calculated values at the same grain (one row per play) — no aggregation.
- `completion_rate` = `ms_played / (duration_sec * 1000)` — continuous engagement signal, more useful for ML than the binary `skipped` flag alone. NULL when the track's duration is unknown (not in catalog) or `ms_played` is NULL.
- `hour_of_day`, `day_of_week`, `is_weekend` — temporal features for skip/engagement prediction.
- `session_id` — groups a user's events where the gap between consecutive plays is under `SESSION_GAP_MINUTES` (30, set in `00_config`). Unlocks sequence-based models (next-track prediction, session-based recommendation). This is the one business-ish rule in Silver. It stays here because the course notes Silver is often used directly for ML, and sequence models need sessions at row grain.
- `play_sequence_num` — position of a track within its session, ordered by `played_at`. Needed for any RNN/Transformer-style "predict the next track" model.
- `is_catalog_matched`, `dq_flags` — from Step 2.

### Step 5 — Write with MERGE (upsert)
```sql
MERGE INTO silver.events AS t
USING silver_events_updates AS s                 -- cleaned rows for the days affected in this run
ON t.event_id = s.event_id
WHEN MATCHED AND t._row_hash <> s._row_hash THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
WHEN NOT MATCHED BY SOURCE AND t._partition_dt IN (<affected days>) THEN DELETE   -- event vanished from a backfilled day
```
`_row_hash` = `sha2` over the business columns, so unchanged rows aren't rewritten. `silver.users` and `silver.tracks` use the same pattern, keyed on `user_id` / `track_id`.

Enable Change Data Feed so Gold can load incrementally:
```sql
ALTER TABLE silver.events SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
```

### Automation (course: Bronze → Silver is the easiest step to automate)
Bronze → Silver is predictable (rename, cast, filter, DQ, lookups, defaults), so it's **metadata-driven**. `00_config` holds, per Silver table: column renames, target types, dedup key and DQ rules (`rule_id`, expression, action = `quarantine` | `flag`). The notebook reads the config; adding a rule means editing config, not code (parameterization).

### ML use of Silver
`silver.events` is used directly for row-level models (skip/completion prediction, next-track sequences). Aggregated feature tables are built in the Gold ML sublayer (G2). See the ML task → table mapping there.

---

## Gold Layer — Detailed Design

Purpose (course): the most refined layer, where sources are **harmonized** and **business rules, surrogate keys, SCD2 and aggregations** live. It's the most complex layer, so it's split into three sublayers:

- **G1 — Core star schema (Kimball)**: `gold.dim_*` + `gold.fact_listening`. The public interface of the data; it must match how business users think.
- **G2 — ML feature tables**: `gold.ml_*`. Earlier drafts placed these in Silver, but they are aggregations, which the course puts in Gold. (The course also mentions organizations that keep a separate sandbox/ML layer for feature engineering; here that's a Gold sublayer.)
- **G3 — Business marts**: one table per business question + the Wrapped summary, built on G1.

Gold splits into two audiences: **consumer-facing Wrapped-style summaries** (the flagship dashboard feature) and **business-facing operational/growth analytics** (what the Phase 1 rubric's "business questions" requirement is actually asking for). Both are needed — Wrapped alone doesn't satisfy the rubric.

### G1. Star schema

Kimball steps (course order):
1. **Requirements**: the three business questions + the Wrapped page (G3).
2. **Grain**: `fact_listening` = **one row per play event**, the finest grain available. Everything else aggregates up from it.
3. **Dimensions**: user, track, date, time (hour), device.
4. **Facts (measures)**: `ms_played`, `minutes_played`, `play_count` (always 1), `is_skipped` (0/1), `completion_rate`.

| Dimension | Business key | SCD handling | Main attributes |
|---|---|---|---|
| `gold.dim_user` | `user_id` | **Type 2** on `is_premium` (+ plan/country if present in `users.json`); Type 1 on the rest | `user_sk`, `user_id`, `source_system` (synthetic / real), descriptive attributes from `silver.users`, `valid_from`, `valid_to`, `is_current`, `type1_hash`, `type2_hash`, `created_at`, `updated_at` |
| `gold.dim_track` | `track_id` | Type 1 + inferred members | `track_sk`, `track_id`, `track_name`, `artist_name`, `genre`, `danceability`, `energy`, `valence`, `tempo_bpm`, `duration_sec`, `popularity`, `release_year` (nullable), `is_inferred` |
| `gold.dim_date` | `date_key` (yyyymmdd) | static, generated | `date`, `year`, `quarter`, `month`, `month_name`, `week_of_year`, `day_of_week`, `day_name`, `is_weekend` |
| `gold.dim_time` | `hour_of_day` | static, 24 rows | `hour_label`, `day_part` (Night / Morning / Afternoon / Evening) |
| `gold.dim_device` | `device_type` | Type 1 | `device_sk`, `device_type`, `device_category` (mobile / desktop / console / tablet / tv / web / other) |

`gold.fact_listening` columns: `event_id` (degenerate dimension: an ID kept in the fact with no dim table), `user_sk`, `track_sk`, `date_key`, `hour_of_day`, `device_sk`, `session_id`, `country` and `data_source` (degenerate), measures, `created_at`, `updated_at`.

**Loading dimensions** (course procedure):
- **Harmonize first** in a temp view: decode values (e.g., `device_type` → `device_category`), replace NULLs with `'Unknown'`, compute `type1_hash` / `type2_hash`.
- **Compare incoming vs. existing on the business key**:
  - no match → insert with a new surrogate key;
  - match and `type2_hash` changed → expire the current row (`valid_to`, `is_current = false`) and insert a new version;
  - match and only `type1_hash` changed → overwrite in place.
- **Surrogate keys**: `BIGINT GENERATED ALWAYS AS IDENTITY` (or a hash of business key + `valid_from`). Stable, with no business meaning.
- **Overlapping keys from different sources**: the ID namespaces already differ (`user_synth_*` vs `user_real_*`; Spotify URIs vs `trk_*`), and `source_system` is stored anyway.
- **Validity conventions**: a user's first version gets `valid_from = 1900-01-01`, so it covers all historical plays. Later versions take the snapshot timestamp. Open rows get `valid_to = 9999-12-31`.

```sql
-- staged_users = current silver.users + type1_hash, type2_hash, _snapshot_ts
MERGE INTO gold.dim_user AS t
USING (
  SELECT s.user_id AS merge_key, s.* FROM staged_users s                 -- matches existing rows
  UNION ALL
  SELECT NULL AS merge_key, s.* FROM staged_users s                      -- extra copy -> inserted as the new version
  JOIN gold.dim_user d ON d.user_id = s.user_id AND d.is_current AND d.type2_hash <> s.type2_hash
) AS u
ON t.user_id = u.merge_key AND t.is_current
WHEN MATCHED AND t.type2_hash <> u.type2_hash THEN                       -- tracked attribute changed: expire old version
  UPDATE SET is_current = false, valid_to = u._snapshot_ts, updated_at = current_timestamp()
WHEN MATCHED AND t.type1_hash <> u.type1_hash THEN                       -- only Type-1 attributes changed: overwrite
  UPDATE SET type1_hash = u.type1_hash, updated_at = current_timestamp() /* + each Type-1 column */
WHEN NOT MATCHED THEN                                                    -- brand-new user, or new version of a changed one
  INSERT (user_id, source_system, is_premium, type1_hash, type2_hash, valid_from, valid_to, is_current, created_at, updated_at)
  VALUES (u.user_id, u.source_system, u.is_premium, u.type1_hash, u.type2_hash,
          CASE WHEN u.merge_key IS NULL THEN u._snapshot_ts ELSE TIMESTAMP'1900-01-01' END,
          TIMESTAMP'9999-12-31', true, current_timestamp(), current_timestamp())
```

**Loading facts** (course procedure):
- **Dimensions before facts**, always (enforced by the run order).
- **Replace business keys with surrogate keys**. `user_sk` uses a point-in-time lookup, so a play is linked to the user version valid when it happened:
  ```sql
  JOIN gold.dim_user u ON e.user_id = u.user_id AND e.played_at >= u.valid_from AND e.played_at < u.valid_to
  ```
- **Early-arriving facts**: a play whose `track_id` isn't in `dim_track` yet (real long-tail tracks, new tracks from the live API). Before the fact load, insert a placeholder **inferred member** (`is_inferred = true`, genre `'Unknown'`, names from the event if it carries them). When a later catalog refresh contains the track, the Type-1 update fills it in and sets `is_inferred = false`. The surrogate key doesn't change, so facts stay linked.
- **Incremental via CDF**: read only what changed in Silver since the last processed version (kept in `ops.pipeline_state`), then MERGE into the fact on `event_id`. Deletes in Silver become deletes in the fact.
  ```python
  changes = (spark.read.format("delta")
             .option("readChangeFeed", "true")
             .option("startingVersion", last_version + 1)
             .table("silver.events")
             .filter("_change_type != 'update_preimage'"))
  ```
- **Load optimization** (course): `type1_hash` / `type2_hash` on dims for fast change detection; `created_at` / `updated_at` on every Gold table to find new/changed rows.

### G2. ML feature tables (moved from Silver)
Built from `silver.events` + `silver.tracks`, not from the star schema: ML wants business keys and row-level context, not surrogate keys.

#### `gold.ml_user_track_interactions` — for collaborative filtering
One row per (user, track): `play_count`, `total_ms_played`, `avg_completion_rate`, `first_played_at`, `last_played_at`. Matches the implicit-feedback interaction-matrix shape ALS/matrix-factorization models expect directly.

#### `gold.ml_track_features` — for content-based recommendation
Catalog enriched with cross-user aggregates: `genre`, `danceability`/`energy`/`valence`/`tempo_bpm`, plus `avg_completion_rate_across_users`, `total_play_count`, `skip_rate`. Usable for similarity search and cold-start recommendation independent of any one user's history.

#### `gold.ml_user_sessions` — for sequential/behavioral models
One row per session: `user_id`, `session_id`, `session_start`, `session_end`, `track_count`, `genre_diversity` (unique genres in session), `dominant_device`. Feeds churn-risk/engagement-trend models — session frequency/length is a stronger churn signal than raw play counts.

#### Mapping: ML task → table
| ML task | Table |
|---|---|
| Collaborative filtering recommender (ALS) | `gold.ml_user_track_interactions` |
| Content-based / cold-start recommender | `gold.ml_track_features` |
| Skip/completion prediction (classifier) | `silver.events` |
| Next-track / sequential recommendation | `silver.events` ordered by `session_id`, `play_sequence_num` |
| Churn / re-engagement prediction | `gold.ml_user_sessions` |
| Genre/mood clustering | `gold.ml_track_features` |

**Reconciliation rule**: exclude `data_source == 'real_api'` rows when training anything needing `completion_rate` (skip/engagement models), since those fields are null for that source. Fine to include for collaborative filtering and sequence models, which only need that a play happened.

### G3. Business marts (business questions + consumer summaries)

**Business rules live only here** (course: business rules belong in Gold): engagement-score weights, propensity-tier thresholds and the definition of "discovery" are defined once in `00_config` and used only by Gold notebooks.

#### A. Business questions & supporting tables

**1. "When do users listen most, and how should infrastructure/capacity planning respond?"**
- Table: `gold.peak_usage_patterns` — play count and listening minutes by `hour_of_day`, `day_of_week`, `device_category`, `country` (from `gold.fact_listening` joined to `dim_time`, `dim_date`, `dim_device`).
- Business use: informs server auto-scaling schedules, maintenance-window timing, and CDN/caching decisions around predictable peak windows.
- Visual: heatmap of listening volume across hour-of-day (x-axis) × day-of-week (y-axis).

**2. "Which free-tier users show premium-level engagement, and should be targeted for upgrade offers?"**
- Table: `gold.subscription_propensity_segments` — one row per user: `total_listening_minutes`, `session_frequency`, `genre_diversity`, `skip_rate`, `device_diversity`, `discovery_rate`, current `is_premium` status (from `dim_user` where `is_current`), and a derived `engagement_score` (weighted combination of the above).
- Business use: segments free users into propensity tiers (High/Medium/Low) so upgrade-offer campaigns target genuinely engaged listeners rather than blasting the whole free-tier base.
- Visual: bar chart of user counts per propensity tier, split by current subscription status — the "High engagement, still Free" bar is the actionable business insight.

**3. "Which genres/artists are gaining or losing traction over time, to guide content curation or licensing decisions?"**
- Table: `gold.content_performance_trends` — genre/artist share of total listening minutes, computed monthly, with month-over-month delta (from `fact_listening` joined to `dim_track`, `dim_date`). Plays on inferred (unclassified) tracks are reported as genre `'Unclassified'` rather than dropped, so shares add up to 100%.
- Business use: informs which genres/artists deserve playlist placement, editorial curation, or licensing investment.
- Visual: line chart of genre share of total listening over time, highlighting fastest-rising and fastest-declining genres.

#### B. Consumer-facing "Wrapped" summaries
- Primary model: the G1 star schema (`fact_listening` + `dim_user`, `dim_track`, `dim_date`, `dim_time`) for personalized reporting.
- Plus `gold.wrapped_user_year`: one **flat, wide row per user per year** (`total_minutes`, `top_artist_1..5`, `top_genre_1..3`, `top_track_1..5`, `discovery_rate`, `peak_hour`). This is a deliberate **One Big Table (OBT)**-style mart. The course's OBT pros apply (no joins, simple for one page), and flat columns instead of nested arrays avoid OBT's main con: nested data is hard to query and aggregate in Power BI.
- Metrics: top artists/genres per user per period, monthly listening-minutes trend, "discovery rate" (% new artists listened to per month).
- Visual: the Wrapped-style dashboard page — top artists/genres list, total minutes, discovery rate over the year.

#### Dashboard visual selection for the proposal (2-3, per rubric)
Lead with these three as the primary demo visuals, since each maps to a distinct business question rather than three variations on the same idea:
1. Peak-usage heatmap (capacity planning)
2. Subscription-propensity segment bar chart (revenue/growth)
3. Genre/artist trend line chart (content strategy) — with the Wrapped summary page as a bonus fourth, consumer-facing visual.

#### C. Additional business questions considered (ideas — not yet built into Gold tables)
Worth adding in Phase 2/3 if time allows, or mentioning in the proposal as future scope:

- **Churn/retention risk**: declining session frequency or listening-minutes trend over trailing weeks per user → flags users worth a re-engagement push. Visual: active-user-count trend line, or "days since last session" histogram. Pairs naturally with the subscription-propensity table for a "full funnel" story (who's about to leave vs. who's about to upgrade).
- **Platform/device investment priorities**: engagement (minutes, completion rate) broken down by `device_type` → informs whether to prioritize console/smart-TV vs. mobile app investment. Real data already shows a clear signal here (`user_real_01` is PlayStation-heavy, `user_real_02` is mobile-dominant).
- **Discovery vs. habit balance**: ratio of repeat-track plays vs. genuinely new-to-them tracks per week → segments "explorers" vs. "loyalists," tunes recommendation-algorithm aggressiveness.
- **Content/catalog health (skip-rate by genre/artist)**: aggregate skip rate per genre/artist → flags poor catalog matching or content-quality issues; distinct from the growth-focused genre-trend table since this is about quality, not popularity.
- **Weekday vs. weekend content patterns**: genre mix split by weekday/weekend → informs whether editorial/curated playlists should differ by day-type.
- **Multi-device engagement as a loyalty signal**: number of distinct devices per user correlated with engagement/tenure → argues for (or against) investing in cross-device continuity features.

### Serving layer (course: Gold → serving)
- Power BI reads the `gold` schema only, never Bronze or Silver.
- **Import mode**: data is copied into Power BI's in-memory columnar engine (**VertiPaq**) for fast, consistent performance. It's refreshed after each pipeline run.
- If the Databricks workspace can't expose a SQL endpoint/token to Power BI (typical of Community Edition), `40_serving_export` writes each `gold.*` table to CSV/Parquet at the end of the run and those files are imported. This export *is* the project's serving-layer replication (course: Gold data replicated to another service for a specific consumer).
- **Semantic model**: relationships mirror the star schema (fact → dims, many-to-one, single direction). Measures (Total Minutes, Skip Rate, Active Users, Discovery Rate) are defined once in the model.
- Gold governance: table and column comments on every Gold table; simple, self-explanatory names; `OPTIMIZE` after each load (read-optimized).

---

## Orchestration & Pipeline Run Order

```
notebooks/
  00_config            <- paths, StructTypes, DQ rules, device mapping, SESSION_GAP_MINUTES, business-rule constants
  01_setup             <- CREATE SCHEMA bronze/silver/gold/ops; CREATE TABLEs with comments, CHECK constraints, CDF
  10_bronze_ingest     <- landing validation -> append into bronze.* -> ops.ingestion_log -> anomaly checks
  20_silver_dims       <- latest users/catalog snapshot -> silver.users, silver.tracks (MERGE)
  21_silver_events     <- latest delivery -> clean -> quarantine -> MERGE into silver.events
  30_gold_dims         <- dim_date, dim_time, dim_device, dim_track (+ inferred members), dim_user (SCD2)
  31_gold_fact         <- fact_listening from silver.events CDF
  32_gold_ml           <- gold.ml_*
  33_gold_marts        <- business marts + wrapped_user_year
  40_serving_export    <- export gold.* for Power BI, OPTIMIZE, reconciliation report (row counts Silver vs Gold)
  90_demos             <- Phase 2 demonstration scenarios (below)
  run_pipeline         <- driver: runs 10 -> 20 -> 21 -> 30 -> 31 -> 32 -> 33 -> 40, stops on first failure
```

- **Order rules**: Bronze → Silver → Gold; Silver dims before Silver events; **Gold dims before Gold facts**; marts last.
- **Idempotent**: rerunning with no new landing files → Bronze logs `SKIPPED_UNCHANGED`, CDF has no new versions, nothing downstream changes.
- **Scheduling**: producers run locally (Python) → upload new/changed files to the landing location → `run_pipeline`. Use Databricks Jobs if your workspace has them (Community Edition doesn't; Free Edition does). Otherwise trigger `run_pipeline` manually. Apache Airflow, covered in the course's hands-on part, is the orchestration option to mention or use if time allows.
- **Automation difficulty** (course): Source → Bronze is hardest (three producers with different inputs; that's why the scripts conform everything to one event schema before landing). Bronze → Silver is easiest (metadata-driven config). Silver → Gold is hardest to parameterize (business logic), so it's hand-written notebooks with shared helpers for hashing and SCD2.

---

## Phase 2 Demonstration Scenarios

Each scenario is a cell group in `90_demos` and maps directly to course concepts:

| # | Scenario | Steps | Course concepts shown |
|---|---|---|---|
| 1 | Initial full load | `generate_data_v2.py full` + `sanitize_real_data.py` → `run_pipeline` | Full load, Bronze append, first commits in `_delta_log` |
| 2 | Daily incremental | `generate_data_v2.py incremental --date ...` → `run_pipeline` | Incremental load, control table, CDF-driven Gold |
| 3 | Backfill | `generate_data_v2.py backfill --start ... --end ...` → `run_pipeline` | Redelivery kept in Bronze (historization); Silver MERGE incl. `NOT MATCHED BY SOURCE`; Gold via CDF |
| 4 | Corrupt file | Break one `events.json` / its checksum | Intrusive validation, `REJECTED` in `ops.ingestion_log`, run halts, nothing partial committed (ACID) |
| 5 | Bad rows | Inject negative `ms_played`, an unknown `user_id` | Nonintrusive DQ, quarantine table, `CHECK` constraint rejecting a direct bad write |
| 6 | Schema evolution | Carry a new optional field (e.g., `shuffle` from real exports) | `mergeSchema` (old rows NULL); schema enforcement blocking a type change |
| 7 | Bad-load recovery | `DESCRIBE HISTORY` → `VERSION AS OF` → `RESTORE` | Time travel, MVCC, audit history |
| 8 | User upgrades to Premium | Generator flips `is_premium` for some users between snapshots | SCD2 in `dim_user`, point-in-time fact join |
| 9 | New track from live API | `fetch_recently_played.py` returns a track not in the catalog | Early-arriving fact → inferred member, later filled in |
| 10 | Right to be forgotten | `DELETE` a real pseudonym everywhere → show time travel still returns it → `REORG ... APPLY (PURGE)` + `VACUUM` → gone | Soft delete (tombstones) vs `VACUUM`, deletion vectors. Demo only: `VACUUM ... RETAIN 0 HOURS` requires disabling the retention check and breaks time travel |
| 11 | Inside the log | List `_delta_log/`, open a commit JSON (`add`/`remove` actions), find the checkpoint after 10+ commits, `SHOW TBLPROPERTIES` | Transaction log, checkpoints, table features (`minReaderVersion` / `minWriterVersion`) |
| 12 | Idempotent rerun | Run `run_pipeline` twice with no new files | Control table, no duplicate data, no empty commits downstream |

---

## Course Concept Coverage

### Ch. 1 — Delta Lake lakehouse format

| Concept | Where in this project | Demo # |
|---|---|---|
| Warehouse vs. lake vs. lakehouse; ACID vs. BASE | [Why a Lakehouse](#why-a-lakehouse-delta-lake--course-ch-1) (incl. the `write_partition()` story) | 4 |
| Table anatomy: Parquet data files, `_delta_log`, metadata, schema, checkpoints | Every table; inspected in demo | 11 |
| Transaction protocol, log = single source of truth | Rule: always read through the log | 11 |
| Partial-file failure scenario | Bronze `FAILFAST` load aborts the whole commit | 4 |
| Soft delete (tombstones) and `VACUUM` | PII rule 7 | 10 |
| MVCC, time travel | Recovery runbook | 7 |
| Schema enforcement / evolution | Bronze schema management; Silver constraints | 5, 6 |
| DML: MERGE / UPDATE / DELETE | Silver MERGE, `dim_user` SCD2, right-to-be-forgotten | 3, 8, 10 |
| Audit history | `DESCRIBE HISTORY` | 7 |
| Change Data Feed | `silver.events` → `gold.fact_listening` | 2, 3 |
| Unified batch/streaming | Auto Loader `availableNow` considered (reason for not using it in Bronze) | optional |
| Table features (vs. old protocol versions) | `SHOW TBLPROPERTIES` | 11 |
| Deletion vectors | `REORG ... APPLY (PURGE)` before `VACUUM` | 10 |
| Delta Kernel, UniForm | Theory only — not needed (single engine, single format) | — |

### Ch. 3 — Medallion architecture

| Concept | Where in this project | Demo # |
|---|---|---|
| Layers are logical; organization defines its own standards | Layer contract + table catalog | — |
| Landing zone (decompress, checksum, metadata) | Landing Zone section, `_manifest.json` | 4 |
| Full vs. incremental loads; append vs. merge | Producers table; Bronze append; Silver/Gold MERGE | 1, 2, 3 |
| Incremental-load requirements; tracking the last load | `event_id`/`played_at`; `ops.ingestion_log`, `ops.pipeline_state` | 2, 12 |
| Historization in Bronze (not SCD2); time travel ≠ archive | Every delivery kept in Bronze | 3 |
| Schema-on-read vs. schema-on-write; `mergeSchema`; incompatible changes | Bronze schema management | 6 |
| Intrusive vs. nonintrusive validation | Bronze validation policy | 4, 5 |
| PII classify/encrypt | Done pre-landing (documented deviation) | 10 |
| Bronze governance (access, logs, alerts, runbook, catalog) | Bronze governance | 4 |
| Silver cleaning activities; quarantine table | Silver Step 2 | 5 |
| Conforming names, smallest data types, comments | Silver Step 3 | — |
| SCD1 vs. SCD2 placement; surrogate keys in Gold | Silver = current state; `dim_user` SCD2 | 8 |
| Source harmonization in Gold, not Silver | Silver harmonization note | — |
| 3NF / data vault vs. denormalized | Considered, not used (Silver Step 3) | — |
| Minimal business rules in Silver; business rules in Gold | Silver Step 4; G3 | — |
| ML from Silver / separate ML layer | `silver.events` + Gold G2 | — |
| Automation difficulty per stage; metadata-driven | Silver automation; Orchestration | — |
| Star schema steps (requirements → grain → dims → facts) | Gold G1 | — |
| Loading dims (business key compare, hashes, surrogate keys) | Gold G1 | 8 |
| Loading facts (SK lookup, dims first, early-arriving facts) | Gold G1 | 9 |
| Load optimization (`type1_hash`/`type2_hash`, created/updated dates) | Gold G1 | — |
| OBT pros/cons | `gold.wrapped_user_year` | — |
| Gold sublayers / semantic layer | G1/G2/G3; Power BI semantic model | — |
| Serving layer, Power BI Import mode, VertiPaq | Serving layer | — |

---

## Tech Stack

- **Table format**: Delta Lake (Parquet data files + `_delta_log` transaction log)
- **Processing**: Apache Spark (PySpark + Spark SQL) via Databricks Community Edition. Databricks has been replacing Community Edition with **Free Edition** (serverless + Unity Catalog); check which one your workspace is. On Free Edition, put the landing files in a Unity Catalog Volume (`/Volumes/<catalog>/<schema>/<volume>/...`) and create the `bronze`/`silver`/`gold`/`ops` schemas inside a catalog.
- **Orchestration**: `run_pipeline` driver notebook; Databricks Jobs where available; Apache Airflow optional
- **BI/Dashboard**: Power BI (Import mode) or Tableau
- **Data generation/sanitization**: Python 3 (stdlib only — `json`, `hashlib`, `argparse`, `csv` — no external deps required for the generator scripts)
- **Version control**: GitHub

The course's hands-on chapters use Microsoft Fabric. The concepts carry over one-to-one: Fabric lakehouses also store Delta tables and run Spark notebooks.

---

## Conventions for Agents Working on This Repo

- Keep the generator's `PERSONAS` list as the single source of truth for named real-taste-mirroring users; don't hardcode persona data elsewhere.
- Any new script that touches real export data must import/reuse `sanitize_real_data.py`'s field-dropping logic rather than re-implementing PII handling from scratch.
- Preserve the `dt=YYYY-MM-DD/events.json` partitioning convention in any new ingestion or transformation code — Phase 2 grading depends on this structure being real and functional, not just described in the proposal.
- When adding catalog tracks, only add real track/artist/genre names (public metadata) — never fabricate fake tracks; the deterministic pseudo audio-features are generated automatically from whatever `track_id` you assign.
- Favor stdlib-only Python for generator/sanitization scripts, since they may need to run in constrained free-tier environments.
- Read and write lakehouse tables only through Delta (`spark.read.table`, `format("delta")`). Never read a Delta folder as plain Parquet.
- Bronze is append-only. No `overwrite`, `UPDATE` or `DELETE` on Bronze except the right-to-be-forgotten procedure.
- Use the explicit StructTypes in `00_config`; never infer schemas in pipeline code.
- Surrogate keys, SCD2, business rules and aggregations belong in Gold only. Silver stays row-level.
- Load Gold dimensions before facts.
- Every new table gets a table comment, column comments and a row in the table catalog above.
- Power BI connects to the `gold` schema only.

---

## Status / Next Steps

- [x] Domain and data-source strategy finalized
- [x] Synthetic generator (v2, time-partitioned) built and tested
- [x] Sanitization/repartitioning script built for real exports
- [x] TA feedback incorporated (partitioned incremental strategy + explicit PII docs)
- [x] Live API puller (`fetch_recently_played.py`) built for OAuth-based real incremental data
- [x] 3 of ~6-7 real personas analyzed and derived into `PERSONAS` (indie-alt/hip-hop/pop, Bollywood/Punjabi/Sufi, J-pop/J-rock clusters)
- [x] Catalog replaced with real-data extraction (`real_catalog_extract.json`, 200 tracks, 46% of analyzed real plays covered)
- [x] Merge-safety bug fixed in `generate_data_v2.py` — script run order no longer risks destroying real data
- [x] Demo sample generated and verified (genre fidelity ~70-72% match to persona's favorite cluster)
- [x] Additional business questions brainstormed for Phase 2/3 scope (see Gold G3 section C)
- [x] Architecture aligned with course material (Delta Lake Ch. 1, Medallion Ch. 3) — this document
- [ ] Remaining 3-4 real users' export files collected and analyzed; `real_catalog_extract.json` and `PERSONAS` refreshed accordingly
- [ ] Up to 5 accounts onboarded to live API pulls (aiming for 5, not yet confirmed) — each needs its own `--login` run; personas without API access use `synthetic_persona_matched` incrementals instead
- [ ] Producers write `_manifest.json` (row count + SHA-256) per touched partition
- [ ] Generator writes dims as dated snapshots (`dims/snapshot_dt=.../`) instead of overwriting
- [ ] Generator flips `is_premium` for a small % of users between snapshots (SCD2 and the propensity question need real changes to track)
- [ ] Verify `users.json` includes the `user_real_0N` pseudonyms — otherwise Silver's orphan check quarantines every real event
- [ ] Verify whether `events.json` is JSON Lines or a single JSON array (sets `multiLine` in the Bronze reader)
- [ ] GitHub repo populated with sanitized sample data + full scripts
- [ ] `00_config` + `01_setup` (schemas, tables, comments, constraints, CDF)
- [ ] Bronze ingest notebook + `ops.ingestion_log` + anomaly checks
- [ ] Silver dims + events notebooks (metadata-driven DQ, quarantine, MERGE)
- [ ] Gold G1: dims (SCD2 `dim_user`, inferred members) + `fact_listening` via CDF
- [ ] Gold G2 ML tables + G3 business marts
- [ ] `90_demos` with the 12 Phase 2 scenarios
- [ ] Serving export + Power BI semantic model (Import mode)
- [ ] BI dashboard built
- [ ] Formal Phase 1 proposal document assembled and submitted
