# Architecture

This document describes **what the pipeline is and how its pieces fit together** — the data flow, the layers, the tables, and the orchestration. It is the middle layer between the one-paragraph overview in [`README.md`](../README.md) and the exhaustive design rationale in [`AGENTS.md`](../AGENTS.md). For the *why* behind each decision (surrogate-key strategy, SCD2 scope, CDF vs. recompute, PII deviation, FinOps), see the decision log in [`docs/report/REPORT.md`](report/REPORT.md); for the full column-level schemas, see [`docs/DATA_DICTIONARY.md`](DATA_DICTIONARY.md) (Landing/Bronze/Silver) and [`docs/GOLD_DATA_DICTIONARY.md`](GOLD_DATA_DICTIONARY.md) (Gold).

---

## 1. Overview

The pipeline turns music-listening events from three producers into a curated lakehouse (Medallion architecture) on **Apache Spark + Delta Lake**, culminating in Power BI dashboards and a "Spotify Wrapped"-style summary page.

Three producers write to a **Landing zone** of day-partitioned JSON files. From there, data moves through **Bronze → Silver → Gold** Delta tables, with an **`ops`** schema acting as the control plane. Gold is split into three sublayers (star schema, ML features, business marts) and exported to Power BI (Import mode).

Because Spotify restricted its developer API, most volume is **synthetic but calibrated to real listening behaviour**, layered with **sanitized real exports** and **live API pulls**. Every event carries a `source` tag so the feeds stay distinguishable end-to-end.

---

## 2. Data flow

```
        producers (Python, stdlib only)
        ================================
generate_data_v2.py  (synthetic: full / incremental / backfill)
sanitize_real_data.py (real exports -> pseudonymised + PII stripped)
fetch_recently_played.py (real live API pulls)
        |
        |  write JSON
        v
  LANDING   output/raw/dt=YYYY-MM-DD/events.json, output/dims/{catalog,users}.json
        |   schema-on-read, PII already removed upstream
        |
        v
  BRONZE    bronze.events (append-only, partitioned by _partition_dt)
            bronze.users, bronze.catalog (append-only snapshots)
            + ingestion metadata (_batch_id, _source_file, _ingested_at, ...)
        |   every delivery kept (historization); latest delivery per day wins
        v
  SILVER    silver.events (MERGE, CDF enabled), silver.events_quarantine
            silver.users, silver.tracks (MERGE, SCD1)
        |   cleaned, conformed, deduplicated, row-level DQ -> quarantine
        v
  GOLD      G1  gold.dim_* + gold.fact_listening   (Kimball star schema)
            G2  gold.ml_*                          (ML feature tables)
            G3  gold.* marts + gold.audit_*        (business + observability)
        |   surrogate keys, SCD2, business rules, aggregations
        v
  SERVING   Power BI (Import mode, VertiPaq) — reads gold only
            fallback: 40_serving_export writes gold.* to CSV/Parquet
```

The **`ops` schema** runs alongside the data path, recording every step and version:

```
  ops.pipeline_execution_logs   one row per pipeline step (layer, table, rows, duration, status)
  ops.pipeline_state            one row per CDF-tracked table (last processed version)
```

---

## 3. Producers and the `source` tag

| Producer | Load type | `source` tag | Notes |
|---|---|---|---|
| `generate_data_v2.py full` | Full (initial) | `synthetic`, `synthetic_persona_matched` | 450 days of history + dims snapshot |
| `generate_data_v2.py incremental` | Incremental | `synthetic`, `synthetic_persona_matched` | exactly one new `dt=` partition |
| `generate_data_v2.py backfill` | Re-delivery | `synthetic`, `synthetic_persona_matched` | regenerates a past range in place |
| `sanitize_real_data.py` | Full (per person) | `real` | whole export, pseudonymised, `ip_addr` dropped |
| `fetch_recently_played.py` | Incremental | `real_api` | live recently-played pulls via OAuth |

`source` is the raw tag carried through Bronze. Silver standardises it into `data_source`, renaming `real` → `real_export`, giving the four canonical values `synthetic`, `synthetic_persona_matched`, `real_export`, `real_api`.

---

## 4. Layers

### 4.1 Landing zone (pre-Bronze, files only)

- `output/raw/dt=YYYY-MM-DD/events.json` — one JSON array of events per UTC day. Real and synthetic rows for the same day live in the same file.
- `output/dims/{catalog,users}.json` — full dimension snapshots, rewritten each run.
- `output/validation/user_segments.json` — the planted per-user answer key for testing Gold; **never ingested**.
- Schema-on-read. PII is removed *before* landing, so raw IP addresses never enter the lakehouse.

### 4.2 Bronze (append-only, historical record / single source of truth)

| Table | Grain | Write pattern |
|---|---|---|
| `bronze.events` | one row per event per delivery | append, partitioned by `_partition_dt` |
| `bronze.events_quarantine` | one row per structurally invalid event | append |
| `bronze.users` | one row per user per snapshot | append |
| `bronze.catalog` | one row per track per snapshot | append |

Bronze keeps **every delivery** (including backfills and API top-ups) with ingestion metadata (`_ingested_at`, `_source_type`, `_source_file`, `_partition_dt`, `_batch_id`). It is never `overwrite`/`UPDATE`/`DELETE`d (the only exception is the right-to-be-forgotten procedure). Ingest is checksum-driven: unchanged landing files are logged `SKIPPED_UNCHANGED`.

### 4.3 Silver (cleaned, conformed, current state)

| Table | Grain | Write pattern |
|---|---|---|
| `silver.events` | one row per unique `event_id` | MERGE, CDF enabled, partitioned by `_partition_dt` |
| `silver.events_quarantine` | one row per rejected event | append |
| `silver.users` | one row per user (current) | MERGE (SCD1) |
| `silver.tracks` | one row per track (current) | MERGE (SCD1) |

Silver applies one rule: **for each `_partition_dt`, only the latest delivery counts** (a backfill replaces that day rather than adding to it). Cleaning is row-level and non-intrusive: bad rows go to `silver.events_quarantine` with a `_dq_rule`/`_dq_reason` rather than halting the run. It adds row-level features (`completion_rate`, `hour_of_day`, `day_of_week`, `is_weekend`, `session_id`, `play_sequence_num`, `is_catalog_matched`). No surrogate keys or SCD2 here — those belong in Gold.

### 4.4 Gold (harmonized, business-ready)

**G1 — Star schema (Kimball).** The public interface of the data.

| Table | Grain / note |
|---|---|
| `gold.dim_user` | one row per user *version* — **SCD2** on `is_premium`, SCD1 on `country`/`timezone` |
| `gold.dim_track` | one row per track — SCD1 + **inferred members** for early-arriving tracks |
| `gold.dim_date` | one row per calendar day (generated) |
| `gold.dim_time` | 24 rows, one per hour of day |
| `gold.dim_device` | one row per device type |
| `gold.fact_listening` | one row per play — incremental via **CDF**, Liquid Clustering |

Dimensions are keyed by deterministic surrogate keys (`xxhash64`), carry a `-1` Unknown member so no fact is ever dropped, and are loaded **before** facts. `dim_user` is joined point-in-time (`played_at >= valid_from AND played_at < valid_to`) so each play links to the user version valid when it happened.

**G2 — ML feature tables** (built from `silver.events` + `silver.tracks`, business keys): `gold.ml_user_track_interactions` (user × track, collaborative filtering), `gold.ml_track_features` (track, content-based), `gold.ml_user_sessions` (session, sequential/churn models).

**G3 — Business marts** (one per business question + consumer + observability):

| Table | Answers |
|---|---|
| `gold.peak_usage_patterns` | When do users listen most (capacity planning)? |
| `gold.subscription_propensity_segments` | Which free users behave like premium users? |
| `gold.content_performance_trends` | Which genres/artists are rising or falling? |
| `gold.wrapped_user_year` | Consumer "Wrapped" page (one flat row per user × year) |
| `gold.user_activity_daily` | Daily per-user summary (DAU/MAU, churn) |
| `gold.user_cohort_retention` | Monthly cohort retention matrix |
| `gold.skip_by_genre_artist` | Skip/completion quality by genre and artist |
| `gold.audit_pipeline_runs` | Pipeline health and run durations |
| `gold.audit_quality_summary` | Quarantine volumes and rates by rule |
| `gold.audit_freshness` | High-water marks and catalog coverage per layer |

### 4.5 Serving (Power BI)

Power BI reads the `gold` schema only, in **Import mode** (data copied into VertiPaq). If the workspace exposes no SQL endpoint, `40_serving_export` writes each `gold.*` table to CSV/Parquet for import. The semantic model mirrors the star schema: fact → dims, many-to-one, single direction, with measures (`Total Minutes`, `Plays`, `Skip Rate`, `Active Users`, `Discovery Rate`) defined once.

---

## 5. Orchestration and run order

```
01_setup          create schemas/tables, constraints, CDF, ops.pipeline_state
10_bronze_ingest  landing validation -> append bronze.* -> log -> anomaly checks
20_silver_dims    latest users/catalog snapshot -> silver.users, silver.tracks
21_silver_events  latest delivery -> clean -> quarantine -> MERGE silver.events
30_gold_dims      dim_date, dim_time, dim_device, dim_track, dim_user (SCD2)
31_gold_fact      fact_listening from silver.events CDF
32_gold_ml        gold.ml_*
33_gold_marts     business + audit marts
40_serving_export reconciliation, OPTIMIZE, stats, export
```

`run_pipeline` drives the whole sequence with one `batch_id`, stopping on the first failure. Order rules are enforced: **Bronze → Silver → Gold; Silver dims before Silver events; Gold dims before Gold facts; marts last.** The pipeline is idempotent: rerunning with no new landing files logs `SKIPPED_UNCHANGED` and changes nothing downstream.

---

## 6. Delta Lake mechanics in use

- **ACID transactions** — every write is one atomic commit; a failed job is never visible.
- **Transaction log + checkpoints** — `_delta_log/` is the single source of truth.
- **Append (Bronze) vs. MERGE (Silver/Gold)** — historization in Bronze, upsert downstream.
- **Change Data Feed** — `silver.events` → `gold.fact_listening` incremental loads and delete propagation.
- **Time travel / MVCC** — `VERSION AS OF`, `TIMESTAMP AS OF`, `RESTORE`.
- **Schema enforcement / evolution** — explicit `StructType`s, `mergeSchema` for additive changes, `CHECK` constraints.
- **Soft delete + `VACUUM`** — tombstones until retention, physical removal via `REORG ... APPLY (PURGE)` + `VACUUM` (right-to-be-forgotten).
- **Liquid Clustering** — `CLUSTER BY` on `gold.fact_listening` (`date_key`, `user_sk`), `dim_user`, `dim_track`, plus `OPTIMIZE` at the end of each run.
- **Surrogate keys** — deterministic `xxhash64` rather than identity columns, so keys survive `RESTORE` and concurrent MERGE.

---

## 7. Where to go next

- [`AGENTS.md`](../AGENTS.md) — the full layer contract, demo scenarios, and course-concept coverage.
- [`docs/DATA_DICTIONARY.md`](DATA_DICTIONARY.md) — Landing/Bronze/Silver column-level schema.
- [`docs/GOLD_DATA_DICTIONARY.md`](GOLD_DATA_DICTIONARY.md) — Gold star schema, ML tables, and marts.
- [`docs/PII_NOTES.md`](PII_NOTES.md) — field-by-field PII treatment and right-to-be-forgotten.
- [`docs/report/REPORT.md`](report/REPORT.md) — decision log, Spark/DAG analysis, FinOps, and measured evidence.
