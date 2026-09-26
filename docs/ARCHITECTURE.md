# Spotify Medallion Lakehouse Architecture

## Overview
```
[Producers: Synthetic / Real Export / Live API]
                         │
                         ▼
             LANDING (output/raw/dt=*, output/dims/*)
                         │
                         ▼ (10_bronze_ingest: validation gate + append)
             BRONZE (bronze.events, bronze.users, bronze.catalog)
                         │
                         ▼ (20_silver_dims, 21_silver_events)
             SILVER (silver.users, silver.tracks, silver.events ──► silver.events_quarantine)
                         │
                         ▼ (30_gold_dims → 31_gold_fact → 32_gold_ml → 33_gold_marts)
             GOLD   (G1: Star Schema | G2: ML Features | G3: Business Marts/OBT)
                         │
                         ▼ (40_serving_export)
             SERVING (Power BI Semantic Model via VertiPaq)
```
`00_config` is included by every notebook, `01_setup` creates the schemas and tables once, `run_pipeline` runs 10 → 20 → 21 → 30 → 31 → 32 → 33 → 40, and `90_demos` holds the Phase 2 scenarios.

## Layer Contracts
- **Landing:** Raw JSON files, day-partitioned by UTC date, schema-on-read.
- **Bronze:** Append-only Delta tables, source schema + ingestion metadata columns (`_ingested_at`, `_source_type`, `_source_file`, `_partition_dt`, `_batch_id`).
- **Silver:** Cleaned, deduplicated, standardized Delta tables. Non-intrusive quality gate routes bad records to `silver.events_quarantine`.
- **Gold:** Business-ready star schema (G1), ML feature tables (G2), and business marts including the One Big Table (OBT) `gold.wrapped_user_year` (G3).

Full design: `AGENTS.md`.
