# Spotify Medallion Lakehouse Architecture

## Overview
```
[Producers: Synthetic / Real Export / Live API]
                         │
                         ▼
             LANDING (output/raw/dt=*)
                         │
                         ▼ (01_landing_validation & 02_bronze_ingestion)
             BRONZE (bronze.events, bronze.users, bronze.catalog)
                         │
                         ▼ (03_silver_transformation)
             SILVER (silver.events ──► silver.events_quarantine)
                         │
                         ▼ (04_gold_star_schema & 05_gold_marts_and_ml)
             GOLD   (G1: Star Schema | G2: ML Features | G3: Business Marts/OBT)
                         │
                         ▼
             SERVING (Power BI Semantic Model via VertiPaq)
```

## Layer Contracts
- **Landing:** Raw JSON files, day-partitioned, schema-on-read.
- **Bronze:** Append-only Delta tables, source schema + ingestion metadata columns (`_ingested_at`, `_source_type`, `_source_file`, `_batch_id`).
- **Silver:** Cleaned, deduplicated, standardized Delta tables. Non-intrusive quality gate routes bad records to `silver.events_quarantine`.
- **Gold:** Business-ready star schema (G1), ML feature tables (G2), and One Big Table (OBT) `gold.wrapped_user_year` (G3).
