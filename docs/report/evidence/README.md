# Evidence Pack — Phase 3 Full Run (2026-10-23)

Everything measured and captured for the fresh full load (backfill dt=2026-10-20..23) lives in this folder.
Machine-readable values: **`docs/report/measured.json`** (repo root of this folder).

## Contents

| File | What it is |
|---|---|
| `00_pipeline_dag.md` | Pipeline DAG (mermaid) + batch→step map: every notebook, layer, batch ID, duration, row counts. Total logged duration 304.5 s across 33 steps |
| `01_full_run_log.txt` | Full `ops.pipeline_execution_logs` dump for the run — proof every layer incl. SILVER_TO_GOLD per Gold table succeeded (screenshot-able via `run_pipeline` cells 16-19) |
| `02_storage_describe_detail.txt` | `DESCRIBE DETAIL` storage for all 27 Bronze/Silver/Gold tables (88.1 MB total) + OPTIMIZE before/after: fact_listening 12→1 files, 22.58→21.68 MB; point query 0.791→0.715 s |
| `03_delta_history_fact_listening.txt` | `DESCRIBE HISTORY` (time travel): v0 CREATE → v1 ADD CONSTRAINT → v2 MERGE 411,511 rows → v3/v4 OPTIMIZE |
| `04_propensity_match.txt` | Propensity mart vs `validation/user_segments.json` answer key: **tier 84.0% (below 85% target — reported, not retuned)**, flag 98%, premium 100%, score corr 0.989; all 8 mismatches are near-threshold flips |
| `05_quarantine.txt` | DQ rejections: Silver 1,847 `INVALID_PLAYED_AT` (future timestamps), Bronze 0 |
| `06_scd2_status.txt` | dim_user SCD2 status: implemented and verified (multi-version history active with 51 rows across versions) |
| `07_schema_drift.txt` | Bronze auto-rescued additive column `ms_played_source` on all 4 backfill files |
| `08_pricing_aws.txt` | Actual SKU `PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO` at **$0.75/DBU** from `system.billing.list_prices`. Note: report asked for Azure pricing; workspace is on AWS — AWS rate recorded |
| `explain_propensity_formatted.txt` | `EXPLAIN FORMATTED` of the propensity/discovery query: 10 Photon broadcast joins, 48 shuffles, AQE, LAG window+sort |
| `explain_fact_merge_formatted.txt` | `EXPLAIN FORMATTED` of the 31_gold_fact MERGE: `MergeIntoCommandEdge` (physical rewrite inside Delta command on this runtime) |

## Screenshot capture (for the report & dashboards)

Evidence and telemetry are staged and captured:
* Databricks pipeline execution and DAG run verified (33 steps, 100% SUCCESS)
* Power BI dashboard canvases exported and verified in `dashboards/pics/` (`Overview.png`, `Capacity-Usage-Patterns.png`, `Catalog-performance.png`, `User-Behaviours.png`, `Pipeline-info.png`)
* Spotify Wrapped interactive storyboard slides captured in `dashboards/pics/wrapped_01.png` through `wrapped_09.png`
* "Evidence: measured values (01–08)" cell re-derives and records all 01–08 text files

## Completed Operational Milestones & Verification Runs

* ✅ **Daily Incremental Run (Completed)** — Processed through `dt=2026-10-25`, advancing total fact volume from 411,511 to 565,334 listening events across the lakehouse.
* ✅ **Idempotent Reruns (Completed)** — Validated zero mutations on unchanged source data; Bronze checksums (`ops.ingestion_log`) correctly skipped unmutated files (`SKIPPED_UNCHANGED`), and Silver/Gold MERGEs mutated 0 rows.
* ✅ **Automated Tests & Demos (Completed)** — Full test assertion suite executed (`tests/01_test_landing`, `02_test_bronze_silver`, `03_test_gold`), with all results cataloged into `gold.audit_test_results` (100% PASS across T1.1–T7.1 and Wrapped tests W1–W8).
* ✅ **SCD2 Multi-Version Delivery (Completed)** — Delivered subscription status changes via `output/dims/users_v2.json`, producing historical closed intervals (`is_current = false`) and active versions (`is_current = true`) with deterministic surrogate keys.
* ✅ **Power BI Production Semantic Model (Completed)** — Built and validated `dashboards/SpotifyLakehouse.pbix` (98 MB) with DAX measures and 5 interactive canvases.
* ✅ **Wrapped Web Storyboard (Completed)** — Produced zero-dependency `Mixtape_Wrapped_storyboard.html` consuming bundled lakehouse exports (`gold_export_2`).
