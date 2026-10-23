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
| `06_scd2_status.txt` | dim_user SCD2 status: implemented, 0 flips yet (second delivery pending — see below) |
| `07_schema_drift.txt` | Bronze auto-rescued additive column `ms_played_source` on all 4 backfill files |
| `08_pricing_aws.txt` | Actual SKU `PREMIUM_ALL_PURPOSE_SERVERLESS_COMPUTE_US_EAST_OHIO` at **$0.75/DBU** from `system.billing.list_prices`. Note: report asked for Azure pricing; workspace is on AWS — AWS rate recorded |
| `explain_propensity_formatted.txt` | `EXPLAIN FORMATTED` of the propensity/discovery query: 10 Photon broadcast joins, 48 shuffles, AQE, LAG window+sort |
| `explain_fact_merge_formatted.txt` | `EXPLAIN FORMATTED` of the 31_gold_fact MERGE: `MergeIntoCommandEdge` (physical rewrite inside Delta command on this runtime) |

## Screenshot capture (for the report)

The same evidence is staged as runnable cells in `notebooks/run_pipeline` (bottom of the notebook):
* cells 16–19 — execution log + ingestion log + run summary displays (DAG success screenshot)
* "EXPLAIN propensity" / "EXPLAIN fact MERGE" cells — plan features + file writes
* "Evidence: measured values (01–08)" cell — re-derives and rewrites all 01–08 text files; run it with outputs visible and screenshot

## Not yet run (left open — one-liners when you want them)

* **Daily incremental run** — `python3 scripts/generate_data_v2.py incremental --date 2026-10-24` (then upload `output/raw/dt=2026-10-24/` to the landing raw/ and run steps 10→33 per notebook). Note: day-24 rows are future-dated until the wall clock passes them, so Silver will quarantine them (correct DQ behavior) unless run after 2026-10-24 UTC.
* **Idempotent rerun** — rerun the steps unchanged; Bronze checksum-skips (already demonstrated: users + catalog were SKIPPED_UNCHANGED in the full run) and Silver/Gold MERGEs mutate 0 rows.
* **Tests + demos** — `tests/01_test_landing`, `02_test_bronze_silver`, `03_test_gold` (run notebooks directly; `run_all_tests` uses `dbutils.notebook.run` which times out on serverless), then `90_demos`.
* **SCD2 second delivery** — `dims/users_v2.json` (3 flips: user_real_03, user_synth_003 free→premium; user_synth_007 premium→free) is already staged in the landing `dims/` alongside `users_v1.json`. To produce the 04_scd2 screenshot: copy `users_v2.json` over `dims/users.json`, run 10_bronze_ingest → 20_silver_dims → 30_gold_dims, then `DESCRIBE HISTORY gold.dim_user` (3 expired rows + 3 new versions, new `user_sk` per `xxhash64(user_id || valid_from)`).
