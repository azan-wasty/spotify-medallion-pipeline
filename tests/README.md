# Pipeline Testing & Validation Suite

This directory contains executable Databricks test notebooks and assertion suites covering the entire Medallion Lakehouse lifecycle.

## Test Notebooks

| Notebook | Focus Area | Description & Assertions |
|---|---|---|
| [`01_test_landing.ipynb`](01_test_landing.ipynb) | Landing Zone Gate | Directory structure validation, non-empty JSON files, schema-on-read compliance, SHA-256 manifest verification, and zero-PII leakage check. |
| [`02_test_bronze_silver.ipynb`](02_test_bronze_silver.ipynb) | Bronze & Silver Layers | Ingestion metadata presence (`_ingested_at`, `_raw_file_name`, `_raw_file_row_number`, `_batch_id`, `_raw_checksum`), structural quarantine isolation, Silver primary key deduplication, data quality rule enforcement, Delta `CHECK` constraints, and MERGE idempotency. |
| [`03_test_gold.ipynb`](03_test_gold.ipynb) | Gold Star Schema & Marts | Full assertion suite: Fact vs Silver row count parity & PK uniqueness (T1), zero orphan FKs across all 5 dimensions + inferred members (T2), SCD Type 2 temporal integrity (T3), metric conservation (T4), CDF synchronization (T5), propensity calibration match rate (T6), and market share 100% normalization (T7). |
| [`run_all_tests.ipynb`](run_all_tests.ipynb) | Master Test Orchestrator | Runs all test notebooks in sequence via `dbutils.notebook.run(...)` and outputs a unified execution scorecard with duration and PASS/FAIL status. |

---

## Detailed Assertion Reference

See [`tests/gold_checks.md`](gold_checks.md) for individual test queries and acceptance thresholds.
