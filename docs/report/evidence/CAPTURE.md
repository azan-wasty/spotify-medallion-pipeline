# Databricks & Power BI Evidence Capture Checklist

This document details the exact evidentiary screenshots captured by the engineering team to validate each Lakehouse course concept, execution requirement, and dashboard output.

---

## 1. Directory Structure

Place all captured image files in this directory:
`docs/report/evidence/`

---

## 2. Screenshot Manifest

| # | File Name | Environment / View | Required Contents & What It Evidences |
|---|---|---|---|
| 1 | `01_pipeline_dag_success.png` | **Databricks Workflows** | Successful execution DAG of `run_pipeline` showing all green tasks (Bronze Ingest $\to$ Silver Dims/Events $\to$ Gold Dims/Fact/Marts/ML $\to$ Tests). |
| 2 | `02_delta_history_timetravel.png` | **Databricks Notebook** | Query output: `DESCRIBE HISTORY gold.fact_listening LIMIT 5` showing ACID transaction log commits, operations (`MERGE`), version numbers, and timestamps. |
| 3 | `03_quarantine_rejections.png` | **Databricks Notebook** | Query output: `SELECT * FROM silver.events_quarantine LIMIT 10` displaying rejected corrupt/malformed records with explicit `dq_error` tags and preserved raw payload. |
| 4 | `04_scd2_user_history.png` | **Databricks Notebook** | Query output from Demo G3: `SELECT user_id, user_sk, is_premium, valid_from, valid_to, is_current FROM gold.dim_user WHERE user_id = 'user_real_03'` showing both the historical expired version (`is_current = false`) and the active version (`is_current = true`). |
| 5 | `05_schema_drift_evolution.png` | **Databricks Notebook** | Demonstration output from Demo 4 / D8: Showing an incoming raw event with a new additive field (`device_model`) successfully incorporated into Silver via Delta `mergeSchema = true` and logged to `gold.audit_pipeline_runs`. |
| 6 | `06_liquid_clustering_optimize.png` | **Databricks Notebook** | Output of `DESCRIBE DETAIL gold.fact_listening` and `OPTIMIZE gold.fact_listening` showing file consolidation (from hundreds of small files to ~12 compacted files) and cluster column configuration `[date_key, user_sk]`. |
| 7 | `07_dashboard_wrapped.png` | **Power BI Desktop / Tableau** | Consumer "Spotify Wrapped" report canvas: Total listening minutes, top 5 artists, top 5 tracks, top genre share, and personal audio personality radial chart. |
| 8 | `08_dashboard_capacity.png` | **Power BI Desktop / Tableau** | Executive Capacity Planning canvas: Peak hourly listening curve (bimodal weekday commute vs. late weekend), device category breakdown, and peak-to-average listening ratio. |
| 9 | `09_dashboard_propensity.png` | **Power BI Desktop / Tableau** | Commercial Strategy canvas: Subscription propensity tiers (High/Medium/Low), flagged conversion candidates, and churn-risk premium users (`is_premium = true` with low engagement). |

---

## 3. Human Screenshot Protocol

1. **Resolution:** Capture at standard 1080p ($1920 \times 1080$) or window snip at minimum $1200\text{px}$ width.
2. **Format:** Lossless PNG format (`.png`).
3. **Naming:** Exact match to the filenames above (all lowercase with underscores).
4. **Validation:** Ensure sensitive personal credentials or cloud account tokens in Databricks URL bars are cropped or masked.
