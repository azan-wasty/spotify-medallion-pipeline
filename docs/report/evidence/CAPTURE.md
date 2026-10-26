# Databricks & Power BI Evidence Capture Checklist

This document details the evidentiary screenshots captured to validate each Lakehouse course concept, execution requirement, and dashboard output.

**Status: 100% CAPTURED & VERIFIED**

---

## 1. Directory Structure

All captured evidence images are cataloged in the repository:
* **Power BI & Wrapped Canvases**: [`dashboards/pics/`](../../../dashboards/pics/)
* **Lakehouse Architecture & Spark DAGs**: [`docs/report/figures/`](../figures/)
* **Execution Telemetry Dumps**: [`docs/report/evidence/`](./)

---

## 2. Screenshot Manifest & Verification Status

| # | Item Name | Environment / View | File Location | Status | What It Evidences |
|---|---|---|---|---|---|
| 1 | **Pipeline Execution DAG** | Databricks Workflows / Spark | `docs/report/figures/dag.png` | ✅ Captured | End-to-end task run showing all green stages (Landing → Bronze → Silver → Gold → Tests). |
| 2 | **End-to-End Architecture** | Medallion Architecture Map | `docs/report/figures/architecture_diagram.png` | ✅ Captured | Multi-hop data flow, quarantine routing, ops control plane, and Power BI import. |
| 3 | **Dimensional Star Schema** | Kimball ER Diagram | `docs/report/figures/er_diagram.png` | ✅ Captured | `fact_listening` surrounded by conformed dimensions (`dim_user` SCD2, `dim_track`, `dim_device`, `dim_date`, `dim_time`). |
| 4 | **Executive Lakehouse Overview** | Power BI Desktop | `dashboards/pics/Overview.png` | ✅ Captured | Cross-functional KPI summary: total listening hours, active listeners, top genres, device split. |
| 5 | **Capacity & Usage Patterns** | Power BI Desktop | `dashboards/pics/Capacity-Usage-Patterns.png` | ✅ Captured | Hourly demand curves (weekday commute vs weekend evening), peak-to-average load ratio, device mix. |
| 6 | **Catalog Performance Trends** | Power BI Desktop | `dashboards/pics/Catalog-performance.png` | ✅ Captured | Content momentum, rising vs. falling musical genres, track popularity distribution, completion quadrant. |
| 7 | **User Behaviors & Propensity** | Power BI Desktop | `dashboards/pics/User-Behaviours.png` | ✅ Captured | Subscription propensity segmentation (High/Medium/Low), conversion candidates, churn risk alerts. |
| 8 | **Pipeline Observability & Ops** | Power BI Desktop | `dashboards/pics/Pipeline-info.png` | ✅ Captured | Batch runtime history, layer execution duration p50/max, quarantine rejection counts by rule, data freshness watermarks. |
| 9 | **Consumer Spotify Wrapped (Deck)** | HTML5 Storyboard / Power BI | `dashboards/pics/wrapped_01.png` - `wrapped_09.png` | ✅ Captured | Full 9-slide interactive story deck: listening milestones, top artists, tracks, genre radar, traits, streak, and persona badge. |

---

## 3. Telemetry & Log Artifacts

All supporting raw execution logs and physical query plans verifying these visual artifacts are stored in this directory:
- [`01_full_run_log.txt`](01_full_run_log.txt) — 33-step pipeline execution log (100% SUCCESS)
- [`02_storage_describe_detail.txt`](02_storage_describe_detail.txt) — Storage detail across 27 tables & `OPTIMIZE` compaction metrics
- [`03_delta_history_fact_listening.txt`](03_delta_history_fact_listening.txt) — ACID transaction log history and time travel
- [`04_propensity_match.txt`](04_propensity_match.txt) — Calibration reconciliation against answer key
- [`05_quarantine.txt`](05_quarantine.txt) — Quarantine isolation verification
- [`06_scd2_status.txt`](06_scd2_status.txt) — SCD Type 2 dimension timeline verification
- [`07_schema_drift.txt`](07_schema_drift.txt) — Bronze schema drift auto-rescue
- [`08_pricing_aws.txt`](08_pricing_aws.txt) — Databricks FinOps billing data
- [`explain_fact_merge_formatted.txt`](explain_fact_merge_formatted.txt) — Spark physical execution plan for Delta Merge
- [`explain_propensity_formatted.txt`](explain_propensity_formatted.txt) — Spark physical execution plan for Photon joins & windowing

