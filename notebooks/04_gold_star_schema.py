"""
Databricks Notebook: 04_gold_star_schema
========================================
Purpose:
    Builds the Kimball Star Schema layer (Gold G1) from Silver tables.
    Provides business-optimized dimensions and fact tables supporting analytical queries and BI dashboards.

Target Tables:
    - gold.dim_user (SCD Type 2 history for user subscription state changes)
    - gold.dim_track (SCD Type 1 track dimension with genre & audio profile features)
    - gold.dim_date (Calendar dimension table)
    - gold.dim_time (Hour/time-of-day dimension table)
    - gold.dim_device (Device category dimension table)
    - gold.fact_listening (Fact table populated incrementally using Delta Change Data Feed - CDF)

Key Characteristics:
    - Surrogate key assignment (user_sk, track_sk, device_sk).
    - Change Data Feed (CDF) enabled on silver.events for micro-batch fact insertion.

Layer Scope: Silver -> Gold G1 (Star Schema)
"""

# Placeholder Execution Flow:
# 1. Update gold.dim_user using SCD Type 2 merge logic.
# 2. Update gold.dim_track using SCD Type 1 merge logic.
# 3. Read silver.events CDF changes and join with dimension surrogate keys.
# 4. Upsert (MERGE) into gold.fact_listening.
