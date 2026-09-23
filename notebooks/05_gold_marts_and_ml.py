"""
Databricks Notebook: 05_gold_marts_and_ml
=========================================
Purpose:
    Generates Gold G2 ML Feature Store tables and Gold G3 Business Reporting Data Marts.
    Serves specialized downstream analytical consumers (Power BI dashboards, recommendations, Wrapped summaries).

Target Tables:
    - Gold G2 (ML Feature Store):
        * gold.ml_user_track_interactions (User-track affinity matrix, play counts, skip rates)
        * gold.ml_track_features (Aggregated track audio profile metrics)
        * gold.ml_user_sessions (Session-level listening duration and device context)
    - Gold G3 (Business Marts & One Big Table - OBT):
        * gold.wrapped_user_year (Spotify Wrapped annual summary per user)
        * gold.peak_usage_patterns (Hourly/daily listening patterns by device & country)
        * gold.content_performance_trends (Monthly genre & artist performance aggregates)
        * gold.subscription_propensity_segments (User engagement scoring for premium conversion)

Layer Scope: Gold G1 -> Gold G2 (ML) & Gold G3 (Data Marts)
"""

# Placeholder Execution Flow:
# 1. Aggregate fact_listening and dim tables into ML feature matrices.
# 2. Build One Big Table (OBT) gold.wrapped_user_year for rapid dashboard querying.
# 3. Overwrite/Merge business mart tables.
