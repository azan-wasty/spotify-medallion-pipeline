"""
Databricks Notebook: 00_config
===============================
Purpose:
    Centralized configuration notebook for the Spotify Medallion Pipeline.
    Defines storage paths, catalog/schema names, table parameters, and shared utility functions.

Key Responsibilities:
    1. Define storage root variables (LANDING_ROOT, BRONZE_DB, SILVER_DB, GOLD_DB, OPS_DB).
    2. Set up Spark session default configurations for Delta Lake and Change Data Feed (CDF).
    3. Define table metadata, schemas, and retention policies.

Layer Scope: Global Configuration
"""

# Placeholder Configuration Constants
CATALOG_NAME = "spotify_lakehouse"
LANDING_ROOT = "/mnt/landing/spotify"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
OPS_SCHEMA = "ops"

# Retention & Cleanup Policy
VACUUM_RETENTION_HOURS = 168  # 7 days default retention
