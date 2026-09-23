"""
Databricks Notebook: 02_bronze_ingestion
========================================
Purpose:
    Appends validated raw landing data into Delta Lake Bronze tables.
    Preserves raw source structures while attaching audit/ingestion metadata columns.

Target Tables:
    - bronze.events (Append-only listening events)
    - bronze.users  (Append-only user snapshot history)
    - bronze.catalog (Append-only track/artist snapshot history)

Ingestion Metadata Added:
    - _ingested_at (timestamp)
    - _source_type (synthetic / synthetic_persona_matched / real_export / real_api)
    - _source_file (path to landing partition file)
    - _batch_id     (unique execution job ID)

Layer Scope: Landing -> Bronze
"""

# Placeholder Execution Flow:
# 1. Read landing JSON files with schema enforcement.
# 2. Append metadata columns (_ingested_at, _source_type, _source_file, _batch_id).
# 3. Write to Delta Lake bronze.events partitioned by _partition_dt.
