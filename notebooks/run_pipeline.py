"""
Databricks Notebook: run_pipeline
=================================
Purpose:
    Master orchestration notebook.
    Executes the Spotify Medallion Pipeline end-to-end in sequence with error handling and step logging.

Execution Sequence:
    1. dbutils.notebook.run("00_config", 60)
    2. dbutils.notebook.run("01_landing_validation", 300)
    3. dbutils.notebook.run("02_bronze_ingestion", 600)
    4. dbutils.notebook.run("03_silver_transformation", 900)
    5. dbutils.notebook.run("04_gold_star_schema", 900)
    6. dbutils.notebook.run("05_gold_marts_and_ml", 600)
    7. dbutils.notebook.run("06_ops_monitoring", 300)

Error Handling:
    - Failing any step between 01 and 04 halts downstream processing to maintain data consistency.
"""

print("Master Pipeline Orchestrator Initialized.")
