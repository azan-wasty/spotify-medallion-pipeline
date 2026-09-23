# Pipeline Testing & Validation Suite

## Test Scope
1. **Landing Gate Tests:** Schema-on-read JSON structure check, non-empty file assertions, manifest SHA256 checksums.
2. **Bronze Tests:** Schema enforcement, ingestion metadata column presence.
3. **Silver Tests:** Deduplication correctness, quarantine routing logic for missing foreign keys or invalid timestamps.
4. **Gold Tests:** Referential integrity checks between `fact_listening` and dimension surrogate keys (`dim_user`, `dim_track`).
