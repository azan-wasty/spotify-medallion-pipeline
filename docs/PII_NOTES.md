# PII Handling

How personal data is handled in this project. This is the single write-up; AGENTS.md ("PII Rules") has the rules contributors must follow.

## Where personal data exists

Only in the **real** data. Synthetic users (`user_synth_NNN`) and the synthetic events of persona users carry no personal data by construction.

| Source | Personal data in the raw input | What reaches the landing zone |
|---|---|---|
| Extended Streaming History export (`Streaming_History_Audio_*.json`) | The listening history itself, `ip_addr` for every play, `platform` (OS version and device model), `conn_country`, offline/incognito flags | Only `event_id`, `user_id` (pseudonym), `track_id`, `played_at`, `ms_played`, `skipped`, `device_type` (coarse category), `source` |
| Live API (`/v1/me/player/recently-played`) | Recent plays of the authorised account; OAuth tokens | The same event fields; `ms_played`, `skipped` and `device_type` are `null` because the endpoint doesn't provide them. Tokens stay in local, git-ignored `.spotify_tokens_<user>.json` files |
| Catalog (`real_catalog_extract.json`, `dims/catalog.json`) | None: public track, artist and album names, with play counts summed over all real users | Same; nothing links a track to a person |
| `dims/users.json` | None | `user_id` (pseudonym), `country`, `timezone`, `is_premium` |

## Sanitization

1. **Raw exports never enter the repo or the lakehouse.** They are git-ignored and live only on the owner's machine. `sanitize_real_data.py` reads them locally and writes only the fields listed above.
2. **Dropped, not encrypted.** `ip_addr` (and `ip_addr_decrypted` / `user_agent_decrypted` in older exports), the `platform` device string, `conn_country` and every other export field are dropped before landing. The course's Bronze flow suggests classifying and encrypting PII; this project drops it instead, because none of those fields has an analytical use here. This is a documented deviation.
3. **Device model is reduced to a category.** `platform` becomes `mobile`, `desktop`, `web_player` or `smart_speaker`.
4. **Pseudonyms only.** Each real person is `user_real_01`, `user_real_02`, and so on. The mapping to real names is kept offline by the team and never appears in code, data, docs or commit messages.
5. **Podcasts and audiobooks are skipped.** Rows with `episode_name` or `audiobook_title` are excluded, because episode choices can reveal more about a person than music does.
6. **Synthetic data goes through the same Silver code path** as real data, so the pipeline treats every source identically.

## Right to be forgotten (Delta Lake)

When a real person withdraws consent:

1. Delete their rows from every layer (`bronze.*`, `silver.*`, `silver.events_quarantine`, `gold.*`) and from the landing files: `DELETE FROM <table> WHERE user_id = 'user_real_0N'`.
2. A `DELETE` is a soft delete: the old Parquet files are only tombstoned and remain reachable through time travel.
3. Physically remove them with `REORG TABLE <table> APPLY (PURGE)` (needed when deletion vectors are enabled), then `VACUUM <table>`.
4. `VACUUM` keeps 7 days of history by default. `VACUUM ... RETAIN 0 HOURS` removes everything immediately, but requires `spark.databricks.delta.retentionDurationCheck.enabled = false` and breaks time travel, so it is used only in the demo (scenario 10).
