# Security & PII Compliance Architecture

## 1. PII Rules & Data Sanitization
- **Raw IP Address Drop Strategy:** `ip_addr` and `ip_addr_decrypted` are stripped pre-landing via `scripts/sanitize_real_data.py`.
- **Pseudonymization:** All real individual identities are mapped to pseudonyms (`user_real_01`, `user_real_02`). The mapping key is kept off repository.
- **Audiobook/Podcast Filtering:** Non-music records are excluded during transformation to eliminate secondary user identifiers.

## 2. Right-to-be-Forgotten Compliance (Delta Lake)
To handle deletion requests under GDPR/CCPA:
1. Issue Delta `DELETE FROM table WHERE user_id = 'user_real_0N';` (soft delete / tombstone).
2. Execute `REORG TABLE table APPLY (PURGE);` (purge tombstones when deletion vectors are enabled).
3. Run `VACUUM table RETAIN 0 HOURS;` to physically remove obsolete Parquet files from cloud storage.
