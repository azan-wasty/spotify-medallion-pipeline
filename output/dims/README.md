# Landing Zone - Dimensions (`output/dims/`)

This directory holds dimensional snapshot files for track catalog and user profiles.

## Files
- `catalog.json`: Full track dimension snapshot (track_id, track_name, artist, genre, audio features, popularity).
- `users.json`: Full user dimension snapshot (user_id, country, timezone, is_premium). Behavioural traits used to generate the data are deliberately not here; they live in `output/validation/user_segments.json` as the answer key for testing Gold.
