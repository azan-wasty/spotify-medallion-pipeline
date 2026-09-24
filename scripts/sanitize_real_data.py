"""
Script: sanitize_real_data.py
==============================
Purpose:
    Processes Spotify Extended Streaming History export files (JSON), strips PII (raw IP addresses),
    maps tracks against real_catalog_extract.json, formats events into the standardized schema,
    and merges them into the landing zone (`output/raw/dt=YYYY-MM-DD/events.json`).

PII Compliance Rules:
    1. Strips `ip_addr` and `ip_addr_decrypted` before any file is written to output.
    2. Replaces real personal identities with pseudonyms (e.g., `user_real_01`).
    3. Skips non-music items (e.g., podcasts/audiobooks) unless explicitly flagged.

Usage:
    python3 scripts/sanitize_real_data.py --input-dir "./dav data/azan" --user-id user_real_01 --output-dir output/raw
"""

import argparse
import glob
import hashlib
import json
import os
from datetime import datetime


def map_platform_to_device_type(platform_str):
    if not platform_str:
        return "mobile"
    p = str(platform_str).lower()
    if any(k in p for k in ["windows", "mac", "linux", "osx", "desktop"]):
        return "desktop"
    elif any(k in p for k in ["android", "ios", "iphone", "ipad", "mobile"]):
        return "mobile"
    elif "web" in p:
        return "web_player"
    else:
        return "smart_speaker"


def transform_record(item, user_id, idx):
    # Skip podcasts / audiobooks / non-music
    if item.get("episode_name") is not None or item.get("audiobook_title") is not None:
        return None
    track_uri = item.get("spotify_track_uri")
    if not track_uri:
        return None

    ts_str = item.get("ts")
    if not ts_str:
        return None

    # Deterministic event_id based on user, timestamp, track URI, and index
    h = hashlib.sha256(f"{user_id}_{ts_str}_{track_uri}_{idx}".encode("utf-8")).hexdigest()[:12]
    event_id = f"evt_real_{h}"

    device_type = map_platform_to_device_type(item.get("platform"))
    skipped = bool(item.get("skipped", False))
    ms_played = int(item.get("ms_played", 0))

    return {
        "event_id": event_id,
        "user_id": user_id,
        "track_id": track_uri,
        "played_at": ts_str,
        "ms_played": ms_played,
        "skipped": skipped,
        "device_type": device_type,
        "source": "real"
    }


def write_or_merge_partition(day_str, new_events, output_dir):
    partition_dir = os.path.join(output_dir, f"dt={day_str}")
    os.makedirs(partition_dir, exist_ok=True)
    file_path = os.path.join(partition_dir, "events.json")

    existing_events = []
    if os.path.exists(file_path):
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                existing_events = json.load(f)
        except Exception:
            existing_events = []

    # Create key set for deduplication
    existing_keys = {
        (e.get("user_id"), e.get("played_at"), e.get("track_id"))
        for e in existing_events
    }

    added_count = 0
    for evt in new_events:
        key = (evt.get("user_id"), evt.get("played_at"), evt.get("track_id"))
        if key not in existing_keys:
            existing_events.append(evt)
            existing_keys.add(key)
            added_count += 1

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(existing_events, f, indent=2)

    return added_count


def process_directory(input_dir, user_id, output_dir):
    pattern = os.path.join(input_dir, "Streaming_History_Audio_*.json")
    files = glob.glob(pattern)
    if not files:
        # Fallback to checking files directly in directory
        files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.startswith("Streaming_History_Audio_") and f.endswith(".json")]

    if not files:
        print(f"No Streaming_History_Audio_*.json files found in {input_dir}")
        return

    print(f"Found {len(files)} export files for user {user_id} in {input_dir}")

    events_by_day = {}
    total_processed = 0
    skipped_non_music = 0

    for file_path in sorted(files):
        print(f"Processing {os.path.basename(file_path)}...")
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except Exception as e:
                print(f"Error reading {file_path}: {e}")
                continue

        raw_tracks = []
        for idx, item in enumerate(data):
            evt = transform_record(item, user_id, idx)
            if evt is None:
                skipped_non_music += 1
                continue

            raw_tracks.append({
                "track_id": item.get("spotify_track_uri"),
                "track_name": item.get("master_metadata_track_name"),
                "artist": item.get("master_metadata_album_artist_name"),
                "album": item.get("master_metadata_album_album_name"),
                "ms_played": item.get("ms_played", 0)
            })

            day_str = evt["played_at"][:10]
            if day_str not in events_by_day:
                events_by_day[day_str] = []
            events_by_day[day_str].append(evt)
            total_processed += 1

        if raw_tracks:
            try:
                from catalog_utils import register_new_tracks
                new_cat_count = register_new_tracks(raw_tracks)
                if new_cat_count > 0:
                    print(f"  -> Auto-cataloged {new_cat_count} new track(s) from {os.path.basename(file_path)}")
            except Exception as e:
                pass

    total_added = 0
    for day_str, evts in sorted(events_by_day.items()):
        added = write_or_merge_partition(day_str, evts, output_dir)
        total_added += added

    print(f"\nSanitization complete for {user_id}:")
    print(f"  - Total music events processed: {total_processed}")
    print(f"  - Non-music/invalid records skipped: {skipped_non_music}")
    print(f"  - New events merged across {len(events_by_day)} daily partitions: {total_added}")


def parse_args():
    parser = argparse.ArgumentParser(description="Sanitize real Spotify Extended Streaming History data.")
    parser.add_argument("--input-dir", required=True, help="Directory containing raw export files")
    parser.add_argument("--user-id", required=True, help="Pseudonym ID for user (e.g. user_real_01)")
    parser.add_argument("--output-dir", default="output/raw", help="Target landing directory")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_directory(args.input_dir, args.user_id, args.output_dir)

