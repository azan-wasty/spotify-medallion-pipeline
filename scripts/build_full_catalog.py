"""
Script: build_full_catalog.py
==============================
Purpose:
    Extracts ALL distinct music tracks across all real user exports
    (user_real_01, user_real_02, user_real_03, user_real_04), maps/infers genres,
    builds an exhaustive real catalog, and updates:
      - scripts/real_catalog_extract.json
      - output/dims/catalog.json

    Genres always come from catalog_utils.infer_genre (scripts/artist_genres.json), so
    editing that file and re-running is how genres get fixed.

Usage:
    python3 scripts/build_full_catalog.py               # rebuild from the raw exports
    python3 scripts/build_full_catalog.py --reclassify  # relabel the existing catalog, no exports needed
"""

import argparse
import glob
import json
import os
import sys
from collections import Counter

from catalog_utils import ROOT_DIR, UNCLASSIFIED, infer_genre, load_catalog_extract, write_catalog_files

# Each entry is the raw-export folder for one real user.
# Folder names must match what was agreed locally — do NOT put
# real names here; use the pseudonymized folder names instead.
DATA_DIRS = [
    os.path.join(ROOT_DIR, "dav data", "user_real_01"),
    os.path.join(ROOT_DIR, "dav data", "user_real_02"),
    os.path.join(ROOT_DIR, "dav data", "user_real_03"),
    os.path.join(ROOT_DIR, "dav data", "user_real_04"),
]


def build_full_catalog():
    track_stats = {}

    for data_dir in DATA_DIRS:
        if not os.path.exists(data_dir):
            print(f"Directory missing: {data_dir}")
            continue

        files = glob.glob(os.path.join(data_dir, "Streaming_History_Audio_*.json"))
        print(f"Found {len(files)} files in {os.path.basename(data_dir)}")

        for fpath in files:
            with open(fpath, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except Exception as e:
                    print(f"Error reading {fpath}: {e}")
                    continue

            for item in data:
                # Filter non-music
                if item.get("episode_name") is not None or item.get("audiobook_title") is not None:
                    continue
                track_uri = item.get("spotify_track_uri")
                track_name = item.get("master_metadata_track_name")
                artist = item.get("master_metadata_album_artist_name")
                album = item.get("master_metadata_album_album_name")

                if not track_uri or not track_name or not artist:
                    continue

                ms_played = int(item.get("ms_played", 0))

                if track_uri not in track_stats:
                    track_stats[track_uri] = {
                        "track_id": track_uri,
                        "track_name": track_name,
                        "artist": artist,
                        "album": album or "Single",
                        "genre": infer_genre(artist, track_name, album),
                        "plays": 0,
                        "max_ms": 0
                    }

                track_stats[track_uri]["plays"] += 1
                if ms_played > track_stats[track_uri]["max_ms"]:
                    track_stats[track_uri]["max_ms"] = ms_played

    if not track_stats:
        print("No export data found; nothing written. Use --reclassify to relabel the existing catalog.")
        return []

    write_catalog_files(track_stats.values())
    print(f"\nExtracted {len(track_stats)} distinct tracks across all users and wrote both catalog files.")
    return list(track_stats.values())


def reclassify_catalog():
    tracks = list(load_catalog_extract().values())
    changed = 0
    for t in tracks:
        genre = infer_genre(t["artist"], t["track_name"], t.get("album"))
        if genre != t["genre"]:
            t["genre"] = genre
            changed += 1
    write_catalog_files(tracks)
    print(f"Relabelled {changed} of {len(tracks)} tracks and rewrote both catalog files.")
    return tracks


def print_genre_report(tracks, top_unclassified=30):
    total_plays = sum(t["plays"] for t in tracks) or 1
    by_tracks = Counter(t["genre"] for t in tracks)
    by_plays = Counter()
    for t in tracks:
        by_plays[t["genre"]] += t["plays"]
    print("\nGenre            tracks   share of plays")
    for genre, plays in by_plays.most_common():
        print(f"  {genre:<15} {by_tracks[genre]:>6}   {plays / total_plays:6.1%}")

    unclassified = Counter()
    for t in tracks:
        if t["genre"] == UNCLASSIFIED:
            unclassified[t["artist"]] += t["plays"]
    if unclassified:
        print(f"\nTop unclassified artists by plays (add them to scripts/artist_genres.json):")
        for artist, plays in unclassified.most_common(top_unclassified):
            print(f"  {plays:>5}  {artist}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # artist names include Japanese; Windows consoles default to cp1252
    parser = argparse.ArgumentParser(description="Build or relabel the real-track catalog.")
    parser.add_argument("--reclassify", action="store_true",
                        help="Re-derive genres for the existing real_catalog_extract.json instead of reading exports.")
    args = parser.parse_args()
    tracks = reclassify_catalog() if args.reclassify else build_full_catalog()
    if tracks:
        print_genre_report(tracks)