"""
Module: catalog_utils.py
========================
Purpose:
    Provides auto-cataloging utility functions to record new track titles dynamically
    from incremental live API pulls (fetch_recently_played.py) or export sanitization (sanitize_real_data.py),
    updating both scripts/real_catalog_extract.json and output/dims/catalog.json.

    Genres come from scripts/artist_genres.json (curated, exact artist match), then from
    script hints (Japanese text -> jpop), and otherwise stay 'unclassified' rather than
    being guessed.
"""

import hashlib
import json
import os
import re

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_EXTRACT_PATH = os.path.join(ROOT_DIR, "scripts", "real_catalog_extract.json")
DIM_CATALOG_PATH = os.path.join(ROOT_DIR, "output", "dims", "catalog.json")
ARTIST_GENRES_PATH = os.path.join(ROOT_DIR, "scripts", "artist_genres.json")

UNCLASSIFIED = "unclassified"
JAPANESE_SCRIPT = re.compile(r"[぀-ヿ一-鿿]")  # hiragana, katakana, CJK ideographs


def normalize_artist(name):
    return " ".join(name.split()).casefold()


def load_artist_genres():
    """Reverse the genre -> [artists] file into a normalized artist -> genre lookup."""
    with open(ARTIST_GENRES_PATH, encoding="utf-8") as f:
        grouped = json.load(f)["genres"]
    lookup = {}
    for genre, artists in grouped.items():
        for artist in artists:
            key = normalize_artist(artist)
            if lookup.get(key, genre) != genre:
                raise ValueError(f"{artist!r} is listed under both {lookup[key]!r} and {genre!r} in {ARTIST_GENRES_PATH}")
            lookup[key] = genre
    return lookup


ARTIST_GENRES = load_artist_genres()


def infer_genre(artist_name, track_name="", album_name=""):
    genre = ARTIST_GENRES.get(normalize_artist(artist_name or ""))
    if genre:
        return genre
    # Only the Japanese cluster has a reliable script signal in this catalog;
    # Desi artists are all romanized, so they must be listed in the map.
    if JAPANESE_SCRIPT.search(" ".join(filter(None, (artist_name, track_name, album_name)))):
        return "jpop"
    return UNCLASSIFIED


def deterministic_feature(track_id, salt, lo, hi):
    h = hashlib.sha256(f"{track_id}-{salt}".encode()).hexdigest()
    frac = int(h[:8], 16) / 0xFFFFFFFF
    return round(lo + frac * (hi - lo), 3)


def load_catalog_extract():
    if os.path.exists(CATALOG_EXTRACT_PATH):
        with open(CATALOG_EXTRACT_PATH, "r", encoding="utf-8") as f:
            try:
                return {item["track_id"]: item for item in json.load(f)}
            except Exception:
                pass
    return {}


def write_catalog_files(extract_tracks):
    """Write real_catalog_extract.json (sorted by plays) and the output/dims/catalog.json derived from it."""
    sorted_extract = sorted(extract_tracks, key=lambda x: x.get("plays", 0), reverse=True)
    os.makedirs(os.path.dirname(CATALOG_EXTRACT_PATH), exist_ok=True)
    with open(CATALOG_EXTRACT_PATH, "w", encoding="utf-8") as f:
        json.dump(sorted_extract, f, indent=2)

    max_plays = max((t.get("plays", 1) for t in sorted_extract), default=1)
    dim_catalog = []

    for t in sorted_extract:
        tid = t["track_id"]
        popularity = round(30 + (t.get("plays", 1) / max_plays) * 70)
        duration_sec = max(60, min(600, round((t.get("max_ms") or 200000) / 1000)))

        dim_catalog.append({
            "track_id": tid,
            "track_name": t["track_name"],
            "artist": t["artist"],
            "genre": t["genre"],
            "release_year": None,
            "duration_sec": duration_sec,
            "popularity": popularity,
            "danceability": deterministic_feature(tid, "dance", 0.2, 0.95),
            "energy": deterministic_feature(tid, "energy", 0.15, 0.98),
            "valence": deterministic_feature(tid, "valence", 0.05, 0.95),
            "tempo_bpm": round(deterministic_feature(tid, "tempo", 70, 175), 1)
        })

    os.makedirs(os.path.dirname(DIM_CATALOG_PATH), exist_ok=True)
    with open(DIM_CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(dim_catalog, f, indent=2)


def register_new_tracks(new_track_list):
    """
    Given a list of track dicts:
      [{ "track_id": ..., "track_name": ..., "artist": ..., "album": ..., "ms_played": ... }]
    Registers any new tracks into real_catalog_extract.json and output/dims/catalog.json.
    Returns the number of new tracks added.
    """
    if not new_track_list:
        return 0

    extract_map = load_catalog_extract()
    added_count = 0

    for t in new_track_list:
        track_id = t.get("track_id")
        if not track_id:
            continue

        ms_played = int(t.get("ms_played", 0))

        if track_id in extract_map:
            extract_map[track_id]["plays"] = extract_map[track_id].get("plays", 0) + 1
            if ms_played > extract_map[track_id].get("max_ms", 0):
                extract_map[track_id]["max_ms"] = ms_played
        else:
            artist = t.get("artist") or "Unknown Artist"
            track_name = t.get("track_name") or "Unknown Track"
            album = t.get("album") or "Single"
            genre = infer_genre(artist, track_name, album)

            extract_map[track_id] = {
                "track_id": track_id,
                "track_name": track_name,
                "artist": artist,
                "album": album,
                "genre": genre,
                "plays": 1,
                "max_ms": ms_played
            }
            added_count += 1

    write_catalog_files(extract_map.values())
    return added_count
