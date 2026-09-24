"""
Module: catalog_utils.py
========================
Purpose:
    Provides auto-cataloging utility functions to record new track titles dynamically
    from incremental live API pulls (fetch_recently_played.py) or export sanitization (sanitize_real_data.py),
    updating both scripts/real_catalog_extract.json and output/dims/catalog.json.
"""

import hashlib
import json
import os

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOG_EXTRACT_PATH = os.path.join(ROOT_DIR, "scripts", "real_catalog_extract.json")
DIM_CATALOG_PATH = os.path.join(ROOT_DIR, "output", "dims", "catalog.json")

ARTIST_GENRE_MAP = {
    # Rock / Alt / Indie
    "Arctic Monkeys": "rock", "The Neighbourhood": "indie-alt", "Chase Atlantic": "indie-alt",
    "Cigarettes After Sex": "indie-alt", "TV Girl": "indie-alt", "Joji": "indie-alt", "d4vd": "indie-alt",
    "Nirvana": "rock", "Queen": "rock", "The Killers": "rock", "Linkin Park": "rock", "Lana Del Rey": "indie-alt",
    
    # Desi / South Asian
    "Atif Aslam": "bollywood", "Nusrat Fateh Ali Khan": "qawwali", "Rahat Fateh Ali Khan": "sufi-pop",
    "Arijit Singh": "bollywood", "AP Dhillon": "punjabi", "Ali Zafar": "pakistani-pop", "Shubh": "punjabi",
    "Pritam": "bollywood", "Vishal-Shekhar": "bollywood", "Aditya Rikhari": "indie-pop-desi",
    "Anuv Jain": "indie-pop-desi", "Abdul Hannan": "pakistani-pop", "Talha Anjum": "hip-hop",
    "Young Stunners": "hip-hop", "Talhah Yunus": "hip-hop", "Asim Azhar": "pakistani-pop",
    "Hasan Raheem": "indie-pop-desi", "Umair": "hip-hop", "Kaifi Khalil": "pakistani-pop",
    "Sidhu Moose Wala": "punjabi", "Karan Aujla": "punjabi", "KRSNA": "hip-hop", "DIVINE": "hip-hop",
    
    # J-Pop / J-Rock / Anime
    "YOASOBI": "jpop", "Ado": "jpop", "BABYMETAL": "jmetal", "Eve": "jrock", "majiko": "jpop",
    "SUPER BEAVER": "jrock", "Galileo Galilei": "jrock", "Yorushika": "jrock", "OFFICIAL HIGE DANDISM": "jpop",
    "Kenshi Yonezu": "jpop", "Radwimps": "jrock", "ONE OK ROCK": "jrock",
    
    # Western Hip-Hop & Pop
    "The Weeknd": "pop", "Dua Lipa": "pop", "Harry Styles": "pop", "Miley Cyrus": "pop",
    "Taylor Swift": "pop", "Billie Eilish": "pop", "Kendrick Lamar": "hip-hop", "Drake": "hip-hop",
    "Travis Scott": "hip-hop", "Juice WRLD": "hip-hop", "21 Savage": "hip-hop", "Kanye West": "hip-hop",
    "Eminem": "hip-hop", "Metro Boomin": "hip-hop", "Post Malone": "hip-hop", "Frank Ocean": "indie-alt",
}


def infer_genre(artist_name, track_name=""):
    if not artist_name:
        return "pop"
    for known_artist, genre in ARTIST_GENRE_MAP.items():
        if known_artist.lower() in artist_name.lower():
            return genre
    art_lower = artist_name.lower()
    if any(k in art_lower for k in ["khan", "singh", "prit", "desi", "punjab", "pakistan"]):
        return "bollywood"
    return "pop"


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
            genre = infer_genre(artist, track_name)

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

    if added_count > 0 or new_track_list:
        sorted_extract = sorted(extract_map.values(), key=lambda x: x.get("plays", 0), reverse=True)
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

    return added_count
