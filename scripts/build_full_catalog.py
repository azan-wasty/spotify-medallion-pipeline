"""
Script: build_full_catalog.py
==============================
Purpose:
    Extracts ALL distinct music tracks across all real user exports (azan, izyan, saaif),
    maps/infers genres, builds an exhaustive real catalog, and updates:
      - scripts/real_catalog_extract.json
      - output/dims/catalog.json
"""

import glob
import json
import os
import hashlib

# Path constants
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIRS = [
    os.path.join(ROOT_DIR, "dav data", "azan"),
    os.path.join(ROOT_DIR, "dav data", "izyan"),
    os.path.join(ROOT_DIR, "dav data", "saaif")
]
CATALOG_EXTRACT_PATH = os.path.join(ROOT_DIR, "scripts", "real_catalog_extract.json")
DIM_CATALOG_PATH = os.path.join(ROOT_DIR, "output", "dims", "catalog.json")

# Pre-existing artist -> genre mapping for accurate classification
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


def infer_genre(artist_name, track_name):
    if not artist_name:
        return "pop"
    for known_artist, genre in ARTIST_GENRE_MAP.items():
        if known_artist.lower() in artist_name.lower():
            return genre
    # Default fallbacks based on name hints
    art_lower = artist_name.lower()
    if any(k in art_lower for k in ["khan", "singh", "prit", "desi", "punjab", "pakistan"]):
        return "bollywood"
    return "pop"


def deterministic_feature(track_id, salt, lo, hi):
    h = hashlib.sha256(f"{track_id}-{salt}".encode()).hexdigest()
    frac = int(h[:8], 16) / 0xFFFFFFFF
    return round(lo + frac * (hi - lo), 3)


def build_full_catalog():
    # Load existing extract to preserve handcrafted genres
    existing_extract = {}
    if os.path.exists(CATALOG_EXTRACT_PATH):
        with open(CATALOG_EXTRACT_PATH, "r", encoding="utf-8") as f:
            try:
                for item in json.load(f):
                    existing_extract[item["track_id"]] = item
            except Exception:
                pass

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
                    genre = existing_extract.get(track_uri, {}).get("genre") or infer_genre(artist, track_name)
                    track_stats[track_uri] = {
                        "track_id": track_uri,
                        "track_name": track_name,
                        "artist": artist,
                        "album": album or "Single",
                        "genre": genre,
                        "plays": 0,
                        "max_ms": 0
                    }

                track_stats[track_uri]["plays"] += 1
                if ms_played > track_stats[track_uri]["max_ms"]:
                    track_stats[track_uri]["max_ms"] = ms_played

    # Sort catalog by total plays descending
    full_extract = sorted(track_stats.values(), key=lambda x: x["plays"], reverse=True)

    print(f"\nExtracted {len(full_extract)} distinct tracks across all 3 users.")

    # Save to real_catalog_extract.json
    with open(CATALOG_EXTRACT_PATH, "w", encoding="utf-8") as f:
        json.dump(full_extract, f, indent=2)
    print(f"Updated {CATALOG_EXTRACT_PATH}")

    # Build output/dims/catalog.json
    max_plays = max(t["plays"] for t in full_extract) if full_extract else 1
    dim_catalog = []

    for t in full_extract:
        track_id = t["track_id"]
        popularity = round(30 + (t["plays"] / max_plays) * 70)
        duration_sec = max(60, min(600, round((t["max_ms"] or 200000) / 1000)))

        dim_catalog.append({
            "track_id": track_id,
            "track_name": t["track_name"],
            "artist": t["artist"],
            "genre": t["genre"],
            "release_year": None,
            "duration_sec": duration_sec,
            "popularity": popularity,
            "danceability": deterministic_feature(track_id, "dance", 0.2, 0.95),
            "energy": deterministic_feature(track_id, "energy", 0.15, 0.98),
            "valence": deterministic_feature(track_id, "valence", 0.05, 0.95),
            "tempo_bpm": round(deterministic_feature(track_id, "tempo", 70, 175), 1)
        })

    os.makedirs(os.path.dirname(DIM_CATALOG_PATH), exist_ok=True)
    with open(DIM_CATALOG_PATH, "w", encoding="utf-8") as f:
        json.dump(dim_catalog, f, indent=2)
    print(f"Updated {DIM_CATALOG_PATH}")


if __name__ == "__main__":
    build_full_catalog()
