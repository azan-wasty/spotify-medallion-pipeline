"""
Script: enrich_genres_lastfm.py
================================
Purpose:
    Resolves "unclassified" genres in scripts/real_catalog_extract.json using
    Last.fm's crowd-sourced artist tags, instead of relying solely on the
    hand-curated scripts/artist_genres.json map.

    Why Last.fm instead of Spotify:
    Spotify's Artist `genres` field is now documented as Deprecated and
    returns an empty array / null for every artist regardless of how you
    call it (confirmed against Spotify's own API reference and multiple
    2026 developer reports) -- see the project history for details. Spotify
    is simply no longer a usable source for artist genre data.

    Last.fm's `artist.getTopTags` method still returns real, actively-used
    genre/style tags per artist, ordered by popularity, and needs only the
    artist's name -- no Spotify track/artist ID indirection required. This
    also means one lookup per *unique unclassified artist*, not per track,
    which is fewer requests than the old track-ID-based approach needed.

    Flow:
      1. Group unclassified catalog tracks by artist name (case-insensitive).
      2. GET ws.audioscrobbler.com/2.0/?method=artist.gettoptags per unique
         artist -> a list of community tags, most-used first.
      3. Map the top few tags down to the project's 21-bucket taxonomy
         (scripts/artist_genres.json's genre keys) via keyword rules.
      4. Apply the resolved genre to every track by that artist.

Setup:
    Get a free Last.fm API key (instant, no approval wait) at:
      https://www.last.fm/api/account/create
    Add it to your .env as:
      LASTFM_API_KEY=your_key_here

Caching:
    Results are cached in scripts/lastfm_genre_cache.json (artist name ->
    raw tags), saved incrementally, so an interrupted run resumes cleanly
    and re-runs only look up artists not seen before.

Rate limiting:
    Last.fm has no strict published limit but asks callers to be reasonable;
    this script defaults to ~5 requests/second (--delay 0.2) and retries with
    backoff on errors.

Usage:
    python3 scripts/enrich_genres_lastfm.py --limit 50 --dry-run   # test first
    python3 scripts/enrich_genres_lastfm.py                        # full run, writes catalog
    python3 scripts/enrich_genres_lastfm.py --dry-run              # full run, report only
    python3 scripts/enrich_genres_lastfm.py --env-file .env --delay 0.3
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

from catalog_utils import (
    ROOT_DIR,
    UNCLASSIFIED,
    load_catalog_extract,
    normalize_artist,
    write_catalog_files,
)

CACHE_PATH = os.path.join(ROOT_DIR, "scripts", "lastfm_genre_cache.json")
API_BASE = "http://ws.audioscrobbler.com/2.0/"
DEFAULT_DELAY = 0.2  # ~5 req/sec, Last.fm's informally-suggested ceiling
SAVE_EVERY = 25
TOP_N_TAGS = 5  # only consider each artist's most-used tags (already sorted by Last.fm)

# Keyword rules mapping Last.fm's raw (lowercase) community tags down to this
# project's 21-bucket taxonomy. Order matters: more specific patterns are
# checked first so e.g. "sufi rock" resolves to sufi-pop before generic "rock".
# Last.fm tags are free-text and less standardized than Spotify's used to be,
# so this list is a little broader than the old Spotify-tag version.
GENRE_RULES = [
    ("qawwali",        ["qawwali"]),
    ("sufi-pop",       ["sufi"]),
    ("pakistani-pop",  ["pakistani"]),
    ("punjabi",        ["punjabi"]),
    ("bollywood",      ["bollywood", "filmi", "hindi film", "indian film"]),
    ("indie-pop-desi", ["desi", "indian indie", "indian pop", "hindi pop",
                         "hindi", "urdu", "indian singer-songwriter"]),
    ("jmetal",         ["j-metal", "japanese metal"]),
    ("jrock",          ["j-rock", "japanese rock", "visual kei"]),
    ("jpop",           ["j-pop", "japanese pop", "anime", "vocaloid", "japanese"]),
    ("kpop",           ["k-pop", "korean pop", "korean"]),
    ("synthpop",       ["synthpop", "synth-pop", "new wave", "dream pop"]),
    ("edm",            ["edm", "electro house", "big room", "future bass"]),
    ("electronic",     ["electronic", "house", "techno", "downtempo", "idm"]),
    ("hip-hop",        ["hip hop", "hip-hop", "rap", "trap", "drill"]),
    ("rnb",            ["r&b", "rnb", "soul", "neo soul"]),
    ("jazz",           ["jazz"]),
    ("latin",          ["latin", "reggaeton", "salsa", "bachata"]),
    ("soundtrack",     ["soundtrack", "score", "film score"]),
    ("indie-alt",      ["indie", "alt z", "alternative"]),
    ("rock",           ["rock", "metal", "punk"]),
    ("pop",            ["pop"]),
]


def load_env(env_path):
    config = {}
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                config[k.strip()] = v.strip().strip("'\"")
    return config


def get_api_key(env_path):
    env_vars = load_env(env_path)
    key = os.getenv("LASTFM_API_KEY") or env_vars.get("LASTFM_API_KEY")
    if not key:
        print(f"Error: missing LASTFM_API_KEY in {env_path} / environment.")
        print("Get a free key at https://www.last.fm/api/account/create")
        sys.exit(1)
    return key


def call_api(params, retries=5):
    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"
    for attempt in range(retries):
        req = urllib.request.Request(url, headers={"User-Agent": "spotify-medallion-pipeline/1.0"})
        try:
            with urllib.request.urlopen(req) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 2 * (attempt + 1)
                print(f"  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            print(f"  API error {e.code}: {e.read().decode('utf-8', 'ignore')}")
            return None
        except urllib.error.URLError as e:
            print(f"  Network error: {e}. Retrying...")
            time.sleep(2)
            continue
    return None


def load_cache():
    if os.path.exists(CACHE_PATH):
        with open(CACHE_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"artist_tags_raw": {}}


def save_cache(cache):
    with open(CACHE_PATH, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2)


def bucket_from_raw_tags(raw_tags):
    """Map a list of Last.fm's raw tag strings to one of our 21 buckets."""
    combined = " | ".join(t.lower() for t in raw_tags[:TOP_N_TAGS])
    for bucket, keywords in GENRE_RULES:
        if any(kw in combined for kw in keywords):
            return bucket
    return None


def fetch_artist_tags(api_key, artist_names, cache, delay):
    """artist_names: list of *original-cased* artist name strings."""
    missing = [a for a in artist_names if normalize_artist(a) not in cache["artist_tags_raw"]]
    total = len(missing)
    print(f"Fetching Last.fm tags for {total} artists not in cache "
          f"({len(artist_names) - total} already cached)...")
    for i, artist in enumerate(missing, 1):
        params = {
            "method": "artist.gettoptags",
            "artist": artist,
            "autocorrect": "1",
            "api_key": api_key,
            "format": "json",
        }
        data = call_api(params)
        tags = []
        if data and "toptags" in data:
            tags = [t["name"] for t in data["toptags"].get("tag", [])]
        elif data and "error" in data:
            pass  # e.g. artist not found on Last.fm -- leave tags empty
        cache["artist_tags_raw"][normalize_artist(artist)] = tags
        if i % SAVE_EVERY == 0 or i == total:
            save_cache(cache)
            print(f"  ...{i}/{total} artists looked up")
        time.sleep(delay)


def enrich(dry_run=False, env_path=".env", delay=DEFAULT_DELAY, limit=None):
    api_key = get_api_key(env_path)

    extract_map = load_catalog_extract()
    tracks = list(extract_map.values())
    unclassified_all = [t for t in tracks if t["genre"] == UNCLASSIFIED]

    # Dedup by artist name so each real-world artist is only queried once,
    # regardless of how many unclassified tracks they have.
    unique_artists = []
    seen = set()
    for t in unclassified_all:
        key = normalize_artist(t["artist"])
        if key not in seen:
            seen.add(key)
            unique_artists.append(t["artist"])
    artists_to_query = unique_artists[:limit] if limit else unique_artists

    print(f"Catalog: {len(tracks)} tracks total, {len(unclassified_all)} unclassified tracks "
          f"across {len(unique_artists)} distinct artists"
          + (f" (querying first {len(artists_to_query)} artists due to --limit)" if limit else "")
          + ".\n")

    if not artists_to_query:
        print("Nothing to enrich.")
        return

    cache = load_cache()
    fetch_artist_tags(api_key, artists_to_query, cache, delay)

    queried_keys = {normalize_artist(a) for a in artists_to_query}
    resolved_tracks = 0
    resolved_artists = 0
    still_unresolved_artists = {}

    for t in unclassified_all:
        key = normalize_artist(t["artist"])
        if key not in queried_keys:
            continue  # skipped this round due to --limit
        raw_tags = cache["artist_tags_raw"].get(key, [])
        bucket = bucket_from_raw_tags(raw_tags) if raw_tags else None
        if bucket:
            t["genre"] = bucket
            resolved_tracks += 1
        else:
            still_unresolved_artists[t["artist"]] = still_unresolved_artists.get(t["artist"], 0) + t.get("plays", 0)

    resolved_artists = len({normalize_artist(t["artist"]) for t in unclassified_all
                             if normalize_artist(t["artist"]) in queried_keys and t["genre"] != UNCLASSIFIED})

    print(f"\nResolved {resolved_tracks} previously-unclassified tracks "
          f"({resolved_artists} distinct artists) via Last.fm tags.")
    print(f"Still unclassified: {len(still_unresolved_artists)} distinct artists "
          f"-- Last.fm has no usable tags for them either.")

    if still_unresolved_artists:
        print("\nTop still-unresolved artists by plays (candidates for manual entry in artist_genres.json):")
        for artist, plays in sorted(still_unresolved_artists.items(), key=lambda x: -x[1])[:20]:
            print(f"  {plays:>5}  {artist}")

    if dry_run:
        print("\n--dry-run: no files written.")
        return

    write_catalog_files(tracks)
    print("\nWrote scripts/real_catalog_extract.json and output/dims/catalog.json.")


def parse_args():
    parser = argparse.ArgumentParser(description="Enrich unclassified catalog genres via Last.fm artist tags.")
    parser.add_argument("--dry-run", action="store_true", help="Report resolution counts without writing files.")
    parser.add_argument("--env-file", default=".env", help="Path to .env with LASTFM_API_KEY.")
    parser.add_argument("--delay", type=float, default=DEFAULT_DELAY,
                         help=f"Seconds to sleep between API requests (default {DEFAULT_DELAY}).")
    parser.add_argument("--limit", type=int, default=None,
                         help="Only query the first N distinct unclassified artists (useful for a quick test run).")
    return parser.parse_args()


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    args = parse_args()
    enrich(dry_run=args.dry_run, env_path=args.env_file, delay=args.delay, limit=args.limit)