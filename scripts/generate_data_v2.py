"""
Synthetic Spotify-style Listening Data Generator — v2
=======================================================

Changes from v1 (per TA feedback on Phase 1 proposal):
  1. INCREMENTAL INGESTION STRATEGY: raw output is now a time-partitioned
     dataset — one JSON file per calendar day, under raw/dt=YYYY-MM-DD/events.json.
     This supports:
       - "full" mode: bulk-generate many days of history at once (your
         historical baseline / full load).
       - "incremental" mode: generate exactly ONE new day's partition,
         meant to be re-run daily/periodically — this is your ongoing
         incremental ingestion job.
       - "backfill" mode: regenerate a specific past date range, so you
         can demonstrate Spark handling a merge/backfill into an already-
         populated partitioned dataset in Phase 2.
  2. NAMED PERSONAS: instead of 120 anonymous random users, PERSONAS below
     holds a small number of named, hand-specified listener profiles.
     Fill these in with real summaries from your 5 extended-history users
     (genres they actually listen to, rough listening intensity, country)
     to make the synthetic population mirror real, unique taste profiles.
     Everyone else in the population (if you want extra volume) is filled
     in as generic random personas alongside your 5 named ones.
  3. PII: synthetic users carry no real PII by construction. If you map a
     named persona to a real person's actual identity, DO NOT put their
     real name/email in this file — use a pseudonym like "user_real_01"
     and keep the real-name mapping only in your own private notes, never
     committed to the repo. See docs/PII_NOTES.md for the full
     sanitization write-up.
  4. REPEAT vs. DISCOVERY: each user-day is ~70% replays of tracks from the
     user's own recent history (recency-weighted, read from the landing
     partitions, so real rows count too) and ~30% discovery (persona genres,
     adjacent genres, chart tracks). The ratio varies per user and per day.
     Every day gets its own RNG seed, so each incremental day is distinct
     but re-running the same day on the same history reproduces it exactly.
  5. BEHAVIOURAL CLUSTERS + TIME OF DAY: TOTAL_USERS users (personas plus
     fillers) each belong to a cluster (power_free / casual_free /
     power_premium) whose distributions drive volume, skips, repeats,
     devices and genre breadth. Plays follow local-time weekday/weekend
     curves with device-by-hour habits, shifted to UTC by the user's
     timezone. Planted traits and engagement scores go to
     output/validation/user_segments.json for testing the Gold marts.

Usage
-----
    python3 generate_data_v2.py full                      # historical bulk load
    python3 generate_data_v2.py incremental                # today's single-day partition
    python3 generate_data_v2.py incremental --date 2026-09-19
    python3 generate_data_v2.py backfill --start 2026-09-01 --end 2026-09-05
    python3 generate_data_v2.py full --seed 7             # any mode: a different, still reproducible population
"""

import argparse
import functools
import hashlib
import json
import math
import os
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from types import SimpleNamespace

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HISTORY_DAYS = 450              # how many days back "full" mode generates
OUTPUT_DIR = "output/raw"
DIM_DIR = "output/dims"
VALIDATION_DIR = "output/validation"  # planted per-user ground truth for testing Gold; never ingested
RANDOM_SEED = 42                 # default --seed; combined with the date, so every day gets its own stream

# ---------------------------------------------------------------------------
# Repeat vs. discovery model. Every mode (full / incremental / backfill) uses
# it, so a day's events always depend on the HISTORY_WINDOW_DAYS before it.
# ---------------------------------------------------------------------------
HISTORY_WINDOW_DAYS = 60         # how far back the repeat pool looks
RECENCY_HALF_LIFE_DAYS = 14      # a play 14 days ago counts half as much as yesterday's
SKIPPED_PLAY_WEIGHT = 0.2        # a track the user skipped is rarely replayed
REPLAY_MAX_TRACK_SHARE = 0.10    # cap on one track's share of the replay weight; only binds for
                                 # light listeners, whose small pool otherwise locks onto one song
REPEAT_RATIO_DAY_STD = 0.07      # day-to-day wobble around a user's own ratio
REPEAT_RATIO_BOUNDS = (0.40, 0.95)
DISCOVERY_MIX = (("favorite", 0.70), ("adjacent", 0.20), ("chart", 0.10))
CHART_SIZE = 200                 # "chart" discovery = top-N catalog tracks by popularity
DISCOVERY_RETRIES = 5            # draws spent looking for a track not in the user's window
REPEAT_SKIP_FACTOR = 0.75        # replays are skipped less than the user's overall rate,
DISCOVERY_SKIP_FACTOR = 1.6      # discoveries more (0.7 * 0.75 + 0.3 * 1.6 ~= 1)

# ---------------------------------------------------------------------------
# When people listen (feeds gold.peak_usage_patterns). Curves are in the
# user's local time; events are stored in UTC like the real exports, so each
# country's pattern lands at its own UTC hours. Curve shapes live in
# daily_curve() and device_affinity().
# ---------------------------------------------------------------------------
WEEKEND_VOLUME_FACTOR = 1.175    # a weekend day carries ~15-20% more plays than a weekday
DAILY_VOLUME_SIGMA = 0.30        # lognormal day-to-day noise on a user's volume

# standard UTC offset in minutes, DST rule ("EU", "US" or None)
TIMEZONES = {
    "Asia/Karachi": (300, None),
    "Asia/Kolkata": (330, None),
    "Europe/London": (0, "EU"),
    "Europe/Berlin": (60, "EU"),
    "America/Sao_Paulo": (-180, None),  # Brazil dropped DST in 2019
    "America/New_York": (-300, "US"),
    "America/Chicago": (-360, "US"),
    "America/Denver": (-420, "US"),
    "America/Los_Angeles": (-480, "US"),
}
# US users are spread over its zones roughly by population
COUNTRY_TIMEZONES = {
    "PK": [("Asia/Karachi", 1.0)],
    "IN": [("Asia/Kolkata", 1.0)],
    "GB": [("Europe/London", 1.0)],
    "DE": [("Europe/Berlin", 1.0)],
    "BR": [("America/Sao_Paulo", 1.0)],
    "US": [("America/New_York", 0.47), ("America/Chicago", 0.29), ("America/Denver", 0.07), ("America/Los_Angeles", 0.17)],
}
COUNTRIES = ["US", "GB", "PK", "IN", "DE", "BR"]

# ---------------------------------------------------------------------------
# Who listens (feeds gold.subscription_propensity_segments). Every user is in
# one behavioural cluster and draws their traits from its distributions;
# personas pin their cluster and real-export values, fillers sample theirs.
# Same code path for both (build_user).
#   weight           share of the sampled (filler) population
#   premium_prob     chance of being premium (Power Premium allows a few free exceptions)
#   intensity        lognormal (median events per active day, log-sd)
#   active_day_prob  beta (mean, concentration): chance of listening on a given day
#   skip_rate, repeat_ratio  beta (mean, concentration)
#   n_genres         inclusive range for the number of favourite genres
#   mobile_only_prob chance the user only uses the phone app
#   device_alpha     Dirichlet concentration per device (bigger = larger, steadier share)
# ---------------------------------------------------------------------------
TOTAL_USERS = 50
CLUSTERS = {
    "power_free": {
        "weight": 0.15, "premium_prob": 0.0,
        "intensity": (34, 0.30), "active_day_prob": (0.85, 25),
        "skip_rate": (0.18, 40), "repeat_ratio": (0.57, 40),
        "n_genres": (2, 4), "mobile_only_prob": 0.0,
        "device_alpha": {"mobile": 4.0, "desktop": 3.0, "web_player": 2.0, "smart_speaker": 0.25},
    },
    "casual_free": {
        "weight": 0.55, "premium_prob": 0.0,
        "intensity": (5.5, 0.45), "active_day_prob": (0.50, 10),
        "skip_rate": (0.50, 25), "repeat_ratio": (0.86, 40),
        "n_genres": (1, 2), "mobile_only_prob": 0.85,
        "device_alpha": {"mobile": 6.0, "web_player": 1.2},
    },
    "power_premium": {
        "weight": 0.30, "premium_prob": 0.93,  # ~7% free exceptions, e.g. a lapsed premium trial
        "intensity": (30, 0.35), "active_day_prob": (0.88, 25),
        "skip_rate": (0.20, 30), "repeat_ratio": (0.66, 30),
        "n_genres": (4, 6), "mobile_only_prob": 0.0,
        "device_alpha": {"mobile": 4.0, "smart_speaker": 2.5, "desktop": 2.0, "web_player": 1.2},
    },
}
TRAIT_CORRELATION = 0.5          # how strongly one latent engagement factor moves a user's traits within a cluster
CLUSTER_PURITY = 0.85            # chance each trait comes from the user's own cluster; otherwise from another
                                 # cluster's distribution, so there are in-between users, not three clean blobs
GENRE_COHERENCE = 0.65           # chance each extra favourite genre neighbours one already chosen

# engagement_score = w1*volume_norm + w2*device_diversity + w3*(1 - skip_rate) + w4*discovery_rate
W1_VOLUME = 0.40
W2_DEVICE_DIVERSITY = 0.20
W3_COMPLETION = 0.20
W4_DISCOVERY = 0.20
HIGH_PROPENSITY_THRESHOLD = 0.55  # free users at or above this are flagged "High Conversion Propensity"

# ---------------------------------------------------------------------------
# NAMED PERSONAS — real-export calibration pinned on top of their cluster.
# Keep names as pseudonyms (user_real_01 etc.), not real names. Any trait
# left out is sampled from the cluster like a filler's. skip_rate, intensity
# and genres come from the export analysis; repeat_ratio is a placeholder
# until derived from the exports (share of plays whose track was already
# played in the previous 60 days); device_mix follows the export notes in
# AGENTS.md (PlayStation has no synthetic device type, so it counts as desktop).
# ---------------------------------------------------------------------------
PERSONAS = [
    {"user_id": "user_real_01", "cluster": "power_premium", "favorite_genres": ["rock", "hip-hop", "pop", "indie-alt"],
     "country": "PK", "intensity": 45, "is_premium": True, "skip_rate": 0.379, "repeat_ratio": 0.70,
     "device_mix": {"desktop": 0.50, "mobile": 0.30, "web_player": 0.10, "smart_speaker": 0.10}},
    {"user_id": "user_real_02", "cluster": "power_premium", "favorite_genres": ["bollywood", "punjabi", "sufi-pop", "qawwali"],
     "country": "PK", "intensity": 40, "is_premium": True, "skip_rate": 0.423, "repeat_ratio": 0.70,
     "device_mix": {"mobile": 0.80, "desktop": 0.08, "web_player": 0.05, "smart_speaker": 0.07}},
    {"user_id": "user_real_03", "cluster": "power_free", "favorite_genres": ["jpop", "jrock", "jmetal"],
     "country": "PK", "intensity": 40, "is_premium": False, "skip_rate": 0.179, "repeat_ratio": 0.70,
     "device_mix": {"mobile": 0.50, "desktop": 0.35, "web_player": 0.15}},
]
PERSONA_IDS = {p["user_id"] for p in PERSONAS}
NUM_FILLER = TOTAL_USERS - len(PERSONAS)  # derived, so adding a persona never shrinks the population

# ---------------------------------------------------------------------------
# Catalog seed: (track, artist, genre, release_year) — real names/facts only
# ---------------------------------------------------------------------------
CATALOG_SEED = [
    ("Blinding Lights", "The Weeknd", "pop", 2019),
    ("Levitating", "Dua Lipa", "pop", 2020),
    ("As It Was", "Harry Styles", "pop", 2022),
    ("Flowers", "Miley Cyrus", "pop", 2023),
    ("Anti-Hero", "Taylor Swift", "pop", 2022),
    ("Bad Guy", "Billie Eilish", "pop", 2019),
    ("HUMBLE.", "Kendrick Lamar", "hip-hop", 2017),
    ("God's Plan", "Drake", "hip-hop", 2018),
    ("SICKO MODE", "Travis Scott", "hip-hop", 2018),
    ("Lucid Dreams", "Juice WRLD", "hip-hop", 2018),
    ("Bohemian Rhapsody", "Queen", "rock", 1975),
    ("Smells Like Teen Spirit", "Nirvana", "rock", 1991),
    ("Mr. Brightside", "The Killers", "rock", 2003),
    ("Do I Wanna Know?", "Arctic Monkeys", "rock", 2013),
    ("Numb", "Linkin Park", "rock", 2003),
    ("Levels", "Avicii", "edm", 2011),
    ("Wake Me Up", "Avicii", "edm", 2013),
    ("Titanium", "David Guetta", "edm", 2011),
    ("Animals", "Martin Garrix", "edm", 2013),
    ("Faded", "Alan Walker", "edm", 2015),
    ("So What", "Miles Davis", "jazz", 1959),
    ("Take Five", "Dave Brubeck", "jazz", 1959),
    ("Feeling Good", "Nina Simone", "jazz", 1965),
    ("Autumn Leaves", "Bill Evans", "jazz", 1959),
    ("Symphony No. 5", "Beethoven", "classical", 1808),
    ("Clair de Lune", "Debussy", "classical", 1905),
    ("Canon in D", "Pachelbel", "classical", 1680),
    ("Moonlight Sonata", "Beethoven", "classical", 1801),
    ("Despacito", "Luis Fonsi", "latin", 2017),
    ("Gasolina", "Daddy Yankee", "latin", 2004),
    ("Bailando", "Enrique Iglesias", "latin", 2014),
    ("Danza Kuduro", "Don Omar", "latin", 2010),
    ("Uptown Funk", "Mark Ronson", "funk", 2014),
    ("September", "Earth, Wind & Fire", "funk", 1978),
    ("Superstition", "Stevie Wonder", "funk", 1972),
    ("Take On Me", "a-ha", "synthpop", 1985),
    ("Blue Monday", "New Order", "synthpop", 1983),
    ("Enjoy the Silence", "Depeche Mode", "synthpop", 1990),
    ("Alone Again", "Alan Walker", "electronic", 2019),
    ("Lean On", "Major Lazer", "electronic", 2015),
    ("This Is What You Came For", "Calvin Harris", "electronic", 2016),
    # --- Indie/alt-rock/bedroom-pop cluster (derived from real user_real_01 data) ---
    ("Slow Dancing", "Joji", "indie-alt", 2020),
    ("Sway", "The Neighbourhood", "indie-alt", 2018),
    ("Slip Away", "Chase Atlantic", "indie-alt", 2019),
    ("Apocalypse", "Cigarettes After Sex", "indie-alt", 2017),
    ("Louie", "TV Girl", "indie-alt", 2014),
    ("Here With Me", "d4vd", "indie-alt", 2022),
    # --- Bollywood/Sufi/Punjabi cluster (derived from real user_real_02 data) ---
    ("Tera Hone Laga Hoon", "Atif Aslam", "bollywood", 2010),
    ("Allah Hoo", "Nusrat Fateh Ali Khan", "qawwali", 1996),
    ("Zaroori Tha", "Rahat Fateh Ali Khan", "sufi-pop", 2013),
    ("Tum Hi Ho", "Arijit Singh", "bollywood", 2013),
    ("Brown Munde", "AP Dhillon", "punjabi", 2020),
    ("Jhoom", "Ali Zafar", "pakistani-pop", 2011),
    ("Elevated", "Shubh", "punjabi", 2022),
    ("Tum Se Hi", "Pritam", "bollywood", 2007),
    ("Desi Girl", "Vishal-Shekhar", "bollywood", 2008),
    ("Left Right", "Aditya Rikhari", "indie-pop-desi", 2022),
    # --- J-pop/J-rock/anime cluster (derived from real user_real_03 data) ---
    ("Idol", "YOASOBI", "jpop", 2023),
    ("Usseewa", "Ado", "jpop", 2020),
    ("Gimme Chocolate!!", "BABYMETAL", "jmetal", 2011),
    ("Kaikai Kitan", "Eve", "jrock", 2020),
    ("Piero", "majiko", "jpop", 2019),
    ("Akashi", "SUPER BEAVER", "jrock", 2019),
    ("Aoi Shiori", "Galileo Galilei", "jrock", 2011),
    ("Dakara Boku wa Ongaku wo Yameta", "Yorushika", "jrock", 2019),
    ("Pretender", "OFFICIAL HIGE DANDISM", "jpop", 2019),
    ("What Makes You Beautiful", "One Direction", "pop", 2011),
]

GENRES = sorted(set(g for _, _, g, _ in CATALOG_SEED))
DEVICE_TYPES = ["mobile", "desktop", "web_player", "smart_speaker"]

# Genres a listener of the key genre plausibly drifts into ("adjacent" discovery).
GENRE_NEIGHBORS = {
    "pop": ["indie-alt", "hip-hop", "rnb", "synthpop", "edm"],
    "rock": ["indie-alt"],
    "indie-alt": ["rock", "pop", "synthpop", "indie-pop-desi"],
    "hip-hop": ["pop", "rnb", "punjabi"],
    "rnb": ["pop", "hip-hop"],
    "kpop": ["pop", "jpop"],
    "soundtrack": ["classical", "electronic"],
    "synthpop": ["electronic", "pop", "indie-alt"],
    "electronic": ["edm", "synthpop"],
    "edm": ["electronic", "pop"],
    "funk": ["pop", "jazz"],
    "jazz": ["funk", "classical"],
    "classical": ["jazz"],
    "latin": ["pop"],
    "bollywood": ["punjabi", "sufi-pop", "indie-pop-desi", "pakistani-pop"],
    "punjabi": ["bollywood", "hip-hop", "pakistani-pop"],
    "sufi-pop": ["qawwali", "bollywood", "pakistani-pop"],
    "qawwali": ["sufi-pop"],
    "pakistani-pop": ["indie-pop-desi", "bollywood", "sufi-pop"],
    "indie-pop-desi": ["pakistani-pop", "indie-alt", "bollywood"],
    "jpop": ["jrock", "kpop", "soundtrack"],
    "jrock": ["jpop", "jmetal", "rock"],
    "jmetal": ["jrock", "rock"],
}

# ---------------------------------------------------------------------------
# REAL CATALOG OVERRIDE — extracted from actual Extended Streaming History
# exports across the real personas (top 200 tracks by real play count, from
# artists confidently genre-classified; ~46% of total real listening volume
# came from these artists). If real_catalog_extract.json is present, it
# REPLACES the hand-picked CATALOG_SEED above entirely, since real listening
# data gives far better popularity/genre-mix fidelity than a curated list.
# Regenerate this file any time more real personas are analyzed (see
# AGENTS.md "Data Sources" section for the extraction process).
# ---------------------------------------------------------------------------
REAL_CATALOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "real_catalog_extract.json")
if not os.path.exists(REAL_CATALOG_PATH):
    REAL_CATALOG_PATH = "real_catalog_extract.json"


def load_real_catalog_override():
    if not os.path.exists(REAL_CATALOG_PATH):
        return None
    with open(REAL_CATALOG_PATH, encoding="utf-8") as f:
        raw = json.load(f)
    catalog = []
    for t in raw:
        track_id = t["track_id"]
        # real popularity proxy: rank-based scale from real play counts (100 = most-played)
        catalog.append({
            "track_id": track_id,
            "track_name": t["track_name"],
            "artist": t["artist"],
            "genre": t["genre"],
            "release_year": None,  # not present in Spotify exports; left honestly null
            "duration_sec": max(60, min(600, round((t.get("max_ms") or 200000) / 1000))),
            "popularity": t["plays"],  # raw real play count; rescaled to 0-100 below
            "danceability": deterministic_feature(track_id, "dance", 0.2, 0.95),
            "energy": deterministic_feature(track_id, "energy", 0.15, 0.98),
            "valence": deterministic_feature(track_id, "valence", 0.05, 0.95),
            "tempo_bpm": round(deterministic_feature(track_id, "tempo", 70, 175), 1),
        })
    # rescale popularity (raw play counts) to a 0-100 range like a normal catalog field
    max_plays = max(c["popularity"] for c in catalog)
    for c in catalog:
        c["popularity"] = round(30 + (c["popularity"] / max_plays) * 70)  # floor of 30, up to 100
    return catalog


def deterministic_feature(track_id, salt, lo, hi):
    h = hashlib.sha256(f"{track_id}-{salt}".encode()).hexdigest()
    frac = int(h[:8], 16) / 0xFFFFFFFF
    return round(lo + frac * (hi - lo), 3)


def build_catalog(seed):
    real_catalog = load_real_catalog_override()
    if real_catalog:
        return real_catalog
    # fallback: hand-picked catalog (used only if real_catalog_extract.json is absent)
    rng = random.Random(seed)
    catalog = []
    for i, (track, artist, genre, year) in enumerate(CATALOG_SEED):
        track_id = f"trk_{i:04d}"
        catalog.append({
            "track_id": track_id, "track_name": track, "artist": artist,
            "genre": genre, "release_year": year,
            "duration_sec": rng.randint(150, 280),
            "popularity": rng.randint(30, 100),
            "danceability": deterministic_feature(track_id, "dance", 0.2, 0.95),
            "energy": deterministic_feature(track_id, "energy", 0.15, 0.98),
            "valence": deterministic_feature(track_id, "valence", 0.05, 0.95),
            "tempo_bpm": round(deterministic_feature(track_id, "tempo", 70, 175), 1),
        })
    return catalog


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ---------------------------------------------------------------------------
# Time of day, device habits, timezones
# ---------------------------------------------------------------------------
def _circular_distance(a, b):
    """Hours between two clock times; 23:00 and 01:00 are 2 hours apart, since habits wrap midnight."""
    d = abs(a - b) % 24
    return min(d, 24 - d)


def _bump(hour, center, width):
    """Smooth peak around `center` o'clock: listening builds up and tails off
    around a routine (a commute, dinner) instead of switching on and off."""
    return math.exp(-0.5 * (_circular_distance(hour, center) / width) ** 2)


def _window(hour, start, end, edge):
    """~1 between `start` and `end` o'clock (may wrap past midnight) with soft
    edges `edge` hours wide: a block of time like the working day or a night out."""
    length = (end - start) % 24
    center = start + length / 2
    return 1 / (1 + math.exp(-(length / 2 - _circular_distance(hour, center)) / edge))


def daily_curve(hour, day_type):
    """Relative listening in a local hour. Weekdays are bimodal around the
    commutes (~8 AM, ~5-7 PM) with a lower at-work plateau between; weekends
    start later (~10-11 AM) and stay high through the evening into 1 AM."""
    if day_type == "weekday":
        return (0.10 + 1.00 * _bump(hour, 8.0, 1.1) + 0.45 * _window(hour, 9.5, 16.5, 0.8)
                + 1.00 * _bump(hour, 18.0, 1.4) + 0.40 * _bump(hour, 22.5, 1.6))
    return 0.08 + 0.85 * _window(hour, 10.5, 1.0, 1.0) + 0.30 * _bump(hour, 17.0, 4.0)


def device_affinity(device, hour, day_type):
    """How natural a device is at a local hour. Desktop and web player are work
    tools (weekday 9-5, sharp drop after 6 PM, little at weekends); smart
    speakers play at home around breakfast and dinner, with a higher weekend
    baseline; phones own late nights (10 PM-2 AM) and weekend daytime."""
    weekday = day_type == "weekday"
    if device == "desktop":
        return 0.04 + (1.00 * _window(hour, 9.0, 17.5, 0.45) if weekday else 0.10 * _window(hour, 11.0, 22.0, 1.5))
    if device == "web_player":
        return 0.05 + (0.85 * _window(hour, 9.0, 18.0, 0.6) if weekday else 0.15 * _window(hour, 11.0, 23.0, 1.5))
    if device == "smart_speaker":
        if weekday:
            return 0.10 + _bump(hour, 7.5, 1.1) + _bump(hour, 19.5, 1.3)
        return 0.35 + 0.9 * _bump(hour, 9.5, 1.5) + 0.9 * _bump(hour, 19.5, 1.6)
    night = 1.2 * _window(hour, 22.0, 2.0, 0.8)
    if weekday:
        return 0.35 + 0.5 * _bump(hour, 8.0, 1.0) + 0.5 * _bump(hour, 18.0, 1.2) + night
    return 0.60 + 0.6 * _window(hour, 10.0, 20.0, 1.2) + night


def build_time_model():
    """Hourly tables (evaluated mid-hour). A weekday curve sums to 1, a weekend
    curve to WEEKEND_VOLUME_FACTOR, so a user's weekend day has ~17.5% more
    plays than their weekday with a different shape, not just a scaled copy."""
    curves, affinity = {}, defaultdict(dict)
    for day_type, total in (("weekday", 1.0), ("weekend", WEEKEND_VOLUME_FACTOR)):
        raw = [daily_curve(h + 0.5, day_type) for h in range(24)]
        curves[day_type] = [total * r / sum(raw) for r in raw]
        for device in DEVICE_TYPES:
            affinity[device][day_type] = [device_affinity(device, h + 0.5, day_type) for h in range(24)]
    return SimpleNamespace(curves=curves, affinity=affinity)


def _nth_sunday(year, month, n):
    """Midnight of the n-th Sunday of a month (US clocks change on Sundays)."""
    first = datetime(year, month, 1)
    return first + timedelta(days=(6 - first.weekday()) % 7 + 7 * (n - 1))


def _last_sunday(year, month):
    """Midnight of the last Sunday of a month (EU clocks change on the last Sunday)."""
    last = datetime(year + (month == 12), month % 12 + 1, 1) - timedelta(days=1)
    return last - timedelta(days=(last.weekday() + 1) % 7)


@functools.lru_cache(maxsize=None)
def dst_period_utc(tz, year):
    """Daylight-saving period in UTC: EU clocks change at 01:00 UTC on the last
    Sundays of March and October; US clocks at 02:00 local on the second Sunday
    of March and the first Sunday of November. Hand-coded so the script stays
    stdlib-only (zoneinfo needs the tzdata package on Windows)."""
    std, rule = TIMEZONES[tz]
    if rule == "EU":
        return _last_sunday(year, 3).replace(hour=1), _last_sunday(year, 10).replace(hour=1)
    if rule == "US":
        return (_nth_sunday(year, 3, 2) + timedelta(hours=2, minutes=-std),
                _nth_sunday(year, 11, 1) + timedelta(hours=2, minutes=-(std + 60)))
    return None


def utc_offset_minutes(tz, utc_dt):
    """Minutes to add to UTC to get local time in `tz` at `utc_dt`."""
    std = TIMEZONES[tz][0]
    period = dst_period_utc(tz, utc_dt.year)
    return std + 60 if period and period[0] <= utc_dt < period[1] else std


# ---------------------------------------------------------------------------
# Users: clusters, traits, engagement
# ---------------------------------------------------------------------------
def _beta(rng, mean, concentration, z, direction):
    """Beta draw for a rate (skip, repeat, active-day) whose mean shifts with the
    user's latent engagement z: within one cluster, more engaged listeners skip
    less, explore more and show up more days. Beta keeps values inside (0, 1)
    without clamping, so there are no pile-ups at the bounds."""
    sd = math.sqrt(mean * (1 - mean) / (concentration + 1))
    m = clamp(mean + direction * TRAIT_CORRELATION * sd * z, 0.02, 0.98)
    return rng.betavariate(m * concentration, (1 - m) * concentration)


def sample_favorite_genres(rng, n, genre_sizes):
    """Tastes cluster: the first favourite is likelier to be a big genre, and each
    extra one is usually a neighbouring genre, occasionally a leap elsewhere."""
    genres = sorted(genre_sizes)
    weight = {g: math.sqrt(genre_sizes[g]) for g in genres}
    chosen = []
    while len(chosen) < min(n, len(genres)):
        neighbours = sorted({nb for g in chosen for nb in GENRE_NEIGHBORS.get(g, []) if nb in weight and nb not in chosen})
        pool = neighbours if neighbours and rng.random() < GENRE_COHERENCE else [g for g in genres if g not in chosen]
        chosen.append(rng.choices(pool, weights=[weight[g] for g in pool])[0])
    return chosen


def sample_device_mix(rng, cluster):
    """Device propensities: casual listeners mostly only have the phone app;
    heavier listeners spread over devices with noisy (Dirichlet) shares."""
    if rng.random() < cluster["mobile_only_prob"]:
        return {"mobile": 1.0}
    draws = {d: rng.gammavariate(a, 1.0) for d, a in cluster["device_alpha"].items()}
    total = sum(draws.values())
    return {d: round(v / total, 3) for d, v in draws.items() if v / total >= 0.01}


def assign_clusters(specs, seed):
    """Cluster per user: pinned clusters (personas) are kept; the rest are split
    in proportion to CLUSTERS weights (largest remainder, so ~47 users can't drift
    far from the target mix) and shuffled, so which user lands where is random."""
    unpinned = [s["user_id"] for s in specs if "cluster" not in s]
    quotas = {name: c["weight"] * len(unpinned) for name, c in CLUSTERS.items()}
    counts = {name: int(q) for name, q in quotas.items()}
    for name in sorted(quotas, key=lambda n: quotas[n] - counts[n], reverse=True)[:len(unpinned) - sum(counts.values())]:
        counts[name] += 1
    labels = [name for name in CLUSTERS for _ in range(counts[name])]
    random.Random(f"{seed}:clusters").shuffle(labels)
    assigned = dict(zip(unpinned, labels))
    return {s["user_id"]: s.get("cluster") or assigned[s["user_id"]] for s in specs}


def build_user(spec, cluster_name, genre_sizes, seed):
    """One user profile. Traits are sampled from the cluster (one latent
    engagement factor correlates them); each trait has a 1 - CLUSTER_PURITY
    chance of coming from another cluster, because real segments are
    tendencies (a casual listener who owns a smart speaker, a power user who
    skips a lot). Then any value pinned in `spec` (a persona's real-export
    calibration) replaces the sample. Fillers pin only their user_id, so
    personas and fillers share this path."""
    rng = random.Random(f"{seed}:user:{spec['user_id']}")
    others = [name for name in CLUSTERS if name != cluster_name]

    def trait_cluster():
        return CLUSTERS[cluster_name if rng.random() < CLUSTER_PURITY else rng.choice(others)]

    z = rng.gauss(0, 1)
    median, log_sd = trait_cluster()["intensity"]
    own = math.sqrt(1 - TRAIT_CORRELATION ** 2) * rng.gauss(0, 1)
    sampled = {
        "user_id": spec["user_id"],
        "cluster": cluster_name,
        "country": rng.choice(COUNTRIES),
        "is_premium": rng.random() < CLUSTERS[cluster_name]["premium_prob"],
        "intensity": round(median * math.exp(log_sd * (TRAIT_CORRELATION * z + own)), 1),
        "active_day_prob": round(_beta(rng, *trait_cluster()["active_day_prob"], z, +1), 3),
        "skip_rate": round(_beta(rng, *trait_cluster()["skip_rate"], z, -1), 3),
        "repeat_ratio": round(_beta(rng, *trait_cluster()["repeat_ratio"], z, -1), 3),
        "favorite_genres": sample_favorite_genres(rng, rng.randint(*trait_cluster()["n_genres"]), genre_sizes),
        "device_mix": sample_device_mix(rng, trait_cluster()),
    }
    user = {**sampled, **spec}
    zones, zone_weights = zip(*COUNTRY_TIMEZONES[user["country"]])
    user["timezone"] = spec.get("timezone") or rng.choices(zones, weights=zone_weights)[0]
    return user


def score_engagement(users):
    """engagement_score = w1*volume_norm + w2*device_diversity + w3*(1 - skip_rate)
    + w4*discovery_rate. Volume is log-scaled before min-max normalising because
    listening volume is heavy-tailed; device diversity is the normalised entropy
    of the device mix (0 = one device, 1 = all four equally)."""
    weekly_factor = (5 + 2 * WEEKEND_VOLUME_FACTOR) / 7
    volume = {u["user_id"]: math.log1p(u["intensity"] * u["active_day_prob"] * weekly_factor) for u in users}
    lo, hi = min(volume.values()), max(volume.values())
    for u in users:
        total = sum(u["device_mix"].values())
        shares = [s / total for s in u["device_mix"].values() if s > 0]
        u["volume_norm"] = round((volume[u["user_id"]] - lo) / ((hi - lo) or 1), 3)
        u["device_diversity"] = round(-sum(s * math.log(s) for s in shares) / math.log(len(DEVICE_TYPES)), 3)
        u["discovery_rate"] = round(1 - u["repeat_ratio"], 3)
        u["engagement_score"] = round(W1_VOLUME * u["volume_norm"] + W2_DEVICE_DIVERSITY * u["device_diversity"]
                                      + W3_COMPLETION * (1 - u["skip_rate"]) + W4_DISCOVERY * u["discovery_rate"], 3)
        high = not u["is_premium"] and u["engagement_score"] >= HIGH_PROPENSITY_THRESHOLD
        u["propensity_flag"] = "High Conversion Propensity" if high else None


def build_users(genre_sizes, seed):
    """The whole population: personas first, then NUM_FILLER fillers, all through
    assign_clusters + build_user, then scored."""
    specs = PERSONAS + [{"user_id": f"user_synth_{i:03d}"} for i in range(NUM_FILLER)]
    clusters = assign_clusters(specs, seed)
    users = [build_user(spec, clusters[spec["user_id"]], genre_sizes, seed) for spec in specs]
    score_engagement(users)
    return users


def print_population_summary(users):
    """Cluster mix split persona vs filler, to eyeball that the population isn't skewed."""
    print(f"\nUser population: {len(users)} ({len(PERSONAS)} personas pinned, {NUM_FILLER} fillers sampled)")
    print(f"  {'cluster':<15}{'personas':>9}{'fillers':>9}{'total':>7}{'share':>7}{'premium':>9}{'high propensity':>17}")
    for name in CLUSTERS:
        members = [u for u in users if u["cluster"] == name]
        personas = sum(u["user_id"] in PERSONA_IDS for u in members)
        print(f"  {name:<15}{personas:>9}{len(members) - personas:>9}{len(members):>7}{len(members) / len(users):>7.0%}"
              f"{sum(u['is_premium'] for u in members):>9}{sum(bool(u['propensity_flag']) for u in members):>17}")


# ---------------------------------------------------------------------------
# Listening history and track choice
# ---------------------------------------------------------------------------
class ListeningHistory:
    """Each user's plays over the HISTORY_WINDOW_DAYS before the day being generated.

    Days before the run are read from the landing partitions, so real rows
    merged in by sanitize_real_data.py / fetch_recently_played.py count as
    history too. Days generated in this run are added as they're written.
    """

    def __init__(self):
        self.days = {}  # date -> {user_id: [(track_id, skipped), ...]}

    def add_day(self, d, events):
        by_user = defaultdict(list)
        for e in events:
            by_user[e["user_id"]].append((e["track_id"], bool(e.get("skipped"))))
        self.days[d] = by_user

    def advance_to(self, today):
        window = {today - timedelta(days=age) for age in range(1, HISTORY_WINDOW_DAYS + 1)}
        for d in list(self.days):
            if d not in window:
                del self.days[d]
        for d in sorted(window - set(self.days)):
            self.add_day(d, read_partition(d))

    def replay_weights(self, user_id, today):
        """Recency-weighted replay weight per track for one user."""
        weights = {}
        for d in sorted(self.days):
            decay = 0.5 ** ((today - d).days / RECENCY_HALF_LIFE_DAYS)
            for track_id, skipped in self.days[d].get(user_id, ()):
                weights[track_id] = weights.get(track_id, 0.0) + decay * (SKIPPED_PLAY_WEIGHT if skipped else 1.0)
        return weights


def build_discovery_pools(user, tracks_by_genre, chart):
    """(weight, tracks) pools for one user's discovery plays; empty pools are dropped."""
    favorites = user["favorite_genres"]
    adjacent = sorted({n for g in favorites for n in GENRE_NEIGHBORS.get(g, [])} - set(favorites))
    by_kind = {
        "favorite": [t for g in favorites for t in tracks_by_genre.get(g, [])],
        "adjacent": [t for g in adjacent for t in tracks_by_genre.get(g, [])],
        "chart": chart,
    }
    return [(weight, by_kind[kind]) for kind, weight in DISCOVERY_MIX if by_kind[kind]]


def pick_discovery(rng, pools, heard, catalog):
    """A track the user hasn't played in the window, from their discovery pools if possible."""
    weights = [w for w, _ in pools]
    for _ in range(DISCOVERY_RETRIES):
        track = rng.choice(rng.choices(pools, weights=weights)[0][1])
        if track["track_id"] not in heard:
            return track
    # Niche genres have only a few hundred catalog tracks, so a heavy listener
    # wears them out within the window. Wander into the long tail instead of
    # counting a replay as a discovery.
    for _ in range(DISCOVERY_RETRIES):
        track = rng.choice(catalog)
        if track["track_id"] not in heard:
            break
    return track


def synthetic_event_id(user_id, day, index):
    # Deterministic: regenerating an unchanged day gives a byte-identical file,
    # so its checksum doesn't change and Bronze doesn't see a new delivery.
    h = hashlib.sha256(f"{user_id}|{day:%Y-%m-%d}|{index}".encode()).hexdigest()[:16]
    return f"evt_syn_{h}"


# ---------------------------------------------------------------------------
# One UTC day of events
# ---------------------------------------------------------------------------
def local_day_factor(user, local_date, ctx):
    """0 on days the user doesn't listen, otherwise a mean-1 lognormal volume
    multiplier (good days and quiet days). Keyed on the user's local date, so a
    local day split across two UTC partitions is active in both or neither."""
    key = (user["user_id"], local_date)
    if key not in ctx.day_factors:
        rng = random.Random(f"{ctx.seed}:{user['user_id']}:{local_date}")
        active = rng.random() < user["active_day_prob"]
        noise = math.exp(DAILY_VOLUME_SIGMA * rng.gauss(0, 1) - DAILY_VOLUME_SIGMA ** 2 / 2)
        ctx.day_factors[key] = noise if active else 0.0
    return ctx.day_factors[key]


def hourly_rates(user, day, ctx):
    """Expected plays in each UTC hour of `day`: the user's local weekday/weekend
    curve, shifted by their timezone (DST-aware) and scaled by that local day's
    volume. Also returns each hour's local (day_type, hour) for device choice."""
    rates, slots = [], []
    for h in range(24):
        utc = day + timedelta(hours=h, minutes=30)
        local = utc + timedelta(minutes=utc_offset_minutes(user["timezone"], utc))
        day_type = "weekend" if local.weekday() >= 5 else "weekday"
        rates.append(user["intensity"] * local_day_factor(user, local.date(), ctx) * ctx.time.curves[day_type][local.hour])
        slots.append((day_type, local.hour))
    return rates, slots


def device_table(user, time_model):
    """P(device | local day_type, hour) for one user: their device propensities
    times each device's hourly affinity, so a desktop+phone user is on the
    desktop at 11 AM on a Tuesday and on the phone at 1 AM or on Saturday."""
    devices = sorted(user["device_mix"])
    return {(day_type, h): (devices, [user["device_mix"][d] * time_model.affinity[d][day_type][h] for d in devices])
            for day_type in ("weekday", "weekend") for h in range(24)}


def generate_day_events(day: datetime, users, ctx):
    """One UTC day of events for every user. Returns (events, number of replays)."""
    rng = random.Random(f"{ctx.seed}:{day:%Y-%m-%d}")
    today = day.date()
    events, n_replays = [], 0
    for user in users:
        user_id = user["user_id"]
        rates, slots = hourly_rates(user, day, ctx)
        expected = sum(rates)
        n_events = int(expected) + (rng.random() < expected - int(expected))
        if not n_events:
            continue

        weights = ctx.history.replay_weights(user_id, today)
        replayable = [t for t in weights if t in ctx.tracks_by_id]
        repeat_ratio = clamp(rng.gauss(user["repeat_ratio"], REPEAT_RATIO_DAY_STD), *REPEAT_RATIO_BOUNDS)
        n_repeat = round(n_events * repeat_ratio) if replayable else 0  # no history yet -> all discovery
        plays = []
        if n_repeat:
            cap = REPLAY_MAX_TRACK_SHARE * sum(weights[t] for t in replayable)
            replays = rng.choices(replayable, weights=[min(weights[t], cap) for t in replayable], k=n_repeat)
            plays = [(ctx.tracks_by_id[t], True) for t in replays]
        plays += [(pick_discovery(rng, ctx.discovery_pools[user_id], weights, ctx.catalog), False)
                  for _ in range(n_events - n_repeat)]
        rng.shuffle(plays)
        n_replays += n_repeat

        hours = rng.choices(range(24), weights=rates, k=n_events)
        times = sorted(day + timedelta(hours=h, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59)) for h in hours)
        for i, (played_at, (track, is_repeat)) in enumerate(zip(times, plays)):
            devices, device_weights = ctx.device_tables[user_id][slots[played_at.hour]]
            skipped = rng.random() < min(0.95, user["skip_rate"] * (REPEAT_SKIP_FACTOR if is_repeat else DISCOVERY_SKIP_FACTOR))
            listened = rng.uniform(0.02, 0.5) if skipped else rng.uniform(0.6, 1.0)
            events.append({
                "event_id": synthetic_event_id(user_id, day, i),
                "user_id": user_id,
                "track_id": track["track_id"],
                "played_at": played_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ms_played": int(track["duration_sec"] * 1000 * listened),
                "skipped": skipped,
                "device_type": rng.choices(devices, weights=device_weights)[0],
                "source": "synthetic_persona_matched" if user_id in PERSONA_IDS else "synthetic",
            })
    return events, n_replays


def partition_path(day):
    return os.path.join(OUTPUT_DIR, f"dt={day:%Y-%m-%d}", "events.json")


def read_partition(day):
    path = partition_path(day)
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception as exc:
        # Fail loudly: silently returning [] would cause write_partition to see
        # no real rows, strip them out, and permanently destroy them.
        raise IOError(
            f"Cannot read landing partition {path} \u2014 refusing to continue "
            f"to avoid data loss. Fix or remove the file first. "
            f"Original error: {exc}"
        ) from exc


def write_partition(day: datetime, events):
    path = partition_path(day)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    # Keep any existing real events (source == "real"/"real_api") untouched;
    # replace only this script's own prior synthetic rows for this day so
    # re-running full/backfill doesn't destroy real data merged in earlier.
    existing_real = [e for e in read_partition(day) if e.get("source") in ("real", "real_api")]
    merged = existing_real + events

    with open(path, "w", encoding="utf-8") as f:
        json.dump(merged, f, indent=2)
    return merged


def write_dims(catalog, users):
    """users.json holds only what a real user table would (id, country, timezone,
    plan). Planted traits (cluster, skip rate, ...) stay out of the landing zone,
    so Gold has to recover them from listening behaviour instead of reading them."""
    os.makedirs(DIM_DIR, exist_ok=True)
    with open(f"{DIM_DIR}/catalog.json", "w") as f:
        json.dump(catalog, f, indent=2)
    with open(f"{DIM_DIR}/users.json", "w") as f:
        json.dump([{k: u[k] for k in ("user_id", "country", "timezone", "is_premium")} for u in users], f, indent=2)


def write_validation(users):
    """Planted ground truth per user (cluster, traits, engagement score, propensity
    flag) for checking what gold.subscription_propensity_segments recovers."""
    os.makedirs(VALIDATION_DIR, exist_ok=True)
    rows = [{"is_persona": u["user_id"] in PERSONA_IDS, **u} for u in users]
    with open(f"{VALIDATION_DIR}/user_segments.json", "w") as f:
        json.dump(rows, f, indent=2)


def generate_days(days, catalog, users, seed):
    """Generate and write each day in order; each written day becomes history for the next."""
    tracks_by_genre = defaultdict(list)
    for t in catalog:
        tracks_by_genre[t["genre"]].append(t)
    chart = sorted(catalog, key=lambda t: t["popularity"], reverse=True)[:CHART_SIZE]
    time_model = build_time_model()
    ctx = SimpleNamespace(
        seed=seed,
        catalog=catalog,
        tracks_by_id={t["track_id"]: t for t in catalog},
        discovery_pools={u["user_id"]: build_discovery_pools(u, tracks_by_genre, chart) for u in users},
        device_tables={u["user_id"]: device_table(u, time_model) for u in users},
        time=time_model,
        history=ListeningHistory(),
        day_factors={},
    )
    total = replays = 0
    for day in days:
        ctx.history.advance_to(day.date())
        events, n_replays = generate_day_events(day, users, ctx)
        ctx.history.add_day(day.date(), write_partition(day, events))
        total += len(events)
        replays += n_replays
    return f"{total} events, {replays / max(total, 1):.0%} replays"


def date_range(start, end):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def run_full(catalog, users, seed):
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = date_range(end_date - timedelta(days=HISTORY_DAYS), end_date - timedelta(days=1))
    summary = generate_days(days, catalog, users, seed)
    print(f"Full load: wrote {len(days)} daily partitions ({summary}) to {OUTPUT_DIR}/")


def run_incremental(catalog, users, seed, date_str=None):
    day = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    summary = generate_days([day], catalog, users, seed)
    print(f"Incremental load: wrote {summary} to {partition_path(day)}")


def run_backfill(catalog, users, seed, start_str, end_str):
    days = date_range(datetime.strptime(start_str, "%Y-%m-%d"), datetime.strptime(end_str, "%Y-%m-%d"))
    summary = generate_days(days, catalog, users, seed)
    print(f"Backfill: regenerated {len(days)} daily partitions ({summary}) from {start_str} to {end_str}")


def main():
    parser = argparse.ArgumentParser(description="Generate time-partitioned synthetic Spotify-style listening data.")
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--seed", type=int, default=RANDOM_SEED,
                        help=f"Seed for the user population and every day's events (default {RANDOM_SEED}).")
    sub = parser.add_subparsers(dest="mode", required=True)

    sub.add_parser("full", parents=[common], help="Generate the full historical load (many day-partitions at once).")

    p_inc = sub.add_parser("incremental", parents=[common], help="Generate a single day's partition (run this daily).")
    p_inc.add_argument("--date", help="YYYY-MM-DD (defaults to today).")

    p_bf = sub.add_parser("backfill", parents=[common], help="Regenerate a range of day-partitions.")
    p_bf.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_bf.add_argument("--end", required=True, help="YYYY-MM-DD")

    args = parser.parse_args()

    catalog = build_catalog(args.seed)
    # "unclassified" is catalog_utils' label for artists not yet in artist_genres.json, not a taste
    genre_sizes = Counter(t["genre"] for t in catalog)
    genre_sizes.pop("unclassified", None)
    users = build_users(genre_sizes, args.seed)
    write_dims(catalog, users)
    write_validation(users)

    if args.mode == "full":
        run_full(catalog, users, args.seed)
    elif args.mode == "incremental":
        run_incremental(catalog, users, args.seed, args.date)
    elif args.mode == "backfill":
        run_backfill(catalog, users, args.seed, args.start, args.end)
    print_population_summary(users)


if __name__ == "__main__":
    main()
