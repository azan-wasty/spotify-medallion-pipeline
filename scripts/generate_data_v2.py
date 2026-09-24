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
     committed to the repo. See PII_NOTES.md (generated alongside output)
     for the full sanitization write-up.
  4. REPEAT vs. DISCOVERY: each user-day is ~70% replays of tracks from the
     user's own recent history (recency-weighted, read from the landing
     partitions, so real rows count too) and ~30% discovery (persona genres,
     adjacent genres, chart tracks). The ratio varies per user and per day.
     Every day gets its own RNG seed, so each incremental day is distinct
     but re-running the same day on the same history reproduces it exactly.

Usage
-----
    python3 generate_data_v2.py full                      # historical bulk load
    python3 generate_data_v2.py incremental                # today's single-day partition
    python3 generate_data_v2.py incremental --date 2026-09-19
    python3 generate_data_v2.py backfill --start 2026-09-01 --end 2026-09-05
"""

import argparse
import hashlib
import json
import os
import random
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HISTORY_DAYS = 450              # how many days back "full" mode generates
AVG_EVENTS_PER_USER_PER_DAY = 8  # bumped up since we have fewer, named users
EXTRA_RANDOM_USERS = 20          # generic filler users alongside named personas
INTENSITY_MEAN = 15              # filler-population mean events/active day (bell curve)
INTENSITY_STD = 15               # std dev; real personas (40-45) sit ~1.7-2.0 std devs above this
FILLER_SKIP_RATE_MEAN = 0.30     # filler-population overall skip rate (real personas: 18-42%)
FILLER_SKIP_RATE_STD = 0.08
OUTPUT_DIR = "output/raw"
DIM_DIR = "output/dims"
RANDOM_SEED = 42                 # combined with the date, so every day gets its own stream

# ---------------------------------------------------------------------------
# Repeat vs. discovery model. Every mode (full / incremental / backfill) uses
# it, so a day's events always depend on the HISTORY_WINDOW_DAYS before it.
# ---------------------------------------------------------------------------
HISTORY_WINDOW_DAYS = 60         # how far back the repeat pool looks
RECENCY_HALF_LIFE_DAYS = 14      # a play 14 days ago counts half as much as yesterday's
SKIPPED_PLAY_WEIGHT = 0.2        # a track the user skipped is rarely replayed
REPLAY_MAX_TRACK_SHARE = 0.10    # cap on one track's share of the replay weight; only binds for
                                 # light listeners, whose small pool otherwise locks onto one song
REPEAT_RATIO_MEAN = 0.70         # population share of plays that are replays
REPEAT_RATIO_USER_STD = 0.08     # spread between users (loyalists vs. explorers)
REPEAT_RATIO_DAY_STD = 0.07      # day-to-day wobble around a user's own ratio
REPEAT_RATIO_BOUNDS = (0.40, 0.95)
DISCOVERY_MIX = (("favorite", 0.70), ("adjacent", 0.20), ("chart", 0.10))
CHART_SIZE = 200                 # "chart" discovery = top-N catalog tracks by popularity
DISCOVERY_RETRIES = 5            # draws spent looking for a track not in the user's window
REPEAT_SKIP_FACTOR = 0.75        # replays are skipped less than the user's overall rate,
DISCOVERY_SKIP_FACTOR = 1.6      # discoveries more (0.7 * 0.75 + 0.3 * 1.6 ~= 1)
DEVICE_SWITCH_PROB = 0.10        # chance a play ignores the user's usual device mix

# ---------------------------------------------------------------------------
# NAMED PERSONAS — replace these placeholders with real summaries once you
# have them. Keep names as pseudonyms (user_real_01 etc.), not real names.
# favorite_genres must be drawn from GENRES below (or add new catalog rows).
# skip_rate is the real export's overall skip rate. repeat_ratio is a
# placeholder at the population mean until derived from the exports (share
# of plays whose track was already played in the previous 60 days).
# ---------------------------------------------------------------------------
PERSONAS = [
    {"user_id": "user_real_01", "favorite_genres": ["rock", "hip-hop", "pop", "indie-alt"], "country": "PK", "intensity": 45, "is_premium": True,
     "skip_rate": 0.379, "repeat_ratio": 0.70},
    {"user_id": "user_real_02", "favorite_genres": ["bollywood", "punjabi", "sufi-pop", "qawwali"], "country": "PK", "intensity": 40, "is_premium": True,
     "skip_rate": 0.423, "repeat_ratio": 0.70},
    {"user_id": "user_real_03", "favorite_genres": ["jpop", "jrock", "jmetal"], "country": "PK", "intensity": 40, "is_premium": False,
     "skip_rate": 0.179, "repeat_ratio": 0.70},
]
PERSONA_IDS = {p["user_id"] for p in PERSONAS}

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


def build_catalog():
    real_catalog = load_real_catalog_override()
    if real_catalog:
        return real_catalog
    # fallback: hand-picked catalog (used only if real_catalog_extract.json is absent)
    rng = random.Random(RANDOM_SEED)
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


def sample_filler_intensity(rng):
    # Truncated normal: real listeners can't have negative/zero intensity,
    # so floor at 1 rather than letting the left tail go negative.
    return max(1, round(rng.gauss(INTENSITY_MEAN, INTENSITY_STD)))


def build_users(available_genres):
    rng = random.Random(RANDOM_SEED)  # own stream, so the filler population is the same every run
    users = list(PERSONAS)  # named, real-taste-mirroring personas first
    for i in range(EXTRA_RANDOM_USERS):
        users.append({
            "user_id": f"user_synth_{i:03d}",
            "favorite_genres": rng.sample(available_genres, k=min(3, len(available_genres))),
            "country": rng.choice(["US", "GB", "PK", "IN", "DE", "BR"]),
            "intensity": sample_filler_intensity(rng),
            "is_premium": rng.random() < 0.55,
            "skip_rate": round(clamp(rng.gauss(FILLER_SKIP_RATE_MEAN, FILLER_SKIP_RATE_STD), 0.05, 0.60), 3),
            "repeat_ratio": round(clamp(rng.gauss(REPEAT_RATIO_MEAN, REPEAT_RATIO_USER_STD), *REPEAT_RATIO_BOUNDS), 3),
        })
    return users


def hour_weight(hour):
    if 0 <= hour < 6: return 0.2
    if 6 <= hour < 9: return 1.4
    if 9 <= hour < 17: return 1.0
    if 17 <= hour < 22: return 1.6
    return 0.6


def sample_hour(rng):
    hours = list(range(24))
    return rng.choices(hours, weights=[hour_weight(h) for h in hours], k=1)[0]


class ListeningHistory:
    """Each user's plays over the HISTORY_WINDOW_DAYS before the day being generated.

    Days before the run are read from the landing partitions, so real rows
    merged in by sanitize_real_data.py / fetch_recently_played.py count as
    history too. Days generated in this run are added as they're written.
    """

    def __init__(self):
        self.days = {}  # date -> {user_id: [(track_id, skipped, device_type), ...]}

    def add_day(self, d, events):
        by_user = defaultdict(list)
        for e in events:
            by_user[e["user_id"]].append((e["track_id"], bool(e.get("skipped")), e.get("device_type")))
        self.days[d] = by_user

    def advance_to(self, today):
        window = {today - timedelta(days=age) for age in range(1, HISTORY_WINDOW_DAYS + 1)}
        for d in list(self.days):
            if d not in window:
                del self.days[d]
        for d in sorted(window - set(self.days)):
            self.add_day(d, read_partition(d))

    def user_profile(self, user_id, today):
        """Recency-weighted replay weight per track, plus device counts, for one user."""
        weights, devices = {}, Counter()
        for d in sorted(self.days):
            decay = 0.5 ** ((today - d).days / RECENCY_HALF_LIFE_DAYS)
            for track_id, skipped, device in self.days[d].get(user_id, ()):
                weights[track_id] = weights.get(track_id, 0.0) + decay * (SKIPPED_PLAY_WEIGHT if skipped else 1.0)
                if device:
                    devices[device] += 1
        return weights, devices


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


def generate_day_events(day: datetime, users, catalog, tracks_by_id, discovery_pools, history):
    """One day's events for every user. Returns (events, number of replays)."""
    rng = random.Random(f"{RANDOM_SEED}:{day:%Y-%m-%d}")
    today = day.date()
    events, n_replays = [], 0
    for user in users:
        user_id = user["user_id"]
        n_events = max(0, int(rng.gauss(user["intensity"], 2.0)))
        weights, devices = history.user_profile(user_id, today)
        replayable = [t for t in weights if t in tracks_by_id]
        repeat_ratio = clamp(rng.gauss(user.get("repeat_ratio", REPEAT_RATIO_MEAN), REPEAT_RATIO_DAY_STD), *REPEAT_RATIO_BOUNDS)
        n_repeat = round(n_events * repeat_ratio) if replayable else 0  # no history yet -> all discovery
        plays = []
        if n_repeat:
            cap = REPLAY_MAX_TRACK_SHARE * sum(weights[t] for t in replayable)
            replays = rng.choices(replayable, weights=[min(weights[t], cap) for t in replayable], k=n_repeat)
            plays = [(tracks_by_id[t], True) for t in replays]
        plays += [(pick_discovery(rng, discovery_pools[user_id], weights, catalog), False)
                  for _ in range(n_events - n_repeat)]
        n_replays += n_repeat

        skip_rate = user.get("skip_rate", FILLER_SKIP_RATE_MEAN)
        for i, (track, is_repeat) in enumerate(plays):
            played_at = day.replace(hour=sample_hour(rng), minute=rng.randint(0, 59), second=rng.randint(0, 59))
            skipped = rng.random() < min(0.95, skip_rate * (REPEAT_SKIP_FACTOR if is_repeat else DISCOVERY_SKIP_FACTOR))
            listened = rng.uniform(0.02, 0.5) if skipped else rng.uniform(0.6, 1.0)
            if devices and rng.random() >= DEVICE_SWITCH_PROB:
                device = rng.choices(list(devices), weights=list(devices.values()))[0]
            else:
                device = rng.choice(DEVICE_TYPES)
            events.append({
                "event_id": synthetic_event_id(user_id, day, i),
                "user_id": user_id,
                "track_id": track["track_id"],
                "played_at": played_at.isoformat(),
                "ms_played": int(track["duration_sec"] * 1000 * listened),
                "skipped": skipped,
                "device_type": device,
                "source": "synthetic_persona_matched" if user_id in PERSONA_IDS else "synthetic",
            })
    return events, n_replays


def partition_path(day):
    return os.path.join(OUTPUT_DIR, f"dt={day:%Y-%m-%d}", "events.json")


def read_partition(day):
    path = partition_path(day)
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return json.load(f)


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
    os.makedirs(DIM_DIR, exist_ok=True)
    with open(f"{DIM_DIR}/catalog.json", "w") as f:
        json.dump(catalog, f, indent=2)
    with open(f"{DIM_DIR}/users.json", "w") as f:
        json.dump(users, f, indent=2)


def generate_days(days, catalog, users):
    """Generate and write each day in order; each written day becomes history for the next."""
    tracks_by_id = {t["track_id"]: t for t in catalog}
    tracks_by_genre = defaultdict(list)
    for t in catalog:
        tracks_by_genre[t["genre"]].append(t)
    chart = sorted(catalog, key=lambda t: t["popularity"], reverse=True)[:CHART_SIZE]
    discovery_pools = {u["user_id"]: build_discovery_pools(u, tracks_by_genre, chart) for u in users}

    history = ListeningHistory()
    total = replays = 0
    for day in days:
        history.advance_to(day.date())
        events, n_replays = generate_day_events(day, users, catalog, tracks_by_id, discovery_pools, history)
        history.add_day(day.date(), write_partition(day, events))
        total += len(events)
        replays += n_replays
    return f"{total} events, {replays / max(total, 1):.0%} replays"


def date_range(start, end):
    return [start + timedelta(days=i) for i in range((end - start).days + 1)]


def run_full(catalog, users):
    end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    days = date_range(end_date - timedelta(days=HISTORY_DAYS), end_date - timedelta(days=1))
    summary = generate_days(days, catalog, users)
    print(f"Full load: wrote {len(days)} daily partitions ({summary}) to {OUTPUT_DIR}/")


def run_incremental(catalog, users, date_str=None):
    day = datetime.strptime(date_str, "%Y-%m-%d") if date_str else datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    summary = generate_days([day], catalog, users)
    print(f"Incremental load: wrote {summary} to {partition_path(day)}")


def run_backfill(catalog, users, start_str, end_str):
    days = date_range(datetime.strptime(start_str, "%Y-%m-%d"), datetime.strptime(end_str, "%Y-%m-%d"))
    summary = generate_days(days, catalog, users)
    print(f"Backfill: regenerated {len(days)} daily partitions ({summary}) from {start_str} to {end_str}")


def write_pii_notes():
    notes = """# PII Sanitization Notes

## Columns containing PII (real-user data only — synthetic data has none)
- `user_id` (when mapped to a real person) -> pseudonymized as user_real_0X, real identity kept only in private local notes, never committed.
- `ip_addr_decrypted` (from Extended Streaming History export) -> DROPPED entirely before Silver layer.
- `username` (from export) -> DROPPED before Silver layer; replaced by the pseudonymized user_id.
- `conn_country` -> retained (coarse geography, not directly identifying) but reviewed case-by-case.
- Exact `played_at` timestamps -> retained (needed for time-series analysis) but not combined with any other direct identifier once username/IP are dropped.

## Sanitization strategy
1. Bronze layer: raw data landed as-is (including real export fields) in a
   restricted/local-only location, never pushed to the public GitHub repo.
2. Silver layer transformation drops `ip_addr_decrypted`, `username`,
   `user_agent_decrypted`, and hashes `user_id` (SHA-256, truncated) before
   any data is persisted to shared/versioned storage.
3. Only pseudonymized, IP-free data ever reaches Gold or the BI dashboard.
4. Synthetic data requires no sanitization (no PII by construction) but
   flows through the same Silver transformation code path for consistency.
"""
    with open("output/PII_NOTES.md", "w") as f:
        f.write(notes)


def main():
    parser = argparse.ArgumentParser(description="Generate time-partitioned synthetic Spotify-style listening data.")
    sub = parser.add_subparsers(dest="mode", required=True)

    sub.add_parser("full", help="Generate the full historical load (many day-partitions at once).")

    p_inc = sub.add_parser("incremental", help="Generate a single day's partition (run this daily).")
    p_inc.add_argument("--date", help="YYYY-MM-DD (defaults to today).")

    p_bf = sub.add_parser("backfill", help="Regenerate a range of day-partitions.")
    p_bf.add_argument("--start", required=True, help="YYYY-MM-DD")
    p_bf.add_argument("--end", required=True, help="YYYY-MM-DD")

    args = parser.parse_args()

    catalog = build_catalog()
    # "unclassified" is catalog_utils' label for artists not yet in artist_genres.json, not a taste
    real_genres = sorted(set(t["genre"] for t in catalog) - {"unclassified"})
    users = build_users(real_genres)
    write_dims(catalog, users)
    write_pii_notes()

    if args.mode == "full":
        run_full(catalog, users)
    elif args.mode == "incremental":
        run_incremental(catalog, users, args.date)
    elif args.mode == "backfill":
        run_backfill(catalog, users, args.start, args.end)


if __name__ == "__main__":
    main()
