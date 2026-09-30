"""
Module: spotify_resolver.py
===========================
Purpose:
    Resolves Spotify track URIs and artist IDs for (artist, title) pairs
    using the Spotify Web API Search endpoint (/v1/search?type=track)
    with Client Credentials flow (app-only credentials from .env).

Rules:
    1. Exact matching: Accept a result ONLY when artist and title match exactly
       after case-folding (strip + casefold). Otherwise, keep the 'nm:' key
       (e.g., 'nm:Artist:Track') to prevent remasters/covers from being wrongly assigned.
    2. Rate limiting & robust handling: Respect 429 Retry-After, short pause between calls.
    3. Persistent caching: Cache every result locally in scripts/spotify_track_cache.json
       so that reruns make zero redundant API calls.
    4. Catalog integration: Also checks scripts/real_catalog_extract.json first.
"""

import base64
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from http.client import RemoteDisconnected
from urllib.error import HTTPError, URLError

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_PATH = os.path.join(ROOT_DIR, "scripts", "spotify_track_cache.json")
CATALOG_EXTRACT_PATH = os.path.join(ROOT_DIR, "scripts", "real_catalog_extract.json")
ENV_PATH = os.path.join(ROOT_DIR, ".env")


def load_env(env_path=ENV_PATH):
    """Load key-value pairs from .env file."""
    config = {}
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, v = line.split("=", 1)
                    config[k.strip()] = v.strip().strip("'\"")
    return config


class SpotifyResolver:
    def __init__(self, env_path=ENV_PATH, cache_path=CACHE_PATH):
        self.env_path = env_path
        self.cache_path = cache_path
        self.client_id = None
        self.client_secret = None
        self.access_token = None
        self.token_expiry = 0
        self.cache = self._load_cache()
        self.existing_catalog = self._load_catalog_extract()
        self._init_credentials()

    def _init_credentials(self):
        env = load_env(self.env_path)
        self.client_id = (
            os.getenv("SPOTIFY_CLIENT_ID")
            or os.getenv("CLIENT_ID")
            or env.get("Client_ID")
            or env.get("SPOTIFY_CLIENT_ID")
            or env.get("CLIENT_ID")
        )
        self.client_secret = (
            os.getenv("SPOTIFY_CLIENT_SECRET")
            or os.getenv("CLIENT_SECRET")
            or env.get("Client_secret")
            or env.get("SPOTIFY_CLIENT_SECRET")
            or env.get("CLIENT_SECRET")
        )
        if not self.client_id or not self.client_secret:
            raise ValueError("Spotify Client_ID or Client_secret not found in environment or .env file.")

    def _load_cache(self):
        if os.path.exists(self.cache_path):
            try:
                with open(self.cache_path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception as e:
                print(f"Warning: Could not load cache from {self.cache_path}: {e}")
        return {}

    def save_cache(self):
        os.makedirs(os.path.dirname(self.cache_path), exist_ok=True)
        with open(self.cache_path, "w", encoding="utf-8") as f:
            json.dump(self.cache, f, indent=2, ensure_ascii=False)

    def _load_catalog_extract(self):
        lookup = {}
        if os.path.exists(CATALOG_EXTRACT_PATH):
            try:
                with open(CATALOG_EXTRACT_PATH, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    for item in data:
                        art = (item.get("artist") or "").strip().casefold()
                        trk = (item.get("track_name") or "").strip().casefold()
                        tid = item.get("track_id")
                        if art and trk and tid and tid.startswith("spotify:track:"):
                            lookup[(art, trk)] = item
            except Exception as e:
                print(f"Warning: Could not load catalog extract from {CATALOG_EXTRACT_PATH}: {e}")
        return lookup

    def _get_token(self):
        now = time.time()
        if self.access_token and now < self.token_expiry - 60:
            return self.access_token

        auth_str = base64.b64encode(f"{self.client_id}:{self.client_secret}".encode("utf-8")).decode("utf-8")
        req = urllib.request.Request(
            "https://accounts.spotify.com/api/token",
            data=b"grant_type=client_credentials",
            headers={
                "Authorization": f"Basic {auth_str}",
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        for attempt in range(5):
            try:
                with urllib.request.urlopen(req, timeout=15) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    self.access_token = data["access_token"]
                    self.token_expiry = now + int(data.get("expires_in", 3600))
                    return self.access_token
            except HTTPError as e:
                if e.code == 429:
                    retry_after = int(e.headers.get("Retry-After", 5))
                    time.sleep(retry_after + 1)
                else:
                    time.sleep(2)
            except Exception:
                time.sleep(2)

        raise RuntimeError("Failed to obtain Spotify access token via Client Credentials.")

    @staticmethod
    def cache_key(artist, title):
        return f"{artist.strip().casefold()}__@@__{title.strip().casefold()}"

    @staticmethod
    def fallback_nm_key(artist, title):
        return f"nm:{artist.strip()}:{title.strip()}"

    def _search_api(self, query):
        if getattr(self, "_rate_limited", False):
            return []
        token = self._get_token()
        url = "https://api.spotify.com/v1/search?" + urllib.parse.urlencode({
            "q": query,
            "type": "track",
            "limit": 10,
        })

        for attempt in range(5):
            req = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
            try:
                with urllib.request.urlopen(req, timeout=12) as resp:
                    return json.loads(resp.read().decode("utf-8")).get("tracks", {}).get("items", [])
            except HTTPError as e:
                if e.code == 429:
                    retry_after = int(e.headers.get("Retry-After", 5))
                    if retry_after > 30:
                        print(f"\n[Rate Limit 429] Daily/Hourly quota exceeded (Retry-After {retry_after}s). Falling back to nm: IDs.")
                        self._rate_limited = True
                        return []
                    print(f"\n[Rate Limit 429] Sleeping {retry_after + 1}s...")
                    time.sleep(retry_after + 1)
                elif e.code == 401:
                    self.access_token = None
                    token = self._get_token()
                else:
                    time.sleep(1)
            except (URLError, RemoteDisconnected, TimeoutError) as e:
                time.sleep(1.5)
            except Exception as e:
                time.sleep(1)

        return []

    def resolve_track(self, artist, title):
        art_clean = artist.strip()
        trk_clean = title.strip()
        c_key = self.cache_key(art_clean, trk_clean)

        # 1. Check local cache
        if c_key in self.cache:
            return self.cache[c_key]

        # 2. Check existing real_catalog_extract
        cat_key = (art_clean.casefold(), trk_clean.casefold())
        if cat_key in self.existing_catalog:
            cat_item = self.existing_catalog[cat_key]
            res = {
                "track_id": cat_item["track_id"],
                "track_name": cat_item["track_name"],
                "artist": cat_item["artist"],
                "artist_id": None,
                "album": cat_item.get("album") or "Single",
                "duration_ms": cat_item.get("max_ms"),
                "release_date": None,
                "matched": True,
                "source": "catalog_extract"
            }
            self.cache[c_key] = res
            return res

        # 3. Query Spotify Search API
        # Try structured field query first
        safe_title = trk_clean.replace(":", " ")
        safe_artist = art_clean.replace(":", " ")
        q_field = f"track:{safe_title} artist:{safe_artist}"
        items = self._search_api(q_field)

        # Fallback to plain search if field query returned nothing
        if not items:
            q_plain = f"{safe_title} {safe_artist}"
            items = self._search_api(q_plain)

        # Strict exact match evaluation
        target_art_norm = art_clean.casefold()
        target_trk_norm = trk_clean.casefold()
        matched_item = None

        for item in items:
            item_name_norm = item.get("name", "").strip().casefold()
            item_artists = [a.get("name", "").strip().casefold() for a in item.get("artists", [])]

            # Exact match after case-folding
            if item_name_norm == target_trk_norm and target_art_norm in item_artists:
                matched_item = item
                break

        if matched_item:
            primary_artist = matched_item["artists"][0]
            album_info = matched_item.get("album", {})
            res = {
                "track_id": matched_item.get("uri"),
                "track_name": matched_item.get("name"),
                "artist": primary_artist.get("name"),
                "artist_id": primary_artist.get("id"),
                "album": album_info.get("name") or "Single",
                "duration_ms": matched_item.get("duration_ms"),
                "release_date": album_info.get("release_date"),
                "matched": True,
                "source": "spotify_search_exact"
            }
        else:
            # Fallback to nm: key (keeps original name key to prevent misattribution)
            res = {
                "track_id": self.fallback_nm_key(art_clean, trk_clean),
                "track_name": trk_clean,
                "artist": art_clean,
                "artist_id": None,
                "album": "Single",
                "duration_ms": None,
                "release_date": None,
                "matched": False,
                "source": "name_fallback"
            }

        self.cache[c_key] = res
        return res
