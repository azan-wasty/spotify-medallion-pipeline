"""
Script: fetch_recently_played.py
================================
Purpose:
    Fetches live listening history via the Spotify Web API `/v1/me/player/recently-played` endpoint using OAuth.
    Formats pulled events into the standardized schema and drops them into the landing zone (`output/raw/dt=YYYY-MM-DD/events.json`).

Spotify API Quota & Security Compliance:
    - Restricted to maximum 5 registered test users (Feb 2026 Developer Mode limits).
    - Uses OAuth 2.0 Authorization Code flow for secure authorization.
    - Tags output events with `source: "real_api"`.

Usage:
    python3 scripts/fetch_recently_played.py --user-id user_real_01 --login     # One-time login OAuth
    python3 scripts/fetch_recently_played.py --user-id user_real_01            # Ongoing scheduled pull
"""

import argparse
import base64
import hashlib
import json
import os
import sys
import time
import urllib.parse
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer


def load_env(env_path=".env"):
    """Load environment variables from a .env file."""
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


def get_spotify_credentials(env_path=".env"):
    """Retrieve Client ID and Client Secret from environment or .env file."""
    env_vars = load_env(env_path)

    client_id = (
        os.getenv("SPOTIFY_CLIENT_ID")
        or os.getenv("CLIENT_ID")
        or env_vars.get("Client_ID")
        or env_vars.get("SPOTIFY_CLIENT_ID")
        or env_vars.get("CLIENT_ID")
    )
    client_secret = (
        os.getenv("SPOTIFY_CLIENT_SECRET")
        or os.getenv("CLIENT_SECRET")
        or env_vars.get("Client_secret")
        or env_vars.get("SPOTIFY_CLIENT_SECRET")
        or env_vars.get("CLIENT_SECRET")
    )

    return client_id, client_secret


def get_token_file_path(user_id):
    """Returns the token cache file path for a user."""
    return f".spotify_tokens_{user_id}.json"


def save_tokens(user_id, token_data):
    """Save token dict to local JSON cache."""
    path = get_token_file_path(user_id)
    if "expires_in" in token_data:
        token_data["expires_at"] = time.time() + token_data["expires_in"] - 60
    with open(path, "w", encoding="utf-8") as f:
        json.dump(token_data, f, indent=2)
    print(f"Tokens saved successfully to {path}")


def load_tokens(user_id):
    """Load cached token dict for a user."""
    path = get_token_file_path(user_id)
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


class OAuthCallbackHandler(BaseHTTPRequestHandler):
    """HTTP Request Handler for OAuth redirect callback."""
    code = None
    error = None

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = urllib.parse.parse_qs(parsed.query)

        if "code" in params:
            OAuthCallbackHandler.code = params["code"][0]
            message = "<h1>Authorization Successful!</h1><p>You can close this window and return to your terminal.</p>"
            status_code = 200
        else:
            OAuthCallbackHandler.error = params.get("error", ["Unknown error"])[0]
            message = f"<h1>Authorization Failed</h1><p>Error: {OAuthCallbackHandler.error}</p>"
            status_code = 400

        self.send_response(status_code)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(message.encode("utf-8"))

    def log_message(self, format, *args):
        return


def run_oauth_login(client_id, client_secret, user_id, port=8888):
    """Interactively log in user via browser OAuth flow."""
    redirect_uri = f"http://127.0.0.1:{port}/callback"
    scope = "user-read-recently-played"

    auth_params = {
        "client_id": client_id,
        "response_type": "code",
        "redirect_uri": redirect_uri,
        "scope": scope,
        "show_dialog": "true",
    }
    auth_url = "https://accounts.spotify.com/authorize?" + urllib.parse.urlencode(auth_params)

    print("\n" + "=" * 60)
    print(f"Spotify OAuth Authorization for user: {user_id}")
    print("=" * 60)
    print("Opening your web browser for Spotify authentication...")
    print(f"If it does not open automatically, visit:\n{auth_url}\n")

    webbrowser.open(auth_url)

    server_address = ("127.0.0.1", port)
    httpd = HTTPServer(server_address, OAuthCallbackHandler)
    httpd.handle_request()

    if OAuthCallbackHandler.error:
        print(f"OAuth Login failed: {OAuthCallbackHandler.error}")
        sys.exit(1)

    auth_code = OAuthCallbackHandler.code
    if not auth_code:
        print("Failed to receive authorization code from Spotify.")
        sys.exit(1)

    print("Authorization code received. Requesting access & refresh tokens...")

    token_url = "https://accounts.spotify.com/api/token"
    body = urllib.parse.urlencode({
        "grant_type": "authorization_code",
        "code": auth_code,
        "redirect_uri": redirect_uri,
    }).encode("utf-8")

    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    req = urllib.request.Request(
        token_url,
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req) as resp:
            token_data = json.loads(resp.read().decode("utf-8"))
            save_tokens(user_id, token_data)
            return token_data
    except Exception as e:
        print(f"Error exchanging token: {e}")
        sys.exit(1)


def refresh_access_token(client_id, client_secret, user_id, refresh_token):
    """Refresh an expired Spotify access token using the refresh_token."""
    token_url = "https://accounts.spotify.com/api/token"
    body = urllib.parse.urlencode({
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }).encode("utf-8")

    auth_header = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("utf-8")
    req = urllib.request.Request(
        token_url,
        data=body,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST"
    )

    try:
        with urllib.request.urlopen(req) as resp:
            new_tokens = json.loads(resp.read().decode("utf-8"))
            if "refresh_token" not in new_tokens:
                new_tokens["refresh_token"] = refresh_token
            save_tokens(user_id, new_tokens)
            return new_tokens
    except Exception as e:
        print(f"Error refreshing token for user {user_id}: {e}")
        sys.exit(1)


def fetch_recently_played_api(access_token, limit=50):
    """Call Spotify API GET /v1/me/player/recently-played."""
    url = f"https://api.spotify.com/v1/me/player/recently-played?limit={limit}"
    req = urllib.request.Request(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        method="GET"
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        print(f"Spotify API Error: {e}")
        sys.exit(1)


def transform_api_item(item, user_id):
    """Transform API recently-played item to standard medallion schema."""
    played_at = item.get("played_at")
    track = item.get("track", {})
    track_uri = track.get("uri") or f"spotify:track:{track.get('id')}"
    duration_ms = track.get("duration_ms", 0)

    if not played_at or not track_uri:
        return None

    h = hashlib.sha256(f"{user_id}_{played_at}_{track_uri}".encode("utf-8")).hexdigest()[:12]
    event_id = f"evt_api_{h}"

    return {
        "event_id": event_id,
        "user_id": user_id,
        "track_id": track_uri,
        "played_at": played_at,
        "ms_played": duration_ms,
        "skipped": False,
        "device_type": "mobile",
        "source": "real_api"
    }


def write_or_merge_partition(day_str, new_events, output_dir):
    """Merge API events into the landing zone daily partition file."""
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


def process_live_api_fetch(user_id, login=False, env_path=".env", output_dir="output/raw"):
    client_id, client_secret = get_spotify_credentials(env_path)
    if not client_id or not client_secret:
        print(f"Error: Missing Spotify API credentials in {env_path} or environment variables.")
        print("Please ensure Client_ID and Client_secret are set.")
        sys.exit(1)

    tokens = load_tokens(user_id)

    if login or not tokens:
        print(f"Initiating OAuth login for user {user_id}...")
        tokens = run_oauth_login(client_id, client_secret, user_id)

    if tokens.get("expires_at") and time.time() >= tokens["expires_at"]:
        print("Access token expired. Refreshing token...")
        tokens = refresh_access_token(client_id, client_secret, user_id, tokens["refresh_token"])

    access_token = tokens.get("access_token")
    if not access_token:
        print("Error: No valid access token found.")
        sys.exit(1)

    print(f"Fetching recently played tracks for {user_id} via Spotify API...")
    data = fetch_recently_played_api(access_token)
    items = data.get("items", [])
    print(f"Retrieved {len(items)} tracks from Spotify API.")

    # Auto-catalog any new tracks dynamically
    raw_tracks = []
    for item in items:
        track = item.get("track", {})
        track_uri = track.get("uri") or f"spotify:track:{track.get('id')}"
        if track_uri:
            raw_tracks.append({
                "track_id": track_uri,
                "track_name": track.get("name"),
                "artist": track.get("artists", [{}])[0].get("name") if track.get("artists") else "Unknown Artist",
                "album": track.get("album", {}).get("name") if track.get("album") else "Single",
                "ms_played": track.get("duration_ms", 0)
            })

    if raw_tracks:
        try:
            from catalog_utils import register_new_tracks
            new_cat_count = register_new_tracks(raw_tracks)
            if new_cat_count > 0:
                print(f"Auto-cataloged {new_cat_count} new track(s) into catalog.json and real_catalog_extract.json")
        except Exception as e:
            print(f"Warning: Failed to auto-catalog tracks: {e}")

    events_by_day = {}
    for item in items:
        evt = transform_api_item(item, user_id)
        if not evt:
            continue
        day_str = evt["played_at"][:10]
        if day_str not in events_by_day:
            events_by_day[day_str] = []
        events_by_day[day_str].append(evt)

    total_added = 0
    for day_str, evts in sorted(events_by_day.items()):
        added = write_or_merge_partition(day_str, evts, output_dir)
        total_added += added

    print(f"API pull complete. Merged {total_added} new events across {len(events_by_day)} partitions into {output_dir}/")


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch live recently-played tracks via Spotify Web API.")
    parser.add_argument("--user-id", required=True, help="Pseudonym ID for user")
    parser.add_argument("--login", action="store_true", help="Trigger interactive OAuth login flow")
    parser.add_argument("--env-file", default=".env", help="Path to .env credentials file")
    parser.add_argument("--output-dir", default="output/raw", help="Target landing directory")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    process_live_api_fetch(args.user_id, login=args.login, env_path=args.env_file, output_dir=args.output_dir)

