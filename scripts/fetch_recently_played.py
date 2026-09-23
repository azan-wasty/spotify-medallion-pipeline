"""
Script: fetch_recently_played.py
================================
Purpose:
    Fetches live listening history via the Spotify Web API `/v1/me/player/recently-played` endpoint using OAuth.
    Formats pulled events into the standardized schema and drops them into the landing zone (`output/raw/dt=YYYY-MM-DD/events.json`).

Spotify API Quota & Security Compliance:
    - Restricted to maximum 5 registered test users (Feb 2026 Developer Mode limits).
    - Uses OAuth 2.0 PKCE flow for secure authorization.
    - Tags output events with `source: "real_api"`.

Usage:
    python3 scripts/fetch_recently_played.py --user-id user_real_01 --login     # One-time login OAuth
    python3 scripts/fetch_recently_played.py --user-id user_real_01            # Ongoing scheduled pull
"""

import argparse

def parse_args():
    parser = argparse.ArgumentParser(description="Fetch live recently-played tracks via Spotify Web API.")
    parser.add_argument("--user-id", required=True, help="Pseudonym ID for user")
    parser.add_argument("--login", action="store_true", help="Trigger interactive OAuth login flow")
    return parser.parse_args()

if __name__ == "__main__":
    print("Live API pull script placeholder.")
