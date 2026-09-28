#!/usr/bin/env python3
"""
user_overview.py — quick-look report over the medallion pipeline's output/
=============================================================================
Joins output/dims/users.json + output/dims/catalog.json against every
partition under output/raw/dt=*/events.json to answer, per user:
  - how many plays, over how many days, how many unique tracks
  - skip rate
  - top genres (by play count)
  - top artists (by play count)
  - device mix (if device_type is populated on the events)

If output/validation/user_segments.json exists, its planted cluster /
favorite_genres / engagement_score are shown alongside the *observed*
numbers so you can sanity-check the generator against its own ground truth.

Usage
-----
    python3 scripts/user_overview.py
    python3 scripts/user_overview.py --top 5              # top-N genres/artists per user
    python3 scripts/user_overview.py --user user_real_01  # single user, full detail
    python3 scripts/user_overview.py --json out.json      # dump the full report as JSON
    python3 scripts/user_overview.py --csv out.csv        # one row per user, for a spreadsheet

Run this from the repo root (it expects the output/ layout generate_data_v2.py
writes: output/dims/, output/raw/dt=YYYY-MM-DD/, output/validation/).
"""

import argparse
import csv
import glob
import json
import os
from collections import Counter, defaultdict


def load_json(path, default=None):
    if not os.path.exists(path):
        return default
    with open(path, "r") as f:
        return json.load(f)


def load_all_events(raw_dir):
    events = []
    for path in sorted(glob.glob(os.path.join(raw_dir, "dt=*", "events.json"))):
        day_events = load_json(path, [])
        events.extend(day_events)
    # also pick up sample_events.json / any stray events.json directly under raw/
    stray = os.path.join(raw_dir, "sample_events.json")
    if os.path.exists(stray):
        events.extend(load_json(stray, []))
    return events


def build_report(root="."):
    dims_dir = os.path.join(root, "output", "dims")
    raw_dir = os.path.join(root, "output", "raw")
    val_dir = os.path.join(root, "output", "validation")

    users = load_json(os.path.join(dims_dir, "users.json"), [])
    catalog = load_json(os.path.join(dims_dir, "catalog.json"), [])
    segments = load_json(os.path.join(val_dir, "user_segments.json"), [])
    events = load_all_events(raw_dir)

    if not users:
        raise SystemExit(f"No users found at {dims_dir}/users.json — run this from the repo root.")

    track_lookup = {t["track_id"]: t for t in catalog}
    segment_lookup = {s["user_id"]: s for s in segments}
    user_lookup = {u["user_id"]: u for u in users}

    per_user = {
        u["user_id"]: {
            "plays": 0,
            "unique_tracks": set(),
            "skipped": 0,
            "skipped_known": 0,  # events where skipped is not None
            "genre_counts": Counter(),
            "artist_counts": Counter(),
            "device_counts": Counter(),
            "days": set(),
            "unmatched_tracks": 0,
        }
        for u in users
    }

    unresolved_users = 0
    for e in events:
        uid = e.get("user_id")
        if uid not in per_user:
            unresolved_users += 1
            continue
        bucket = per_user[uid]
        bucket["plays"] += 1
        tid = e.get("track_id")
        bucket["unique_tracks"].add(tid)
        played_at = e.get("played_at") or ""
        if played_at:
            bucket["days"].add(played_at[:10])
        if e.get("skipped") is not None:
            bucket["skipped_known"] += 1
            if e.get("skipped"):
                bucket["skipped"] += 1
        if e.get("device_type"):
            bucket["device_counts"][e["device_type"]] += 1

        track = track_lookup.get(tid)
        if track:
            bucket["artist_counts"][track.get("artist", "Unknown")] += 1
            bucket["genre_counts"][track.get("genre", "Unknown")] += 1
        else:
            bucket["unmatched_tracks"] += 1

    report = {"generated_from": {"users": len(users), "catalog_tracks": len(catalog), "events": len(events)},
              "users": []}

    for uid, bucket in per_user.items():
        dim = user_lookup.get(uid, {})
        seg = segment_lookup.get(uid, {})
        skip_rate = (bucket["skipped"] / bucket["skipped_known"]) if bucket["skipped_known"] else None
        report["users"].append({
            "user_id": uid,
            "country": dim.get("country"),
            "is_premium": dim.get("is_premium"),
            "cluster": seg.get("cluster"),
            "planted_favorite_genres": seg.get("favorite_genres"),
            "plays": bucket["plays"],
            "unique_tracks": len(bucket["unique_tracks"]),
            "active_days": len(bucket["days"]),
            "skip_rate": round(skip_rate, 3) if skip_rate is not None else None,
            "top_genres": bucket["genre_counts"].most_common(),
            "top_artists": bucket["artist_counts"].most_common(),
            "device_mix": dict(bucket["device_counts"]),
            "unmatched_tracks": bucket["unmatched_tracks"],
        })

    report["users"].sort(key=lambda u: u["plays"], reverse=True)
    if unresolved_users:
        report["generated_from"]["events_with_unknown_user"] = unresolved_users
    return report


def print_report(report, top_n, only_user=None):
    meta = report["generated_from"]
    print(f"Users: {meta['users']}  |  Catalog tracks: {meta['catalog_tracks']}  |  Events loaded: {meta['events']}")
    if meta.get("events_with_unknown_user"):
        print(f"  (skipped {meta['events_with_unknown_user']} events for user_ids not in dims/users.json)")
    print("-" * 100)

    users = report["users"]
    if only_user:
        users = [u for u in users if u["user_id"] == only_user]
        if not users:
            print(f"No such user_id: {only_user}")
            return

    for u in users:
        print(f"\n{u['user_id']}  [{u['country']}]  {'premium' if u['is_premium'] else 'free'}"
              f"{'  cluster=' + u['cluster'] if u['cluster'] else ''}")
        print(f"  plays: {u['plays']:<6}  unique tracks: {u['unique_tracks']:<6}  "
              f"active days: {u['active_days']:<4}  skip rate: {u['skip_rate']}")
        if u["planted_favorite_genres"]:
            print(f"  planted favorite genres: {', '.join(u['planted_favorite_genres'])}")
        if u["top_genres"]:
            top_g = ", ".join(f"{g} ({c})" for g, c in u["top_genres"][:top_n])
            print(f"  top genres:  {top_g}")
        if u["top_artists"]:
            top_a = ", ".join(f"{a} ({c})" for a, c in u["top_artists"][:top_n])
            print(f"  top artists: {top_a}")
        if u["device_mix"]:
            dm = ", ".join(f"{d}: {c}" for d, c in u["device_mix"].items())
            print(f"  devices: {dm}")
        if u["unmatched_tracks"]:
            print(f"  ! {u['unmatched_tracks']} plays referenced a track_id not in catalog.json")

    if not only_user:
        total_plays = sum(u["plays"] for u in users)
        overall_genres = Counter()
        overall_artists = Counter()
        for u in users:
            for g, c in u["top_genres"]:
                overall_genres[g] += c
            for a, c in u["top_artists"]:
                overall_artists[a] += c
        print("\n" + "=" * 100)
        print(f"TOTAL plays across all users: {total_plays}")
        if overall_genres:
            print("Overall top genres: " + ", ".join(f"{g} ({c})" for g, c in overall_genres.most_common(top_n)))
        if overall_artists:
            print("Overall top artists: " + ", ".join(f"{a} ({c})" for a, c in overall_artists.most_common(top_n)))


def write_csv(report, path, top_n):
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["user_id", "country", "is_premium", "cluster", "plays", "unique_tracks",
                    "active_days", "skip_rate", f"top_{top_n}_genres", f"top_{top_n}_artists"])
        for u in report["users"]:
            w.writerow([
                u["user_id"], u["country"], u["is_premium"], u["cluster"],
                u["plays"], u["unique_tracks"], u["active_days"], u["skip_rate"],
                "; ".join(f"{g}:{c}" for g, c in u["top_genres"][:top_n]),
                "; ".join(f"{a}:{c}" for a, c in u["top_artists"][:top_n]),
            ])
    print(f"\nWrote {path}")


def main():
    ap = argparse.ArgumentParser(description="Overview of synthetic users, plays, genres and top artists.")
    ap.add_argument("--root", default=".", help="Repo root (default: current directory)")
    ap.add_argument("--top", type=int, default=5, help="Top-N genres/artists to show per user (default: 5)")
    ap.add_argument("--user", default=None, help="Show full detail for a single user_id only")
    ap.add_argument("--json", default=None, help="Write the full report to this JSON file")
    ap.add_argument("--csv", default=None, help="Write a per-user summary row to this CSV file")
    args = ap.parse_args()

    report = build_report(args.root)
    print_report(report, args.top, only_user=args.user)

    if args.json:
        with open(args.json, "w") as f:
            json.dump(report, f, indent=2, default=list)
        print(f"\nWrote {args.json}")
    if args.csv:
        write_csv(report, args.csv, args.top)


if __name__ == "__main__":
    main()