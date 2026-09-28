"""
clean_real_synthetic_overlap.py
================================
One-time cleanup for raw partitions generated before generate_data_v2.py
checked per-day real coverage before generating synthetic events for a user.

For each partition, removes synthetic_persona_matched rows for any user who
already has a real or real_api row in that same partition. Leaves everything
else untouched: other users' synthetic rows on that day, real/real_api rows,
and days where no overlap exists.

Usage:
    python3 clean_real_synthetic_overlap.py output/raw
"""
import glob
import json
import os
import sys

REAL_SOURCES = {"real", "real_api"}


def clean_partition(path):
    with open(path, encoding="utf-8") as f:
        events = json.load(f)
    real_users = {e["user_id"] for e in events if e.get("source") in REAL_SOURCES}
    cleaned = [e for e in events
               if not (e.get("source") == "synthetic_persona_matched" and e["user_id"] in real_users)]
    removed = len(events) - len(cleaned)
    if removed:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(cleaned, f, indent=2)
    return removed


def main():
    raw_dir = sys.argv[1] if len(sys.argv) > 1 else "output/raw"
    files = sorted(glob.glob(os.path.join(raw_dir, "dt=*", "events.json")))
    total_removed = 0
    days_touched = 0
    for fp in files:
        removed = clean_partition(fp)
        if removed:
            days_touched += 1
            total_removed += removed
    print(f"Scanned {len(files)} partitions under {raw_dir}")
    print(f"Removed {total_removed} inflated synthetic_persona_matched events across {days_touched} partitions")


if __name__ == "__main__":
    main()