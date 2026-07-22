#!/usr/bin/env python3
"""
Local end-to-end simulator for the Lambda Architecture pipeline.

This lets you develop and test the full flow (producer -> speed windows -> batch
views -> serving merge) WITHOUT any AWS services.

It uses in-memory structures that mimic:
- Kinesis (direct hand-off)
- Lambda speed processor (calls the SAME window logic as the real handler)
- DynamoDB (dict of buckets)
- Batch view (computed on the fly with pandas, mirroring the PySpark job)

It also measures per-record end-to-end latency (scheduled time -> processed time),
so higher ingestion rates that outrun the machine surface as rising latency.
This produces a genuine latency-vs-load signal for Phase 3.

Run:
  python scripts/local_simulator.py --sample data/sample_train.csv --rate 100 --duration 30 --window 5
"""
import argparse
import json
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone
import pandas as pd

# In-memory "DynamoDB"
speed_windows = defaultdict(lambda: {"impressions": 0, "clicks": 0})

# In-memory batch view (simulated full history)
batch_view = {}


def get_bucket(event_time: datetime) -> str:
    floored = event_time.replace(second=0, microsecond=0)
    return floored.strftime("%Y-%m-%dT%H:%M")


def process_speed_record(record: dict):
    """Mimics the Lambda handler window logic."""
    try:
        event_time = datetime.fromisoformat(record["event_time"].replace("Z", "+00:00"))
    except Exception:
        event_time = datetime.now(timezone.utc)

    bucket = get_bucket(event_time)
    is_click = int(record.get("label", 0)) == 1

    speed_windows[bucket]["impressions"] += 1
    if is_click:
        speed_windows[bucket]["clicks"] += 1


def compute_batch_view(df: pd.DataFrame) -> dict:
    """Mimics the PySpark batch job (CTR + averages)."""
    total_imps = len(df)
    total_clicks = int(df["label"].sum())
    ctr = total_clicks / total_imps if total_imps > 0 else 0

    avg_is = {}
    for i in range(1, 14):
        col = f"I{i}"
        if col in df.columns:
            avg_is[f"avg_I{i}"] = float(df[col].mean()) if df[col].notna().any() else None

    return {
        "total_impressions": total_imps,
        "total_clicks": total_clicks,
        "ctr": round(ctr, 6),
        "avg_numeric_features": {k: (round(v, 4) if v is not None else None) for k, v in avg_is.items()},
    }


def get_recent_speed(window_minutes: int = 5):
    """Mimics serving query against DDB: aggregate last N 1-minute buckets."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(minutes=window_minutes + 1)

    recent = []
    total_imps = 0
    total_clicks = 0

    for bucket, stats in sorted(speed_windows.items()):
        try:
            bdt = datetime.strptime(bucket, "%Y-%m-%dT%H:%M").replace(tzinfo=timezone.utc)
            if bdt >= cutoff:
                recent.append({"bucket": bucket, **stats})
                total_imps += stats["impressions"]
                total_clicks += stats["clicks"]
        except Exception:
            pass

    ctr = total_clicks / total_imps if total_imps > 0 else 0.0
    return {
        "window_minutes": window_minutes,
        "recent_impressions": total_imps,
        "recent_clicks": total_clicks,
        "recent_ctr": round(ctr, 6),
        "buckets": recent[-10:],
    }


def _percentile(values, pct):
    if not values:
        return 0.0
    s = sorted(values)
    k = (len(s) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    frac = k - lo
    return s[lo] + (s[hi] - s[lo]) * frac


def run_simulation(sample_path: str, rate: int, duration_sec: int, window_min: int, quiet: bool = False):
    if not quiet:
        print("=== LOCAL LAMBDA ARCHITECTURE SIMULATOR ===")
        print(f"Sample: {sample_path}")
        print(f"Rate: {rate} rec/s | Duration: {duration_sec}s | Window: {window_min} min\n")

    df = pd.read_csv(sample_path)
    if not quiet:
        print(f"Loaded {len(df)} rows")

    global batch_view, speed_windows
    speed_windows = defaultdict(lambda: {"impressions": 0, "clicks": 0})
    batch_view = compute_batch_view(df)
    if not quiet:
        print(f"Batch view computed: CTR={batch_view['ctr']}, Imps={batch_view['total_impressions']}")

    interval = 1.0 / rate if rate > 0 else 0.0
    start_wall = time.time()
    start_time = datetime.now(timezone.utc)
    sent = 0
    idx = 0
    latencies_ms = []  # end-to-end: scheduled dispatch -> processed

    end_time = start_wall + duration_sec

    while time.time() < end_time:
        # When this record was *scheduled* to be dispatched (perfect rate).
        scheduled_wall = start_wall + sent * interval
        row = df.iloc[idx % len(df)]
        event_time = start_time + timedelta(seconds=sent * interval)

        record = {
            "event_id": f"sim-{sent}",
            "event_time": event_time.isoformat().replace("+00:00", "Z"),
            "label": int(row["label"]) if pd.notna(row["label"]) else 0,
        }
        for i in range(1, 14):
            col = f"I{i}"
            val = row.get(col)
            record[col] = int(val) if pd.notna(val) else None

        # Process (Kinesis -> Lambda -> DDB), then record how late it was.
        process_speed_record(record)
        processed_wall = time.time()
        latency_ms = max(0.0, (processed_wall - scheduled_wall) * 1000.0)
        latencies_ms.append(latency_ms)

        sent += 1
        idx += 1

        if not quiet and (sent % max(1, rate // 2) == 0 or sent < 5):
            speed = get_recent_speed(window_min)
            print(f"[{sent:5d}] recent {window_min}min: imps={speed['recent_impressions']}, "
                  f"clicks={speed['recent_clicks']}, ctr={speed['recent_ctr']:.4f}")

        # Pace to target rate.
        sleep_for = scheduled_wall + interval - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)

    latency_stats = {
        "mean_ms": round(sum(latencies_ms) / len(latencies_ms), 2) if latencies_ms else 0.0,
        "p50_ms": round(_percentile(latencies_ms, 50), 2),
        "p95_ms": round(_percentile(latencies_ms, 95), 2),
        "p99_ms": round(_percentile(latencies_ms, 99), 2),
        "max_ms": round(max(latencies_ms), 2) if latencies_ms else 0.0,
    }

    if not quiet:
        print(f"\n=== FINAL RESULTS after {sent} records ===")
        print("Batch (historical):")
        print(json.dumps(batch_view, indent=2))
        final_speed = get_recent_speed(window_min)
        print("\nSpeed (recent window):")
        print(json.dumps(final_speed, indent=2))
        print("\nLatency (end-to-end, ms):")
        print(json.dumps(latency_stats, indent=2))
        merged = {
            "batch_ctr": batch_view["ctr"],
            "speed_recent_ctr": final_speed["recent_ctr"],
            "speed_volume": final_speed["recent_impressions"],
            "note": "In real system: speed from DDB, batch from S3",
        }
        print("\nMerged serving view:")
        print(json.dumps(merged, indent=2))
    else:
        final_speed = get_recent_speed(window_min)

    return {
        "sent": sent,
        "batch": batch_view,
        "speed": final_speed,
        "latency": latency_stats,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--sample", default="data/sample_train.csv")
    parser.add_argument("--rate", type=int, default=80)
    parser.add_argument("--duration", type=int, default=25, help="Seconds to simulate")
    parser.add_argument("--window", type=int, default=5)
    args = parser.parse_args()

    run_simulation(args.sample, args.rate, args.duration, args.window)
