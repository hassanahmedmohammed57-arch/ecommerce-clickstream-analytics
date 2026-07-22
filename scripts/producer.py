#!/usr/bin/env python3
"""
Kinesis Producer for Criteo clickstream replay.

Simulates a live e-commerce ad impression / click stream by replaying
rows from the sampled dataset at a controlled rate.

Each record is sent as JSON:
{
  "event_id": "criteo-<row>",
  "event_time": "2026-07-02T12:34:56.789Z",
  "label": 0,
  "I1": 5, "I2": null, ...,
  "C1": "a1b2c3", ...
}

Usage (example):
  python scripts/producer.py \
      --stream-name ecomm-clickstream \
      --sample-file data/sample_train.csv \
      --rate 50 \
      --loop 3 \
      --region us-east-1

Requirements:
  pip install boto3 pandas
"""
import argparse
import boto3
import json
import time
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--stream-name", required=True, help="Kinesis stream name")
    p.add_argument("--sample-file", required=True, help="CSV produced by prepare_sample.py")
    p.add_argument("--rate", type=int, default=20, help="Records per second target")
    p.add_argument("--loop", type=int, default=1, help="How many times to replay the sample")
    p.add_argument("--region", default="us-east-1")
    p.add_argument("--dry-run", action="store_true", help="Print records instead of sending")
    p.add_argument("--start-time", default=None, help="ISO start time for synthetic timestamps")
    return p.parse_args()


def main():
    args = parse_args()

    df = pd.read_csv(args.sample_file)
    print(f"Loaded {len(df)} rows from {args.sample_file}")

    if len(df) == 0:
        print("Empty sample, aborting.")
        sys.exit(1)

    kinesis = None
    if not args.dry_run:
        kinesis = boto3.client("kinesis", region_name=args.region)
        # Quick check that stream exists
        try:
            kinesis.describe_stream(StreamName=args.stream_name)
        except Exception as e:
            print(f"Error describing stream: {e}")
            sys.exit(2)

    # Synthetic time base
    if args.start_time:
        base_time = datetime.fromisoformat(args.start_time.replace("Z", "+00:00"))
    else:
        base_time = datetime.now(timezone.utc)

    records_sent = 0
    interval = 1.0 / args.rate if args.rate > 0 else 0
    BATCH_MAX = 500  # Kinesis PutRecords hard limit is 500 records per call

    def flush(buffer):
        """Send a buffer via a single put_records call and retry failures once."""
        if not buffer:
            return 0
        try:
            resp = kinesis.put_records(StreamName=args.stream_name, Records=buffer)
            failed = resp.get("FailedRecordCount", 0)
            if failed:
                # Retry only the records that Kinesis rejected (throttling/partial failure).
                retry = [buffer[i] for i, r in enumerate(resp["Records"]) if r.get("ErrorCode")]
                time.sleep(0.5)
                kinesis.put_records(StreamName=args.stream_name, Records=retry)
            return len(buffer)
        except Exception as e:
            print(f"put_records failed: {e}")
            time.sleep(1)
            return 0

    buffer = []
    for loop_idx in range(args.loop):
        print(f"=== Replay loop {loop_idx + 1}/{args.loop} ===")
        for idx, row in df.iterrows():
            event_time = base_time + timedelta(seconds=records_sent * (1.0 / max(args.rate, 1)))

            record = {
                "event_id": f"criteo-{loop_idx}-{idx}",
                "event_time": event_time.isoformat().replace("+00:00", "Z"),
                "label": int(row["label"]) if pd.notna(row["label"]) else 0,
            }
            for i in range(1, 14):
                col = f"I{i}"
                val = row[col]
                record[col] = int(val) if pd.notna(val) else None
            for i in range(1, 27):
                col = f"C{i}"
                val = row[col]
                record[col] = str(val) if pd.notna(val) else None

            payload = json.dumps(record, default=str)

            if args.dry_run:
                if idx < 3 or idx % 5000 == 0:
                    print(payload[:200] + "..." if len(payload) > 200 else payload)
            else:
                buffer.append({"Data": payload, "PartitionKey": record["event_id"]})
                if len(buffer) >= BATCH_MAX:
                    records_sent += flush(buffer)
                    buffer = []

            records_sent += 1 if args.dry_run else 0

            # Rate limiting (applied per record so --rate stays meaningful)
            if args.rate > 0:
                time.sleep(interval)

            if idx % 1000 == 0 and idx > 0:
                print(f"  Sent {idx} in current loop (total {records_sent})")

        if not args.dry_run:
            records_sent += flush(buffer)
            buffer = []

    print(f"Done. Total records sent: {records_sent}")


if __name__ == "__main__":
    main()
