#!/usr/bin/env python3
"""
Offline unit test for the speed-layer Lambda handler.

Proves the handler correctly decodes REAL (base64) Kinesis records and
updates windowed counters, WITHOUT needing AWS. Run:

  python speed/test_lambda_handler.py

Exit code 0 = all assertions passed.
"""
import base64
import json
import sys
from datetime import datetime, timezone
from unittest import mock


class FakeTable:
    """Minimal in-memory stand-in for a DynamoDB Table supporting ADD/SET."""

    def __init__(self):
        self.items = {}

    def update_item(self, Key, UpdateExpression, ExpressionAttributeValues):
        bucket = Key["window_bucket"]
        row = self.items.setdefault(bucket, {"impressions": 0, "clicks": 0})
        if ":one" in ExpressionAttributeValues:
            row["impressions"] += ExpressionAttributeValues[":one"]
        if ":click" in ExpressionAttributeValues:
            row["clicks"] += ExpressionAttributeValues[":click"]
        if ":lat" in ExpressionAttributeValues:
            row["last_latency_ms"] = ExpressionAttributeValues[":lat"]


def make_kinesis_event(records):
    """Build a realistic Kinesis event: data is base64-encoded, like real AWS."""
    return {
        "Records": [
            {"kinesis": {"data": base64.b64encode(json.dumps(r).encode()).decode()}}
            for r in records
        ]
    }


def main():
    # Import with boto3 patched so no AWS connection is attempted at import time.
    with mock.patch("boto3.resource"):
        import importlib
        import lambda_handler as lh
        importlib.reload(lh)

    fake = FakeTable()
    lh.table = fake

    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    ts = now.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    bucket = now.strftime("%Y-%m-%dT%H:%M")

    events = [
        {"event_time": ts, "label": 1},
        {"event_time": ts, "label": 0},
        {"event_time": ts, "label": 1},
        {"event_time": ts, "label": 0},
    ]
    result = lh.lambda_handler(make_kinesis_event(events), None)

    assert result["processed"] == 4, f"expected 4 processed, got {result['processed']}"
    assert result["failed"] == 0, f"expected 0 failed, got {result['failed']}"
    row = fake.items[bucket]
    assert row["impressions"] == 4, f"expected 4 impressions, got {row['impressions']}"
    assert row["clicks"] == 2, f"expected 2 clicks, got {row['clicks']}"

    # Prove the OLD (buggy) approach would have failed on the same input.
    raw_b64 = make_kinesis_event([events[0]])["Records"][0]["kinesis"]["data"]
    old_approach_failed = False
    try:
        json.loads(raw_b64)  # what the original code did — no base64 decode
    except Exception:
        old_approach_failed = True
    assert old_approach_failed, "sanity: base64 data should not parse as JSON directly"

    print("PASS: 4 impressions, 2 clicks parsed from base64 Kinesis records.")
    print(f"      window={bucket} row={row}")
    print("      (confirmed the pre-fix json.loads-only approach would have raised)")
    return 0


if __name__ == "__main__":
    sys.path.insert(0, __file__.rsplit("/", 1)[0])
    sys.exit(main())
