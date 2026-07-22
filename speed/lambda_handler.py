"""
AWS Lambda handler for the Speed Layer (Lambda Architecture).

Triggered by Kinesis Data Streams (event source mapping).

For each batch of records it:
- base64-decodes the Kinesis payload (REAL Kinesis records are base64 encoded)
- parses the JSON impression/click event
- determines the 1-minute time bucket from event_time
- updates DynamoDB counters with a single atomic ADD (impressions [+ clicks])
- records processing latency (now - event_time) so the serving/benchmark layer
  can report end-to-end freshness.

Expected DynamoDB table:
  Table name: speed_window_stats
  PK: window_bucket (string, e.g. "2026-07-02T12:34")
  Attributes: impressions (N), clicks (N), last_latency_ms (N), updated_at (S)

Environment variables:
  DDB_TABLE       = "speed_window_stats"
  WINDOW_MINUTES  = "5"
"""
import base64
import json
import os
from datetime import datetime, timezone

import boto3

dynamodb = boto3.resource("dynamodb")
table_name = os.environ.get("DDB_TABLE", "speed_window_stats")
table = dynamodb.Table(table_name)

WINDOW_MINUTES = int(os.environ.get("WINDOW_MINUTES", "5"))


def decode_kinesis_data(raw):
    """Return the decoded UTF-8 string for a Kinesis record's data field.

    Real AWS Kinesis records deliver ``record["kinesis"]["data"]`` as a
    base64-encoded string. Some local test harnesses pass raw JSON. This
    handles both: try base64 first, fall back to treating it as plain text.
    """
    if isinstance(raw, (bytes, bytearray)):
        return raw.decode("utf-8")
    try:
        return base64.b64decode(raw, validate=True).decode("utf-8")
    except Exception:
        # Already-decoded JSON (local invoke / unit test)
        return raw


def get_bucket(event_time_str):
    """Floor event_time to the minute bucket. Returns (bucket_key, event_datetime)."""
    try:
        dt = datetime.fromisoformat(str(event_time_str).replace("Z", "+00:00"))
        floored = dt.replace(second=0, microsecond=0)
        return floored.strftime("%Y-%m-%dT%H:%M"), dt
    except Exception:
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        return now.strftime("%Y-%m-%dT%H:%M"), now


def compute_latency_ms(event_dt):
    """End-to-end freshness: wall-clock now minus the record's event_time."""
    try:
        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - event_dt
        return max(0.0, delta.total_seconds() * 1000.0)
    except Exception:
        return 0.0


def update_window(bucket, is_click, latency_ms):
    """Single atomic update of window counters (impressions [+ clicks])."""
    update_expr = "ADD impressions :one" + (", clicks :click" if is_click else "")
    values = {":one": 1, ":click": 1} if is_click else {":one": 1}
    update_expr += " SET last_latency_ms = :lat, updated_at = :ts"
    values[":lat"] = int(latency_ms)
    values[":ts"] = datetime.now(timezone.utc).isoformat()
    table.update_item(
        Key={"window_bucket": bucket},
        UpdateExpression=update_expr,
        ExpressionAttributeValues=values,
    )


def lambda_handler(event, context):
    records = event.get("Records", [])
    processed = 0
    failed = 0
    latencies = []

    for record in records:
        try:
            raw = record["kinesis"]["data"]
            payload = json.loads(decode_kinesis_data(raw))

            event_time = payload.get("event_time")
            label = payload.get("label", 0)

            bucket, event_dt = get_bucket(event_time)
            latency_ms = compute_latency_ms(event_dt)
            latencies.append(latency_ms)

            update_window(bucket, int(label) == 1, latency_ms)
            processed += 1

            if processed % 100 == 0:
                print(f"Processed {processed} records...")

        except Exception as e:
            failed += 1
            print(f"Failed to process record: {e}")
            # Continue; a poison record must not stall the whole batch.

    avg_latency = round(sum(latencies) / len(latencies), 1) if latencies else 0.0
    print(f"Batch done: processed={processed} failed={failed} avg_latency_ms={avg_latency}")

    return {
        "statusCode": 200,
        "processed": processed,
        "failed": failed,
        "avg_latency_ms": avg_latency,
    }
