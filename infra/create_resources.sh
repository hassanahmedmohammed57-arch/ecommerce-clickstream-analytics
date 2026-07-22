#!/bin/bash
# Quick helper script to create core resources in AWS Academy Learner Lab.
# Run in CloudShell or local with AWS CLI + credentials from lab.

set -e

REGION="${AWS_REGION:-us-east-1}"
STREAM_NAME="ecomm-clickstream"
BUCKET_NAME="ecomm-clickstream-$(date +%s | tail -c 6)"
DDB_TABLE="speed_window_stats"

echo "=== Creating resources in region $REGION ==="

# S3
aws s3 mb "s3://${BUCKET_NAME}" --region "$REGION" || true
echo "S3 bucket: s3://${BUCKET_NAME}"

# Kinesis (start with 2 shards)
aws kinesis create-stream \
  --stream-name "$STREAM_NAME" \
  --shard-count 2 \
  --region "$REGION" || true
echo "Kinesis stream: $STREAM_NAME"

# DynamoDB (on-demand for simplicity)
aws dynamodb create-table \
  --table-name "$DDB_TABLE" \
  --attribute-definitions AttributeName=window_bucket,AttributeType=S \
  --key-schema AttributeName=window_bucket,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region "$REGION" || true
echo "DynamoDB table: $DDB_TABLE"

echo ""
echo "Next steps:"
echo "1. Upload sample data: aws s3 cp data/sample_train.csv s3://${BUCKET_NAME}/raw/"
echo "2. Update producer / scripts to use stream and bucket."
echo "3. Launch EMR cluster via console or CLI (include Spark)."
echo "4. Create Lambda with speed/lambda_handler.py (attach to Kinesis, set env DDB_TABLE)."
echo ""
echo "Remember to terminate resources after demo!"
