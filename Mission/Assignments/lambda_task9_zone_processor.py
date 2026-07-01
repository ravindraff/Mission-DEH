"""
lambda_task9_zone_processor.py
================================
AWS Lambda Function — Task 9: Process Zone Lookup with Pandas (Custom Layer)

Setup:
  Function name : mission-deh-hof-zone-lookup-processor
  Runtime       : Python 3.12
  Memory        : 256 MB
  Timeout       : 30 seconds
  Layer         : mission-deh-hof-pandas-layer  (pandas + numpy)
  IAM Policy    : AmazonS3FullAccess

Create the pandas layer (Windows PowerShell):
  mkdir python
  pip install pandas -t python/
  Compress-Archive -Path "python" -DestinationPath "pandas-layer.zip" -Force

Create the pandas layer (Linux / AWS CloudShell):
  mkdir -p python
  pip install pandas -t python/
  zip -r pandas-layer.zip python/
"""

import io
import json
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd

# ── Logging setup ─────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Lambda entry point.

    Steps:
      1. Resolve bucket name from AWS Account ID (no hardcoding)
      2. Read taxi zone lookup CSV from S3 into pandas DataFrame
      3. Print shape and first 5 rows
      4. Count zones per borough
      5. Find borough with the most zones
      6. Write summary JSON to S3
      7. Return borough zone counts as response
    """

    logger.info("=" * 56)
    logger.info("  Task 9 — Zone Lookup Processor")
    logger.info("  %s", datetime.now(tz=timezone.utc).isoformat())
    logger.info("=" * 56)

    # ── Step 1: Resolve account ID and bucket name ────────────────
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    bucket     = "mission-deh-hof-nyctlc-" + account_id

    SOURCE_KEY  = "raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv"
    SUMMARY_KEY = "processed/nyctlc/zone_summary.json"

    logger.info("Account  : %s", account_id)
    logger.info("Bucket   : %s", bucket)
    logger.info("Reading  : s3://%s/%s", bucket, SOURCE_KEY)

    # ── Step 2: Read CSV from S3 into pandas DataFrame ────────────
    s3       = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=SOURCE_KEY)
    raw_csv  = response["Body"].read()

    df = pd.read_csv(io.BytesIO(raw_csv))

    # ── Step 3: Print shape and first 5 rows ──────────────────────
    logger.info("Shape    : %d rows x %d columns", df.shape[0], df.shape[1])
    logger.info("Columns  : %s", list(df.columns))
    logger.info("First 5 rows:\n%s", df.head(5).to_string(index=False))

    # ── Step 4: Count zones per borough ───────────────────────────
    # Borough column contains: Manhattan, Brooklyn, Queens,
    #                          Bronx, Staten Island, EWR, Unknown
    zones_per_borough = (
        df.groupby("Borough")["Zone"]
        .count()
        .sort_values(ascending=False)
    )

    logger.info("Zones per borough:\n%s", zones_per_borough.to_string())

    # ── Step 5: Borough with the most zones ───────────────────────
    top_borough    = zones_per_borough.index[0]
    top_zone_count = int(zones_per_borough.iloc[0])

    logger.info("Top borough: %s with %d zones", top_borough, top_zone_count)

    # ── Step 6: Build summary and write JSON to S3 ────────────────
    # Convert borough counts to plain dict for JSON serialisation
    borough_zone_counts = {
        borough: int(count)
        for borough, count in zones_per_borough.items()
    }

    # Service zone breakdown as a bonus metric
    service_zone_counts = {
        sz: int(cnt)
        for sz, cnt in df.groupby("service_zone")["Zone"]
        .count()
        .sort_values(ascending=False)
        .items()
    }

    summary = {
        "generated_at"             : datetime.now(tz=timezone.utc).isoformat(),
        "source"                   : f"s3://{bucket}/{SOURCE_KEY}",
        "total_zones"              : int(df.shape[0]),
        "total_boroughs"           : int(zones_per_borough.shape[0]),
        "borough_with_most_zones"  : {
            "borough"    : top_borough,
            "zone_count" : top_zone_count,
        },
        "zones_per_borough"        : borough_zone_counts,
        "zones_per_service_zone"   : service_zone_counts,
        "sample_records"           : df.head(5).to_dict(orient="records"),
    }

    summary_json = json.dumps(summary, indent=2, default=str)

    s3.put_object(
        Bucket      = bucket,
        Key         = SUMMARY_KEY,
        Body        = summary_json.encode("utf-8"),
        ContentType = "application/json",
    )

    logger.info("Summary written → s3://%s/%s", bucket, SUMMARY_KEY)

    # ── Final report in logs ──────────────────────────────────────
    logger.info("")
    logger.info("=" * 56)
    logger.info("  ZONE LOOKUP SUMMARY REPORT")
    logger.info("=" * 56)
    logger.info("  Total zones    : %d", summary["total_zones"])
    logger.info("  Total boroughs : %d", summary["total_boroughs"])
    logger.info("  Top borough    : %s (%d zones)", top_borough, top_zone_count)
    logger.info("")
    logger.info("  Zones per Borough:")
    for borough, count in borough_zone_counts.items():
        logger.info("    %-22s : %d", borough, count)
    logger.info("")
    logger.info("  Zones per Service Zone:")
    for szone, count in service_zone_counts.items():
        logger.info("    %-22s : %d", szone, count)
    logger.info("=" * 56)

    # ── Step 7: Return response ───────────────────────────────────
    return {
        "statusCode"              : 200,
        "total_zones"             : summary["total_zones"],
        "total_boroughs"          : summary["total_boroughs"],
        "borough_with_most_zones" : summary["borough_with_most_zones"],
        "zones_per_borough"       : borough_zone_counts,
        "zones_per_service_zone"  : service_zone_counts,
        "summary_written_to"      : f"s3://{bucket}/{SUMMARY_KEY}",
    }
