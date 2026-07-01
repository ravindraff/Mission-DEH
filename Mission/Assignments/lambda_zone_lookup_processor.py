"""
lambda_zone_lookup_processor.py
================================
AWS Lambda Function — Task 9: Process Zone Lookup with Pandas (Custom Layer)

Prerequisites:
  - pandas Lambda Layer named "mission-deh-hof-pandas-layer" attached
  - Lambda IAM role has AmazonS3FullAccess
  - Runtime: Python 3.12 | Memory: 256 MB | Timeout: 30 sec

What it does:
  1. Reads  s3://mission-deh-hof-nyctlc-<account_id>/raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv
  2. Loads it into a pandas DataFrame
  3. Prints shape and first 5 rows
  4. Counts zones per borough
  5. Finds the borough with the most zones
  6. Writes a summary JSON back to S3 at:
       s3://mission-deh-hof-nyctlc-<account_id>/processed/nyctlc/zone_summary.json
  7. Returns borough zone counts as the Lambda response

Layer packaging (Windows PowerShell):
  mkdir python
  pip install pandas -t python/
  Compress-Archive -Path "python" -DestinationPath "pandas-layer.zip" -Force

Layer packaging (Linux / CloudShell):
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

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# ── Constants ─────────────────────────────────────────────────────────────────
ZONE_CSV_KEY    = "raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv"
SUMMARY_S3_KEY  = "processed/nyctlc/zone_summary.json"


def lambda_handler(event, context):
    """
    Main Lambda entry point.
    Reads taxi zone CSV from S3, processes with pandas,
    writes a summary JSON back to S3, and returns borough counts.
    """
    logger.info("=" * 54)
    logger.info("  Zone Lookup Processor — Task 9")
    logger.info("  Started: %s", datetime.now(tz=timezone.utc).isoformat())
    logger.info("=" * 54)

    # ── 1. Resolve account ID and bucket name ─────────────────────
    account_id = boto3.client("sts").get_caller_identity()["Account"]
    bucket     = f"mission-deh-hof-nyctlc-{account_id}"

    logger.info("Account ID : %s", account_id)
    logger.info("Bucket     : %s", bucket)
    logger.info("Source key : %s", ZONE_CSV_KEY)

    # ── 2. Read the CSV from S3 into a pandas DataFrame ───────────
    logger.info("Reading taxi zone lookup CSV from S3...")
    s3 = boto3.client("s3")

    response = s3.get_object(Bucket=bucket, Key=ZONE_CSV_KEY)
    csv_body = response["Body"].read()

    # Load into DataFrame
    zone_df = pd.read_csv(io.BytesIO(csv_body))

    # ── 3. Print shape and first 5 rows ───────────────────────────
    logger.info("DataFrame shape: %s rows x %s columns", *zone_df.shape)
    logger.info("Columns: %s", list(zone_df.columns))
    logger.info("First 5 rows:\n%s", zone_df.head(5).to_string(index=False))

    # ── 4. Count zones per borough ────────────────────────────────
    # The CSV has a column named "Borough" (case-sensitive)
    borough_counts = (
        zone_df
        .groupby("Borough")["Zone"]
        .count()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"Zone": "zone_count"})
    )

    logger.info("Zones per borough:\n%s", borough_counts.to_string(index=False))

    # ── 5. Find borough with the most zones ───────────────────────
    top_borough     = borough_counts.iloc[0]["Borough"]
    top_zone_count  = int(borough_counts.iloc[0]["zone_count"])

    logger.info("Borough with most zones: %s (%d zones)", top_borough, top_zone_count)

    # ── 6. Build summary dict ─────────────────────────────────────
    borough_zone_counts = {
        row["Borough"]: int(row["zone_count"])
        for _, row in borough_counts.iterrows()
    }

    # Service zone breakdown
    service_zone_counts = (
        zone_df
        .groupby("service_zone")["Zone"]
        .count()
        .sort_values(ascending=False)
        .to_dict()
    )
    service_zone_counts = {k: int(v) for k, v in service_zone_counts.items()}

    summary = {
        "generated_at"        : datetime.now(tz=timezone.utc).isoformat(),
        "source_bucket"       : bucket,
        "source_key"          : ZONE_CSV_KEY,
        "total_zones"         : int(zone_df.shape[0]),
        "total_boroughs"      : int(borough_counts.shape[0]),
        "borough_with_most_zones": {
            "borough"    : top_borough,
            "zone_count" : top_zone_count,
        },
        "zones_per_borough"   : borough_zone_counts,
        "zones_per_service_zone": service_zone_counts,
        "sample_zones"        : zone_df.head(5).to_dict(orient="records"),
    }

    # ── 7. Write summary JSON to S3 ───────────────────────────────
    summary_json = json.dumps(summary, indent=2, default=str)

    s3.put_object(
        Bucket      = bucket,
        Key         = SUMMARY_S3_KEY,
        Body        = summary_json.encode("utf-8"),
        ContentType = "application/json",
    )

    logger.info("Summary written to: s3://%s/%s", bucket, SUMMARY_S3_KEY)

    # ── 8. Print formatted report ─────────────────────────────────
    logger.info("")
    logger.info("=" * 54)
    logger.info("  ZONE LOOKUP PROCESSING REPORT")
    logger.info("=" * 54)
    logger.info("  Total zones     : %d", summary["total_zones"])
    logger.info("  Total boroughs  : %d", summary["total_boroughs"])
    logger.info("  Top borough     : %s (%d zones)", top_borough, top_zone_count)
    logger.info("")
    logger.info("  Zones per Borough:")
    for borough, count in borough_zone_counts.items():
        logger.info("    %-25s : %d", borough, count)
    logger.info("")
    logger.info("  Zones per Service Zone:")
    for sz, count in service_zone_counts.items():
        logger.info("    %-25s : %d", sz, count)
    logger.info("=" * 54)
    logger.info("  Output: s3://%s/%s", bucket, SUMMARY_S3_KEY)
    logger.info("=" * 54)

    # ── 9. Return response ────────────────────────────────────────
    return {
        "statusCode"          : 200,
        "total_zones"         : summary["total_zones"],
        "total_boroughs"      : summary["total_boroughs"],
        "borough_with_most_zones": summary["borough_with_most_zones"],
        "zones_per_borough"   : borough_zone_counts,
        "zones_per_service_zone": service_zone_counts,
        "summary_s3_path"     : f"s3://{bucket}/{SUMMARY_S3_KEY}",
    }
