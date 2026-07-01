"""
lambda_nyctlc_ingestion.py
===========================
AWS Lambda Function — NYC TLC HVFHV Ingestion

Trigger:    EventBridge (monthly schedule) or manual invocation
Runtime:    Python 3.12
Memory:     512 MB  (parquet files are ~300-500 MB, streamed in chunks)
Timeout:    900 seconds (15 min — Lambda max; large file download)
Role needs: s3:PutObject, s3:CreateBucket (optional), sts:GetCallerIdentity

What it does:
  1. Auto-discovers latest HVFHV parquet (current month-2 → month-6)
     using HEAD requests via urllib3 (no requests library needed).
  2. Downloads trip parquet → /tmp → upload to S3 → delete from /tmp.
  3. Downloads zone lookup CSV → /tmp → upload to S3 → delete from /tmp.
  4. Returns a JSON summary of everything uploaded.

Libraries used:
  boto3    — AWS SDK (Lambda built-in)
  urllib3  — HTTP client (Lambda built-in, no requests needed)
  os, sys, json, datetime  — stdlib only
"""

import os
import sys
import json
import logging
from datetime import datetime, timezone

import boto3
import urllib3

# ── Logging ───────────────────────────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# urllib3 connection pool — reused across the function lifecycle
http = urllib3.PoolManager(
    timeout=urllib3.Timeout(connect=10, read=300),   # 5-min read timeout
    retries=urllib3.Retry(total=3, backoff_factor=2)
)

# ── Constants ─────────────────────────────────────────────────────────────────
BASE_URL        = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL        = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONE_S3_KEY     = "raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv"
TRIP_S3_PREFIX  = "raw/nyctlc/fhvhv_tripdata/"
TMP_DIR         = "/tmp"

START_OFFSET    = 2    # TLC publishes ~2 months behind current date
MAX_LOOKBACK    = 6    # try up to 6 months back before giving up
CHUNK_SIZE      = 8 * 1024 * 1024   # 8 MB download chunks


# ── Pure stdlib month subtraction (no dateutil) ───────────────────────────────
def subtract_months(year: int, month: int, n: int):
    """Subtract n months from (year, month). Returns (new_year, new_month)."""
    month = month - n
    while month <= 0:
        month += 12
        year  -= 1
    return year, month


# ── Helper: HEAD request to check file availability ──────────────────────────
def url_exists(url: str) -> tuple:
    """
    Send a HEAD request. Returns (exists: bool, content_length: int).
    Uses urllib3 — no requests library required.
    """
    try:
        resp = http.request("HEAD", url, redirect=True)
        if resp.status == 200:
            size = int(resp.headers.get("Content-Length", 0))
            return True, size
        return False, 0
    except urllib3.exceptions.RequestError as exc:
        logger.warning("HEAD request failed for %s: %s", url, exc)
        return False, 0


# ── Helper: Stream-download to /tmp ──────────────────────────────────────────
def download_to_tmp(url: str, local_path: str) -> int:
    """
    Stream-download `url` to `local_path` in CHUNK_SIZE chunks.
    Returns total bytes written.
    Raises RuntimeError on non-200 status.
    """
    logger.info("Downloading: %s → %s", url, local_path)

    resp = http.request("GET", url, preload_content=False)
    if resp.status != 200:
        resp.release_conn()
        raise RuntimeError(
            f"HTTP {resp.status} downloading {url}"
        )

    total_bytes = 0
    try:
        with open(local_path, "wb") as fh:
            for chunk in resp.stream(CHUNK_SIZE):
                fh.write(chunk)
                total_bytes += len(chunk)
                logger.info(
                    "  Downloaded %s MB...",
                    round(total_bytes / (1024 ** 2), 1)
                )
    finally:
        resp.release_conn()

    logger.info("Download complete: %.1f MB", total_bytes / (1024 ** 2))
    return total_bytes


# ── Helper: Upload /tmp file to S3 then delete locally ───────────────────────
def upload_and_cleanup(local_path: str, bucket: str, s3_key: str) -> int:
    """
    Upload local_path to s3://bucket/s3_key using boto3 upload_file
    (handles multipart automatically for large files), then remove
    the local file to free /tmp space.
    Returns the file size in bytes.
    """
    file_size = os.path.getsize(local_path)
    logger.info(
        "Uploading to s3://%s/%s  (%.1f MB)",
        bucket, s3_key, file_size / (1024 ** 2)
    )

    s3 = boto3.client("s3")

    # boto3 upload_file uses multipart automatically for files > 8 MB
    # and handles retries internally via TransferConfig defaults.
    s3.upload_file(
        Filename=local_path,
        Bucket=bucket,
        Key=s3_key,
    )
    logger.info("Upload complete: s3://%s/%s", bucket, s3_key)

    # Delete from /tmp to free space for next file
    os.remove(local_path)
    logger.info("Removed local file: %s", local_path)

    return file_size


# ── Helper: Discover latest available HVFHV parquet URL ──────────────────────
def find_latest_hvfhv() -> tuple:
    """
    Try current_month - START_OFFSET backwards until MAX_LOOKBACK.
    Returns (url, filename, size_bytes) for the first HTTP-200 found.
    Raises RuntimeError if nothing found.
    """
    now   = datetime.now(tz=timezone.utc)
    year  = now.year
    month = now.month

    logger.info(
        "Searching for HVFHV parquet (offsets %d to %d from %s)...",
        START_OFFSET, MAX_LOOKBACK, now.strftime("%Y-%m")
    )

    for offset in range(START_OFFSET, MAX_LOOKBACK + 1):
        cy, cm   = subtract_months(year, month, offset)
        yyyymm   = f"{cy:04d}-{cm:02d}"
        filename = f"fhvhv_tripdata_{yyyymm}.parquet"
        url      = f"{BASE_URL}/{filename}"

        logger.info("  Trying (month-%d): %s", offset, filename)
        exists, size = url_exists(url)

        if exists:
            logger.info(
                "  Found: %s  (%.1f MB)", filename, size / (1024 ** 2)
            )
            return url, filename, size

    raise RuntimeError(
        f"No HVFHV parquet found in last {MAX_LOOKBACK} months. "
        "Check TLC data availability."
    )


# ── Lambda Handler ────────────────────────────────────────────────────────────
def lambda_handler(event, context):
    """
    Main Lambda entry point.

    Supports optional event keys:
      event["dry_run"] = true  — discover URL but skip download/upload
    """
    logger.info("=" * 56)
    logger.info("  NYC TLC Lambda Ingestion started")
    logger.info("  Invoked at: %s", datetime.now(tz=timezone.utc).isoformat())
    logger.info("=" * 56)

    dry_run = bool(event.get("dry_run", False))
    if dry_run:
        logger.info("DRY RUN mode — no files will be downloaded or uploaded.")

    uploaded  = []
    errors    = []

    try:
        # ── 1. Resolve account ID and bucket name ─────────────────
        logger.info("Resolving AWS Account ID...")
        account_id = boto3.client("sts").get_caller_identity()["Account"]
        bucket     = f"mission-deh-hof-nyctlc-{account_id}"
        logger.info("Account ID : %s", account_id)
        logger.info("S3 Bucket  : %s", bucket)

        # ── 2. Discover latest HVFHV parquet URL ──────────────────
        trip_url, filename, trip_size = find_latest_hvfhv()
        trip_s3_key = f"{TRIP_S3_PREFIX}{filename}"
        trip_tmp    = os.path.join(TMP_DIR, filename)

        # ── 3. Download trip parquet → /tmp → S3 → delete /tmp ───
        if not dry_run:
            logger.info("--- Trip Data ---")
            bytes_downloaded = download_to_tmp(trip_url, trip_tmp)
            bytes_uploaded   = upload_and_cleanup(trip_tmp, bucket, trip_s3_key)

            uploaded.append({
                "file"        : filename,
                "source_url"  : trip_url,
                "s3_path"     : f"s3://{bucket}/{trip_s3_key}",
                "size_bytes"  : bytes_uploaded,
                "size_mb"     : round(bytes_uploaded / (1024 ** 2), 1),
                "status"      : "SUCCESS",
            })
            logger.info("Trip data upload complete ✅")
        else:
            uploaded.append({
                "file"       : filename,
                "source_url" : trip_url,
                "s3_path"    : f"s3://{bucket}/{trip_s3_key}",
                "size_mb"    : round(trip_size / (1024 ** 2), 1),
                "status"     : "DRY_RUN",
            })

        # ── 4. Download zone lookup CSV → /tmp → S3 → delete /tmp ─
        logger.info("--- Taxi Zone Lookup CSV ---")
        zone_tmp    = os.path.join(TMP_DIR, "taxi_zone_lookup.csv")
        zone_s3_key = ZONE_S3_KEY

        if not dry_run:
            bytes_downloaded = download_to_tmp(ZONE_URL, zone_tmp)
            bytes_uploaded   = upload_and_cleanup(zone_tmp, bucket, zone_s3_key)

            uploaded.append({
                "file"       : "taxi_zone_lookup.csv",
                "source_url" : ZONE_URL,
                "s3_path"    : f"s3://{bucket}/{zone_s3_key}",
                "size_bytes" : bytes_uploaded,
                "size_kb"    : round(bytes_uploaded / 1024, 1),
                "status"     : "SUCCESS",
            })
            logger.info("Zone lookup upload complete ✅")
        else:
            zone_exists, zone_size = url_exists(ZONE_URL)
            uploaded.append({
                "file"       : "taxi_zone_lookup.csv",
                "source_url" : ZONE_URL,
                "s3_path"    : f"s3://{bucket}/{zone_s3_key}",
                "size_kb"    : round(zone_size / 1024, 1),
                "status"     : "DRY_RUN",
            })

    except Exception as exc:
        logger.error("Ingestion failed: %s", exc, exc_info=True)
        errors.append(str(exc))

    # ── 5. Build and return summary ───────────────────────────────
    success = len(errors) == 0

    summary = {
        "status"          : "SUCCESS" if success else "FAILED",
        "timestamp"       : datetime.now(tz=timezone.utc).isoformat(),
        "dry_run"         : dry_run,
        "files_uploaded"  : len([u for u in uploaded if u["status"] == "SUCCESS"]),
        "uploaded"        : uploaded,
        "errors"          : errors,
    }

    logger.info("=" * 56)
    logger.info("  SUMMARY")
    logger.info("=" * 56)
    logger.info("  Status  : %s", summary["status"])
    for item in uploaded:
        logger.info(
            "  [%s] %s → %s",
            item["status"], item["file"], item["s3_path"]
        )
    if errors:
        for err in errors:
            logger.error("  ERROR: %s", err)
    logger.info("=" * 56)

    # Lambda must return a serialisable dict
    return {
        "statusCode" : 200 if success else 500,
        "body"       : json.dumps(summary, default=str),
    }
