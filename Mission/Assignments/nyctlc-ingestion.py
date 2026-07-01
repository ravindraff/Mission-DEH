"""
nyctlc-ingestion.py
-------------------
AWS Glue Python Shell job that:
  1. Auto-discovers the latest available HVFHV parquet file
     (tries current month - 2, -3, -4 until HTTP 200).
  2. Downloads the taxi zone lookup CSV.
  3. Streams both files directly into S3
     (no local disk — Glue Python Shell has limited /tmp space).

Libraries used: boto3, requests, sys, datetime  (all native to Glue Python Shell)
"""

import sys
import boto3
import requests
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta   # available in Glue Python Shell

# ── Constants ────────────────────────────────────────────────────────────────

BASE_URL      = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL      = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONE_S3_KEY   = "raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv"
TRIP_S3_PREFIX = "raw/nyctlc/fhvhv_tripdata/"

CHUNK_SIZE    = 8 * 1024 * 1024   # 8 MB streaming chunks
MAX_LOOKBACK  = 4                  # try up to 4 months back

# ── Helpers ───────────────────────────────────────────────────────────────────

def get_account_id() -> str:
    """Resolve the 12-digit AWS account ID at runtime."""
    sts = boto3.client("sts")
    account_id = sts.get_caller_identity()["Account"]
    print(f"[INFO] AWS Account ID resolved: {account_id}")
    return account_id


def build_bucket_name(account_id: str) -> str:
    bucket = f"mission-deh-hof-nyctlc-{account_id}"
    print(f"[INFO] Target S3 bucket: {bucket}")
    return bucket


def find_latest_hvfhv_url() -> tuple[str, str]:
    """
    Try current_month - 2, -3, -4 until an HTTP 200 is found.
    Returns (url, filename).
    Raises RuntimeError if none found.
    """
    now = datetime.now(tz=timezone.utc)
    print(f"[INFO] Current UTC date: {now.strftime('%Y-%m-%d')}")

    for months_back in range(2, MAX_LOOKBACK + 1):
        candidate = now - relativedelta(months=months_back)
        yyyymm    = candidate.strftime("%Y-%m")
        filename  = f"fhvhv_tripdata_{yyyymm}.parquet"
        url       = f"{BASE_URL}/{filename}"

        print(f"[INFO] Checking ({months_back} months back): {url}")
        response = requests.head(url, timeout=15, allow_redirects=True)
        print(f"[INFO]   HTTP {response.status_code}")

        if response.status_code == 200:
            size_mb = int(response.headers.get("Content-Length", 0)) / (1024 ** 2)
            print(f"[INFO] ✅ Found: {filename}  ({size_mb:.1f} MB)")
            return url, filename

    raise RuntimeError(
        f"No HVFHV parquet file found for the last {MAX_LOOKBACK - 1} months. "
        "Check TLC data availability."
    )


def stream_upload_to_s3(url: str, bucket: str, s3_key: str) -> None:
    """
    Stream-download from `url` and multipart-upload directly to S3.
    Never buffers the entire file in memory.
    """
    s3 = boto3.client("s3")

    print(f"[INFO] Starting streaming download → s3://{bucket}/{s3_key}")
    with requests.get(url, stream=True, timeout=60) as response:
        response.raise_for_status()

        total_bytes = int(response.headers.get("Content-Length", 0))
        print(f"[INFO] Expected file size: {total_bytes / (1024**2):.1f} MB")

        # Initiate S3 multipart upload
        mpu = s3.create_multipart_upload(Bucket=bucket, Key=s3_key)
        upload_id   = mpu["UploadId"]
        parts       = []
        part_number = 1
        buffer      = b""
        bytes_done  = 0

        try:
            for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
                buffer     += chunk
                bytes_done += len(chunk)

                # Upload a part when buffer reaches chunk size
                if len(buffer) >= CHUNK_SIZE:
                    part = s3.upload_part(
                        Bucket=bucket, Key=s3_key,
                        UploadId=upload_id, PartNumber=part_number,
                        Body=buffer,
                    )
                    parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                    pct = (bytes_done / total_bytes * 100) if total_bytes else 0
                    print(f"[INFO]   Uploaded part {part_number}  "
                          f"({bytes_done / (1024**2):.1f} MB  {pct:.1f}%)")
                    part_number += 1
                    buffer = b""

            # Upload any remaining bytes as the final part
            if buffer:
                part = s3.upload_part(
                    Bucket=bucket, Key=s3_key,
                    UploadId=upload_id, PartNumber=part_number,
                    Body=buffer,
                )
                parts.append({"PartNumber": part_number, "ETag": part["ETag"]})
                print(f"[INFO]   Uploaded final part {part_number}  "
                      f"({bytes_done / (1024**2):.1f} MB  100%)")

            # Complete multipart upload
            s3.complete_multipart_upload(
                Bucket=bucket, Key=s3_key, UploadId=upload_id,
                MultipartUpload={"Parts": parts},
            )
            print(f"[INFO] ✅ Upload complete: s3://{bucket}/{s3_key}")

        except Exception as exc:
            print(f"[ERROR] Upload failed — aborting multipart upload: {exc}")
            s3.abort_multipart_upload(
                Bucket=bucket, Key=s3_key, UploadId=upload_id
            )
            raise


def upload_zone_lookup(bucket: str) -> None:
    """Download taxi zone CSV and stream it to S3."""
    print(f"\n[INFO] ── Taxi Zone Lookup ──────────────────────────────")
    print(f"[INFO] Downloading: {ZONE_URL}")

    with requests.get(ZONE_URL, stream=True, timeout=30) as response:
        response.raise_for_status()
        content = response.content   # CSV is tiny (~10 KB) — single read is fine

    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=ZONE_S3_KEY, Body=content)
    print(f"[INFO] ✅ Zone lookup uploaded: s3://{bucket}/{ZONE_S3_KEY}")
    print(f"[INFO]    Size: {len(content) / 1024:.1f} KB")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 62)
    print("  NYC TLC HVFHV Ingestion — AWS Glue Python Shell")
    print("=" * 62)
    print(f"[INFO] Job started at {datetime.now(tz=timezone.utc).isoformat()}")

    try:
        # 1. Resolve account & bucket
        account_id  = get_account_id()
        bucket      = build_bucket_name(account_id)

        # 2. Find latest available HVFHV parquet
        print(f"\n[INFO] ── HVFHV Trip Data ───────────────────────────────")
        trip_url, filename = find_latest_hvfhv_url()
        trip_s3_key        = f"{TRIP_S3_PREFIX}{filename}"

        # 3. Stream trip data to S3
        stream_upload_to_s3(trip_url, bucket, trip_s3_key)

        # 4. Download & upload zone lookup
        upload_zone_lookup(bucket)

        # 5. Summary
        print("\n" + "=" * 62)
        print("  INGESTION COMPLETE")
        print("=" * 62)
        print(f"  Trip data : s3://{bucket}/{trip_s3_key}")
        print(f"  Zone CSV  : s3://{bucket}/{ZONE_S3_KEY}")
        print(f"  Finished  : {datetime.now(tz=timezone.utc).isoformat()}")
        print("=" * 62)

    except Exception as exc:
        print(f"\n[ERROR] Job failed: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
