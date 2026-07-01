"""
nyctlc-ingestion-6months.py
AWS Glue Python Shell — ingests latest 6 months of HVFHV trip data + zone lookup into S3.
"""

import sys
import traceback
import boto3
import requests
from datetime import datetime, timezone

# ── Config ────────────────────────────────────────────────────────────────────
BASE_URL       = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL       = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
ZONE_S3_KEY    = "raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv"
TRIP_PREFIX    = "raw/nyctlc/fhvhv_tripdata/"
TARGET_MONTHS  = 6
MAX_LOOKBACK   = 14
START_OFFSET   = 2
CHUNK_SIZE     = 8 * 1024 * 1024   # 8 MB

# ── Pure stdlib month subtraction (no dateutil) ───────────────────────────────
def subtract_months(year, month, n):
    month = month - n
    while month <= 0:
        month += 12
        year  -= 1
    return year, month

# ── Step 1: Get account ID ────────────────────────────────────────────────────
print("=" * 60)
print("NYC TLC HVFHV Ingestion — 6 Months | Glue Python Shell")
print("=" * 60)
print("[INFO] Started: {}".format(datetime.now(tz=timezone.utc).isoformat()))

print("\n[INFO] Getting AWS Account ID...")
sts        = boto3.client("sts")
account_id = sts.get_caller_identity()["Account"]
bucket     = "mission-deh-hof-nyctlc-{}".format(account_id)
print("[INFO] Account ID : {}".format(account_id))
print("[INFO] S3 Bucket  : {}".format(bucket))

# ── Create bucket if it doesn't exist ─────────────────────────────────────────
s3_setup = boto3.client("s3")
try:
    s3_setup.head_bucket(Bucket=bucket)
    print("[INFO] Bucket already exists — skipping creation.")
except Exception:
    print("[INFO] Bucket not found — creating: {}".format(bucket))
    region = boto3.session.Session().region_name or "us-east-1"
    if region == "us-east-1":
        # us-east-1 must NOT pass CreateBucketConfiguration
        s3_setup.create_bucket(Bucket=bucket)
    else:
        s3_setup.create_bucket(
            Bucket=bucket,
            CreateBucketConfiguration={"LocationConstraint": region}
        )
    print("[INFO] Bucket created: s3://{}".format(bucket))

    # Create the expected folder structure
    for prefix in [
        "raw/nyctlc/fhvhv_tripdata/",
        "raw/nyctlc/taxi_zone_lookup/",
        "curated/nyctlc/fhvhv_trips_curated/",
        "aggregated/nyctlc/daily_borough_summary/",
        "aggregated/nyctlc/hourly_pattern_summary/",
        "aggregated/nyctlc/route_summary/",
        "athena-results/",
    ]:
        s3_setup.put_object(Bucket=bucket, Key=prefix, Body=b"")
    print("[INFO] Folder structure created.")

# ── Step 2: Discover 6 available HVFHV files ─────────────────────────────────
print("\n[INFO] Discovering latest {} HVFHV monthly files...".format(TARGET_MONTHS))

now   = datetime.now(tz=timezone.utc)
year  = now.year
month = now.month
found = []
offset = START_OFFSET

while len(found) < TARGET_MONTHS and offset <= MAX_LOOKBACK:
    cy, cm   = subtract_months(year, month, offset)
    yyyymm   = "{:04d}-{:02d}".format(cy, cm)
    filename = "fhvhv_tripdata_{}.parquet".format(yyyymm)
    url      = "{}/{}".format(BASE_URL, filename)

    print("[INFO]   month-{}: {}".format(offset, filename), end="  ")

    try:
        r       = requests.head(url, timeout=20, allow_redirects=True)
        cl      = r.headers.get("Content-Length", "")
        size_mb = int(cl) / (1024 ** 2) if cl.isdigit() else 0
        print("HTTP {}  {:.0f} MB".format(r.status_code,
              size_mb) if size_mb else "HTTP {}".format(r.status_code))

        if r.status_code == 200:
            found.append((url, filename))
            print("[INFO]   -> added [{}/{}]".format(len(found), TARGET_MONTHS))

    except Exception as e:
        print("ERROR: {}".format(e))

    offset += 1

print("\n[INFO] Found {} file(s):".format(len(found)))
for i, (_, fn) in enumerate(found, 1):
    print("[INFO]   {}. {}".format(i, fn))

if not found:
    print("[ERROR] No files found. Exiting.")
    raise SystemError("No HVFHV files available in the last {} months".format(MAX_LOOKBACK))

# ── Step 3: Stream-upload each trip file ─────────────────────────────────────
s3       = boto3.client("s3")
uploaded = []
failed   = []

for idx, (url, filename) in enumerate(found, 1):
    s3_key = "{}{}".format(TRIP_PREFIX, filename)
    print("\n[INFO] [{}/{}] Uploading: {}".format(idx, len(found), filename))
    print("[INFO]   Source : {}".format(url))
    print("[INFO]   Target : s3://{}/{}".format(bucket, s3_key))

    try:
        with requests.get(url, stream=True, timeout=180) as resp:
            resp.raise_for_status()

            cl          = resp.headers.get("Content-Length", "")
            total_bytes = int(cl) if cl.isdigit() else 0
            if total_bytes:
                print("[INFO]   Size   : {:.1f} MB".format(total_bytes / (1024 ** 2)))

            mpu       = s3.create_multipart_upload(Bucket=bucket, Key=s3_key)
            upload_id = mpu["UploadId"]
            parts     = []
            part_num  = 1
            buf       = b""
            done      = 0

            try:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    buf  += chunk
                    done += len(chunk)

                    if len(buf) >= CHUNK_SIZE:
                        part = s3.upload_part(
                            Bucket=bucket, Key=s3_key,
                            UploadId=upload_id, PartNumber=part_num, Body=buf)
                        parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                        pct = (done / total_bytes * 100) if total_bytes else 0
                        print("[INFO]   Part {:>3}  {:>7.1f} MB  {:>5.1f}%".format(
                            part_num, done / (1024 ** 2), pct))
                        part_num += 1
                        buf = b""

                if buf:
                    part = s3.upload_part(
                        Bucket=bucket, Key=s3_key,
                        UploadId=upload_id, PartNumber=part_num, Body=buf)
                    parts.append({"PartNumber": part_num, "ETag": part["ETag"]})
                    print("[INFO]   Part {:>3}  {:>7.1f} MB  100.0% (final)".format(
                        part_num, done / (1024 ** 2)))

                s3.complete_multipart_upload(
                    Bucket=bucket, Key=s3_key,
                    UploadId=upload_id,
                    MultipartUpload={"Parts": parts})
                print("[INFO]   DONE: s3://{}/{}".format(bucket, s3_key))
                uploaded.append(filename)

            except Exception as upload_err:
                print("[ERROR] Upload failed: {}".format(upload_err))
                traceback.print_exc()
                s3.abort_multipart_upload(Bucket=bucket, Key=s3_key, UploadId=upload_id)
                print("[INFO]  Multipart upload aborted.")
                failed.append(filename)

    except Exception as outer_err:
        print("[ERROR] Could not process {}: {}".format(filename, outer_err))
        traceback.print_exc()
        failed.append(filename)

# ── Step 4: Upload zone lookup CSV ────────────────────────────────────────────
print("\n[INFO] Uploading taxi zone lookup CSV...")
print("[INFO]   Source: {}".format(ZONE_URL))
try:
    zr = requests.get(ZONE_URL, timeout=30)
    zr.raise_for_status()
    s3.put_object(Bucket=bucket, Key=ZONE_S3_KEY, Body=zr.content)
    print("[INFO]   DONE: s3://{}/{}  ({:.1f} KB)".format(
        bucket, ZONE_S3_KEY, len(zr.content) / 1024))
    zone_ok = True
except Exception as ze:
    print("[ERROR] Zone lookup upload failed: {}".format(ze))
    traceback.print_exc()
    zone_ok = False

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print("Bucket     : s3://{}".format(bucket))
print("Uploaded   : {}/{}".format(len(uploaded), len(found)))
for f in uploaded:
    print("  [OK]   {}".format(f))
for f in failed:
    print("  [FAIL] {}".format(f))
print("Zone CSV   : {}".format("OK" if zone_ok else "FAILED"))
print("Finished   : {}".format(datetime.now(tz=timezone.utc).isoformat()))
print("=" * 60)
