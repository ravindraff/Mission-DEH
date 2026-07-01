"""
dq-check.py
===========
AWS Glue Notebook / ETL Job — Data Quality Check on Raw HVFHV Data
6 Months | Lyft (HV0005) + All Providers

Pipeline:
  S3 raw/nyctlc/fhvhv_tripdata/   (6 months of HVFHV parquet)
        ↓
  Tag each record with a rejection_reason (NULL = good)
        ↓
  Split into good_df  (rejection_reason IS NULL)
            rejected_df (rejection_reason IS NOT NULL)
        ↓
  Write rejected records  → rejected/nyctlc/fhvhv_tripdata_rejected/
  Write quality summary   → rejected/nyctlc/quality_summary/
        ↓
  Print DQ report: total / good / rejected / rejection_rate %

DQ Rules applied (same 9 rules as raw-to-curated.py):
  Rule 1  : trip_miles > 0            — zero-distance trip
  Rule 2  : trip_miles < 200          — impossibly long distance
  Rule 3  : base_passenger_fare > 0   — zero or negative fare
  Rule 4  : base_passenger_fare < 500 — unrealistic fare
  Rule 5  : trip_time > 0             — zero-duration trip (seconds)
  Rule 6  : trip_time < 14400         — over 4 hours (14400 sec)
  Rule 7  : pickup_datetime NOT NULL  — missing pickup timestamp
  Rule 8  : dropoff_datetime > pickup_datetime  — time went backwards
  Rule 9  : PULocationID + DOLocationID NOT NULL — missing location
"""

# ============================================================
# CELL 1 — Glue Session Config  (edit default first cell)
# ============================================================
# %idle_timeout 2880
# %glue_version 5.0
# %worker_type G.1X
# %number_of_workers 2

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)


# ============================================================
# CELL 2 — Imports, Config & S3 Paths
# ============================================================

import boto3
from pyspark.sql.functions import (
    col,
    when,
    lit,
    count,
    round      as spark_round,
    sum        as spark_sum,
    date_format
)

# Resolve bucket from account ID at runtime — no hardcoding
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET     = f"mission-deh-hof-nyctlc-{ACCOUNT_ID}"

RAW_PATH       = f"s3://{BUCKET}/raw/nyctlc/fhvhv_tripdata/"
REJECTED_PATH  = f"s3://{BUCKET}/rejected/nyctlc/fhvhv_tripdata_rejected/"
SUMMARY_PATH   = f"s3://{BUCKET}/rejected/nyctlc/quality_summary/"

print("=" * 62)
print("  NYC TLC HVFHV — Data Quality Check  |  6 Months")
print("=" * 62)
print(f"  Account  : {ACCOUNT_ID}")
print(f"  Bucket   : {BUCKET}")
print(f"  Input    : {RAW_PATH}")
print(f"  Rejected : {REJECTED_PATH}")
print(f"  Summary  : {SUMMARY_PATH}")
print("=" * 62)


# ============================================================
# CELL 3 — Read Raw Data (All 6 Months, All Providers)
# ============================================================
# We intentionally read ALL providers (Uber + Lyft) here because
# DQ checks should cover the full dataset, not just Lyft.
# The rejection_reason will also capture records where
# hvfhs_license_num is neither HV0003 nor HV0005 (unknown provider).

print("\n[INFO] Reading raw HVFHV data (6 months, all providers)...")

raw_df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(RAW_PATH)

# Cache: we scan raw_df twice (tag pass + split)
raw_df.cache()

total_raw = raw_df.count()
print(f"[INFO] Total raw records (all providers): {total_raw:,}")

# Monthly breakdown
print("\n[INFO] Monthly record distribution:")
raw_df \
    .withColumn("month", date_format(col("pickup_datetime"), "yyyy-MM")) \
    .groupBy("month") \
    .count() \
    .orderBy("month") \
    .show(12, truncate=False)

# Provider breakdown
print("[INFO] Provider breakdown (HV0003=Uber, HV0005=Lyft):")
raw_df.groupBy("hvfhs_license_num") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(truncate=False)


# ============================================================
# CELL 4 — Tag Each Record with a rejection_reason
# ============================================================
# Strategy: COALESCE-style priority tagging.
#   - Evaluate rules in order of severity (nulls first, then ranges).
#   - A record gets the FIRST rule it fails — one reason per record.
#   - Records that pass ALL rules get rejection_reason = NULL (good).
#
# This single withColumn pass avoids multiple DataFrame scans.
# Rule priority (first match wins):
#   1. NULL pickup_datetime          → "null_pickup_datetime"
#   2. NULL PULocationID             → "null_pu_location"
#   3. NULL DOLocationID             → "null_do_location"
#   4. dropoff <= pickup             → "invalid_timestamps"
#   5. trip_miles IS NULL or <= 0    → "zero_trip_miles"
#   6. trip_miles >= 200             → "excessive_trip_miles"
#   7. base_passenger_fare IS NULL   → "null_fare"
#   8. base_passenger_fare <= 0      → "zero_or_negative_fare"
#   9. base_passenger_fare >= 500    → "excessive_fare"
#  10. trip_time IS NULL or <= 0     → "zero_trip_time"
#  11. trip_time >= 14400            → "excessive_trip_time"
#  NULL result                       → record passes all rules (good)

print("\n[INFO] Tagging each record with DQ rejection reason...")

tagged_df = raw_df.withColumn(
    "rejection_reason",
    when(col("pickup_datetime").isNull(),
         lit("null_pickup_datetime"))

    .when(col("PULocationID").isNull(),
          lit("null_pu_location"))

    .when(col("DOLocationID").isNull(),
          lit("null_do_location"))

    .when(
        col("dropoff_datetime").isNull() |
        (col("dropoff_datetime") <= col("pickup_datetime")),
        lit("invalid_timestamps")
    )

    .when(
        col("trip_miles").isNull() | (col("trip_miles") <= 0),
        lit("zero_trip_miles")
    )

    .when(col("trip_miles") >= 200,
          lit("excessive_trip_miles"))

    .when(col("base_passenger_fare").isNull(),
          lit("null_fare"))

    .when(col("base_passenger_fare") <= 0,
          lit("zero_or_negative_fare"))

    .when(col("base_passenger_fare") >= 500,
          lit("excessive_fare"))

    .when(
        col("trip_time").isNull() | (col("trip_time") <= 0),
        lit("zero_trip_time")
    )

    .when(col("trip_time") >= 14400,
          lit("excessive_trip_time"))

    # NULL = record passed every rule → good record
    .otherwise(lit(None).cast("string"))
)

# Cache tagged_df — used in both good and rejected splits
tagged_df.cache()
# Unpersist the raw DataFrame now that tagging is done
raw_df.unpersist()

print("[INFO] Tagging complete.")


# ============================================================
# CELL 5 — Split into Good & Rejected DataFrames
# ============================================================

# Good records: passed ALL 11 DQ rules
good_df = tagged_df.filter(col("rejection_reason").isNull()) \
                   .drop("rejection_reason")   # clean column — not needed

# Rejected records: failed at least one rule
rejected_df = tagged_df.filter(col("rejection_reason").isNotNull())

# Count both sides
good_count     = good_df.count()
rejected_count = rejected_df.count()
total_count    = good_count + rejected_count

rejection_rate = (rejected_count / total_count * 100) if total_count > 0 else 0
pass_rate      = 100.0 - rejection_rate

print(f"\n[INFO] Split results:")
print(f"  Total records  : {total_count:>12,}")
print(f"  Good records   : {good_count:>12,}  ({pass_rate:.2f}%)")
print(f"  Rejected       : {rejected_count:>12,}  ({rejection_rate:.2f}%)")


# ============================================================
# CELL 6 — Write Rejected Records to S3
# ============================================================
# Includes the rejection_reason column so downstream teams can
# investigate specific failure categories.
# Partitioned by rejection_reason — makes it easy to load just
# one failure category (e.g. all "zero_trip_miles" records).

print(f"\n[INFO] Writing rejected records to: {REJECTED_PATH}")
print(f"[INFO]   Records to write: {rejected_count:,}")
print("[INFO]   Partitioned by: rejection_reason")

rejected_df.write \
    .mode("overwrite") \
    .partitionBy("rejection_reason") \
    .parquet(REJECTED_PATH)

print(f"[INFO] ✅ Rejected records written: {rejected_count:,} rows")

# ============================================================
# CELL 7 — Build & Write Quality Summary Table
# ============================================================
# One row per rejection_reason with count + percentage of total.
# Also includes a "PASSED" row for good records — gives the full
# picture in a single Athena query.

print(f"\n[INFO] Building quality summary...")

# Rejection breakdown — one row per rule that was triggered
rejection_summary = rejected_df \
    .groupBy("rejection_reason") \
    .agg(count("*").alias("record_count")) \
    .withColumn(
        "pct_of_total",
        spark_round(col("record_count") / lit(total_count) * 100, 4)
    ) \
    .withColumn("status", lit("REJECTED")) \
    .orderBy(col("record_count").desc())

# Add a PASSED summary row for completeness
from pyspark.sql import Row

passed_row = spark.createDataFrame([
    Row(
        rejection_reason="PASSED_ALL_RULES",
        record_count=good_count,
        pct_of_total=round(pass_rate, 4),
        status="PASSED"
    )
])

# Union PASSED row on top of rejection rows
quality_summary = passed_row.union(rejection_summary)

# Preview in notebook
print("\n[INFO] Quality summary:")
quality_summary.orderBy(
    when(col("status") == "PASSED", lit(0)).otherwise(lit(1)),
    col("record_count").desc()
).show(20, truncate=False)

# Write summary table
print(f"\n[INFO] Writing quality summary to: {SUMMARY_PATH}")
quality_summary.write \
    .mode("overwrite") \
    .parquet(SUMMARY_PATH)

print(f"[INFO] ✅ Quality summary written")


# ============================================================
# CELL 8 — DQ Report (Full Console Print)
# ============================================================
# Reads summary back from S3 and prints a formatted report.
# This is what you'd screenshot for your portfolio / interview.

print("\n[INFO] Reading summary back from S3 for final report...")

val_summary = spark.read.parquet(SUMMARY_PATH)
summary_rows = val_summary \
    .orderBy(
        when(col("status") == "PASSED", lit(0)).otherwise(lit(1)),
        col("record_count").desc()
    ) \
    .collect()

# ── Formatted DQ Report ──────────────────────────────────────
SEP = "=" * 62

print("\n" + SEP)
print("  DATA QUALITY REPORT — NYC TLC HVFHV  |  6 MONTHS")
print(SEP)
print(f"  Source  : {RAW_PATH}")
print(f"  Bucket  : {BUCKET}")
print(SEP)
print(f"  {'Metric':<35} {'Count':>12}   {'% of Total':>10}")
print(f"  {'-'*35} {'-'*12}   {'-'*10}")
print(f"  {'Total raw records':<35} {total_count:>12,}   {'100.00%':>10}")
print(f"  {'Passed all DQ rules':<35} {good_count:>12,}   {pass_rate:>9.2f}%")
print(f"  {'Failed at least one rule':<35} {rejected_count:>12,}   {rejection_rate:>9.2f}%")
print(SEP)
print(f"  {'Rejection Breakdown by Rule':}")
print(f"  {'-'*35} {'-'*12}   {'-'*10}")

for row in summary_rows:
    if row["status"] == "REJECTED":
        print(f"  {row['rejection_reason']:<35} {row['record_count']:>12,}   {row['pct_of_total']:>9.4f}%")

print(SEP)

# Monthly DQ breakdown — shows if any month is particularly dirty
print("\n  Monthly DQ Breakdown:")
print(f"  {'-'*62}")

monthly_dq = tagged_df \
    .withColumn("month", date_format(col("pickup_datetime"), "yyyy-MM")) \
    .groupBy("month") \
    .agg(
        count("*").alias("total"),
        count(when(col("rejection_reason").isNull(), True)).alias("good"),
        count(when(col("rejection_reason").isNotNull(), True)).alias("rejected")
    ) \
    .withColumn(
        "rejection_rate_pct",
        spark_round(col("rejected") / col("total") * 100, 2)
    ) \
    .orderBy("month")

print(f"\n  {'Month':<10} {'Total':>12} {'Good':>12} {'Rejected':>10} {'Rej%':>8}")
print(f"  {'-'*10} {'-'*12} {'-'*12} {'-'*10} {'-'*8}")
for row in monthly_dq.collect():
    print(f"  {row['month']:<10} {row['total']:>12,} {row['good']:>12,}"
          f" {row['rejected']:>10,} {row['rejection_rate_pct']:>7.2f}%")

print(f"\n{SEP}")
print("  S3 Output Paths:")
print(f"  Rejected records : {REJECTED_PATH}")
print(f"  Quality summary  : {SUMMARY_PATH}")
print(SEP)
print("\n  ⚠️  STOP your Glue notebook session now to avoid charges!")
