"""
raw-to-curated.py
=================
AWS Glue Notebook / ETL Job — Raw → Curated Transformation
Medallion Architecture: Bronze (raw) → Silver (curated)

Pipeline:
  S3 raw/nyctlc/fhvhv_tripdata/        (6 months of HVFHV parquet)
  S3 raw/nyctlc/taxi_zone_lookup/      (zone lookup CSV)
        ↓
  Filter Lyft only (HV0005)
  Apply Data Quality filters (9 rules)
  Add Derived columns (8 new fields)
  Join Zone lookup (pickup + dropoff)
  Select final curated schema
        ↓
  S3 curated/nyctlc/fhvhv_trips_curated/   (partitioned by pickup_date)
"""

# ============================================================
# CELL 1 — Glue Session Config (Edit default cell, keep as-is)
# ============================================================
# Paste this into the FIRST cell of your Glue notebook.
# Change %number_of_workers from 5 → 2 to save cost (~$0.44/DPU-hr).

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
# CELL 2 — Imports & S3 Bucket Setup
# ============================================================
# Resolve the S3 bucket name dynamically from the AWS Account ID.
# No hardcoded values — works on any AWS account.

import boto3
from pyspark.sql.functions import (
    col,
    hour,
    dayofweek,
    to_date,
    unix_timestamp,
    round as spark_round,
    when,
    lit
)

# Resolve account ID at runtime — bucket name is account-specific
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET     = f"mission-deh-hof-nyctlc-{ACCOUNT_ID}"

RAW_TRIP_PATH    = f"s3://{BUCKET}/raw/nyctlc/fhvhv_tripdata/"
RAW_ZONE_PATH    = f"s3://{BUCKET}/raw/nyctlc/taxi_zone_lookup/"
CURATED_PATH     = f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/"

# HV0005 = Lyft  |  HV0003 = Uber
LYFT_LICENSE = "HV0005"

print(f"Account  : {ACCOUNT_ID}")
print(f"Bucket   : {BUCKET}")
print(f"Raw path : {RAW_TRIP_PATH}")
print(f"Output   : {CURATED_PATH}")
print("Setup complete ✅")


# ============================================================
# CELL 3 — Read Raw Data & Filter Lyft Only
# ============================================================
# Reads ALL 6 months of HVFHV parquet in one shot (Spark merges
# schemas automatically) then immediately narrows to Lyft trips.
# HV0005 = Lyft, HV0003 = Uber.

# Read all 6 months of raw HVFHV trip data
trip_df = spark.read \
    .option("mergeSchema", "true") \
    .parquet(RAW_TRIP_PATH)

total_raw = trip_df.count()
print(f"Total raw HVFHV records (all providers, 6 months): {total_raw:,}")

# Filter Lyft only — HV0005
trip_df = trip_df.filter(col("hvfhs_license_num") == LYFT_LICENSE)
lyft_count = trip_df.count()
print(f"Lyft-only records (HV0005): {lyft_count:,}")
print(f"Lyft share of raw data    : {lyft_count / total_raw * 100:.1f}%")

# Read zone lookup CSV (small reference table — ~265 rows)
zone_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(RAW_ZONE_PATH)

zone_count = zone_df.count()
print(f"Zone lookup rows          : {zone_count}")

# Preview raw schema
print("\n--- Raw Trip Schema ---")
trip_df.printSchema()


# ============================================================
# CELL 4 — Data Quality Filters (9 Rules)
# ============================================================
# Each filter removes a specific category of bad records.
# We count before & after each rule so you can see the impact.
#
# Rule  1: trip_miles > 0          — no zero-distance trips
# Rule  2: trip_miles < 200        — NYC is ~35mi across; 200mi is impossible
# Rule  3: base_passenger_fare > 0 — no free/negative fares
# Rule  4: base_passenger_fare < 500 — no unrealistic fares
# Rule  5: trip_time > 0           — no zero-duration trips (in seconds)
# Rule  6: trip_time < 14400       — max 4 hours = 4 * 60 * 60 seconds
# Rule  7: pickup_datetime NOT NULL — every trip needs a pickup time
# Rule  8: dropoff_datetime > pickup_datetime — time must move forward
# Rule  9: PU/DOLocationID NOT NULL — must have pickup and dropoff zones

before_dq = trip_df.count()
print(f"Records before DQ filters: {before_dq:,}")

trip_df = trip_df.filter(
    # Rule 1 & 2 — distance sanity
    (col("trip_miles") > 0) &
    (col("trip_miles") < 200) &

    # Rule 3 & 4 — fare sanity
    (col("base_passenger_fare") > 0) &
    (col("base_passenger_fare") < 500) &

    # Rule 5 & 6 — duration sanity (seconds)
    (col("trip_time") > 0) &
    (col("trip_time") < 14400) &

    # Rule 7 & 8 — timestamp validity
    (col("pickup_datetime").isNotNull()) &
    (col("dropoff_datetime") > col("pickup_datetime")) &

    # Rule 9 — location IDs must exist
    (col("PULocationID").isNotNull()) &
    (col("DOLocationID").isNotNull())
)

after_dq = trip_df.count()
removed  = before_dq - after_dq
print(f"Records after DQ filters : {after_dq:,}")
print(f"Records removed by DQ    : {removed:,}  ({removed / before_dq * 100:.2f}%)")


# ============================================================
# CELL 5 — Add Derived Columns (8 New Fields)
# ============================================================
# These calculated fields make downstream analysis much easier
# in Athena — analysts can GROUP BY pickup_hour, filter is_weekend,
# or bucket speed_mph without re-deriving the logic every time.
#
# New columns:
#   trip_duration_minutes  — trip_time (seconds) ÷ 60
#   pickup_date            — DATE portion of pickup_datetime
#   pickup_hour            — 0-23 hour of pickup (for hourly patterns)
#   pickup_day_of_week     — 1=Sunday ... 7=Saturday (Spark convention)
#   is_weekend             — 1 if Saturday(7) or Sunday(1), else 0
#   speed_mph              — trip_miles ÷ (trip_duration_minutes ÷ 60)
#   is_shared_ride         — 1 if shared_match_flag='Y', else 0
#   wait_time_minutes      — minutes from request_datetime → pickup_datetime

trip_df = trip_df \
    .withColumn(
        "trip_duration_minutes",
        spark_round(col("trip_time") / 60.0, 2)
    ) \
    .withColumn(
        "pickup_date",
        to_date(col("pickup_datetime"))
    ) \
    .withColumn(
        "pickup_hour",
        hour(col("pickup_datetime"))
    ) \
    .withColumn(
        "pickup_day_of_week",
        dayofweek(col("pickup_datetime"))          # 1=Sun, 7=Sat
    ) \
    .withColumn(
        "is_weekend",
        when(
            dayofweek(col("pickup_datetime")).isin(1, 7), lit(1)
        ).otherwise(lit(0))
    ) \
    .withColumn(
        "speed_mph",
        spark_round(
            when(
                col("trip_duration_minutes") > 0,
                col("trip_miles") / (col("trip_duration_minutes") / 60.0)
            ),
            2
        )
    ) \
    .withColumn(
        "is_shared_ride",
        when(col("shared_match_flag") == "Y", lit(1)).otherwise(lit(0))
    ) \
    .withColumn(
        "wait_time_minutes",
        spark_round(
            when(
                col("request_datetime").isNotNull(),
                (unix_timestamp(col("pickup_datetime"))
                 - unix_timestamp(col("request_datetime"))) / 60.0
            ),
            2
        )
    )

# ── Post-derivation quality filter ───────────────────────────
# Speed > 80 mph in NYC city traffic = GPS error or data corruption.
# Duration < 1 min or > 240 min = edge cases we don't trust.
before_speed = trip_df.count()

trip_df = trip_df.filter(
    (col("speed_mph").isNull()       | (col("speed_mph") <= 80)) &
    (col("trip_duration_minutes")   >= 1) &
    (col("trip_duration_minutes")   <= 240)
)

after_speed = trip_df.count()
print(f"After derived columns + speed filter: {after_speed:,} records")
print(f"Removed by speed/duration filter    : {before_speed - after_speed:,}")

# Quick sanity check on the new columns
print("\n--- Sample of Derived Columns ---")
trip_df.select(
    "pickup_datetime", "trip_duration_minutes", "pickup_hour",
    "pickup_day_of_week", "is_weekend", "speed_mph",
    "is_shared_ride", "wait_time_minutes"
).show(5, truncate=False)


# ============================================================
# CELL 6 — Join Zone Lookup (Pickup + Dropoff)
# ============================================================
# Enrich every trip with human-readable borough and zone names
# for both the pickup and dropoff locations.
#
# We do TWO left joins on the same zone_df:
#   Join 1:  PULocationID → pickup_borough, pickup_zone, pickup_service_zone
#   Join 2:  DOLocationID → dropoff_borough, dropoff_zone, dropoff_service_zone
#
# "left" join preserves all trip records even if a location ID
# has no match in the zone table (rare edge-case zones like 264/265).

# Prepare pickup zone reference (rename cols to avoid ambiguity)
pickup_zone_df = zone_df.select(
    col("LocationID").cast("int").alias("PULocationID"),
    col("Borough").alias("pickup_borough"),
    col("Zone").alias("pickup_zone"),
    col("service_zone").alias("pickup_service_zone")
)

# Prepare dropoff zone reference
dropoff_zone_df = zone_df.select(
    col("LocationID").cast("int").alias("DOLocationID"),
    col("Borough").alias("dropoff_borough"),
    col("Zone").alias("dropoff_zone"),
    col("service_zone").alias("dropoff_service_zone")
)

# Join pickup zones
trip_df = trip_df.join(pickup_zone_df, on="PULocationID", how="left")

# Join dropoff zones
trip_df = trip_df.join(dropoff_zone_df, on="DOLocationID", how="left")

print("Zone enrichment complete ✅")
print(f"Total columns after join: {len(trip_df.columns)}")

# Preview joined location data
print("\n--- Sample Location Enrichment ---")
trip_df.select(
    "PULocationID", "pickup_borough", "pickup_zone",
    "DOLocationID", "dropoff_borough", "dropoff_zone"
).show(5, truncate=False)


# ============================================================
# CELL 7 — Select Final Curated Columns
# ============================================================
# Drop raw columns we no longer need (hvfhs_license_num is always
# HV0005 at this point; trip_time is kept for completeness alongside
# trip_duration_minutes).
#
# Final schema is grouped by category for readability:
#   Time (7)          — datetimes, date, hour, day, weekend flag
#   Trip (4)          — miles, time, duration, speed
#   Sharing & Wait(2) — shared flag, wait time
#   Pickup Loc (4)    — ID, borough, zone, service zone
#   Dropoff Loc (4)   — ID, borough, zone, service zone
#   Fare (8)          — all fare components
#   Metadata (7)      — base nums, flag columns
# Total: 36 columns

curated_df = trip_df.select(

    # ── Time ──────────────────────────────────────────────────
    "request_datetime",          # when the ride was booked
    "pickup_datetime",           # actual pickup timestamp
    "dropoff_datetime",          # actual dropoff timestamp
    "pickup_date",               # DATE(pickup_datetime) — used as partition key
    "pickup_hour",               # 0–23, for hourly pattern analysis
    "pickup_day_of_week",        # 1=Sun … 7=Sat
    "is_weekend",                # 1 if Sat/Sun, 0 otherwise

    # ── Trip ──────────────────────────────────────────────────
    "trip_miles",                # distance in miles
    "trip_time",                 # duration in seconds (raw)
    "trip_duration_minutes",     # duration in minutes (derived)
    "speed_mph",                 # average speed (derived)

    # ── Sharing & Wait ────────────────────────────────────────
    "is_shared_ride",            # 1 if matched as shared ride
    "wait_time_minutes",         # minutes from request to pickup

    # ── Pickup Location ───────────────────────────────────────
    "PULocationID",              # TLC zone ID
    "pickup_borough",            # e.g. Manhattan, Brooklyn
    "pickup_zone",               # e.g. Upper East Side North
    "pickup_service_zone",       # Yellow Zone, Boro Zone, Airports

    # ── Dropoff Location ──────────────────────────────────────
    "DOLocationID",              # TLC zone ID
    "dropoff_borough",
    "dropoff_zone",
    "dropoff_service_zone",

    # ── Fare Components ───────────────────────────────────────
    "base_passenger_fare",       # base fare before extras
    "tolls",                     # toll charges
    "bcf",                       # Black Car Fund surcharge
    "sales_tax",                 # NY state sales tax
    "congestion_surcharge",      # congestion pricing surcharge
    "airport_fee",               # JFK/LGA airport fee
    "tips",                      # tip (credit card only)
    "driver_pay",                # amount paid to driver

    # ── Metadata ──────────────────────────────────────────────
    "dispatching_base_num",      # TLC base that dispatched trip
    "originating_base_num",      # TLC base of the vehicle
    "shared_request_flag",       # Y/N — rider requested shared
    "shared_match_flag",         # Y/N — shared match found
    "access_a_ride_flag",        # Y/N — Access-A-Ride program
    "wav_request_flag",          # Y/N — wheelchair accessible requested
    "wav_match_flag"             # Y/N — wheelchair accessible matched
)

print(f"Curated schema: {len(curated_df.columns)} columns")
print(f"Curated records: {curated_df.count():,}")
print("\n--- Curated Schema ---")
curated_df.printSchema()


# ============================================================
# CELL 8 — Write to Curated Layer (Partitioned by pickup_date)
# ============================================================
# Writes Parquet partitioned by pickup_date.
# Partitioning means Athena only scans the date partitions that
# match a WHERE clause — critical for query efficiency on 6 months
# of data (~180 daily partitions × several GB total).
#
# mode("overwrite") = safe to re-run; replaces previous output.
# This rewrites ALL partitions. If you only want to update specific
# months, use mode("overwrite") with partitionOverwriteMode dynamic.

print(f"Writing curated data to: {CURATED_PATH}")
print("Partitioned by: pickup_date")
print("This may take 5–10 minutes for 6 months of data...")

curated_df.write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet(CURATED_PATH)

print(f"\n✅ Write complete: {CURATED_PATH}")


# ============================================================
# CELL 9 — Validation & Summary
# ============================================================
# Read back the curated data to confirm it was written correctly.
# Checks:
#   - Total record count
#   - Number of distinct date partitions (should be ~180 for 6 months)
#   - Borough distribution (sanity check on zone join)
#   - Shared vs non-shared ride breakdown
#   - Fare statistics (avg, min, max)

print("Reading back curated data for validation...")
val_df = spark.read.parquet(CURATED_PATH)

total_curated = val_df.count()
date_partitions = val_df.select("pickup_date").distinct().count()

print("\n" + "=" * 52)
print("  TRANSFORMATION SUMMARY")
print("=" * 52)
print(f"  Raw records (all providers) : {total_raw:,}")
print(f"  After Lyft filter           : {lyft_count:,}")
print(f"  After DQ + speed filters    : {total_curated:,}")
print(f"  Retention rate              : {total_curated / total_raw * 100:.1f}%")
print(f"  Date partitions written     : {date_partitions}")
print(f"  Columns in curated schema   : {len(val_df.columns)}")
print("=" * 52)

# Borough distribution
print("\n--- Pickup Borough Distribution ---")
val_df.groupBy("pickup_borough") \
    .count() \
    .orderBy("count", ascending=False) \
    .show(10, truncate=False)

# Shared ride breakdown
print("--- Shared vs Solo Rides ---")
val_df.groupBy("is_shared_ride") \
    .count() \
    .withColumnRenamed("is_shared_ride", "shared") \
    .orderBy("shared") \
    .show()

# Fare statistics
print("--- Fare Statistics (base_passenger_fare) ---")
val_df.selectExpr(
    "round(avg(base_passenger_fare), 2) as avg_fare",
    "round(min(base_passenger_fare), 2) as min_fare",
    "round(max(base_passenger_fare), 2) as max_fare",
    "round(avg(trip_miles), 2)          as avg_miles",
    "round(avg(trip_duration_minutes), 2) as avg_duration_min",
    "round(avg(wait_time_minutes), 2)   as avg_wait_min"
).show(truncate=False)

# Monthly breakdown (useful for 6-month view)
print("--- Monthly Trip Counts ---")
val_df.selectExpr("date_format(pickup_date, 'yyyy-MM') as month") \
    .groupBy("month") \
    .count() \
    .orderBy("month") \
    .show(12, truncate=False)

print("\n✅ Validation complete. Raw → Curated pipeline finished.")
print("⚠️  STOP your notebook session now to avoid extra charges!")
