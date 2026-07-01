"""
curated-to-aggregated.py
=========================
AWS Glue Notebook / ETL Job — Curated → Aggregated Layer
Medallion Architecture: Silver (curated) → Gold (aggregated)

Reference: DEH_Mission2_Pipeline_Mapping.xlsx
           bootcamp-mission2-part4-aggregation.html

Reads ALL 6 months of curated Lyft data and builds 3 aggregate tables:

  Table 1 — daily_borough_summary
    GROUP BY: pickup_date, pickup_borough
    Metrics:  total_trips, total_revenue, avg_fare, avg_tip,
              avg_trip_miles, avg_trip_duration_minutes, avg_speed_mph
    Partition: pickup_date   (~180 daily partitions across 6 months)

  Table 2 — hourly_pattern_summary
    GROUP BY: pickup_hour, is_weekend, pickup_borough, is_shared_ride
    Metrics:  total_trips, avg_fare, avg_trip_miles, avg_tip_percentage,
              avg_speed_mph, avg_wait_time_minutes
    No partition (small lookup table — 24h × 2 × 6 boroughs × 2 = ~576 rows)

  Table 3 — route_summary
    GROUP BY: pickup_borough, pickup_zone, dropoff_borough, dropoff_zone
    Metrics:  total_trips, avg_fare, avg_trip_miles, avg_trip_duration_minutes
    Filter:   total_trips >= 10  (removes noise routes)
    No partition (pre-aggregated — fast for dashboards)

Output root: s3://mission-deh-hof-nyctlc-<account_id>/aggregated/nyctlc/
"""

# ============================================================
# CELL 1 — Glue Session Config  (edit the DEFAULT first cell)
# ============================================================
# In the Glue Notebook, this IS the first cell.
# Change %number_of_workers 5  →  2  to save cost.
# Cost: ~$0.44 / DPU-hour  |  2 workers × G.1X = 2 DPUs

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
# All S3 paths derived from the AWS Account ID at runtime.
# No hardcoded values — works on any account.

import boto3
from pyspark.sql.functions import (
    col,
    count,
    sum        as spark_sum,
    avg        as spark_avg,
    round      as spark_round,
    min        as spark_min,
    max        as spark_max,
    when,
    lit,
    date_format
)

# ── Resolve account + bucket at runtime ──────────────────────
ACCOUNT_ID = boto3.client("sts").get_caller_identity()["Account"]
BUCKET     = f"mission-deh-hof-nyctlc-{ACCOUNT_ID}"

# ── S3 paths ─────────────────────────────────────────────────
CURATED_PATH   = f"s3://{BUCKET}/curated/nyctlc/fhvhv_trips_curated/"
AGG_ROOT       = f"s3://{BUCKET}/aggregated/nyctlc"
OUT_DAILY      = f"{AGG_ROOT}/daily_borough_summary/"
OUT_HOURLY     = f"{AGG_ROOT}/hourly_pattern_summary/"
OUT_ROUTE      = f"{AGG_ROOT}/route_summary/"

# Minimum trip count for route_summary (removes noise routes)
ROUTE_MIN_TRIPS = 10

print("=" * 60)
print("  CURATED → AGGREGATED  |  6 Months  |  Glue PySpark")
print("=" * 60)
print(f"  Account  : {ACCOUNT_ID}")
print(f"  Bucket   : {BUCKET}")
print(f"  Input    : {CURATED_PATH}")
print(f"  Output   : {AGG_ROOT}/")
print("=" * 60)


# ============================================================
# CELL 3 — Read All 6 Months of Curated Data
# ============================================================
# Spark reads all partitions (pickup_date=YYYY-MM-DD/) in one call.
# The curated layer is already filtered to Lyft-only (HV0005),
# DQ-validated, and enriched with zone names — ready to aggregate.

print("\n[INFO] Reading curated data (all 6 months)...")
curated_df = spark.read.parquet(CURATED_PATH)

# Cache because all 3 aggregations will scan this same DataFrame.
# For 6 months (~300-500M rows after DQ filtering), caching avoids
# re-reading the parquet files from S3 three separate times.
curated_df.cache()

total_records = curated_df.count()
print(f"[INFO] Total curated records (6 months, Lyft only): {total_records:,}")

# Show the monthly breakdown so we can verify all 6 months loaded
print("\n[INFO] Monthly record distribution:")
curated_df \
    .withColumn("month", date_format(col("pickup_date"), "yyyy-MM")) \
    .groupBy("month") \
    .count() \
    .orderBy("month") \
    .show(12, truncate=False)

# Quick schema reminder
print(f"[INFO] Columns available: {len(curated_df.columns)}")
curated_df.printSchema()


# ============================================================
# CELL 4 — Aggregate 1: Daily Borough Summary
# ============================================================
# Purpose: Daily executive KPIs per NYC borough.
#          One row per (date × borough) combination.
#          Partitioned by pickup_date for fast Athena date-range scans.
#
# Column mapping (from DEH_Mission2_Pipeline_Mapping.xlsx):
#   pickup_date              → GROUP BY (partition key)
#   pickup_borough           → GROUP BY
#   total_trips              → COUNT(*)
#   total_revenue            → SUM(base_passenger_fare)  rounded 2dp
#   avg_fare                 → AVG(base_passenger_fare)  rounded 2dp
#   avg_tip                  → AVG(tips)                 rounded 2dp
#   avg_trip_miles           → AVG(trip_miles)           rounded 2dp
#   avg_trip_duration_minutes→ AVG(trip_duration_minutes)rounded 2dp
#   avg_speed_mph            → AVG(speed_mph)            rounded 2dp
#
# With 6 months × ~6 boroughs × ~180 days ≈ ~1,080 rows total.

print("\n[INFO] Building Aggregate 1: Daily Borough Summary...")

daily_borough = curated_df.groupBy(
    "pickup_date",
    "pickup_borough"
).agg(
    count("*")                                         .alias("total_trips"),
    spark_round(spark_sum("base_passenger_fare"), 2)   .alias("total_revenue"),
    spark_round(spark_avg("base_passenger_fare"), 2)   .alias("avg_fare"),
    spark_round(spark_avg("tips"), 2)                  .alias("avg_tip"),
    spark_round(spark_avg("trip_miles"), 2)            .alias("avg_trip_miles"),
    spark_round(spark_avg("trip_duration_minutes"), 2) .alias("avg_trip_duration_minutes"),
    spark_round(spark_avg("speed_mph"), 2)             .alias("avg_speed_mph")
).orderBy("pickup_date", "pickup_borough")

daily_row_count = daily_borough.count()
print(f"[INFO] daily_borough_summary rows: {daily_row_count:,}")

# Preview top 10 by revenue
print("\n[INFO] Preview — Top 10 rows by total_revenue:")
daily_borough.orderBy(col("total_revenue").desc()).show(10, truncate=False)

# Write partitioned by pickup_date
# Overwrite all partitions — safe to re-run
print(f"\n[INFO] Writing to: {OUT_DAILY}")
daily_borough.write \
    .mode("overwrite") \
    .partitionBy("pickup_date") \
    .parquet(OUT_DAILY)

print(f"[INFO] ✅ daily_borough_summary written  ({daily_row_count:,} rows)")


# ============================================================
# CELL 5 — Aggregate 2: Hourly Pattern Summary
# ============================================================
# Purpose: Understand Lyft demand patterns by time of day,
#          weekday vs weekend, borough, and shared vs solo ride.
#          Used for surge pricing analysis, driver allocation,
#          and operational planning dashboards.
#
# Column mapping (from DEH_Mission2_Pipeline_Mapping.xlsx):
#   pickup_hour          → GROUP BY  (0–23)
#   is_weekend           → GROUP BY  (0=weekday, 1=weekend)
#   pickup_borough       → GROUP BY
#   is_shared_ride       → GROUP BY  (0=solo, 1=shared match)
#   total_trips          → COUNT(*)
#   avg_fare             → AVG(base_passenger_fare)        rounded 2dp
#   avg_trip_miles       → AVG(trip_miles)                 rounded 2dp
#   avg_tip_percentage   → AVG(tips / base_passenger_fare * 100)
#                          WHERE base_passenger_fare > 0   rounded 2dp
#   avg_speed_mph        → AVG(speed_mph)                  rounded 2dp
#   avg_wait_time_minutes→ AVG(wait_time_minutes)          rounded 2dp
#
# Max possible rows: 24h × 2 (weekend) × 6 boroughs × 2 (shared) = 576
# Aggregated across ALL 6 months — bigger sample = more stable averages.

print("\n[INFO] Building Aggregate 2: Hourly Pattern Summary...")

hourly_pattern = curated_df.groupBy(
    "pickup_hour",
    "is_weekend",
    "pickup_borough",
    "is_shared_ride"
).agg(
    count("*")                                                        .alias("total_trips"),
    spark_round(spark_avg("base_passenger_fare"), 2)                  .alias("avg_fare"),
    spark_round(spark_avg("trip_miles"), 2)                           .alias("avg_trip_miles"),
    spark_round(
        spark_avg(
            when(
                col("base_passenger_fare") > 0,
                col("tips") / col("base_passenger_fare") * 100.0
            )
        ), 2
    )                                                                 .alias("avg_tip_percentage"),
    spark_round(spark_avg("speed_mph"), 2)                            .alias("avg_speed_mph"),
    spark_round(spark_avg("wait_time_minutes"), 2)                    .alias("avg_wait_time_minutes")
).orderBy("pickup_borough", "is_weekend", "pickup_hour", "is_shared_ride")

hourly_row_count = hourly_pattern.count()
print(f"[INFO] hourly_pattern_summary rows: {hourly_row_count:,}")

# Preview — Manhattan rush hour comparison
print("\n[INFO] Preview — Manhattan rush hours (shared vs solo):")
hourly_pattern.filter(
    (col("pickup_borough") == "Manhattan") & (col("is_weekend") == 0)
).orderBy("pickup_hour").show(24, truncate=False)

# Write — no partition (small table, fast to scan in full)
print(f"\n[INFO] Writing to: {OUT_HOURLY}")
hourly_pattern.write \
    .mode("overwrite") \
    .parquet(OUT_HOURLY)

print(f"[INFO] ✅ hourly_pattern_summary written  ({hourly_row_count:,} rows)")


# ============================================================
# CELL 6 — Aggregate 3: Route Summary
# ============================================================
# Purpose: Identify the most popular pickup→dropoff zone pairs.
#          Aggregated across all 6 months for statistical stability.
#          Filter: only routes with ≥10 trips total (removes
#          one-off GPS errors and data noise routes).
#          Used by: pricing teams, driver supply planning,
#                   zone-level demand forecasting dashboards.
#
# Column mapping (from DEH_Mission2_Pipeline_Mapping.xlsx):
#   pickup_borough           → GROUP BY
#   pickup_zone              → GROUP BY
#   dropoff_borough          → GROUP BY
#   dropoff_zone             → GROUP BY
#   total_trips              → COUNT(*)
#   avg_fare                 → AVG(base_passenger_fare)    rounded 2dp
#   avg_trip_miles           → AVG(trip_miles)             rounded 2dp
#   avg_trip_duration_minutes→ AVG(trip_duration_minutes)  rounded 2dp
#
# Note: With 6 months of data the route table will be much richer
# than with 1 month — more routes will cross the ≥10 threshold,
# giving better coverage of long-tail zone pairs.

print("\n[INFO] Building Aggregate 3: Route Summary...")

route_summary = curated_df.groupBy(
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone"
).agg(
    count("*")                                         .alias("total_trips"),
    spark_round(spark_avg("base_passenger_fare"), 2)   .alias("avg_fare"),
    spark_round(spark_avg("trip_miles"), 2)            .alias("avg_trip_miles"),
    spark_round(spark_avg("trip_duration_minutes"), 2) .alias("avg_trip_duration_minutes")
).filter(
    col("total_trips") >= ROUTE_MIN_TRIPS   # remove noise routes
).orderBy(
    col("total_trips").desc()               # busiest routes first
)

route_row_count = route_summary.count()
print(f"[INFO] route_summary rows (trips >= {ROUTE_MIN_TRIPS}): {route_row_count:,}")

# Preview top 20 busiest routes
print("\n[INFO] Preview — Top 20 busiest routes (6-month aggregate):")
route_summary.show(20, truncate=False)

# Write — no partition (pre-aggregated, already a small table)
print(f"\n[INFO] Writing to: {OUT_ROUTE}")
route_summary.write \
    .mode("overwrite") \
    .parquet(OUT_ROUTE)

print(f"[INFO] ✅ route_summary written  ({route_row_count:,} rows)")


# ============================================================
# CELL 7 — Validation: Read Back All 3 Tables
# ============================================================
# Reads each aggregated table back from S3 and confirms:
#   - Row counts match what was written
#   - Schemas look correct
#   - Spot-checks on key metrics

print("\n[INFO] Validating all 3 aggregated tables...")

# ── Table 1: Daily Borough Summary ───────────────────────────
val_daily = spark.read.parquet(OUT_DAILY)
val_daily_count = val_daily.count()

print("\n--- Table 1: daily_borough_summary ---")
print(f"  Rows       : {val_daily_count:,}")
print(f"  Columns    : {len(val_daily.columns)}")
print(f"  Date range :")
val_daily.selectExpr(
    "min(pickup_date) as earliest_date",
    "max(pickup_date) as latest_date",
    "count(distinct pickup_date) as distinct_dates",
    "count(distinct pickup_borough) as distinct_boroughs"
).show(truncate=False)

print("  Top 5 boroughs by total revenue (6-month sum):")
val_daily.groupBy("pickup_borough") \
    .agg(
        spark_round(spark_sum("total_revenue"), 2).alias("total_6m_revenue"),
        spark_sum("total_trips").alias("total_6m_trips")
    ) \
    .orderBy(col("total_6m_revenue").desc()) \
    .show(6, truncate=False)

# ── Table 2: Hourly Pattern Summary ──────────────────────────
val_hourly = spark.read.parquet(OUT_HOURLY)
val_hourly_count = val_hourly.count()

print("\n--- Table 2: hourly_pattern_summary ---")
print(f"  Rows       : {val_hourly_count:,}")
print(f"  Columns    : {len(val_hourly.columns)}")
print("  Shared vs Solo — avg fare & wait (all boroughs):")
val_hourly.groupBy("is_shared_ride") \
    .agg(
        spark_round(spark_avg("avg_fare"), 2).alias("avg_fare"),
        spark_round(spark_avg("avg_wait_time_minutes"), 2).alias("avg_wait_min"),
        spark_round(spark_avg("avg_tip_percentage"), 2).alias("avg_tip_pct"),
        spark_sum("total_trips").alias("total_trips")
    ) \
    .orderBy("is_shared_ride") \
    .show(truncate=False)

# ── Table 3: Route Summary ────────────────────────────────────
val_route = spark.read.parquet(OUT_ROUTE)
val_route_count = val_route.count()

print("\n--- Table 3: route_summary ---")
print(f"  Rows       : {val_route_count:,}")
print(f"  Columns    : {len(val_route.columns)}")
print("  Top 10 busiest routes:")
val_route.orderBy(col("total_trips").desc()).show(10, truncate=False)


# ============================================================
# CELL 8 — Final Summary & Athena Sample Queries
# ============================================================

print("\n" + "=" * 60)
print("  AGGREGATION COMPLETE — 6 MONTHS")
print("=" * 60)
print(f"  Source  : {CURATED_PATH}")
print(f"  Records : {total_records:,}")
print("")
print(f"  Table 1  daily_borough_summary")
print(f"    Path   : {OUT_DAILY}")
print(f"    Rows   : {val_daily_count:,}")
print(f"    Partitioned by pickup_date")
print("")
print(f"  Table 2  hourly_pattern_summary")
print(f"    Path   : {OUT_HOURLY}")
print(f"    Rows   : {val_hourly_count:,}")
print(f"    No partition (small lookup table)")
print("")
print(f"  Table 3  route_summary")
print(f"    Path   : {OUT_ROUTE}")
print(f"    Rows   : {val_route_count:,}")
print(f"    Filter : routes with >= {ROUTE_MIN_TRIPS} trips")
print("=" * 60)
print("")
print("  Next steps:")
print("  1. Run: aws glue start-crawler --name mission-deh-hof-crawler-aggregated")
print("  2. Query with Athena (sample queries below)")
print("  3. STOP notebook session to avoid charges!")
print("=" * 60)

# ── Athena queries to run after crawling ─────────────────────
ATHENA_QUERIES = """
-- ── Run these in Athena after the aggregated crawler completes ──

-- 1. Which borough generates the most revenue per day? (6-month trend)
SELECT pickup_date, pickup_borough, total_trips, total_revenue, avg_fare
FROM   nyctlc_aggregated.daily_borough_summary
ORDER  BY pickup_date, total_revenue DESC;

-- 2. Rush-hour patterns: Manhattan weekday vs weekend (shared vs solo)
SELECT pickup_hour, is_weekend, is_shared_ride,
       total_trips, avg_fare, avg_speed_mph, avg_wait_time_minutes
FROM   nyctlc_aggregated.hourly_pattern_summary
WHERE  pickup_borough = 'Manhattan'
ORDER  BY pickup_hour, is_weekend, is_shared_ride;

-- 3. Tipping behaviour by hour and ride type
SELECT pickup_hour, is_shared_ride,
       SUM(total_trips)                       AS trips,
       ROUND(AVG(avg_tip_percentage), 2)       AS avg_tip_pct,
       ROUND(AVG(avg_fare), 2)                 AS avg_fare
FROM   nyctlc_aggregated.hourly_pattern_summary
GROUP  BY pickup_hour, is_shared_ride
ORDER  BY pickup_hour, is_shared_ride;

-- 4. Top 10 busiest routes (6-month aggregate)
SELECT pickup_zone, dropoff_zone, pickup_borough, dropoff_borough,
       total_trips, avg_fare, avg_trip_miles, avg_trip_duration_minutes
FROM   nyctlc_aggregated.route_summary
ORDER  BY total_trips DESC
LIMIT  10;

-- 5. Airport routes: JFK and LGA
SELECT pickup_zone, dropoff_zone, total_trips, avg_fare, avg_trip_miles
FROM   nyctlc_aggregated.route_summary
WHERE  pickup_zone  IN ('JFK Airport', 'LaGuardia Airport')
    OR dropoff_zone IN ('JFK Airport', 'LaGuardia Airport')
ORDER  BY total_trips DESC;

-- 6. Monthly revenue trend across boroughs
SELECT DATE_TRUNC('month', pickup_date) AS month,
       pickup_borough,
       SUM(total_trips)                  AS monthly_trips,
       ROUND(SUM(total_revenue), 2)      AS monthly_revenue,
       ROUND(AVG(avg_fare), 2)           AS avg_fare
FROM   nyctlc_aggregated.daily_borough_summary
GROUP  BY 1, 2
ORDER  BY month, monthly_revenue DESC;

-- 7. Wait time: shared vs solo by borough (6-month avg)
SELECT pickup_borough, is_shared_ride,
       SUM(total_trips)                            AS total_trips,
       ROUND(AVG(avg_wait_time_minutes), 2)        AS avg_wait_min
FROM   nyctlc_aggregated.hourly_pattern_summary
GROUP  BY pickup_borough, is_shared_ride
ORDER  BY pickup_borough, is_shared_ride;
"""

print("\n[INFO] Sample Athena queries saved below:")
print(ATHENA_QUERIES)

# ⚠️ STOP SESSION — active sessions bill at ~$0.44/DPU-hr
print("\n⚠️  STOP your Glue notebook session now!")
