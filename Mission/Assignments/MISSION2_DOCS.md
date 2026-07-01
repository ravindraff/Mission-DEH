# Mission 2 — NYC TLC HVFHV Data Engineering Pipeline
## DEH (Data Engineering Hands-on) Bootcamp — Complete Documentation

---

## Overview

Mission 2 builds a full **cloud-native medallion data pipeline** on AWS, ingesting 6 months of NYC Taxi & Limousine Commission (TLC) High-Volume For-Hire Vehicle (HVFHV) trip data, transforming it through Bronze → Silver → Gold layers, and surfacing analytics-ready tables in Amazon Athena.

The pipeline filters to **Lyft trips only** (license code `HV0005`) and produces three aggregated output tables for business intelligence.

---

## Architecture

```
TLC Public S3 (CloudFront CDN)
    ↓  Task 3/8 — Ingestion (Lambda or Glue Python Shell)
Raw Layer (Bronze)
s3://mission-deh-hof-nyctlc-<account_id>/raw/
    ↓  Task 7 — Data Quality Check (dq-check.py)
    ↓  Task 4 — Transformation (raw-to-curated.py)
Curated Layer (Silver)
s3://mission-deh-hof-nyctlc-<account_id>/curated/
    ↓  Task 6 — Aggregation (curated-to-aggregated.py)
Aggregated Layer (Gold)
s3://mission-deh-hof-nyctlc-<account_id>/aggregated/
    ↓  Glue Crawlers → Glue Data Catalog
Amazon Athena (SQL analytics)
```

---

## Task Summary

| Task | Description | File(s) | Status |
|------|-------------|---------|--------|
| Task 3 | Ingest 6 months of HVFHV data via Glue Python Shell | `nyctlc-ingestion-6months.py` | ✅ |
| Task 4 | Raw → Curated transformation (Bronze → Silver) | `raw-to-curated.py` | ✅ |
| Task 5 | Building Data Engineering Code with Agentic AI course | — | ✅ |
| Task 6 | Curated → Aggregated pipeline (Silver → Gold) | `curated-to-aggregated.py` | ✅ |
| Task 7 | Data Quality Report | `dq-check.py` | ✅ |
| Task 8 | Lambda-based ingestion (optional) | `lambda_nyctlc_ingestion.py` | ✅ |
| Task 9 | Zone Lookup with Pandas in Lambda (custom layer) | `lambda_zone_lookup_processor.py` / `lambda_task9_zone_processor.py` | ✅ |
| Task 10 | Teardown | `clean-up.sh` | ✅ |

---

## Infrastructure

### S3 Data Lake Layout

All data lives in a single account-scoped bucket: `mission-deh-hof-nyctlc-<account_id>`

```
s3://mission-deh-hof-nyctlc-<account_id>/
├── raw/
│   └── nyctlc/
│       ├── fhvhv_tripdata/
│       │   └── fhvhv_tripdata_YYYY-MM.parquet   (one file per month)
│       └── taxi_zone_lookup/
│           └── taxi_zone_lookup.csv
├── curated/
│   └── nyctlc/
│       └── fhvhv_trips_curated/                 (partitioned by pickup_date)
├── aggregated/
│   └── nyctlc/
│       ├── daily_borough_summary/               (partitioned by pickup_date)
│       ├── hourly_pattern_summary/
│       └── route_summary/
└── rejected/
    └── nyctlc/
        ├── fhvhv_tripdata_rejected/             (partitioned by rejection_reason)
        └── quality_summary/
```

> **Bucket naming**: bucket names are always resolved at runtime from the AWS Account ID — never hardcoded.
> ```python
> account_id = boto3.client("sts").get_caller_identity()["Account"]
> bucket = f"mission-deh-hof-nyctlc-{account_id}"
> ```

### AWS Resources

| Resource | Name | Purpose |
|----------|------|---------|
| S3 Bucket | `mission-deh-hof-nyctlc-<account_id>` | Data lake (all layers) |
| IAM Role | `mission-deh-hof-glue-role` | Glue notebooks and crawlers |
| Lambda Function | `mission-deh-hof-nyctlc-ingestion` | HVFHV parquet + zone CSV ingestion |
| Lambda Function | `mission-deh-hof-zone-lookup-processor` | Zone lookup processing with pandas |
| Lambda Layer | `mission-deh-hof-pandas-layer` | pandas + numpy for zone processor |
| Glue Crawler | `mission-deh-hof-crawler-raw` | Catalog raw layer |
| Glue Crawler | `mission-deh-hof-crawler-curated` | Catalog curated layer |
| Glue Crawler | `mission-deh-hof-crawler-aggregated` | Catalog aggregated layer |
| Glue Database | `nyctlc_raw` | Schema registry → raw/nyctlc/ |
| Glue Database | `nyctlc_curated` | Schema registry → curated/nyctlc/ |
| Glue Database | `nyctlc_aggregated` | Schema registry → aggregated/nyctlc/ |
| EventBridge Rule | Monthly schedule | Triggers Lambda ingestion monthly |

---

## Pipeline Files

### Task 3 / Task 8 — Ingestion

#### `nyctlc-ingestion.py` — Glue Python Shell (single month)

Discovers and uploads the latest available HVFHV parquet file plus the zone lookup CSV to S3. Designed for Glue Python Shell (no local `/tmp` storage required — streams directly to S3 via multipart upload).

**Key behavior:**
- Uses `requests.head()` to probe `current_month - 2` through `current_month - 4` and picks the first HTTP 200
- Streams the parquet in 8 MB chunks using S3 multipart upload (never buffers the full file)
- Zone lookup CSV (~10 KB) is uploaded with a simple `put_object`

**Libraries:** `boto3`, `requests`, `dateutil.relativedelta`

---

#### `nyctlc-ingestion-6months.py` — Glue Python Shell (6-month backfill)

Extended version that ingests **6 consecutive months** of HVFHV data in a single run.

**Key behavior:**
- Probes up to 14 months back (`MAX_LOOKBACK = 14`) to find 6 available files
- Creates the S3 bucket and folder structure if it does not exist
- Uploads all 6 parquet files sequentially via S3 multipart upload
- Prints a summary table of uploaded / failed files
- Pure stdlib month arithmetic (no `dateutil` dependency)

**Libraries:** `boto3`, `requests`

---

#### `lambda_nyctlc_ingestion.py` — Lambda (Task 8, optional)

Lambda-based ingestion function, equivalent to the Glue Python Shell version but designed for serverless execution.

**Configuration:**

| Setting | Value |
|---------|-------|
| Runtime | Python 3.12 |
| Memory | 512 MB |
| Timeout | 900 s (15 min) |
| Trigger | EventBridge monthly schedule or manual `invoke` |
| IAM | `s3:PutObject`, `sts:GetCallerIdentity` |

**Key behavior:**
- Uses `urllib3` instead of `requests` (built into Lambda runtime — no layer needed)
- Downloads parquet to `/tmp` then uploads to S3 using `boto3.upload_file` (handles multipart automatically)
- Supports `{"dry_run": true}` event payload to discover the latest URL without downloading
- Returns a structured JSON summary with per-file status, sizes, and any errors

**Dry run example:**
```bash
aws lambda invoke \
  --function-name mission-deh-hof-nyctlc-ingestion \
  --payload '{"dry_run": true}' response.json
```

**Full ingestion:**
```bash
aws lambda invoke \
  --function-name mission-deh-hof-nyctlc-ingestion \
  --payload '{}' response.json
```

---

### Task 7 — Data Quality Check

#### `dq-check.py` — Glue PySpark Notebook

Reads all 6 months of raw HVFHV data (all providers), tags every record with the first DQ rule it fails, splits into good/rejected sets, and writes rejected records and a quality summary table to S3.

**DQ Rules (11 checks, priority-ordered):**

| Priority | Rule | Rejection Reason |
|----------|------|-----------------|
| 1 | `pickup_datetime` IS NOT NULL | `null_pickup_datetime` |
| 2 | `PULocationID` IS NOT NULL | `null_pu_location` |
| 3 | `DOLocationID` IS NOT NULL | `null_do_location` |
| 4 | `dropoff_datetime > pickup_datetime` | `invalid_timestamps` |
| 5 | `trip_miles > 0` | `zero_trip_miles` |
| 6 | `trip_miles < 200` | `excessive_trip_miles` |
| 7 | `base_passenger_fare` IS NOT NULL | `null_fare` |
| 8 | `base_passenger_fare > 0` | `zero_or_negative_fare` |
| 9 | `base_passenger_fare < 500` | `excessive_fare` |
| 10 | `trip_time > 0` | `zero_trip_time` |
| 11 | `trip_time < 14400` (4 hours) | `excessive_trip_time` |

**Outputs:**
- `rejected/nyctlc/fhvhv_tripdata_rejected/` — rejected records partitioned by `rejection_reason`
- `rejected/nyctlc/quality_summary/` — one row per rule with count and `% of total`, plus a `PASSED_ALL_RULES` summary row

**Console report example:**
```
==============================================================
  DATA QUALITY REPORT — NYC TLC HVFHV  |  6 MONTHS
==============================================================
  Metric                              Count         % of Total
  -----------------------------------  ------------  ----------
  Total raw records                   12,345,678    100.00%
  Passed all DQ rules                 11,987,654     97.10%
  Failed at least one rule               358,024      2.90%
==============================================================
  Rejection Breakdown by Rule
  zero_trip_miles                        120,445      0.9758%
  invalid_timestamps                      98,312      0.7965%
  ...
```

**Glue config:**
```
%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 2
```

---

### Task 4 — Raw to Curated (Bronze → Silver)

#### `raw-to-curated.py` — Glue PySpark Notebook

Reads all 6 months of raw HVFHV parquet, filters to Lyft only, applies 9 DQ rules, adds 8 derived columns, joins zone lookup for pickup and dropoff enrichment, and writes a clean 36-column Parquet dataset partitioned by `pickup_date`.

**Processing steps:**

**Step 1 — Read & Filter Lyft**
- Reads all raw parquet with `mergeSchema=true`
- Filters to `hvfhs_license_num == "HV0005"` (Lyft)

**Step 2 — Data Quality (9 rules)**

| Rule | Condition |
|------|-----------|
| 1 | `trip_miles > 0` |
| 2 | `trip_miles < 200` |
| 3 | `base_passenger_fare > 0` |
| 4 | `base_passenger_fare < 500` |
| 5 | `trip_time > 0` |
| 6 | `trip_time < 14400` |
| 7 | `pickup_datetime` IS NOT NULL |
| 8 | `dropoff_datetime > pickup_datetime` |
| 9 | `PULocationID` and `DOLocationID` NOT NULL |

**Step 3 — Derived Columns (8 new fields)**

| Column | Derivation |
|--------|-----------|
| `trip_duration_minutes` | `trip_time / 60` |
| `pickup_date` | `DATE(pickup_datetime)` — partition key |
| `pickup_hour` | `HOUR(pickup_datetime)` (0–23) |
| `pickup_day_of_week` | `DAYOFWEEK(pickup_datetime)` (1=Sun, 7=Sat) |
| `is_weekend` | `1` if Sat/Sun, `0` otherwise |
| `speed_mph` | `trip_miles / (trip_duration_minutes / 60)` |
| `is_shared_ride` | `1` if `shared_match_flag = 'Y'` |
| `wait_time_minutes` | `(pickup_datetime - request_datetime) / 60` |

Post-derivation filter: `speed_mph ≤ 80`, `trip_duration_minutes` between 1 and 240.

**Step 4 — Zone Enrichment**
- Two left joins on `taxi_zone_lookup.csv`:
  - `PULocationID` → `pickup_borough`, `pickup_zone`, `pickup_service_zone`
  - `DOLocationID` → `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone`

**Step 5 — Final Schema (36 columns)**

| Category | Columns |
|----------|---------|
| Time (7) | `request_datetime`, `pickup_datetime`, `dropoff_datetime`, `pickup_date`, `pickup_hour`, `pickup_day_of_week`, `is_weekend` |
| Trip (4) | `trip_miles`, `trip_time`, `trip_duration_minutes`, `speed_mph` |
| Sharing & Wait (2) | `is_shared_ride`, `wait_time_minutes` |
| Pickup Location (4) | `PULocationID`, `pickup_borough`, `pickup_zone`, `pickup_service_zone` |
| Dropoff Location (4) | `DOLocationID`, `dropoff_borough`, `dropoff_zone`, `dropoff_service_zone` |
| Fare (8) | `base_passenger_fare`, `tolls`, `bcf`, `sales_tax`, `congestion_surcharge`, `airport_fee`, `tips`, `driver_pay` |
| Metadata (7) | `dispatching_base_num`, `originating_base_num`, `shared_request_flag`, `shared_match_flag`, `access_a_ride_flag`, `wav_request_flag`, `wav_match_flag` |

**Output:** `s3://.../curated/nyctlc/fhvhv_trips_curated/` — Parquet, partitioned by `pickup_date` (~180 partitions for 6 months), `mode = overwrite`.

---

### Task 6 — Curated to Aggregated (Silver → Gold)

#### `curated-to-aggregated.py` — Glue PySpark Notebook

Reads all 6 months of curated Lyft data (cached for efficiency) and produces 3 analytics-ready aggregate tables.

**Table 1 — `daily_borough_summary`**

- Group by: `pickup_date`, `pickup_borough`
- Partitioned by `pickup_date` (~1,080 rows for 6 months × 6 boroughs)

| Column | Aggregation |
|--------|-------------|
| `total_trips` | `COUNT(*)` |
| `total_revenue` | `SUM(base_passenger_fare)` |
| `avg_fare` | `AVG(base_passenger_fare)` |
| `avg_tip` | `AVG(tips)` |
| `avg_trip_miles` | `AVG(trip_miles)` |
| `avg_trip_duration_minutes` | `AVG(trip_duration_minutes)` |
| `avg_speed_mph` | `AVG(speed_mph)` |

**Table 2 — `hourly_pattern_summary`**

- Group by: `pickup_hour`, `is_weekend`, `pickup_borough`, `is_shared_ride`
- No partition (max ~576 rows — small lookup table)
- Aggregated across all 6 months for statistical stability

| Column | Aggregation |
|--------|-------------|
| `total_trips` | `COUNT(*)` |
| `avg_fare` | `AVG(base_passenger_fare)` |
| `avg_trip_miles` | `AVG(trip_miles)` |
| `avg_tip_percentage` | `AVG(tips / base_passenger_fare * 100)` where fare > 0 |
| `avg_speed_mph` | `AVG(speed_mph)` |
| `avg_wait_time_minutes` | `AVG(wait_time_minutes)` |

**Table 3 — `route_summary`**

- Group by: `pickup_borough`, `pickup_zone`, `dropoff_borough`, `dropoff_zone`
- Filtered to routes with `total_trips >= 10` (removes GPS errors and noise)
- No partition (pre-aggregated, fast for dashboards)
- Sorted by `total_trips DESC` (busiest routes first)

| Column | Aggregation |
|--------|-------------|
| `total_trips` | `COUNT(*)` |
| `avg_fare` | `AVG(base_passenger_fare)` |
| `avg_trip_miles` | `AVG(trip_miles)` |
| `avg_trip_duration_minutes` | `AVG(trip_duration_minutes)` |

**After running:** start the aggregated crawler to update Athena:
```bash
aws glue start-crawler --name mission-deh-hof-crawler-aggregated
```

---

### Task 9 — Zone Lookup Processor (Lambda + pandas)

#### `lambda_zone_lookup_processor.py` / `lambda_task9_zone_processor.py`

Two equivalent implementations of the Task 9 Lambda function (canonical and alternate). Both process the taxi zone lookup CSV from S3 using pandas and write a summary JSON back to S3.

**Configuration:**

| Setting | Value |
|---------|-------|
| Runtime | Python 3.12 |
| Memory | 256 MB |
| Timeout | 30 s |
| Layer | `mission-deh-hof-pandas-layer` (pandas + numpy) |
| IAM | `AmazonS3FullAccess` |

**Processing steps:**
1. Resolve bucket from `sts:GetCallerIdentity`
2. Read `raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv` from S3 into a pandas DataFrame
3. Print shape and first 5 rows to CloudWatch logs
4. Count zones per borough using `groupby("Borough")["Zone"].count()`
5. Identify borough with the most zones
6. Build a service zone breakdown as a bonus metric
7. Write summary JSON to `processed/nyctlc/zone_summary.json`
8. Return borough zone counts in the Lambda response

**Output JSON structure (`zone_summary.json`):**
```json
{
  "generated_at": "2026-07-01T...",
  "source": "s3://mission-deh-hof-nyctlc-<account_id>/raw/nyctlc/taxi_zone_lookup/taxi_zone_lookup.csv",
  "total_zones": 265,
  "total_boroughs": 7,
  "borough_with_most_zones": { "borough": "Queens", "zone_count": 69 },
  "zones_per_borough": { "Queens": 69, "Manhattan": 67, ... },
  "zones_per_service_zone": { "Boro Zone": 128, "Yellow Zone": 73, ... },
  "sample_records": [...]
}
```

**Packaging the pandas Lambda layer:**

Windows (PowerShell):
```powershell
mkdir python
pip install pandas -t python/
Compress-Archive -Path "python" -DestinationPath "pandas-layer.zip" -Force
```

Linux / AWS CloudShell:
```bash
mkdir -p python
pip install pandas -t python/
zip -r pandas-layer.zip python/
```

---

## Glue Notebook Configuration

All Glue notebooks use this standard config (edit the default first cell):

```
%idle_timeout 2880
%glue_version 5.0
%worker_type G.1X
%number_of_workers 2
```

> Using 2 workers instead of 5 saves ~60% cost (~$0.44/DPU-hr × 2 DPUs vs 5 DPUs).

---

## Running the Pipeline (End-to-End)

### Step 1 — Ingest 6 months of data

**Option A: Glue Python Shell**
1. Open AWS Glue → ETL Jobs → create a Python Shell job
2. Upload `nyctlc-ingestion-6months.py`
3. Run the job — it will discover and upload 6 HVFHV parquet files + zone CSV

**Option B: Lambda**
```bash
aws lambda invoke \
  --function-name mission-deh-hof-nyctlc-ingestion \
  --payload '{}' response.json
cat response.json
```

### Step 2 — Run Data Quality Check (optional but recommended)

1. Open Glue Notebooks → create a new notebook using `mission-deh-hof-glue-role`
2. Paste `dq-check.py` cells in order
3. Run all cells — review the DQ report printed at the end
4. **Stop the session immediately** after completion

### Step 3 — Raw → Curated

1. Create a new Glue notebook
2. Paste `raw-to-curated.py` cells in order
3. Run all cells (~5–10 minutes for 6 months)
4. Verify output in Cell 9 validation section
5. **Stop the session**

Start the curated crawler:
```bash
aws glue start-crawler --name mission-deh-hof-crawler-curated
aws glue get-crawler --name mission-deh-hof-crawler-curated \
  --query 'Crawler.State' --output text
```

### Step 4 — Curated → Aggregated

1. Create a new Glue notebook
2. Paste `curated-to-aggregated.py` cells in order
3. Run all cells (~5–10 minutes for 6 months)
4. Review validation output in Cell 7
5. **Stop the session**

Start the aggregated crawler:
```bash
aws glue start-crawler --name mission-deh-hof-crawler-aggregated
```

### Step 5 — Query with Athena

Sample queries (run after crawlers complete):

```sql
-- Monthly revenue trend by borough
SELECT DATE_TRUNC('month', pickup_date) AS month,
       pickup_borough,
       SUM(total_trips)             AS monthly_trips,
       ROUND(SUM(total_revenue), 2) AS monthly_revenue
FROM   nyctlc_aggregated.daily_borough_summary
GROUP BY 1, 2
ORDER BY month, monthly_revenue DESC;

-- Rush-hour patterns: Manhattan weekday vs weekend
SELECT pickup_hour, is_weekend, is_shared_ride,
       total_trips, avg_fare, avg_speed_mph, avg_wait_time_minutes
FROM   nyctlc_aggregated.hourly_pattern_summary
WHERE  pickup_borough = 'Manhattan'
ORDER BY pickup_hour, is_weekend, is_shared_ride;

-- Top 10 busiest routes
SELECT pickup_zone, dropoff_zone, pickup_borough, dropoff_borough,
       total_trips, avg_fare, avg_trip_miles
FROM   nyctlc_aggregated.route_summary
ORDER BY total_trips DESC
LIMIT 10;

-- Airport routes (JFK and LGA)
SELECT pickup_zone, dropoff_zone, total_trips, avg_fare, avg_trip_miles
FROM   nyctlc_aggregated.route_summary
WHERE  pickup_zone  IN ('JFK Airport', 'LaGuardia Airport')
    OR dropoff_zone IN ('JFK Airport', 'LaGuardia Airport')
ORDER BY total_trips DESC;
```

### Step 6 — Zone Lookup Lambda (Task 9)

1. Package the pandas layer (see packaging instructions above)
2. Upload `pandas-layer.zip` as a Lambda layer named `mission-deh-hof-pandas-layer`
3. Create Lambda function `mission-deh-hof-zone-lookup-processor` with Python 3.12, 256 MB, 30s timeout
4. Attach the pandas layer
5. Deploy `lambda_zone_lookup_processor.py` as the function code
6. Invoke:
```bash
aws lambda invoke \
  --function-name mission-deh-hof-zone-lookup-processor \
  --payload '{}' response.json
cat response.json
```

---

## Teardown

To remove all Mission 2 AWS resources:
```bash
bash clean-up.sh
```

This removes the S3 bucket (and all data), Lambda functions, Glue jobs, crawlers, databases, IAM roles, and EventBridge rules created by the bootcamp.

---

## Troubleshooting

### Glue `iam:PassRole` error
```bash
bash fix-glue-role.sh
```

### Crawler not updating schema
```bash
# Force a re-crawl
aws glue start-crawler --name mission-deh-hof-crawler-aggregated

# Wait for READY state
aws glue get-crawler --name mission-deh-hof-crawler-aggregated \
  --query 'Crawler.State' --output text
```

### Lambda timeout on large parquet files
The HVFHV parquet files are 300–500 MB. The Lambda timeout is set to 900 s (15 minutes — the maximum). If you hit timeouts, switch to the Glue Python Shell ingestion instead which has no timeout limit.

### Glue notebook charges
Glue Interactive Sessions bill per DPU-hour even when idle. Always stop your session immediately after running each notebook:
- Click **Stop Session** in the Glue notebook toolbar, or
- The `%idle_timeout 2880` magic will stop the session after 2 days of inactivity (fallback only — do not rely on it)

---

## Key Design Decisions

**No hardcoded account IDs or bucket names** — all S3 paths are derived at runtime using `sts:GetCallerIdentity`. This makes every script portable across AWS accounts without any changes.

**Streaming uploads for large files** — parquet files are 300–500 MB. Both the Glue and Lambda ingestion scripts stream data in 8 MB chunks via S3 multipart upload, avoiding memory exhaustion.

**Partition strategy** — the curated and daily_borough_summary layers are partitioned by `pickup_date`. Athena partition pruning means date-range queries scan only the relevant day folders, not the full 6-month dataset.

**Cache before multi-scan aggregations** — `curated-to-aggregated.py` calls `.cache()` on the curated DataFrame before building all 3 aggregate tables. Without caching, Spark would re-read the parquet from S3 three separate times.

**2 workers, not 5** — the bootcamp default of `%number_of_workers 2` (instead of the notebook default of 5) cuts DPU costs by 60% with only a modest increase in runtime for this dataset size.

**Route noise filter** — `route_summary` filters to routes with `total_trips >= 10`. With 6 months of data, this threshold is meaningful — routes below it are typically GPS artifacts or one-off trips that would skew averages.
