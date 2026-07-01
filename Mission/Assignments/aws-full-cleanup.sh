#!/bin/bash
# ================================================================
# DEH Bootcamp — Complete AWS Account Cleanup Script
# ================================================================
#
# SERVICES ANALYZED & CLEANED:
# ─────────────────────────────────────────────────────────────────
# 1. EC2
#    - Instance:        mission-deh-hof-unix-training-bootcamp
#    - Key Pair:        mission-deh-hof-unix-key-bootcamp
#    - Security Group:  mission-deh-hof-unix-sg-bootcamp
#    - IAM Role:        mission-deh-hof-unix-role-bootcamp
#    - IAM Profile:     mission-deh-hof-unix-profile-bootcamp
#
# 2. Aurora RDS (PostgreSQL Serverless v2)
#    - DB Instance:     mission-deh-hof-bootcamp-aurora-instance
#    - DB Cluster:      mission-deh-hof-bootcamp-aurora-cluster
#    - Subnet Group:    mission-deh-hof-bootcamp-subnet-group
#    - Security Group:  mission-deh-hof-bootcamp-aurora-sg
#
# 3. S3 Buckets
#    - Mission 1:       mission-deh-hof-bootcamp-<account-id>
#    - Mission 2:       mission-deh-hof-nyctlc-<account-id>
#
# 4. AWS Glue — Mission 1
#    - Crawler:         mission-deh-hof-bootcamp-hvfhv-crawler
#    - Database:        mission_deh_hof_bootcamp
#    - IAM Role:        mission-deh-hof-bootcamp-glue-role
#      (policies: AWSGlueServiceRole, AmazonS3FullAccess, AthenaFullAccessInline)
#
# 5. AWS Glue — Mission 2
#    - Jobs:            mission-deh-hof-nyctlc-ingestion
#                       mission-deh-hof-raw-to-curated-etl
#                       mission-deh-hof-curated-to-aggregated-etl
#    - Crawlers:        mission-deh-hof-crawler-raw
#                       mission-deh-hof-crawler-curated
#                       mission-deh-hof-crawler-aggregated
#    - Databases:       nyctlc_raw, nyctlc_curated, nyctlc_aggregated
#    - IAM Role:        mission-deh-hof-glue-role
#      (policies: AWSGlueServiceRole, AmazonS3FullAccess,
#                 CloudWatchLogsFullAccess, GluePassRolePolicy inline)
#
# 6. Athena
#    - Results stored in S3 (deleted with bucket above)
#    - Primary workgroup query history cleared
#
# NOTE: Deletes everything — account will be empty of bootcamp resources.
# ================================================================

export AWS_PAGER=""

# ── Colour helpers ─────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

LOG_FILE="aws-full-cleanup-$(date +%Y%m%d-%H%M%S).log"
ERRORS=0

log()  { echo -e "[$(date +'%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  ✅ $1${RESET}"; }
skip() { log "${YELLOW}  ⚠️  $1 — not found, skipping${RESET}"; }
err()  { log "${RED}  ❌ $1${RESET}"; ERRORS=$((ERRORS + 1)); }
hdr()  { log ""; log "${CYAN}${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"; log "  $1${RESET}"; log "${CYAN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"; }

# ── Preflight ──────────────────────────────────────────────────
hdr "DEH Bootcamp — Full AWS Cleanup"
log "Log file : $LOG_FILE"
log "Started  : $(date)"
log ""

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    err "AWS CLI not configured or no credentials found. Aborting."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --no-paginate)
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")
log "Account  : $ACCOUNT_ID"
log "Region   : $REGION"
log ""

# ================================================================
# STEP 1 — EC2 Resources
# ================================================================
hdr "STEP 1 of 6 │ EC2 — Instance, Key Pair, Security Group, IAM"

EC2_INSTANCE_TAG="mission-deh-hof-unix-training-bootcamp"
EC2_KEY="mission-deh-hof-unix-key-bootcamp"
EC2_SG="mission-deh-hof-unix-sg-bootcamp"
EC2_ROLE="mission-deh-hof-unix-role-bootcamp"
EC2_PROFILE="mission-deh-hof-unix-profile-bootcamp"

# 1a. Find and terminate EC2 instance
log "Searching for EC2 instance tagged: $EC2_INSTANCE_TAG"
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters \
        "Name=tag:Name,Values=$EC2_INSTANCE_TAG" \
        "Name=instance-state-name,Values=running,stopped,stopping,pending" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [[ -n "$INSTANCE_ID" && "$INSTANCE_ID" != "None" ]]; then
    log "  Terminating instance: $INSTANCE_ID"
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --no-paginate > /dev/null 2>&1 \
        && log "  Waiting for termination (this takes ~1–2 min)..." \
        || err "Failed to terminate instance $INSTANCE_ID"
    aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" --no-paginate 2>/dev/null \
        && ok "EC2 instance terminated: $INSTANCE_ID" \
        || err "Timed out waiting for instance termination"
else
    skip "EC2 instance ($EC2_INSTANCE_TAG)"
fi

# 1b. Delete key pair
log "Deleting key pair: $EC2_KEY"
aws ec2 delete-key-pair --key-name "$EC2_KEY" --no-paginate 2>/dev/null \
    && ok "Key pair deleted: $EC2_KEY" \
    || skip "Key pair ($EC2_KEY)"
[[ -f "${EC2_KEY}.pem" ]] && rm -f "${EC2_KEY}.pem" && ok "Local .pem file removed"

# 1c. Delete EC2 security group (after instance is gone)
log "Looking up security group: $EC2_SG"
EC2_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$EC2_SG" \
    --query "SecurityGroups[0].GroupId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [[ -n "$EC2_SG_ID" && "$EC2_SG_ID" != "None" ]]; then
    aws ec2 delete-security-group --group-id "$EC2_SG_ID" --no-paginate 2>/dev/null \
        && ok "EC2 security group deleted: $EC2_SG_ID" \
        || err "Could not delete EC2 security group (may still be in use)"
else
    skip "EC2 security group ($EC2_SG)"
fi

# 1d. IAM — remove role from profile, delete profile, detach policies, delete role
log "Cleaning IAM instance profile: $EC2_PROFILE"
aws iam remove-role-from-instance-profile \
    --instance-profile-name "$EC2_PROFILE" \
    --role-name "$EC2_ROLE" --no-paginate 2>/dev/null || true
aws iam delete-instance-profile \
    --instance-profile-name "$EC2_PROFILE" --no-paginate 2>/dev/null \
    && ok "Instance profile deleted: $EC2_PROFILE" \
    || skip "Instance profile ($EC2_PROFILE)"

log "Cleaning IAM role: $EC2_ROLE"
aws iam detach-role-policy --role-name "$EC2_ROLE" \
    --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" \
    --no-paginate 2>/dev/null || true
aws iam delete-role --role-name "$EC2_ROLE" --no-paginate 2>/dev/null \
    && ok "IAM role deleted: $EC2_ROLE" \
    || skip "IAM role ($EC2_ROLE)"

# ================================================================
# STEP 2 — Aurora RDS
# ================================================================
hdr "STEP 2 of 6 │ Aurora RDS — Cluster, Instance, Subnet Group, SG"

RDS_PREFIX="mission-deh-hof-bootcamp"
RDS_INSTANCE="${RDS_PREFIX}-aurora-instance"
RDS_CLUSTER="${RDS_PREFIX}-aurora-cluster"
RDS_SUBNET="${RDS_PREFIX}-subnet-group"
RDS_SG="${RDS_PREFIX}-aurora-sg"

# 2a. Delete DB instance first
log "Deleting RDS instance: $RDS_INSTANCE"
if aws rds describe-db-instances \
        --db-instance-identifier "$RDS_INSTANCE" --no-paginate > /dev/null 2>&1; then
    aws rds delete-db-instance \
        --db-instance-identifier "$RDS_INSTANCE" \
        --skip-final-snapshot --no-paginate > /dev/null 2>&1 \
        && log "  Waiting for DB instance deletion (this takes ~5 min)..." \
        || err "Failed to delete RDS instance"
    aws rds wait db-instance-deleted \
        --db-instance-identifier "$RDS_INSTANCE" --no-paginate 2>/dev/null \
        && ok "RDS instance deleted: $RDS_INSTANCE" \
        || err "Timed out waiting for RDS instance deletion"
else
    skip "RDS instance ($RDS_INSTANCE)"
fi

# 2b. Delete DB cluster
log "Deleting RDS cluster: $RDS_CLUSTER"
if aws rds describe-db-clusters \
        --db-cluster-identifier "$RDS_CLUSTER" --no-paginate > /dev/null 2>&1; then
    aws rds delete-db-cluster \
        --db-cluster-identifier "$RDS_CLUSTER" \
        --skip-final-snapshot --no-paginate > /dev/null 2>&1 \
        && log "  Waiting for DB cluster deletion..." \
        || err "Failed to delete RDS cluster"
    aws rds wait db-cluster-deleted \
        --db-cluster-identifier "$RDS_CLUSTER" --no-paginate 2>/dev/null \
        && ok "RDS cluster deleted: $RDS_CLUSTER" \
        || err "Timed out waiting for RDS cluster deletion"
else
    skip "RDS cluster ($RDS_CLUSTER)"
fi

# 2c. Delete DB subnet group
log "Deleting DB subnet group: $RDS_SUBNET"
aws rds delete-db-subnet-group \
    --db-subnet-group-name "$RDS_SUBNET" --no-paginate 2>/dev/null \
    && ok "DB subnet group deleted: $RDS_SUBNET" \
    || skip "DB subnet group ($RDS_SUBNET)"

# 2d. Delete RDS security group
log "Looking up RDS security group: $RDS_SG"
RDS_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$RDS_SG" \
    --query "SecurityGroups[0].GroupId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [[ -n "$RDS_SG_ID" && "$RDS_SG_ID" != "None" ]]; then
    aws ec2 delete-security-group --group-id "$RDS_SG_ID" --no-paginate 2>/dev/null \
        && ok "RDS security group deleted: $RDS_SG_ID" \
        || err "Could not delete RDS security group (may still be in use)"
else
    skip "RDS security group ($RDS_SG)"
fi

# ================================================================
# STEP 3 — S3 Buckets
# ================================================================
hdr "STEP 3 of 6 │ S3 — All Bootcamp Buckets"

M1_BUCKET="mission-deh-hof-bootcamp-${ACCOUNT_ID}"
M2_BUCKET="mission-deh-hof-nyctlc-${ACCOUNT_ID}"

for BUCKET in "$M1_BUCKET" "$M2_BUCKET"; do
    log "Checking S3 bucket: $BUCKET"
    if aws s3api head-bucket --bucket "$BUCKET" --no-paginate > /dev/null 2>&1; then
        log "  Force-deleting all objects and bucket..."
        aws s3 rb "s3://${BUCKET}" --force --no-paginate > /dev/null 2>&1 \
            && ok "S3 bucket deleted: $BUCKET" \
            || err "Failed to delete S3 bucket: $BUCKET"
    else
        skip "S3 bucket ($BUCKET)"
    fi
done

# ================================================================
# STEP 4 — Glue Mission 1 (Crawler + Database + IAM Role)
# ================================================================
hdr "STEP 4 of 6 │ Glue Mission 1 — Crawler, Database, IAM Role"

M1_CRAWLER="mission-deh-hof-bootcamp-hvfhv-crawler"
M1_DB="mission_deh_hof_bootcamp"
M1_ROLE="mission-deh-hof-bootcamp-glue-role"

# 4a. Delete crawler
log "Deleting Glue crawler: $M1_CRAWLER"
aws glue delete-crawler --name "$M1_CRAWLER" --no-paginate 2>/dev/null \
    && ok "Glue crawler deleted: $M1_CRAWLER" \
    || skip "Glue crawler ($M1_CRAWLER)"

# 4b. Delete Glue database (tables are auto-deleted)
log "Deleting Glue database: $M1_DB"
aws glue delete-database --name "$M1_DB" --no-paginate 2>/dev/null \
    && ok "Glue database deleted: $M1_DB" \
    || skip "Glue database ($M1_DB)"

# 4c. Clean up IAM role
log "Cleaning IAM role: $M1_ROLE"
if aws iam get-role --role-name "$M1_ROLE" --no-paginate > /dev/null 2>&1; then
    aws iam detach-role-policy --role-name "$M1_ROLE" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" \
        --no-paginate 2>/dev/null || true
    aws iam detach-role-policy --role-name "$M1_ROLE" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess" \
        --no-paginate 2>/dev/null || true
    aws iam delete-role-policy --role-name "$M1_ROLE" \
        --policy-name "AthenaFullAccessInline" --no-paginate 2>/dev/null || true
    aws iam delete-role --role-name "$M1_ROLE" --no-paginate 2>/dev/null \
        && ok "IAM role deleted: $M1_ROLE" \
        || err "Failed to delete IAM role: $M1_ROLE"
else
    skip "IAM role ($M1_ROLE)"
fi

# ================================================================
# STEP 5 — Glue Mission 2 (Jobs + Crawlers + Databases + IAM Role)
# ================================================================
hdr "STEP 5 of 6 │ Glue Mission 2 — Jobs, Crawlers, Databases, IAM Role"

M2_ROLE="mission-deh-hof-glue-role"

# 5a. Delete Glue jobs
log "Deleting Glue jobs (Mission 2)..."
for JOB in \
    "mission-deh-hof-nyctlc-ingestion" \
    "mission-deh-hof-raw-to-curated-etl" \
    "mission-deh-hof-curated-to-aggregated-etl"
do
    aws glue delete-job --job-name "$JOB" --no-paginate 2>/dev/null \
        && ok "Glue job deleted: $JOB" \
        || skip "Glue job ($JOB)"
done

# 5b. Delete Glue crawlers
log "Deleting Glue crawlers (Mission 2)..."
for CRAWLER in \
    "mission-deh-hof-crawler-raw" \
    "mission-deh-hof-crawler-curated" \
    "mission-deh-hof-crawler-aggregated"
do
    aws glue delete-crawler --name "$CRAWLER" --no-paginate 2>/dev/null \
        && ok "Glue crawler deleted: $CRAWLER" \
        || skip "Glue crawler ($CRAWLER)"
done

# 5c. Delete Glue databases + all tables inside
log "Deleting Glue databases (Mission 2)..."
for DB in "nyctlc_raw" "nyctlc_curated" "nyctlc_aggregated"; do
    if aws glue get-database --name "$DB" --no-paginate > /dev/null 2>&1; then
        # Delete all tables first
        TABLES=$(aws glue get-tables --database-name "$DB" \
            --query "TableList[].Name" --output text --no-paginate 2>/dev/null || echo "")
        for TABLE in $TABLES; do
            aws glue delete-table \
                --database-name "$DB" --name "$TABLE" \
                --no-paginate > /dev/null 2>&1 || true
        done
        aws glue delete-database --name "$DB" --no-paginate > /dev/null 2>&1 \
            && ok "Glue database deleted: $DB" \
            || err "Failed to delete Glue database: $DB"
    else
        skip "Glue database ($DB)"
    fi
done

# 5d. Clean up Mission 2 IAM role (dynamic policy detach)
log "Cleaning IAM role: $M2_ROLE"
if aws iam get-role --role-name "$M2_ROLE" --no-paginate > /dev/null 2>&1; then
    # Detach all managed policies dynamically
    POLICIES=$(aws iam list-attached-role-policies \
        --role-name "$M2_ROLE" \
        --query "AttachedPolicies[].PolicyArn" \
        --output text --no-paginate 2>/dev/null || echo "")
    for POLICY_ARN in $POLICIES; do
        aws iam detach-role-policy \
            --role-name "$M2_ROLE" --policy-arn "$POLICY_ARN" \
            --no-paginate 2>/dev/null || true
    done
    # Delete all inline policies dynamically
    INLINE=$(aws iam list-role-policies \
        --role-name "$M2_ROLE" \
        --query "PolicyNames[]" \
        --output text --no-paginate 2>/dev/null || echo "")
    for POLICY_NAME in $INLINE; do
        aws iam delete-role-policy \
            --role-name "$M2_ROLE" --policy-name "$POLICY_NAME" \
            --no-paginate 2>/dev/null || true
    done
    aws iam delete-role --role-name "$M2_ROLE" --no-paginate 2>/dev/null \
        && ok "IAM role deleted: $M2_ROLE" \
        || err "Failed to delete IAM role: $M2_ROLE"
else
    skip "IAM role ($M2_ROLE)"
fi

# ================================================================
# STEP 6 — Athena & Local File Cleanup
# ================================================================
hdr "STEP 6 of 6 │ Athena Query History + Local Files"

# Athena stores results in S3 (already deleted in Step 3).
# Clear named query history from the primary workgroup.
log "Clearing Athena saved queries from primary workgroup..."
NAMED_QUERIES=$(aws athena list-named-queries \
    --work-group "primary" \
    --query "NamedQueryIds[]" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$NAMED_QUERIES" && "$NAMED_QUERIES" != "None" ]]; then
    for QID in $NAMED_QUERIES; do
        aws athena delete-named-query \
            --named-query-id "$QID" --no-paginate 2>/dev/null || true
    done
    ok "Athena named queries cleared"
else
    log "  No Athena named queries found"
fi

# Remove local leftover files
log "Removing local detail/key files..."
for FILE in \
    "mission-deh-hof-ec2-details.txt" \
    "${EC2_KEY}.pem"
do
    [[ -f "$FILE" ]] && rm -f "$FILE" && ok "Removed local file: $FILE"
done

# ================================================================
# FINAL SUMMARY
# ================================================================
log ""
log "${CYAN}${BOLD}================================================================${RESET}"
log "${BOLD}  CLEANUP COMPLETE${RESET}"
log "${CYAN}================================================================${RESET}"
log ""
log "  Account  : $ACCOUNT_ID"
log "  Finished : $(date)"
log "  Log file : $LOG_FILE"
log ""
log "  Resources targeted:"
log "    [EC2]     Instance + Key Pair + Security Group + IAM Role/Profile"
log "    [RDS]     Aurora Cluster + Instance + Subnet Group + Security Group"
log "    [S3]      mission-deh-hof-bootcamp-${ACCOUNT_ID}"
log "              mission-deh-hof-nyctlc-${ACCOUNT_ID}"
log "    [Glue M1] Crawler + Database + IAM Role"
log "    [Glue M2] 3 Jobs + 3 Crawlers + 3 Databases + IAM Role"
log "    [Athena]  Saved queries (results deleted with S3)"
log ""

if [[ $ERRORS -gt 0 ]]; then
    log "${RED}  ⚠️  Completed with $ERRORS error(s). Review log: $LOG_FILE${RESET}"
    exit 1
else
    log "${GREEN}  ✅ All resources successfully removed. Your account is clean.${RESET}"
    exit 0
fi
