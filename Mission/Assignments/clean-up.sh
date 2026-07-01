#!/bin/bash
# ============================================================
# DEH Bootcamp — Master AWS Clean-Up Script
# Clears ALL resources from Mission 1 & Mission 2:
#   - EC2 instance, key pair, security groups, IAM role/profile
#   - Aurora RDS cluster & instance, subnet group, security group
#   - Glue jobs, crawlers, databases (Mission 1 & Mission 2)
#   - S3 buckets (Mission 1 & Mission 2)
#   - IAM roles for Glue (Mission 1 & Mission 2)
#   - Athena workgroup query results (Mission 1)
# ============================================================

export AWS_PAGER=""
set -e

LOG_FILE="cleanup-$(date +%Y%m%d-%H%M%S).log"

log() {
    echo "[$(date +'%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "============================================================"
log "  DEH Bootcamp — Master AWS Clean-Up"
log "  Log file: $LOG_FILE"
log "============================================================"
log ""

# Get AWS Account ID
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --no-paginate)
log "AWS Account ID: $ACCOUNT_ID"
log ""

# ============================================================
# STEP 1: Terminate EC2 Instance (Mission 1)
# ============================================================
log "------------------------------------------------------------"
log "STEP 1: EC2 — Instance, Key Pair, Security Group, IAM Role"
log "------------------------------------------------------------"

EC2_INSTANCE_NAME="mission-deh-hof-unix-training-bootcamp"
EC2_SG_NAME="mission-deh-hof-unix-sg-bootcamp"
EC2_KEY_NAME="mission-deh-hof-unix-key-bootcamp"
EC2_ROLE_NAME="mission-deh-hof-unix-role-bootcamp"
EC2_PROFILE_NAME="mission-deh-hof-unix-profile-bootcamp"

# Find and terminate instance
log "Searching for EC2 instance: $EC2_INSTANCE_NAME"
INSTANCE_ID=$(aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=$EC2_INSTANCE_NAME" \
              "Name=instance-state-name,Values=running,stopped,stopping,pending" \
    --query "Reservations[0].Instances[0].InstanceId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [ -n "$INSTANCE_ID" ] && [ "$INSTANCE_ID" != "None" ]; then
    log "Terminating EC2 instance: $INSTANCE_ID"
    aws ec2 terminate-instances --instance-ids "$INSTANCE_ID" --no-paginate > /dev/null 2>&1
    log "Waiting for instance to terminate..."
    aws ec2 wait instance-terminated --instance-ids "$INSTANCE_ID" --no-paginate
    log "  ✅ EC2 instance terminated: $INSTANCE_ID"
else
    log "  ⚠️  No EC2 instance found (skipping)"
fi

# Delete key pair
log "Deleting key pair: $EC2_KEY_NAME"
aws ec2 delete-key-pair --key-name "$EC2_KEY_NAME" --no-paginate 2>/dev/null \
    && log "  ✅ Key pair deleted: $EC2_KEY_NAME" \
    || log "  ⚠️  Key pair not found (skipping)"

# Remove local .pem file if present
if [ -f "${EC2_KEY_NAME}.pem" ]; then
    rm -f "${EC2_KEY_NAME}.pem"
    log "  ✅ Local .pem file removed"
fi

# Delete security group (must happen after instance termination)
log "Searching for security group: $EC2_SG_NAME"
EC2_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$EC2_SG_NAME" \
    --query "SecurityGroups[0].GroupId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [ -n "$EC2_SG_ID" ] && [ "$EC2_SG_ID" != "None" ]; then
    aws ec2 delete-security-group --group-id "$EC2_SG_ID" --no-paginate 2>/dev/null \
        && log "  ✅ Security group deleted: $EC2_SG_ID" \
        || log "  ⚠️  Security group in use or already deleted"
else
    log "  ⚠️  Security group not found (skipping)"
fi

# Remove IAM role from instance profile, then delete both
log "Cleaning up IAM instance profile: $EC2_PROFILE_NAME"
aws iam remove-role-from-instance-profile \
    --instance-profile-name "$EC2_PROFILE_NAME" \
    --role-name "$EC2_ROLE_NAME" --no-paginate 2>/dev/null || true
aws iam delete-instance-profile \
    --instance-profile-name "$EC2_PROFILE_NAME" --no-paginate 2>/dev/null \
    && log "  ✅ Instance profile deleted" \
    || log "  ⚠️  Instance profile not found (skipping)"

log "Cleaning up IAM role: $EC2_ROLE_NAME"
aws iam detach-role-policy --role-name "$EC2_ROLE_NAME" \
    --policy-arn "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore" --no-paginate 2>/dev/null || true
aws iam delete-role --role-name "$EC2_ROLE_NAME" --no-paginate 2>/dev/null \
    && log "  ✅ IAM role deleted: $EC2_ROLE_NAME" \
    || log "  ⚠️  IAM role not found (skipping)"
log ""

# ============================================================
# STEP 2: Aurora RDS Cluster (Mission 1)
# ============================================================
log "------------------------------------------------------------"
log "STEP 2: Aurora RDS — Cluster, Instance, Subnet Group, SG"
log "------------------------------------------------------------"

RDS_PREFIX="mission-deh-hof-bootcamp"
RDS_CLUSTER_ID="${RDS_PREFIX}-aurora-cluster"
RDS_INSTANCE_ID="${RDS_PREFIX}-aurora-instance"
RDS_SG_NAME="${RDS_PREFIX}-aurora-sg"
RDS_SUBNET_GROUP="${RDS_PREFIX}-subnet-group"

log "Deleting RDS DB instance: $RDS_INSTANCE_ID"
aws rds delete-db-instance \
    --db-instance-identifier "$RDS_INSTANCE_ID" \
    --skip-final-snapshot --no-paginate 2>/dev/null \
    && log "  Waiting for DB instance deletion..." \
    || log "  ⚠️  DB instance not found (skipping)"

if aws rds describe-db-instances --db-instance-identifier "$RDS_INSTANCE_ID" --no-paginate > /dev/null 2>&1; then
    aws rds wait db-instance-deleted --db-instance-identifier "$RDS_INSTANCE_ID" --no-paginate
    log "  ✅ DB instance deleted"
fi

log "Deleting RDS DB cluster: $RDS_CLUSTER_ID"
aws rds delete-db-cluster \
    --db-cluster-identifier "$RDS_CLUSTER_ID" \
    --skip-final-snapshot --no-paginate 2>/dev/null \
    && log "  Waiting for DB cluster deletion..." \
    || log "  ⚠️  DB cluster not found (skipping)"

if aws rds describe-db-clusters --db-cluster-identifier "$RDS_CLUSTER_ID" --no-paginate > /dev/null 2>&1; then
    aws rds wait db-cluster-deleted --db-cluster-identifier "$RDS_CLUSTER_ID" --no-paginate
    log "  ✅ DB cluster deleted"
fi

log "Deleting DB subnet group: $RDS_SUBNET_GROUP"
aws rds delete-db-subnet-group \
    --db-subnet-group-name "$RDS_SUBNET_GROUP" --no-paginate 2>/dev/null \
    && log "  ✅ Subnet group deleted" \
    || log "  ⚠️  Subnet group not found (skipping)"

log "Searching for security group: $RDS_SG_NAME"
RDS_SG_ID=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=$RDS_SG_NAME" \
    --query "SecurityGroups[0].GroupId" \
    --output text --no-paginate 2>/dev/null || echo "None")

if [ -n "$RDS_SG_ID" ] && [ "$RDS_SG_ID" != "None" ]; then
    aws ec2 delete-security-group --group-id "$RDS_SG_ID" --no-paginate 2>/dev/null \
        && log "  ✅ RDS security group deleted: $RDS_SG_ID" \
        || log "  ⚠️  RDS security group in use or already deleted"
else
    log "  ⚠️  RDS security group not found (skipping)"
fi
log ""

# ============================================================
# STEP 3: Glue + Athena Cleanup (Mission 1)
# ============================================================
log "------------------------------------------------------------"
log "STEP 3: Mission 1 — Glue Crawler, Database, S3, IAM Role"
log "------------------------------------------------------------"

M1_BUCKET="mission-deh-hof-bootcamp-${ACCOUNT_ID}"
M1_GLUE_ROLE="mission-deh-hof-bootcamp-glue-role"
M1_GLUE_DB="mission_deh_hof_bootcamp"
M1_CRAWLER="mission-deh-hof-bootcamp-hvfhv-crawler"

log "Deleting Glue crawler: $M1_CRAWLER"
aws glue delete-crawler --name "$M1_CRAWLER" --no-paginate 2>/dev/null \
    && log "  ✅ Crawler deleted: $M1_CRAWLER" \
    || log "  ⚠️  Crawler not found (skipping)"

log "Deleting Glue database: $M1_GLUE_DB"
aws glue delete-database --name "$M1_GLUE_DB" --no-paginate 2>/dev/null \
    && log "  ✅ Glue database deleted: $M1_GLUE_DB" \
    || log "  ⚠️  Glue database not found (skipping)"

log "Deleting S3 bucket: $M1_BUCKET"
if aws s3api head-bucket --bucket "$M1_BUCKET" --no-paginate > /dev/null 2>&1; then
    aws s3 rb "s3://${M1_BUCKET}" --force --no-paginate > /dev/null 2>&1
    log "  ✅ S3 bucket deleted: $M1_BUCKET"
else
    log "  ⚠️  S3 bucket not found (skipping)"
fi

log "Cleaning up IAM role: $M1_GLUE_ROLE"
if aws iam get-role --role-name "$M1_GLUE_ROLE" --no-paginate > /dev/null 2>&1; then
    aws iam detach-role-policy --role-name "$M1_GLUE_ROLE" \
        --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" --no-paginate 2>/dev/null || true
    aws iam detach-role-policy --role-name "$M1_GLUE_ROLE" \
        --policy-arn "arn:aws:iam::aws:policy/AmazonS3FullAccess" --no-paginate 2>/dev/null || true
    aws iam delete-role-policy --role-name "$M1_GLUE_ROLE" \
        --policy-name "AthenaFullAccessInline" --no-paginate 2>/dev/null || true
    aws iam delete-role --role-name "$M1_GLUE_ROLE" --no-paginate 2>/dev/null
    log "  ✅ IAM role deleted: $M1_GLUE_ROLE"
else
    log "  ⚠️  IAM role not found (skipping)"
fi
log ""

# ============================================================
# STEP 4: Glue Jobs, Crawlers, Databases + S3 (Mission 2)
# ============================================================
log "------------------------------------------------------------"
log "STEP 4: Mission 2 — Glue Jobs, Crawlers, Databases, S3, IAM"
log "------------------------------------------------------------"

M2_BUCKET="mission-deh-hof-nyctlc-${ACCOUNT_ID}"
M2_GLUE_ROLE="mission-deh-hof-glue-role"

# Delete Glue Jobs
log "Deleting Glue jobs (Mission 2)..."
for JOB_NAME in \
    "mission-deh-hof-nyctlc-ingestion" \
    "mission-deh-hof-raw-to-curated-etl" \
    "mission-deh-hof-curated-to-aggregated-etl"
do
    aws glue delete-job --job-name "$JOB_NAME" --no-paginate 2>/dev/null \
        && log "  ✅ Deleted job: $JOB_NAME" \
        || log "  ⚠️  Job not found: $JOB_NAME (skipping)"
done

# Delete Glue Crawlers
log "Deleting Glue crawlers (Mission 2)..."
for CRAWLER_NAME in \
    "mission-deh-hof-crawler-raw" \
    "mission-deh-hof-crawler-curated" \
    "mission-deh-hof-crawler-aggregated"
do
    aws glue delete-crawler --name "$CRAWLER_NAME" --no-paginate 2>/dev/null \
        && log "  ✅ Deleted crawler: $CRAWLER_NAME" \
        || log "  ⚠️  Crawler not found: $CRAWLER_NAME (skipping)"
done

# Delete Glue Databases (and all tables inside)
log "Deleting Glue databases (Mission 2)..."
for DB_NAME in "nyctlc_raw" "nyctlc_curated" "nyctlc_aggregated"; do
    if aws glue get-database --name "$DB_NAME" --no-paginate > /dev/null 2>&1; then
        TABLES=$(aws glue get-tables --database-name "$DB_NAME" \
            --query 'TableList[].Name' --output text --no-paginate 2>/dev/null || echo "")
        for TABLE_NAME in $TABLES; do
            aws glue delete-table --database-name "$DB_NAME" \
                --name "$TABLE_NAME" --no-paginate > /dev/null 2>&1 || true
        done
        aws glue delete-database --name "$DB_NAME" --no-paginate > /dev/null 2>&1
        log "  ✅ Deleted database: $DB_NAME"
    else
        log "  ⚠️  Database not found: $DB_NAME (skipping)"
    fi
done

# Delete S3 Bucket (Mission 2)
log "Deleting S3 bucket: $M2_BUCKET"
if aws s3api head-bucket --bucket "$M2_BUCKET" --no-paginate > /dev/null 2>&1; then
    aws s3 rb "s3://${M2_BUCKET}" --force --no-paginate > /dev/null 2>&1
    log "  ✅ S3 bucket deleted: $M2_BUCKET"
else
    log "  ⚠️  S3 bucket not found (skipping)"
fi

# Delete IAM Role (Mission 2)
log "Cleaning up IAM role: $M2_GLUE_ROLE"
if aws iam get-role --role-name "$M2_GLUE_ROLE" --no-paginate > /dev/null 2>&1; then
    POLICIES=$(aws iam list-attached-role-policies --role-name "$M2_GLUE_ROLE" \
        --query 'AttachedPolicies[].PolicyArn' --output text --no-paginate 2>/dev/null || echo "")
    for POLICY_ARN in $POLICIES; do
        aws iam detach-role-policy --role-name "$M2_GLUE_ROLE" \
            --policy-arn "$POLICY_ARN" --no-paginate 2>/dev/null || true
    done
    INLINE_POLICIES=$(aws iam list-role-policies --role-name "$M2_GLUE_ROLE" \
        --query 'PolicyNames[]' --output text --no-paginate 2>/dev/null || echo "")
    for POLICY_NAME in $INLINE_POLICIES; do
        aws iam delete-role-policy --role-name "$M2_GLUE_ROLE" \
            --policy-name "$POLICY_NAME" --no-paginate 2>/dev/null || true
    done
    aws iam delete-role --role-name "$M2_GLUE_ROLE" --no-paginate 2>/dev/null
    log "  ✅ IAM role deleted: $M2_GLUE_ROLE"
else
    log "  ⚠️  IAM role not found (skipping)"
fi
log ""

# ============================================================
# STEP 5: Clean up local detail files
# ============================================================
log "------------------------------------------------------------"
log "STEP 5: Removing local resource detail files"
log "------------------------------------------------------------"

for FILE in \
    "mission-deh-hof-ec2-details.txt" \
    "mission-deh-hof-unix-key-bootcamp.pem"
do
    if [ -f "$FILE" ]; then
        rm -f "$FILE"
        log "  ✅ Removed: $FILE"
    fi
done
log ""

# ============================================================
# SUMMARY
# ============================================================
log "============================================================"
log "  ✅ MASTER CLEAN-UP COMPLETE!"
log "============================================================"
log ""
log "  Resources removed:"
log "    ✔ EC2 instance, key pair, security group, IAM role/profile"
log "    ✔ Aurora RDS cluster, instance, subnet group, security group"
log "    ✔ Mission 1 — Glue crawler, database, S3 bucket, IAM role"
log "    ✔ Mission 2 — Glue jobs, crawlers, databases, S3 bucket, IAM role"
log ""
log "  Log file: $LOG_FILE"
log "============================================================"
