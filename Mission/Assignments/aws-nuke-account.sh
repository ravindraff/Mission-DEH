#!/bin/bash
# ================================================================
# AWS FULL ACCOUNT NUKE — Deletes ALL resources in your account
# ================================================================
# WARNING: This script is IRREVERSIBLE. It will delete EVERYTHING:
#   EC2, RDS, S3, Glue, Athena, Lambda, EMR, Redshift, DynamoDB,
#   SNS, SQS, CloudWatch, Kinesis, Secrets Manager, IAM (custom),
#   VPC (non-default), CloudFormation stacks, ECS, EKS, and more.
#
# USE WITH CAUTION — intended for sandbox/training accounts only.
# ================================================================

export AWS_PAGER=""

# ── Colours ────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

LOG_FILE="aws-nuke-$(date +%Y%m%d-%H%M%S).log"
ERRORS=0
REGION=""

log()  { echo -e "[$(date +'%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  ✅ $1${RESET}"; }
skip() { log "${YELLOW}  ─  $1${RESET}"; }
err()  { log "${RED}  ❌ ERROR: $1${RESET}"; ERRORS=$((ERRORS+1)); }
hdr()  { log ""; log "${CYAN}${BOLD}══════════════════════════════════════════════════════"; \
         log "  $1${RESET}"; \
         log "${CYAN}══════════════════════════════════════════════════════${RESET}"; }

# ================================================================
# PREFLIGHT — Confirm identity & get explicit consent
# ================================================================
hdr "AWS FULL ACCOUNT NUKE"
log "Log file : $LOG_FILE"
log ""

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    err "AWS CLI not configured. Run 'aws configure' first. Aborting."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --no-paginate)
IAM_USER=$(aws sts get-caller-identity --query Arn --output text --no-paginate)
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")

log "${RED}${BOLD}  ⚠️  WARNING: DESTRUCTIVE OPERATION ⚠️${RESET}"
log ""
log "  Account ID : ${BOLD}$ACCOUNT_ID${RESET}"
log "  Identity   : $IAM_USER"
log "  Region     : $REGION"
log ""
log "${RED}  This will permanently delete ALL AWS resources in this account.${RESET}"
log "${RED}  This action CANNOT be undone.${RESET}"
log ""
read -r -p "  Type the account ID to confirm deletion: " CONFIRM

if [[ "$CONFIRM" != "$ACCOUNT_ID" ]]; then
    log "Confirmation did not match. Aborting."
    exit 1
fi

log ""
log "${GREEN}  Confirmed. Starting full account cleanup...${RESET}"
log "  Started : $(date)"

# ================================================================
# STEP 1 — CloudFormation Stacks (delete first to avoid conflicts)
# ================================================================
hdr "STEP 1 │ CloudFormation Stacks"

STACKS=$(aws cloudformation list-stacks \
    --stack-status-filter CREATE_COMPLETE UPDATE_COMPLETE ROLLBACK_COMPLETE \
                          UPDATE_ROLLBACK_COMPLETE IMPORT_COMPLETE \
    --query "StackSummaries[].StackName" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$STACKS" && "$STACKS" != "None" ]]; then
    for STACK in $STACKS; do
        log "Deleting CloudFormation stack: $STACK"
        aws cloudformation delete-stack --stack-name "$STACK" --no-paginate 2>/dev/null \
            && ok "Stack delete initiated: $STACK" \
            || err "Failed to delete stack: $STACK"
    done
    log "Waiting for all stacks to finish deleting..."
    for STACK in $STACKS; do
        aws cloudformation wait stack-delete-complete \
            --stack-name "$STACK" --no-paginate 2>/dev/null || true
    done
    ok "All CloudFormation stacks deleted"
else
    skip "No CloudFormation stacks found"
fi

# ================================================================
# STEP 2 — EC2: Instances, AMIs, Snapshots, Key Pairs, Security Groups
# ================================================================
hdr "STEP 2 │ EC2 — Instances, AMIs, Snapshots, Volumes, Key Pairs, SGs"

# 2a. Terminate all EC2 instances
log "Fetching all EC2 instances..."
INSTANCE_IDS=$(aws ec2 describe-instances \
    --filters "Name=instance-state-name,Values=running,stopped,stopping,pending" \
    --query "Reservations[].Instances[].InstanceId" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$INSTANCE_IDS" && "$INSTANCE_IDS" != "None" ]]; then
    log "Terminating instances: $INSTANCE_IDS"
    aws ec2 terminate-instances --instance-ids $INSTANCE_IDS --no-paginate > /dev/null 2>&1 \
        && log "  Waiting for all instances to terminate..." \
        || err "Failed to terminate some instances"
    for ID in $INSTANCE_IDS; do
        aws ec2 wait instance-terminated --instance-ids "$ID" --no-paginate 2>/dev/null || true
    done
    ok "All EC2 instances terminated"
else
    skip "No EC2 instances found"
fi

# 2b. Deregister all custom AMIs
log "Fetching owned AMIs..."
AMI_IDS=$(aws ec2 describe-images --owners self \
    --query "Images[].ImageId" \
    --output text --no-paginate 2>/dev/null || echo "")

for AMI in $AMI_IDS; do
    aws ec2 deregister-image --image-id "$AMI" --no-paginate 2>/dev/null \
        && ok "AMI deregistered: $AMI" || err "Failed to deregister AMI: $AMI"
done
[[ -z "$AMI_IDS" || "$AMI_IDS" == "None" ]] && skip "No custom AMIs found"

# 2c. Delete all EBS snapshots
log "Fetching owned snapshots..."
SNAP_IDS=$(aws ec2 describe-snapshots --owner-ids self \
    --query "Snapshots[].SnapshotId" \
    --output text --no-paginate 2>/dev/null || echo "")

for SNAP in $SNAP_IDS; do
    aws ec2 delete-snapshot --snapshot-id "$SNAP" --no-paginate 2>/dev/null \
        && ok "Snapshot deleted: $SNAP" || skip "Snapshot $SNAP (in use or not found)"
done
[[ -z "$SNAP_IDS" || "$SNAP_IDS" == "None" ]] && skip "No EBS snapshots found"

# 2d. Delete all unattached EBS volumes
log "Fetching available EBS volumes..."
VOL_IDS=$(aws ec2 describe-volumes \
    --filters "Name=status,Values=available" \
    --query "Volumes[].VolumeId" \
    --output text --no-paginate 2>/dev/null || echo "")

for VOL in $VOL_IDS; do
    aws ec2 delete-volume --volume-id "$VOL" --no-paginate 2>/dev/null \
        && ok "EBS volume deleted: $VOL" || err "Failed to delete volume: $VOL"
done
[[ -z "$VOL_IDS" || "$VOL_IDS" == "None" ]] && skip "No available EBS volumes found"

# 2e. Delete all key pairs
log "Fetching key pairs..."
KEY_NAMES=$(aws ec2 describe-key-pairs \
    --query "KeyPairs[].KeyName" \
    --output text --no-paginate 2>/dev/null || echo "")

for KEY in $KEY_NAMES; do
    aws ec2 delete-key-pair --key-name "$KEY" --no-paginate 2>/dev/null \
        && ok "Key pair deleted: $KEY" || err "Failed to delete key pair: $KEY"
done
[[ -z "$KEY_NAMES" || "$KEY_NAMES" == "None" ]] && skip "No key pairs found"

# 2f. Delete all non-default security groups
log "Fetching non-default security groups..."
SG_IDS=$(aws ec2 describe-security-groups \
    --filters "Name=group-name,Values=*" \
    --query "SecurityGroups[?GroupName!='default'].GroupId" \
    --output text --no-paginate 2>/dev/null || echo "")

for SG in $SG_IDS; do
    aws ec2 delete-security-group --group-id "$SG" --no-paginate 2>/dev/null \
        && ok "Security group deleted: $SG" || skip "SG $SG (in use or default)"
done
[[ -z "$SG_IDS" || "$SG_IDS" == "None" ]] && skip "No custom security groups found"

# 2g. Release all Elastic IPs
log "Fetching Elastic IPs..."
ALLOC_IDS=$(aws ec2 describe-addresses \
    --query "Addresses[].AllocationId" \
    --output text --no-paginate 2>/dev/null || echo "")

for ALLOC in $ALLOC_IDS; do
    aws ec2 release-address --allocation-id "$ALLOC" --no-paginate 2>/dev/null \
        && ok "Elastic IP released: $ALLOC" || err "Failed to release EIP: $ALLOC"
done
[[ -z "$ALLOC_IDS" || "$ALLOC_IDS" == "None" ]] && skip "No Elastic IPs found"

# ================================================================
# STEP 3 — RDS: Instances, Clusters, Snapshots, Subnet Groups, Parameter Groups
# ================================================================
hdr "STEP 3 │ RDS — Instances, Clusters, Snapshots, Subnet Groups"

# 3a. Delete all RDS instances
log "Fetching RDS DB instances..."
RDS_INSTANCES=$(aws rds describe-db-instances \
    --query "DBInstances[].DBInstanceIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")

for DB in $RDS_INSTANCES; do
    log "  Deleting RDS instance: $DB"
    aws rds delete-db-instance \
        --db-instance-identifier "$DB" \
        --skip-final-snapshot \
        --delete-automated-backups \
        --no-paginate 2>/dev/null \
        && ok "RDS instance delete initiated: $DB" || err "Failed to delete RDS instance: $DB"
done
if [[ -n "$RDS_INSTANCES" && "$RDS_INSTANCES" != "None" ]]; then
    for DB in $RDS_INSTANCES; do
        aws rds wait db-instance-deleted \
            --db-instance-identifier "$DB" --no-paginate 2>/dev/null || true
    done
    ok "All RDS instances deleted"
else
    skip "No RDS instances found"
fi

# 3b. Delete all RDS clusters
log "Fetching RDS DB clusters..."
RDS_CLUSTERS=$(aws rds describe-db-clusters \
    --query "DBClusters[].DBClusterIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")

for CLUSTER in $RDS_CLUSTERS; do
    log "  Deleting RDS cluster: $CLUSTER"
    aws rds delete-db-cluster \
        --db-cluster-identifier "$CLUSTER" \
        --skip-final-snapshot \
        --no-paginate 2>/dev/null \
        && ok "RDS cluster delete initiated: $CLUSTER" || err "Failed to delete RDS cluster: $CLUSTER"
done
if [[ -n "$RDS_CLUSTERS" && "$RDS_CLUSTERS" != "None" ]]; then
    for CLUSTER in $RDS_CLUSTERS; do
        aws rds wait db-cluster-deleted \
            --db-cluster-identifier "$CLUSTER" --no-paginate 2>/dev/null || true
    done
    ok "All RDS clusters deleted"
else
    skip "No RDS clusters found"
fi

# 3c. Delete manual RDS snapshots
log "Fetching manual RDS snapshots..."
RDS_SNAPS=$(aws rds describe-db-snapshots \
    --snapshot-type manual \
    --query "DBSnapshots[].DBSnapshotIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")

for SNAP in $RDS_SNAPS; do
    aws rds delete-db-snapshot \
        --db-snapshot-identifier "$SNAP" --no-paginate 2>/dev/null \
        && ok "RDS snapshot deleted: $SNAP" || err "Failed to delete RDS snapshot: $SNAP"
done
[[ -z "$RDS_SNAPS" || "$RDS_SNAPS" == "None" ]] && skip "No manual RDS snapshots found"

# 3d. Delete custom DB subnet groups
log "Fetching DB subnet groups..."
DB_SUBNETS=$(aws rds describe-db-subnet-groups \
    --query "DBSubnetGroups[?DBSubnetGroupName!='default'].DBSubnetGroupName" \
    --output text --no-paginate 2>/dev/null || echo "")

for SG in $DB_SUBNETS; do
    aws rds delete-db-subnet-group \
        --db-subnet-group-name "$SG" --no-paginate 2>/dev/null \
        && ok "DB subnet group deleted: $SG" || err "Failed to delete DB subnet group: $SG"
done
[[ -z "$DB_SUBNETS" || "$DB_SUBNETS" == "None" ]] && skip "No custom DB subnet groups found"

# ================================================================
# STEP 4 — S3: All Buckets (force delete all objects + versions)
# ================================================================
hdr "STEP 4 │ S3 — All Buckets"

log "Fetching all S3 buckets..."
BUCKETS=$(aws s3api list-buckets \
    --query "Buckets[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$BUCKETS" && "$BUCKETS" != "None" ]]; then
    for BUCKET in $BUCKETS; do
        log "  Emptying and deleting bucket: $BUCKET"

        # Remove all versioned objects (handles versioning-enabled buckets)
        aws s3api list-object-versions --bucket "$BUCKET" --no-paginate \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null | \
            python3 -c "
import sys, json, subprocess
data = json.load(sys.stdin)
objs = data.get('Objects') or []
if objs:
    payload = json.dumps({'Objects': objs, 'Quiet': True})
    subprocess.run(['aws','s3api','delete-objects',
        '--bucket','$BUCKET','--delete',payload,'--no-paginate'], capture_output=True)
" 2>/dev/null || true

        # Remove all delete markers
        aws s3api list-object-versions --bucket "$BUCKET" --no-paginate \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null | \
            python3 -c "
import sys, json, subprocess
data = json.load(sys.stdin)
objs = data.get('Objects') or []
if objs:
    payload = json.dumps({'Objects': objs, 'Quiet': True})
    subprocess.run(['aws','s3api','delete-objects',
        '--bucket','$BUCKET','--delete',payload,'--no-paginate'], capture_output=True)
" 2>/dev/null || true

        # Force remove remaining objects and delete bucket
        aws s3 rb "s3://$BUCKET" --force --no-paginate > /dev/null 2>&1 \
            && ok "S3 bucket deleted: $BUCKET" \
            || err "Failed to delete bucket: $BUCKET"
    done
else
    skip "No S3 buckets found"
fi

# ================================================================
# STEP 5 — AWS Glue: Jobs, Crawlers, Databases, Connections, Triggers
# ================================================================
hdr "STEP 5 │ Glue — Jobs, Crawlers, Databases, Triggers, Connections"

# 5a. Delete Glue jobs
log "Fetching Glue jobs..."
GLUE_JOBS=$(aws glue list-jobs \
    --query "JobNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for JOB in $GLUE_JOBS; do
    aws glue delete-job --job-name "$JOB" --no-paginate 2>/dev/null \
        && ok "Glue job deleted: $JOB" || err "Failed to delete Glue job: $JOB"
done
[[ -z "$GLUE_JOBS" || "$GLUE_JOBS" == "None" ]] && skip "No Glue jobs found"

# 5b. Delete Glue triggers
log "Fetching Glue triggers..."
GLUE_TRIGGERS=$(aws glue list-triggers \
    --query "TriggerNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for TRIGGER in $GLUE_TRIGGERS; do
    aws glue delete-trigger --name "$TRIGGER" --no-paginate 2>/dev/null \
        && ok "Glue trigger deleted: $TRIGGER" || err "Failed to delete Glue trigger: $TRIGGER"
done
[[ -z "$GLUE_TRIGGERS" || "$GLUE_TRIGGERS" == "None" ]] && skip "No Glue triggers found"

# 5c. Delete Glue crawlers
log "Fetching Glue crawlers..."
GLUE_CRAWLERS=$(aws glue list-crawlers \
    --query "CrawlerNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for CRAWLER in $GLUE_CRAWLERS; do
    aws glue delete-crawler --name "$CRAWLER" --no-paginate 2>/dev/null \
        && ok "Glue crawler deleted: $CRAWLER" || err "Failed to delete Glue crawler: $CRAWLER"
done
[[ -z "$GLUE_CRAWLERS" || "$GLUE_CRAWLERS" == "None" ]] && skip "No Glue crawlers found"

# 5d. Delete Glue databases (and all tables within)
log "Fetching Glue databases..."
GLUE_DBS=$(aws glue get-databases \
    --query "DatabaseList[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

for DB in $GLUE_DBS; do
    # Delete all tables first
    TABLES=$(aws glue get-tables --database-name "$DB" \
        --query "TableList[].Name" --output text --no-paginate 2>/dev/null || echo "")
    for TABLE in $TABLES; do
        aws glue delete-table --database-name "$DB" --name "$TABLE" \
            --no-paginate 2>/dev/null || true
    done
    aws glue delete-database --name "$DB" --no-paginate 2>/dev/null \
        && ok "Glue database deleted: $DB" || err "Failed to delete Glue database: $DB"
done
[[ -z "$GLUE_DBS" || "$GLUE_DBS" == "None" ]] && skip "No Glue databases found"

# 5e. Delete Glue connections
log "Fetching Glue connections..."
GLUE_CONNS=$(aws glue get-connections \
    --query "ConnectionList[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

for CONN in $GLUE_CONNS; do
    aws glue delete-connection --connection-name "$CONN" --no-paginate 2>/dev/null \
        && ok "Glue connection deleted: $CONN" || err "Failed to delete Glue connection: $CONN"
done
[[ -z "$GLUE_CONNS" || "$GLUE_CONNS" == "None" ]] && skip "No Glue connections found"

# ================================================================
# STEP 6 — Athena: Named Queries, Workgroups, Data Catalogs
# ================================================================
hdr "STEP 6 │ Athena — Named Queries, Workgroups"

# 6a. Clear named queries from all workgroups
log "Fetching Athena workgroups..."
WORKGROUPS=$(aws athena list-work-groups \
    --query "WorkGroups[?Name!='primary'].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

# Clear primary workgroup named queries
for WG in "primary" $WORKGROUPS; do
    NAMED_QUERIES=$(aws athena list-named-queries \
        --work-group "$WG" \
        --query "NamedQueryIds[]" \
        --output text --no-paginate 2>/dev/null || echo "")
    for QID in $NAMED_QUERIES; do
        aws athena delete-named-query \
            --named-query-id "$QID" --no-paginate 2>/dev/null || true
    done
    [[ -n "$NAMED_QUERIES" && "$NAMED_QUERIES" != "None" ]] \
        && ok "Athena named queries cleared from workgroup: $WG"
done

# 6b. Delete non-primary workgroups
for WG in $WORKGROUPS; do
    aws athena delete-work-group \
        --work-group "$WG" --recursive-delete-option \
        --no-paginate 2>/dev/null \
        && ok "Athena workgroup deleted: $WG" || err "Failed to delete Athena workgroup: $WG"
done
[[ -z "$WORKGROUPS" || "$WORKGROUPS" == "None" ]] && skip "No custom Athena workgroups found"

# ================================================================
# STEP 7 — Lambda Functions
# ================================================================
hdr "STEP 7 │ Lambda — Functions"

log "Fetching Lambda functions..."
LAMBDA_FUNCS=$(aws lambda list-functions \
    --query "Functions[].FunctionName" \
    --output text --no-paginate 2>/dev/null || echo "")

for FUNC in $LAMBDA_FUNCS; do
    aws lambda delete-function --function-name "$FUNC" --no-paginate 2>/dev/null \
        && ok "Lambda function deleted: $FUNC" || err "Failed to delete Lambda function: $FUNC"
done
[[ -z "$LAMBDA_FUNCS" || "$LAMBDA_FUNCS" == "None" ]] && skip "No Lambda functions found"

# ================================================================
# STEP 8 — DynamoDB Tables
# ================================================================
hdr "STEP 8 │ DynamoDB — Tables"

log "Fetching DynamoDB tables..."
DYNAMO_TABLES=$(aws dynamodb list-tables \
    --query "TableNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for TABLE in $DYNAMO_TABLES; do
    aws dynamodb delete-table --table-name "$TABLE" --no-paginate 2>/dev/null \
        && ok "DynamoDB table deleted: $TABLE" || err "Failed to delete DynamoDB table: $TABLE"
done
[[ -z "$DYNAMO_TABLES" || "$DYNAMO_TABLES" == "None" ]] && skip "No DynamoDB tables found"

# ================================================================
# STEP 9 — Kinesis: Data Streams & Firehose Delivery Streams
# ================================================================
hdr "STEP 9 │ Kinesis — Data Streams & Firehose"

log "Fetching Kinesis Data Streams..."
KINESIS_STREAMS=$(aws kinesis list-streams \
    --query "StreamNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for STREAM in $KINESIS_STREAMS; do
    aws kinesis delete-stream --stream-name "$STREAM" --no-paginate 2>/dev/null \
        && ok "Kinesis stream deleted: $STREAM" || err "Failed to delete Kinesis stream: $STREAM"
done
[[ -z "$KINESIS_STREAMS" || "$KINESIS_STREAMS" == "None" ]] && skip "No Kinesis Data Streams found"

log "Fetching Kinesis Firehose delivery streams..."
FIREHOSE_STREAMS=$(aws firehose list-delivery-streams \
    --query "DeliveryStreamNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for STREAM in $FIREHOSE_STREAMS; do
    aws firehose delete-delivery-stream \
        --delivery-stream-name "$STREAM" --no-paginate 2>/dev/null \
        && ok "Firehose stream deleted: $STREAM" || err "Failed to delete Firehose stream: $STREAM"
done
[[ -z "$FIREHOSE_STREAMS" || "$FIREHOSE_STREAMS" == "None" ]] && skip "No Firehose delivery streams found"

# ================================================================
# STEP 10 — SNS Topics & SQS Queues
# ================================================================
hdr "STEP 10 │ SNS Topics & SQS Queues"

log "Fetching SNS topics..."
SNS_TOPICS=$(aws sns list-topics \
    --query "Topics[].TopicArn" \
    --output text --no-paginate 2>/dev/null || echo "")

for TOPIC in $SNS_TOPICS; do
    aws sns delete-topic --topic-arn "$TOPIC" --no-paginate 2>/dev/null \
        && ok "SNS topic deleted: $TOPIC" || err "Failed to delete SNS topic: $TOPIC"
done
[[ -z "$SNS_TOPICS" || "$SNS_TOPICS" == "None" ]] && skip "No SNS topics found"

log "Fetching SQS queues..."
SQS_QUEUES=$(aws sqs list-queues \
    --query "QueueUrls[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for QUEUE in $SQS_QUEUES; do
    aws sqs delete-queue --queue-url "$QUEUE" --no-paginate 2>/dev/null \
        && ok "SQS queue deleted: $QUEUE" || err "Failed to delete SQS queue: $QUEUE"
done
[[ -z "$SQS_QUEUES" || "$SQS_QUEUES" == "None" ]] && skip "No SQS queues found"

# ================================================================
# STEP 11 — CloudWatch: Log Groups, Alarms, Dashboards
# ================================================================
hdr "STEP 11 │ CloudWatch — Log Groups, Alarms, Dashboards"

log "Fetching CloudWatch Log Groups..."
LOG_GROUPS=$(aws logs describe-log-groups \
    --query "logGroups[].logGroupName" \
    --output text --no-paginate 2>/dev/null || echo "")

for LG in $LOG_GROUPS; do
    aws logs delete-log-group --log-group-name "$LG" --no-paginate 2>/dev/null \
        && ok "Log group deleted: $LG" || err "Failed to delete log group: $LG"
done
[[ -z "$LOG_GROUPS" || "$LOG_GROUPS" == "None" ]] && skip "No CloudWatch Log Groups found"

log "Fetching CloudWatch Alarms..."
CW_ALARMS=$(aws cloudwatch describe-alarms \
    --query "MetricAlarms[].AlarmName" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$CW_ALARMS" && "$CW_ALARMS" != "None" ]]; then
    aws cloudwatch delete-alarms --alarm-names $CW_ALARMS --no-paginate 2>/dev/null \
        && ok "All CloudWatch alarms deleted" || err "Failed to delete some alarms"
else
    skip "No CloudWatch alarms found"
fi

log "Fetching CloudWatch Dashboards..."
CW_DASHBOARDS=$(aws cloudwatch list-dashboards \
    --query "DashboardEntries[].DashboardName" \
    --output text --no-paginate 2>/dev/null || echo "")

for DASH in $CW_DASHBOARDS; do
    aws cloudwatch delete-dashboards --dashboard-names "$DASH" --no-paginate 2>/dev/null \
        && ok "Dashboard deleted: $DASH" || err "Failed to delete dashboard: $DASH"
done
[[ -z "$CW_DASHBOARDS" || "$CW_DASHBOARDS" == "None" ]] && skip "No CloudWatch dashboards found"

# ================================================================
# STEP 12 — Secrets Manager & SSM Parameter Store
# ================================================================
hdr "STEP 12 │ Secrets Manager & SSM Parameter Store"

log "Fetching Secrets Manager secrets..."
SECRETS=$(aws secretsmanager list-secrets \
    --query "SecretList[].ARN" \
    --output text --no-paginate 2>/dev/null || echo "")

for SECRET in $SECRETS; do
    aws secretsmanager delete-secret \
        --secret-id "$SECRET" \
        --force-delete-without-recovery \
        --no-paginate 2>/dev/null \
        && ok "Secret deleted: $SECRET" || err "Failed to delete secret: $SECRET"
done
[[ -z "$SECRETS" || "$SECRETS" == "None" ]] && skip "No Secrets Manager secrets found"

log "Fetching SSM parameters..."
SSM_PARAMS=$(aws ssm describe-parameters \
    --query "Parameters[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

for PARAM in $SSM_PARAMS; do
    aws ssm delete-parameter --name "$PARAM" --no-paginate 2>/dev/null \
        && ok "SSM parameter deleted: $PARAM" || err "Failed to delete SSM parameter: $PARAM"
done
[[ -z "$SSM_PARAMS" || "$SSM_PARAMS" == "None" ]] && skip "No SSM parameters found"

# ================================================================
# STEP 13 — EMR Clusters
# ================================================================
hdr "STEP 13 │ EMR — Clusters"

log "Fetching active EMR clusters..."
EMR_IDS=$(aws emr list-clusters \
    --active \
    --query "Clusters[].Id" \
    --output text --no-paginate 2>/dev/null || echo "")

for EMR in $EMR_IDS; do
    aws emr terminate-clusters --cluster-ids "$EMR" --no-paginate 2>/dev/null \
        && ok "EMR cluster terminate initiated: $EMR" || err "Failed to terminate EMR cluster: $EMR"
done
[[ -z "$EMR_IDS" || "$EMR_IDS" == "None" ]] && skip "No active EMR clusters found"

# ================================================================
# STEP 14 — Redshift Clusters
# ================================================================
hdr "STEP 14 │ Redshift — Clusters"

log "Fetching Redshift clusters..."
RS_CLUSTERS=$(aws redshift describe-clusters \
    --query "Clusters[].ClusterIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")

for RS in $RS_CLUSTERS; do
    aws redshift delete-cluster \
        --cluster-identifier "$RS" \
        --skip-final-cluster-snapshot \
        --no-paginate 2>/dev/null \
        && ok "Redshift cluster deleted: $RS" || err "Failed to delete Redshift cluster: $RS"
done
[[ -z "$RS_CLUSTERS" || "$RS_CLUSTERS" == "None" ]] && skip "No Redshift clusters found"

# ================================================================
# STEP 15 — ECS: Services, Clusters
# ================================================================
hdr "STEP 15 │ ECS — Services & Clusters"

log "Fetching ECS clusters..."
ECS_CLUSTERS=$(aws ecs list-clusters \
    --query "clusterArns[]" \
    --output text --no-paginate 2>/dev/null || echo "")

for CLUSTER_ARN in $ECS_CLUSTERS; do
    # Stop and delete all services in the cluster
    SERVICES=$(aws ecs list-services --cluster "$CLUSTER_ARN" \
        --query "serviceArns[]" --output text --no-paginate 2>/dev/null || echo "")
    for SVC in $SERVICES; do
        aws ecs update-service --cluster "$CLUSTER_ARN" \
            --service "$SVC" --desired-count 0 --no-paginate > /dev/null 2>&1 || true
        aws ecs delete-service --cluster "$CLUSTER_ARN" \
            --service "$SVC" --force --no-paginate 2>/dev/null \
            && ok "ECS service deleted: $SVC" || err "Failed to delete ECS service: $SVC"
    done
    aws ecs delete-cluster --cluster "$CLUSTER_ARN" --no-paginate 2>/dev/null \
        && ok "ECS cluster deleted: $CLUSTER_ARN" || err "Failed to delete ECS cluster: $CLUSTER_ARN"
done
[[ -z "$ECS_CLUSTERS" || "$ECS_CLUSTERS" == "None" ]] && skip "No ECS clusters found"

# ================================================================
# STEP 16 — VPC: Non-default VPCs, Subnets, IGWs, Route Tables, NAT GWs
# ================================================================
hdr "STEP 16 │ VPC — Non-default VPCs & All Sub-Resources"

log "Fetching non-default VPCs..."
VPC_IDS=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=false" \
    --query "Vpcs[].VpcId" \
    --output text --no-paginate 2>/dev/null || echo "")

for VPC in $VPC_IDS; do
    log "  Cleaning VPC: $VPC"

    # Delete NAT Gateways
    NAT_IDS=$(aws ec2 describe-nat-gateways \
        --filter "Name=vpc-id,Values=$VPC" \
        --query "NatGateways[?State!='deleted'].NatGatewayId" \
        --output text --no-paginate 2>/dev/null || echo "")
    for NAT in $NAT_IDS; do
        aws ec2 delete-nat-gateway --nat-gateway-id "$NAT" --no-paginate > /dev/null 2>&1 || true
        ok "NAT gateway delete initiated: $NAT"
    done

    # Detach and delete internet gateways
    IGW_IDS=$(aws ec2 describe-internet-gateways \
        --filters "Name=attachment.vpc-id,Values=$VPC" \
        --query "InternetGateways[].InternetGatewayId" \
        --output text --no-paginate 2>/dev/null || echo "")
    for IGW in $IGW_IDS; do
        aws ec2 detach-internet-gateway \
            --internet-gateway-id "$IGW" --vpc-id "$VPC" --no-paginate 2>/dev/null || true
        aws ec2 delete-internet-gateway \
            --internet-gateway-id "$IGW" --no-paginate 2>/dev/null \
            && ok "IGW deleted: $IGW" || err "Failed to delete IGW: $IGW"
    done

    # Delete non-main route table associations + route tables
    RT_IDS=$(aws ec2 describe-route-tables \
        --filters "Name=vpc-id,Values=$VPC" \
        --query "RouteTables[?Associations[0].Main!=\`true\`].RouteTableId" \
        --output text --no-paginate 2>/dev/null || echo "")
    for RT in $RT_IDS; do
        aws ec2 delete-route-table \
            --route-table-id "$RT" --no-paginate 2>/dev/null \
            && ok "Route table deleted: $RT" || skip "Route table $RT (in use)"
    done

    # Delete subnets
    SUBNET_IDS=$(aws ec2 describe-subnets \
        --filters "Name=vpc-id,Values=$VPC" \
        --query "Subnets[].SubnetId" \
        --output text --no-paginate 2>/dev/null || echo "")
    for SUBNET in $SUBNET_IDS; do
        aws ec2 delete-subnet \
            --subnet-id "$SUBNET" --no-paginate 2>/dev/null \
            && ok "Subnet deleted: $SUBNET" || err "Failed to delete subnet: $SUBNET"
    done

    # Delete network ACLs (non-default)
    NACL_IDS=$(aws ec2 describe-network-acls \
        --filters "Name=vpc-id,Values=$VPC" \
        --query "NetworkAcls[?IsDefault==\`false\`].NetworkAclId" \
        --output text --no-paginate 2>/dev/null || echo "")
    for NACL in $NACL_IDS; do
        aws ec2 delete-network-acl \
            --network-acl-id "$NACL" --no-paginate 2>/dev/null \
            && ok "Network ACL deleted: $NACL" || skip "NACL $NACL (in use)"
    done

    # Finally delete the VPC
    aws ec2 delete-vpc --vpc-id "$VPC" --no-paginate 2>/dev/null \
        && ok "VPC deleted: $VPC" || err "Failed to delete VPC: $VPC (resources may remain)"
done
[[ -z "$VPC_IDS" || "$VPC_IDS" == "None" ]] && skip "No non-default VPCs found"
