#!/bin/bash
# ================================================================
# Recently-Visited AWS Services Cleanup Script
# ================================================================
# Targets exactly the services from your recently-visited list:
#
#   1.  IAM                 — Roles, Policies, Users, Groups
#   2.  Lambda              — Functions, Layers, Event Source Mappings
#   3.  S3                  — All buckets (incl. versioned objects)
#   4.  CloudWatch          — Log groups, Alarms, Dashboards
#   5.  Athena              — Named queries, custom workgroups
#   6.  AWS Glue            — Jobs, Triggers, Crawlers, Databases
#   7.  Amazon SageMaker    — Endpoints, Models, Notebooks,
#                             Training jobs, Pipelines, Domains
#   8.  Step Functions      — State machines, Activities
#   9.  DynamoDB            — All tables
#  10.  Aurora & RDS        — Clusters, Instances, Snapshots,
#                             Subnet groups
#  11.  VPC                 — Non-default VPCs + sub-resources
#                             (IGW, Subnets, Route tables,
#                              Security groups, NAT gateways)
#
# ⚠️  NOTE: Billing and Cost Management has no deletable resources
#           via CLI — skipped intentionally.
#
# ⚠️  IRREVERSIBLE — run only on sandbox/training accounts.
# ================================================================

export AWS_PAGER=""

# ── Colours ────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

LOG_FILE="recently-visited-cleanup-$(date +%Y%m%d-%H%M%S).log"
ERRORS=0

log()  { echo -e "[$(date +'%H:%M:%S')] $1" | tee -a "$LOG_FILE"; }
ok()   { log "${GREEN}  ✅ $1${RESET}"; }
skip() { log "${YELLOW}  ─  No $1 found, skipping${RESET}"; }
err()  { log "${RED}  ❌ ERROR: $1${RESET}"; ERRORS=$((ERRORS+1)); }
hdr()  { log ""; \
         log "${CYAN}${BOLD}══════════════════════════════════════════════════════"; \
         log "  $1"; \
         log "══════════════════════════════════════════════════════${RESET}"; }

# ================================================================
# PREFLIGHT — Verify credentials & confirm intent
# ================================================================
hdr "PREFLIGHT"

if ! aws sts get-caller-identity > /dev/null 2>&1; then
    err "AWS CLI not configured. Run 'aws configure' first. Aborting."
    exit 1
fi

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --no-paginate)
IAM_USER=$(aws sts get-caller-identity --query Arn --output text --no-paginate)
REGION=$(aws configure get region 2>/dev/null || echo "us-east-1")

log "${RED}${BOLD}  ⚠️  WARNING: DESTRUCTIVE & IRREVERSIBLE OPERATION ⚠️${RESET}"
log ""
log "  Account ID : ${BOLD}$ACCOUNT_ID${RESET}"
log "  Identity   : $IAM_USER"
log "  Region     : $REGION"
log ""
log "  Services targeted (from Recently Visited):"
log "    IAM · Lambda · S3 · CloudWatch · Athena · AWS Glue"
log "    SageMaker · Step Functions · DynamoDB · Aurora/RDS · VPC"
log ""
read -r -p "  Type the Account ID to confirm: " CONFIRM

if [[ "$CONFIRM" != "$ACCOUNT_ID" ]]; then
    log "Account ID did not match. Aborting."
    exit 1
fi

log ""
log "${GREEN}  Confirmed. Starting cleanup at $(date)${RESET}"

# ================================================================
# STEP 1 — IAM: Customer-managed Policies, Roles, Users, Groups
# ================================================================
hdr "STEP 1 of 11 │ IAM — Roles, Policies, Users, Groups"

# 1a. Detach + delete all non-AWS-managed roles
log "  Fetching IAM roles..."
IAM_ROLES=$(aws iam list-roles \
    --query "Roles[?starts_with(Path, '/') && !starts_with(Arn, 'arn:aws:iam::aws:')].RoleName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$IAM_ROLES" && "$IAM_ROLES" != "None" ]]; then
    for ROLE in $IAM_ROLES; do
        # Skip AWS service-linked roles
        PATH_VAL=$(aws iam get-role --role-name "$ROLE" \
            --query "Role.Path" --output text --no-paginate 2>/dev/null || echo "")
        if [[ "$PATH_VAL" == "/aws-service-role/"* ]]; then
            skip "service-linked role $ROLE (skipping)"
            continue
        fi
        # Detach all managed policies
        ATTACHED=$(aws iam list-attached-role-policies --role-name "$ROLE" \
            --query "AttachedPolicies[].PolicyArn" --output text --no-paginate 2>/dev/null || echo "")
        for POLICY_ARN in $ATTACHED; do
            aws iam detach-role-policy --role-name "$ROLE" \
                --policy-arn "$POLICY_ARN" --no-paginate 2>/dev/null || true
        done
        # Delete all inline policies
        INLINE=$(aws iam list-role-policies --role-name "$ROLE" \
            --query "PolicyNames[]" --output text --no-paginate 2>/dev/null || echo "")
        for PNAME in $INLINE; do
            aws iam delete-role-policy --role-name "$ROLE" \
                --policy-name "$PNAME" --no-paginate 2>/dev/null || true
        done
        # Remove from instance profiles
        PROFILES=$(aws iam list-instance-profiles-for-role --role-name "$ROLE" \
            --query "InstanceProfiles[].InstanceProfileName" --output text --no-paginate 2>/dev/null || echo "")
        for PROF in $PROFILES; do
            aws iam remove-role-from-instance-profile \
                --instance-profile-name "$PROF" \
                --role-name "$ROLE" --no-paginate 2>/dev/null || true
            aws iam delete-instance-profile \
                --instance-profile-name "$PROF" --no-paginate 2>/dev/null || true
        done
        aws iam delete-role --role-name "$ROLE" --no-paginate 2>/dev/null \
            && ok "IAM role deleted: $ROLE" || err "IAM role: $ROLE"
    done
else
    skip "customer-managed IAM roles"
fi

# 1b. Delete all customer-managed policies
log "  Fetching customer-managed IAM policies..."
IAM_POLICIES=$(aws iam list-policies --scope Local \
    --query "Policies[].Arn" --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$IAM_POLICIES" && "$IAM_POLICIES" != "None" ]]; then
    for POL_ARN in $IAM_POLICIES; do
        # Detach from all entities before deletion
        ENTITIES=$(aws iam list-entities-for-policy --policy-arn "$POL_ARN" \
            --output json --no-paginate 2>/dev/null || echo "{}")
        echo "$ENTITIES" | python3 -c "
import sys, json, subprocess
d = json.load(sys.stdin)
for r in d.get('PolicyRoles', []):
    subprocess.run(['aws','iam','detach-role-policy','--role-name',r['RoleName'],'--policy-arn','$POL_ARN','--no-paginate'], capture_output=True)
for u in d.get('PolicyUsers', []):
    subprocess.run(['aws','iam','detach-user-policy','--user-name',u['UserName'],'--policy-arn','$POL_ARN','--no-paginate'], capture_output=True)
for g in d.get('PolicyGroups', []):
    subprocess.run(['aws','iam','detach-group-policy','--group-name',g['GroupName'],'--policy-arn','$POL_ARN','--no-paginate'], capture_output=True)
" 2>/dev/null || true
        # Delete non-default versions first
        VERSIONS=$(aws iam list-policy-versions --policy-arn "$POL_ARN" \
            --query "Versions[?!IsDefaultVersion].VersionId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for VER in $VERSIONS; do
            aws iam delete-policy-version --policy-arn "$POL_ARN" \
                --version-id "$VER" --no-paginate 2>/dev/null || true
        done
        aws iam delete-policy --policy-arn "$POL_ARN" --no-paginate 2>/dev/null \
            && ok "IAM policy deleted: $POL_ARN" || err "IAM policy: $POL_ARN"
    done
else
    skip "customer-managed IAM policies"
fi

# 1c. Delete IAM users (non-root)
log "  Fetching IAM users..."
IAM_USERS=$(aws iam list-users --query "Users[].UserName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$IAM_USERS" && "$IAM_USERS" != "None" ]]; then
    for USER in $IAM_USERS; do
        # Detach managed policies
        UPOLICIES=$(aws iam list-attached-user-policies --user-name "$USER" \
            --query "AttachedPolicies[].PolicyArn" --output text --no-paginate 2>/dev/null || echo "")
        for UPOL in $UPOLICIES; do
            aws iam detach-user-policy --user-name "$USER" \
                --policy-arn "$UPOL" --no-paginate 2>/dev/null || true
        done
        # Delete inline policies
        UINLINE=$(aws iam list-user-policies --user-name "$USER" \
            --query "PolicyNames[]" --output text --no-paginate 2>/dev/null || echo "")
        for UPNAME in $UINLINE; do
            aws iam delete-user-policy --user-name "$USER" \
                --policy-name "$UPNAME" --no-paginate 2>/dev/null || true
        done
        # Remove from groups
        UGROUPS=$(aws iam list-groups-for-user --user-name "$USER" \
            --query "Groups[].GroupName" --output text --no-paginate 2>/dev/null || echo "")
        for GRP in $UGROUPS; do
            aws iam remove-user-from-group --group-name "$GRP" \
                --user-name "$USER" --no-paginate 2>/dev/null || true
        done
        # Delete access keys
        KEYS=$(aws iam list-access-keys --user-name "$USER" \
            --query "AccessKeyMetadata[].AccessKeyId" --output text --no-paginate 2>/dev/null || echo "")
        for KEY in $KEYS; do
            aws iam delete-access-key --user-name "$USER" \
                --access-key-id "$KEY" --no-paginate 2>/dev/null || true
        done
        # Delete login profile (console password)
        aws iam delete-login-profile --user-name "$USER" --no-paginate 2>/dev/null || true
        aws iam delete-user --user-name "$USER" --no-paginate 2>/dev/null \
            && ok "IAM user deleted: $USER" || err "IAM user: $USER"
    done
else
    skip "IAM users"
fi

# 1d. Delete IAM groups
log "  Fetching IAM groups..."
IAM_GROUPS=$(aws iam list-groups --query "Groups[].GroupName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$IAM_GROUPS" && "$IAM_GROUPS" != "None" ]]; then
    for GRP in $IAM_GROUPS; do
        GPOLICIES=$(aws iam list-attached-group-policies --group-name "$GRP" \
            --query "AttachedPolicies[].PolicyArn" --output text --no-paginate 2>/dev/null || echo "")
        for GPOL in $GPOLICIES; do
            aws iam detach-group-policy --group-name "$GRP" \
                --policy-arn "$GPOL" --no-paginate 2>/dev/null || true
        done
        GINLINE=$(aws iam list-group-policies --group-name "$GRP" \
            --query "PolicyNames[]" --output text --no-paginate 2>/dev/null || echo "")
        for GPNAME in $GINLINE; do
            aws iam delete-group-policy --group-name "$GRP" \
                --policy-name "$GPNAME" --no-paginate 2>/dev/null || true
        done
        aws iam delete-group --group-name "$GRP" --no-paginate 2>/dev/null \
            && ok "IAM group deleted: $GRP" || err "IAM group: $GRP"
    done
else
    skip "IAM groups"
fi

# ================================================================
# STEP 2 — Lambda: Functions, Layers, Event Source Mappings
# ================================================================
hdr "STEP 2 of 11 │ Lambda — Functions, Layers, Event Source Mappings"

# 2a. Event source mappings (detach before function deletion)
log "  Fetching Lambda event source mappings..."
ESM_UUIDS=$(aws lambda list-event-source-mappings \
    --query "EventSourceMappings[].UUID" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$ESM_UUIDS" && "$ESM_UUIDS" != "None" ]]; then
    for UUID in $ESM_UUIDS; do
        aws lambda delete-event-source-mapping \
            --uuid "$UUID" --no-paginate 2>/dev/null \
            && ok "Event source mapping deleted: $UUID" || err "ESM: $UUID"
    done
else
    skip "Lambda event source mappings"
fi

# 2b. Lambda functions
log "  Fetching Lambda functions..."
LAMBDA_FUNCS=$(aws lambda list-functions \
    --query "Functions[].FunctionName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$LAMBDA_FUNCS" && "$LAMBDA_FUNCS" != "None" ]]; then
    for FUNC in $LAMBDA_FUNCS; do
        aws lambda delete-function \
            --function-name "$FUNC" --no-paginate 2>/dev/null \
            && ok "Lambda function deleted: $FUNC" || err "Lambda function: $FUNC"
    done
else
    skip "Lambda functions"
fi

# 2c. Lambda layers (all versions)
log "  Fetching Lambda layers..."
LAMBDA_LAYERS=$(aws lambda list-layers \
    --query "Layers[].LayerName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$LAMBDA_LAYERS" && "$LAMBDA_LAYERS" != "None" ]]; then
    for LAYER in $LAMBDA_LAYERS; do
        VERSIONS=$(aws lambda list-layer-versions \
            --layer-name "$LAYER" \
            --query "LayerVersions[].Version" \
            --output text --no-paginate 2>/dev/null || echo "")
        for VER in $VERSIONS; do
            aws lambda delete-layer-version \
                --layer-name "$LAYER" --version-number "$VER" \
                --no-paginate 2>/dev/null \
                && ok "Layer version deleted: $LAYER:$VER" || err "Layer version: $LAYER:$VER"
        done
    done
else
    skip "Lambda layers"
fi

# ================================================================
# STEP 3 — S3: All Buckets (handles versioning & delete markers)
# ================================================================
hdr "STEP 3 of 11 │ S3 — All Buckets"

BUCKETS=$(aws s3api list-buckets \
    --query "Buckets[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$BUCKETS" && "$BUCKETS" != "None" ]]; then
    for BUCKET in $BUCKETS; do
        log "  Processing bucket: $BUCKET"
        # Delete all versioned objects
        VERSIONS=$(aws s3api list-object-versions --bucket "$BUCKET" --no-paginate \
            --query '{Objects: Versions[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null)
        OBJS=$(echo "$VERSIONS" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(len(d.get('Objects') or []))" 2>/dev/null || echo "0")
        if [[ "$OBJS" -gt 0 ]]; then
            echo "$VERSIONS" | python3 -c "
import sys, json, subprocess
d = json.load(sys.stdin)
objs = d.get('Objects') or []
if objs:
    payload = json.dumps({'Objects': objs, 'Quiet': True})
    subprocess.run(['aws','s3api','delete-objects','--bucket','$BUCKET',
        '--delete', payload,'--no-paginate'], capture_output=True)
" 2>/dev/null || true
        fi
        # Delete all delete markers
        MARKERS=$(aws s3api list-object-versions --bucket "$BUCKET" --no-paginate \
            --query '{Objects: DeleteMarkers[].{Key:Key,VersionId:VersionId}}' \
            --output json 2>/dev/null)
        MCOUNT=$(echo "$MARKERS" | python3 -c \
            "import sys,json; d=json.load(sys.stdin); print(len(d.get('Objects') or []))" 2>/dev/null || echo "0")
        if [[ "$MCOUNT" -gt 0 ]]; then
            echo "$MARKERS" | python3 -c "
import sys, json, subprocess
d = json.load(sys.stdin)
objs = d.get('Objects') or []
if objs:
    payload = json.dumps({'Objects': objs, 'Quiet': True})
    subprocess.run(['aws','s3api','delete-objects','--bucket','$BUCKET',
        '--delete', payload,'--no-paginate'], capture_output=True)
" 2>/dev/null || true
        fi
        aws s3 rb "s3://$BUCKET" --force --no-paginate > /dev/null 2>&1 \
            && ok "S3 bucket deleted: $BUCKET" || err "S3 bucket: $BUCKET"
    done
else
    skip "S3 buckets"
fi

# ================================================================
# STEP 4 — CloudWatch: Log Groups, Alarms, Dashboards
# ================================================================
hdr "STEP 4 of 11 │ CloudWatch — Log Groups, Alarms, Dashboards"

# 4a. Log groups
LOG_GROUPS=$(aws logs describe-log-groups \
    --query "logGroups[].logGroupName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$LOG_GROUPS" && "$LOG_GROUPS" != "None" ]]; then
    for LG in $LOG_GROUPS; do
        aws logs delete-log-group \
            --log-group-name "$LG" --no-paginate 2>/dev/null \
            && ok "Log group deleted: $LG" || err "Log group: $LG"
    done
else
    skip "CloudWatch log groups"
fi

# 4b. Alarms
CW_ALARMS=$(aws cloudwatch describe-alarms \
    --query "MetricAlarms[].AlarmName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$CW_ALARMS" && "$CW_ALARMS" != "None" ]]; then
    aws cloudwatch delete-alarms --alarm-names $CW_ALARMS --no-paginate 2>/dev/null \
        && ok "All CloudWatch alarms deleted" || err "Some CloudWatch alarms failed"
else
    skip "CloudWatch alarms"
fi

# 4c. Dashboards
CW_DASHBOARDS=$(aws cloudwatch list-dashboards \
    --query "DashboardEntries[].DashboardName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$CW_DASHBOARDS" && "$CW_DASHBOARDS" != "None" ]]; then
    for DASH in $CW_DASHBOARDS; do
        aws cloudwatch delete-dashboards \
            --dashboard-names "$DASH" --no-paginate 2>/dev/null \
            && ok "Dashboard deleted: $DASH" || err "Dashboard: $DASH"
    done
else
    skip "CloudWatch dashboards"
fi

# 4d. CloudWatch EventBridge rules (cron triggers used by Lambda)
log "  Fetching EventBridge rules..."
EB_RULES=$(aws events list-rules \
    --query "Rules[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$EB_RULES" && "$EB_RULES" != "None" ]]; then
    for RULE in $EB_RULES; do
        # Remove all targets first
        TARGETS=$(aws events list-targets-by-rule --rule "$RULE" \
            --query "Targets[].Id" --output text --no-paginate 2>/dev/null || echo "")
        if [[ -n "$TARGETS" && "$TARGETS" != "None" ]]; then
            aws events remove-targets --rule "$RULE" \
                --ids $TARGETS --no-paginate 2>/dev/null || true
        fi
        aws events delete-rule --name "$RULE" --no-paginate 2>/dev/null \
            && ok "EventBridge rule deleted: $RULE" || err "EventBridge rule: $RULE"
    done
else
    skip "EventBridge rules"
fi

# ================================================================
# STEP 5 — Athena: Named Queries & Custom Workgroups
# ================================================================
hdr "STEP 5 of 11 │ Athena — Named Queries & Workgroups"

# Clear named queries from primary workgroup
PRIMARY_QUERIES=$(aws athena list-named-queries --work-group "primary" \
    --query "NamedQueryIds[]" --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$PRIMARY_QUERIES" && "$PRIMARY_QUERIES" != "None" ]]; then
    for QID in $PRIMARY_QUERIES; do
        aws athena delete-named-query \
            --named-query-id "$QID" --no-paginate 2>/dev/null || true
    done
    ok "Athena named queries cleared from primary workgroup"
else
    skip "Athena named queries (primary workgroup)"
fi

# Delete custom workgroups (primary cannot be deleted)
WORKGROUPS=$(aws athena list-work-groups \
    --query "WorkGroups[?Name!='primary'].Name" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$WORKGROUPS" && "$WORKGROUPS" != "None" ]]; then
    for WG in $WORKGROUPS; do
        WG_QUERIES=$(aws athena list-named-queries --work-group "$WG" \
            --query "NamedQueryIds[]" --output text --no-paginate 2>/dev/null || echo "")
        for QID in $WG_QUERIES; do
            aws athena delete-named-query \
                --named-query-id "$QID" --no-paginate 2>/dev/null || true
        done
        aws athena delete-work-group \
            --work-group "$WG" \
            --recursive-delete-option \
            --no-paginate 2>/dev/null \
            && ok "Athena workgroup deleted: $WG" || err "Athena workgroup: $WG"
    done
else
    skip "custom Athena workgroups"
fi

# ================================================================
# STEP 6 — AWS Glue: Jobs, Triggers, Crawlers, Databases, Connections
# ================================================================
hdr "STEP 6 of 11 │ AWS Glue — Jobs, Triggers, Crawlers, Databases, Connections"

# 6a. Jobs
GLUE_JOBS=$(aws glue list-jobs --query "JobNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$GLUE_JOBS" && "$GLUE_JOBS" != "None" ]]; then
    for JOB in $GLUE_JOBS; do
        aws glue delete-job --job-name "$JOB" --no-paginate 2>/dev/null \
            && ok "Glue job deleted: $JOB" || err "Glue job: $JOB"
    done
else
    skip "Glue jobs"
fi

# 6b. Triggers
GLUE_TRIGGERS=$(aws glue list-triggers --query "TriggerNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$GLUE_TRIGGERS" && "$GLUE_TRIGGERS" != "None" ]]; then
    for TRIGGER in $GLUE_TRIGGERS; do
        aws glue delete-trigger --name "$TRIGGER" --no-paginate 2>/dev/null \
            && ok "Glue trigger deleted: $TRIGGER" || err "Glue trigger: $TRIGGER"
    done
else
    skip "Glue triggers"
fi

# 6c. Crawlers
GLUE_CRAWLERS=$(aws glue list-crawlers --query "CrawlerNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$GLUE_CRAWLERS" && "$GLUE_CRAWLERS" != "None" ]]; then
    for CRAWLER in $GLUE_CRAWLERS; do
        aws glue delete-crawler --name "$CRAWLER" --no-paginate 2>/dev/null \
            && ok "Glue crawler deleted: $CRAWLER" || err "Glue crawler: $CRAWLER"
    done
else
    skip "Glue crawlers"
fi

# 6d. Databases (and all tables inside)
GLUE_DBS=$(aws glue get-databases --query "DatabaseList[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$GLUE_DBS" && "$GLUE_DBS" != "None" ]]; then
    for DB in $GLUE_DBS; do
        TABLES=$(aws glue get-tables --database-name "$DB" \
            --query "TableList[].Name" --output text --no-paginate 2>/dev/null || echo "")
        for TABLE in $TABLES; do
            aws glue delete-table --database-name "$DB" --name "$TABLE" \
                --no-paginate 2>/dev/null || true
        done
        aws glue delete-database --name "$DB" --no-paginate 2>/dev/null \
            && ok "Glue database deleted: $DB (with all tables)" || err "Glue database: $DB"
    done
else
    skip "Glue databases"
fi

# 6e. Connections
GLUE_CONNS=$(aws glue get-connections --query "ConnectionList[].Name" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$GLUE_CONNS" && "$GLUE_CONNS" != "None" ]]; then
    for CONN in $GLUE_CONNS; do
        aws glue delete-connection --connection-name "$CONN" --no-paginate 2>/dev/null \
            && ok "Glue connection deleted: $CONN" || err "Glue connection: $CONN"
    done
else
    skip "Glue connections"
fi

# ================================================================
# STEP 7 — Amazon SageMaker: Endpoints, Models, Notebooks,
#           Training Jobs, Pipelines, Domains, Feature Groups
# ================================================================
hdr "STEP 7 of 11 │ SageMaker — Endpoints, Models, Notebooks, Pipelines, Domains"

# 7a. Endpoints (most expensive — delete first)
SM_ENDPOINTS=$(aws sagemaker list-endpoints \
    --query "Endpoints[].EndpointName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_ENDPOINTS" && "$SM_ENDPOINTS" != "None" ]]; then
    for EP in $SM_ENDPOINTS; do
        aws sagemaker delete-endpoint \
            --endpoint-name "$EP" --no-paginate 2>/dev/null \
            && ok "SageMaker endpoint deleted: $EP" || err "SageMaker endpoint: $EP"
    done
else
    skip "SageMaker endpoints"
fi

# 7b. Endpoint Configs
SM_CONFIGS=$(aws sagemaker list-endpoint-configs \
    --query "EndpointConfigs[].EndpointConfigName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_CONFIGS" && "$SM_CONFIGS" != "None" ]]; then
    for CFG in $SM_CONFIGS; do
        aws sagemaker delete-endpoint-config \
            --endpoint-config-name "$CFG" --no-paginate 2>/dev/null \
            && ok "Endpoint config deleted: $CFG" || err "Endpoint config: $CFG"
    done
else
    skip "SageMaker endpoint configs"
fi

# 7c. Models
SM_MODELS=$(aws sagemaker list-models \
    --query "Models[].ModelName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_MODELS" && "$SM_MODELS" != "None" ]]; then
    for MODEL in $SM_MODELS; do
        aws sagemaker delete-model \
            --model-name "$MODEL" --no-paginate 2>/dev/null \
            && ok "SageMaker model deleted: $MODEL" || err "SageMaker model: $MODEL"
    done
else
    skip "SageMaker models"
fi

# 7d. Notebook Instances (stop first, then delete)
SM_NOTEBOOKS=$(aws sagemaker list-notebook-instances \
    --query "NotebookInstances[].NotebookInstanceName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_NOTEBOOKS" && "$SM_NOTEBOOKS" != "None" ]]; then
    for NB in $SM_NOTEBOOKS; do
        STATUS=$(aws sagemaker describe-notebook-instance \
            --notebook-instance-name "$NB" \
            --query "NotebookInstanceStatus" --output text --no-paginate 2>/dev/null)
        if [[ "$STATUS" == "InService" ]]; then
            log "    Stopping notebook: $NB"
            aws sagemaker stop-notebook-instance \
                --notebook-instance-name "$NB" --no-paginate 2>/dev/null || true
            aws sagemaker wait notebook-instance-stopped \
                --notebook-instance-name "$NB" --no-paginate 2>/dev/null || true
        fi
        aws sagemaker delete-notebook-instance \
            --notebook-instance-name "$NB" --no-paginate 2>/dev/null \
            && ok "Notebook instance deleted: $NB" || err "Notebook instance: $NB"
    done
else
    skip "SageMaker notebook instances"
fi

# 7e. Stop running training jobs
SM_TRAINING=$(aws sagemaker list-training-jobs \
    --status-equals InProgress \
    --query "TrainingJobSummaries[].TrainingJobName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_TRAINING" && "$SM_TRAINING" != "None" ]]; then
    for JOB in $SM_TRAINING; do
        aws sagemaker stop-training-job \
            --training-job-name "$JOB" --no-paginate 2>/dev/null \
            && ok "Training job stopped: $JOB" || err "Training job: $JOB"
    done
else
    skip "running SageMaker training jobs"
fi

# 7f. Pipelines
SM_PIPELINES=$(aws sagemaker list-pipelines \
    --query "PipelineSummaries[].PipelineName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_PIPELINES" && "$SM_PIPELINES" != "None" ]]; then
    for PIPELINE in $SM_PIPELINES; do
        aws sagemaker delete-pipeline \
            --pipeline-name "$PIPELINE" --no-paginate 2>/dev/null \
            && ok "SageMaker pipeline deleted: $PIPELINE" || err "SageMaker pipeline: $PIPELINE"
    done
else
    skip "SageMaker pipelines"
fi

# 7g. Feature Groups
SM_FG=$(aws sagemaker list-feature-groups \
    --query "FeatureGroupSummaries[].FeatureGroupName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_FG" && "$SM_FG" != "None" ]]; then
    for FG in $SM_FG; do
        aws sagemaker delete-feature-group \
            --feature-group-name "$FG" --no-paginate 2>/dev/null \
            && ok "Feature group deleted: $FG" || err "Feature group: $FG"
    done
else
    skip "SageMaker feature groups"
fi

# 7h. Studio Domains (delete user profiles + apps first)
SM_DOMAINS=$(aws sagemaker list-domains \
    --query "Domains[].DomainId" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SM_DOMAINS" && "$SM_DOMAINS" != "None" ]]; then
    for DOMAIN in $SM_DOMAINS; do
        USERS=$(aws sagemaker list-user-profiles \
            --domain-id-equals "$DOMAIN" \
            --query "UserProfiles[].UserProfileName" \
            --output text --no-paginate 2>/dev/null || echo "")
        for USER in $USERS; do
            APPS=$(aws sagemaker list-apps \
                --domain-id-equals "$DOMAIN" \
                --user-profile-name-equals "$USER" \
                --query "Apps[?Status!='Deleted'].{Name:AppName,Type:AppType}" \
                --output text --no-paginate 2>/dev/null || echo "")
            while IFS=$'\t' read -r APP_NAME APP_TYPE; do
                [[ -z "$APP_NAME" ]] && continue
                aws sagemaker delete-app \
                    --domain-id "$DOMAIN" --user-profile-name "$USER" \
                    --app-type "$APP_TYPE" --app-name "$APP_NAME" \
                    --no-paginate 2>/dev/null && ok "App deleted: $APP_NAME" || true
            done <<< "$APPS"
            aws sagemaker delete-user-profile \
                --domain-id "$DOMAIN" --user-profile-name "$USER" \
                --no-paginate 2>/dev/null \
                && ok "User profile deleted: $USER" || err "User profile: $USER"
        done
        aws sagemaker delete-domain \
            --domain-id "$DOMAIN" \
            --retention-policy '{"HomeEfsFileSystem":"Delete"}' \
            --no-paginate 2>/dev/null \
            && ok "SageMaker domain deleted: $DOMAIN" || err "SageMaker domain: $DOMAIN"
    done
else
    skip "SageMaker domains"
fi

# ================================================================
# STEP 8 — Step Functions: State Machines & Activities
# ================================================================
hdr "STEP 8 of 11 │ Step Functions — State Machines & Activities"

# 8a. State machines (stop running executions first)
SF_MACHINES=$(aws stepfunctions list-state-machines \
    --query "stateMachines[].stateMachineArn" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SF_MACHINES" && "$SF_MACHINES" != "None" ]]; then
    for SM in $SF_MACHINES; do
        RUNNING=$(aws stepfunctions list-executions \
            --state-machine-arn "$SM" --status-filter RUNNING \
            --query "executions[].executionArn" \
            --output text --no-paginate 2>/dev/null || echo "")
        for EXEC in $RUNNING; do
            aws stepfunctions stop-execution \
                --execution-arn "$EXEC" --no-paginate 2>/dev/null || true
        done
        aws stepfunctions delete-state-machine \
            --state-machine-arn "$SM" --no-paginate 2>/dev/null \
            && ok "State machine deleted: $SM" || err "State machine: $SM"
    done
else
    skip "Step Functions state machines"
fi

# 8b. Activities
SF_ACTIVITIES=$(aws stepfunctions list-activities \
    --query "activities[].activityArn" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$SF_ACTIVITIES" && "$SF_ACTIVITIES" != "None" ]]; then
    for ACT in $SF_ACTIVITIES; do
        aws stepfunctions delete-activity \
            --activity-arn "$ACT" --no-paginate 2>/dev/null \
            && ok "Activity deleted: $ACT" || err "Activity: $ACT"
    done
else
    skip "Step Functions activities"
fi

# ================================================================
# STEP 9 — DynamoDB: All Tables
# ================================================================
hdr "STEP 9 of 11 │ DynamoDB — All Tables"

DYNAMO_TABLES=$(aws dynamodb list-tables \
    --query "TableNames[]" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$DYNAMO_TABLES" && "$DYNAMO_TABLES" != "None" ]]; then
    for TABLE in $DYNAMO_TABLES; do
        aws dynamodb delete-table \
            --table-name "$TABLE" --no-paginate 2>/dev/null \
            && ok "DynamoDB table deleted: $TABLE" || err "DynamoDB table: $TABLE"
    done
else
    skip "DynamoDB tables"
fi

# ================================================================
# STEP 10 — Aurora & RDS: Instances, Clusters, Snapshots, Subnet Groups
# ================================================================
hdr "STEP 10 of 11 │ Aurora & RDS — Instances, Clusters, Snapshots, Subnet Groups"

# 10a. Delete all DB instances first
RDS_INSTANCES=$(aws rds describe-db-instances \
    --query "DBInstances[].DBInstanceIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$RDS_INSTANCES" && "$RDS_INSTANCES" != "None" ]]; then
    for DB in $RDS_INSTANCES; do
        log "  Deleting RDS instance: $DB (no final snapshot)..."
        aws rds delete-db-instance \
            --db-instance-identifier "$DB" \
            --skip-final-snapshot \
            --delete-automated-backups \
            --no-paginate 2>/dev/null \
            && ok "RDS instance delete initiated: $DB" || err "RDS instance: $DB"
    done
    log "  Waiting for all RDS instances to be deleted (~5 min)..."
    for DB in $RDS_INSTANCES; do
        aws rds wait db-instance-deleted \
            --db-instance-identifier "$DB" --no-paginate 2>/dev/null \
            && ok "RDS instance fully deleted: $DB" || err "Timed out waiting for: $DB"
    done
else
    skip "RDS instances"
fi

# 10b. Delete all DB clusters
RDS_CLUSTERS=$(aws rds describe-db-clusters \
    --query "DBClusters[].DBClusterIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$RDS_CLUSTERS" && "$RDS_CLUSTERS" != "None" ]]; then
    for CLUSTER in $RDS_CLUSTERS; do
        log "  Deleting RDS cluster: $CLUSTER (no final snapshot)..."
        aws rds delete-db-cluster \
            --db-cluster-identifier "$CLUSTER" \
            --skip-final-snapshot \
            --no-paginate 2>/dev/null \
            && ok "RDS cluster delete initiated: $CLUSTER" || err "RDS cluster: $CLUSTER"
    done
    for CLUSTER in $RDS_CLUSTERS; do
        aws rds wait db-cluster-deleted \
            --db-cluster-identifier "$CLUSTER" --no-paginate 2>/dev/null \
            && ok "RDS cluster fully deleted: $CLUSTER" || err "Timed out waiting for: $CLUSTER"
    done
else
    skip "RDS clusters"
fi

# 10c. Delete manual DB snapshots
RDS_SNAPS=$(aws rds describe-db-snapshots \
    --snapshot-type manual \
    --query "DBSnapshots[].DBSnapshotIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$RDS_SNAPS" && "$RDS_SNAPS" != "None" ]]; then
    for SNAP in $RDS_SNAPS; do
        aws rds delete-db-snapshot \
            --db-snapshot-identifier "$SNAP" --no-paginate 2>/dev/null \
            && ok "RDS snapshot deleted: $SNAP" || err "RDS snapshot: $SNAP"
    done
else
    skip "manual RDS snapshots"
fi

# 10d. Delete manual cluster snapshots
CLUSTER_SNAPS=$(aws rds describe-db-cluster-snapshots \
    --snapshot-type manual \
    --query "DBClusterSnapshots[].DBClusterSnapshotIdentifier" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$CLUSTER_SNAPS" && "$CLUSTER_SNAPS" != "None" ]]; then
    for SNAP in $CLUSTER_SNAPS; do
        aws rds delete-db-cluster-snapshot \
            --db-cluster-snapshot-identifier "$SNAP" --no-paginate 2>/dev/null \
            && ok "Cluster snapshot deleted: $SNAP" || err "Cluster snapshot: $SNAP"
    done
else
    skip "manual cluster snapshots"
fi

# 10e. Delete custom DB subnet groups
DB_SUBNETS=$(aws rds describe-db-subnet-groups \
    --query "DBSubnetGroups[?DBSubnetGroupName!='default'].DBSubnetGroupName" \
    --output text --no-paginate 2>/dev/null || echo "")
if [[ -n "$DB_SUBNETS" && "$DB_SUBNETS" != "None" ]]; then
    for SG in $DB_SUBNETS; do
        aws rds delete-db-subnet-group \
            --db-subnet-group-name "$SG" --no-paginate 2>/dev/null \
            && ok "DB subnet group deleted: $SG" || err "DB subnet group: $SG"
    done
else
    skip "custom DB subnet groups"
fi

# ================================================================
# STEP 11 — VPC: Non-default VPCs & All Sub-Resources
# ================================================================
hdr "STEP 11 of 11 │ VPC — Non-default VPCs & Sub-Resources"

VPC_IDS=$(aws ec2 describe-vpcs \
    --filters "Name=isDefault,Values=false" \
    --query "Vpcs[].VpcId" \
    --output text --no-paginate 2>/dev/null || echo "")

if [[ -n "$VPC_IDS" && "$VPC_IDS" != "None" ]]; then
    for VPC in $VPC_IDS; do
        log "  Cleaning VPC: $VPC"

        # NAT Gateways (must delete before subnets/IGW)
        NAT_IDS=$(aws ec2 describe-nat-gateways \
            --filter "Name=vpc-id,Values=$VPC" \
            --query "NatGateways[?State!='deleted'].NatGatewayId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for NAT in $NAT_IDS; do
            aws ec2 delete-nat-gateway \
                --nat-gateway-id "$NAT" --no-paginate > /dev/null 2>&1 \
                && ok "NAT gateway delete initiated: $NAT" || err "NAT gateway: $NAT"
        done
        [[ -n "$NAT_IDS" ]] && sleep 5

        # Internet Gateways
        IGW_IDS=$(aws ec2 describe-internet-gateways \
            --filters "Name=attachment.vpc-id,Values=$VPC" \
            --query "InternetGateways[].InternetGatewayId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for IGW in $IGW_IDS; do
            aws ec2 detach-internet-gateway \
                --internet-gateway-id "$IGW" --vpc-id "$VPC" \
                --no-paginate 2>/dev/null || true
            aws ec2 delete-internet-gateway \
                --internet-gateway-id "$IGW" --no-paginate 2>/dev/null \
                && ok "Internet gateway deleted: $IGW" || err "IGW: $IGW"
        done

        # Non-default Security Groups
        SG_IDS=$(aws ec2 describe-security-groups \
            --filters "Name=vpc-id,Values=$VPC" \
            --query "SecurityGroups[?GroupName!='default'].GroupId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for SG in $SG_IDS; do
            aws ec2 delete-security-group \
                --group-id "$SG" --no-paginate 2>/dev/null \
                && ok "Security group deleted: $SG" || skip "SG $SG (in use)"
        done

        # Non-main Route Tables
        RT_IDS=$(aws ec2 describe-route-tables \
            --filters "Name=vpc-id,Values=$VPC" \
            --query "RouteTables[?Associations[0].Main!=\`true\`].RouteTableId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for RT in $RT_IDS; do
            aws ec2 delete-route-table \
                --route-table-id "$RT" --no-paginate 2>/dev/null \
                && ok "Route table deleted: $RT" || skip "Route table $RT (in use)"
        done

        # Subnets
        SUBNET_IDS=$(aws ec2 describe-subnets \
            --filters "Name=vpc-id,Values=$VPC" \
            --query "Subnets[].SubnetId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for SUBNET in $SUBNET_IDS; do
            aws ec2 delete-subnet \
                --subnet-id "$SUBNET" --no-paginate 2>/dev/null \
                && ok "Subnet deleted: $SUBNET" || err "Subnet: $SUBNET"
        done

        # Network ACLs (non-default)
        NACL_IDS=$(aws ec2 describe-network-acls \
            --filters "Name=vpc-id,Values=$VPC" \
            --query "NetworkAcls[?IsDefault==\`false\`].NetworkAclId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for NACL in $NACL_IDS; do
            aws ec2 delete-network-acl \
                --network-acl-id "$NACL" --no-paginate 2>/dev/null \
                && ok "Network ACL deleted: $NACL" || skip "NACL $NACL (default/in use)"
        done

        # VPC Endpoints
        ENDPOINTS=$(aws ec2 describe-vpc-endpoints \
            --filters "Name=vpc-id,Values=$VPC" \
            --query "VpcEndpoints[?State!='deleted'].VpcEndpointId" \
            --output text --no-paginate 2>/dev/null || echo "")
        for EP in $ENDPOINTS; do
            aws ec2 delete-vpc-endpoints \
                --vpc-endpoint-ids "$EP" --no-paginate 2>/dev/null \
                && ok "VPC endpoint deleted: $EP" || err "VPC endpoint: $EP"
        done

        # Delete the VPC itself
        aws ec2 delete-vpc --vpc-id "$VPC" --no-paginate 2>/dev/null \
            && ok "VPC deleted: $VPC" \
            || err "Could not delete VPC: $VPC (check for remaining dependencies)"
    done
else
    skip "non-default VPCs"
fi

# ================================================================
# SUMMARY
# ================================================================
hdr "CLEANUP COMPLETE"

log ""
log "  Services processed:"
log "    ✔  1. IAM            — Roles, Policies, Users, Groups"
log "    ✔  2. Lambda         — Functions, Layers, Event Source Mappings"
log "    ✔  3. S3             — All buckets (versioned-aware)"
log "    ✔  4. CloudWatch     — Log groups, Alarms, Dashboards, EventBridge rules"
log "    ✔  5. Athena         — Named queries, custom workgroups"
log "    ✔  6. AWS Glue       — Jobs, Triggers, Crawlers, Databases, Connections"
log "    ✔  7. SageMaker      — Endpoints, Models, Notebooks, Pipelines, Domains"
log "    ✔  8. Step Functions — State machines, Activities"
log "    ✔  9. DynamoDB       — All tables"
log "    ✔ 10. Aurora & RDS   — Instances, Clusters, Snapshots, Subnet groups"
log "    ✔ 11. VPC            — Non-default VPCs + all sub-resources"
log "    ─  Billing           — No deletable resources (skipped by design)"
log ""

if [[ "$ERRORS" -gt 0 ]]; then
    log "${RED}${BOLD}  ⚠️  Completed with $ERRORS error(s). Review log: $LOG_FILE${RESET}"
else
    log "${GREEN}${BOLD}  ✅ All done — no errors. Log: $LOG_FILE${RESET}"
fi
log ""
