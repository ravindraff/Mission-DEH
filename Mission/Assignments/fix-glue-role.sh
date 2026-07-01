#!/bin/bash
# ================================================================
# Fix: Glue Notebook AccessDeniedException — iam:PassRole
# ================================================================
# Error: mission-deh-hof-glue-role cannot perform iam:PassRole
#        on itself, so Glue Interactive Sessions (notebooks) fail.
#
# Fix: Attach an inline policy that allows the role to pass itself.
# ================================================================

export AWS_PAGER=""

ROLE_NAME="mission-deh-hof-glue-role"
POLICY_NAME="GluePassRolePolicy"

# Get account ID dynamically
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --no-paginate)
echo "[INFO] Account ID : $ACCOUNT_ID"
echo "[INFO] Role       : $ROLE_NAME"
echo ""

# Build the inline policy document
POLICY_DOC=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowPassSelf",
      "Effect": "Allow",
      "Action": "iam:PassRole",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}",
      "Condition": {
        "StringEquals": {
          "iam:PassedToService": "glue.amazonaws.com"
        }
      }
    },
    {
      "Sid": "AllowGlueInteractiveSessions",
      "Effect": "Allow",
      "Action": [
        "glue:CreateSession",
        "glue:DeleteSession",
        "glue:GetSession",
        "glue:ListSessions",
        "glue:StopSession",
        "glue:RunStatement",
        "glue:GetStatement",
        "glue:ListStatements",
        "glue:CancelStatement"
      ],
      "Resource": "*"
    }
  ]
}
EOF
)

echo "[INFO] Attaching inline policy: $POLICY_NAME"
aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "$POLICY_NAME" \
    --policy-document "$POLICY_DOC" \
    --no-paginate

if [[ $? -eq 0 ]]; then
    echo ""
    echo "✅ Policy attached successfully!"
    echo ""
    echo "   Role     : $ROLE_NAME"
    echo "   Policy   : $POLICY_NAME"
    echo ""
    echo "   Go back to your Glue notebook and re-run the first cell."
    echo "   The session should now create successfully."
else
    echo ""
    echo "❌ Failed to attach policy. Check your AWS credentials and permissions."
    exit 1
fi
