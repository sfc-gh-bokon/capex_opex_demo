#!/usr/bin/env bash
# Deploys one release of the CapEx/OpEx pipeline to one environment.
# Same script runs locally and in GitHub Actions, so what was tested is what gets promoted.
#
#   scripts/deploy.sh <DEV|UAT|PROD> <release-alias> [pipeline-run-url]
#
# Locally:  SNOW_CONN="-c <your-connection>" scripts/deploy.sh DEV v0.2.0
# In CI:    SNOW_CONN="-x" (connection assembled from SNOWFLAKE_* env + OIDC)
#
# What is promoted: DCM definitions (pipeline, transformations, criteria logic, policies, grants)
# and the reviewer app. What is NOT promoted: source data, UAT test data, AI suggestions, or
# reviewer decisions. Those tables are environment-local.
set -euo pipefail

ENV="${1:?env required: DEV|UAT|PROD}"
ALIAS="${2:?release alias required}"
RUN_URL="${3:-local}"
SNOW="${SNOW_BIN:-snow}"
CONN=${SNOW_CONN:--x}
ROLE="CAPEX_${ENV}_DEPLOYER"
WH="CAPEX_${ENV}_WH"
DB="CAPEX_${ENV}"
SHA="$(git rev-parse --short HEAD 2>/dev/null || echo unknown)"
cd "$(dirname "$0")/.."

echo "::group::DCM plan ${ENV}"
$SNOW dcm describe "${DB}.DCM.CAPEX_PIPELINE" $CONN --role "$ROLE" >/dev/null 2>&1 \
  || $SNOW dcm create --target "$ENV" $CONN --role "$ROLE" --warehouse "$WH"
$SNOW dcm plan --target "$ENV" $CONN --role "$ROLE" --warehouse "$WH" --save-output
echo "::endgroup::"

echo "::group::DCM deploy ${ENV} (${ALIAS})"
$SNOW dcm deploy --target "$ENV" $CONN --role "$ROLE" --warehouse "$WH" --alias "${ALIAS}-${SHA}"
echo "::endgroup::"

echo "::group::Load version-controlled reference data and governance attachments"
$SNOW sql $CONN --role "$ROLE" --warehouse "$WH" -q "
  CALL ${DB}.CONTROL.SEED_REFERENCE_DATA();
  CALL ${DB}.GOVERNANCE.APPLY_POLICIES();"
echo "::endgroup::"

echo "::group::Deploy reviewer app"
(cd app && $SNOW streamlit deploy capex_review_app --replace $CONN --role "$ROLE" --warehouse "$WH" \
    --env CAPEX_DB="$DB" --env CAPEX_WH="$WH")
$SNOW sql $CONN --role "$ROLE" --warehouse "$WH" -q "
  GRANT USAGE ON SCHEMA ${DB}.APP TO ROLE CAPEX_FIN_REVIEWER;
  GRANT USAGE ON STREAMLIT ${DB}.APP.CAPEX_REVIEW_APP TO ROLE CAPEX_FIN_REVIEWER;"
echo "::endgroup::"

echo "::group::Post-deployment checks"
RESULT=$($SNOW sql $CONN --role "$ROLE" --warehouse "$WH" --format json -q \
  "CALL ${DB}.CONTROL.POST_DEPLOY_CHECKS('${ALIAS}', '${SHA}', '${RUN_URL}')")
echo "$RESULT"
echo "::endgroup::"
echo "$RESULT" | python3 -c '
import json, sys
row = json.load(sys.stdin)[0]
val = next(iter(row.values()))
val = json.loads(val) if isinstance(val, str) else val
for c in val["checks"]:
    print(("PASS " if c["pass"] else "FAIL ") + c["check"])
sys.exit(0 if val["status"] == "SUCCEEDED" else 1)
' || { echo "Post-deployment checks failed"; exit 1; }
echo "Deployed ${ALIAS} (${SHA}) to ${ENV}"
