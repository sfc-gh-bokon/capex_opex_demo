# CapEx/OpEx ingestion and classification review on Snowflake

An end-to-end demo pipeline. It lands project financial data from Clarity, ServiceNow and Azure DevOps on a 30-minute schedule and models it through Bronze, Silver and Gold layers. Finance-approved rules run first, then AI flags potential CapEx↔OpEx reclassifications for Finance review. One version-controlled release is promoted DEV → UAT → PROD.

```
SRC_MOCK (Clarity / ServiceNow / ADO "APIs")
   │  BRONZE.INGEST_SOURCE  (task DAG, 30 min; paging, retry, watermark, run log)
   ▼
BRONZE.RAW_RECORDS  ── append-only payload + source/ingest timestamps + run id
   │  dynamic tables
   ▼
SILVER.PROJECT / COST_LINE / WORK_ITEM / SERVICENOW_RECORD / COST_LINE_EVIDENCE / DQ_EXCEPTIONS
   │  dynamic tables
   ▼
GOLD.PROJECT_FINANCIALS / COST_LINE_DETAIL / RULE_EVALUATION (+ RECONCILIATION view)
   │  stream on Bronze → task → REVIEW.RUN_AI_REVIEW (AI_CLASSIFY + AI_COMPLETE)
   ▼
REVIEW.AI_SUGGESTION → REVIEW_QUEUE → Streamlit app → REVIEW.RECORD_DECISION → DECISION_HISTORY (append-only)
```

## Layout

| Path | Purpose |
|---|---|
| `manifest.yml` | DCM project: DEV / UAT / PROD targets and per-environment templating |
| `sources/definitions/*.sql` | All objects as `DEFINE` statements: schemas, tables, dynamic tables, streams, tasks, procedures, policies, tags, DMFs, roles, grants |
| `app/` | Streamlit reviewer app (`snow streamlit deploy`) |
| `scripts/deploy.sh` | The single promotion path, used locally and by CI |
| `scripts/check_eval.py` | AI regression gate (blocks a release below 90% on reviewed examples) |
| `.github/workflows/` | `validate-pr` (plan), `release` (DEV → UAT → PROD with approval), `rollback` |
| `sql/00_platform_setup.sql` | One-time account setup: environment databases, warehouses, roles (ACCOUNTADMIN) |
| `sql/01_cicd_service_users.sql` | One-time CI identities: one GitHub Actions service user per environment (ACCOUNTADMIN) |
| `.github/actions/snowflake-login/` | Shared login step: OIDC by default, key pair if a secret is set |
| `sql/demo/` | Reset, demo runbook, security walkthrough |

## Set up in your account

You need ACCOUNTADMIN once, Snowflake CLI 3.x, and a GitHub repository with Actions enabled. Nothing in this repo contains credentials; you supply them in Snowflake and GitHub settings.

1. **Point the project at your account.** In `manifest.yml`, replace `MYORG-MYACCOUNT` in all three targets with your `<org>-<account>` identifier (`SELECT CURRENT_ORGANIZATION_NAME() || '-' || CURRENT_ACCOUNT_NAME();`).
2. **Create environments and roles.** Run `sql/00_platform_setup.sql` as ACCOUNTADMIN. It creates `CAPEX_DEV/UAT/PROD`, their warehouses, deployer roles and team roles, and grants them to you (`DEMO_USER`, default `CURRENT_USER()`).
3. **First deploy from your laptop** (creates the DCM projects, seeds reference data, applies policies, deploys the app):
   ```bash
   snow connection add            # once; any auth method your account allows
   SNOW_CONN="-c <your-connection>" bash scripts/deploy.sh DEV r0
   ```
   Load demo data with `sql/demo/00_reset_dev.sql`.
4. **Create the CI identities.** Set `GITHUB_REPO` at the top of `sql/01_cicd_service_users.sql` to your `<owner>/<repo>` and run it as ACCOUNTADMIN. It creates `SVC_CAPEX_GHA_DEV/UAT/PROD` (TYPE = SERVICE), each trusting only GitHub jobs from your repo running in the matching GitHub environment. If your account has a network policy, uncomment the network-policy block.
5. **Configure GitHub** (Settings of your repo):

   | Where | Name | Value |
   |---|---|---|
   | Environments | `dev`, `uat`, `prod` | Create all three. On `prod`, add **Required reviewers** (this is the change-approval gate). |
   | Actions » Variables (repository) | `SNOWFLAKE_ACCOUNT` | Your account identifier, e.g. `MYORG-MYACCOUNT` |
   | Actions » Variables (per environment, optional) | `SNOWFLAKE_USER` | Only if you renamed the service users. Default `SVC_CAPEX_GHA_<ENV>` |
   | Environment secrets (optional) | `SNOWFLAKE_PRIVATE_KEY` | Only for key-pair auth instead of OIDC; see the end of `01_cicd_service_users.sql` |

6. **Release.** Push to `main` (or run *Release CapEx/OpEx pipeline* manually). DEV deploys and passes the AI accuracy gate, UAT deploys, then PROD waits for the reviewer's approval.

The environment names (`dev`, `uat`, `prod`) matter for OIDC: Snowflake only accepts a token whose subject is `repo:<owner>/<repo>:environment:<name>`, so a job can only act as the service user for its own environment.


## Promote

```bash
# local (same as CI)
SNOW_CONN="-c <connection>" scripts/deploy.sh UAT r1
# CI: push to main → release.yml → dev → uat → prod (required reviewer on `prod` environment)
# rollback: Actions → "Roll back an environment" → choose env + known-good tag
```

What moves between environments: definitions, procedures, criteria and the app. What does not move: source data, UAT fixtures, AI suggestions and reviewer decisions.
