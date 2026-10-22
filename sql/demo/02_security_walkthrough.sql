-- =====================================================================
-- MULTI-TEAM SECURITY WALKTHROUGH
-- Same object, same query, three teams, three different answers.
-- IMPORTANT: USE SECONDARY ROLES NONE so only the demonstrated role is active.
-- =====================================================================

-- ---------------------------------------------------------------------
-- A. The model: functional (account) roles -> access (database) roles per environment
-- ---------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;
SHOW DATABASE ROLES IN DATABASE CAPEX_PROD;
SHOW GRANTS TO DATABASE ROLE CAPEX_PROD.REVIEWER;
SHOW GRANTS TO ROLE CAPEX_DATA_ENG;      -- observer in UAT/PROD; builder only in DEV
SHOW GRANTS OF ROLE CAPEX_PROD_DEPLOYER; -- only the CI service user deploys to PROD
DESCRIBE USER SVC_CAPEX_GHA_PROD;        -- TYPE=SERVICE, WORKLOAD_IDENTITY (OIDC), no password/key

-- ---------------------------------------------------------------------
-- B. Same query, three teams
-- ---------------------------------------------------------------------
USE ROLE CAPEX_FIN_REVIEWER;  USE SECONDARY ROLES NONE; USE WAREHOUSE CAPEX_DEV_WH;
SELECT CURRENT_ROLE() ROLE_, COST_CENTER, PROJECT_NAME, CONTRACTOR_NAME, HOURLY_RATE, AMOUNT
FROM CAPEX_DEV.GOLD.COST_LINE_DETAIL ORDER BY COST_CENTER, PROJECT_NAME;     -- all rows, all values

USE ROLE CAPEX_PMO_ANALYST;   USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() ROLE_, COST_CENTER, PROJECT_NAME, CONTRACTOR_NAME, HOURLY_RATE, AMOUNT
FROM CAPEX_DEV.GOLD.COST_LINE_DETAIL ORDER BY COST_CENTER, PROJECT_NAME;     -- CC-4100/5200 only, masked

USE ROLE CAPEX_ITSEC_ANALYST; USE SECONDARY ROLES NONE;
SELECT CURRENT_ROLE() ROLE_, COST_CENTER, PROJECT_NAME, CONTRACTOR_NAME, HOURLY_RATE, AMOUNT
FROM CAPEX_DEV.GOLD.COST_LINE_DETAIL ORDER BY COST_CENTER, PROJECT_NAME;     -- CC-9000/9300 only, masked

-- Independence: other teams cannot see the review workspace or raw layers at all
SELECT * FROM CAPEX_DEV.REVIEW.REVIEW_QUEUE LIMIT 1;      -- expected: does not exist or not authorized
SELECT * FROM CAPEX_DEV.BRONZE.RAW_RECORDS LIMIT 1;       -- expected: does not exist or not authorized

-- Append-only audit: even reviewers cannot alter history
USE ROLE CAPEX_FIN_REVIEWER; USE SECONDARY ROLES NONE;
DELETE FROM CAPEX_DEV.REVIEW.DECISION_HISTORY;            -- expected: insufficient privileges

-- ---------------------------------------------------------------------
-- C. How the protection is defined (all in the DCM project, version-controlled)
-- ---------------------------------------------------------------------
USE ROLE CAPEX_DEV_DEPLOYER; USE SECONDARY ROLES ALL;
SELECT * FROM TABLE(CAPEX_DEV.INFORMATION_SCHEMA.POLICY_REFERENCES(REF_ENTITY_NAME => 'CAPEX_DEV.GOLD.COST_LINE_DETAIL', REF_ENTITY_DOMAIN => 'TABLE'));
SELECT * FROM TABLE(CAPEX_DEV.INFORMATION_SCHEMA.TAG_REFERENCES_ALL_COLUMNS('CAPEX_DEV.GOLD.COST_LINE_DETAIL', 'TABLE'));
SELECT GET_DDL('POLICY', 'CAPEX_DEV.GOVERNANCE.MASK_SENSITIVE_STRING');
SELECT GET_DDL('POLICY', 'CAPEX_DEV.GOVERNANCE.RAP_COST_CENTER');
SELECT * FROM CAPEX_DEV.GOVERNANCE.COST_CENTER_ACCESS;

-- ---------------------------------------------------------------------
-- D. Authentication controls (talk track; do not run against the shared demo account)
-- ---------------------------------------------------------------------
-- Humans: SAML SSO (Entra ID / Okta) + SCIM provisioning of users and team groups -> roles
-- Services: WORKLOAD_IDENTITY (OIDC) for CI; key-pair or OAuth for integrations; no passwords
-- Network: account policy + per-user policies (e.g. CAPEX_GHA_NETWORK_POLICY for CI runners)
-- Example authentication policy (MFA for people, OIDC-only for CI):
--   CREATE AUTHENTICATION POLICY CAPEX_HUMAN_AUTH
--     AUTHENTICATION_METHODS = ('SAML') MFA_ENROLLMENT = REQUIRED CLIENT_TYPES = ('SNOWFLAKE_UI', 'DRIVERS');
--   CREATE AUTHENTICATION POLICY CAPEX_CI_AUTH AUTHENTICATION_METHODS = ('WORKLOAD_IDENTITY');
--   ALTER USER SVC_CAPEX_GHA_PROD SET AUTHENTICATION POLICY CAPEX_CI_AUTH;
SHOW NETWORK POLICIES LIKE 'CAPEX%';
