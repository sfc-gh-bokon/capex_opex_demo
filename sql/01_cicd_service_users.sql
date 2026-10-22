-- One-time CI/CD identity setup (run as ACCOUNTADMIN, after 00_platform_setup.sql).
-- Creates one service user per environment for GitHub Actions. Each user holds only that
-- environment's deployer role, so a DEV job can never deploy to PROD.
--
-- EDIT THIS: set GITHUB_REPO to your repository, as <owner>/<repo>.
SET GITHUB_REPO = 'my-org/capex-opex-demo';

USE ROLE ACCOUNTADMIN;

-- Default: GitHub OIDC (workload identity federation). No password, key or token is stored
-- anywhere. Snowflake trusts a GitHub-issued token only when it comes from this repo AND
-- the job runs in the matching GitHub environment (dev / uat / prod).
EXECUTE IMMEDIATE $$
DECLARE
  envs ARRAY DEFAULT ARRAY_CONSTRUCT('DEV','UAT','PROD');
  e STRING;
  usr STRING;
  wif STRING;
BEGIN
  FOR i IN 0 TO ARRAY_SIZE(:envs) - 1 DO
    e := envs[i];
    usr := 'SVC_CAPEX_GHA_' || e;
    wif := 'WORKLOAD_IDENTITY = (TYPE = OIDC ISSUER = ''https://token.actions.githubusercontent.com'' '
        || 'SUBJECT = ''repo:' || $GITHUB_REPO || ':environment:' || LOWER(e) || ''')';
    EXECUTE IMMEDIATE 'CREATE USER IF NOT EXISTS ' || usr || ' TYPE = SERVICE ' || wif
      || ' DEFAULT_ROLE = CAPEX_' || e || '_DEPLOYER DEFAULT_WAREHOUSE = CAPEX_' || e || '_WH'
      || ' COMMENT = ''GitHub Actions deployer for CAPEX_' || e || ' (OIDC)''';
    -- Re-running with a different repo re-points the trust.
    EXECUTE IMMEDIATE 'ALTER USER ' || usr || ' SET ' || wif;
    EXECUTE IMMEDIATE 'GRANT ROLE CAPEX_' || e || '_DEPLOYER TO USER ' || usr;
  END FOR;
  RETURN 'Service users trust repo:' || $GITHUB_REPO || ':environment:<dev|uat|prod>';
END;
$$;

SHOW USER WORKLOAD IDENTITY AUTHENTICATION METHODS FOR USER SVC_CAPEX_GHA_PROD;

-- Optional: allow these users to sign in only from GitHub-hosted runners.
-- Required if your account has an account-level network policy that would block them.
-- CREATE NETWORK POLICY IF NOT EXISTS CAPEX_GHA_NETWORK_POLICY
--   ALLOWED_NETWORK_RULE_LIST = ('SNOWFLAKE.NETWORK_SECURITY.GITHUBACTIONS_GLOBAL');
-- ALTER USER SVC_CAPEX_GHA_DEV  SET NETWORK_POLICY = CAPEX_GHA_NETWORK_POLICY;
-- ALTER USER SVC_CAPEX_GHA_UAT  SET NETWORK_POLICY = CAPEX_GHA_NETWORK_POLICY;
-- ALTER USER SVC_CAPEX_GHA_PROD SET NETWORK_POLICY = CAPEX_GHA_NETWORK_POLICY;

-- Alternative: key-pair authentication, if OIDC is not allowed in your organization.
-- Generate a key per environment, register the public key here, and store the private key
-- as the GitHub environment secret SNOWFLAKE_PRIVATE_KEY:
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out svc_dev.p8
--   openssl rsa -in svc_dev.p8 -pubout | grep -v -- ----- | tr -d '\n'
-- ALTER USER SVC_CAPEX_GHA_DEV SET RSA_PUBLIC_KEY = '<public key body>';
