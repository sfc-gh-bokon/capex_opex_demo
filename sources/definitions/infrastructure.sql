-- Schemas for the CapEx/OpEx review pipeline. The database and DCM schema are
-- created by sql/00_platform_setup.sql (a DCM project cannot define its parent).

DEFINE SCHEMA CAPEX_{{env}}.SRC_MOCK
    COMMENT = 'Simulated source systems (Clarity, ServiceNow, Azure DevOps). Stands in for external APIs.';

DEFINE SCHEMA CAPEX_{{env}}.BRONZE
    COMMENT = 'Raw: source payloads exactly as received, plus ingestion metadata. Append-only, kept for replay.';

DEFINE SCHEMA CAPEX_{{env}}.SILVER
    COMMENT = 'Modeled: standardized IDs, types, dates, amounts; deduplicated; DQ exceptions flagged.';

DEFINE SCHEMA CAPEX_{{env}}.GOLD
    COMMENT = 'Analytics: project financials, cost line evidence, rule evaluation, reconciliation.';

DEFINE SCHEMA CAPEX_{{env}}.CONTROL
    COMMENT = 'Run log, watermarks, versioned classification criteria, AI evaluation, deployment log.';

DEFINE SCHEMA CAPEX_{{env}}.REVIEW
    COMMENT = 'AI suggestions and append-only reviewer decision history.';

DEFINE SCHEMA CAPEX_{{env}}.GOVERNANCE
    COMMENT = 'Tags, masking and row access policies, entitlement mappings.';

DEFINE SCHEMA CAPEX_{{env}}.APP
    COMMENT = 'Streamlit reviewer application.';

DEFINE STAGE CAPEX_{{env}}.APP.STREAMLIT_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Streamlit app source files';
