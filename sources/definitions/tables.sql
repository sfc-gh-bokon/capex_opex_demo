-- ===================== Simulated source systems =====================
-- Field names intentionally mimic each source's native API shape.

DEFINE TABLE CAPEX_{{env}}.SRC_MOCK.CLARITY_PROJECT (
    PROJECT_CODE      VARCHAR,
    PROJECT_NAME      VARCHAR,
    PORTFOLIO         VARCHAR,
    BUSINESS_UNIT     VARCHAR,
    COST_CENTER       VARCHAR,
    PROJECT_MANAGER   VARCHAR,
    LIFECYCLE_PHASE   VARCHAR,
    START_DATE        VARCHAR,
    STATUS            VARCHAR,
    LAST_UPDATED_TS   TIMESTAMP_NTZ
) COMMENT = 'Mock Clarity PPM projects';

DEFINE TABLE CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE (
    COST_LINE_ID      VARCHAR,
    PROJECT_CODE      VARCHAR,
    GL_ACCOUNT        VARCHAR,
    COST_TYPE         VARCHAR,
    RECORDED_CLASS    VARCHAR,
    AMOUNT            VARCHAR,
    CURRENCY          VARCHAR,
    FISCAL_PERIOD     VARCHAR,
    INVOICE_DATE      VARCHAR,
    VENDOR            VARCHAR,
    CONTRACTOR_NAME   VARCHAR,
    HOURLY_RATE       VARCHAR,
    DESCRIPTION       VARCHAR,
    WORK_PHASE        VARCHAR,
    LAST_UPDATED_TS   TIMESTAMP_NTZ
) COMMENT = 'Mock Clarity PPM actuals / cost lines';

DEFINE TABLE CAPEX_{{env}}.SRC_MOCK.ADO_WORK_ITEM (
    ID                NUMBER,
    TEAM_PROJECT      VARCHAR,
    COST_LINE_REF     VARCHAR,
    WORK_ITEM_TYPE    VARCHAR,
    TITLE             VARCHAR,
    DESCRIPTION       VARCHAR,
    STATE             VARCHAR,
    AREA_PATH         VARCHAR,
    CHANGED_DATE      TIMESTAMP_NTZ
) COMMENT = 'Mock Azure DevOps work items';

DEFINE TABLE CAPEX_{{env}}.SRC_MOCK.SNOW_RECORD (
    SYS_ID            VARCHAR,
    NUMBER            VARCHAR,
    U_PROJECT         VARCHAR,
    U_COST_LINE       VARCHAR,
    SYS_CLASS_NAME    VARCHAR,
    SHORT_DESCRIPTION VARCHAR,
    DESCRIPTION       VARCHAR,
    STATE             VARCHAR,
    SYS_UPDATED_ON    TIMESTAMP_NTZ
) COMMENT = 'Mock ServiceNow change requests / requested items';

DEFINE TABLE CAPEX_{{env}}.SRC_MOCK.API_BEHAVIOR (
    SOURCE_SYSTEM     VARCHAR,
    PAGE_SIZE         NUMBER,
    FAIL_RATE         FLOAT,
    LATENCY_MS        NUMBER,
    MAX_RETRIES       NUMBER
) COMMENT = 'Controls simulated API paging, latency and transient failures';

-- ===================== Bronze =====================

DEFINE TABLE CAPEX_{{env}}.BRONZE.RAW_RECORDS (
    SOURCE_SYSTEM     VARCHAR       NOT NULL,
    ENTITY            VARCHAR       NOT NULL,
    SOURCE_RECORD_ID  VARCHAR       NOT NULL,
    PAYLOAD           VARIANT,
    PAYLOAD_HASH      VARCHAR,
    SOURCE_UPDATED_TS TIMESTAMP_NTZ,
    INGESTED_TS       TIMESTAMP_LTZ NOT NULL,
    RUN_ID            VARCHAR       NOT NULL,
    API_PAGE          NUMBER
)
CHANGE_TRACKING = TRUE
DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES'
COMMENT = 'Every record delivery from every source, unmodified. Never updated or deleted.';

-- ===================== Control =====================

DEFINE TABLE CAPEX_{{env}}.CONTROL.INGEST_RUN_LOG (
    RUN_ID            VARCHAR,
    SOURCE_SYSTEM     VARCHAR,
    ENTITY            VARCHAR,
    TRIGGERED_BY      VARCHAR,
    STARTED_TS        TIMESTAMP_LTZ,
    ENDED_TS          TIMESTAMP_LTZ,
    STATUS            VARCHAR,
    PAGES             NUMBER,
    API_RETRIES       NUMBER,
    RECORDS_RECEIVED  NUMBER,
    RECORDS_NEW_OR_CHANGED NUMBER,
    RECORDS_UNCHANGED NUMBER,
    WATERMARK_FROM    TIMESTAMP_NTZ,
    WATERMARK_TO      TIMESTAMP_NTZ,
    ERROR_MESSAGE     VARCHAR
) COMMENT = 'One row per source/entity pull attempt';

DEFINE TABLE CAPEX_{{env}}.CONTROL.SOURCE_WATERMARK (
    SOURCE_SYSTEM     VARCHAR,
    ENTITY            VARCHAR,
    HIGH_WATERMARK_TS TIMESTAMP_NTZ,
    UPDATED_TS        TIMESTAMP_LTZ
) COMMENT = 'Last source timestamp successfully landed per entity';

DEFINE TABLE CAPEX_{{env}}.CONTROL.CLASSIFICATION_CRITERIA (
    CRITERIA_VERSION  VARCHAR,
    RULE_ID           VARCHAR,
    RULE_NAME         VARCHAR,
    PRIORITY          NUMBER,
    MATCH_PATTERN     VARCHAR,
    IMPLIED_CLASS     VARCHAR,
    RULE_ACTION       VARCHAR,
    RATIONALE         VARCHAR,
    POLICY_REFERENCE  VARCHAR,
    STATUS            VARCHAR,
    APPROVED_BY       VARCHAR,
    APPROVED_TS       TIMESTAMP_NTZ
) COMMENT = 'Finance-approved, versioned classification criteria. Promoted with the release.';

DEFINE TABLE CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG (
    CONFIG_KEY        VARCHAR,
    CONFIG_VALUE      VARCHAR,
    UPDATED_TS        TIMESTAMP_LTZ
) COMMENT = 'Active criteria version, prompt version and model';

DEFINE TABLE CAPEX_{{env}}.CONTROL.REVIEWED_EXAMPLES (
    EXAMPLE_ID        VARCHAR,
    EVIDENCE_TEXT     VARCHAR,
    RECORDED_CLASS    VARCHAR,
    EXPECTED_CLASS    VARCHAR,
    REVIEWED_BY       VARCHAR,
    NOTES             VARCHAR
) COMMENT = 'Finance-labeled examples used to evaluate AI_CLASSIFY vs AI_COMPLETE';

DEFINE TABLE CAPEX_{{env}}.CONTROL.AI_EVAL_RESULTS (
    EVAL_RUN_ID       VARCHAR,
    EVAL_TS           TIMESTAMP_LTZ,
    METHOD            VARCHAR,
    MODEL             VARCHAR,
    PROMPT_VERSION    VARCHAR,
    EXAMPLE_ID        VARCHAR,
    EXPECTED_CLASS    VARCHAR,
    PREDICTED_CLASS   VARCHAR,
    IS_MATCH          BOOLEAN,
    RAW_OUTPUT        VARIANT
) COMMENT = 'Per-example AI evaluation results';

DEFINE TABLE CAPEX_{{env}}.CONTROL.DEPLOYMENT_LOG (
    DEPLOY_ID         VARCHAR,
    ENV               VARCHAR,
    RELEASE_TAG       VARCHAR,
    GIT_SHA           VARCHAR,
    CRITERIA_VERSION  VARCHAR,
    DEPLOYED_BY       VARCHAR,
    PIPELINE_RUN_URL  VARCHAR,
    DEPLOYED_TS       TIMESTAMP_LTZ,
    STATUS            VARCHAR,
    CHECKS            VARIANT
) COMMENT = 'Deployment record with post-deployment check results';

-- ===================== Review =====================

DEFINE TABLE CAPEX_{{env}}.REVIEW.AI_SUGGESTION (
    SUGGESTION_ID     VARCHAR,
    COST_LINE_ID      VARCHAR,
    PROJECT_ID        VARCHAR,
    EVIDENCE_HASH     VARCHAR,
    RECORDED_CLASS    VARCHAR,
    RULE_IMPLIED_CLASS VARCHAR,
    RULE_ID           VARCHAR,
    RULE_NAME         VARCHAR,
    CRITERIA_VERSION  VARCHAR,
    AI_CLASSIFY_LABEL VARCHAR,
    AI_COMPLETE_LABEL VARCHAR,
    PROPOSED_CLASS    VARCHAR,
    CONFIDENCE        FLOAT,
    RATIONALE         VARCHAR,
    EVIDENCE_QUOTES   VARIANT,
    MODEL             VARCHAR,
    PROMPT_VERSION    VARCHAR,
    SOURCE_SNAPSHOT   VARIANT,
    CREATED_TS        TIMESTAMP_LTZ,
    SUPERSEDED_TS     TIMESTAMP_LTZ
) COMMENT = 'AI suggestions. A new row is written when evidence changes; old rows are superseded, never edited.';

DEFINE TABLE CAPEX_{{env}}.REVIEW.DECISION_HISTORY (
    DECISION_ID       VARCHAR,
    SUGGESTION_ID     VARCHAR,
    COST_LINE_ID      VARCHAR,
    DECISION          VARCHAR,
    REVIEWER_COMMENT  VARCHAR,
    DECIDED_BY        VARCHAR,
    DECIDED_TS        TIMESTAMP_LTZ,
    RECORDED_CLASS    VARCHAR,
    PROPOSED_CLASS    VARCHAR,
    CRITERIA_VERSION  VARCHAR,
    MODEL             VARCHAR,
    PROMPT_VERSION    VARCHAR,
    SOURCE_SNAPSHOT   VARIANT
) COMMENT = 'Append-only reviewer decisions. No role has UPDATE or DELETE.';

