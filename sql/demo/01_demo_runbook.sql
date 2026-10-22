-- =====================================================================
-- DEMO RUNBOOK (DEV). Run top to bottom in a Snowsight worksheet.
-- Before the meeting: run sql/demo/00_reset_dev.sql
-- =====================================================================
USE ROLE CAPEX_DEV_DEPLOYER;
USE WAREHOUSE CAPEX_DEV_WH;
USE DATABASE CAPEX_DEV;

-- ---------------------------------------------------------------------
-- 1. INGEST: scheduled pull every 30 minutes (task DAG), shown on demand
-- ---------------------------------------------------------------------
SHOW TASKS IN SCHEMA CONTROL;                     -- T_INGEST_CLARITY (30 MINUTE) -> ADO, ServiceNow
EXECUTE TASK CONTROL.T_INGEST_CLARITY;            -- same code path as the schedule

-- Source timestamp vs Snowflake receipt timestamp, run status, retries, failures
SELECT * FROM CONTROL.INGEST_STATUS ORDER BY SOURCE_SYSTEM, ENTITY;
SELECT SOURCE_SYSTEM, ENTITY, TRIGGERED_BY, STATUS, PAGES, API_RETRIES,
       RECORDS_RECEIVED, RECORDS_NEW_OR_CHANGED, RECORDS_UNCHANGED, ERROR_MESSAGE
FROM CONTROL.INGEST_RUN_LOG ORDER BY STARTED_TS DESC LIMIT 10;

-- ---------------------------------------------------------------------
-- 2. BRONZE: raw payload preserved for replay + traceability
-- ---------------------------------------------------------------------
SELECT SOURCE_SYSTEM, ENTITY, SOURCE_RECORD_ID, SOURCE_UPDATED_TS, INGESTED_TS, RUN_ID, API_PAGE, PAYLOAD
FROM BRONZE.RAW_RECORDS WHERE SOURCE_RECORD_ID = 'CL-10004' ORDER BY INGESTED_TS;

-- ---------------------------------------------------------------------
-- 3. SILVER: standardized + deduplicated + DQ exceptions (not silently dropped)
-- ---------------------------------------------------------------------
SELECT SOURCE_PROJECT_CODE, PROJECT_ID, PROJECT_NAME FROM SILVER.PROJECT;      -- PRJ-1001 / prj1002 / PRJ_1003 -> PRJnnnn
SELECT COST_LINE_ID, SOURCE_AMOUNT, AMOUNT, SOURCE_INVOICE_DATE, INVOICE_DATE, DQ_ISSUES, IS_VALID
FROM SILVER.COST_LINE ORDER BY IS_VALID, COST_LINE_ID;
SELECT * FROM SILVER.DQ_EXCEPTIONS;
SELECT * FROM GOLD.RECONCILIATION;                -- Bronze = Silver = Gold + DQ exclusions

-- Data metric functions + expectations attached by the DCM project
SELECT * FROM TABLE(SYSTEM$EVALUATE_DATA_QUALITY_EXPECTATIONS(REF_ENTITY_NAME => 'CAPEX_DEV.SILVER.COST_LINE'));

-- ---------------------------------------------------------------------
-- 4. GOLD + RULES: Finance-approved, versioned criteria applied first
-- ---------------------------------------------------------------------
SELECT CRITERIA_VERSION, RULE_ID, RULE_NAME, PRIORITY, IMPLIED_CLASS, STATUS, APPROVED_BY, POLICY_REFERENCE
FROM CONTROL.CLASSIFICATION_CRITERIA ORDER BY CRITERIA_VERSION, PRIORITY;   -- note v1.1 DRAFT is not applied

-- Mixed-treatment project: one project, CapEx and OpEx decided per cost component
SELECT COST_LINE_ID, COST_TYPE, DESCRIPTION, RECORDED_CLASS, RULE_NAME, RULE_OUTCOME
FROM GOLD.RULE_EVALUATION WHERE PROJECT_ID = 'PRJ1001' ORDER BY COST_LINE_ID;

SELECT PROJECT_NAME, FISCAL_MONTH, RECORDED_CAPEX, RECORDED_OPEX FROM GOLD.PROJECT_FINANCIALS ORDER BY 1, 2;

-- ---------------------------------------------------------------------
-- 5. AI: which function is appropriate? Tested against reviewed examples
-- ---------------------------------------------------------------------
SELECT METHOD, COUNT(*) EXAMPLES, COUNT_IF(IS_MATCH) CORRECT, ROUND(CORRECT / EXAMPLES, 2) ACCURACY
FROM CONTROL.AI_EVAL_RESULTS
WHERE EVAL_RUN_ID = (SELECT EVAL_RUN_ID FROM CONTROL.AI_EVAL_RESULTS ORDER BY EVAL_TS DESC LIMIT 1)
GROUP BY METHOD ORDER BY ACCURACY DESC;

SELECT COST_LINE_ID, CURRENT_CLASSIFICATION, PROPOSED_CLASSIFICATION, TRIGGERING_CRITERIA,
       AI_CLASSIFY_LABEL, AI_COMPLETE_LABEL, CONFIDENCE, RATIONALE, EVIDENCE_QUOTES, REVIEW_STATUS
FROM REVIEW.REVIEW_QUEUE ORDER BY COST_LINE_ID;

-- ---------------------------------------------------------------------
-- 6. LIVE CHANGE: reviewer requests info, PM updates source, it flows through
--    (Do the REQUEST_INFO on CL-10004 in the Streamlit app first.)
-- ---------------------------------------------------------------------
CALL SRC_MOCK.DEMO_CHANGE('CONTRACTOR_DETAIL');   -- PM edits Clarity line + adds ADO work item
CALL SRC_MOCK.DEMO_CHANGE('NEW_COST_LINE');       -- new hypercare line recorded as CAPEX
EXECUTE TASK CONTROL.T_INGEST_CLARITY;            -- next scheduled pull (on demand)
-- wait ~30s for the DAG, then:
SELECT * FROM CONTROL.INGEST_STATUS;
EXECUTE TASK CONTROL.T_AI_REVIEW;                 -- stream has data -> rules + AI
-- wait ~30s, then:
SELECT COST_LINE_ID, PROPOSED_CLASS, RULE_NAME, CREATED_TS, SUPERSEDED_TS
FROM REVIEW.AI_SUGGESTION WHERE COST_LINE_ID IN ('CL-10004', 'CL-10008') ORDER BY COST_LINE_ID, CREATED_TS;
SELECT * FROM REVIEW.DECISION_HISTORY ORDER BY DECIDED_TS;   -- original request retained

-- Optional: failure handling
CALL SRC_MOCK.DEMO_CHANGE('API_OUTAGE');
CALL BRONZE.INGEST_SOURCE('CLARITY', 'DEMO_OUTAGE');          -- logged FAILED, watermark not advanced
SELECT * FROM CONTROL.INGEST_RUN_LOG ORDER BY STARTED_TS DESC LIMIT 2;
CALL SRC_MOCK.DEMO_CHANGE('API_RESTORE');

-- ---------------------------------------------------------------------
-- 7. PROMOTION: what each environment is running and how it got there
-- ---------------------------------------------------------------------
SELECT 'DEV' ENV, * FROM CAPEX_DEV.CONTROL.DEPLOYMENT_LOG
UNION ALL SELECT 'UAT', * FROM CAPEX_UAT.CONTROL.DEPLOYMENT_LOG
UNION ALL SELECT 'PROD', * FROM CAPEX_PROD.CONTROL.DEPLOYMENT_LOG
ORDER BY DEPLOYED_TS DESC;

-- Code promoted, decisions not
SELECT 'DEV' ENV, COUNT(*) DECISIONS FROM CAPEX_DEV.REVIEW.DECISION_HISTORY
UNION ALL SELECT 'UAT', COUNT(*) FROM CAPEX_UAT.REVIEW.DECISION_HISTORY;
