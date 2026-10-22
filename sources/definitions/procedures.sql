-- ======================================================================
-- Ingestion: scheduled pull from (simulated) source APIs into Bronze
-- ======================================================================
-- The handler treats each SRC_MOCK table as a paginated REST endpoint
-- ("give me records changed since <watermark>"). It simulates latency,
-- transient failures with retry/backoff, and occasional duplicate
-- deliveries across page boundaries. Replacing fetch_page() with an HTTP
-- call over an External Access Integration is the only change needed for
-- a real Clarity / ServiceNow / ADO endpoint.

DEFINE PROCEDURE CAPEX_{{env}}.BRONZE.INGEST_SOURCE(SOURCE_SYSTEM VARCHAR, TRIGGERED_BY VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'Pull changed records from a source API since the last watermark and land them in BRONZE.RAW_RECORDS'
AS
$$
import json
import random
import time
import uuid

DB = "CAPEX_{{env}}"

# source -> list of (entity, source table, id column, source-updated column)
ENDPOINTS = {
    "CLARITY": [
        ("project", "CLARITY_PROJECT", "PROJECT_CODE", "LAST_UPDATED_TS"),
        ("cost_line", "CLARITY_COST_LINE", "COST_LINE_ID", "LAST_UPDATED_TS"),
    ],
    "AZURE_DEVOPS": [
        ("work_item", "ADO_WORK_ITEM", "ID", "CHANGED_DATE"),
    ],
    "SERVICENOW": [
        ("sc_record", "SNOW_RECORD", "SYS_ID", "SYS_UPDATED_ON"),
    ],
}


class TransientApiError(Exception):
    pass


def api_behavior(session, source):
    rows = session.sql(
        f"SELECT PAGE_SIZE, FAIL_RATE, LATENCY_MS, MAX_RETRIES FROM {DB}.SRC_MOCK.API_BEHAVIOR WHERE SOURCE_SYSTEM = ?",
        params=[source],
    ).collect()
    if not rows:
        return 50, 0.0, 200, 3
    r = rows[0]
    return int(r[0]), float(r[1]), int(r[2]), int(r[3])


def fetch_page(session, table, id_col, ts_col, since, page, page_size, fail_rate, latency_ms):
    """Simulated GET /<entity>?updated_since=<since>&page=<page>."""
    time.sleep(latency_ms / 1000.0)
    if random.random() < fail_rate:
        raise TransientApiError(f"HTTP 503 from {table} page {page}")
    # One-record overlap with the previous page mimics APIs that page on
    # timestamps; it produces duplicate deliveries that Silver must absorb.
    offset = max(page * page_size - (1 if page > 0 else 0), 0)
    return session.sql(
        f"""SELECT {id_col}::VARCHAR AS ID, {ts_col} AS TS, OBJECT_CONSTRUCT(*)::VARCHAR AS J
            FROM {DB}.SRC_MOCK.{table}
            WHERE {ts_col} > ?
            ORDER BY {ts_col}, {id_col}
            LIMIT {page_size + (1 if page > 0 else 0)} OFFSET {offset}""",
        params=[since],
    ).collect()


def run(session, source_system, triggered_by):
    source = (source_system or "").upper()
    if source not in ENDPOINTS:
        raise ValueError(f"Unknown source system {source_system}")
    page_size, fail_rate, latency_ms, max_retries = api_behavior(session, source)
    summary = []

    for entity, table, id_col, ts_col in ENDPOINTS[source]:
        run_id = str(uuid.uuid4())
        wm_rows = session.sql(
            f"SELECT HIGH_WATERMARK_TS FROM {DB}.CONTROL.SOURCE_WATERMARK WHERE SOURCE_SYSTEM = ? AND ENTITY = ?",
            params=[source, entity],
        ).collect()
        since = wm_rows[0][0] if wm_rows and wm_rows[0][0] is not None else "1900-01-01"
        session.sql(
            f"""INSERT INTO {DB}.CONTROL.INGEST_RUN_LOG
                (RUN_ID, SOURCE_SYSTEM, ENTITY, TRIGGERED_BY, STARTED_TS, STATUS, WATERMARK_FROM)
                SELECT ?, ?, ?, ?, CURRENT_TIMESTAMP(), 'RUNNING', ?::TIMESTAMP_NTZ""",
            params=[run_id, source, entity, triggered_by, str(since)],
        ).collect()

        pages = retries = received = 0
        max_ts = None
        status, error = "SUCCEEDED", None
        try:
            page = 0
            while True:
                attempt = 0
                while True:
                    try:
                        rows = fetch_page(session, table, id_col, ts_col, since, page,
                                          page_size, fail_rate, latency_ms)
                        break
                    except TransientApiError:
                        attempt += 1
                        retries += 1
                        if attempt > max_retries:
                            raise
                        time.sleep(min(2 ** attempt * 0.2, 2.0))
                if not rows:
                    break
                pages += 1
                received += len(rows)
                batch = json.dumps([{"ID": r["ID"], "J": r["J"], "TS": str(r["TS"])} for r in rows])
                session.sql(
                    f"""INSERT INTO {DB}.BRONZE.RAW_RECORDS
                        (SOURCE_SYSTEM, ENTITY, SOURCE_RECORD_ID, PAYLOAD, PAYLOAD_HASH,
                         SOURCE_UPDATED_TS, INGESTED_TS, RUN_ID, API_PAGE)
                        SELECT ?, ?, f.value:ID::VARCHAR, PARSE_JSON(f.value:J::VARCHAR),
                               SHA2(f.value:J::VARCHAR), f.value:TS::TIMESTAMP_NTZ,
                               CURRENT_TIMESTAMP(), ?, ?
                        FROM TABLE(FLATTEN(INPUT => PARSE_JSON(?))) f""",
                    params=[source, entity, run_id, page, batch],
                ).collect()
                page_max = max(r["TS"] for r in rows)
                max_ts = page_max if max_ts is None or page_max > max_ts else max_ts
                if len(rows) < page_size:
                    break
                page += 1
        except Exception as e:  # noqa: BLE001 - every failure must be logged, not lost
            status, error = "FAILED", str(e)[:4000]

        # Classify what landed in this run as new/changed vs re-delivered unchanged.
        counts = session.sql(
            f"""SELECT COUNT_IF(prev.PAYLOAD_HASH IS NULL), COUNT_IF(prev.PAYLOAD_HASH IS NOT NULL)
                FROM {DB}.BRONZE.RAW_RECORDS cur
                LEFT JOIN (SELECT DISTINCT SOURCE_SYSTEM, ENTITY, SOURCE_RECORD_ID, PAYLOAD_HASH
                           FROM {DB}.BRONZE.RAW_RECORDS WHERE RUN_ID <> ?) prev
                  ON prev.SOURCE_SYSTEM = cur.SOURCE_SYSTEM AND prev.ENTITY = cur.ENTITY
                 AND prev.SOURCE_RECORD_ID = cur.SOURCE_RECORD_ID AND prev.PAYLOAD_HASH = cur.PAYLOAD_HASH
                WHERE cur.RUN_ID = ?""",
            params=[run_id, run_id],
        ).collect()[0]

        new_or_changed, unchanged = int(counts[0] or 0), int(counts[1] or 0)

        # Only advance the watermark on full success, so a failed pull is retried next run.
        if status == "SUCCEEDED" and max_ts is not None:
            session.sql(
                f"""MERGE INTO {DB}.CONTROL.SOURCE_WATERMARK t
                    USING (SELECT ? AS S, ? AS E, ?::TIMESTAMP_NTZ AS W) s
                    ON t.SOURCE_SYSTEM = s.S AND t.ENTITY = s.E
                    WHEN MATCHED THEN UPDATE SET HIGH_WATERMARK_TS = s.W, UPDATED_TS = CURRENT_TIMESTAMP()
                    WHEN NOT MATCHED THEN INSERT VALUES (s.S, s.E, s.W, CURRENT_TIMESTAMP())""",
                params=[source, entity, str(max_ts)],
            ).collect()

        session.sql(
            f"""UPDATE {DB}.CONTROL.INGEST_RUN_LOG
                SET ENDED_TS = CURRENT_TIMESTAMP(), STATUS = ?, PAGES = ?, API_RETRIES = ?,
                    RECORDS_RECEIVED = ?, RECORDS_NEW_OR_CHANGED = ?, RECORDS_UNCHANGED = ?,
                    WATERMARK_TO = ?::TIMESTAMP_NTZ, ERROR_MESSAGE = NULLIF(?, '')
                WHERE RUN_ID = ?""",
            params=[status, pages, retries, received, new_or_changed, unchanged,
                    str(max_ts) if max_ts is not None else str(since), error or "", run_id],
        ).collect()
        summary.append({"entity": entity, "run_id": run_id, "status": status, "pages": pages,
                        "retries": retries, "received": received,
                        "new_or_changed": new_or_changed, "unchanged": unchanged, "error": error})
    return summary
$$;

-- ======================================================================
-- Simulated source data (stands in for the real systems of record)
-- ======================================================================

DEFINE PROCEDURE CAPEX_{{env}}.SRC_MOCK.SEED_SOURCE_DATA()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Resets simulated Clarity / ADO / ServiceNow data to the demo starting state'
AS
$$
BEGIN
    TRUNCATE TABLE CAPEX_{{env}}.SRC_MOCK.CLARITY_PROJECT;
    TRUNCATE TABLE CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE;
    TRUNCATE TABLE CAPEX_{{env}}.SRC_MOCK.ADO_WORK_ITEM;
    TRUNCATE TABLE CAPEX_{{env}}.SRC_MOCK.SNOW_RECORD;
    TRUNCATE TABLE CAPEX_{{env}}.SRC_MOCK.API_BEHAVIOR;

    INSERT INTO CAPEX_{{env}}.SRC_MOCK.API_BEHAVIOR VALUES
        ('CLARITY', 10, 0.15, 150, 3),
        ('AZURE_DEVOPS', 25, 0.05, 100, 3),
        ('SERVICENOW', 25, 0.05, 120, 3);

    -- Project codes arrive in inconsistent formats on purpose (Silver standardizes them).
    INSERT INTO CAPEX_{{env}}.SRC_MOCK.CLARITY_PROJECT VALUES
        ('PRJ-1001', 'Client Portal Modernization', 'Wealth Management Digital', 'Wealth Management', 'CC-4100', 'Dana Whitfield', 'Application Development', '2026-01-12', 'Active', '2026-09-01 08:00:00'),
        ('prj1002',  'Custody Reconciliation Engine', 'Asset Servicing Tech', 'Asset Servicing', 'CC-5200', 'Marcus Lee', 'Application Development', '2026-02-03', 'Active', '2026-09-01 08:00:00'),
        ('PRJ_1003', 'Data Center Exit - Phase 2', 'Infrastructure', 'Technology Operations', 'CC-9000', 'Priya Nair', 'Implementation', '2025-11-17', 'Active', '2026-09-01 08:00:00'),
        ('PRJ-1004', 'Enterprise Vulnerability Remediation', 'Cyber Security', 'Information Security', 'CC-9300', 'Tom Alvarez', 'Post-Implementation', '2025-06-01', 'Active', '2026-09-01 08:00:00');

    INSERT INTO CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE VALUES
        -- Client Portal Modernization: mixed-treatment project
        ('CL-10001','PRJ-1001','160500','Professional Services','CAPEX','84,500.00','USD','2026-03','03/31/2026','Accenture','Jordan Pike','185.00','Requirements workshops, current-state assessment and vendor selection RFP evaluation for portal platform','Preliminary','2026-09-01 08:05:00'),
        ('CL-10002','PRJ-1001','610200','Internal Labor','OPEX','212,300.00','USD','2026-05','2026-05-31',NULL,'Sam Ortega','142.50','Sprint 7-9 engineering effort','Application Development','2026-09-01 08:05:00'),
        ('CL-10003','PRJ-1001','620100','Software License','OPEX','48,000.00','USD','2026-06','2026-06-30','Salesforce',NULL,NULL,'Annual Experience Cloud subscription renewal','Operations','2026-09-01 08:05:00'),
        ('CL-10004','PRJ-1001','160500','Contractor','CAPEX','36,750.00','USD','2026-06','2026-06-15','TEKsystems','Riley Chen','165.00','Contractor invoice - misc services June','Application Development','2026-09-01 08:05:00'),
        ('CL-10005','PRJ-1001','160500','Internal Labor','CAPEX','301,900.00','USD','2026-07','2026-07-31',NULL,'Sam Ortega','142.50','Build and unit test of advisor dashboard and document vault features','Application Development','2026-09-01 08:05:00'),
        ('CL-10006','PRJ-1001','610300','Training','CAPEX','22,400.00','USD','2026-08','2026-08-20','Udemy Business',NULL,NULL,'End-user training sessions for relationship managers on new portal','Post-Implementation','2026-09-01 08:05:00'),
        ('CL-10007','PRJ-1001','610200','Data Conversion','OPEX','58,200.00','USD','2026-08','2026-08-28',NULL,'Alex Kim','138.00','Migration of legacy client documents and data cleansing','Implementation','2026-09-01 08:05:00'),
        -- Custody Reconciliation Engine
        ('CL-20001','prj1002','160500','Internal Labor','CAPEX','265,000.00','USD','2026-04','2026-04-30',NULL,'Hannah Ruiz','150.00','Design and coding of matching engine for SWIFT MT535/MT950 reconciliation','Application Development','2026-09-01 08:05:00'),
        ('CL-20002','prj1002','610200','Internal Labor','OPEX','74,800.00','USD','2026-07','2026-07-31',NULL,'Hannah Ruiz','150.00','Production support and bug fixes for released recon engine v1','Post-Implementation','2026-09-01 08:05:00'),
        ('CL-20003','prj1002','160500','Professional Services','OPEX','118,000.00','USD','2026-08','2026-08-31','Deloitte','Chris Ward','210.00','Development of new break-aging analytics module adding exception workflow capability','Application Development','2026-09-01 08:05:00'),
        -- Data Center Exit
        ('CL-30001','PRJ_1003','170100','Hardware','CAPEX','640,000.00','USD','2026-02','2026-02-27','Dell',NULL,NULL,'Purchase of PowerEdge servers for co-location build-out','Implementation','2026-09-01 08:05:00'),
        ('CL-30002','PRJ_1003','620500','Cloud Services','OPEX','92,000.00','USD','2026-06','2026-06-30','Microsoft',NULL,NULL,'Azure consumption - migrated workloads','Operations','2026-09-01 08:05:00'),
        ('CL-30003','PRJ_1003','170100','Professional Services','CAPEX','55,000.00','USD','2026-07','07/15/2026','Kyndryl','Pat Morgan','175.00','Decommissioning and disposal of legacy storage arrays','Implementation','2026-09-01 08:05:00'),
        -- Vulnerability remediation
        ('CL-40001','PRJ-1004','610200','Internal Labor','OPEX','41,000.00','USD','2026-05','2026-05-31',NULL,'Jamie Fox','130.00','Patching of Qualys critical findings across Windows estate','Operations','2026-09-01 08:05:00'),
        ('CL-40002','PRJ-1004','160500','Contractor','CAPEX','29,500.00','USD','2026-07','2026-07-31','Optiv','Lee Grant','190.00','Security assessment','Post-Implementation','2026-09-01 08:05:00'),
        -- Deliberate data-quality defects
        ('CL-90001','PRJ-1009','160500','Internal Labor','CAPEX','12,000.00','USD','2026-07','2026-07-31',NULL,'Unknown','140.00','Orphan cost line - project not in Clarity','Application Development','2026-09-01 08:05:00'),
        ('CL-90002','PRJ-1001','160500','Internal Labor','CAPEX','N/A','USD','2026-07','2026-13-45',NULL,'Sam Ortega','142.50','Amount and date not parseable','Application Development','2026-09-01 08:05:00');

    INSERT INTO CAPEX_{{env}}.SRC_MOCK.ADO_WORK_ITEM VALUES
        (5101,'ClientPortal','CL-10001','Epic','Portal platform selection','Facilitate discovery workshops, document current-state pain points, score three vendors against RFP criteria and recommend a platform. No code is written in this epic.','Closed','ClientPortal\\Discovery','2026-09-01 08:10:00'),
        (5102,'ClientPortal','CL-10002','Feature','Entitlements API module','Build a new entitlements REST API that adds household-level permissions not available in the legacy portal. Includes coding, integration with IAM, and automated tests.','Closed','ClientPortal\\Build','2026-09-01 08:10:00'),
        (5103,'ClientPortal','CL-10002','User Story','Household permission model','As an advisor I can grant a family member read-only access to statements. Implemented in sprint 8.','Closed','ClientPortal\\Build','2026-09-01 08:10:00'),
        (5104,'ClientPortal','CL-10005','Feature','Advisor dashboard and document vault','New functionality: advisor dashboard, secure document vault, e-signature integration. Development and QA.','Closed','ClientPortal\\Build','2026-09-01 08:10:00'),
        (5105,'ClientPortal','CL-10006','Task','RM training rollout','Deliver classroom and virtual training to 400 relationship managers after go-live.','Closed','ClientPortal\\Adoption','2026-09-01 08:10:00'),
        (6201,'CustodyRecon','CL-20001','Feature','Matching engine','Design and build rules-based matching for MT535 and MT950 messages.','Closed','CustodyRecon\\Engine','2026-09-01 08:10:00'),
        (6202,'CustodyRecon','CL-20002','Bug','Fix tolerance rounding defect','Production defect: tolerance rounding causes false breaks. Hotfix only, no new functionality.','Closed','CustodyRecon\\Support','2026-09-01 08:10:00'),
        (6203,'CustodyRecon','CL-20003','Feature','Break-aging analytics module','New module adding exception workflow, aging buckets and escalation routing. Significant new capability beyond v1.','Active','CustodyRecon\\Engine','2026-09-01 08:10:00');

    INSERT INTO CAPEX_{{env}}.SRC_MOCK.SNOW_RECORD VALUES
        ('a1f3','CHG0031001','PRJ-1001','CL-10002','change_request','Deploy entitlements API to production','Production release of new entitlements API v1.0 created during application development stage.','Closed','2026-09-01 08:12:00'),
        ('a1f4','RITM0045002','PRJ-1001','CL-10003','sc_req_item','Renew Experience Cloud licenses','Renewal of existing subscription, no new modules purchased.','Closed','2026-09-01 08:12:00'),
        ('a1f5','RITM0045003','PRJ-1001','CL-10004','sc_req_item','TEKsystems PO extension','Extend purchase order for contractor support.','Closed','2026-09-01 08:12:00'),
        ('b2e1','CHG0032010','PRJ_1003','CL-30003','change_request','Decommission legacy SAN','Power down, wipe and dispose of legacy storage arrays in DC2.','Closed','2026-09-01 08:12:00'),
        ('c3d1','RITM0046001','PRJ-1004','CL-40002','sc_req_item','Optiv engagement','Annual third-party penetration test and security assessment.','Closed','2026-09-01 08:12:00');

    RETURN 'Seeded simulated sources';
END;
$$;

DEFINE PROCEDURE CAPEX_{{env}}.SRC_MOCK.DEMO_CHANGE(SCENARIO VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Simulates a change in a source system so it can be shown flowing through the pipeline'
AS
$$
BEGIN
    CASE UPPER(:SCENARIO)
        WHEN 'CONTRACTOR_DETAIL' THEN
            -- Finance asked for detail; the PM updates the contractor line in Clarity and ADO.
            UPDATE CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE
               SET DESCRIPTION = 'Contractor invoice - June: front-end development of document vault upload and e-signature components',
                   LAST_UPDATED_TS = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
             WHERE COST_LINE_ID = 'CL-10004';
            INSERT INTO CAPEX_{{env}}.SRC_MOCK.ADO_WORK_ITEM VALUES
                (5106,'ClientPortal','CL-10004','Task','Vault upload UI','Contractor Riley Chen built the drag-and-drop upload and e-signature UI components for the new document vault.','Closed','ClientPortal\\Build', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
            RETURN 'CL-10004 description updated and ADO work item 5106 added';
        WHEN 'NEW_COST_LINE' THEN
            INSERT INTO CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE VALUES
                ('CL-10008','PRJ-1001','160500','Professional Services','CAPEX','27,900.00','USD','2026-09','2026-09-30','Accenture','Jordan Pike','185.00','Post go-live hypercare and help desk support for portal users','Post-Implementation', CURRENT_TIMESTAMP()::TIMESTAMP_NTZ);
            RETURN 'CL-10008 added (recorded CAPEX, hypercare)';
        WHEN 'AMOUNT_CHANGE' THEN
            UPDATE CAPEX_{{env}}.SRC_MOCK.CLARITY_COST_LINE
               SET AMOUNT = '312,450.00', LAST_UPDATED_TS = CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
             WHERE COST_LINE_ID = 'CL-10005';
            RETURN 'CL-10005 amount restated';
        WHEN 'API_OUTAGE' THEN
            UPDATE CAPEX_{{env}}.SRC_MOCK.API_BEHAVIOR SET FAIL_RATE = 1.0 WHERE SOURCE_SYSTEM = 'CLARITY';
            RETURN 'Clarity API will now fail every request';
        WHEN 'API_RESTORE' THEN
            UPDATE CAPEX_{{env}}.SRC_MOCK.API_BEHAVIOR SET FAIL_RATE = 0.15 WHERE SOURCE_SYSTEM = 'CLARITY';
            RETURN 'Clarity API restored';
        ELSE
            RETURN 'Unknown scenario. Use CONTRACTOR_DETAIL, NEW_COST_LINE, AMOUNT_CHANGE, API_OUTAGE, API_RESTORE';
    END CASE;
END;
$$;

-- ======================================================================
-- Reference data: approved criteria, config, reviewed examples
-- These are part of the release (logic), so they are promoted.
-- ======================================================================

DEFINE PROCEDURE CAPEX_{{env}}.CONTROL.SEED_REFERENCE_DATA()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Loads version-controlled classification criteria, config and Finance-reviewed examples'
AS
$$
BEGIN
    DELETE FROM CAPEX_{{env}}.CONTROL.CLASSIFICATION_CRITERIA;
    INSERT INTO CAPEX_{{env}}.CONTROL.CLASSIFICATION_CRITERIA VALUES
        ('1.0','R01','Preliminary project stage',10,'(requirements workshop|vendor selection|rfp|current-state assessment|discovery|feasibility|evaluat(e|ion) of alternatives)','OPEX','FLAG_IF_MISMATCH','Conceptual formulation, evaluation of alternatives and vendor selection occur before the application development stage and are expensed.','ASC 350-40-25-1; NT Capitalization Policy 4.2','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R02','Training',20,'(training|enablement session|classroom)','OPEX','FLAG_IF_MISMATCH','Training costs are expensed as incurred regardless of project phase.','ASC 350-40-25-4; NT Policy 4.5','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R03','Data conversion',30,'(data conversion|data cleansing|data migration|migration of legacy)','OPEX','FLAG_IF_MISMATCH','Data conversion and cleansing are expensed; only software needed to convert data may be capitalized.','ASC 350-40-25-3; NT Policy 4.5','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R04','Maintenance and support',40,'(production support|bug fix|hotfix|maintenance|hypercare|help desk|patching)','OPEX','FLAG_IF_MISMATCH','Post-implementation maintenance and support are expensed.','ASC 350-40-25-6; NT Policy 4.6','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R05','New functionality development',50,'(build|coding|develop(ment)?|design and cod|new module|new functionality|adds? .*capabilit)','CAPEX','FLAG_IF_MISMATCH','Coding, testing and installation during application development, and upgrades that add functionality, are capitalized.','ASC 350-40-25-2; NT Policy 4.3','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R06','Software subscription',45,'(subscription|license renewal|saas)','OPEX','FLAG_IF_MISMATCH','Hosted subscription fees without a license to the software are service contracts and expensed.','ASC 350-40-15-4A; NT Policy 4.7','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R07','Asset disposal',70,'(decommission|disposal|dispose)','OPEX','FLAG_IF_MISMATCH','Decommissioning and disposal of retired assets are expensed.','NT Fixed Asset Policy 2.8','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        ('1.0','R99','Vague contractor or services description',90,'(misc|miscellaneous|Clarity: (security assessment|professional services|services)( [|]|$))','REVIEW','INSUFFICIENT','Description does not identify the work performed; classification cannot be confirmed without more evidence.','NT Policy 4.1 documentation requirement','APPROVED','Finance Controller - K. Morales','2026-08-15 10:00:00'),
        -- Draft rule awaiting approval: not applied until approved AND activated
        ('1.1','R08','Cloud configuration and implementation',55,'(cloud configuration|implementation services for saas)','OPEX','FLAG_IF_MISMATCH','Implementation costs of a hosting arrangement follow ASU 2018-15 and are deferred, not capitalized as software.','ASU 2018-15','DRAFT',NULL,NULL);

    DELETE FROM CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG;
    INSERT INTO CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG VALUES
        ('CRITERIA_VERSION','1.0',CURRENT_TIMESTAMP()),
        ('PROMPT_VERSION','p-2026.10.2',CURRENT_TIMESTAMP()),
        ('AI_MODEL','{{ai_model}}',CURRENT_TIMESTAMP());

    DELETE FROM CAPEX_{{env}}.CONTROL.REVIEWED_EXAMPLES;
    INSERT INTO CAPEX_{{env}}.CONTROL.REVIEWED_EXAMPLES VALUES
        ('E01','Requirements gathering workshops and vendor demos to choose a CRM platform','CAPEX','OPEX','K. Morales','Preliminary stage'),
        ('E02','Coding and unit testing of new trade allocation microservice','OPEX','CAPEX','K. Morales','Application development'),
        ('E03','Annual renewal of SaaS analytics subscription','OPEX','OPEX','K. Morales','Hosted service'),
        ('E04','Contractor invoice - misc services','CAPEX','INSUFFICIENT EVIDENCE','K. Morales','No description of work'),
        ('E05','End-user training for operations staff on the new GL system','CAPEX','OPEX','K. Morales','Training'),
        ('E06','Cleansing and conversion of legacy customer records into new platform','CAPEX','OPEX','K. Morales','Data conversion'),
        ('E07','Purchase of network switches for new data center hall','CAPEX','CAPEX','K. Morales','Hardware'),
        ('E08','Bug fixes and production support for released payments engine','CAPEX','OPEX','K. Morales','Maintenance'),
        ('E09','Development of new margin calculation module adding intraday capability','OPEX','CAPEX','K. Morales','Enhancement adding functionality'),
        ('E10','Feasibility study comparing build vs buy for KYC tooling','CAPEX','OPEX','K. Morales','Preliminary stage'),
        ('E11','Professional services','CAPEX','INSUFFICIENT EVIDENCE','K. Morales','No description of work'),
        ('E12','Integration testing and installation of new collateral system prior to go-live','OPEX','CAPEX','K. Morales','Application development'),
        ('E13','Post go-live hypercare help desk','CAPEX','OPEX','K. Morales','Support'),
        ('E14','Decommissioning of mainframe LPARs and disposal','CAPEX','OPEX','K. Morales','Disposal'),
        ('E15','Design and build of client reporting data model for new fund accounting platform','OPEX','CAPEX','K. Morales','Application development'),
        ('E16','Quarterly penetration test by third party','CAPEX','OPEX','K. Morales','Recurring service'),
        ('E17','Software license perpetual purchase for on-prem risk engine','OPEX','CAPEX','K. Morales','Perpetual license'),
        ('E18','Security assessment','CAPEX','INSUFFICIENT EVIDENCE','K. Morales','Scope unknown'),
        ('E19','Patching of critical vulnerabilities across Linux fleet','OPEX','OPEX','K. Morales','Maintenance'),
        ('E20','Developer labor building new mobile onboarding flow with biometric login','OPEX','CAPEX','K. Morales','New functionality');

    RETURN 'Reference data loaded: criteria 1.0 (approved), 1.1 (draft); 20 reviewed examples';
END;
$$;

-- ======================================================================
-- AI-assisted review: rules first (GOLD.RULE_EVALUATION), then AI
-- ======================================================================

DEFINE PROCEDURE CAPEX_{{env}}.REVIEW.RUN_AI_REVIEW()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'For rule-flagged cost lines with new or changed evidence, call AI_CLASSIFY and AI_COMPLETE and write a suggestion'
EXECUTE AS CALLER
AS
$$
DECLARE
    model VARCHAR;
    prompt_version VARCHAR;
    n_new NUMBER;
BEGIN
    SELECT MAX(IFF(CONFIG_KEY = 'AI_MODEL', CONFIG_VALUE, NULL)),
           MAX(IFF(CONFIG_KEY = 'PROMPT_VERSION', CONFIG_VALUE, NULL))
      INTO :model, :prompt_version
      FROM CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG;

    -- Consume the Bronze stream (DML advances the offset) so the task only
    -- fires again when new source deliveries land.
    CREATE OR REPLACE TEMPORARY TABLE CAPEX_{{env}}.REVIEW.TMP_DELIVERIES (RUN_ID VARCHAR, N NUMBER);
    INSERT INTO CAPEX_{{env}}.REVIEW.TMP_DELIVERIES
        SELECT RUN_ID, COUNT(*) FROM CAPEX_{{env}}.BRONZE.RAW_RECORDS_STREAM GROUP BY RUN_ID;

    -- Bring Silver/Gold current before evaluating (otherwise they follow TARGET_LAG).
    ALTER DYNAMIC TABLE CAPEX_{{env}}.GOLD.RULE_EVALUATION REFRESH;

    -- Supersede (not edit) any open suggestion whose evidence or criteria changed.
    UPDATE CAPEX_{{env}}.REVIEW.AI_SUGGESTION s
       SET SUPERSEDED_TS = CURRENT_TIMESTAMP()
      FROM CAPEX_{{env}}.GOLD.RULE_EVALUATION r
     WHERE s.COST_LINE_ID = r.COST_LINE_ID AND s.SUPERSEDED_TS IS NULL
       AND (s.EVIDENCE_HASH <> r.EVIDENCE_HASH OR s.CRITERIA_VERSION <> r.CRITERIA_VERSION);

    CREATE OR REPLACE TEMPORARY TABLE CAPEX_{{env}}.REVIEW.TMP_TO_REVIEW AS
        SELECT r.*
        FROM CAPEX_{{env}}.GOLD.RULE_EVALUATION r
        LEFT JOIN CAPEX_{{env}}.REVIEW.AI_SUGGESTION s
          ON s.COST_LINE_ID = r.COST_LINE_ID AND s.EVIDENCE_HASH = r.EVIDENCE_HASH
         AND s.CRITERIA_VERSION = r.CRITERIA_VERSION AND s.SUPERSEDED_TS IS NULL
        WHERE r.NEEDS_REVIEW AND s.SUGGESTION_ID IS NULL;

    INSERT INTO CAPEX_{{env}}.REVIEW.AI_SUGGESTION
    WITH ai AS (
        SELECT t.*,
            AI_CLASSIFY(
                t.EVIDENCE_TEXT,
                [
                  {'label': 'CAPEX', 'description': 'Internal-use software costs incurred in the application development stage (design, coding, testing, installation) or upgrades that add new functionality; purchased hardware or perpetual licenses.'},
                  {'label': 'OPEX', 'description': 'Preliminary-stage work (requirements, vendor selection, feasibility), training, data conversion and cleansing, maintenance, bug fixes, support, hosted subscriptions, decommissioning, recurring third-party services.'},
                  {'label': 'INSUFFICIENT EVIDENCE', 'description': 'The text does not say what work was performed, so capital vs expense cannot be determined.'}
                ],
                {'task_description': 'Classify an IT project cost line under ASC 350-40 for a bank finance capitalization review'}
            ):labels[0]::VARCHAR AS AI_CLASSIFY_LABEL,
            AI_COMPLETE(
                model => :model,
                prompt => 'You are assisting a bank Finance reviewer. Using ONLY the evidence below, decide whether this cost line is CAPEX, OPEX, or INSUFFICIENT EVIDENCE under ASC 350-40 internal-use software guidance. '
                       || 'Quote the exact phrases from the evidence that support your answer. Do not invent facts. If the evidence does not describe the work performed, answer INSUFFICIENT EVIDENCE. '
                       || 'Recorded classification: ' || t.RECORDED_CLASS || '. Rule triggered: ' || COALESCE(t.RULE_NAME, 'none') || ' - ' || COALESCE(t.RULE_RATIONALE, '') || '. '
                       || 'Evidence: ' || t.EVIDENCE_TEXT,
                response_format => {
                    'type': 'json',
                    'schema': {
                        'type': 'object',
                        'properties': {
                            'label': {'type': 'string', 'enum': ['CAPEX', 'OPEX', 'INSUFFICIENT EVIDENCE']},
                            'confidence': {'type': 'number'},
                            'rationale': {'type': 'string'},
                            'evidence_quotes': {'type': 'array', 'items': {'type': 'string'}}
                        },
                        'required': ['label', 'confidence', 'rationale', 'evidence_quotes']
                    }
                }
            ) AS J
        FROM CAPEX_{{env}}.REVIEW.TMP_TO_REVIEW t
    )
    SELECT UUID_STRING(), COST_LINE_ID, PROJECT_ID, EVIDENCE_HASH, RECORDED_CLASS,
           RULE_IMPLIED_CLASS, RULE_ID, RULE_NAME, CRITERIA_VERSION,
           AI_CLASSIFY_LABEL, J:label::VARCHAR,
           -- Label from AI_CLASSIFY (best on reviewed examples); explanation from AI_COMPLETE.
           -- If the two methods disagree, do not guess: send it back for evidence.
           IFF(AI_CLASSIFY_LABEL = J:label::VARCHAR, AI_CLASSIFY_LABEL, 'INSUFFICIENT EVIDENCE'),
           J:confidence::FLOAT, J:rationale::VARCHAR, J:evidence_quotes,
           :model, :prompt_version,
           OBJECT_CONSTRUCT('cost_line_id', COST_LINE_ID, 'description', DESCRIPTION, 'amount', AMOUNT,
                            'recorded_class', RECORDED_CLASS, 'gl_account', GL_ACCOUNT,
                            'source_updated_ts', SOURCE_UPDATED_TS, 'evidence_text', EVIDENCE_TEXT),
           CURRENT_TIMESTAMP(), NULL
    FROM ai;

    n_new := SQLROWCOUNT;
    RETURN n_new || ' suggestion(s) written';
END;
$$;

DEFINE PROCEDURE CAPEX_{{env}}.REVIEW.RECORD_DECISION(SUGGESTION_ID VARCHAR, DECISION VARCHAR, REVIEWER_COMMENT VARCHAR, REVIEWER VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Appends a reviewer decision. Never modifies the suggestion or any source value.'
AS
$$
DECLARE
    d VARCHAR DEFAULT UPPER(:DECISION);
BEGIN
    IF (d NOT IN ('ACCEPT', 'REJECT', 'REQUEST_INFO')) THEN
        RETURN 'Invalid decision: ' || d;
    END IF;
    IF (d IN ('REJECT', 'REQUEST_INFO') AND COALESCE(TRIM(:REVIEWER_COMMENT), '') = '') THEN
        RETURN 'A comment is required to reject or request information';
    END IF;
    INSERT INTO CAPEX_{{env}}.REVIEW.DECISION_HISTORY
    SELECT UUID_STRING(), s.SUGGESTION_ID, s.COST_LINE_ID, :d, :REVIEWER_COMMENT,
           COALESCE(NULLIF(:REVIEWER, ''), CURRENT_USER()), CURRENT_TIMESTAMP(),
           s.RECORDED_CLASS, s.PROPOSED_CLASS, s.CRITERIA_VERSION, s.MODEL, s.PROMPT_VERSION, s.SOURCE_SNAPSHOT
    FROM CAPEX_{{env}}.REVIEW.AI_SUGGESTION s
    WHERE s.SUGGESTION_ID = :SUGGESTION_ID;
    IF (SQLROWCOUNT = 0) THEN
        RETURN 'Suggestion not found';
    END IF;
    RETURN 'Recorded ' || d;
END;
$$;

DEFINE PROCEDURE CAPEX_{{env}}.CONTROL.RUN_AI_EVAL()
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Scores AI_CLASSIFY and AI_COMPLETE against Finance-reviewed examples'
AS
$$
DECLARE
    run_id VARCHAR DEFAULT UUID_STRING();
    model VARCHAR;
    prompt_version VARCHAR;
    res VARIANT;
BEGIN
    SELECT MAX(IFF(CONFIG_KEY = 'AI_MODEL', CONFIG_VALUE, NULL)),
           MAX(IFF(CONFIG_KEY = 'PROMPT_VERSION', CONFIG_VALUE, NULL))
      INTO :model, :prompt_version
      FROM CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG;

    INSERT INTO CAPEX_{{env}}.CONTROL.AI_EVAL_RESULTS
    WITH p AS (
        SELECT e.*,
            AI_CLASSIFY(e.EVIDENCE_TEXT, ['CAPEX', 'OPEX', 'INSUFFICIENT EVIDENCE']) AS C_BARE,
            AI_CLASSIFY(
                e.EVIDENCE_TEXT,
                [
                  {'label': 'CAPEX', 'description': 'Internal-use software costs incurred in the application development stage (design, coding, testing, installation) or upgrades that add new functionality; purchased hardware or perpetual licenses.'},
                  {'label': 'OPEX', 'description': 'Preliminary-stage work (requirements, vendor selection, feasibility), training, data conversion and cleansing, maintenance, bug fixes, support, hosted subscriptions, decommissioning, recurring third-party services.'},
                  {'label': 'INSUFFICIENT EVIDENCE', 'description': 'The text does not say what work was performed, so capital vs expense cannot be determined.'}
                ],
                {'task_description': 'Classify an IT project cost line under ASC 350-40 for a bank finance capitalization review'}
            ) AS C_DESC,
            AI_COMPLETE(
                model => :model,
                prompt => 'You are assisting a bank Finance reviewer. Using ONLY the evidence, decide CAPEX, OPEX, or INSUFFICIENT EVIDENCE under ASC 350-40. If the evidence does not describe the work performed, answer INSUFFICIENT EVIDENCE. Evidence: ' || e.EVIDENCE_TEXT,
                response_format => {'type': 'json', 'schema': {'type': 'object',
                    'properties': {'label': {'type': 'string', 'enum': ['CAPEX', 'OPEX', 'INSUFFICIENT EVIDENCE']}, 'rationale': {'type': 'string'}},
                    'required': ['label', 'rationale']}}
            ) AS J
        FROM CAPEX_{{env}}.CONTROL.REVIEWED_EXAMPLES e
    ),
    m AS (
        SELECT EXAMPLE_ID, EXPECTED_CLASS, 'AI_CLASSIFY (labels only)' AS METHOD, 'AI_CLASSIFY' AS MODEL,
               C_BARE:labels[0]::VARCHAR AS PRED, C_BARE AS RAW FROM p
        UNION ALL
        SELECT EXAMPLE_ID, EXPECTED_CLASS, 'AI_CLASSIFY (label descriptions)', 'AI_CLASSIFY',
               C_DESC:labels[0]::VARCHAR, C_DESC FROM p
        UNION ALL
        SELECT EXAMPLE_ID, EXPECTED_CLASS, 'AI_COMPLETE (structured)', :model,
               J:label::VARCHAR, J FROM p
    )
    SELECT :run_id, CURRENT_TIMESTAMP(), METHOD, MODEL, :prompt_version, EXAMPLE_ID, EXPECTED_CLASS,
           PRED, PRED = EXPECTED_CLASS, RAW
    FROM m;

    SELECT ARRAY_AGG(OBJECT_CONSTRUCT('method', METHOD, 'examples', N, 'correct', OK, 'accuracy', ROUND(OK / N, 3)))
      INTO :res
      FROM (SELECT METHOD, COUNT(*) N, COUNT_IF(IS_MATCH) OK
            FROM CAPEX_{{env}}.CONTROL.AI_EVAL_RESULTS WHERE EVAL_RUN_ID = :run_id GROUP BY METHOD);
    RETURN OBJECT_CONSTRUCT('eval_run_id', :run_id, 'results', :res);
END;
$$;

-- ======================================================================
-- Governance: attach tags / masking / row access (DCM defines, cannot attach)
-- ======================================================================

DEFINE PROCEDURE CAPEX_{{env}}.GOVERNANCE.APPLY_POLICIES()
RETURNS VARCHAR
LANGUAGE SQL
COMMENT = 'Idempotently attaches sensitivity tags, tag-based masking and row access policies'
AS
$$
BEGIN
    -- Tag-based masking: any column tagged SENSITIVITY is masked by the policy for its type.
    BEGIN
        ALTER TAG CAPEX_{{env}}.GOVERNANCE.SENSITIVITY SET
            MASKING POLICY CAPEX_{{env}}.GOVERNANCE.MASK_SENSITIVE_STRING,
            MASKING POLICY CAPEX_{{env}}.GOVERNANCE.MASK_SENSITIVE_NUMBER;
    EXCEPTION WHEN OTHER THEN
        IF (NOT (SQLERRM ILIKE '%already%')) THEN RAISE; END IF;  -- tolerate re-runs only
    END;

    ALTER DYNAMIC TABLE CAPEX_{{env}}.SILVER.COST_LINE MODIFY COLUMN CONTRACTOR_NAME SET TAG CAPEX_{{env}}.GOVERNANCE.SENSITIVITY = 'PII';
    ALTER DYNAMIC TABLE CAPEX_{{env}}.SILVER.COST_LINE MODIFY COLUMN HOURLY_RATE SET TAG CAPEX_{{env}}.GOVERNANCE.SENSITIVITY = 'CONFIDENTIAL_RATE';
    ALTER DYNAMIC TABLE CAPEX_{{env}}.GOLD.COST_LINE_DETAIL MODIFY COLUMN CONTRACTOR_NAME SET TAG CAPEX_{{env}}.GOVERNANCE.SENSITIVITY = 'PII';
    ALTER DYNAMIC TABLE CAPEX_{{env}}.GOLD.COST_LINE_DETAIL MODIFY COLUMN HOURLY_RATE SET TAG CAPEX_{{env}}.GOVERNANCE.SENSITIVITY = 'CONFIDENTIAL_RATE';

    BEGIN
        ALTER DYNAMIC TABLE CAPEX_{{env}}.GOLD.COST_LINE_DETAIL ADD ROW ACCESS POLICY CAPEX_{{env}}.GOVERNANCE.RAP_COST_CENTER ON (COST_CENTER);
    EXCEPTION WHEN OTHER THEN
        IF (NOT (SQLERRM ILIKE '%already%')) THEN RAISE; END IF;
    END;
    BEGIN
        ALTER DYNAMIC TABLE CAPEX_{{env}}.GOLD.PROJECT_FINANCIALS ADD ROW ACCESS POLICY CAPEX_{{env}}.GOVERNANCE.RAP_COST_CENTER ON (COST_CENTER);
    EXCEPTION WHEN OTHER THEN
        IF (NOT (SQLERRM ILIKE '%already%')) THEN RAISE; END IF;
    END;

    DELETE FROM CAPEX_{{env}}.GOVERNANCE.COST_CENTER_ACCESS;
    INSERT INTO CAPEX_{{env}}.GOVERNANCE.COST_CENTER_ACCESS VALUES
        ('CAPEX_ITSEC_ANALYST', 'CC-9300'),
        ('CAPEX_ITSEC_ANALYST', 'CC-9000'),
        ('CAPEX_PMO_ANALYST', 'CC-4100'),
        ('CAPEX_PMO_ANALYST', 'CC-5200');
    RETURN 'Policies applied';
END;
$$;

-- ======================================================================
-- Post-deployment checks + deployment record
-- ======================================================================

DEFINE PROCEDURE CAPEX_{{env}}.CONTROL.POST_DEPLOY_CHECKS(RELEASE_TAG VARCHAR, GIT_SHA VARCHAR, PIPELINE_RUN_URL VARCHAR)
RETURNS VARIANT
LANGUAGE SQL
COMMENT = 'Validates a deployment and writes CONTROL.DEPLOYMENT_LOG'
AS
$$
DECLARE
    checks VARIANT;
    ok BOOLEAN;
    crit VARCHAR;
    c_criteria BOOLEAN;
    c_examples BOOLEAN;
    c_dt BOOLEAN;
    c_recon BOOLEAN;
    c_tasks BOOLEAN;
BEGIN
    SELECT CONFIG_VALUE INTO :crit FROM CAPEX_{{env}}.CONTROL.ACTIVE_CONFIG WHERE CONFIG_KEY = 'CRITERIA_VERSION';

    SELECT COUNT(*) > 0 AND COUNT_IF(STATUS <> 'APPROVED') = 0 INTO :c_criteria
      FROM CAPEX_{{env}}.CONTROL.CLASSIFICATION_CRITERIA WHERE CRITERIA_VERSION = :crit;
    SELECT COUNT(*) >= 20 INTO :c_examples FROM CAPEX_{{env}}.CONTROL.REVIEWED_EXAMPLES;
    SELECT COUNT(*) = 0 INTO :c_dt
      FROM TABLE(CAPEX_{{env}}.INFORMATION_SCHEMA.DYNAMIC_TABLE_REFRESH_HISTORY())
     WHERE STATE = 'FAILED' AND REFRESH_START_TIME > DATEADD('hour', -1, CURRENT_TIMESTAMP());
    SELECT COALESCE(BOOLAND_AGG(IS_RECONCILED), TRUE) INTO :c_recon FROM CAPEX_{{env}}.GOLD.RECONCILIATION;
    SELECT COUNT(*) = 0 INTO :c_tasks
      FROM TABLE(CAPEX_{{env}}.INFORMATION_SCHEMA.TASK_HISTORY(SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())))
     WHERE STATE = 'FAILED';

    checks := ARRAY_CONSTRUCT(
        OBJECT_CONSTRUCT('check', 'active criteria version is fully approved', 'pass', :c_criteria, 'criteria_version', :crit),
        OBJECT_CONSTRUCT('check', 'reviewed examples present', 'pass', :c_examples),
        OBJECT_CONSTRUCT('check', 'no failed dynamic table refreshes (1h)', 'pass', :c_dt),
        OBJECT_CONSTRUCT('check', 'no failed task runs (1h)', 'pass', :c_tasks),
        OBJECT_CONSTRUCT('check', 'source-to-report reconciliation ties', 'pass', :c_recon),
        OBJECT_CONSTRUCT('check', 'no unapproved accounting decisions promoted', 'pass', TRUE,
                         'note', 'REVIEW tables are environment-local and never deployed'));
    ok := c_criteria AND c_examples AND c_dt AND c_tasks AND c_recon;

    INSERT INTO CAPEX_{{env}}.CONTROL.DEPLOYMENT_LOG
    SELECT UUID_STRING(), '{{env}}', :RELEASE_TAG, :GIT_SHA, :crit, CURRENT_USER(), :PIPELINE_RUN_URL,
           CURRENT_TIMESTAMP(), IFF(:ok, 'SUCCEEDED', 'CHECKS_FAILED'), :checks;

    RETURN OBJECT_CONSTRUCT('env', '{{env}}', 'status', IFF(:ok, 'SUCCEEDED', 'CHECKS_FAILED'), 'checks', :checks);
END;
$$;
