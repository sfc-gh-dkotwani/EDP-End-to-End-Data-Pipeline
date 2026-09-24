/*
=============================================================================
  BlackRock EDP v2 — Step 10: Alerts and Monitoring
=============================================================================
  Run after: 09_tasks.sql
  Role: EDP_ADMIN_ROLE

  Same alerts as v1 — they monitor the shared AUDIT tables which both
  v1 and v2 pipelines write to.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA ORCHESTRATION;
USE WAREHOUSE EDP_TDQ_WH;

----------------------------------------------------------------------
-- 1. NOTIFICATION INTEGRATION (reuse existing from v1)
----------------------------------------------------------------------
-- If not already created by v1, run as ACCOUNTADMIN:
-- CREATE OR REPLACE NOTIFICATION INTEGRATION EDP_EMAIL_INTEGRATION
--     TYPE = EMAIL ENABLED = TRUE;
-- GRANT USAGE ON INTEGRATION EDP_EMAIL_INTEGRATION TO ROLE EDP_ADMIN_ROLE;

----------------------------------------------------------------------
-- 2. TDQ/BDQ FAILURE ALERT (reuse if already exists from v1)
----------------------------------------------------------------------
CREATE ALERT IF NOT EXISTS EDP_DB.ORCHESTRATION.TDQ_FAILURE_ALERT
    WAREHOUSE = EDP_TDQ_WH
    SCHEDULE  = '5 MINUTES'
    COMMENT   = 'Fires when any TDQ/BDQ check fails across any layer (v1 or v2)'
    IF (EXISTS (
        SELECT 1 FROM EDP_DB.AUDIT.TDQ_RESULTS
        WHERE PASSED   = FALSE
          AND RUN_AT  >= DATEADD('minute', -10, CURRENT_TIMESTAMP())
    ))
THEN
        CALL SYSTEM$SEND_EMAIL(
            'EDP_EMAIL_INTEGRATION',
            'divya.kotwani@snowflake.com',
            'EDP Pipeline Alert: Data Quality Check Failed',
            (SELECT
                'Layer: ' || LAYER ||
                ' | Table: ' || TABLE_NAME ||
                ' | Failed: ' || FAILED_COUNT || '/' || TOTAL_CHECKS ||
                ' | Time: ' || TO_VARCHAR(RUN_AT, 'YYYY-MM-DD HH24:MI:SS') ||
                ' | Batch: ' || BATCH_ID
            FROM EDP_DB.AUDIT.TDQ_RESULTS
            WHERE PASSED = FALSE
            ORDER BY RUN_AT DESC
            LIMIT 1)
        );

-- ALTER ALERT EDP_DB.ORCHESTRATION.TDQ_FAILURE_ALERT RESUME;

----------------------------------------------------------------------
-- 3. MONITORING QUERIES (same as v1)
----------------------------------------------------------------------

-- v2 Task execution history:
/*
SELECT
    NAME AS TASK_NAME, STATE, SCHEDULED_TIME, COMPLETED_TIME,
    DATEDIFF('second', SCHEDULED_TIME, COMPLETED_TIME) AS DURATION_SECS,
    ERROR_MESSAGE
FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
    SCHEDULED_TIME_RANGE_START => DATEADD('hour', -24, CURRENT_TIMESTAMP())
))
WHERE DATABASE_NAME = 'EDP_DB'
  AND NAME LIKE '%_V2'
ORDER BY SCHEDULED_TIME DESC;
*/

-- v2 Pipeline runs:
/*
SELECT * FROM EDP_DB.AUDIT.PIPELINE_RUNS
WHERE NOTES LIKE '%v2%'
ORDER BY RUN_AT DESC LIMIT 20;
*/

-- Performance comparison v1 vs v2:
/*
SELECT NOTES, AVG(EXECUTION_TIME_SECS) AS AVG_TIME, AVG(ROWS_PER_SECOND) AS AVG_RPS
FROM EDP_DB.AUDIT.PERFORMANCE_BENCHMARKS
GROUP BY NOTES
ORDER BY AVG_TIME;
*/

SELECT 'v2 Step 10 complete: Alerts and monitoring configured.' AS status;
