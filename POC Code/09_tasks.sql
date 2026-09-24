/*
=============================================================================
  BlackRock EDP v2 — Step 09: Task DAG (6 Tasks, _V2 naming)
=============================================================================
  Run after: All SPs created (01-08)
  Role: EDP_ADMIN_ROLE

  Task DAG:
  INGEST_TASK_V2 → ML_TDQ_TASK_V2 → DBT_TASK_V2 → STG_BDQ_TASK_V2 → SDM_BDQ_TASK_V2 → FINALIZER_TASK_V2

  PRE-REQUISITE: Deploy the v2 dbt project:
    cd implementation_v2_variant/dbt_project
    snow dbt deploy \
        --project-path . \
        --name edp_dbt_project_v2 \
        --schema ORCHESTRATION \
        --database EDP_DB
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA ORCHESTRATION;

----------------------------------------------------------------------
-- 1. ROOT TASK: Ingest (VARIANT COPY INTO)
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2
    WAREHOUSE   = EDP_INGEST_WH
    SCHEDULE    = 'USING CRON 0 */4 * * * UTC'
    COMMENT     = 'v2 Root task: loads Parquet into VARIANT mirror (named columns, no positional mapping)'
AS
    CALL EDP_DB.ORCHESTRATION.RUN_COPY_INTO_V2('TRANSACTIONS_V2');

----------------------------------------------------------------------
-- 2. ML TDQ TASK: Snowpark lazy agg + GX on VARIANT mirror
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.ML_TDQ_TASK_V2
    WAREHOUSE   = EDP_TDQ_WH
    COMMENT     = 'v2 ML TDQ: Snowpark lazy agg (1 scan) + GX metrics validation on VARIANT mirror'
    AFTER       EDP_DB.ORCHESTRATION.INGEST_TASK_V2
AS
    CALL EDP_DB.ORCHESTRATION.RUN_ML_TDQ_V2(
        'TRANSACTIONS_V2',
        (SELECT _BATCH_ID FROM EDP_DB.MIRROR.TRANSACTIONS_V2 ORDER BY _LOADED_AT DESC LIMIT 1)
    );

----------------------------------------------------------------------
-- 3. DBT TASK: Transform VARIANT Mirror → Staging
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.DBT_TASK_V2
    WAREHOUSE   = EDP_DBT_WH
    COMMENT     = 'v2 dbt: extracts typed columns from VARIANT mirror → staging (incremental)'
    AFTER       EDP_DB.ORCHESTRATION.ML_TDQ_TASK_V2
AS
    EXECUTE DBT PROJECT EDP_DB.ORCHESTRATION.EDP_DBT_PROJECT_V2;

----------------------------------------------------------------------
-- 4. STG BDQ TASK
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.STG_BDQ_TASK_V2
    WAREHOUSE   = EDP_TDQ_WH
    COMMENT     = 'v2 STG BDQ: Snowpark lazy agg + ref joins + GX metrics'
    AFTER       EDP_DB.ORCHESTRATION.DBT_TASK_V2
AS
    CALL EDP_DB.ORCHESTRATION.RUN_STG_BDQ_V2(
        'STG_TRANSACTIONS',
        (SELECT _BATCH_ID FROM EDP_DB.STAGING.STG_TRANSACTIONS ORDER BY _LOADED_AT DESC LIMIT 1)
    );

----------------------------------------------------------------------
-- 5. SDM BDQ TASK
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.SDM_BDQ_TASK_V2
    WAREHOUSE   = EDP_TDQ_WH
    COMMENT     = 'v2 SDM BDQ: DT refresh + Snowpark lazy agg + GX metrics + cross-table'
    AFTER       EDP_DB.ORCHESTRATION.STG_BDQ_TASK_V2
AS
    CALL EDP_DB.ORCHESTRATION.RUN_SDM_BDQ_V2(
        'TRANSACTIONS_MART',
        (SELECT _BATCH_ID FROM EDP_DB.STAGING.STG_TRANSACTIONS ORDER BY _LOADED_AT DESC LIMIT 1)
    );

----------------------------------------------------------------------
-- 6. FINALIZER STORED PROCEDURE
--    Logs pipeline completion AND captures per-task timing into
--    STRESS_TEST_RESULTS. Derives timing from TDQ_RESULTS (execution
--    timestamps) and COPY_HISTORY (ingest start).
----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_FINALIZER_V2()
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_batch_id VARCHAR;
    v_run_id VARCHAR DEFAULT UUID_STRING();
    v_ml_start TIMESTAMP_NTZ;
    v_ml_end TIMESTAMP_NTZ;
    v_ml_secs FLOAT;
    v_stg_start TIMESTAMP_NTZ;
    v_stg_end TIMESTAMP_NTZ;
    v_stg_secs FLOAT;
    v_sdm_start TIMESTAMP_NTZ;
    v_sdm_end TIMESTAMP_NTZ;
    v_sdm_secs FLOAT;
    v_ingest_start TIMESTAMP_NTZ;
    v_ingest_secs FLOAT DEFAULT 0;
    v_dbt_secs FLOAT DEFAULT 0;
    v_row_count NUMBER;
    v_ingest_wh_size VARCHAR DEFAULT 'UNKNOWN';
    v_tdq_wh_size VARCHAR DEFAULT 'UNKNOWN';
    v_dbt_wh_size VARCHAR DEFAULT 'UNKNOWN';
BEGIN
    SELECT _BATCH_ID INTO :v_batch_id
    FROM EDP_DB.MIRROR.TRANSACTIONS_V2
    ORDER BY _LOADED_AT DESC LIMIT 1;

    INSERT INTO EDP_DB.AUDIT.PIPELINE_RUNS
        (BATCH_ID, STATUS, ML_TDQ_PASSED, STG_BDQ_PASSED, SDM_BDQ_PASSED, NOTES)
    VALUES (:v_batch_id, 'SUCCESS', TRUE, TRUE, TRUE,
        'v2 Full pipeline completed successfully (VARIANT mirror + optimized GX)');

    -- ML TDQ timing from TDQ_RESULTS
    SELECT TIMESTAMPADD('second', -EXECUTION_TIME_SECS::INTEGER, RUN_AT),
           RUN_AT, EXECUTION_TIME_SECS, ROW_COUNT
    INTO :v_ml_start, :v_ml_end, :v_ml_secs, :v_row_count
    FROM EDP_DB.AUDIT.TDQ_RESULTS
    WHERE BATCH_ID = :v_batch_id AND LAYER = 'ML'
    ORDER BY RUN_AT DESC LIMIT 1;

    -- STG BDQ timing
    SELECT TIMESTAMPADD('second', -EXECUTION_TIME_SECS::INTEGER, RUN_AT),
           RUN_AT, EXECUTION_TIME_SECS
    INTO :v_stg_start, :v_stg_end, :v_stg_secs
    FROM EDP_DB.AUDIT.TDQ_RESULTS
    WHERE BATCH_ID = :v_batch_id AND LAYER = 'STG'
    ORDER BY RUN_AT DESC LIMIT 1;

    -- SDM BDQ timing
    SELECT TIMESTAMPADD('second', -EXECUTION_TIME_SECS::INTEGER, RUN_AT),
           RUN_AT, EXECUTION_TIME_SECS
    INTO :v_sdm_start, :v_sdm_end, :v_sdm_secs
    FROM EDP_DB.AUDIT.TDQ_RESULTS
    WHERE BATCH_ID = :v_batch_id AND LAYER = 'SDM'
    ORDER BY RUN_AT DESC LIMIT 1;

    -- INGEST start from COPY_HISTORY
    SELECT MIN(LAST_LOAD_TIME)
    INTO :v_ingest_start
    FROM TABLE(EDP_DB.INFORMATION_SCHEMA.COPY_HISTORY(
        TABLE_NAME => 'EDP_DB.MIRROR.TRANSACTIONS_V2',
        START_TIME => DATEADD('hour', -2, CURRENT_TIMESTAMP())
    ));

    v_ingest_secs := TIMESTAMPDIFF('second', :v_ingest_start, :v_ml_start);
    v_dbt_secs := TIMESTAMPDIFF('second', :v_ml_end, :v_stg_start);

    -- Look up actual warehouse sizes dynamically
    SHOW WAREHOUSES LIKE 'EDP_INGEST_WH';
    SELECT "size" INTO :v_ingest_wh_size FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW WAREHOUSES LIKE 'EDP_TDQ_WH';
    SELECT "size" INTO :v_tdq_wh_size FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    SHOW WAREHOUSES LIKE 'EDP_DBT_WH';
    SELECT "size" INTO :v_dbt_wh_size FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()));

    -- Insert all 5 task rows
    INSERT INTO EDP_DB.AUDIT.STRESS_TEST_RESULTS
        (TEST_RUN_ID, PHASE, START_TS, END_TS, DURATION_SEC, ROWS_PROCESSED, WAREHOUSE_SIZE, NOTES)
    SELECT :v_run_id, 'INGEST_TASK_V2', :v_ingest_start, :v_ml_start, :v_ingest_secs, :v_row_count,
           :v_ingest_wh_size, 'EDP_INGEST_WH — COPY INTO VARIANT mirror'
    UNION ALL
    SELECT :v_run_id, 'ML_TDQ_TASK_V2', :v_ml_start, :v_ml_end, :v_ml_secs, :v_row_count,
           :v_tdq_wh_size, 'EDP_TDQ_WH — Snowpark lazy agg + GX + cross-batch join'
    UNION ALL
    SELECT :v_run_id, 'DBT_TASK_V2', :v_ml_end, :v_stg_start, :v_dbt_secs, :v_row_count,
           :v_dbt_wh_size, 'EDP_DBT_WH — dbt incremental MERGE into staging'
    UNION ALL
    SELECT :v_run_id, 'STG_BDQ_TASK_V2', :v_stg_start, :v_stg_end, :v_stg_secs, :v_row_count,
           :v_tdq_wh_size, 'EDP_TDQ_WH — Snowpark lazy agg + ref integrity + provenance'
    UNION ALL
    SELECT :v_run_id, 'SDM_BDQ_TASK_V2', :v_sdm_start, :v_sdm_end, :v_sdm_secs, :v_row_count,
           :v_tdq_wh_size, 'EDP_TDQ_WH — DT refresh + GX + cross-table consistency';

    -- TOTAL E2E
    INSERT INTO EDP_DB.AUDIT.STRESS_TEST_RESULTS
        (TEST_RUN_ID, PHASE, START_TS, END_TS, DURATION_SEC, ROWS_PROCESSED, WAREHOUSE_SIZE, NOTES)
    SELECT :v_run_id, 'TOTAL_PIPELINE_E2E',
        MIN(START_TS), MAX(END_TS),
        TIMESTAMPDIFF('second', MIN(START_TS), MAX(END_TS)),
        :v_row_count, 'MIXED',
        '5 tasks + finalizer, batch_id=' || :v_batch_id
    FROM EDP_DB.AUDIT.STRESS_TEST_RESULTS
    WHERE TEST_RUN_ID = :v_run_id;

    RETURN 'Finalizer complete. Run ID: ' || :v_run_id || ', Batch: ' || :v_batch_id;
END;
$$;

----------------------------------------------------------------------
-- 6b. FINALIZER TASK (calls the SP above)
----------------------------------------------------------------------
CREATE OR REPLACE TASK EDP_DB.ORCHESTRATION.FINALIZER_TASK_V2
    WAREHOUSE   = EDP_INGEST_WH
    COMMENT     = 'v2 Logs pipeline completion + captures per-task timing into STRESS_TEST_RESULTS'
    AFTER       EDP_DB.ORCHESTRATION.SDM_BDQ_TASK_V2
AS
    CALL EDP_DB.ORCHESTRATION.RUN_FINALIZER_V2();

----------------------------------------------------------------------
-- 7. RESUME TASKS (leaf to root)
----------------------------------------------------------------------
ALTER TASK EDP_DB.ORCHESTRATION.FINALIZER_TASK_V2 RESUME;
ALTER TASK EDP_DB.ORCHESTRATION.SDM_BDQ_TASK_V2   RESUME;
ALTER TASK EDP_DB.ORCHESTRATION.STG_BDQ_TASK_V2   RESUME;
ALTER TASK EDP_DB.ORCHESTRATION.DBT_TASK_V2       RESUME;
ALTER TASK EDP_DB.ORCHESTRATION.ML_TDQ_TASK_V2    RESUME;
ALTER TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2    RESUME;

----------------------------------------------------------------------
-- 8. MANUAL TRIGGER (for testing)
----------------------------------------------------------------------
-- EXECUTE TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2;

SELECT 'v2 Step 09 complete: Task DAG created and resumed.' AS status;
