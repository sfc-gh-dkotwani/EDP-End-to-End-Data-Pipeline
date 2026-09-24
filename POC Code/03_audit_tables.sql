/*
=============================================================================
  BlackRock EDP v2 — Step 03: Audit Tables
=============================================================================
  Run after: 02_mirror_layer_variant.sql
  Role: EDP_ADMIN_ROLE

  These are identical to v1 audit tables. Uses CREATE TABLE IF NOT EXISTS
  so they can coexist with the v1 deployment.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA AUDIT;
USE WAREHOUSE EDP_INGEST_WH;

----------------------------------------------------------------------
-- 1. TDQ/BDQ RESULTS
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EDP_DB.AUDIT.TDQ_RESULTS (
    RESULT_ID       NUMBER AUTOINCREMENT PRIMARY KEY,
    BATCH_ID        VARCHAR         NOT NULL,
    TABLE_NAME      VARCHAR         NOT NULL,
    LAYER           VARCHAR         NOT NULL,
    CHECK_TYPE      VARCHAR         NOT NULL,
    RUN_AT          TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP(),
    PASSED          BOOLEAN         NOT NULL,
    TOTAL_CHECKS    NUMBER          DEFAULT 0,
    FAILED_COUNT    NUMBER          DEFAULT 0,
    FAILED_DETAILS  VARIANT,
    ROW_COUNT       NUMBER,
    EXECUTION_TIME_SECS FLOAT,
    WAREHOUSE_SIZE  VARCHAR,
    SUITE_NAME      VARCHAR
);

----------------------------------------------------------------------
-- 2. PIPELINE RUNS
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EDP_DB.AUDIT.PIPELINE_RUNS (
    RUN_ID          NUMBER AUTOINCREMENT PRIMARY KEY,
    BATCH_ID        VARCHAR,
    RUN_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    STATUS          VARCHAR,
    ML_TDQ_PASSED   BOOLEAN,
    STG_BDQ_PASSED  BOOLEAN,
    SDM_BDQ_PASSED  BOOLEAN,
    TOTAL_TIME_SECS FLOAT,
    NOTES           VARCHAR
);

----------------------------------------------------------------------
-- 3. PERFORMANCE BENCHMARKS
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EDP_DB.AUDIT.PERFORMANCE_BENCHMARKS (
    BENCHMARK_ID    NUMBER AUTOINCREMENT PRIMARY KEY,
    RUN_AT          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    LAYER           VARCHAR,
    CHECK_CATEGORY  VARCHAR,
    ROW_COUNT       NUMBER,
    DATA_SIZE_GB    FLOAT,
    WAREHOUSE_SIZE  VARCHAR,
    EXECUTION_TIME_SECS FLOAT,
    ROWS_PER_SECOND FLOAT,
    NOTES           VARCHAR
);

----------------------------------------------------------------------
-- 4. STRESS TEST RESULTS (per-task timing, auto-populated by Finalizer)
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EDP_DB.AUDIT.STRESS_TEST_RESULTS (
    TEST_RUN_ID     VARCHAR DEFAULT UUID_STRING(),
    PHASE           VARCHAR,
    START_TS        TIMESTAMP_NTZ,
    END_TS          TIMESTAMP_NTZ,
    DURATION_SEC    FLOAT,
    ROWS_PROCESSED  NUMBER,
    FILES_PROCESSED NUMBER,
    ROWS_PER_SEC    FLOAT,
    WAREHOUSE_SIZE  VARCHAR,
    NOTES           VARCHAR,
    CREATED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- NOTE: SCHEMA_EVOLUTION table is not used in v2 (no INFER_SCHEMA).
-- It still exists from v1 for backwards compatibility.

SELECT 'v2 Step 03 complete: Audit tables verified.' AS status;
