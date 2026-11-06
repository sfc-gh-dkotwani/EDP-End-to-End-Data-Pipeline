/*
=============================================================================
  BlackRock EDP — Step 02a: Migrate an existing Mirror to the technical columns
=============================================================================
  Run after: 02_mirror_layer_parquet.sql (or _variant.sql) on an account where
  EDP_DB.MIRROR.TRANSACTIONS_V2 and RAW_STAGE already exist.
  Role: EDP_ADMIN_ROLE

  WHY:
    The 02 scripts use CREATE ... IF NOT EXISTS, so they do not change a table
    or stage that is already there. The ingestion procedure writes 17 more
    columns and the ingest task reads DIRECTORY(@RAW_STAGE); both fail on the
    original 5-column table and a stage without a directory table.

  SAFE TO RE-RUN:
    Every statement is IF [NOT] EXISTS. Existing rows keep their values; the
    new columns are NULL for rows loaded before this migration.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE WAREHOUSE EDP_INGEST_WH;

----------------------------------------------------------------------
-- 1. MIRROR TABLE: add _SCHEMA_FP and the EDP technical columns
----------------------------------------------------------------------
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS _SCHEMA_FP           VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS DATA_DOMAIN          VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS CORRECTION_FL        VARCHAR(1);
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS FILE_RECORD_NBR      NUMBER;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS JOB_VERSION_NM       VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS PROVIDER_FILE_ID     NUMBER;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS PROVIDER_FILE_DT     TIMESTAMP_NTZ;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS UNIQUE_HASH_ID       VARCHAR(32);
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS UPDATED_BY_USER_ID   VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS PROVIDER_NM          VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS CREATED_DTS          TIMESTAMP_LTZ;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS PROVIDER_FILE_NM     VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS UPDATED_DTS          TIMESTAMP_LTZ;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS CREATED_BY_USER_ID   VARCHAR;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS FILE_CORRECTION_DTS  TIMESTAMP_NTZ;
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS ROW_HASH_ID          VARCHAR(32);
ALTER TABLE IF EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 ADD COLUMN IF NOT EXISTS ACQ_REPROCESSING_DTM TIMESTAMP_NTZ;

----------------------------------------------------------------------
-- 2. STAGE: the ingest task lists the batch from DIRECTORY(@RAW_STAGE)
----------------------------------------------------------------------
ALTER STAGE IF EXISTS EDP_DB.MIRROR.RAW_STAGE SET DIRECTORY = (ENABLE = TRUE);
ALTER STAGE IF EXISTS EDP_DB.MIRROR.RAW_STAGE REFRESH;

----------------------------------------------------------------------
-- 3. HEADER-READ FORMAT: an empty header name must read as '' (not NULL)
--    so the CSV procedure rejects it. Only needed if the format already
--    exists from an earlier run of 02_mirror_layer_variant.sql.
----------------------------------------------------------------------
ALTER FILE FORMAT IF EXISTS EDP_DB.MIRROR.CSV_HEADER_FMT SET EMPTY_FIELD_AS_NULL = FALSE;

----------------------------------------------------------------------
-- 4. VERIFY: expect 22 columns and directory_enabled = Y
----------------------------------------------------------------------
SELECT COUNT(*) AS column_count
FROM EDP_DB.INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_SCHEMA = 'MIRROR' AND TABLE_NAME = 'TRANSACTIONS_V2';

SHOW STAGES LIKE 'RAW_STAGE' IN SCHEMA EDP_DB.MIRROR;

SELECT 'Step 02a complete: Mirror migrated to the technical columns.' AS status;
