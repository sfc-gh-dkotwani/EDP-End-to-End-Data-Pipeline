/*
=============================================================================
  BlackRock EDP v2 — Step 02p: VARIANT Mirror Layer (Parquet)
=============================================================================
  Run after: 01_setup.sql (or v1 01_setup.sql for initial infra)
  Role: EDP_ADMIN_ROLE

  APPROACH:
    Parquet files are self-describing — each row is a named-column object.
    Snowflake loads Parquet $1 directly as a VARIANT with the original
    column names preserved. No OBJECT_CONSTRUCT or positional mapping needed.

  SCHEMA CHANGE CONTRACT:
    - New columns appended: safe — new keys appear in VARIANT automatically.
    - Columns inserted in the middle: safe — Parquet identifies by name.
    - Columns reordered: safe — Parquet identifies by name.
    - Columns removed: missing keys resolve to NULL in downstream extraction.
    - Columns renamed: old key disappears, new key appears. Downstream
      extraction (RAW_DATA:OLD_NAME) returns NULL. Caught by completeness
      checks in ML TDQ.

  NOTE ON CSV:
    For CSV sources, Snowflake cannot use header-based column mapping
    in COPY INTO with transformations. CSV ingestion requires positional
    $N mapping with OBJECT_CONSTRUCT — see 02_mirror_layer_variant.sql
    for that approach.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA MIRROR;
USE WAREHOUSE EDP_INGEST_WH;

----------------------------------------------------------------------
-- 1. FILE FORMAT
----------------------------------------------------------------------
CREATE FILE FORMAT IF NOT EXISTS EDP_DB.MIRROR.PARQUET_FMT
    TYPE = 'PARQUET'
    COMMENT = 'Parquet format for VARIANT mirror ingestion';

----------------------------------------------------------------------
-- 2. VARIANT MIRROR TABLE
----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS EDP_DB.MIRROR.TRANSACTIONS_V2 (
    RAW_DATA        VARIANT,
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _FILENAME       VARCHAR,
    _FILE_ROW_NUM   NUMBER,
    _BATCH_ID       VARCHAR
)
COMMENT = 'v2 Mirror layer: raw source data as VARIANT (Parquet — named columns, no positional mapping)';

----------------------------------------------------------------------
-- 3. INTERNAL STAGE
----------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS EDP_DB.MIRROR.RAW_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.PARQUET_FMT')
    COMMENT     = 'Landing zone for EDP source files (Parquet)';

----------------------------------------------------------------------
-- 4. STORED PROCEDURE — RUN_COPY_INTO_V2 (Parquet)
----------------------------------------------------------------------
USE SCHEMA EDP_DB.ORCHESTRATION;

CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_COPY_INTO_V2(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'v2 COPY INTO: loads Parquet into VARIANT. Column names from file metadata. No positional mapping. Scales to 10K+ files.'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import uuid
from datetime import datetime

def run(session: Session, table_name: str) -> str:
    batch_id   = str(uuid.uuid4())
    start_time = datetime.utcnow()

    # ================================================================
    # PARQUET VARIANT LOADING
    #
    # Parquet files are self-describing: $1 is a VARIANT object with
    # the original column names from the file. No OBJECT_CONSTRUCT
    # or positional mapping needed.
    #
    # SCHEMA CHANGE SAFETY:
    #   - Columns added/removed/reordered: all safe (Parquet uses names)
    #   - Columns renamed: downstream extraction returns NULL (caught by
    #     ML TDQ completeness checks)
    #
    # PERFORMANCE:
    #   - No INFER_SCHEMA (eliminates 10K-file bottleneck)
    #   - Single COPY INTO statement
    #   - VARIANT column: no ALTER TABLE for schema evolution
    # ================================================================
    copy_sql = f"""
        COPY INTO EDP_DB.MIRROR.TRANSACTIONS_V2 (RAW_DATA, _FILENAME, _FILE_ROW_NUM, _BATCH_ID)
        FROM (
            SELECT
                $1,
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                '{batch_id}'
            FROM @EDP_DB.MIRROR.RAW_STAGE
            (FILE_FORMAT => 'EDP_DB.MIRROR.PARQUET_FMT')
        )
        ON_ERROR    = 'SKIP_FILE'
        PURGE       = FALSE
        FORCE       = FALSE
    """

    result       = session.sql(copy_sql).collect()
    files_loaded = len(result)
    total_rows   = 0
    for r in result:
        try:
            val = r.get('ROWS_LOADED') or r.get('rows_loaded')
            if val is not None:
                total_rows += int(val)
        except (KeyError, TypeError, ValueError, AttributeError):
            pass

    elapsed = (datetime.utcnow() - start_time).total_seconds()
    return (
        f"COPY INTO complete (v2 VARIANT, Parquet). batch_id={batch_id}, "
        f"files={files_loaded}, rows_loaded={total_rows}, elapsed={elapsed:.1f}s"
    )
$$;

SELECT 'v2 Step 02p complete: VARIANT mirror layer created (Parquet — named columns).' AS status;
