/*
=============================================================================
  BlackRock EDP v2 — Step 02: VARIANT Mirror Layer
=============================================================================
  Run after: 01_setup.sql (or v1 01_setup.sql for initial infra)
  Role: EDP_ADMIN_ROLE

  KEY CHANGE FROM v1:
    v1: INFER_SCHEMA on every load → typed VARCHAR columns → ALTER TABLE for drift
    v2: OBJECT_CONSTRUCT with positional $N → single VARIANT column → no schema inference

  WHY POSITIONAL MAPPING ($1, $2, ...):
    Snowflake's COPY INTO has a limitation: PARSE_HEADER=TRUE (which reads
    column names from CSV headers) is NOT allowed in COPY transformations
    (SELECT ... FROM @stage). This means you cannot do:
      COPY INTO variant_table FROM (SELECT OBJECT_CONSTRUCT(*) FROM @stage)
    with PARSE_HEADER — it either errors or produces generic keys (c1, c2).

    Positional mapping with explicit key names solves this:
      OBJECT_CONSTRUCT('TXN_ID', $1, 'AMOUNT', $4, ...)
    This is the simplest, fastest single-statement approach.

  SCHEMA CHANGE CONTRACT:
    - New columns APPENDED at the end: safe. Existing columns unaffected.
      New column data is silently dropped until the SP is updated.
    - Columns INSERTED in the middle: BREAKS positional mapping.
      Data goes into wrong VARIANT keys (silent corruption).
    - Columns REMOVED: BREAKS positional mapping.
      Subsequent columns shift left (silent corruption).

    SAFETY NET: The ML TDQ layer (05_ml_tdq_optimized.sql) includes
    column-shift detection checks that catch scenarios 2 and 3 by
    validating that each VARIANT key's values match expected data types
    (e.g., AMOUNT is numeric, TXN_DATE is a valid date). If columns
    shift, type mismatches surface as high failure counts.

  CUSTOMER ACTION:
    If the source CSV schema changes (columns added/removed/reordered),
    update the OBJECT_CONSTRUCT mapping in this SP. This is a one-time
    change per schema update — not per load.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA MIRROR;
USE WAREHOUSE EDP_INGEST_WH;

----------------------------------------------------------------------
-- 1. FILE FORMATS
----------------------------------------------------------------------
-- SKIP_HEADER=1: standard load format for positional $N references.
CREATE FILE FORMAT IF NOT EXISTS EDP_DB.MIRROR.CSV_FMT
    TYPE                          = 'CSV'
    FIELD_DELIMITER               = ','
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    SKIP_HEADER                   = 1
    NULL_IF                       = ('NULL', '', 'N/A', 'n/a', '\N')
    EMPTY_FIELD_AS_NULL           = TRUE
    TRIM_SPACE                    = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'CSV format with SKIP_HEADER for positional COPY INTO';

-- PARSE_HEADER=TRUE: kept for INFER_SCHEMA ad-hoc usage (not used in load path).
CREATE FILE FORMAT IF NOT EXISTS EDP_DB.MIRROR.CSV_INFER_FMT
    TYPE                          = 'CSV'
    FIELD_DELIMITER               = ','
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    PARSE_HEADER                  = TRUE
    NULL_IF                       = ('NULL', '', 'N/A', 'n/a', '\N')
    EMPTY_FIELD_AS_NULL           = TRUE
    TRIM_SPACE                    = TRUE
    COMMENT = 'CSV format with PARSE_HEADER — for ad-hoc INFER_SCHEMA';

----------------------------------------------------------------------
-- 2. VARIANT MIRROR TABLE
----------------------------------------------------------------------
CREATE OR REPLACE TABLE EDP_DB.MIRROR.TRANSACTIONS_V2 (
    RAW_DATA        VARIANT,
    _LOADED_AT      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    _FILENAME       VARCHAR,
    _FILE_ROW_NUM   NUMBER,
    _BATCH_ID       VARCHAR
)
COMMENT = 'v2 Mirror layer: raw source data as VARIANT (positional column mapping, no schema inference)';

----------------------------------------------------------------------
-- 3. INTERNAL STAGE (reuse existing)
----------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS EDP_DB.MIRROR.RAW_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.CSV_FMT')
    COMMENT     = 'Landing zone for EDP source files';

----------------------------------------------------------------------
-- 4. STORED PROCEDURE — RUN_COPY_INTO_V2 (positional mapping)
----------------------------------------------------------------------
USE SCHEMA EDP_DB.ORCHESTRATION;

CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_COPY_INTO_V2(TABLE_NAME VARCHAR)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'v2 COPY INTO: loads CSV into VARIANT via OBJECT_CONSTRUCT with positional $N. No INFER_SCHEMA. Scales to 10K+ files.'
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
    # SINGLE-STATEMENT VARIANT LOADING — POSITIONAL $N MAPPING
    #
    # WHY POSITIONAL:
    #   PARSE_HEADER=TRUE is not allowed in COPY INTO with
    #   transformations (SELECT ... FROM @stage). So we use
    #   SKIP_HEADER=1 (CSV_FMT) and map column positions explicitly.
    #
    # SCHEMA CHANGE CONTRACT:
    #   - Columns appended at end: safe (new cols ignored)
    #   - Columns inserted/removed: breaks mapping (caught by ML TDQ)
    #   - Update OBJECT_CONSTRUCT below if source schema changes
    #
    # PERFORMANCE vs v1:
    #   - No INFER_SCHEMA (eliminates 10K-file bottleneck)
    #   - Single COPY INTO statement (vs INFER + CREATE TABLE + COPY)
    #   - VARIANT column: no ALTER TABLE for schema evolution
    # ================================================================
    copy_sql = f"""
        COPY INTO EDP_DB.MIRROR.TRANSACTIONS_V2 (RAW_DATA, _FILENAME, _FILE_ROW_NUM, _BATCH_ID)
        FROM (
            SELECT
                OBJECT_CONSTRUCT(
                    'TXN_ID',          $1,
                    'ACCOUNT_ID',      $2,
                    'INSTRUMENT_ID',   $3,
                    'AMOUNT',          $4,
                    'CURRENCY',        $5,
                    'TXN_DATE',        $6,
                    'SETTLEMENT_DATE', $7,
                    'STATUS',          $8
                ),
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                '{batch_id}'
            FROM @EDP_DB.MIRROR.RAW_STAGE
            (FILE_FORMAT => 'EDP_DB.MIRROR.CSV_FMT')
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
        f"COPY INTO complete (v2 VARIANT, positional mapping). batch_id={batch_id}, "
        f"files={files_loaded}, rows_loaded={total_rows}, elapsed={elapsed:.1f}s"
    )
$$;

SELECT 'v2 Step 02 complete: VARIANT mirror layer created (positional mapping, no INFER_SCHEMA).' AS status;
