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
    COPY INTO cannot read a CSV header inside a transformation, so the CSV
    procedure reads each batch's headers first and keys RAW_DATA from them
    — see 02_mirror_layer_variant.sql.

  TECHNICAL COLUMNS:
    The BlackRock EDP technical columns are filled in the same COPY, from the
    caller's run context, the metadata values, and METADATA$ fields. The
    _ columns are kept for the downstream layers (05-09, dbt).
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
    _BATCH_ID       VARCHAR,
    _SCHEMA_FP      VARCHAR,
    -- BlackRock EDP technical columns, filled in the same COPY
    DATA_DOMAIN             VARCHAR,
    CORRECTION_FL           VARCHAR(1),
    FILE_RECORD_NBR         NUMBER,
    JOB_VERSION_NM          VARCHAR,
    PROVIDER_FILE_ID        NUMBER,
    PROVIDER_FILE_DT        TIMESTAMP_NTZ,
    UNIQUE_HASH_ID          VARCHAR(32),
    UPDATED_BY_USER_ID      VARCHAR,
    PROVIDER_NM             VARCHAR,
    CREATED_DTS             TIMESTAMP_LTZ,
    PROVIDER_FILE_NM        VARCHAR,
    UPDATED_DTS             TIMESTAMP_LTZ,
    CREATED_BY_USER_ID      VARCHAR,
    FILE_CORRECTION_DTS     TIMESTAMP_NTZ,
    ROW_HASH_ID             VARCHAR(32),
    ACQ_REPROCESSING_DTM    TIMESTAMP_NTZ
)
COMMENT = 'v2 Mirror layer: raw source data as VARIANT (Parquet — named columns). _ columns kept for downstream; EDP technical columns alongside.';

----------------------------------------------------------------------
-- 3. INTERNAL STAGE
----------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS EDP_DB.MIRROR.RAW_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.PARQUET_FMT')
    DIRECTORY   = (ENABLE = TRUE)
    COMMENT     = 'Landing zone for EDP source files (Parquet)';

----------------------------------------------------------------------
-- 4. STORED PROCEDURE — RUN_COPY_INTO_V2 (Parquet)
----------------------------------------------------------------------
USE SCHEMA EDP_DB.ORCHESTRATION;

CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_COPY_INTO_V2(
    TABLE_NAME VARCHAR,
    FILES      ARRAY,                 -- the batch, stage-relative paths (required; empty = nothing to load)
    CONTEXT    OBJECT DEFAULT NULL,   -- run context: BATCH_ID, DATA_DOMAIN, PROVIDER_NM, CORRECTION_FL, PROVIDER_FILE_DT, ...
    METADATA   OBJECT DEFAULT NULL    -- metadata values: JOB_VERSION_NM, PROVIDER_FILE_ID, BUSINESS_KEY
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
COMMENT = 'v2 COPY INTO: loads Parquet into VARIANT with the EDP technical columns. Column names from file metadata. No positional mapping.'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import uuid
from datetime import datetime

STAGE     = "@EDP_DB.MIRROR.RAW_STAGE"
TARGET    = "EDP_DB.MIRROR.TRANSACTIONS_V2"
MAX_FILES = 1000    # COPY ... FILES = (...) accepts at most 1,000 files


def lit(v) -> str:
    """SQL literal for a value rendered into the statement."""
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def copy_sql(files, ctx, md, batch_id) -> str:
    """One COPY: RAW_DATA is $1, plus the _ columns and the EDP technical columns. Hash definitions
    are placeholders until BlackRock's are agreed."""
    key = md.get("BUSINESS_KEY")
    key_expr = f"$1:{key}::STRING" if key else "NULL"
    cols = [
        ("RAW_DATA", "$1"),
        ("_LOADED_AT", "CURRENT_TIMESTAMP()"),
        ("_FILENAME", "METADATA$FILENAME"),
        ("_FILE_ROW_NUM", "METADATA$FILE_ROW_NUMBER"),
        ("_BATCH_ID", lit(batch_id)),
        ("_SCHEMA_FP", "NULL"),
        ("DATA_DOMAIN", lit(ctx.get("DATA_DOMAIN"))),
        ("CORRECTION_FL", lit(ctx.get("CORRECTION_FL"))),
        ("FILE_RECORD_NBR", "METADATA$FILE_ROW_NUMBER"),
        ("JOB_VERSION_NM", lit(md.get("JOB_VERSION_NM"))),
        ("PROVIDER_FILE_ID", f"{lit(md.get('PROVIDER_FILE_ID'))}::NUMBER"),
        ("PROVIDER_FILE_DT", f"{lit(ctx.get('PROVIDER_FILE_DT'))}::TIMESTAMP_NTZ"),
        ("UNIQUE_HASH_ID", f"MD5(CONCAT_WS('|', COALESCE({key_expr}, ''), METADATA$FILENAME))"),
        ("UPDATED_BY_USER_ID", "CURRENT_USER()"),
        ("PROVIDER_NM", lit(ctx.get("PROVIDER_NM"))),
        ("CREATED_DTS", "CURRENT_TIMESTAMP()"),
        ("PROVIDER_FILE_NM", "METADATA$FILENAME"),
        ("UPDATED_DTS", "CURRENT_TIMESTAMP()"),
        ("CREATED_BY_USER_ID", "CURRENT_USER()"),
        ("FILE_CORRECTION_DTS", f"{lit(ctx.get('FILE_CORRECTION_DTS'))}::TIMESTAMP_NTZ"),
        ("ROW_HASH_ID", "MD5($1::VARCHAR)"),
        ("ACQ_REPROCESSING_DTM", f"{lit(ctx.get('ACQ_REPROCESSING_DTM'))}::TIMESTAMP_NTZ"),
    ]
    return f"""
        COPY INTO {TARGET} ({", ".join(n for n, _ in cols)})
        FROM (
            SELECT {", ".join(e for _, e in cols)}
            FROM {STAGE}
        )
        FILES       = ({", ".join(lit(f) for f in files)})
        FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.PARQUET_FMT')
        ON_ERROR    = ABORT_STATEMENT
        PURGE       = FALSE
        FORCE       = FALSE"""


def run(session: Session, table_name: str, files: list, context: dict = None, metadata: dict = None) -> str:
    ctx        = context or {}
    md         = metadata or {}
    batch_id   = ctx.get("BATCH_ID") or str(uuid.uuid4())
    start_time = datetime.utcnow()

    # ================================================================
    # PARQUET VARIANT LOADING
    #
    # Parquet files are self-describing: $1 is a VARIANT object with
    # the original column names from the file. No OBJECT_CONSTRUCT
    # or positional mapping needed.
    #
    # The batch's COPYs (one per 1,000 files) run in one transaction:
    # the batch loads whole or not at all (ON_ERROR = ABORT_STATEMENT).
    # ================================================================
    if files is None:
        raise ValueError("FILES is required: pass the batch's file list")
    if not files:
        return f"Nothing to load: the file list is empty. batch_id={batch_id}"

    statements = [copy_sql(files[i:i + MAX_FILES], ctx, md, batch_id) for i in range(0, len(files), MAX_FILES)]
    session.sql("BEGIN").collect()
    try:
        result = [r for s in statements for r in session.sql(s).collect()]
        session.sql("COMMIT").collect()
    except Exception:
        session.sql("ROLLBACK").collect()
        raise

    rows = [{k.lower(): v for k, v in r.as_dict().items()} for r in result]
    files_loaded = sum(1 for r in rows if r.get("status") == "LOADED")
    total_rows   = sum(int(r.get("rows_loaded") or 0) for r in rows if r.get("status") == "LOADED")
    elapsed = (datetime.utcnow() - start_time).total_seconds()
    return (
        f"COPY INTO complete (v2 VARIANT, Parquet). batch_id={batch_id}, "
        f"copies={len(statements)}, files={files_loaded}, rows_loaded={total_rows}, elapsed={elapsed:.1f}s"
    )
$$;

SELECT 'v2 Step 02p complete: VARIANT mirror layer created (Parquet — named columns).' AS status;
