/*
=============================================================================
  BlackRock EDP v2 — Step 02: VARIANT Mirror Layer
=============================================================================
  Run after: 01_setup.sql (or v1 01_setup.sql for initial infra)
  Role: EDP_ADMIN_ROLE

  KEY CHANGE FROM v1:
    v1: INFER_SCHEMA on every load → typed VARCHAR columns → ALTER TABLE for drift
    v2: one header read per batch → one COPY per distinct header, keys taken
        from that header → single VARIANT column → no schema inference, no DDL

  WHY A SEPARATE HEADER READ:
    Snowflake's COPY INTO has a limitation: PARSE_HEADER=TRUE (which reads
    column names from CSV headers) is NOT allowed in COPY transformations
    (SELECT ... FROM @stage). So the header is read first, in one query over
    the whole batch (row 1 of every file), and the COPY is generated from it:
      OBJECT_CONSTRUCT_KEEP_NULL('TXN_ID', $1, 'AMOUNT', $2, ...)
    Files with different headers in one batch get one COPY each. All of the
    batch's COPYs run in one transaction.

  SCHEMA CHANGE CONTRACT:
    - Columns appended, inserted, removed or reordered: safe. Each file's own
      header names its keys.
    - Columns renamed: old key disappears, new key appears (as for Parquet).
      Caught by completeness checks in ML TDQ.
    - Keys are the header names exactly as written in the file (case-sensitive);
      downstream reads RAW_DATA:TXN_ID, so headers are expected in upper case.
    - A row with more or fewer fields than its header loads (extra fields
      dropped, missing ones NULL): ERROR_ON_COLUMN_COUNT_MISMATCH does not
      apply to COPY transformations. ML TDQ catches the NULLs.

  TECHNICAL COLUMNS:
    The BlackRock EDP technical columns are filled in the same COPY, from the
    caller's run context, the metadata values, and METADATA$ fields. The
    _ columns are kept for the downstream layers (05-09, dbt); three hold the
    same values as EDP columns (_FILENAME / PROVIDER_FILE_NM,
    _FILE_ROW_NUM / FILE_RECORD_NBR, _LOADED_AT / CREATED_DTS).
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

-- Row 1 as data, for the per-batch header read (not used to load).
CREATE FILE FORMAT IF NOT EXISTS EDP_DB.MIRROR.CSV_HEADER_FMT
    TYPE                          = 'CSV'
    FIELD_DELIMITER               = ','
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    SKIP_HEADER                   = 0
    TRIM_SPACE                    = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    COMMENT = 'CSV row 1 as data, for the per-batch header read';

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
COMMENT = 'v2 Mirror layer: raw source data as VARIANT (header-keyed CSV, no schema inference). _ columns kept for downstream; EDP technical columns alongside.';

----------------------------------------------------------------------
-- 3. INTERNAL STAGE (reuse existing)
----------------------------------------------------------------------
CREATE STAGE IF NOT EXISTS EDP_DB.MIRROR.RAW_STAGE
    FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.CSV_FMT')
    DIRECTORY   = (ENABLE = TRUE)
    COMMENT     = 'Landing zone for EDP source files';

----------------------------------------------------------------------
-- 4. STORED PROCEDURE — RUN_COPY_INTO_V2 (positional mapping)
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
COMMENT = 'v2 COPY INTO: loads CSV into VARIANT keyed by each file\'s header (one header read per batch, one COPY per header). No INFER_SCHEMA, no DDL.'
EXECUTE AS CALLER
AS
$$
from snowflake.snowpark import Session
import hashlib
import re
import uuid
from datetime import datetime

STAGE      = "@EDP_DB.MIRROR.RAW_STAGE"
TARGET     = "EDP_DB.MIRROR.TRANSACTIONS_V2"
MAX_COLS   = 200     # widest header the read accepts
MAX_FILES  = 1000    # COPY ... FILES = (...) accepts at most 1,000 files


def lit(v) -> str:
    """SQL literal for a value rendered into the statement."""
    return "NULL" if v is None else "'" + str(v).replace("'", "''") + "'"


def chunks(xs, n):
    return [xs[i:i + n] for i in range(0, len(xs), n)]


def read_headers(session, files) -> dict:
    """Row 1 of every file, one query per 1,000 files. LIMIT stops the scan once every file has
    returned its row 1; without it the row filter runs after every row is parsed (a 1 GB file is
    read in full)."""
    cols = ", ".join(f"${i}" for i in range(1, MAX_COLS + 1))
    headers = {}
    for part in chunks(files, MAX_FILES):
        pattern = "|".join(re.escape(f) for f in part)
        rows = session.sql(f"""
            SELECT METADATA$FILENAME, {cols}
            FROM {STAGE} (FILE_FORMAT => 'EDP_DB.MIRROR.CSV_HEADER_FMT', PATTERN => {lit(pattern)})
            WHERE METADATA$FILE_ROW_NUMBER = 1
            LIMIT {len(part)}""").collect()
        for r in rows:
            names = [v for v in list(r)[1:] if v is not None]
            if names:
                names[0] = names[0].lstrip("\ufeff")          # byte-order mark
            names = [n.strip() for n in names]
            headers[r[0]] = names
    missing = [f for f in files if f not in headers]
    if missing:
        raise ValueError(f"No header row read for {len(missing)} file(s), e.g. {missing[:3]}")
    for f, h in headers.items():
        if len(h) >= MAX_COLS:
            raise ValueError(f"{f}: header has {MAX_COLS}+ columns; raise MAX_COLS")
        if "" in h or len(set(h)) != len(h):
            raise ValueError(f"{f}: header has empty or duplicate column names: {h}")
    return headers


def copy_sql(files, header, fp, ctx, md, batch_id) -> str:
    """One COPY for files sharing a header: RAW_DATA keyed by that header, plus the _ columns
    and the EDP technical columns. Hash definitions are placeholders until BlackRock's are agreed."""
    payload = "OBJECT_CONSTRUCT_KEEP_NULL(" + ", ".join(f"{lit(c)}, ${i + 1}" for i, c in enumerate(header)) + ")"
    key = md.get("BUSINESS_KEY")
    key_expr = f"${header.index(key) + 1}" if key in header else "NULL"
    cols = [
        ("RAW_DATA", payload),
        ("_LOADED_AT", "CURRENT_TIMESTAMP()"),
        ("_FILENAME", "METADATA$FILENAME"),
        ("_FILE_ROW_NUM", "METADATA$FILE_ROW_NUMBER"),
        ("_BATCH_ID", lit(batch_id)),
        ("_SCHEMA_FP", lit(fp)),
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
        ("ROW_HASH_ID", f"MD5(({payload})::VARCHAR)"),      # TO_JSON is not allowed in a CSV COPY transform
        ("ACQ_REPROCESSING_DTM", f"{lit(ctx.get('ACQ_REPROCESSING_DTM'))}::TIMESTAMP_NTZ"),
    ]
    return f"""
        COPY INTO {TARGET} ({", ".join(n for n, _ in cols)})
        FROM (
            SELECT {", ".join(e for _, e in cols)}
            FROM {STAGE}
        )
        FILES       = ({", ".join(lit(f) for f in files)})
        FILE_FORMAT = (FORMAT_NAME = 'EDP_DB.MIRROR.CSV_FMT')
        ON_ERROR    = ABORT_STATEMENT
        PURGE       = FALSE
        FORCE       = FALSE"""


def run(session: Session, table_name: str, files: list, context: dict = None, metadata: dict = None) -> str:
    ctx        = context or {}
    md         = metadata or {}
    batch_id   = ctx.get("BATCH_ID") or str(uuid.uuid4())
    start_time = datetime.utcnow()

    # ================================================================
    # HEADER-KEYED VARIANT LOADING
    #
    #   1. Read row 1 of every file in the batch (one query).
    #   2. Group files by header; one COPY per header, keys from it.
    #   3. Run every COPY in one transaction: the batch loads whole
    #      or not at all (ON_ERROR = ABORT_STATEMENT).
    #
    # PERFORMANCE vs v1:
    #   - One header read per batch instead of INFER_SCHEMA per file
    #     (10K small files on X-Small: 1 statement, ~3.5 min, vs
    #     10K statements, ~51 min of statement time)
    #   - No CREATE / ALTER TABLE
    # ================================================================
    if files is None:
        raise ValueError("FILES is required: pass the batch's file list")
    if not files:
        return f"Nothing to load: the file list is empty. batch_id={batch_id}"

    headers = read_headers(session, files)
    groups = {}
    for f in files:
        h = headers[f]
        fp = hashlib.md5(",".join(h).encode()).hexdigest()[:12]
        groups.setdefault(fp, {"header": h, "files": []})["files"].append(f)

    statements = [copy_sql(part, g["header"], fp, ctx, md, batch_id)
                  for fp, g in groups.items() for part in chunks(g["files"], MAX_FILES)]

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
        f"COPY INTO complete (v2 VARIANT, header-keyed). batch_id={batch_id}, "
        f"headers={len(groups)}, copies={len(statements)}, files={files_loaded}, "
        f"rows_loaded={total_rows}, elapsed={elapsed:.1f}s"
    )
$$;

SELECT 'v2 Step 02 complete: VARIANT mirror layer created (header-keyed CSV, no INFER_SCHEMA).' AS status;
