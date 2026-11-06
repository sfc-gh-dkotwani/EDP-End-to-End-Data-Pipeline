# Test Plan — `header-keyed-csv` Branch (Ingestion)

Step-by-step test of the branch's ingestion changes (explicit file-list batches, EDP technical columns, atomic loads, header-keyed CSV) plus the follow-up fixes, run **directly on `EDP_DB` without changing any code**.

The test is non-destructive: the 322.5M-row mirror and the 5,000 stress-test files stay in place. Test files go in their own stage folder (`e2e_test/`), test rows use their own TXN_IDs (`TXN-E2E-…`), and everything is removed at the end.

## What is being tested

| Change | Where | Test |
|--------|-------|------|
| Migration of the existing mirror (17 new columns, stage directory) | `02a_migrate_mirror.sql` | Step 3 |
| New `RUN_COPY_INTO_V2(TABLE_NAME, FILES, CONTEXT, METADATA)` | `02_mirror_layer_parquet.sql` | T1–T3 |
| EDP technical columns filled in the COPY | 02 scripts | T2 |
| Batch discovery from the stage directory table | `09_tasks.sql` (ingest task body) | Pre-flight, T3 |
| Atomic batch (`ON_ERROR = ABORT_STATEMENT`, one transaction) | 02 scripts | T4, T5 |
| ML TDQ works on the new table shape | `05_ml_tdq_optimized.sql` (unchanged) | T6 |
| CSV: script keeps the mirror, header keying, empty-header fix | `02_mirror_layer_variant.sql` | C0–C2 (optional) |

Out of scope here: dbt, STG BDQ, the Dynamic Table, SDM BDQ, and the finalizer. They are tested end-to-end in a later step.

---

## Step 1 — Pause the pipeline and back up the mirror

```sql
USE ROLE EDP_ADMIN_ROLE;
USE WAREHOUSE EDP_INGEST_WH;

-- Stop the 4-hourly schedule so it does not pick up the test files
ALTER TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2 SUSPEND;

-- Zero-copy safety net for the mirror
CREATE TABLE EDP_DB.MIRROR.TRANSACTIONS_V2_BACKUP CLONE EDP_DB.MIRROR.TRANSACTIONS_V2;
```

Record the starting point:

```sql
SELECT COUNT(*) AS mirror_rows FROM EDP_DB.MIRROR.TRANSACTIONS_V2;   -- 322,500,000
SELECT CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS test_start;            -- save this value
```

## Step 2 — Deploy the branch

From the repo on branch `test-header-keyed-csv`, open each file in Snowsight and **Run All**, in order:

| Step | Script | Expected result |
|------|--------|-----------------|
| 2.1 | `POC Code/02_mirror_layer_parquet.sql` | Ends with `Function RUN_COPY_INTO_V2 successfully created` |
| 2.2 | `POC Code/02a_migrate_mirror.sql` | `column_count = 22`; stage shows `directory_enabled = Y` |
| 2.3 | `POC Code/02a_migrate_mirror.sql` **again** | No errors, still `22` (proves it is re-runnable) |

> Do **not** run `09_tasks.sql` in this test. It recreates and resumes the whole DAG.

Check both procedure versions exist:

```sql
SHOW PROCEDURES LIKE 'RUN_COPY_INTO_V2' IN SCHEMA EDP_DB.ORCHESTRATION;
```

**Pass:** two rows — `RUN_COPY_INTO_V2(VARCHAR)` (old, still used by the live task) and `RUN_COPY_INTO_V2(VARCHAR, ARRAY, OBJECT, OBJECT)` (new).

## Step 3 — Create the Parquet test files

```sql
USE ROLE EDP_ADMIN_ROLE;
USE WAREHOUSE EDP_INGEST_WH;

COPY INTO @EDP_DB.MIRROR.RAW_STAGE/e2e_test/good_
FROM (
    SELECT REPLACE(TXN_ID, 'TXN-', 'TXN-E2E-') AS TXN_ID,
           ACCOUNT_ID, INSTRUMENT_ID,
           AMOUNT::VARCHAR          AS AMOUNT,
           CURRENCY,
           TXN_DATE::VARCHAR        AS TXN_DATE,
           SETTLEMENT_DATE::VARCHAR AS SETTLEMENT_DATE,
           STATUS
    FROM EDP_DB.STAGING.STG_TRANSACTIONS
    ORDER BY TXN_ID
    LIMIT 3000
)
FILE_FORMAT   = (TYPE = PARQUET)
HEADER        = TRUE
MAX_FILE_SIZE = 50000;

LIST @EDP_DB.MIRROR.RAW_STAGE/e2e_test/;   -- note the number of files
```

---

## Step 4 — Ingestion tests

### Pre-flight check (run before every ingest)

Shows which files the ingest step will pick up. It must list **only** `e2e_test/` files — never `stress_test/` files.

```sql
ALTER STAGE EDP_DB.MIRROR.RAW_STAGE REFRESH;

SELECT d.RELATIVE_PATH
FROM DIRECTORY(@EDP_DB.MIRROR.RAW_STAGE) d
WHERE NOT EXISTS (
    SELECT 1 FROM EDP_DB.MIRROR.TRANSACTIONS_V2 m
    WHERE m._FILENAME = d.RELATIVE_PATH
);
```

> **Stop** if any `stress_test/` file appears — the ingest would reload stress-test data.

### The ingest block

The body of `INGEST_TASK_V2` from `09_tasks.sql`, run directly. Save it in a worksheet; T1–T5 re-run it.

```sql
EXECUTE IMMEDIATE $$
DECLARE
    batch ARRAY;
BEGIN
    ALTER STAGE EDP_DB.MIRROR.RAW_STAGE REFRESH;
    SELECT COALESCE(ARRAY_AGG(d.RELATIVE_PATH), ARRAY_CONSTRUCT()) INTO :batch
    FROM DIRECTORY(@EDP_DB.MIRROR.RAW_STAGE) d
    WHERE NOT EXISTS (
        SELECT 1 FROM EDP_DB.MIRROR.TRANSACTIONS_V2 m
        WHERE m._FILENAME = d.RELATIVE_PATH
    );
    CALL EDP_DB.ORCHESTRATION.RUN_COPY_INTO_V2('TRANSACTIONS_V2', :batch);
    RETURN 'batch size: ' || ARRAY_SIZE(:batch);
END;
$$;
```

### Test-row query (used by several tests)

```sql
SELECT _BATCH_ID, COUNT(*) AS rows, COUNT(DISTINCT _FILENAME) AS files, MIN(_LOADED_AT) AS loaded_at
FROM EDP_DB.MIRROR.TRANSACTIONS_V2
WHERE _FILENAME LIKE 'e2e_test/%'
GROUP BY 1
ORDER BY loaded_at;
```

### T1 — Normal batch

1. Run the **pre-flight check** → only the `e2e_test/good_*` files.
2. Run the **ingest block** → `batch size: <file count from Step 3>`.
3. Run the **test-row query**.

**Pass:** one batch, 3,000 rows, file count matches Step 3. Total mirror rows = 322,503,000.

### T2 — Columns filled correctly

```sql
SELECT RAW_DATA, _BATCH_ID, _FILENAME, PROVIDER_FILE_NM,
       _FILE_ROW_NUM, FILE_RECORD_NBR,
       UNIQUE_HASH_ID, ROW_HASH_ID,
       CREATED_BY_USER_ID, UPDATED_BY_USER_ID, CREATED_DTS, UPDATED_DTS,
       _SCHEMA_FP, DATA_DOMAIN, PROVIDER_NM
FROM EDP_DB.MIRROR.TRANSACTIONS_V2
WHERE _FILENAME LIKE 'e2e_test/%'
LIMIT 5;

SELECT COUNT(*)                        AS rows,
       COUNT(DISTINCT ROW_HASH_ID)     AS distinct_row_hash,
       COUNT(DISTINCT UNIQUE_HASH_ID)  AS distinct_unique_hash,
       COUNT(DISTINCT _FILENAME)       AS files
FROM EDP_DB.MIRROR.TRANSACTIONS_V2
WHERE _FILENAME LIKE 'e2e_test/%';
```

**Pass:**
- `RAW_DATA` has named keys: `TXN_ID` (starting `TXN-E2E-`), `ACCOUNT_ID`, `AMOUNT`, …
- `_FILENAME = PROVIDER_FILE_NM` and `_FILE_ROW_NUM = FILE_RECORD_NBR`
- `CREATED_BY_USER_ID` / `UPDATED_BY_USER_ID` = your user; `CREATED_DTS` / `UPDATED_DTS` populated
- `distinct_row_hash = rows` (every row hashes differently)
- `distinct_unique_hash = files` — **expected with the current placeholder**: no `BUSINESS_KEY` is passed, so the hash is built from the file name only. Note this for the PR.
- `_SCHEMA_FP`, `DATA_DOMAIN`, `PROVIDER_NM` are NULL (Parquet has no header fingerprint; the task passes no `CONTEXT`)

### T3 — Re-run loads nothing

1. Pre-flight check → **no rows**.
2. Ingest block → `batch size: 0`.

**Pass:** test-row query unchanged (still one batch, 3,000 rows).

### T4 — One bad file fails the whole batch

Create two more good files (different rows, so TXN_IDs don't overlap with T1) and one corrupt file:

```sql
COPY INTO @EDP_DB.MIRROR.RAW_STAGE/e2e_test/good2_
FROM (
    SELECT REPLACE(TXN_ID, 'TXN-', 'TXN-E2E-') AS TXN_ID,
           ACCOUNT_ID, INSTRUMENT_ID,
           AMOUNT::VARCHAR          AS AMOUNT,
           CURRENCY,
           TXN_DATE::VARCHAR        AS TXN_DATE,
           SETTLEMENT_DATE::VARCHAR AS SETTLEMENT_DATE,
           STATUS
    FROM EDP_DB.STAGING.STG_TRANSACTIONS
    ORDER BY TXN_ID
    LIMIT 2000 OFFSET 5000
)
FILE_FORMAT   = (TYPE = PARQUET)
HEADER        = TRUE
MAX_FILE_SIZE = 50000;

-- A CSV file named .parquet, so it cannot be read as Parquet
COPY INTO @EDP_DB.MIRROR.RAW_STAGE/e2e_test/corrupt.parquet
FROM (SELECT 'this is not parquet')
FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE)
SINGLE      = TRUE;
```

1. Pre-flight check → the `good2_*` files **and** `corrupt.parquet`, nothing else.
2. Ingest block → **errors** (Parquet read error on `corrupt.parquet`).
3. Test-row query.

**Pass:** still only the T1 batch with 3,000 rows — none of the `good2_` rows loaded.

### T5 — Retry after removing the bad file

```sql
REMOVE @EDP_DB.MIRROR.RAW_STAGE/e2e_test/corrupt.parquet;
```

1. Pre-flight check → only the `good2_*` files.
2. Ingest block → `batch size: <number of good2 files>`.
3. Test-row query.

**Pass:** two batches — 3,000 rows and 2,000 rows.

> **Record this for the PR.** If the `good2_` files are skipped as "already loaded" here, the `FORCE = FALSE` load history survived the rollback in T4, which means a failed batch cannot simply be retried.

### T6 — ML TDQ passes on the new table shape

```sql
CALL EDP_DB.ORCHESTRATION.RUN_ML_TDQ_V2(
    'TRANSACTIONS_V2',
    (SELECT _BATCH_ID FROM EDP_DB.MIRROR.TRANSACTIONS_V2
     ORDER BY _LOADED_AT DESC LIMIT 1)
);
```

**Pass:** returns `"success": true` and `"batch_rows_validated": 2000`. This also confirms the T5 batch has no TXN_IDs duplicated from any other batch.

---

## Step 5 (optional) — CSV tests

Only if BlackRock will send CSV. Running the CSV script replaces the new `RUN_COPY_INTO_V2` with the CSV version (restored in Step 6).

### C0 — CSV script keeps the mirror

```sql
SELECT COUNT(*) FROM EDP_DB.MIRROR.TRANSACTIONS_V2;   -- note this number
```

Run `POC Code/02_mirror_layer_variant.sql` (Run All).

```sql
SELECT COUNT(*) FROM EDP_DB.MIRROR.TRANSACTIONS_V2;   -- same number
```

**Pass:** row count unchanged.

### C1 — Normal and reordered CSV columns

```sql
COPY INTO @EDP_DB.MIRROR.RAW_STAGE/e2e_test/csv/normal.csv
FROM (SELECT REPLACE(TXN_ID, 'TXN-', 'TXN-E2E-') AS TXN_ID,
             ACCOUNT_ID, INSTRUMENT_ID, AMOUNT, CURRENCY, TXN_DATE, SETTLEMENT_DATE, STATUS
      FROM EDP_DB.STAGING.STG_TRANSACTIONS ORDER BY TXN_ID LIMIT 500 OFFSET 20000)
FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE)
HEADER = TRUE  SINGLE = TRUE;

COPY INTO @EDP_DB.MIRROR.RAW_STAGE/e2e_test/csv/reordered.csv
FROM (SELECT STATUS, AMOUNT, REPLACE(TXN_ID, 'TXN-', 'TXN-E2E-') AS TXN_ID,
             CURRENCY, ACCOUNT_ID, SETTLEMENT_DATE, TXN_DATE, INSTRUMENT_ID
      FROM EDP_DB.STAGING.STG_TRANSACTIONS ORDER BY TXN_ID LIMIT 500 OFFSET 30000)
FILE_FORMAT = (TYPE = CSV COMPRESSION = NONE)
HEADER = TRUE  SINGLE = TRUE;
```

1. Pre-flight check → only the two `e2e_test/csv/` files.
2. Ingest block → `batch size: 2`.

```sql
SELECT _FILENAME, _SCHEMA_FP,
       RAW_DATA:TXN_ID::VARCHAR AS txn_id,
       RAW_DATA:AMOUNT::VARCHAR AS amount,
       RAW_DATA:STATUS::VARCHAR AS status
FROM EDP_DB.MIRROR.TRANSACTIONS_V2
WHERE _FILENAME LIKE 'e2e_test/csv/%'
LIMIT 10;
```

**Pass:** both files loaded (1,000 rows); in `reordered.csv`, `amount` holds numbers and `status` holds statuses (not swapped); the two files have **different** `_SCHEMA_FP` values.

### C2 — Empty header name is rejected

Create a local file `empty_header.csv`:

```
TXN_ID,,AMOUNT
TXN-E2E-TEST-1,X,100.00
```

Upload it to folder `e2e_test/csv/` — Snowsight: **Data » Databases » EDP_DB » MIRROR » Stages » RAW_STAGE » + Files**, or with SnowSQL:

```sql
PUT file:///path/to/empty_header.csv @EDP_DB.MIRROR.RAW_STAGE/e2e_test/csv/ AUTO_COMPRESS = FALSE;
```

1. Pre-flight check → only `e2e_test/csv/empty_header.csv`.
2. Ingest block → **errors** with *"header has empty or duplicate column names"*.

**Pass:** the error above, and no rows from `empty_header.csv` in the mirror.

---

## Step 6 — Record results and clean up

### Results (copy into the PR description)

| Test | Result | Notes |
|------|--------|-------|
| Step 2 — migration (22 cols, directory Y, re-runnable) | | |
| T1 — normal batch | | |
| T2 — columns filled | | UNIQUE_HASH_ID is per file with no BUSINESS_KEY |
| T3 — re-run loads nothing | | |
| T4 — bad file fails whole batch | | |
| T5 — retry after rollback | | |
| T6 — ML TDQ passes | | |
| C0 — CSV script keeps mirror (optional) | | |
| C1 — reordered CSV keyed correctly (optional) | | |
| C2 — empty header rejected (optional) | | |

### Clean up

```sql
USE ROLE EDP_ADMIN_ROLE;
USE WAREHOUSE EDP_INGEST_WH;

-- 1. Test rows and test files
DELETE FROM EDP_DB.MIRROR.TRANSACTIONS_V2 WHERE _FILENAME LIKE 'e2e_test/%';
REMOVE @EDP_DB.MIRROR.RAW_STAGE/e2e_test/;

-- 2. Audit rows written by T6
DELETE FROM EDP_DB.AUDIT.TDQ_RESULTS            WHERE RUN_AT >= '<test_start from Step 1>';
DELETE FROM EDP_DB.AUDIT.PERFORMANCE_BENCHMARKS WHERE RUN_AT >= '<test_start from Step 1>';

-- 3. Verify the mirror is back to its starting row count
SELECT COUNT(*) FROM EDP_DB.MIRROR.TRANSACTIONS_V2;   -- 322,500,000
```

If you ran the CSV tests, re-run `POC Code/02_mirror_layer_parquet.sql` to put the Parquet version of the new procedure back.

Then:

```sql
-- 4. Drop the backup once the count matches
DROP TABLE EDP_DB.MIRROR.TRANSACTIONS_V2_BACKUP;

-- 5. Resume the pipeline ONLY if it was running before the test
--    (check last_suspended_reason in SHOW TASKS; SUSPENDED_DUE_TO_ERRORS means it
--    was already failing before the test — investigate before resuming)
-- ALTER TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2 RESUME;
```

### What stays in place after the test

These are part of the branch and are kept for the end-to-end test and merge:

- The 17 new columns on `TRANSACTIONS_V2` (NULL for existing rows)
- The stage directory table on `RAW_STAGE`
- The new 4-argument `RUN_COPY_INTO_V2` (the live task still calls the 1-argument one)

---

## Open items for the PR

- **`UNIQUE_HASH_ID` with no `BUSINESS_KEY`:** identical for every row in a file. Agree the definition (and the business key) with BlackRock.
- **`ROW_HASH_ID`:** placeholder definition, to be agreed with BlackRock.
- **Batch discovery cost:** the ingest task anti-joins every stage file against the full mirror on each run. A small loaded-files log table would be cheaper at 322.5M+ rows.
- **Old procedure signature:** decide whether to drop `RUN_COPY_INTO_V2(VARCHAR)` after rollout.
- **T5 result:** whether a rolled-back batch can be retried as-is.
