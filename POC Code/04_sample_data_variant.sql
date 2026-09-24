/*
=============================================================================
  BlackRock EDP v2 — Step 04: Generate Sample Test Data (VARIANT)
=============================================================================
  Run after: 03_audit_tables.sql
  Role: EDP_ADMIN_ROLE

  Generates ~30M rows of synthetic transaction data directly into the
  VARIANT mirror table using OBJECT_CONSTRUCT.

  NOTE: This script inserts data directly (bypassing the file stage) for
  initial development. The production ingestion path uses Parquet files
  loaded via COPY INTO — see 02_mirror_layer_parquet.sql and
  e2e_test_parquet.sql for the full file-based flow.
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE WAREHOUSE EDP_TDQ_WH;

----------------------------------------------------------------------
-- 1. POPULATE REFERENCE TABLES (if not already populated by v1)
----------------------------------------------------------------------
USE SCHEMA EDP_DB.REFERENCE;

INSERT INTO EDP_DB.REFERENCE.ACCOUNTS (ACCOUNT_ID, ACCOUNT_NAME, ACCOUNT_TYPE)
SELECT
    'ACCT-BR-' || LPAD(SEQ4()::VARCHAR, 3, '0'),
    'Portfolio ' || SEQ4(),
    CASE MOD(SEQ4(), 4)
        WHEN 0 THEN 'EQUITY'
        WHEN 1 THEN 'FIXED_INCOME'
        WHEN 2 THEN 'MULTI_ASSET'
        WHEN 3 THEN 'ALTERNATIVES'
    END
FROM TABLE(GENERATOR(ROWCOUNT => 500))
WHERE NOT EXISTS (SELECT 1 FROM EDP_DB.REFERENCE.ACCOUNTS LIMIT 1);

INSERT INTO EDP_DB.REFERENCE.INSTRUMENTS (INSTRUMENT_ID, INSTRUMENT_NAME, INSTRUMENT_TYPE, CURRENCY)
SELECT
    'ISIN-' ||
    CASE MOD(SEQ4(), 5) WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' WHEN 3 THEN 'JP' WHEN 4 THEN 'CH' END ||
    LPAD(SEQ4()::VARCHAR, 10, '0'),
    'Instrument ' || SEQ4(),
    CASE MOD(SEQ4(), 5) WHEN 0 THEN 'EQUITY' WHEN 1 THEN 'BOND' WHEN 2 THEN 'ETF' WHEN 3 THEN 'FUTURE' WHEN 4 THEN 'OPTION' END,
    CASE MOD(SEQ4(), 5) WHEN 0 THEN 'USD' WHEN 1 THEN 'GBP' WHEN 2 THEN 'EUR' WHEN 3 THEN 'JPY' WHEN 4 THEN 'CHF' END
FROM TABLE(GENERATOR(ROWCOUNT => 2000))
WHERE NOT EXISTS (SELECT 1 FROM EDP_DB.REFERENCE.INSTRUMENTS LIMIT 1);

----------------------------------------------------------------------
-- 2. Generate VARIANT data directly into Mirror table
----------------------------------------------------------------------
USE SCHEMA EDP_DB.MIRROR;

INSERT INTO EDP_DB.MIRROR.TRANSACTIONS_V2 (
    RAW_DATA, _LOADED_AT, _FILENAME, _FILE_ROW_NUM, _BATCH_ID
)
SELECT
    OBJECT_CONSTRUCT(
        'TXN_ID',          'TXN-' || LPAD(SEQ8()::VARCHAR, 10, '0'),
        'ACCOUNT_ID',      'ACCT-BR-' || LPAD(MOD(ABS(RANDOM()), 500)::VARCHAR, 3, '0'),
        'INSTRUMENT_ID',   'ISIN-' ||
            CASE MOD(ABS(RANDOM()), 5) WHEN 0 THEN 'US' WHEN 1 THEN 'GB' WHEN 2 THEN 'DE' WHEN 3 THEN 'JP' WHEN 4 THEN 'CH' END ||
            LPAD(MOD(ABS(RANDOM()), 2000)::VARCHAR, 10, '0'),
        'AMOUNT',          ROUND(UNIFORM(1000::FLOAT, 50000000::FLOAT, RANDOM()), 4)::VARCHAR,
        'CURRENCY',        CASE MOD(ABS(RANDOM()), 5) WHEN 0 THEN 'USD' WHEN 1 THEN 'EUR' WHEN 2 THEN 'GBP' WHEN 3 THEN 'JPY' WHEN 4 THEN 'CHF' END,
        'TXN_DATE',        TO_CHAR(DATEADD('day', -MOD(ABS(RANDOM()), 90), CURRENT_DATE()), 'YYYY-MM-DD'),
        'SETTLEMENT_DATE', TO_CHAR(DATEADD('day', -MOD(ABS(RANDOM()), 90) + MOD(ABS(RANDOM()), 3) + 1, CURRENT_DATE()), 'YYYY-MM-DD'),
        'STATUS',          CASE MOD(ABS(RANDOM()), 10) WHEN 0 THEN 'PENDING' WHEN 1 THEN 'PENDING' WHEN 2 THEN 'CANCELLED' WHEN 3 THEN 'FAILED' ELSE 'SETTLED' END
    ),
    CURRENT_TIMESTAMP(),
    'synthetic_v2_batch_001.csv',
    SEQ8(),
    'POC-BATCH-001'
FROM TABLE(GENERATOR(ROWCOUNT => 30000000));

----------------------------------------------------------------------
-- 3. VERIFY DATA
----------------------------------------------------------------------
SELECT
    COUNT(*)                                            AS total_rows,
    COUNT(DISTINCT RAW_DATA:TXN_ID::VARCHAR)            AS distinct_txns,
    COUNT(DISTINCT RAW_DATA:ACCOUNT_ID::VARCHAR)        AS distinct_accounts,
    MIN(_LOADED_AT)                                     AS earliest_load,
    MAX(_LOADED_AT)                                     AS latest_load
FROM EDP_DB.MIRROR.TRANSACTIONS_V2;

SELECT 'v2 Step 04 complete: VARIANT sample data generated.' AS status;
