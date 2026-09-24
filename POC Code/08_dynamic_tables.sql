/*
=============================================================================
  BlackRock EDP v2 — Step 08: Presentation Layer (Dynamic Table)
=============================================================================
  *** IMPORTANT: Run AFTER dbt has populated STG_TRANSACTIONS ***
  Role: EDP_ADMIN_ROLE

  Same DT definition as v1 — reads from STAGING.STG_TRANSACTIONS which is
  now populated by the v2 dbt model (extracting from VARIANT mirror).
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA PRESENTATION;
USE WAREHOUSE EDP_ANALYTICS_WH;

CREATE OR REPLACE DYNAMIC TABLE EDP_DB.PRESENTATION.TRANSACTIONS_MART
    TARGET_LAG   = 'DOWNSTREAM'
    WAREHOUSE    = EDP_ANALYTICS_WH
    REFRESH_MODE = AUTO
    COMMENT      = 'v2 Analytics mart: enriched transactions. DOWNSTREAM lag — refreshes when queried by SDM_BDQ_TASK_V2.'
AS
SELECT
    TXN_ID,
    ACCOUNT_ID,
    INSTRUMENT_ID,
    AMOUNT,
    CURRENCY,
    TXN_DATE,
    SETTLEMENT_DATE,
    STATUS,
    DATE_TRUNC('month', TXN_DATE)           AS TXN_MONTH,
    DATE_TRUNC('year',  TXN_DATE)           AS TXN_YEAR,
    DAYOFWEEK(TXN_DATE)                     AS TXN_DAY_OF_WEEK,
    SUM(AMOUNT) OVER (
        PARTITION BY ACCOUNT_ID
        ORDER BY TXN_DATE, TXN_ID
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                       AS RUNNING_POSITION_BY_ACCOUNT,
    _LOADED_AT,
    _BATCH_ID
FROM EDP_DB.STAGING.STG_TRANSACTIONS;

SELECT 'v2 Step 08 complete: Dynamic Table created.' AS status;
