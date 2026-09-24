{{
    config(
        materialized     = 'incremental',
        unique_key       = 'TXN_ID',
        on_schema_change = 'fail'
    )
}}

{#
  v2: Extracts typed columns from the VARIANT RAW_DATA column.
  This replaces the v1 model that read from typed VARCHAR columns.

  The VARIANT approach means:
    - No INFER_SCHEMA needed at ingestion
    - Schema evolution is automatic (new keys appear in VARIANT)
    - Column extraction and type casting happens here in staging
#}

WITH source AS (
    SELECT * FROM {{ source('mirror', 'TRANSACTIONS_V2') }}
    {% if is_incremental() %}
        WHERE _LOADED_AT > (
            SELECT COALESCE(MAX(_LOADED_AT), '1900-01-01'::TIMESTAMP_NTZ)
            FROM {{ this }}
        )
    {% endif %}
),

typed AS (
    SELECT
        RAW_DATA:TXN_ID::VARCHAR                            AS TXN_ID,
        RAW_DATA:ACCOUNT_ID::VARCHAR                        AS ACCOUNT_ID,
        RAW_DATA:INSTRUMENT_ID::VARCHAR                     AS INSTRUMENT_ID,
        TRY_CAST(RAW_DATA:AMOUNT::VARCHAR AS DECIMAL(18, 4))         AS AMOUNT,
        UPPER(TRIM(RAW_DATA:CURRENCY::VARCHAR))              AS CURRENCY,
        TRY_CAST(RAW_DATA:TXN_DATE::VARCHAR AS DATE)        AS TXN_DATE,
        TRY_CAST(RAW_DATA:SETTLEMENT_DATE::VARCHAR AS DATE) AS SETTLEMENT_DATE,
        UPPER(TRIM(RAW_DATA:STATUS::VARCHAR))                AS STATUS,
        _LOADED_AT,
        _FILENAME,
        _BATCH_ID
    FROM source
),

valid AS (
    SELECT *
    FROM typed
    WHERE AMOUNT    IS NOT NULL
      AND TXN_DATE  IS NOT NULL
      AND TXN_ID    IS NOT NULL
)

SELECT * FROM valid
