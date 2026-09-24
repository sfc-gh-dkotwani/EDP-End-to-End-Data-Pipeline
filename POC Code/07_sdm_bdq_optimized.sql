/*
=============================================================================
  BlackRock EDP v2 — Step 07: SDM BDQ (Snowpark Lazy DataFrame + GX)
=============================================================================
  Run after: 08_dynamic_tables.sql (DT must exist)
  Role: EDP_ADMIN_ROLE

  KEY OPTIMIZATION — TRUE SNOWPARK LAZY EXECUTION:
    v1: DT refresh + Pandas pull (scan 1) + cross-table SQL (scan 2-3) = 3-4 scans
    v2: DT refresh + Snowpark .table().filter().agg() (scan 1)
        + Snowpark cross-table agg (scan 2) = 2-3 scans
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA ORCHESTRATION;
USE WAREHOUSE EDP_TDQ_WH;

CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_SDM_BDQ_V2(
    TABLE_NAME  VARCHAR,
    BATCH_ID    VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'great-expectations', 'snowflake-sqlalchemy')
HANDLER = 'run_sdm_bdq_v2'
COMMENT = 'v2 SDM BDQ: DT refresh + Snowpark lazy DataFrame + GX metrics + cross-table.'
EXECUTE AS CALLER
AS
$$
import great_expectations as gx
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, count, count_distinct, lit,
    max as sf_max, sum as sf_sum, abs as sf_abs,
    datediff, call_function
)
from snowflake.snowpark.types import StringType
import json
import time
import pandas as pd


def run_sdm_bdq_v2(session: Session, table_name: str, batch_id: str) -> dict:
    start_time = time.time()
    fqn = f"EDP_DB.PRESENTATION.{table_name}"

    # ================================================================
    # 1. FORCE-REFRESH THE DYNAMIC TABLE
    #    DOWNSTREAM TARGET_LAG only triggers from downstream DTs.
    # ================================================================
    session.sql(f"ALTER DYNAMIC TABLE {fqn} REFRESH").collect()

    for _attempt in range(12):
        lag_check = session.sql(f"""
            SELECT DATEDIFF('minute',
                (SELECT MAX(_LOADED_AT) FROM {fqn}),
                (SELECT MAX(_LOADED_AT) FROM EDP_DB.STAGING.STG_TRANSACTIONS)
            ) AS lag_min
        """).collect()[0]
        if int(lag_check["LAG_MIN"] or 999) <= 5:
            break
        time.sleep(5)

    # ================================================================
    # 2. SNOWPARK LAZY DATAFRAME — BATCH METRICS (scan 1)
    #
    #    .table() → .filter() → .agg() builds ONE query plan.
    #    All completeness, domain, range, and derived checks combined.
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   DMFs can be attached directly to Dynamic Tables:
    #   ALTER DYNAMIC TABLE TRANSACTIONS_MART
    #     ADD DATA METRIC FUNCTION SNOWFLAKE.CORE.NULL_COUNT ON (TXN_ID);
    #   SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
    #   DMFs run automatically after each DT refresh.
    # ================================================================

    # --- Lazy: point to DT, filter, aggregate ---
    sdm_df = session.table(fqn)
    batch_df = sdm_df.filter(col("_BATCH_ID") == batch_id)

    metrics_df = batch_df.agg(
        # Completeness
        call_function("COUNT_IF", col("TXN_ID").is_null()).alias("NULL_TXN_ID"),
        call_function("COUNT_IF", col("ACCOUNT_ID").is_null()).alias("NULL_ACCOUNT_ID"),
        call_function("COUNT_IF", col("AMOUNT").is_null()).alias("NULL_AMOUNT"),
        call_function("COUNT_IF", col("TXN_DATE").is_null()).alias("NULL_TXN_DATE"),
        call_function("COUNT_IF", col("TXN_MONTH").is_null()).alias("NULL_TXN_MONTH"),
        call_function("COUNT_IF", col("TXN_YEAR").is_null()).alias("NULL_TXN_YEAR"),

        # Domain
        call_function("COUNT_IF",
            ~col("CURRENCY").isin(["USD", "EUR", "GBP", "JPY", "CHF"])
        ).alias("BAD_DOMAIN_CURRENCY"),
        call_function("COUNT_IF",
            ~col("STATUS").isin(["PENDING", "SETTLED", "CANCELLED", "FAILED"])
        ).alias("BAD_DOMAIN_STATUS"),

        # Range
        call_function("COUNT_IF",
            (col("AMOUNT") <= lit(0)) | (col("AMOUNT") > lit(100_000_000))
        ).alias("BAD_RANGE_AMOUNT"),

        # Derived column: TXN_MONTH must equal DATE_TRUNC('month', TXN_DATE)
        call_function("COUNT_IF",
            col("TXN_MONTH") != call_function("DATE_TRUNC", lit("month"), col("TXN_DATE"))
        ).alias("BAD_DERIVED_TXN_MONTH"),

        # Volume
        count("*").alias("TOTAL_ROWS"),
    )

    # --- Execute: ONE scan ---
    agg_start = time.time()
    metrics_row = metrics_df.collect()[0]
    agg_elapsed = time.time() - agg_start

    total_rows = int(metrics_row["TOTAL_ROWS"])
    if total_rows == 0:
        raise RuntimeError(
            f"SDM BDQ v2 FAILED: no rows in {fqn} for batch_id='{batch_id}'. "
            f"Run ALTER DYNAMIC TABLE {fqn} REFRESH; and retry."
        )

    # ================================================================
    # 3. GX VALIDATES METRICS (1-row DF)
    # ================================================================
    gx_start = time.time()

    metrics_dict = {k: int(v) if isinstance(v, (int, float)) and v is not None else v
                    for k, v in metrics_row.as_dict().items()}
    metrics_pdf = pd.DataFrame([metrics_dict])

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(name="edp_sdm_bdq_v2")
    asset = ds.add_dataframe_asset(name="sdm_metrics_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("sdm_metrics_batch")

    suite = ctx.suites.add(gx.ExpectationSuite(name="sdm_bdq_v2_suite"))

    zero_checks = [
        ("NULL_TXN_ID",          "completeness",   "TXN_ID not null"),
        ("NULL_ACCOUNT_ID",      "completeness",   "ACCOUNT_ID not null"),
        ("NULL_AMOUNT",          "completeness",   "AMOUNT not null"),
        ("NULL_TXN_DATE",        "completeness",   "TXN_DATE not null"),
        ("NULL_TXN_MONTH",       "completeness",   "TXN_MONTH not null"),
        ("NULL_TXN_YEAR",        "completeness",   "TXN_YEAR not null"),
        ("BAD_DOMAIN_CURRENCY",  "domain",          "CURRENCY in allowed set"),
        ("BAD_DOMAIN_STATUS",    "domain",          "STATUS in allowed set"),
        ("BAD_RANGE_AMOUNT",     "range",           "AMOUNT in valid range"),
        ("BAD_DERIVED_TXN_MONTH","derived_column",  "TXN_MONTH = DATE_TRUNC(month, TXN_DATE)"),
    ]
    for col_name, category, desc in zero_checks:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col_name, min_value=0, max_value=0,
                meta={"category": category, "original_check": desc}
            )
        )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="TOTAL_ROWS", min_value=1, max_value=None,
            meta={"category": "volume", "original_check": "row count >= 1 (no upper bound)"}
        )
    )

    vd = ctx.validation_definitions.add(
        gx.ValidationDefinition(name="sdm_bdq_v2_vd", data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": metrics_pdf})
    gx_elapsed = time.time() - gx_start

    gx_rows = []
    for r in result.results:
        exp_cfg = r.expectation_config
        kwargs = getattr(exp_cfg, 'kwargs', {}) or {}
        check_col = kwargs.get("column", "table-level") if isinstance(kwargs, dict) else "table-level"
        meta = getattr(exp_cfg, 'meta', None) or {}
        category = meta.get("category", "validation") if isinstance(meta, dict) else "validation"
        gx_rows.append({
            "source": "great_expectations_v2", "gx_version": gx.__version__,
            "category": category,
            "check": f"{exp_cfg.type}({check_col})",
            "passed": r.success,
            "detail": f"value={metrics_dict.get(check_col, '?')}",
        })

    if not result.success:
        failed = [r for r in gx_rows if not r["passed"]]
        raise RuntimeError(
            f"SDM BDQ v2 FAILED (GX metrics) for {table_name} batch '{batch_id}': "
            f"{json.dumps([r['check'] + ' — ' + r['detail'] for r in failed])}"
        )

    # ================================================================
    # 4. CROSS-TABLE CONSISTENCY — Snowpark lazy (scan 2)
    #
    #    Compare SDM totals vs STG totals using Snowpark DataFrames.
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   CREATE DATA METRIC FUNCTION edp_amount_consistency(
    #       ARG_T1 TABLE(ARG_C1 DECIMAL(18,4)),
    #       ARG_T2 TABLE(ARG_C2 DECIMAL(18,4)))
    #     RETURNS NUMBER AS
    #     'SELECT ABS(SUM(ARG_C1)-(SELECT SUM(ARG_C2) FROM ARG_T2))
    #      FROM ARG_T1';
    # ================================================================
    sdm_stats = sdm_df.agg(
        sf_sum(col("AMOUNT")).alias("SDM_AMOUNT"),
        count("*").alias("SDM_ROWS"),
        count_distinct(col("TXN_ID")).alias("SDM_DISTINCT_TXN"),
        count_distinct(col("ACCOUNT_ID")).alias("SDM_DISTINCT_ACCT"),
        count_distinct(col("CURRENCY")).alias("SDM_DISTINCT_CCY"),
        sf_max(col("_LOADED_AT")).alias("SDM_LATEST"),
    )

    stg_df = session.table("EDP_DB.STAGING.STG_TRANSACTIONS")
    stg_stats = stg_df.agg(
        sf_sum(col("AMOUNT")).alias("STG_AMOUNT"),
        count("*").alias("STG_ROWS"),
        count_distinct(col("TXN_ID")).alias("STG_DISTINCT_TXN"),
        count_distinct(col("ACCOUNT_ID")).alias("STG_DISTINCT_ACCT"),
        count_distinct(col("CURRENCY")).alias("STG_DISTINCT_CCY"),
        sf_max(col("_LOADED_AT")).alias("STG_LATEST"),
    )

    sdm_row = sdm_stats.collect()[0]
    stg_row = stg_stats.collect()[0]

    sdm_rows = int(sdm_row["SDM_ROWS"])
    stg_rows = int(stg_row["STG_ROWS"])
    amount_diff = abs(float(sdm_row["SDM_AMOUNT"] or 0) - float(stg_row["STG_AMOUNT"] or 0))
    row_diff_pct = abs(sdm_rows - stg_rows) / max(stg_rows, 1) * 100

    sdm_latest = sdm_row["SDM_LATEST"]
    stg_latest = stg_row["STG_LATEST"]
    lag = 0
    if sdm_latest and stg_latest:
        lag = int((stg_latest - sdm_latest).total_seconds() / 60)

    # Dimension completeness: accounts and currencies in SDM vs STG
    missing_accounts = int(stg_row["STG_DISTINCT_ACCT"]) - int(sdm_row["SDM_DISTINCT_ACCT"])
    missing_currencies = int(stg_row["STG_DISTINCT_CCY"]) - int(sdm_row["SDM_DISTINCT_CCY"])

    sql_failures = []
    if amount_diff > 0.01:
        sql_failures.append(f"aggregate_consistency: AMOUNT differs by {amount_diff:.4f}")
    if row_diff_pct > 1.0:
        sql_failures.append(f"aggregate_consistency: row count diff {row_diff_pct:.2f}% (SDM={sdm_rows}, STG={stg_rows})")
    if int(sdm_row["SDM_DISTINCT_TXN"]) != int(stg_row["STG_DISTINCT_TXN"]):
        sql_failures.append("aggregate_consistency: distinct TXN_ID mismatch")
    if missing_accounts > 0:
        sql_failures.append(f"dimension_completeness: {missing_accounts} account(s) in STG not in SDM")
    if missing_currencies > 0:
        sql_failures.append(f"dimension_completeness: {missing_currencies} currency(ies) in STG not in SDM")
    if lag > 30:
        sql_failures.append(f"freshness: SDM is {lag} min behind STG")

    if sql_failures:
        raise RuntimeError(
            f"SDM BDQ v2 FAILED (cross-table) for {table_name} batch '{batch_id}': "
            f"{json.dumps(sql_failures)}"
        )

    # ================================================================
    # 5. WRITE AUDIT
    # ================================================================
    total_elapsed = time.time() - start_time
    all_results = gx_rows + [
        {"source": "snowpark_agg", "category": "aggregate_consistency", "check": "amount_matches_stg", "passed": True, "detail": f"diff={amount_diff:.4f}"},
        {"source": "snowpark_agg", "category": "aggregate_consistency", "check": "row_count_within_1pct", "passed": True, "detail": f"diff={row_diff_pct:.2f}%"},
        {"source": "snowpark_agg", "category": "aggregate_consistency", "check": "distinct_txn_id_matches_stg", "passed": True, "detail": f"sdm={sdm_row['SDM_DISTINCT_TXN']}, stg={stg_row['STG_DISTINCT_TXN']}"},
        {"source": "snowpark_agg", "category": "dimension_completeness", "check": "all_accounts_in_sdm", "passed": True, "detail": f"missing={missing_accounts}"},
        {"source": "snowpark_agg", "category": "dimension_completeness", "check": "all_currencies_in_sdm", "passed": True, "detail": f"missing={missing_currencies}"},
        {"source": "snowpark_agg", "category": "freshness", "check": "sdm_within_30min_of_stg", "passed": True, "detail": f"lag={lag} min"},
    ]

    session.sql("""
        INSERT INTO EDP_DB.AUDIT.TDQ_RESULTS
            (BATCH_ID, TABLE_NAME, LAYER, CHECK_TYPE, PASSED,
             TOTAL_CHECKS, FAILED_COUNT, FAILED_DETAILS, ROW_COUNT,
             EXECUTION_TIME_SECS, WAREHOUSE_SIZE, SUITE_NAME)
        SELECT ?, ?, 'SDM', 'BDQ', TRUE, ?, 0, PARSE_JSON('[]'), ?,
               ?, 'SMALL', ?
    """, params=[
        batch_id, table_name,
        len(all_results), sdm_rows,
        total_elapsed, f"{table_name}_sdm_bdq_v2"
    ]).collect()

    return {
        "success": True, "version": "v2_snowpark_lazy_gx",
        "gx_version": gx.__version__, "batch_id": batch_id,
        "table": table_name, "layer": "SDM",
        "batch_rows_validated": total_rows,
        "sdm_stg_row_diff_pct": round(row_diff_pct, 2),
        "total_scans": 3,
        "total_elapsed_secs": round(total_elapsed, 2),
    }
$$;

SELECT 'v2 Step 07 complete: SDM BDQ with true Snowpark lazy DataFrame + GX.' AS status;
