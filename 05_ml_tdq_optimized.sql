/*
=============================================================================
  BlackRock EDP v2 — Step 05: ML TDQ (Snowpark Lazy DataFrame + GX)
=============================================================================
  Run after: 04_sample_data_variant.sql
  Role: EDP_ADMIN_ROLE

  KEY OPTIMIZATION — TRUE SNOWPARK LAZY EXECUTION:
    v1: Pull entire batch into Pandas (scan 1) → GX validates raw rows
        → SQL aggregates (scan 2) → cross-batch join (scan 3-4)
        = 3-4 table scans

    v2: Snowpark DataFrame API — .table().filter().agg() — builds the
        query plan lazily. Nothing executes until .collect().
        All COUNT_IF checks are combined into ONE agg(). Snowpark
        generates a single optimized SQL statement.
        + cross-batch join via Snowpark DataFrame join (scan 2)
        = 2 table scans

  WHY SNOWPARK LAZY vs RAW SQL:
    - Composable: add/remove checks programmatically, no string concat
    - Type-safe: column errors caught at plan-building, not execution
    - Snowpark optimizer can merge chained operations
    - Same performance as hand-written SQL (generates identical query)
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA ORCHESTRATION;
USE WAREHOUSE EDP_TDQ_WH;

----------------------------------------------------------------------
-- ML TDQ v2 — True Snowpark Lazy DataFrame + GX Metrics Validation
----------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_ML_TDQ_V2(
    TABLE_NAME  VARCHAR,
    BATCH_ID    VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'great-expectations', 'snowflake-sqlalchemy')
HANDLER = 'run_ml_tdq_v2'
COMMENT = 'v2 ML TDQ: Snowpark lazy DataFrame agg (1 scan) + GX validates 1-row metrics. VARIANT mirror.'
EXECUTE AS CALLER
AS
$$
import great_expectations as gx
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, count, count_distinct, lit,
    max as sf_max, call_function, is_null
)
from snowflake.snowpark.types import DecimalType, DateType, StringType
from copy import copy
import json
import time
import pandas as pd
from datetime import datetime, timezone, timedelta


def run_ml_tdq_v2(session: Session, table_name: str, batch_id: str) -> dict:
    start_time = time.time()
    fqn = f"EDP_DB.MIRROR.{table_name}"

    # ================================================================
    # 1. SNOWPARK LAZY DATAFRAME — BUILD QUERY PLAN (NO EXECUTION)
    #
    #    session.table() creates a lazy DataFrame pointing to the table.
    #    .filter() adds a WHERE clause — still no execution.
    #    .agg() adds SELECT with aggregations — still no execution.
    #    Only .collect() triggers the actual Snowflake scan.
    #
    #    Snowpark generates one optimized SQL statement from the
    #    chained DataFrame operations. This is equivalent to writing
    #    a single SELECT with all COUNT_IF in one query.
    #
    # ---- WITHOUT GX ALTERNATIVE (Native SQL): ----
    #   SELECT COUNT_IF(RAW_DATA:TXN_ID IS NULL) AS null_txn_id, ...
    #   FROM EDP_DB.MIRROR.TRANSACTIONS_V2 WHERE _BATCH_ID = :id;
    #   Cost: 1 scan, no GX overhead. Lose: structured reporting.
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   ALTER TABLE MIRROR.TRANSACTIONS_V2 ADD DATA METRIC FUNCTION
    #     SNOWFLAKE.CORE.NULL_COUNT ON (RAW_DATA);
    #   SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
    #   Cost: 0 scans in pipeline (serverless). Lose: batch scoping.
    # ================================================================

    # --- Lazy step 1: point to table (no scan) ---
    raw_df = session.table(fqn)

    # --- Lazy step 2: filter to batch (no scan) ---
    batch_df = raw_df.filter(col("_BATCH_ID") == batch_id)

    # --- Lazy step 3: define VARIANT column accessors (no scan) ---
    txn_id      = col("RAW_DATA")["TXN_ID"]
    account_id  = col("RAW_DATA")["ACCOUNT_ID"]
    amount_str  = col("RAW_DATA")["AMOUNT"]
    currency    = col("RAW_DATA")["CURRENCY"].cast(StringType())
    txn_date    = col("RAW_DATA")["TXN_DATE"]
    settle_date = col("RAW_DATA")["SETTLEMENT_DATE"]
    status      = col("RAW_DATA")["STATUS"].cast(StringType())

    # --- Lazy step 4: define all aggregations (no scan) ---
    #     call_function("COUNT_IF", condition) maps to Snowflake's COUNT_IF()
    #     All these are combined into ONE SELECT statement by Snowpark
    metrics_df = batch_df.agg(
        # Completeness: null counts
        call_function("COUNT_IF", txn_id.is_null()).alias("NULL_TXN_ID"),
        call_function("COUNT_IF", account_id.is_null()).alias("NULL_ACCOUNT_ID"),
        call_function("COUNT_IF", amount_str.is_null()).alias("NULL_AMOUNT"),
        call_function("COUNT_IF", txn_date.is_null()).alias("NULL_TXN_DATE"),

        # Format: validation using LIKE / TRANSLATE / LENGTH+UPPER
        # NOTE: RLIKE ^ anchor does not work on VARIANT-extracted ::VARCHAR
        # strings (returns FALSE despite clean data). LIKE 'TXN-%' confirmed
        # working. All format checks avoid RLIKE anchors.
        call_function("COUNT_IF",
            amount_str.is_not_null()
            & (call_function("LENGTH",
                call_function("TRANSLATE", amount_str.cast(StringType()), lit('0123456789.-'), lit('')))
               > lit(0))
        ).alias("BAD_FMT_AMOUNT"),
        (call_function("COUNT_IF", txn_id.is_not_null())
         - call_function("COUNT_IF", txn_id.cast(StringType()).like('TXN-%'))
        ).alias("BAD_FMT_TXN_ID"),
        (call_function("COUNT_IF", col("RAW_DATA")["CURRENCY"].is_not_null())
         - call_function("COUNT_IF",
             (call_function("LENGTH", currency) == lit(3))
             & (call_function("UPPER", currency) == currency))
        ).alias("BAD_FMT_CURRENCY"),

        # Type cast: TRY_TO_* failures
        # NOTE: TRY_CAST uses AS syntax (TRY_CAST(x AS type)) which cannot
        # be called via Snowpark's call_function(). TRY_TO_DECIMAL and
        # TRY_TO_DATE are regular functions that work with call_function().
        call_function("COUNT_IF",
            call_function("TRY_TO_DECIMAL", amount_str.cast(StringType()), lit(18), lit(4)).is_null()
            & amount_str.is_not_null()
        ).alias("BAD_CAST_AMOUNT"),
        call_function("COUNT_IF",
            call_function("TRY_TO_DATE", txn_date.cast(StringType())).is_null()
            & txn_date.is_not_null()
        ).alias("BAD_CAST_TXN_DATE"),
        call_function("COUNT_IF",
            call_function("TRY_TO_DATE", settle_date.cast(StringType())).is_null()
            & settle_date.is_not_null()
        ).alias("BAD_CAST_SETTLE_DATE"),

        # Domain: value set validation (subtraction pattern: non_null − matching = bad)
        (call_function("COUNT_IF", col("RAW_DATA")["CURRENCY"].is_not_null())
         - call_function("COUNT_IF", currency.isin(["USD", "EUR", "GBP", "JPY", "CHF"]))
        ).alias("BAD_DOMAIN_CURRENCY"),
        (call_function("COUNT_IF", col("RAW_DATA")["STATUS"].is_not_null())
         - call_function("COUNT_IF", status.isin(["PENDING", "SETTLED", "CANCELLED", "FAILED"]))
        ).alias("BAD_DOMAIN_STATUS"),

        # Uniqueness: in-batch duplicates
        (count("*") - count_distinct(txn_id.cast(StringType()))).alias("DUP_TXN_ID_IN_BATCH"),

        # Schema: verify expected VARIANT keys exist in every row
        # Equivalent to v1's GX ExpectTableColumnsToMatchSet.
        # If a required key is missing from the VARIANT, it resolves to NULL.
        # MISSING_SCHEMA_KEYS = rows where at least one required key is absent.
        call_function("COUNT_IF",
            txn_id.is_null() | account_id.is_null()
            | col("RAW_DATA")["INSTRUMENT_ID"].is_null()
            | amount_str.is_null() | currency.is_null()
            | txn_date.is_null() | settle_date.is_null()
            | col("RAW_DATA")["STATUS"].is_null()
        ).alias("MISSING_SCHEMA_KEYS"),

        # Volume + freshness
        count("*").alias("TOTAL_ROWS"),
        sf_max(col("_LOADED_AT")).alias("LATEST_LOAD"),
    )

    # --- Execute step: .collect() triggers ONE scan ---
    agg_start = time.time()
    metrics_row = metrics_df.collect()[0]
    agg_elapsed = time.time() - agg_start

    total_rows = int(metrics_row["TOTAL_ROWS"])
    if total_rows == 0:
        raise RuntimeError(
            f"ML TDQ v2 FAILED: no rows in {fqn} for batch_id='{batch_id}'."
        )

    # ================================================================
    # 2. GX VALIDATES METRICS (1-row Pandas DF — no additional scan)
    #
    #    GX validates the computed metric values:
    #    "NULL_TXN_ID should be 0", "TOTAL_ROWS between 1 and 100M"
    #    This replaces v1's pattern of pulling 30M raw rows into Pandas.
    # ================================================================
    gx_start = time.time()

    metrics_dict = {}
    for k, v in metrics_row.as_dict().items():
        if isinstance(v, (int, float)) and v is not None:
            metrics_dict[k] = int(v)
        else:
            metrics_dict[k] = v
    metrics_pdf = pd.DataFrame([metrics_dict])

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(name="edp_ml_tdq_v2")
    asset = ds.add_dataframe_asset(name="metrics_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("metrics_batch")

    suite = ctx.suites.add(gx.ExpectationSuite(name="ml_tdq_v2_suite"))

    # Define all GX expectations against the 1-row metrics DF
    # Each maps to one of the Snowpark lazy aggregation columns above
    check_defs = [
        # (metric_column, category, description)
        ("NULL_TXN_ID",          "completeness", "TXN_ID not null"),
        ("NULL_ACCOUNT_ID",      "completeness", "ACCOUNT_ID not null"),
        ("NULL_AMOUNT",          "completeness", "AMOUNT not null"),
        ("NULL_TXN_DATE",        "completeness", "TXN_DATE not null"),
        ("BAD_FMT_AMOUNT",       "column_type",  "AMOUNT matches decimal format"),
        ("BAD_FMT_TXN_ID",       "column_type",  "TXN_ID starts with TXN-"),
        ("BAD_FMT_CURRENCY",     "column_type",  "CURRENCY is 3-letter ISO code"),
        ("BAD_CAST_AMOUNT",      "column_type",  "AMOUNT castable to DECIMAL(18,4)"),
        ("BAD_CAST_TXN_DATE",    "column_type",  "TXN_DATE castable to DATE"),
        ("BAD_CAST_SETTLE_DATE", "column_type",  "SETTLEMENT_DATE castable to DATE"),
        ("BAD_DOMAIN_CURRENCY",  "domain",       "CURRENCY in {USD,EUR,GBP,JPY,CHF}"),
        ("BAD_DOMAIN_STATUS",    "domain",       "STATUS in {PENDING,SETTLED,CANCELLED,FAILED}"),
        ("DUP_TXN_ID_IN_BATCH",  "uniqueness",  "TXN_ID unique within batch"),
        ("MISSING_SCHEMA_KEYS",   "schema",      "All required VARIANT keys present (TXN_ID,ACCOUNT_ID,INSTRUMENT_ID,AMOUNT,CURRENCY,TXN_DATE,SETTLEMENT_DATE,STATUS)"),
    ]

    for metric_col, category, desc in check_defs:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=metric_col, min_value=0, max_value=0,
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
        gx.ValidationDefinition(name="ml_tdq_v2_vd", data=batch_def, suite=suite)
    )
    result = vd.run(batch_parameters={"dataframe": metrics_pdf})
    gx_elapsed = time.time() - gx_start

    # Collect GX results into structured format
    gx_rows = []
    for r in result.results:
        exp_cfg = r.expectation_config
        kwargs = getattr(exp_cfg, 'kwargs', {}) or {}
        check_col = kwargs.get("column", "table-level") if isinstance(kwargs, dict) else "table-level"
        meta = getattr(exp_cfg, 'meta', None) or {}
        category = meta.get("category", "validation") if isinstance(meta, dict) else "validation"
        gx_rows.append({
            "source": "great_expectations_v2",
            "gx_version": gx.__version__,
            "category": category,
            "check": f"{exp_cfg.type}({check_col})",
            "passed": r.success,
            "detail": f"value={metrics_dict.get(check_col, '?')}" if r.success
                      else f"FAILED: value={metrics_dict.get(check_col, '?')}",
        })

    if not result.success:
        failed = [r for r in gx_rows if not r["passed"]]
        raise RuntimeError(
            f"ML TDQ v2 FAILED (GX metrics) for {table_name} batch '{batch_id}': "
            f"{json.dumps([r['check'] + ' — ' + r['detail'] for r in failed])}"
        )

    # ================================================================
    # 3. CROSS-BATCH UNIQUENESS — Snowpark DataFrame join (scan 2)
    #
    #    Uses Snowpark .join() instead of raw SQL string.
    #    Snowpark generates the INNER JOIN query lazily, executes on .collect().
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   DMFs cannot take runtime params like batch_id. Would need a custom
    #   DMF that compares latest batch vs all prior — harder to express.
    # ================================================================
    curr_df = session.table(fqn).filter(
        col("_BATCH_ID") == batch_id
    ).select(col("RAW_DATA")["TXN_ID"].cast(StringType()).alias("TXN_ID"))

    hist_df = session.table(fqn).filter(
        col("_BATCH_ID") != batch_id
    ).select(col("RAW_DATA")["TXN_ID"].cast(StringType()).alias("TXN_ID")).distinct()

    cross_batch_dups = curr_df.join(hist_df, curr_df["TXN_ID"] == hist_df["TXN_ID"]).count()

    if cross_batch_dups > 0:
        raise RuntimeError(
            f"ML TDQ v2 FAILED (cross-batch): {cross_batch_dups} TXN_IDs in batch "
            f"'{batch_id}' already exist in prior batches"
        )

    # ================================================================
    # 4. FRESHNESS CHECK (from aggregated metrics — no extra scan)
    # ================================================================
    latest = metrics_row["LATEST_LOAD"]
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    if latest and latest.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=None)
    if not (latest and latest >= cutoff):
        raise RuntimeError(
            f"ML TDQ v2 FAILED (freshness): latest_load={latest} outside 7-day SLA"
        )

    # ================================================================
    # 5. WRITE AUDIT + PERFORMANCE BENCHMARK
    # ================================================================
    total_elapsed = time.time() - start_time
    all_results = gx_rows + [
        {"source": "snowpark_join", "category": "uniqueness", "check": "cross_batch_dup_txn_ids", "passed": True, "detail": f"dups={cross_batch_dups}"},
        {"source": "snowpark_metrics", "category": "freshness", "check": "loaded_within_7_days", "passed": True, "detail": f"latest={latest}"},
    ]

    session.sql("""
        INSERT INTO EDP_DB.AUDIT.TDQ_RESULTS
            (BATCH_ID, TABLE_NAME, LAYER, CHECK_TYPE, PASSED,
             TOTAL_CHECKS, FAILED_COUNT, FAILED_DETAILS, ROW_COUNT,
             EXECUTION_TIME_SECS, WAREHOUSE_SIZE, SUITE_NAME)
        SELECT ?, ?, 'ML', 'TDQ', TRUE, ?, 0, PARSE_JSON('[]'), ?,
               ?, 'SMALL', ?
    """, params=[
        batch_id, table_name,
        len(all_results), total_rows,
        total_elapsed, f"{table_name}_ml_tdq_v2"
    ]).collect()

    session.sql("""
        INSERT INTO EDP_DB.AUDIT.PERFORMANCE_BENCHMARKS
            (LAYER, CHECK_CATEGORY, ROW_COUNT, WAREHOUSE_SIZE,
             EXECUTION_TIME_SECS, ROWS_PER_SECOND, NOTES)
        VALUES ('ML', 'v2_snowpark_lazy_gx', ?, 'SMALL', ?, ?, ?)
    """, params=[
        total_rows, total_elapsed,
        round(total_rows / total_elapsed, 0) if total_elapsed > 0 else 0,
        f"v2: Snowpark lazy .table().filter().agg() + GX ({total_rows:,} rows)"
    ]).collect()

    return {
        "success": True,
        "version": "v2_snowpark_lazy_gx",
        "gx_version": gx.__version__,
        "batch_id": batch_id,
        "table": table_name,
        "layer": "ML",
        "batch_rows_validated": total_rows,
        "total_scans": 2,
        "agg_scan_elapsed_secs": round(agg_elapsed, 2),
        "gx_elapsed_secs": round(gx_elapsed, 2),
        "total_elapsed_secs": round(total_elapsed, 2),
    }
$$;

SELECT 'v2 Step 05 complete: ML TDQ with true Snowpark lazy DataFrame + GX.' AS status;
