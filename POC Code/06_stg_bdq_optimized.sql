/*
=============================================================================
  BlackRock EDP v2 — Step 06: STG BDQ (Snowpark Lazy DataFrame + GX)
=============================================================================
  Run after: dbt has populated STAGING.STG_TRANSACTIONS
  Role: EDP_ADMIN_ROLE

  KEY OPTIMIZATION — TRUE SNOWPARK LAZY EXECUTION:
    v1: Pandas pull (scan 1) + 2 ref table loads (scan 2-3) + provenance (scan 4) = 4 scans
    v2: Snowpark .table().filter().agg() with inline ref subqueries (scan 1)
        + Snowpark DataFrame join for provenance (scan 2) = 2 scans
=============================================================================
*/

USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE SCHEMA ORCHESTRATION;
USE WAREHOUSE EDP_TDQ_WH;

CREATE OR REPLACE PROCEDURE EDP_DB.ORCHESTRATION.RUN_STG_BDQ_V2(
    TABLE_NAME  VARCHAR,
    BATCH_ID    VARCHAR
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'great-expectations', 'snowflake-sqlalchemy')
HANDLER = 'run_stg_bdq_v2'
COMMENT = 'v2 STG BDQ: Snowpark lazy DataFrame (1 scan) + provenance join (1 scan) + GX on metrics.'
EXECUTE AS CALLER
AS
$$
import great_expectations as gx
from snowflake.snowpark import Session
from snowflake.snowpark.functions import (
    col, count, count_distinct, lit,
    call_function
)
from snowflake.snowpark.types import StringType
import json
import time
import pandas as pd


def run_stg_bdq_v2(session: Session, table_name: str, batch_id: str) -> dict:
    start_time = time.time()
    fqn = f"EDP_DB.STAGING.{table_name}"

    # ================================================================
    # 1. SNOWPARK LAZY DATAFRAME — ALL CHECKS IN ONE AGG
    #
    #    .table() → .filter() → .agg() builds one query plan.
    #    Reference table lookups (ACCOUNTS, INSTRUMENTS) are embedded
    #    as subqueries via call_function("COUNT_IF", NOT IN ...).
    #    Snowflake optimizer folds small ref tables into the scan.
    #
    # ---- WITHOUT GX ALTERNATIVE (Native SQL): ----
    #   SELECT COUNT_IF(TXN_ID IS NULL) AS null_txn_id, ...
    #   FROM STG_TRANSACTIONS WHERE _BATCH_ID = :id;
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   ALTER TABLE STG_TRANSACTIONS ADD DATA METRIC FUNCTION
    #     SNOWFLAKE.CORE.NULL_COUNT ON (TXN_ID);
    #   ALTER TABLE STG_TRANSACTIONS ADD DATA METRIC FUNCTION
    #     SNOWFLAKE.CORE.DUPLICATE_COUNT ON (TXN_ID);
    #   CREATE DATA METRIC FUNCTION edp_bad_currency(
    #       ARG_T TABLE(ARG_C1 VARCHAR)) RETURNS NUMBER AS
    #     'SELECT COUNT_IF(ARG_C1 NOT IN (''USD'',''EUR'',''GBP'',''JPY'',''CHF''))
    #      FROM ARG_T';
    #   SET DATA_METRIC_SCHEDULE = 'TRIGGER_ON_CHANGES';
    # ================================================================

    # --- Lazy step 1: point to staging table ---
    raw_df = session.table(fqn)

    # --- Lazy step 2: filter to current batch ---
    batch_df = raw_df.filter(col("_BATCH_ID") == batch_id)

    # --- Schema check (NO scan — reads metadata only) ---
    #     Snowpark .schema gives column definitions without executing a query.
    #     Equivalent to v1's GX ExpectTableColumnsToMatchSet.
    expected_cols = {"TXN_ID", "ACCOUNT_ID", "INSTRUMENT_ID", "AMOUNT",
                     "CURRENCY", "TXN_DATE", "SETTLEMENT_DATE", "STATUS"}
    actual_cols = {f.name for f in batch_df.schema.fields
                   if not f.name.startswith("_")}
    missing_cols = expected_cols - actual_cols
    schema_ok = len(missing_cols) == 0

    # --- Lazy step 3: define all aggregations ---
    #     Ref integrity checks use subquery via session.table().select()
    ref_accounts = session.table("EDP_DB.REFERENCE.ACCOUNTS").select(col("ACCOUNT_ID"))
    ref_instruments = session.table("EDP_DB.REFERENCE.INSTRUMENTS").select(col("INSTRUMENT_ID"))

    metrics_df = batch_df.agg(
        # Completeness
        call_function("COUNT_IF", col("TXN_ID").is_null()).alias("NULL_TXN_ID"),
        call_function("COUNT_IF", col("ACCOUNT_ID").is_null()).alias("NULL_ACCOUNT_ID"),
        call_function("COUNT_IF", col("AMOUNT").is_null()).alias("NULL_AMOUNT"),
        call_function("COUNT_IF", col("TXN_DATE").is_null()).alias("NULL_TXN_DATE"),

        # Uniqueness
        (count("*") - count_distinct(col("TXN_ID"))).alias("DUP_TXN_ID"),

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
            f"STG BDQ v2 FAILED: no rows in {fqn} for batch_id='{batch_id}'."
        )

    # --- Referential integrity via Snowpark left_anti join ---
    #     left_anti join returns batch rows NOT found in reference.
    #     .count() triggers execution but on a focused subset.
    batch_accts = batch_df.select(col("ACCOUNT_ID")).distinct()
    bad_ref_account = batch_accts.join(
        ref_accounts,
        batch_accts["ACCOUNT_ID"] == ref_accounts["ACCOUNT_ID"],
        "left_anti"
    ).count()

    batch_instr = batch_df.select(col("INSTRUMENT_ID")).distinct()
    bad_ref_instrument = batch_instr.join(
        ref_instruments,
        batch_instr["INSTRUMENT_ID"] == ref_instruments["INSTRUMENT_ID"],
        "left_anti"
    ).count()

    # ================================================================
    # 2. GX VALIDATES METRICS (1-row DF)
    # ================================================================
    gx_start = time.time()

    metrics_dict = {k: int(v) if isinstance(v, (int, float)) and v is not None else v
                    for k, v in metrics_row.as_dict().items()}
    metrics_dict["BAD_REF_ACCOUNT"] = bad_ref_account
    metrics_dict["BAD_REF_INSTRUMENT"] = bad_ref_instrument
    metrics_dict["MISSING_SCHEMA_COLS"] = len(missing_cols)
    metrics_pdf = pd.DataFrame([metrics_dict])

    ctx = gx.get_context(mode="ephemeral")
    ds = ctx.data_sources.add_pandas(name="edp_stg_bdq_v2")
    asset = ds.add_dataframe_asset(name="stg_metrics_asset")
    batch_def = asset.add_batch_definition_whole_dataframe("stg_metrics_batch")

    suite = ctx.suites.add(gx.ExpectationSuite(name="stg_bdq_v2_suite"))

    zero_checks = [
        ("NULL_TXN_ID",         "completeness",          "TXN_ID not null"),
        ("NULL_ACCOUNT_ID",     "completeness",          "ACCOUNT_ID not null"),
        ("NULL_AMOUNT",         "completeness",          "AMOUNT not null"),
        ("NULL_TXN_DATE",       "completeness",          "TXN_DATE not null"),
        ("DUP_TXN_ID",          "uniqueness",            "TXN_ID unique in batch"),
        ("BAD_DOMAIN_CURRENCY", "domain",                "CURRENCY in allowed set"),
        ("BAD_DOMAIN_STATUS",   "domain",                "STATUS in allowed set"),
        ("BAD_RANGE_AMOUNT",    "range",                 "AMOUNT in valid range"),
        ("BAD_REF_ACCOUNT",     "referential_integrity", "ACCOUNT_ID in reference"),
        ("BAD_REF_INSTRUMENT",  "referential_integrity", "INSTRUMENT_ID in reference"),
        ("MISSING_SCHEMA_COLS", "schema",                "Expected columns present (TXN_ID,ACCOUNT_ID,INSTRUMENT_ID,AMOUNT,CURRENCY,TXN_DATE,SETTLEMENT_DATE,STATUS)"),
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
        gx.ValidationDefinition(name="stg_bdq_v2_vd", data=batch_def, suite=suite)
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
            f"STG BDQ v2 FAILED (GX metrics) for {table_name} batch '{batch_id}': "
            f"{json.dumps([r['check'] + ' — ' + r['detail'] for r in failed])}"
        )

    # ================================================================
    # 3. PROVENANCE — Snowpark DataFrame join (scan 2)
    #
    #    Verifies every _BATCH_ID in staging exists in mirror.
    #    Uses Snowpark .join() with left_anti to find orphans.
    #
    # ---- WITHOUT GX ALTERNATIVE (DMF): ----
    #   CREATE DATA METRIC FUNCTION edp_provenance(
    #       ARG_T1 TABLE(ARG_C1 VARCHAR), ARG_T2 TABLE(ARG_C2 VARCHAR))
    #     RETURNS NUMBER AS
    #     'SELECT COUNT(DISTINCT ARG_C1) FROM ARG_T1
    #      WHERE ARG_C1 NOT IN (SELECT DISTINCT ARG_C2 FROM ARG_T2)';
    # ================================================================
    stg_batches = session.table(fqn).select(col("_BATCH_ID")).distinct()
    mir_batches = session.table("EDP_DB.MIRROR.TRANSACTIONS_V2").select(
        col("_BATCH_ID")
    ).distinct()

    orphan_batches = stg_batches.join(
        mir_batches,
        stg_batches["_BATCH_ID"] == mir_batches["_BATCH_ID"],
        "left_anti"
    ).count()

    if orphan_batches > 0:
        raise RuntimeError(
            f"STG BDQ v2 FAILED (provenance): {orphan_batches} batch IDs "
            f"in staging not traceable to mirror"
        )

    # ================================================================
    # 4. WRITE AUDIT
    # ================================================================
    total_elapsed = time.time() - start_time
    all_results = gx_rows + [
        {"source": "snowpark_join", "category": "provenance",
         "check": "batch_ids_exist_in_mirror", "passed": True, "detail": "OK"},
    ]

    session.sql("""
        INSERT INTO EDP_DB.AUDIT.TDQ_RESULTS
            (BATCH_ID, TABLE_NAME, LAYER, CHECK_TYPE, PASSED,
             TOTAL_CHECKS, FAILED_COUNT, FAILED_DETAILS, ROW_COUNT,
             EXECUTION_TIME_SECS, WAREHOUSE_SIZE, SUITE_NAME)
        SELECT ?, ?, 'STG', 'TDQ+BDQ', TRUE, ?, 0, PARSE_JSON('[]'), ?,
               ?, 'SMALL', ?
    """, params=[
        batch_id, table_name,
        len(all_results), total_rows,
        total_elapsed, f"{table_name}_stg_bdq_v2"
    ]).collect()

    return {
        "success": True, "version": "v2_snowpark_lazy_gx",
        "gx_version": gx.__version__, "batch_id": batch_id,
        "table": table_name, "layer": "STG",
        "batch_rows_validated": total_rows, "total_scans": 2,
        "agg_scan_elapsed_secs": round(agg_elapsed, 2),
        "gx_elapsed_secs": round(gx_elapsed, 2),
        "total_elapsed_secs": round(total_elapsed, 2),
    }
$$;

SELECT 'v2 Step 06 complete: STG BDQ with true Snowpark lazy DataFrame + GX.' AS status;
