# BlackRock EDP — Enterprise Data Pipeline POC

## Overview

End-to-end data pipeline for BlackRock's transaction data on Snowflake. Parquet files are ingested into a schema-flexible VARIANT mirror layer, validated across three quality gates (47 data quality checks), transformed into typed staging tables via dbt, and surfaced in an auto-refreshing presentation layer with derived analytics columns.

### Key Design Decisions

| Feature | Approach |
|---------|----------|
| Mirror table | Single VARIANT column — schema-flexible, no ALTER TABLE for new columns |
| Ingestion | Parquet `$1` loaded directly as named VARIANT — no schema inference needed |
| Schema evolution | Automatic — new Parquet columns appear as new VARIANT keys |
| Data quality | Snowpark lazy aggregation (1 scan) + GX validates 1-row metrics DF |
| Scan efficiency | ~8 table scans per pipeline run across all 3 validation layers |
| Scalability | Tested with 5,000 files / 322.5M rows — no bottlenecks at file count |
| dbt | Native Snowflake dbt project — `RAW_DATA:key::type` extraction from VARIANT |

### Scan Count by Layer

| Procedure | Snowpark + GX | Pure SQL | DMF |
|-----------|---------------|----------|-----|
| Ingestion | 1 (single COPY INTO) | 1 | 1 |
| ML TDQ | **2** | 2 | 0 |
| STG BDQ | **2** | 2 | 0 |
| SDM BDQ | **3** | 2-3 | 0 |
| **Total** | **~8** | **~6** | **~2** |

## Deployment Steps

### Prerequisites

- Snowflake account with ACCOUNTADMIN access (for initial grants)
- **Snowflake CLI v3.x+** is required for dbt deploy
  - Check: `snow --version` — must show 3.x or higher
  - Upgrade: `pip install --upgrade snowflake-cli`

### Step-by-Step

```
-- 1. Set context + grant privileges (run as ACCOUNTADMIN)
Run: 01_setup.sql

-- 2. Create VARIANT mirror table + Parquet file format + ingestion SP
Run: 02_mirror_layer_parquet.sql

-- 3. Create audit tables (IF NOT EXISTS — safe to re-run)
Run: 03_audit_tables.sql

-- 4. Generate sample data (30M rows into VARIANT table)
Run: 04_sample_data_variant.sql

-- 5. Create ML TDQ stored procedure
Run: 05_ml_tdq_optimized.sql

-- 6. Deploy dbt project (see detailed steps below)

-- 7. Run dbt to populate staging
Execute in Snowflake:
  EXECUTE DBT PROJECT EDP_DB.ORCHESTRATION.EDP_DBT_PROJECT_V2;

-- 8. Create STG BDQ stored procedure
Run: 06_stg_bdq_optimized.sql

-- 9. Create Dynamic Table (AFTER dbt has populated staging)
Run: 08_dynamic_tables.sql

-- 10. Create SDM BDQ stored procedure
Run: 07_sdm_bdq_optimized.sql

-- 11. Create alerts
Run: 10_alerts.sql

-- 12. Create task DAG + finalizer SP (creates and resumes all 6 tasks)
Run: 09_tasks.sql

-- 13. Test end-to-end
Execute in Snowflake:
  EXECUTE TASK EDP_DB.ORCHESTRATION.INGEST_TASK_V2;

-- 14. Verify all 6 tasks SUCCEEDED
  SELECT NAME, STATE, ERROR_MESSAGE
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(
      SCHEDULED_TIME_RANGE_START => DATEADD('hour', -1, CURRENT_TIMESTAMP())
  ))
  WHERE DATABASE_NAME = 'EDP_DB' AND NAME LIKE '%_V2'
  ORDER BY SCHEDULED_TIME DESC;

-- 15. Verify pipeline run + timing captured
  SELECT * FROM EDP_DB.AUDIT.STRESS_TEST_RESULTS
  ORDER BY CREATED_AT DESC LIMIT 10;
```

### Step 6: Deploy dbt Project (detailed)

The dbt project must be deployed as a Snowflake dbt project object so that
`EXECUTE DBT PROJECT` can run it from the task DAG.

**NOTE:** The `snow` CLI (v3.x+) is the ONLY way to deploy a dbt project to Snowflake.
There is no SQL equivalent (`CREATE DBT PROJECT FROM @stage` does not exist).

**IMPORTANT:** The `--role` flag must specify a role that has `CREATE DBT PROJECT`
privilege on the target schema. This is granted by `01_setup.sql` (Step 1).

#### Deploy Command

```bash
snow dbt deploy edp_dbt_project_v2 \
    --source "/path/to/dbt_project" \
    --database EDP_DB \
    --schema ORCHESTRATION \
    --connection myconnection \
    --role EDP_ADMIN_ROLE
```

#### Verify Deployment

```sql
SHOW DBT PROJECTS IN SCHEMA EDP_DB.ORCHESTRATION;
```

#### Test the Project

```sql
EXECUTE DBT PROJECT EDP_DB.ORCHESTRATION.EDP_DBT_PROJECT_V2;
```

## How the Data Quality Pattern Works

All validation uses a two-phase pattern: **Snowpark computes metrics server-side**, then **Great Expectations validates the results**.

1. **Snowpark lazy aggregation** — all COUNT_IF checks combined into one SQL query via `.agg()`
2. **Server-side execution** — `.collect()` returns a single row with all metrics
3. **GX validates metrics** — e.g., "NULL_TXN_ID should be 0", "TOTAL_ROWS >= 1 (no upper bound)"
4. **Cross-batch/cross-table checks** — separate Snowpark DataFrame join (second scan)

GX operates on a 1-row Pandas DataFrame of aggregated counts — not millions of raw records. This keeps GX's structured reporting while pushing all heavy computation to Snowflake compute.

## "Without GX" Alternatives

Every validation procedure includes inline comments showing:

- **Native SQL**: The raw COUNT_IF query without GX overhead (~2-5s startup saved)
- **DMF**: The CREATE DATA METRIC FUNCTION approach for zero-SP automation

Search for `WITHOUT GX ALTERNATIVE` in the SQL files to find all alternatives.

## Automated Timing Capture

The finalizer task (`FINALIZER_TASK_V2`) calls `RUN_FINALIZER_V2()`, which automatically captures per-task timing after each pipeline run:

- **Timing sources**: TDQ_RESULTS (execution timestamps), COPY_HISTORY (ingest start)
- **Warehouse sizes**: Read dynamically via `SHOW WAREHOUSES` — no hardcoded values
- **Output**: 6 rows in `STRESS_TEST_RESULTS` (one per task + total E2E)

## Key Gotchas

1. **TRUNCATE breaks change tracking** — after truncating a table, re-enable change tracking and recreate the Dynamic Table
2. **DOWNSTREAM TARGET_LAG** — must explicitly `ALTER DYNAMIC TABLE ... REFRESH` from stored procedures
3. **Batch ID resolution** — use `ORDER BY _LOADED_AT DESC LIMIT 1`, not `MAX(_BATCH_ID)` (UUID v4 is random)
4. **Parquet vs CSV ingestion** — Parquet files are self-describing (column names in file metadata). `COPY INTO` loads Parquet `$1` directly as a named VARIANT. For CSV sources, positional `$N` mapping with `OBJECT_CONSTRUCT` is required. See `02_mirror_layer_parquet.sql` (Parquet) and `02_mirror_layer_variant.sql` (CSV).
5. **TRY_CAST from VARIANT** — `TRY_CAST(variant_col AS type)` fails. Must cast to VARCHAR first: `TRY_CAST(variant_col::VARCHAR AS type)`.
6. **snow CLI v3.x+ required** — `snow dbt deploy` requires Snowflake CLI v3.x+.
7. **CREATE DBT PROJECT privilege** — must be granted explicitly: `GRANT CREATE DBT PROJECT ON SCHEMA ... TO ROLE ...` (included in `01_setup.sql`).

## File Inventory

| File | Purpose |
|------|---------|
| 01_setup.sql | Database, schema, role, warehouse setup + grants |
| 02_mirror_layer_parquet.sql | Parquet ingestion: file format, stage, VARIANT table, SP |
| 02_mirror_layer_variant.sql | CSV ingestion: positional OBJECT_CONSTRUCT mapping |
| 03_audit_tables.sql | Audit tables: TDQ_RESULTS, PIPELINE_RUNS, PERFORMANCE_BENCHMARKS, STRESS_TEST_RESULTS |
| 04_sample_data_variant.sql | Synthetic test data + reference tables |
| 05_ml_tdq_optimized.sql | ML TDQ: Snowpark lazy agg + GX metrics (17 checks, 2 scans) |
| 06_stg_bdq_optimized.sql | STG BDQ: Snowpark lazy agg + ref joins + GX (13 checks, 2 scans) |
| 07_sdm_bdq_optimized.sql | SDM BDQ: DT refresh + agg + cross-table + GX (17 checks, 3 scans) |
| 08_dynamic_tables.sql | Presentation layer Dynamic Table with derived columns |
| 09_tasks.sql | 6-task DAG + RUN_FINALIZER_V2 SP (auto-captures per-task timing) |
| 10_alerts.sql | Email alerts on quality check failure |
| e2e_test.sql | End-to-end test (CSV path) |
| e2e_test_parquet.sql | End-to-end test (Parquet path) |
| e2e_stress_test_parquet.sql | Stress test: 5,000 files, 322.5M rows, automated timing |
| EDP_POC_Architecture_Parquet.html | Architecture doc — Parquet variant |
| dbt_project/ | dbt project: VARIANT-to-typed transformation |

## Stress Test Results (322.5M rows, 5,000 Parquet files)

| Task | Warehouse | Size | Duration | % of Total |
|------|-----------|------|----------|------------|
| INGEST_TASK_V2 | EDP_INGEST_WH | MEDIUM | 79 sec | 22% |
| ML_TDQ_TASK_V2 | EDP_TDQ_WH | SMALL | 13 sec | 4% |
| DBT_TASK_V2 | EDP_DBT_WH | SMALL | 60 sec | 16% |
| STG_BDQ_TASK_V2 | EDP_TDQ_WH | SMALL | 8 sec | 2% |
| SDM_BDQ_TASK_V2 | EDP_TDQ_WH | SMALL | 198 sec | 54% |
| **TOTAL E2E** | ALL | MEDIUM | **367 sec (~6.1 min)** | 100% |

Timing is auto-captured by `RUN_FINALIZER_V2` (the last task in the DAG). Warehouse sizes are read dynamically via `SHOW WAREHOUSES`.
