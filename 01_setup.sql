/*
=============================================================================
  BlackRock EDP v2 — Step 01: Context Setup + Additional Grants
=============================================================================
  This v2 implementation reuses the existing EDP infrastructure created by
  the v1 01_setup.sql (database, schemas, roles, warehouses, grants).

  Run the original implementation/01_setup.sql FIRST if the infrastructure
  does not yet exist.

  This script:
    1. Grants additional privileges needed by v2 (not in v1 setup)
    2. Sets the session context for subsequent v2 scripts
=============================================================================
*/

----------------------------------------------------------------------
-- 1. ADDITIONAL GRANTS (require ACCOUNTADMIN)
--    These are v2-specific privileges not included in v1 01_setup.sql
----------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Required for: snow dbt deploy (Step 6)
-- The CREATE DBT PROJECT privilege is needed to deploy dbt projects
-- as Snowflake-native objects via the snow CLI.
GRANT CREATE DBT PROJECT ON SCHEMA EDP_DB.ORCHESTRATION TO ROLE EDP_ADMIN_ROLE;

----------------------------------------------------------------------
-- 2. SET CONTEXT for subsequent v2 scripts
----------------------------------------------------------------------
USE ROLE EDP_ADMIN_ROLE;
USE DATABASE EDP_DB;
USE WAREHOUSE EDP_INGEST_WH;

SELECT 'v2 Step 01 complete: Grants applied, context set.' AS status;
