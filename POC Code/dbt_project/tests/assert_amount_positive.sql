-- Custom dbt test: verify all AMOUNT values are positive
SELECT *
FROM {{ ref('stg_transactions') }}
WHERE AMOUNT <= 0
