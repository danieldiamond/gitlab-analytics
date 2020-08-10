WITH base AS (

    SELECT *
    FROM {{ ref('dbt_test_results_source') }}
    QUALIFY row_number() OVER (PARTITION BY test_id ORDER BY test_result_generated_at DESC) = 1

), failures AS (

    SELECT *
    FROM base
    WHERE is_failed_test = True

)

SELECT *
FROM failures
