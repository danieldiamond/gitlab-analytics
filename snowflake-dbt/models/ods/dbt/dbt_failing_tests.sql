WITH base AS (

    SELECT *
    FROM {{ ref('dbt_test_results') }}

), failures AS (

    SELECT
      test_name,
      test_error,
      test_result_generated_at
    FROM base
    WHERE 
      is_failed_test
    AND test_result_generated_at = (
        SELECT
          MAX(test_result_generated_at)
        FROM base
    )

)

SELECT *
FROM failures