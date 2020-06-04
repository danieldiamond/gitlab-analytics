WITH test_results AS (
    
    SELECT *
    FROM {{ ref('dbt_test_results') }}

), failing_tests AS (

    SELECT *
    FROM {{ ref('dbt_failing_tests') }}

), last_successful_run AS (

    SELECT 
      test_name, 
      MAX(test_result_generated_at) AS last_successful_run_at
    FROM test_results
    WHERE
      NOT is_failed_test
    AND test_name in (

        SELECT
          test_name
        FROM failing_tests

    )

)

SELECT *
FROM last_successful_run