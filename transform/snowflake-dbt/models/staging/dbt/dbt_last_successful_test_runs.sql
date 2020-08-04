WITH passing_tests AS (
    
    SELECT *
    FROM {{ ref('dbt_test_results_source') }}
    WHERE is_passed_test = True
    QUALIFY row_number() OVER (PARTITION BY test_id ORDER BY test_result_generated_at DESC) = 1

), failing_tests AS (

    SELECT test_id
    FROM {{ ref('dbt_failing_tests') }}

), last_successful_run AS (

    SELECT *
    FROM passing_tests
    WHERE test_id in (SELECT test_id FROM failing_tests)

)

SELECT *
FROM last_successful_run
