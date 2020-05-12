WITH source AS (

    SELECT jsontext
    FROM {{ source('dbt', 'test_run_results') }}

), flattened AS (

    SELECT 
      d.value                             AS data_by_row,
      jsontext['generated_at']::TIMESTAMP AS test_result_generated_at
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT              AS test_execution_time_elapsed,
      data_by_row['node']['name']::VARCHAR              AS test_name,
      data_by_row['error']::VARCHAR                     AS test_error,
      IFF(data_by_row['fail'] = 'true', True, False)    AS is_failed_test,
      IFF(data_by_row['warn'] = 'true', True, False)    AS is_warned_test,
      test_result_generated_at     
    FROM flattened
  
)

SELECT *
FROM model_parsed_out