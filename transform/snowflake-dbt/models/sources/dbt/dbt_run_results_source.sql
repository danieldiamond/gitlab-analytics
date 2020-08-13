WITH source AS (

    SELECT jsontext
    FROM {{ source('dbt', 'run_results') }}

), flattened AS (

    SELECT 
      d.value as data_by_row
    FROM source
    INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext['results']), outer => true) d

), model_parsed_out AS (

    SELECT
      data_by_row['execution_time']::FLOAT AS model_execution_time,
      data_by_row['node']['name']::VARCHAR AS model_name,
      data_by_row['timing']::ARRAY         AS timings
    FROM flattened
  
), timing_flattened_out AS (
    
    SELECT
      model_execution_time,
      model_name,
      d.value AS data_by_row
    FROM model_parsed_out
    LEFT JOIN LATERAL FLATTEN(INPUT => timings, outer => true) d
  
), compilation_information_parsed AS (

    SELECT 
      model_execution_time,
      model_name,
      data_by_row['started_at']::TIMESTAMP      AS compilation_started_at,
      data_by_row['completed_at']::TIMESTAMP    AS compilation_completed_at
    FROM timing_flattened_out
    WHERE IFNULL(data_by_row['name'], 'compile') = 'compile'

)

SELECT *
FROM compilation_information_parsed