WITH source AS (

    SELECT *
    FROM {{ source('dbt', 'sources') }}

), parsed AS (

    SELECT 
      REPLACE(REGEXP_REPLACE(s.path, '\\[|\\]|''', ''), 'source.gitlab_snowflake.', '')::VARCHAR    AS table_name,
      s.value['max_loaded_at']::TIMESTAMP                                                           AS latest_load_at,
      s.value['max_loaded_at_time_ago_in_s']::FLOAT                                                 AS time_since_loaded_seconds,
      s.value['state']::VARCHAR                                                                     AS source_freshness_state,
      s.value['snapshotted_at']::TIMESTAMP                                                          AS freshness_observed_at
    FROM source 
    INNER JOIN LATERAL FLATTEN(jsontext['sources']) s

)
SELECT *
FROM parsed