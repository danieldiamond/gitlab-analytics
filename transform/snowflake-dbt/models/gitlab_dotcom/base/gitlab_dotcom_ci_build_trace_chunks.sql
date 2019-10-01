{{ config({
    "schema": "staging"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_build_trace_chunks') }}

), renamed AS (

    SELECT
      build_id::INTEGER   AS ci_build_id,
      chunk_index         AS ci_build_trace_chunk_index,
      data_store          AS ci_build_trace_chunk_data_store,
      raw_data            AS ci_build_trace_chunk_raw_data

    FROM source

)


SELECT *
FROM renamed
