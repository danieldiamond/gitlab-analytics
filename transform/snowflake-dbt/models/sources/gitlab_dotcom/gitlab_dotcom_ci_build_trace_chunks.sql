WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'ci_build_trace_chunks') }}

), renamed AS (

    SELECT
      build_id::NUMBER     AS ci_build_id,
      chunk_index::VARCHAR  AS chunk_index,
      data_store::VARCHAR   AS data_store,
      raw_data::VARCHAR     AS raw_data

    FROM source

)


SELECT *
FROM renamed
