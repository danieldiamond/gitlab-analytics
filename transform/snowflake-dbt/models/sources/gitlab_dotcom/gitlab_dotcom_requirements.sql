{{ config({
    "materialized": "incremental",
    "unique_key": "requirement_id"
    })
}}

WITH source AS (

  SELECT *
  FROM {{ source('gitlab_dotcom', 'requirements') }}
    {% if is_incremental() %}
      WHERE updated_at >= (SELECT MAX(updated_at) FROM {{this}})
    {% endif %}
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1

)

, renamed AS (

    SELECT
      id::INTEGER                                 AS requirement_id,
      created_at::TIMESTAMP                       AS created_at,
      updated_at::TIMESTAMP                       AS updated_at,
      project_id::INTEGER                         AS project_id,
      author_id::INTEGER                          AS author_id,
      iid::INTEGER                                AS requirement_iid,
      state::VARCHAR                              AS requirement_state
    FROM source

)

SELECT *
FROM renamed
